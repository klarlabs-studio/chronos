package detect

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/felixgeelhaar/chronos"
	"github.com/felixgeelhaar/chronos/internal/config"
	"github.com/felixgeelhaar/chronos/internal/domain"
	"github.com/felixgeelhaar/chronos/internal/observability"
	"github.com/google/uuid"
)

// Engine fans observations out to a set of detectors, deduplicates and
// sorts the resulting signals, and applies a global cap.
//
// Construction is explicit: callers register the detectors they want.
// The default set (see [DefaultDetectors]) includes Recurrence; new
// pattern detectors are added by appending to the slice.
type Engine struct {
	cfg            *config.Config
	detectors      []Detector
	crossDetectors []CrossScopeDetector
	parallel       bool
	metrics        *observability.Metrics
}

// NewEngine builds an Engine. If detectors is empty, DefaultDetectors
// is used. Cross-scope detectors come from DefaultCrossScopeDetectors
// when not set explicitly via WithCrossScopeDetectors.
func NewEngine(cfg *config.Config, detectors ...Detector) *Engine {
	if len(detectors) == 0 {
		detectors = DefaultDetectors(cfg)
	}
	return &Engine{
		cfg:            cfg,
		detectors:      detectors,
		crossDetectors: DefaultCrossScopeDetectors(cfg),
	}
}

// WithCrossScopeDetectors replaces the engine's cross-scope detector
// set. Pass an empty slice to disable cross-scope detection entirely.
func (e *Engine) WithCrossScopeDetectors(ds []CrossScopeDetector) *Engine {
	e.crossDetectors = ds
	return e
}

// WithParallelDetectors enables parallel execution of per-scope
// detectors. Each (scope, detector) pair runs in its own goroutine.
// Detectors are pure functions of their input plus configuration so
// the parallelism is safe.
//
// Off by default — sequential execution keeps deterministic
// signal ordering for tests and small deployments. Operators with
// many detectors and many scopes flip this on.
func (e *Engine) WithParallelDetectors(on bool) *Engine {
	e.parallel = on
	return e
}

// WithMetrics attaches a metrics registry so Detect records per-pattern
// latency, emit counts, skips (zero-signal runs), and cap truncation.
// Optional — tests and embedders omit it.
func (e *Engine) WithMetrics(m *observability.Metrics) *Engine {
	e.metrics = m
	return e
}

// DefaultCrossScopeDetectors returns the standard cross-scope detector
// set. Today this is just CrossScopeCorrelation; future detectors that
// need to compare across scopes (e.g. cross-scope outlier clusters)
// will be added here.
func DefaultCrossScopeDetectors(cfg *config.Config) []CrossScopeDetector {
	return []CrossScopeDetector{
		NewCrossScopeCorrelation(cfg),
	}
}

// DefaultDetectors returns the standard per-scope detector set wired
// with cfg: Recurrence, Trend, Spike, Drop, Stall, Anomaly,
// Seasonality, Correlation, ChangePoint, OutlierCluster, Oscillation,
// Divergence, Convergence. Cross-scope detectors are registered
// separately via DefaultCrossScopeDetectors. Callers wanting a custom
// subset construct an Engine directly with explicit detectors.
func DefaultDetectors(cfg *config.Config) []Detector {
	return []Detector{
		NewRecurrence(cfg),
		NewTrend(cfg),
		NewSpike(cfg),
		NewDrop(cfg),
		NewStall(cfg),
		NewAnomaly(cfg),
		NewSeasonality(cfg),
		NewCorrelation(cfg),
		NewChangePoint(cfg),
		NewOutlierCluster(cfg),
		NewOscillation(cfg),
		NewDivergence(cfg),
		NewConvergence(cfg),
	}
}

// Detect groups states by scope and runs every detector against each
// group. Every signal from one call shares a single DetectedAt, the
// output is capped at cfg.MaxSignalsPerRun by capFairly (round-robin
// across patterns, most confident first within each), and the survivors
// are returned sorted by confidence descending.
//
// Scopes are visited in ascending scope-ID order. That sort is not
// cosmetic: every sort here is stable, so signals that tie on
// confidence keep the order the detectors produced them in. Visiting
// the scope map in Go's randomised iteration order would make the
// surviving set differ between runs over identical input.
func (e *Engine) Detect(ctx context.Context, states []chronos.EntityState) []domain.Signal {
	return e.DetectExcluding(ctx, states, nil)
}

// DetectExcluding is Detect with one difference: signals for which known
// returns true are removed BEFORE the MaxSignalsPerRun cap is applied,
// so the cap is spent only on signals the caller can still use. A nil
// known excludes nothing, which is exactly Detect.
//
// It exists for the scheduler. Every tick re-derives every event in the
// lookback -- last hour's change point has the same identity and window
// on every tick -- and the scheduler drops the ones it has already
// saved. Filtering after the cap meant that once saved events outranked
// new ones they took every slot, every tick: the new events were
// truncated before the duplicate check could see them, and a deployment
// went silent for as long as its history stayed more confident than its
// present. Peak memory is unchanged; the full candidate list was already
// built before the cap ever truncated it.
func (e *Engine) DetectExcluding(ctx context.Context, states []chronos.EntityState, known func(domain.Signal) bool) []domain.Signal {
	if len(states) == 0 {
		return nil
	}
	byScope := make(map[uuid.UUID][]chronos.EntityState)
	for _, s := range states {
		byScope[s.ScopeID] = append(byScope[s.ScopeID], s)
	}

	var all []domain.Signal
	if e.parallel {
		all = e.detectParallel(ctx, byScope)
	} else {
		for _, scopeID := range sortedUUIDKeys(byScope) {
			scoped := byScope[scopeID]
			sortByTimestampAsc(scoped)
			for _, d := range e.detectors {
				all = append(all, e.runDetector(ctx, d, scopeID, scoped)...)
			}
		}
	}
	for _, d := range e.crossDetectors {
		all = append(all, e.runCross(ctx, d, states)...)
	}

	// The last gate before the pipeline persists and the API serves.
	// Each detector already filters its own output, but Detector is an
	// interface: an out-of-tree detector is under the same contract and
	// nothing else checks that it kept it. A signal that fails its own
	// invariants is dropped rather than repaired — the engine has no
	// basis on which to invent the quantity the detector failed to
	// measure.
	all = keepValid(all)

	for i := range all {
		all[i].ID = domain.PerceptionID(all[i])
	}

	stampRun(all)

	if known != nil {
		novel := all[:0]
		for _, s := range all {
			if !known(s) {
				novel = append(novel, s)
			}
		}
		all = novel
	}

	if e.cfg.MaxSignalsPerRun > 0 && len(all) > e.cfg.MaxSignalsPerRun {
		var dropped []domain.Signal
		all, dropped = capFairly(all, e.cfg.MaxSignalsPerRun)
		if e.metrics != nil {
			for _, s := range dropped {
				e.metrics.ObserveDetectorTruncated(string(s.Pattern))
			}
		}
	}

	sort.SliceStable(all, func(i, j int) bool {
		if !all[i].DetectedAt.Equal(all[j].DetectedAt) {
			return all[i].DetectedAt.After(all[j].DetectedAt)
		}
		return all[i].Confidence > all[j].Confidence
	})
	return all
}

// stampRun gives every signal from one Detect call the same DetectedAt:
// the latest any detector recorded.
//
// Each detector stamps time.Now() as it runs, so within one call the
// timestamps differ only by execution order -- microseconds of noise
// that carry no information about the signals. They were nonetheless
// the primary sort key ahead of the MaxSignalsPerRun cap, which meant
// the detector registered last won every slot. When Oscillation,
// Divergence and Convergence were appended to DefaultDetectors, that
// silenced spike, drop, trend and change-point in production.
//
// PerceptionID does not hash DetectedAt, so identity and deduplication
// are unaffected.
func stampRun(sigs []domain.Signal) {
	var runAt time.Time
	for _, s := range sigs {
		if s.DetectedAt.After(runAt) {
			runAt = s.DetectedAt
		}
	}
	for i := range sigs {
		sigs[i].DetectedAt = runAt
	}
}

// capFairly keeps at most limit signals, taking them round-robin across
// patterns in a fixed order, and most confident first within each.
//
// The cap exists to bound memory -- an unlimited run was OOMKilled -- and
// it is not a ranking across patterns. Truncating one confidence-sorted
// list would let a detector that emits many signals at high confidence
// starve one that emits few: pairwise detectors are O(N^2) in entities
// and report confidence near 1, so across 67 entities they would take
// every slot from the detectors that matter most for incident
// prediction. Round-robin gives each pattern an equal claim, and a
// pattern with fewer signals than its share keeps all of them while the
// remainder goes to the patterns that can use it.
//
// Patterns are visited in lexical order and each group is sorted
// stably, so the surviving set is deterministic for identical input.
func capFairly(sigs []domain.Signal, limit int) (kept, dropped []domain.Signal) {
	byPattern := make(map[domain.PatternType][]domain.Signal)
	for _, s := range sigs {
		byPattern[s.Pattern] = append(byPattern[s.Pattern], s)
	}
	patterns := make([]domain.PatternType, 0, len(byPattern))
	for p, group := range byPattern {
		patterns = append(patterns, p)
		sort.SliceStable(group, func(i, j int) bool {
			return group[i].Confidence > group[j].Confidence
		})
	}
	sort.Slice(patterns, func(i, j int) bool { return patterns[i] < patterns[j] })

	kept = make([]domain.Signal, 0, limit)
	next := make(map[domain.PatternType]int, len(patterns))
	for len(kept) < limit {
		progressed := false
		for _, p := range patterns {
			if len(kept) == limit {
				break
			}
			if i := next[p]; i < len(byPattern[p]) {
				kept = append(kept, byPattern[p][i])
				next[p] = i + 1
				progressed = true
			}
		}
		if !progressed {
			break
		}
	}
	for _, p := range patterns {
		dropped = append(dropped, byPattern[p][next[p]:]...)
	}
	return kept, dropped
}

func (e *Engine) runDetector(ctx context.Context, d Detector, scopeID uuid.UUID, states []chronos.EntityState) []domain.Signal {
	start := time.Now()
	sigs := d.Detect(ctx, scopeID, states)
	e.observeRun(d.Pattern(), time.Since(start), len(sigs))
	return sigs
}

func (e *Engine) runCross(ctx context.Context, d CrossScopeDetector, states []chronos.EntityState) []domain.Signal {
	start := time.Now()
	sigs := d.CrossDetect(ctx, states)
	e.observeRun(d.Pattern(), time.Since(start), len(sigs))
	return sigs
}

func (e *Engine) observeRun(pattern domain.PatternType, dur time.Duration, n int) {
	if e.metrics == nil {
		return
	}
	e.metrics.ObserveDetector(string(pattern), dur, n)
}

// detectParallel runs every (scope, detector) pair in its own
// goroutine and gathers results. Sort happens after the merge so
// final ordering is identical to the sequential path.
func (e *Engine) detectParallel(ctx context.Context, byScope map[uuid.UUID][]chronos.EntityState) []domain.Signal {
	type job struct {
		scopeID uuid.UUID
		states  []chronos.EntityState
		det     Detector
	}
	jobs := make([]job, 0, len(byScope)*len(e.detectors))
	// Scope-major in sorted scope order, matching the sequential
	// path: results are gathered by job index, so the job order is
	// the output order for signals that tie on the final sort keys.
	for _, scopeID := range sortedUUIDKeys(byScope) {
		scoped := byScope[scopeID]
		sortByTimestampAsc(scoped)
		for _, d := range e.detectors {
			jobs = append(jobs, job{scopeID: scopeID, states: scoped, det: d})
		}
	}
	results := make([][]domain.Signal, len(jobs))
	var wg sync.WaitGroup
	wg.Add(len(jobs))
	for i, j := range jobs {
		i, j := i, j
		go func() {
			defer wg.Done()
			results[i] = e.runDetector(ctx, j.det, j.scopeID, j.states)
		}()
	}
	wg.Wait()
	var out []domain.Signal
	for _, r := range results {
		out = append(out, r...)
	}
	return out
}

// sortByTimestampAsc orders observations ascending by (timestamp, ID).
// Equal timestamps are broken by observation ID so detector input order
// is deterministic regardless of ingest or store retrieval order — see
// docs/temporal-contract.md.
func sortByTimestampAsc(states []chronos.EntityState) {
	sort.SliceStable(states, func(i, j int) bool {
		return beforeByTimeThenID(states[i], states[j])
	})
}
