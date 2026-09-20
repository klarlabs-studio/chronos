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
// Seasonality, Correlation, ChangePoint, OutlierCluster. Cross-scope
// detectors are registered separately via DefaultCrossScopeDetectors.
// Callers wanting a custom subset construct an Engine directly with
// explicit detectors.
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
	}
}

// Detect groups states by scope and runs every detector against each
// group. The combined output is sorted by detected-at descending,
// confidence descending, and capped at cfg.MaxSignalsPerRun.
//
// Scopes are visited in ascending scope-ID order. That sort is not
// cosmetic: the final sort is stable, so signals that tie on
// detected-at and confidence keep the order the detectors produced
// them in, and MaxSignalsPerRun then truncates the tail. Visiting
// the scope map in Go's randomised iteration order would make the
// surviving set differ between runs over identical input.
func (e *Engine) Detect(ctx context.Context, states []chronos.EntityState) []domain.Signal {
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

	sort.SliceStable(all, func(i, j int) bool {
		if !all[i].DetectedAt.Equal(all[j].DetectedAt) {
			return all[i].DetectedAt.After(all[j].DetectedAt)
		}
		return all[i].Confidence > all[j].Confidence
	})

	if e.cfg.MaxSignalsPerRun > 0 && len(all) > e.cfg.MaxSignalsPerRun {
		if e.metrics != nil {
			for _, s := range all[e.cfg.MaxSignalsPerRun:] {
				e.metrics.ObserveDetectorTruncated(string(s.Pattern))
			}
		}
		all = all[:e.cfg.MaxSignalsPerRun]
	}
	return all
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
