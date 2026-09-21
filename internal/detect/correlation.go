package detect

import (
	"context"
	"math"
	"sort"
	"time"

	"github.com/felixgeelhaar/chronos"
	"github.com/felixgeelhaar/chronos/internal/config"
	"github.com/felixgeelhaar/chronos/internal/domain"
	"github.com/google/uuid"
)

// Correlation detects PatternTypeCorrelation: two series in the same
// scope whose outcome metrics move together (positive r) or opposite
// (negative r). Pearson correlation is computed only on temporally
// aligned pairs ([AlignNearest], tolerance CHRONOS_ALIGN_TOLERANCE).
// Slice position is not evidence. A morning series and an afternoon
// series with the same values produce no signal when no timestamps
// fall within tolerance.
//
// Minimum sample size counts aligned pairs, not raw observations.
// The signal window spans only the observations that entered a pair.
//
// One signal is emitted per pair, with Series being the
// lexicographically smaller of the two entity IDs (so signals are
// deterministic across runs and not duplicated). Evidence is a
// "pair_correlation" record pointing at the partner series.
//
// Cost is O(N²) in series count per scope; the engine's
// MaxSignalsPerRun cap and SignalRepository filters provide downstream
// triage.
type Correlation struct {
	cfg *config.Config
	now func() time.Time
}

// NewCorrelation wires a Correlation detector from configuration.
func NewCorrelation(cfg *config.Config) *Correlation { return &Correlation{cfg: cfg, now: time.Now} }

// Pattern reports the PatternType this detector emits.
func (c *Correlation) Pattern() domain.PatternType { return domain.PatternTypeCorrelation }

// Detect runs pairwise Pearson correlations within the scope.
func (c *Correlation) Detect(_ context.Context, scopeID uuid.UUID, states []chronos.EntityState) []domain.Signal {
	// Two points are always collinear; refuse floors that manufacture |r|=1.
	if c.cfg.CorrelationMinPoints < 3 {
		return nil
	}
	series := bySeries(states)
	if len(series) < 2 {
		return nil
	}

	// Stable iteration order so signals (and pair ownership) are
	// deterministic across runs.
	ids := make([]uuid.UUID, 0, len(series))
	for id := range series {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i].String() < ids[j].String() })

	var signals []domain.Signal
	for i := 0; i < len(ids); i++ {
		for j := i + 1; j < len(ids); j++ {
			sig, ok := c.pair(scopeID, ids[i], ids[j], series[ids[i]], series[ids[j]])
			if !ok {
				continue
			}
			signals = append(signals, sig)
		}
	}
	return keepValid(signals)
}

func (c *Correlation) pair(scopeID, idA, idB uuid.UUID, a, b []chronos.EntityState) (domain.Signal, bool) {
	pairs := AlignNearest(a, b, c.cfg.AlignTolerance)
	n := len(pairs)
	if n < c.cfg.CorrelationMinPoints {
		return domain.Signal{}, false
	}
	xs := make([]float64, n)
	ys := make([]float64, n)
	alignedA := make([]chronos.EntityState, n)
	for i, p := range pairs {
		xs[i] = p.A.Outcome()
		ys[i] = p.B.Outcome()
		alignedA[i] = p.A
	}
	r := pearsonCorrelation(xs, ys)
	if math.Abs(r) < c.cfg.CorrelationMin {
		return domain.Signal{}, false
	}

	strength := clamp01(math.Abs(r))
	confidence := strength * sampleFactor(n, 2*c.cfg.CorrelationMinPoints)
	start, end := alignedWindow(pairs)

	metrics := map[string]float64{
		"r":                           r,
		"abs_r":                       math.Abs(r),
		"n":                           float64(n),
		"aligned_samples":             float64(n),
		"alignment_tolerance_seconds": c.cfg.AlignTolerance.Seconds(),
		"direction":                   directionOf(r),
	}

	return domain.Signal{
		ID:              uuid.New(),
		ScopeID:         scopeID,
		Series:          idA,
		Pattern:         domain.PatternTypeCorrelation,
		DetectedAt:      c.now(),
		Window:          domain.TimeWindow{Start: start, End: end},
		Strength:        strength,
		Confidence:      confidence,
		ConfidenceClass: ClassifyConfidence(n, c.cfg.CorrelationMinPoints, c.cfg),
		Metrics:         metrics,
		Explanation:     explainSeries(alignedA, 1, c.cfg.CorrelationMin, detectorVersionCorrelation),
		Evidence: []domain.Evidence{{
			Series:  idB,
			Time:    end,
			Kind:    "pair_correlation",
			Score:   math.Abs(r),
			Metrics: metrics,
		}},
	}, true
}

// directionOf returns +1 for positive r, -1 for negative, 0 for zero.
// Stored in metrics so consumers can branch on direction without
// re-reading r.
func directionOf(r float64) float64 {
	switch {
	case r > 0:
		return 1
	case r < 0:
		return -1
	default:
		return 0
	}
}
