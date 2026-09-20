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
// (negative r). Pearson correlation is computed on the last
// min(len(a), len(b)) outcomes of each series — the alignment is by
// ordinal index, so this assumes both series share roughly the same
// observation cadence. Adapters that ingest at very different
// cadences will produce noisy correlations.
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
	if c.cfg.CorrelationMinPoints < 2 {
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
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	if n < c.cfg.CorrelationMinPoints {
		return domain.Signal{}, false
	}
	xs := outcomes(a[len(a)-n:])
	ys := outcomes(b[len(b)-n:])
	r := pearsonCorrelation(xs, ys)
	if math.Abs(r) < c.cfg.CorrelationMin {
		return domain.Signal{}, false
	}

	strength := clamp01(math.Abs(r))
	confidence := strength * sampleFactor(n, 2*c.cfg.CorrelationMinPoints)

	// Window covers the overlap interval — earliest of the two starts
	// to latest of the two ends.
	startA := a[len(a)-n].Timestamp
	startB := b[len(b)-n].Timestamp
	start := startA
	if startB.Before(startA) {
		start = startB
	}
	endA := a[len(a)-1].Timestamp
	endB := b[len(b)-1].Timestamp
	end := endA
	if endB.After(endA) {
		end = endB
	}

	metrics := map[string]float64{
		"r":         r,
		"abs_r":     math.Abs(r),
		"n":         float64(n),
		"direction": directionOf(r),
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
		Explanation:     explainSeries(a[len(a)-n:], 1, c.cfg.CorrelationMin, detectorVersionCorrelation),
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
