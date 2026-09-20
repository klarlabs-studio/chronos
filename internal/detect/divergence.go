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

// Divergence detects PatternTypeDivergence: two series in the same
// scope whose absolute outcome gap is growing. Method: align the last
// min(len(a), len(b)) outcomes by ordinal index, form |a−b| at each
// step, fit OLS against ordinal index, emit when the slope clears
// CHRONOS_DIVERGENCE_MIN_SLOPE.
//
// Convergence is the mirror (negative slope). Correlation asks whether
// the series move together; Divergence/Convergence ask whether the gap
// between them is widening or narrowing — orthogonal questions.
//
// One signal per pair; Series is the lex-smaller entity ID.
type Divergence struct {
	cfg *config.Config
	now func() time.Time
}

// NewDivergence wires a Divergence detector from configuration.
func NewDivergence(cfg *config.Config) *Divergence {
	return &Divergence{cfg: cfg, now: time.Now}
}

// Pattern reports the PatternType this detector emits.
func (d *Divergence) Pattern() domain.PatternType { return domain.PatternTypeDivergence }

// Detect runs pairwise absolute-gap slope tests within the scope.
func (d *Divergence) Detect(_ context.Context, scopeID uuid.UUID, states []chronos.EntityState) []domain.Signal {
	if d.cfg.DivergenceMinPoints < 3 {
		return nil
	}
	return keepValid(pairGapSignals(scopeID, states, d.cfg.DivergenceMinPoints, d.cfg.DivergenceMinSlope, +1, domain.PatternTypeDivergence, "pair_divergence", detectorVersionDivergence, d.cfg, d.now))
}

// Convergence detects PatternTypeConvergence: two series in the same
// scope whose absolute outcome gap is shrinking. Mirror of Divergence.
type Convergence struct {
	cfg *config.Config
	now func() time.Time
}

// NewConvergence wires a Convergence detector from configuration.
func NewConvergence(cfg *config.Config) *Convergence {
	return &Convergence{cfg: cfg, now: time.Now}
}

// Pattern reports the PatternType this detector emits.
func (c *Convergence) Pattern() domain.PatternType { return domain.PatternTypeConvergence }

// Detect runs pairwise absolute-gap slope tests within the scope.
func (c *Convergence) Detect(_ context.Context, scopeID uuid.UUID, states []chronos.EntityState) []domain.Signal {
	if c.cfg.ConvergenceMinPoints < 3 {
		return nil
	}
	return keepValid(pairGapSignals(scopeID, states, c.cfg.ConvergenceMinPoints, c.cfg.ConvergenceMinSlope, -1, domain.PatternTypeConvergence, "pair_convergence", detectorVersionConvergence, c.cfg, c.now))
}

// pairGapSignals emits one signal per pair whose |a−b| OLS slope has
// the requested sign and clears minSlope in magnitude. direction is
// +1 for divergence (growing gap) and −1 for convergence (shrinking).
func pairGapSignals(
	scopeID uuid.UUID,
	states []chronos.EntityState,
	minPoints int,
	minSlope float64,
	direction float64,
	pattern domain.PatternType,
	kind, version string,
	cfg *config.Config,
	now func() time.Time,
) []domain.Signal {
	series := bySeries(states)
	if len(series) < 2 {
		return nil
	}
	ids := make([]uuid.UUID, 0, len(series))
	for id := range series {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i].String() < ids[j].String() })

	var signals []domain.Signal
	for i := 0; i < len(ids); i++ {
		for j := i + 1; j < len(ids); j++ {
			sig, ok := gapPair(scopeID, ids[i], ids[j], series[ids[i]], series[ids[j]], minPoints, minSlope, direction, pattern, kind, version, cfg, now)
			if !ok {
				continue
			}
			signals = append(signals, sig)
		}
	}
	return signals
}

func gapPair(
	scopeID, idA, idB uuid.UUID,
	a, b []chronos.EntityState,
	minPoints int,
	minSlope float64,
	direction float64,
	pattern domain.PatternType,
	kind, version string,
	cfg *config.Config,
	now func() time.Time,
) (domain.Signal, bool) {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	if n < minPoints {
		return domain.Signal{}, false
	}
	xs := outcomes(a[len(a)-n:])
	ys := outcomes(b[len(b)-n:])
	gaps := make([]float64, n)
	idx := make([]float64, n)
	for i := 0; i < n; i++ {
		gaps[i] = math.Abs(xs[i] - ys[i])
		idx[i] = float64(i)
	}
	slope, _, r2 := linearRegression(idx, gaps)
	if !isFinite(slope) || !isFinite(r2) {
		return domain.Signal{}, false
	}
	// Require the slope to point the right way and clear the floor.
	if direction > 0 {
		if slope < minSlope {
			return domain.Signal{}, false
		}
	} else {
		if slope > -minSlope {
			return domain.Signal{}, false
		}
	}

	absSlope := math.Abs(slope)
	strength := clamp01(absSlope / (2 * minSlope))
	if r2 > 0 {
		// Prefer fits that actually explain the gap trajectory.
		strength = clamp01(strength * (0.5 + 0.5*r2))
	}
	confidence := strength * sampleFactor(n, 2*minPoints)

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
		"slope":     slope,
		"abs_slope": absSlope,
		"r2":        r2,
		"start_gap": gaps[0],
		"end_gap":   gaps[n-1],
		"n":         float64(n),
	}

	return domain.Signal{
		ID:              uuid.New(),
		ScopeID:         scopeID,
		Series:          idA,
		Pattern:         pattern,
		DetectedAt:      now(),
		Window:          domain.TimeWindow{Start: start, End: end},
		Strength:        strength,
		Confidence:      confidence,
		ConfidenceClass: ClassifyConfidence(n, minPoints, cfg),
		Metrics:         metrics,
		Explanation:     explainSeries(a[len(a)-n:], 1, minSlope, version),
		Evidence: []domain.Evidence{{
			Series:  idB,
			Time:    end,
			Kind:    kind,
			Score:   absSlope,
			Metrics: metrics,
		}},
	}, true
}
