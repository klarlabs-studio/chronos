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
// scope whose absolute outcome gap is growing over wall-clock time.
//
// Pipeline: temporally align the series ([AlignNearest]), form
// gap(t) = |A(t)−B(t)| on each pair, regress the gap against hours
// since the first pair's anchor (same time basis as Trend). Emit only
// when the slope is positive, clears CHRONOS_DIVERGENCE_MIN_SLOPE
// (gap units per hour), and R² clears CHRONOS_DIVERGENCE_MIN_R2.
// A directional but erratic gap is not sustained divergence.
//
// Strength is the shape (slope magnitude scaled by R²). Confidence is
// the evidence (aligned sample size and alignment tightness), not the
// size of the gap. Sampling faster does not inflate the slope.
//
// Convergence is the mirror (negative slope). One signal per pair;
// Series is the lex-smaller entity ID. The window covers only the
// aligned observations.
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
	return keepValid(pairGapSignals(scopeID, states, d.cfg.DivergenceMinPoints, d.cfg.DivergenceMinSlope, d.cfg.DivergenceMinR2, d.cfg.AlignTolerance, +1, domain.PatternTypeDivergence, "pair_divergence", detectorVersionDivergence, d.cfg, d.now))
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
	return keepValid(pairGapSignals(scopeID, states, c.cfg.ConvergenceMinPoints, c.cfg.ConvergenceMinSlope, c.cfg.ConvergenceMinR2, c.cfg.AlignTolerance, -1, domain.PatternTypeConvergence, "pair_convergence", detectorVersionConvergence, c.cfg, c.now))
}

// pairGapSignals emits one signal per pair whose temporally aligned
// |a−b| OLS slope (gap units per hour) has the requested sign, clears
// minSlope, and whose R² clears minR2. direction is +1 for divergence
// and −1 for convergence.
func pairGapSignals(
	scopeID uuid.UUID,
	states []chronos.EntityState,
	minPoints int,
	minSlope, minR2 float64,
	tol time.Duration,
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
			sig, ok := gapPair(scopeID, ids[i], ids[j], series[ids[i]], series[ids[j]], minPoints, minSlope, minR2, tol, direction, pattern, kind, version, cfg, now)
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
	minSlope, minR2 float64,
	tol time.Duration,
	direction float64,
	pattern domain.PatternType,
	kind, version string,
	cfg *config.Config,
	now func() time.Time,
) (domain.Signal, bool) {
	pairs := AlignNearest(a, b, tol)
	n := len(pairs)
	if n < minPoints {
		return domain.Signal{}, false
	}
	gaps := make([]float64, n)
	alignedA := make([]chronos.EntityState, n)
	for i, p := range pairs {
		gaps[i] = math.Abs(p.A.Outcome() - p.B.Outcome())
		alignedA[i] = p.A
	}
	xs := pairAnchorHours(pairs)
	slope, _, r2 := linearRegression(xs, gaps)
	if !isFinite(slope) || !isFinite(r2) {
		return domain.Signal{}, false
	}
	if r2 < minR2 {
		return domain.Signal{}, false
	}
	if direction > 0 {
		if slope < minSlope {
			return domain.Signal{}, false
		}
	} else if slope > -minSlope {
		return domain.Signal{}, false
	}

	absSlope := math.Abs(slope)
	// Shape, not sample size: magnitude saturates at 2× the floor and
	// is discounted by a poor fit. A zero floor means any passing
	// slope already cleared the only magnitude gate, so it saturates.
	magnitude := 1.0
	if minSlope > 0 {
		magnitude = clamp01(absSlope / (2 * minSlope))
	}
	strength := clamp01(magnitude * r2)
	// Evidence, not magnitude. A steep gap seen a handful of times
	// stays strong and poorly supported.
	confidence := clamp01(sampleFactor(n, 2*minPoints) * alignmentQuality(pairs, tol))
	start, end := alignedWindow(pairs)

	metrics := map[string]float64{
		"slope":                       slope,
		"slope_per_hour":              slope,
		"abs_slope":                   absSlope,
		"r2":                          r2,
		"r_squared":                   r2,
		"start_gap":                   gaps[0],
		"end_gap":                     gaps[n-1],
		"n":                           float64(n),
		"aligned_samples":             float64(n),
		"alignment_tolerance_seconds": tol.Seconds(),
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
		Explanation:     explainSeries(alignedA, 1, minSlope, version),
		Evidence: []domain.Evidence{{
			Series:  idB,
			Time:    end,
			Kind:    kind,
			Score:   absSlope,
			Metrics: metrics,
		}},
	}, true
}
