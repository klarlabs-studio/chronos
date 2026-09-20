package detect

import (
	"context"
	"math"
	"time"

	"github.com/felixgeelhaar/chronos"
	"github.com/felixgeelhaar/chronos/internal/config"
	"github.com/felixgeelhaar/chronos/internal/domain"
	"github.com/google/uuid"
)

// ChangePoint detects PatternTypeChangePoint: a step change in the
// mean of the outcome metric. Distinct from Spike/Drop (short-lived
// deviations) and Trend (continuous slope) — a change point is a
// sustained shift between two regimes.
//
// Method: best-split mean-shift test. For each candidate split index
// k in [minSide, n-minSide), compute the standardised mean shift
//
//	shift(k) = |mean(left) - mean(right)| / pooled_stddev
//
// where pooled_stddev is sqrt((var_left*(n_l-1) + var_right*(n_r-1)) /
// (n_l+n_r-2)). Emit a signal at the index that maximises shift,
// gated by CHRONOS_CHANGEPOINT_MIN_SHIFT. Strength is the shift
// scaled to [0, 1] by CHRONOS_CHANGEPOINT_MIN_SHIFT (so a shift of
// 2× the threshold gets strength ≈ 0.5 above the floor); Confidence
// scales by sample-size factor.
//
// Evidence is one record per regime ("regime_before", "regime_after")
// carrying mean, stddev, and n.
type ChangePoint struct {
	cfg *config.Config
	now func() time.Time
}

// NewChangePoint wires a ChangePoint detector from configuration.
func NewChangePoint(cfg *config.Config) *ChangePoint {
	return &ChangePoint{cfg: cfg, now: time.Now}
}

// Pattern reports the PatternType this detector emits.
func (c *ChangePoint) Pattern() domain.PatternType { return domain.PatternTypeChangePoint }

// Detect emits at most one change-point signal per series — the best
// split that exceeds the configured shift threshold.
func (c *ChangePoint) Detect(_ context.Context, scopeID uuid.UUID, states []chronos.EntityState) []domain.Signal {
	if c.cfg.ChangePointMinPoints < 4 {
		return nil
	}
	const minSide = 2
	var signals []domain.Signal
	ids, grouped := seriesInOrder(states)
	for _, series := range ids {
		observations := grouped[series]
		if len(observations) < c.cfg.ChangePointMinPoints {
			continue
		}
		ys := outcomes(observations)
		bestK, bestShift := -1, 0.0
		for k := minSide; k <= len(ys)-minSide; k++ {
			s := standardisedMeanShift(ys[:k], ys[k:])
			if math.IsNaN(s) || math.IsInf(s, 0) {
				continue
			}
			if s > bestShift {
				bestShift = s
				bestK = k
			}
		}
		if bestK < 0 || bestShift < c.cfg.ChangePointMinShift {
			continue
		}
		// A shift can clear the standardised threshold on movement far
		// too small to act on, because the denominator is the series'
		// own variability: a metric that sits still varies in the fourth
		// decimal place, so a change in the third divides out to many
		// sigma. When a deployment declares how much movement matters,
		// require that too.
		if c.cfg.ChangePointMinDelta > 0 {
			delta := math.Abs(mean(ys[:bestK]) - mean(ys[bestK:]))
			if delta < c.cfg.ChangePointMinDelta {
				continue
			}
		}
		signals = append(signals, c.build(scopeID, series, observations, ys, bestK, bestShift))
	}
	return signals
}

func (c *ChangePoint) build(scopeID, series uuid.UUID, observations []chronos.EntityState, ys []float64, k int, shift float64) domain.Signal {
	left, right := ys[:k], ys[k:]
	meanLeft, meanRight := mean(left), mean(right)
	stdLeft, stdRight := stddev(left, meanLeft), stddev(right, meanRight)
	strength := clamp01((shift - c.cfg.ChangePointMinShift) / c.cfg.ChangePointMinShift)
	if shift >= 2*c.cfg.ChangePointMinShift {
		strength = clamp01(0.5 + (shift-2*c.cfg.ChangePointMinShift)/(4*c.cfg.ChangePointMinShift))
	}
	confidence := strength * sampleFactor(len(ys), 2*c.cfg.ChangePointMinPoints)
	metrics := map[string]float64{
		"shift":       shift,
		"split_index": float64(k),
		"mean_before": meanLeft,
		"mean_after":  meanRight,
		"delta_mean":  meanRight - meanLeft,
		"n_before":    float64(len(left)),
		"n_after":     float64(len(right)),
	}
	splitTime := observations[k].Timestamp
	return domain.Signal{
		ID:              uuid.New(),
		ScopeID:         scopeID,
		Series:          series,
		Pattern:         domain.PatternTypeChangePoint,
		DetectedAt:      c.now(),
		Window:          domain.TimeWindow{Start: observations[0].Timestamp, End: observations[len(observations)-1].Timestamp},
		Strength:        strength,
		Confidence:      confidence,
		ConfidenceClass: ClassifyConfidence(len(ys), c.cfg.ChangePointMinPoints, c.cfg),
		Metrics:         metrics,
		Explanation:     explainSeries(observations, 0, c.cfg.ChangePointMinShift, detectorVersionChangePoint),
		Evidence: []domain.Evidence{
			{
				Series:  series,
				Time:    observations[k-1].Timestamp,
				Kind:    "regime_before",
				Score:   meanLeft,
				Metrics: map[string]float64{"mean": meanLeft, "stddev": stdLeft, "n": float64(len(left))},
			},
			{
				Series:  series,
				Time:    splitTime,
				Kind:    "regime_after",
				Score:   meanRight,
				Metrics: map[string]float64{"mean": meanRight, "stddev": stdRight, "n": float64(len(right))},
			},
		},
	}
}

// standardisedMeanShift computes |mean(a) - mean(b)| / pooled_stddev.
// Returns NaN when both regimes are constant (pooled_stddev == 0) and
// the means agree, +Inf when pooled_stddev is zero but the means
// differ (a constant-but-shifted regime).
func standardisedMeanShift(a, b []float64) float64 {
	if len(a) < 2 || len(b) < 2 {
		return math.NaN()
	}
	ma, mb := mean(a), mean(b)
	sa := stddev(a, ma)
	sb := stddev(b, mb)
	pooled := math.Sqrt(((sa*sa)*float64(len(a)-1) + (sb*sb)*float64(len(b)-1)) / float64(len(a)+len(b)-2))
	if pooled == 0 {
		if ma == mb {
			return math.NaN()
		}
		return math.Inf(1)
	}
	return math.Abs(ma-mb) / pooled
}
