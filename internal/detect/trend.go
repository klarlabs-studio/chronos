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

// Trend detects PatternTypeTrend: a sustained directional movement of
// the outcome metric over the analysis window.
//
// Method: ordinary-least-squares linear regression of outcome against
// ordinal index (0..n-1). The detector emits when |slope| exceeds
// CHRONOS_TREND_MIN_SLOPE *and* the regression's R² is meaningful.
// Strength is R² (how cleanly the data is a line); Confidence is R²
// scaled by a sample-size factor.
//
// Evidence: a single "regression_summary" record carrying slope, R²,
// intercept, and n.
type Trend struct {
	cfg *config.Config
	now func() time.Time
}

// NewTrend wires a Trend detector from configuration.
func NewTrend(cfg *config.Config) *Trend { return &Trend{cfg: cfg, now: time.Now} }

// Pattern reports the PatternType this detector emits.
func (t *Trend) Pattern() domain.PatternType { return domain.PatternTypeTrend }

// Detect emits one trend signal per series whose outcome regression
// passes the slope and fit thresholds.
func (t *Trend) Detect(_ context.Context, scopeID uuid.UUID, states []chronos.EntityState) []domain.Signal {
	if t.cfg.TrendMinPoints < 2 {
		return nil
	}
	var signals []domain.Signal
	ids, grouped := seriesInOrder(states)
	for _, series := range ids {
		observations := grouped[series]
		if len(observations) < t.cfg.TrendMinPoints {
			continue
		}
		ys := outcomes(observations)
		xs := make([]float64, len(ys))
		for i := range ys {
			xs[i] = float64(i)
		}
		slope, intercept, r2 := linearRegression(xs, ys)
		if math.Abs(slope) < t.cfg.TrendMinSlope {
			continue
		}
		// Require some structural fit — a high-slope but noisy series
		// shouldn't be called a trend.
		if r2 < 0.3 {
			continue
		}
		signals = append(signals, t.build(scopeID, series, observations, slope, intercept, r2))
	}
	return signals
}

func (t *Trend) build(scopeID, series uuid.UUID, observations []chronos.EntityState, slope, intercept, r2 float64) domain.Signal {
	n := len(observations)
	strength := clamp01(r2)
	confidence := strength * sampleFactor(n, 2*t.cfg.TrendMinPoints)
	metrics := map[string]float64{
		"slope":     slope,
		"intercept": intercept,
		"r2":        r2,
		"n":         float64(n),
	}
	return domain.Signal{
		ID:              uuid.New(),
		ScopeID:         scopeID,
		Series:          series,
		Pattern:         domain.PatternTypeTrend,
		DetectedAt:      t.now(),
		Window:          domain.TimeWindow{Start: observations[0].Timestamp, End: observations[n-1].Timestamp},
		Strength:        strength,
		Confidence:      confidence,
		ConfidenceClass: ClassifyConfidence(n, t.cfg.TrendMinPoints, t.cfg),
		Metrics:         metrics,
		Explanation:     explainSeries(observations, 0, t.cfg.TrendMinSlope, detectorVersionTrend),
		Evidence: []domain.Evidence{{
			Series:  series,
			Time:    observations[n-1].Timestamp,
			Kind:    "regression_summary",
			Score:   r2,
			Metrics: metrics,
		}},
	}
}
