package detect

import (
	"context"
	"time"

	"github.com/felixgeelhaar/chronos"
	"github.com/felixgeelhaar/chronos/internal/config"
	"github.com/felixgeelhaar/chronos/internal/domain"
	"github.com/google/uuid"
)

// Seasonality detects PatternTypeSeasonality: a periodic structure in
// the outcome series. Method: autocorrelation at lags
// [SeasonalityMinPeriod, n/2]; the lag with the highest autocorrelation
// is the candidate period. If that peak exceeds
// CHRONOS_SEASONALITY_MIN_AUTOCORR the detector emits.
//
// Strength is the autocorrelation value; Confidence scales by the
// sample-size factor against 2*SeasonalityMinPoints.
//
// Evidence is a single "autocorrelation_peak" record carrying the
// detected period (lag) and the autocorrelation value.
type Seasonality struct {
	cfg *config.Config
	now func() time.Time
}

// NewSeasonality wires a Seasonality detector from configuration.
func NewSeasonality(cfg *config.Config) *Seasonality { return &Seasonality{cfg: cfg, now: time.Now} }

// Pattern reports the PatternType this detector emits.
func (s *Seasonality) Pattern() domain.PatternType { return domain.PatternTypeSeasonality }

// Detect emits one seasonality signal per series whose strongest
// autocorrelation peak passes the threshold.
func (s *Seasonality) Detect(_ context.Context, scopeID uuid.UUID, states []chronos.EntityState) []domain.Signal {
	if s.cfg.SeasonalityMinPoints < 4 {
		return nil
	}
	var signals []domain.Signal
	ids, grouped := seriesInOrder(states)
	for _, series := range ids {
		observations := grouped[series]
		if len(observations) < s.cfg.SeasonalityMinPoints {
			continue
		}
		ys := outcomes(observations)
		lag, r := bestAutocorrelation(ys, s.cfg.SeasonalityMinPeriod, len(ys)/2)
		if r < s.cfg.SeasonalityMinAutocorr {
			continue
		}
		signals = append(signals, s.build(scopeID, series, observations, lag, r))
	}
	return keepValid(signals)
}

// bestAutocorrelation searches for the lag in [minLag, maxLag] with the
// largest *positive* autocorrelation. Negative correlations are not
// seasonality — they are anti-correlation, a different pattern.
func bestAutocorrelation(ys []float64, minLag, maxLag int) (int, float64) {
	if minLag < 1 {
		minLag = 1
	}
	if maxLag >= len(ys) {
		maxLag = len(ys) - 1
	}
	bestLag := 0
	bestR := -2.0
	for lag := minLag; lag <= maxLag; lag++ {
		r := autocorrelation(ys, lag)
		if r > bestR {
			bestR = r
			bestLag = lag
		}
	}
	if bestR < -1 {
		return 0, 0
	}
	return bestLag, bestR
}

func (s *Seasonality) build(scopeID, series uuid.UUID, observations []chronos.EntityState, lag int, r float64) domain.Signal {
	n := len(observations)
	strength := clamp01(r)
	confidence := strength * sampleFactor(n, 2*s.cfg.SeasonalityMinPoints)
	metrics := map[string]float64{
		"period":          float64(lag),
		"autocorrelation": r,
		"n":               float64(n),
	}
	return domain.Signal{
		ID:              uuid.New(),
		ScopeID:         scopeID,
		Series:          series,
		Pattern:         domain.PatternTypeSeasonality,
		DetectedAt:      s.now(),
		Window:          domain.TimeWindow{Start: observations[0].Timestamp, End: observations[n-1].Timestamp},
		Strength:        strength,
		Confidence:      confidence,
		ConfidenceClass: ClassifyConfidence(n, s.cfg.SeasonalityMinPoints, s.cfg),
		Metrics:         metrics,
		Explanation:     explainSeries(observations, 0, s.cfg.SeasonalityMinAutocorr, detectorVersionSeasonality),
		Evidence: []domain.Evidence{{
			Series:  series,
			Time:    observations[n-1].Timestamp,
			Kind:    "autocorrelation_peak",
			Score:   r,
			Metrics: metrics,
		}},
	}
}
