package detect

import (
	"context"
	"sort"
	"time"

	"github.com/felixgeelhaar/chronos"
	"github.com/felixgeelhaar/chronos/internal/config"
	"github.com/felixgeelhaar/chronos/internal/domain"
	"github.com/google/uuid"
)

// Seasonality detects PatternTypeSeasonality: a periodic structure in
// the outcome series measured in wall-clock time, not merely in
// sample order.
//
// Ordinal autocorrelation is only interpreted as a temporal period
// when the sampling cadence is regular enough. Inter-observation
// intervals must have a coefficient of variation at or below
// CHRONOS_SEASONALITY_MAX_INTERVAL_CV (0 = perfectly equal spacing).
// Duplicate timestamps, reversed input, and chaotic spacing yield no
// signal — repeating values on an irregular clock are not seasonality.
//
// When the cadence qualifies, autocorrelation is taken at lags
// [SeasonalityMinPeriod, n/2]. The lag with the highest positive
// autocorrelation is the candidate period. Evidence reports that lag
// in samples and in seconds (lag × median sampling interval).
//
// Strength is the autocorrelation. Confidence scales by sample size.
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
		intervalSec, ok := regularCadence(observations, s.cfg.SeasonalityMaxIntervalCV)
		if !ok {
			continue
		}
		lag, r := bestAutocorrelation(ys, s.cfg.SeasonalityMinPeriod, len(ys)/2)
		if r < s.cfg.SeasonalityMinAutocorr {
			continue
		}
		signals = append(signals, s.build(scopeID, series, observations, lag, r, intervalSec))
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

// regularCadence reports the median inter-observation interval in
// seconds when successive intervals are positive and their coefficient
// of variation is at most maxCV. maxCV == 0 accepts only equal spacing.
// Non-positive intervals (duplicates, reversed time) fail closed.
func regularCadence(obs []chronos.EntityState, maxCV float64) (intervalSec float64, ok bool) {
	if len(obs) < 2 {
		return 0, false
	}
	intervals := make([]float64, len(obs)-1)
	for i := 1; i < len(obs); i++ {
		d := obs[i].Timestamp.Sub(obs[i-1].Timestamp).Seconds()
		if d <= 0 || !isFinite(d) {
			return 0, false
		}
		intervals[i-1] = d
	}
	m := mean(intervals)
	if m <= 0 || !isFinite(m) {
		return 0, false
	}
	sd := stddev(intervals, m)
	if !isFinite(sd) || sd/m > maxCV {
		return 0, false
	}
	sorted := append([]float64(nil), intervals...)
	sort.Float64s(sorted)
	mid := len(sorted) / 2
	med := sorted[mid]
	if len(sorted)%2 == 0 {
		med = (sorted[mid-1] + sorted[mid]) / 2
	}
	if med <= 0 || !isFinite(med) {
		return 0, false
	}
	return med, true
}

func (s *Seasonality) build(scopeID, series uuid.UUID, observations []chronos.EntityState, lag int, r, intervalSec float64) domain.Signal {
	n := len(observations)
	strength := clamp01(r)
	confidence := strength * sampleFactor(n, 2*s.cfg.SeasonalityMinPoints)
	metrics := map[string]float64{
		"period":                    float64(lag),
		"period_samples":            float64(lag),
		"period_seconds":            float64(lag) * intervalSec,
		"sampling_interval_seconds": intervalSec,
		"autocorrelation":           r,
		"n":                         float64(n),
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
