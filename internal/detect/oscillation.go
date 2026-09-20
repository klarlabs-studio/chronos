package detect

import (
	"context"
	"time"

	"github.com/felixgeelhaar/chronos"
	"github.com/felixgeelhaar/chronos/internal/config"
	"github.com/felixgeelhaar/chronos/internal/domain"
	"github.com/google/uuid"
)

// Oscillation detects PatternTypeOscillation: the outcome repeatedly
// reverses direction. Method: among consecutive first-differences that
// are large enough to count as real moves, measure the fraction that
// change sign. Emit when that flip rate clears
// CHRONOS_OSCILLATION_MIN_FLIP_RATE.
//
// Distinct from Seasonality (periodic positive autocorrelation) and
// from Stall (low variance — Stall series have too few meaningful
// differences to accumulate a high flip rate).
//
// Strength is the flip rate. Confidence scales by sample size.
type Oscillation struct {
	cfg *config.Config
	now func() time.Time
}

// NewOscillation wires an Oscillation detector from configuration.
func NewOscillation(cfg *config.Config) *Oscillation {
	return &Oscillation{cfg: cfg, now: time.Now}
}

// Pattern reports the PatternType this detector emits.
func (o *Oscillation) Pattern() domain.PatternType { return domain.PatternTypeOscillation }

// Detect emits one oscillation signal per series whose sign-flip rate
// clears the configured floor.
func (o *Oscillation) Detect(_ context.Context, scopeID uuid.UUID, states []chronos.EntityState) []domain.Signal {
	// Need ≥3 points for two differences, and ≥2 differences to measure
	// a flip rate. Refuse floors that cannot establish evidence.
	if o.cfg.OscillationMinPoints < 4 {
		return nil
	}
	var signals []domain.Signal
	ids, grouped := seriesInOrder(states)
	for _, series := range ids {
		observations := grouped[series]
		if len(observations) < o.cfg.OscillationMinPoints {
			continue
		}
		ys := outcomes(observations)
		flips, pairs, rate := signFlipRate(ys)
		if pairs < 2 || rate < o.cfg.OscillationMinFlipRate {
			continue
		}
		if !isFinite(rate) {
			continue
		}
		signals = append(signals, o.build(scopeID, series, observations, flips, pairs, rate))
	}
	return keepValid(signals)
}

// signFlipRate returns the number of sign changes among consecutive
// meaningful first-differences, the number of consecutive meaningful
// pairs considered, and flips/pairs (0 when pairs == 0).
func signFlipRate(ys []float64) (flips, pairs int, rate float64) {
	if len(ys) < 3 {
		return 0, 0, 0
	}
	var prev float64
	havePrev := false
	for i := 0; i < len(ys)-1; i++ {
		d := ys[i+1] - ys[i]
		if !meaningfulAbsoluteDeviation(ys[i+1], ys[i]) {
			continue
		}
		if !havePrev {
			prev = d
			havePrev = true
			continue
		}
		pairs++
		if prev*d < 0 {
			flips++
		}
		prev = d
	}
	if pairs == 0 {
		return 0, 0, 0
	}
	return flips, pairs, float64(flips) / float64(pairs)
}

func (o *Oscillation) build(scopeID, series uuid.UUID, observations []chronos.EntityState, flips, pairs int, rate float64) domain.Signal {
	n := len(observations)
	strength := clamp01(rate)
	confidence := strength * sampleFactor(n, 2*o.cfg.OscillationMinPoints)
	metrics := map[string]float64{
		"flip_rate": float64(rate),
		"flips":     float64(flips),
		"pairs":     float64(pairs),
		"n":         float64(n),
	}
	return domain.Signal{
		ID:              uuid.New(),
		ScopeID:         scopeID,
		Series:          series,
		Pattern:         domain.PatternTypeOscillation,
		DetectedAt:      o.now(),
		Window:          domain.TimeWindow{Start: observations[0].Timestamp, End: observations[n-1].Timestamp},
		Strength:        strength,
		Confidence:      confidence,
		ConfidenceClass: ClassifyConfidence(n, o.cfg.OscillationMinPoints, o.cfg),
		Metrics:         metrics,
		Explanation:     explainSeries(observations, 0, o.cfg.OscillationMinFlipRate, detectorVersionOscillation),
		Evidence: []domain.Evidence{{
			Series:  series,
			Time:    observations[n-1].Timestamp,
			Kind:    "sign_flip_rate",
			Score:   rate,
			Metrics: metrics,
		}},
	}
}
