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

// Stall detects PatternTypeStall: the outcome metric shows little to
// no relative variation over the analysis window.
//
// Method: outcomes are normalised by their first value (or, when the
// first value is zero, by the mean) so the stddev is scale-invariant.
// A signal is emitted when the normalised stddev is below
// CHRONOS_STALL_MAX_STDDEV and the series has at least
// CHRONOS_STALL_MIN_POINTS observations. Strength reflects flatness
// (1.0 = perfectly flat, 0.0 = at-threshold); Confidence scales
// flatness by a sample-size factor.
//
// Evidence is a single "variance_window" record carrying the
// normalised stddev, mean, and n.
type Stall struct {
	cfg *config.Config
	now func() time.Time
}

// NewStall wires a Stall detector from configuration.
func NewStall(cfg *config.Config) *Stall { return &Stall{cfg: cfg, now: time.Now} }

// Pattern reports the PatternType this detector emits.
func (s *Stall) Pattern() domain.PatternType { return domain.PatternTypeStall }

// Detect emits one stall signal per series whose normalised stddev
// passes below the threshold.
func (s *Stall) Detect(_ context.Context, scopeID uuid.UUID, states []chronos.EntityState) []domain.Signal {
	if s.cfg.StallMinPoints < 2 {
		return nil
	}
	var signals []domain.Signal
	ids, grouped := seriesInOrder(states)
	for _, series := range ids {
		observations := grouped[series]
		if len(observations) < s.cfg.StallMinPoints {
			continue
		}
		ys := outcomes(observations)
		nm := normalisedStddev(ys)
		if math.IsNaN(nm) {
			continue
		}
		if nm >= s.cfg.StallMaxStdDev {
			continue
		}
		signals = append(signals, s.build(scopeID, series, observations, ys, nm))
	}
	return signals
}

func (s *Stall) build(scopeID, series uuid.UUID, observations []chronos.EntityState, ys []float64, normStddev float64) domain.Signal {
	n := len(observations)
	flatness := clamp01(1 - normStddev/s.cfg.StallMaxStdDev)
	confidence := flatness * sampleFactor(n, 2*s.cfg.StallMinPoints)
	metrics := map[string]float64{
		"normalised_stddev": normStddev,
		"mean":              mean(ys),
		"n":                 float64(n),
	}
	return domain.Signal{
		ID:              uuid.New(),
		ScopeID:         scopeID,
		Series:          series,
		Pattern:         domain.PatternTypeStall,
		DetectedAt:      s.now(),
		Window:          domain.TimeWindow{Start: observations[0].Timestamp, End: observations[n-1].Timestamp},
		Strength:        flatness,
		Confidence:      confidence,
		ConfidenceClass: ClassifyConfidence(n, s.cfg.StallMinPoints, s.cfg),
		Metrics:         metrics,
		Explanation:     explainSeries(observations, 0, s.cfg.StallMaxStdDev, detectorVersionStall),
		Evidence: []domain.Evidence{{
			Series:  series,
			Time:    observations[n-1].Timestamp,
			Kind:    "variance_window",
			Score:   normStddev,
			Metrics: metrics,
		}},
	}
}

// normalisedStddev divides each outcome by a non-zero baseline (the
// first value, or the mean if the first is zero) so the resulting
// stddev is scale-invariant. NaN is returned when no usable baseline
// exists (all values zero).
func normalisedStddev(ys []float64) float64 {
	if len(ys) == 0 {
		return math.NaN()
	}
	base := ys[0]
	if base == 0 {
		base = mean(ys)
		if base == 0 {
			return math.NaN()
		}
	}
	norm := make([]float64, len(ys))
	for i, v := range ys {
		norm[i] = v / base
	}
	return stddev(norm, mean(norm))
}
