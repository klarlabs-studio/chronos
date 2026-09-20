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

// Spike detects PatternTypeSpike: a sharp positive deviation of the
// most recent outcome relative to a rolling baseline of the previous
// CHRONOS_SPIKE_WINDOW points. Drop is the symmetric negative case
// (see [Drop]).
//
// Strength is the z-score normalised against a saturation point of 5
// (z=5 → strength 1.0). Confidence equals strength: for spike-style
// detectors the magnitude of the deviation *is* the confidence.
//
// Evidence is a single "baseline_deviation" record carrying z, the
// baseline mean and stddev, and the observed value.
type Spike struct {
	cfg *config.Config
	now func() time.Time
}

// NewSpike wires a Spike detector from configuration.
func NewSpike(cfg *config.Config) *Spike { return &Spike{cfg: cfg, now: time.Now} }

// Pattern reports the PatternType this detector emits.
func (s *Spike) Pattern() domain.PatternType { return domain.PatternTypeSpike }

// Detect scans each series and emits at most one spike per series
// (the most recent point, if it crosses the threshold).
func (s *Spike) Detect(_ context.Context, scopeID uuid.UUID, states []chronos.EntityState) []domain.Signal {
	return zScoreSignal(scopeID, states, s.cfg.SpikeWindow, s.cfg.SpikeZScore, +1, domain.PatternTypeSpike, "baseline_deviation", detectorVersionSpike, s.now, s.cfg)
}

// Drop detects PatternTypeDrop: a sharp negative deviation of the most
// recent outcome from the rolling baseline. See [Spike] for method.
type Drop struct {
	cfg *config.Config
	now func() time.Time
}

// NewDrop wires a Drop detector from configuration.
func NewDrop(cfg *config.Config) *Drop { return &Drop{cfg: cfg, now: time.Now} }

// Pattern reports the PatternType this detector emits.
func (d *Drop) Pattern() domain.PatternType { return domain.PatternTypeDrop }

// Detect scans each series for negative-direction deviations.
func (d *Drop) Detect(_ context.Context, scopeID uuid.UUID, states []chronos.EntityState) []domain.Signal {
	return zScoreSignal(scopeID, states, d.cfg.SpikeWindow, d.cfg.DropZScore, -1, domain.PatternTypeDrop, "baseline_deviation", detectorVersionDrop, d.now, d.cfg)
}

// zScoreSignal is the shared body for Spike and Drop. direction is +1
// to look for positive deviations, -1 for negative.
func zScoreSignal(scopeID uuid.UUID, states []chronos.EntityState, window int, threshold float64, direction int, pattern domain.PatternType, kind, version string, now func() time.Time, cfg *config.Config) []domain.Signal {
	if window < 2 {
		return nil
	}
	var signals []domain.Signal
	ids, grouped := seriesInOrder(states)
	for _, series := range ids {
		observations := grouped[series]
		if len(observations) < window+1 {
			continue
		}
		last := observations[len(observations)-1]
		baselineStates := observations[len(observations)-1-window : len(observations)-1]
		baseline := outcomes(baselineStates)
		m := mean(baseline)
		sd := stddev(baseline, m)
		// A baseline whose sum overflowed to ±Inf has an infinite
		// mean and a NaN spread: there is no scale to measure the
		// latest point against, so there is no deviation to report.
		if sd == 0 || !isFinite(sd) || !isFinite(m) {
			continue
		}
		z := (last.Outcome() - m) / sd
		if !isFinite(z) {
			continue
		}
		if z*float64(direction) < threshold {
			continue
		}
		strength := clamp01(math.Abs(z) / 5.0)
		windowStates := append(append([]chronos.EntityState{}, baselineStates...), last)
		signals = append(signals, domain.Signal{
			ID:              uuid.New(),
			ScopeID:         scopeID,
			Series:          series,
			Pattern:         pattern,
			DetectedAt:      now(),
			Window:          domain.TimeWindow{Start: baselineStates[0].Timestamp, End: last.Timestamp},
			Strength:        strength,
			Confidence:      strength,
			ConfidenceClass: ClassifyConfidence(len(observations), window+1, cfg),
			Explanation:     explainSeries(windowStates, 0, threshold, version),
			Metrics: map[string]float64{
				"z":                z,
				"baseline_mean":    m,
				"baseline_stddev":  sd,
				"observed_outcome": last.Outcome(),
				"window":           float64(window),
			},
			Evidence: []domain.Evidence{{
				Series: series,
				Time:   last.Timestamp,
				Kind:   kind,
				Score:  math.Abs(z),
				Metrics: map[string]float64{
					"z":               z,
					"baseline_mean":   m,
					"baseline_stddev": sd,
				},
			}},
		})
	}
	return keepValid(signals)
}
