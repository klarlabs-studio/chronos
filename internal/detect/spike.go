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
// (z=5 → strength 1.0). It measures the magnitude of the deviation
// and nothing else.
//
// Confidence measures something different and is derived
// independently of that magnitude: the quality of the evidence behind
// the detection. [baselineConfidence] computes it from how much
// history backs the baseline, how quiet that baseline is, and how far
// past the trigger threshold the deviation sits — so a forty-sigma
// jump read off the thinnest window the detector accepts is not
// reported as certain merely for being large.
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
		confidence := baselineConfidence(len(observations), window+1, m, sd, z, threshold, cfg)
		windowStates := append(append([]chronos.EntityState{}, baselineStates...), last)
		signals = append(signals, domain.Signal{
			ID:              uuid.New(),
			ScopeID:         scopeID,
			Series:          series,
			Pattern:         pattern,
			DetectedAt:      now(),
			Window:          domain.TimeWindow{Start: baselineStates[0].Timestamp, End: last.Timestamp},
			Strength:        strength,
			Confidence:      confidence,
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
	return signals
}

const (
	// spikeNoiseDiscount caps how far a noisy baseline can pull
	// confidence down: at worst it halves it.
	spikeNoiseDiscount = 0.5

	// spikeMarginFloor is the multiplier applied to a deviation
	// sitting exactly on the trigger threshold.
	spikeMarginFloor = 0.5

	// spikeMarginSaturation is how far past the threshold, as a
	// fraction of the threshold, |z| must sit before the margin term
	// stops discounting confidence at all.
	spikeMarginSaturation = 0.25

	// spikeSupportSaturationFallback is the MIN_POINTS multiplier the
	// sample-support term saturates at when neither confidence-class
	// threshold is configured. It matches the 2×MIN_POINTS convention
	// the other detectors use in their own confidence terms.
	spikeSupportSaturationFallback = 2.0
)

// baselineConfidence scores how good the evidence behind one spike or
// drop detection is, independently of how large the deviation was.
// The result is the product of three terms, each in (0, 1]:
//
//   - Sample support — n / (STRONG × minPoints), capped at 1, where n
//     is the number of observations the detector saw for the series
//     and minPoints is the detector's floor (window + 1). This is
//     deliberately the same pair [ClassifyConfidence] buckets, and it
//     saturates exactly at the "strong" boundary, so the number and
//     the class cannot contradict each other: confidence can approach
//     1.0 only where the class is "strong", and a "tentative" signal
//     is capped at ESTABLISHED/STRONG — 0.4 with the shipped 2× / 5×
//     defaults. Only the most recent window points enter the baseline
//     arithmetic; the history behind them is what makes that window a
//     representative sample of the series rather than all there is.
//
//   - Baseline quietness — 1 − ½·sd/(|mean| + sd). The same z measured
//     against a baseline whose spread rivals its own level is weaker
//     evidence than one measured against a quiet baseline, because a
//     volatile series throws large excursions unprompted. The discount
//     is capped at a half: noise weakens evidence, it does not erase
//     it, and a detection that cleared the gate is still a detection.
//     A z-score needs a non-zero spread to exist at all, so this term
//     is always strictly below 1 and confidence never attains it.
//
//   - Threshold margin — ½ for a deviation sitting exactly on the
//     trigger threshold, rising to 1.0 once |z| is 25% past it. A
//     detection on the decision boundary is one a fractionally
//     different baseline would not have made at all. The term
//     saturates far below the strength saturation point of z = 5, so
//     for everything except boundary cases confidence is flat in
//     magnitude; that flatness is what keeps it distinct from
//     Strength. A threshold of 0 accepts every crossing, leaving no
//     margin to measure, and the term is then neutral.
//
// Worked values with the shipped defaults (window 5, z ≥ 2.5,
// established 2×, strong 5×): six observations around 1.0 ending in
// 900 — the fewest the detector accepts — give 0.19, a real deviation
// on the thinnest possible history. The same jump after 30
// observations of the same quiet baseline gives 0.97.
//
// The result passes through [finiteConfidence] rather than [clamp01]:
// domain.Signal.Validate rejects confidence outside [0, 1] with
// `< 0 || > 1`, and both comparisons are false for NaN, so a non-real
// confidence would validate cleanly and reach the wire. Every term
// here is finite by construction, and the guard is there because
// "finite by construction" is the assumption that broke this package
// once already (see the 0.17.0 entry on NaN strength and confidence).
func baselineConfidence(n, minPoints int, baselineMean, baselineStdDev, z, threshold float64, cfg *config.Config) float64 {
	if n <= 0 || minPoints <= 0 {
		return 0
	}
	support := clamp01(float64(n) / (supportSaturation(cfg) * float64(minPoints)))

	noise := baselineStdDev / (math.Abs(baselineMean) + baselineStdDev)
	quietness := 1 - spikeNoiseDiscount*clamp01(noise)

	margin := 1.0
	if threshold > 0 {
		exceedance := math.Abs(z)/threshold - 1
		margin = spikeMarginFloor + (1-spikeMarginFloor)*clamp01(exceedance/spikeMarginSaturation)
	}

	return finiteConfidence(support * quietness * margin)
}

// supportSaturation returns the MIN_POINTS multiplier at which the
// sample-support term reaches 1.0. It is the "strong" class threshold
// so that a maximal confidence number and a "strong" class are the
// same claim; when that knob is disabled it falls back to the
// "established" threshold, and when both are disabled — a
// configuration in which every signal is classed "tentative" and the
// class therefore carries no information — to the house 2×MIN_POINTS
// convention.
func supportSaturation(cfg *config.Config) float64 {
	if cfg == nil {
		return spikeSupportSaturationFallback
	}
	if cfg.ConfidenceClassStrong > 0 {
		return cfg.ConfidenceClassStrong
	}
	if cfg.ConfidenceClassEstablished > 0 {
		return cfg.ConfidenceClassEstablished
	}
	return spikeSupportSaturationFallback
}

// finiteConfidence squashes x into [0, 1] like [clamp01] and maps a
// non-real x to 0. clamp01 alone cannot: NaN < 0 and NaN > 1 are both
// false, so it returns NaN unchanged, and domain.Signal.Validate
// applies the same two comparisons and accepts it.
func finiteConfidence(x float64) float64 {
	if !isFinite(x) {
		return 0
	}
	return clamp01(x)
}
