package detect

import (
	"context"
	"encoding/json"
	"math"
	"testing"
	"time"

	"github.com/felixgeelhaar/chronos"
	"github.com/felixgeelhaar/chronos/internal/config"
	"github.com/felixgeelhaar/chronos/internal/domain"
	"github.com/google/uuid"
)

// This file pins detector behaviour under adversarial numerical
// conditions: zero variance, constant series, extreme magnitudes,
// denormals, sample counts at and below each detector's documented
// minimum, degenerate feature vectors, duplicate timestamps,
// out-of-order observations, irregular sampling and sparse series.
//
// The contract under test is not "the detector fires". It is that the
// detector either stays silent or emits a signal whose Strength and
// Confidence are real numbers in [0, 1] and which satisfies
// domain.Signal.Validate — the guarantee the Detector interface makes
// to the engine. Remaining TestFinding_* cases document deliberate
// product choices (ChangePoint and Stall can both fire on one series;
// detectors require chronological input) rather than bugs.
//
// Inputs are assumed finite: NaN/Inf rejection at the EntityState
// boundary is covered separately. Every fixture carries a non-zero
// Timestamp for the same reason.

const (
	// advHuge is the largest finite float64. It is a legal input, but
	// squaring or summing a handful of them overflows to +Inf.
	advHuge = math.MaxFloat64
	// advDenormal is the smallest positive subnormal float64.
	advDenormal = 5e-324
	// advEpsilonStep is a relative step at the edge of float64
	// resolution for values near 1.0.
	advEpsilonStep = 1e-15
)

// advBase is a fixed, non-zero observation time. Fixed rather than
// time.Now() so failures are reproducible.
var advBase = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// advCfg mirrors the values config.Default() ships, written out so a
// test reads without cross-referencing the config package and does
// not pick up CHRONOS_* environment overrides from the host.
func advCfg() *config.Config {
	return &config.Config{
		MaxSignalsPerRun:           100,
		SimilarityThreshold:        0.85,
		MinSampleSize:              2,
		TrendMinSlope:              0.05,
		TrendMinPoints:             4,
		SpikeZScore:                2.5,
		DropZScore:                 2.5,
		SpikeWindow:                5,
		StallMaxStdDev:             0.05,
		StallMinPoints:             4,
		AnomalyMaxSimilarity:       0.5,
		AnomalyMinPeers:            2,
		SeasonalityMinAutocorr:     0.5,
		SeasonalityMinPoints:       12,
		SeasonalityMinPeriod:       2,
		CorrelationMin:             0.7,
		CorrelationMinPoints:       5,
		ChangePointMinShift:        1.5,
		ChangePointMinPoints:       8,
		ChangePointMinDelta:        0,
		OutlierClusterMinSeries:    3,
		OutlierClusterZ:            2.5,
		OutlierClusterTimeWindow:   5 * time.Minute,
		CrossScopeMin:              0.8,
		CrossScopeMinPoints:        5,
		OscillationMinFlipRate:     0.55,
		OscillationMinPoints:       6,
		SeasonalityMaxIntervalCV:   0,
		AlignTolerance:             0,
		DivergenceMinSlope:         0.05,
		DivergenceMinPoints:        5,
		DivergenceMinR2:            0.5,
		ConvergenceMinSlope:        0.05,
		ConvergenceMinPoints:       5,
		ConvergenceMinR2:           0.5,
		ConfidenceClassEstablished: 2.0,
		ConfidenceClassStrong:      5.0,
	}
}

// advSeries builds one entity's observations at a fixed cadence. A
// negative step produces descending timestamps, which is how the
// out-of-order cases are constructed.
func advSeries(scope, entity uuid.UUID, step time.Duration, ys []float64) []chronos.EntityState {
	out := make([]chronos.EntityState, len(ys))
	for i, y := range ys {
		out[i] = chronos.EntityState{
			ID:        uuid.New(),
			EntityID:  entity,
			ScopeID:   scope,
			Timestamp: advBase.Add(time.Duration(i) * step),
			Features:  []float64{1, 2, y},
		}
	}
	return out
}

// advSeriesAt builds one entity's observations at explicit offsets so
// irregular and sparse sampling can be expressed directly.
func advSeriesAt(scope, entity uuid.UUID, offsets []time.Duration, ys []float64) []chronos.EntityState {
	out := make([]chronos.EntityState, len(ys))
	for i, y := range ys {
		out[i] = chronos.EntityState{
			ID:        uuid.New(),
			EntityID:  entity,
			ScopeID:   scope,
			Timestamp: advBase.Add(offsets[i]),
			Features:  []float64{1, 2, y},
		}
	}
	return out
}

// advConst returns n copies of v.
func advConst(v float64, n int) []float64 {
	out := make([]float64, n)
	for i := range out {
		out[i] = v
	}
	return out
}

// advRamp returns n values starting at start and rising by step.
func advRamp(n int, start, step float64) []float64 {
	out := make([]float64, n)
	for i := range out {
		out[i] = start + float64(i)*step
	}
	return out
}

// advIrregularOffsets returns n offsets whose gaps span seven orders
// of magnitude, from one second to four hundred hours.
func advIrregularOffsets(n int) []time.Duration {
	gaps := []time.Duration{time.Second, 3 * time.Second, 17 * time.Millisecond, 9 * time.Hour, 400 * time.Hour, time.Second, 2 * time.Minute}
	out := make([]time.Duration, n)
	var acc time.Duration
	for i := range out {
		out[i] = acc
		acc += gaps[i%len(gaps)]
	}
	return out
}

// advAssertSane is the shared contract check: every emitted signal
// must carry a real Strength and Confidence inside [0, 1] and must
// satisfy domain.Signal.Validate, which is what the Detector
// interface promises the engine.
//
// The explicit finiteness and range checks are kept alongside the
// Validate call rather than folded into it. They say what this file
// means by a sane signal in the file that asserts it, and they fail
// naming the field and its value, which a sentinel error does not.
func advAssertSane(t *testing.T, sigs []domain.Signal) {
	t.Helper()
	for i, s := range sigs {
		if !isFinite(s.Strength) {
			t.Errorf("signal %d: Strength = %v, want a real number", i, s.Strength)
		}
		if !isFinite(s.Confidence) {
			t.Errorf("signal %d: Confidence = %v, want a real number", i, s.Confidence)
		}
		if s.Strength < 0 || s.Strength > 1 {
			t.Errorf("signal %d: Strength = %v, want [0,1]", i, s.Strength)
		}
		if s.Confidence < 0 || s.Confidence > 1 {
			t.Errorf("signal %d: Confidence = %v, want [0,1]", i, s.Confidence)
		}
		if err := s.Validate(); err != nil {
			t.Errorf("signal %d: Validate() = %v, want nil", i, err)
		}
	}
}

// advCase is one adversarial input for a single-series detector.
type advCase struct {
	name string
	// ys are the outcome values, oldest first.
	ys []float64
	// step is the cadence; zero means every observation shares one
	// timestamp, negative means descending.
	step time.Duration
	// offsets, when set, overrides step with explicit per-observation
	// offsets.
	offsets []time.Duration
	// wantSignals is the exact number of signals expected. -1 means
	// "any count, but every signal must be sane".
	wantSignals int
	// why records what the case is probing.
	why string
}

// advSingleSeriesCases are shared across every per-series detector.
// The expectation for all of them is "no signal": none of these
// inputs carries enough evidence for any detector to speak, and the
// ones that do carry numeric extremes must not manufacture a value.
func advSingleSeriesCases() []advCase {
	return []advCase{
		{name: "empty", ys: nil, step: time.Minute, wantSignals: 0,
			why: "no observations at all"},
		{name: "single observation", ys: []float64{42}, step: time.Minute, wantSignals: 0,
			why: "one point cannot establish any pattern"},
		{name: "two observations", ys: []float64{1, 1000}, step: time.Minute, wantSignals: 0,
			why: "below every detector's documented minimum"},
		{name: "all zeros", ys: advConst(0, 16), step: time.Minute, wantSignals: 0,
			why: "zero variance around a zero baseline; nothing to normalise by"},
		{name: "huge constant", ys: advConst(advHuge, 16), step: time.Minute, wantSignals: -1,
			why: "MaxFloat64 constants: the running sums overflow to +Inf"},
		{name: "huge alternating", ys: []float64{advHuge, -advHuge, advHuge, -advHuge, advHuge, -advHuge, advHuge, -advHuge, advHuge, -advHuge, advHuge, -advHuge, advHuge, -advHuge, advHuge, -advHuge}, step: time.Minute, wantSignals: -1,
			why: "+/-MaxFloat64: sums cancel to NaN rather than overflowing cleanly"},
		{name: "huge descending ramp", ys: []float64{advHuge, advHuge / 2, advHuge / 3, advHuge / 4, advHuge / 5, advHuge / 6, advHuge / 7, advHuge / 8, advHuge / 9, advHuge / 10, advHuge / 11, advHuge / 12, advHuge / 13, advHuge / 14, advHuge / 15, advHuge / 16}, step: time.Minute, wantSignals: -1,
			why: "a clean shape at a magnitude float64 cannot sum"},
		{name: "denormal ramp", ys: []float64{advDenormal, 2 * advDenormal, 3 * advDenormal, 4 * advDenormal, 5 * advDenormal, 6 * advDenormal, 7 * advDenormal, 8 * advDenormal, 9 * advDenormal, 10 * advDenormal, 11 * advDenormal, 12 * advDenormal, 13 * advDenormal, 14 * advDenormal, 15 * advDenormal, 16 * advDenormal}, step: time.Minute, wantSignals: -1,
			why: "subnormal magnitudes: every derived statistic underflows"},
		{name: "duplicate timestamps", ys: advRamp(16, 1, 1), step: 0, wantSignals: -1,
			why: "sixteen observations sharing one instant collapse the window to a point"},
		{name: "irregular intervals", ys: advRamp(16, 1, 1), offsets: advIrregularOffsets(16), wantSignals: -1,
			why: "gaps spanning seconds to weeks; detectors index by ordinal, not by time"},
		{name: "sparse", ys: []float64{1, 1000, 2, 900}, offsets: []time.Duration{0, 24 * time.Hour, 60 * 24 * time.Hour, 400 * 24 * time.Hour}, wantSignals: -1,
			why: "four observations spread over more than a year"},
	}
}

func (c advCase) states(scope, entity uuid.UUID) []chronos.EntityState {
	if c.offsets != nil {
		return advSeriesAt(scope, entity, c.offsets, c.ys)
	}
	return advSeries(scope, entity, c.step, c.ys)
}

// advRunSingleSeries drives one detector across the shared case table.
func advRunSingleSeries(t *testing.T, name string, newDet func(*config.Config) Detector) {
	t.Helper()
	for _, c := range advSingleSeriesCases() {
		t.Run(name+"/"+c.name, func(t *testing.T) {
			d := newDet(advCfg())
			scope, entity := uuid.New(), uuid.New()
			got := d.Detect(context.Background(), scope, c.states(scope, entity))
			if c.wantSignals >= 0 && len(got) != c.wantSignals {
				t.Fatalf("%s: got %d signals, want %d (%s)", c.name, len(got), c.wantSignals, c.why)
			}
			advAssertSane(t, got)
		})
	}
}

func TestAdversarial_EveryPerSeriesDetector(t *testing.T) {
	detectors := map[string]func(*config.Config) Detector{
		"trend":           func(c *config.Config) Detector { return NewTrend(c) },
		"spike":           func(c *config.Config) Detector { return NewSpike(c) },
		"drop":            func(c *config.Config) Detector { return NewDrop(c) },
		"stall":           func(c *config.Config) Detector { return NewStall(c) },
		"seasonality":     func(c *config.Config) Detector { return NewSeasonality(c) },
		"change_point":    func(c *config.Config) Detector { return NewChangePoint(c) },
		"correlation":     func(c *config.Config) Detector { return NewCorrelation(c) },
		"recurrence":      func(c *config.Config) Detector { return NewRecurrence(c) },
		"anomaly":         func(c *config.Config) Detector { return NewAnomaly(c) },
		"outlier_cluster": func(c *config.Config) Detector { return NewOutlierCluster(c) },
		"oscillation":     func(c *config.Config) Detector { return NewOscillation(c) },
		"divergence":      func(c *config.Config) Detector { return NewDivergence(c) },
		"convergence":     func(c *config.Config) Detector { return NewConvergence(c) },
	}
	for name, ctor := range detectors {
		advRunSingleSeries(t, name, ctor)
	}
}

// --- Trend -----------------------------------------------------------------

func TestTrend_Adversarial(t *testing.T) {
	scope := uuid.New()
	tests := []struct {
		name string
		ys   []float64
		want int
		why  string
	}{
		{"constant series", advConst(7, 12), 0,
			"zero variance: the regression has no slope and R2 is undefined"},
		{"one below minimum", advRamp(3, 1, 1), 0,
			"TrendMinPoints is 4"},
		{"exactly at minimum", advRamp(4, 1, 1), 1,
			"four points is the documented floor and must emit"},
		{"huge magnitudes", advConst(advHuge, 12), 0,
			"sums overflow to +Inf; R2 becomes NaN and NaN passes every < gate"},
		{"huge linear ramp", []float64{advHuge / 12, advHuge / 11, advHuge / 10, advHuge / 9, advHuge / 8, advHuge / 7, advHuge / 6, advHuge / 5, advHuge / 4, advHuge / 3, advHuge / 2, advHuge}, 0,
			"a rising shape whose sums cannot be represented is an undefined fit"},
		{"negative huge magnitudes", advConst(-advHuge, 12), 0,
			"same overflow in the negative direction"},
		{"denormal ramp", []float64{advDenormal, 2 * advDenormal, 3 * advDenormal, 4 * advDenormal, 5 * advDenormal, 6 * advDenormal}, 0,
			"a perfect line whose wall-clock slope is far below TrendMinSlope 0.05"},
		{"slope exactly at threshold", advRamp(8, 0, 0.05), 1,
			"|slope| == TrendMinSlope (outcome units per hour) is not below it, so it emits"},
		{"slope just under threshold", advRamp(8, 0, 0.049), 0,
			"|slope| below TrendMinSlope stays silent"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := NewTrend(advCfg())
			// Hourly spacing so wall-clock slope (per hour) matches the
			// per-step dy in advRamp for threshold cases.
			got := d.Detect(context.Background(), scope, advSeries(scope, uuid.New(), time.Hour, tc.ys))
			if len(got) != tc.want {
				t.Fatalf("got %d signals, want %d (%s)", len(got), tc.want, tc.why)
			}
			advAssertSane(t, got)
		})
	}
}

func TestTrend_ExactMinimumSampleReportsLowerConfidenceThanStrength(t *testing.T) {
	// Strength and Confidence must stay distinct: at the documented
	// minimum the shape is perfect (R2 = 1, strength 1) but the
	// evidence is thin, so Confidence must be strictly lower.
	d := NewTrend(advCfg())
	scope := uuid.New()
	got := d.Detect(context.Background(), scope, advSeries(scope, uuid.New(), time.Minute, advRamp(4, 1, 1)))
	if len(got) != 1 {
		t.Fatalf("got %d signals, want 1", len(got))
	}
	s := got[0]
	if s.Strength != 1 {
		t.Errorf("Strength = %v, want 1 for a perfect line", s.Strength)
	}
	if s.Confidence >= s.Strength {
		t.Errorf("Confidence = %v, Strength = %v: at the minimum sample count confidence must be strictly lower", s.Confidence, s.Strength)
	}
	if s.ConfidenceClass != domain.ConfidenceClassTentative {
		t.Errorf("ConfidenceClass = %q, want %q at n == MinPoints", s.ConfidenceClass, domain.ConfidenceClassTentative)
	}
}

// --- Spike / Drop ----------------------------------------------------------

// TestSpike_ExactMinimumSampleReportsLowerConfidenceThanStrength is
// the Spike counterpart of the Trend case above, and replaces the
// finding that used to sit in the section at the bottom of this file:
// Spike and Drop set Confidence = Strength, so six observations — the
// fewest the detector accepts — reported Confidence 1.0 while the
// class alongside it said "tentative". The number now derives from the
// evidence rather than the magnitude, and the two agree.
func TestSpike_ExactMinimumSampleReportsLowerConfidenceThanStrength(t *testing.T) {
	scope := uuid.New()
	d := NewSpike(advCfg())
	// Six observations: five of baseline plus the point under test.
	got := d.Detect(context.Background(), scope, advSeries(scope, uuid.New(), time.Minute, []float64{1, 1.1, 0.9, 1, 1.05, 900}))
	if len(got) != 1 {
		t.Fatalf("got %d signals, want 1", len(got))
	}
	s := got[0]
	if s.Strength != 1 {
		t.Errorf("Strength = %v, want 1 for a deviation past the saturation point", s.Strength)
	}
	if s.Confidence >= s.Strength {
		t.Errorf("Confidence = %v, Strength = %v: at the minimum sample count confidence must be strictly lower", s.Confidence, s.Strength)
	}
	if s.ConfidenceClass != domain.ConfidenceClassTentative {
		t.Errorf("ConfidenceClass = %q, want %q at n == window+1", s.ConfidenceClass, domain.ConfidenceClassTentative)
	}
	// A tentative class caps support at ESTABLISHED/STRONG, so the
	// number cannot claim more certainty than the class does.
	if ceiling := advCfg().ConfidenceClassEstablished / advCfg().ConfidenceClassStrong; s.Confidence >= ceiling {
		t.Errorf("Confidence = %v, want below the %v a tentative class permits", s.Confidence, ceiling)
	}
	advAssertSane(t, got)
}

func TestSpikeDrop_Adversarial(t *testing.T) {
	scope := uuid.New()
	// SpikeWindow is 5, so a signal needs at least six observations.
	tests := []struct {
		name      string
		ys        []float64
		wantSpike int
		wantDrop  int
		why       string
	}{
		{"constant baseline, constant last", advConst(5, 12), 0, 0,
			"zero-variance baseline: no scale to measure a deviation against"},
		{"constant baseline, different last", append(advConst(5, 11), 900), 0, 0,
			"stddev of a constant baseline is 0, so the z-score is undefined; no signal rather than an infinite one"},
		{"one below minimum", []float64{1, 1.1, 0.9, 1, 1.05}, 0, 0,
			"five observations with SpikeWindow 5 leaves no point outside the baseline"},
		{"exactly at minimum", []float64{1, 1.1, 0.9, 1, 1.05, 90}, 1, 0,
			"six observations is the floor: five of baseline plus the point under test"},
		{"huge constant baseline", advConst(advHuge, 12), 0, 0,
			"baseline mean overflows to +Inf and its stddev is NaN"},
		{"huge jump off a unit baseline", []float64{1, 1.1, 0.9, 1, 1.05, advHuge}, 0, 0,
			"z overflows to +Inf; see TestFinding_SpikeDropSilentOnOverflowingJump"},
		{"denormal baseline with denormal jump", []float64{advDenormal, advDenormal, advDenormal, advDenormal, advDenormal, 1000 * advDenormal}, 0, 0,
			"a constant subnormal baseline has zero stddev like any other constant"},
		{"symmetric negative deviation", []float64{1, 1.1, 0.9, 1, 1.05, -90}, 0, 1,
			"drop is the mirror of spike and must not fire as a spike"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			states := advSeries(scope, uuid.New(), time.Minute, tc.ys)
			gotSpike := NewSpike(advCfg()).Detect(context.Background(), scope, states)
			gotDrop := NewDrop(advCfg()).Detect(context.Background(), scope, states)
			if len(gotSpike) != tc.wantSpike {
				t.Errorf("spike: got %d, want %d (%s)", len(gotSpike), tc.wantSpike, tc.why)
			}
			if len(gotDrop) != tc.wantDrop {
				t.Errorf("drop: got %d, want %d (%s)", len(gotDrop), tc.wantDrop, tc.why)
			}
			advAssertSane(t, gotSpike)
			advAssertSane(t, gotDrop)
		})
	}
}

// --- Stall -----------------------------------------------------------------

func TestStall_Adversarial(t *testing.T) {
	scope := uuid.New()
	tests := []struct {
		name string
		ys   []float64
		want int
		why  string
	}{
		{"constant series", advConst(7, 12), 1,
			"a perfectly flat series is exactly what stall detects"},
		{"all zeros", advConst(0, 12), 0,
			"no non-zero baseline to normalise by; normalisedStddev is NaN"},
		{"one below minimum", advConst(7, 3), 0,
			"StallMinPoints is 4"},
		{"exactly at minimum", advConst(7, 4), 1,
			"four points is the documented floor"},
		{"huge constant", advConst(advHuge, 12), 0,
			"the normalised series is exactly flat, but metrics[mean] is the mean of the " +
				"raw values, which overflows to +Inf; a signal whose metrics cannot be " +
				"represented is not emitted"},
		{"denormal constant", advConst(advDenormal, 12), 1,
			"subnormal values normalise to 1.0 as cleanly as any other constant"},
		{"denormal baseline, unit rest", append([]float64{advDenormal}, advConst(1, 11)...), 0,
			"dividing 1 by 5e-324 overflows the normalised series; the spread is not below threshold"},
		{"one-ulp variation", []float64{1, 1 + advEpsilonStep, 1, 1 + advEpsilonStep, 1, 1 + advEpsilonStep, 1, 1 + advEpsilonStep}, 1,
			"variation at the resolution limit of float64 is flat"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := NewStall(advCfg())
			got := d.Detect(context.Background(), scope, advSeries(scope, uuid.New(), time.Minute, tc.ys))
			if len(got) != tc.want {
				t.Fatalf("got %d signals, want %d (%s)", len(got), tc.want, tc.why)
			}
			advAssertSane(t, got)
		})
	}
}

// --- ChangePoint -----------------------------------------------------------

func TestChangePoint_Adversarial(t *testing.T) {
	scope := uuid.New()
	tests := []struct {
		name string
		ys   []float64
		want int
		why  string
	}{
		{"constant series", advConst(7, 12), 0,
			"both regimes constant and equal: pooled stddev 0 and means agree, so the shift is NaN"},
		{"one below minimum", advConst(1, 3), 0,
			"ChangePointMinPoints is 8"},
		{"exactly at minimum, clean step", []float64{1, 1.01, 0.99, 1, 9, 9.01, 8.99, 9}, 1,
			"eight points is the documented floor"},
		{"perfect step, zero variance either side", []float64{1, 1, 1, 1, 9, 9, 9, 9}, 1,
			"clean constant regimes produce an Inf shift ranked as maximum evidence"},
		{"huge constant", advConst(advHuge, 12), 0,
			"no shift is computable when every regime statistic overflows"},
		{"huge step", append(advConst(1, 6), advConst(advHuge, 6)...), 0,
			"every candidate split puts MaxFloat64 values on one side, whose mean and spread overflow; no split yields a real shift"},
		{"denormal step", append(advConst(advDenormal, 6), advConst(2*advDenormal, 6)...), 1,
			"subnormal constant regimes still form a clean step; Inf shift is accepted as maximum evidence"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := NewChangePoint(advCfg())
			got := d.Detect(context.Background(), scope, advSeries(scope, uuid.New(), time.Minute, tc.ys))
			if len(got) != tc.want {
				t.Fatalf("got %d signals, want %d (%s)", len(got), tc.want, tc.why)
			}
			advAssertSane(t, got)
		})
	}
}

// --- Seasonality -----------------------------------------------------------

func TestSeasonality_Adversarial(t *testing.T) {
	scope := uuid.New()
	tests := []struct {
		name string
		ys   []float64
		want int
		why  string
	}{
		{"constant series", advConst(7, 24), 0,
			"autocorrelation of a zero-variance series is undefined and reported as 0"},
		{"one below minimum", advConst(1, 11), 0,
			"SeasonalityMinPoints is 12"},
		{"exactly at minimum, period 2", []float64{1, 5, 1, 5, 1, 5, 1, 5, 1, 5, 1, 5}, 1,
			"twelve points is the documented floor"},
		{"huge alternating", []float64{advHuge, -advHuge, advHuge, -advHuge, advHuge, -advHuge, advHuge, -advHuge, advHuge, -advHuge, advHuge, -advHuge}, 0,
			"a period-2 shape at MaxFloat64: the correlation sums are not representable"},
		{"denormal periodic", []float64{advDenormal, 5 * advDenormal, advDenormal, 5 * advDenormal, advDenormal, 5 * advDenormal, advDenormal, 5 * advDenormal, advDenormal, 5 * advDenormal, advDenormal, 5 * advDenormal}, 0,
			"subnormal products underflow to zero, leaving no measurable variance"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := NewSeasonality(advCfg())
			got := d.Detect(context.Background(), scope, advSeries(scope, uuid.New(), time.Minute, tc.ys))
			if len(got) != tc.want {
				t.Fatalf("got %d signals, want %d (%s)", len(got), tc.want, tc.why)
			}
			advAssertSane(t, got)
		})
	}
}

// --- Correlation -----------------------------------------------------------

func TestCorrelation_Adversarial(t *testing.T) {
	scope := uuid.New()
	tests := []struct {
		name string
		a, b []float64
		want int
		why  string
	}{
		{"both constant", advConst(5, 8), advConst(9, 8), 0,
			"zero variance on both sides: the coefficient is undefined, not 1.0"},
		{"one constant", advRamp(8, 1, 1), advConst(9, 8), 0,
			"one zero-variance side is enough to make r undefined"},
		{"identical series", advRamp(8, 1, 1), advRamp(8, 1, 1), 1,
			"r = 1 for two identical non-constant series"},
		{"one below minimum", advRamp(4, 1, 1), advRamp(4, 1, 1), 0,
			"CorrelationMinPoints is 5"},
		{"exactly at minimum", advRamp(5, 1, 1), advRamp(5, 1, 1), 1,
			"five aligned observations is the documented floor"},
		{"both huge", advConst(advHuge, 8), advConst(advHuge, 8), 0,
			"MaxFloat64 sums overflow; the coefficient is NaN, which passes |r| < min"},
		{"huge ramps", []float64{advHuge, advHuge / 2, advHuge / 3, advHuge / 4, advHuge / 5, advHuge / 6, advHuge / 7, advHuge / 8}, []float64{advHuge, advHuge / 2, advHuge / 3, advHuge / 4, advHuge / 5, advHuge / 6, advHuge / 7, advHuge / 8}, 0,
			"two identically shaped series at an unrepresentable magnitude"},
		{"both denormal ramps", []float64{advDenormal, 2 * advDenormal, 3 * advDenormal, 4 * advDenormal, 5 * advDenormal, 6 * advDenormal, 7 * advDenormal, 8 * advDenormal}, []float64{advDenormal, 2 * advDenormal, 3 * advDenormal, 4 * advDenormal, 5 * advDenormal, 6 * advDenormal, 7 * advDenormal, 8 * advDenormal}, 0,
			"subnormal products underflow to zero variance"},
		{"unequal lengths share a timestamp prefix", advRamp(20, 1, 1), advRamp(5, 1, 1), 1,
			"exact alignment keeps the five shared timestamps, not a tail slice"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := NewCorrelation(advCfg())
			ea, eb := uuid.New(), uuid.New()
			states := append(advSeries(scope, ea, time.Minute, tc.a), advSeries(scope, eb, time.Minute, tc.b)...)
			got := d.Detect(context.Background(), scope, states)
			if len(got) != tc.want {
				t.Fatalf("got %d signals, want %d (%s)", len(got), tc.want, tc.why)
			}
			advAssertSane(t, got)
		})
	}
}

// --- Oscillation -----------------------------------------------------------

func TestOscillation_Adversarial(t *testing.T) {
	scope := uuid.New()
	zigzag := []float64{1, 5, 1, 5, 1, 5, 1, 5}
	tests := []struct {
		name string
		ys   []float64
		want int
		why  string
	}{
		{"constant series", advConst(7, 12), 0,
			"no meaningful differences → no flips"},
		{"monotone ramp", advRamp(12, 1, 1), 0,
			"every consecutive diff keeps the same sign"},
		{"one below minimum", []float64{1, 5, 1}, 0,
			"OscillationMinPoints is 6"},
		{"exactly at minimum zigzag", []float64{1, 5, 1, 5, 1, 5}, 1,
			"six points is the documented floor and a clean zigzag must emit"},
		{"clean zigzag", zigzag, 1,
			"every meaningful consecutive pair flips sign"},
		{"huge zigzag", []float64{advHuge, -advHuge, advHuge, -advHuge, advHuge, -advHuge, advHuge, -advHuge}, 1,
			"±MaxFloat64 still produces finite flip-rate evidence; Strength/Confidence must stay in [0,1]"},
		{"denormal zigzag", []float64{advDenormal, 2 * advDenormal, advDenormal, 2 * advDenormal, advDenormal, 2 * advDenormal, advDenormal, 2 * advDenormal}, 0,
			"subnormal swings are below the meaningful-deviation floor"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := NewOscillation(advCfg())
			got := d.Detect(context.Background(), scope, advSeries(scope, uuid.New(), time.Minute, tc.ys))
			if len(got) != tc.want {
				t.Fatalf("got %d signals, want %d (%s)", len(got), tc.want, tc.why)
			}
			advAssertSane(t, got)
		})
	}
}

// --- Divergence / Convergence ---------------------------------------------

func TestDivergence_Adversarial(t *testing.T) {
	scope := uuid.New()
	tests := []struct {
		name string
		a, b []float64
		want int
		why  string
	}{
		{"both constant", advConst(5, 8), advConst(9, 8), 0,
			"constant gap → slope 0"},
		{"parallel ramps", advRamp(8, 1, 1), advRamp(8, 2, 1), 0,
			"constant gap of 1 → slope 0"},
		{"growing gap", advConst(0, 8), advRamp(8, 1, 1), 1,
			"flat vs rising: |a−b| grows by 1 per minute, 60 gap units per hour"},
		{"one below minimum", advConst(0, 4), advRamp(4, 1, 1), 0,
			"DivergenceMinPoints is 5"},
		{"exactly at minimum", advConst(0, 5), advRamp(5, 1, 1), 1,
			"five aligned observations is the documented floor"},
		{"both huge constants", advConst(advHuge, 8), advConst(advHuge/2, 8), 0,
			"unrepresentable magnitudes: gap OLS must fail closed"},
		{"both denormal ramps", []float64{advDenormal, 2 * advDenormal, 3 * advDenormal, 4 * advDenormal, 5 * advDenormal, 6 * advDenormal}, []float64{0, 0, 0, 0, 0, 0}, 0,
			"subnormal gap growth is below DivergenceMinSlope"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := NewDivergence(advCfg())
			ea, eb := uuid.New(), uuid.New()
			states := append(advSeries(scope, ea, time.Minute, tc.a), advSeries(scope, eb, time.Minute, tc.b)...)
			got := d.Detect(context.Background(), scope, states)
			if len(got) != tc.want {
				t.Fatalf("got %d signals, want %d (%s)", len(got), tc.want, tc.why)
			}
			advAssertSane(t, got)
		})
	}
}

func TestConvergence_Adversarial(t *testing.T) {
	scope := uuid.New()
	tests := []struct {
		name string
		a, b []float64
		want int
		why  string
	}{
		{"both constant", advConst(5, 8), advConst(9, 8), 0,
			"constant gap → slope 0"},
		{"shrinking gap", advConst(0, 8), []float64{10, 8, 6, 4, 2, 1, 0.5, 0}, 1,
			"flat vs descending-toward-zero: |a−b| shrinks"},
		{"growing gap no signal", advConst(0, 8), advRamp(8, 1, 1), 0,
			"Divergence territory — Convergence must stay silent"},
		{"one below minimum", advConst(0, 4), []float64{4, 3, 2, 1}, 0,
			"ConvergenceMinPoints is 5"},
		{"exactly at minimum", advConst(0, 5), []float64{8, 6, 4, 2, 0}, 1,
			"five aligned observations is the documented floor"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := NewConvergence(advCfg())
			ea, eb := uuid.New(), uuid.New()
			states := append(advSeries(scope, ea, time.Minute, tc.a), advSeries(scope, eb, time.Minute, tc.b)...)
			got := d.Detect(context.Background(), scope, states)
			if len(got) != tc.want {
				t.Fatalf("got %d signals, want %d (%s)", len(got), tc.want, tc.why)
			}
			advAssertSane(t, got)
		})
	}
}

func TestCorrelation_DuplicateTimestampsKeepWindowValid(t *testing.T) {
	// Two series whose observations all share one instant: every
	// timestamp matches, so exact alignment still pairs them. IDs are
	// assigned in value order so the tie-break zips the ramps (equal
	// distances prefer the smaller observation ID). The overlap window
	// collapses to a point, which must still satisfy TimeWindow.Validate.
	scope := uuid.New()
	d := NewCorrelation(advCfg())
	ea, eb := uuid.New(), uuid.New()
	ys := advRamp(8, 1, 1)
	var states []chronos.EntityState
	for i, y := range ys {
		states = append(states,
			chronos.EntityState{
				ID:       uuid.MustParse("00000000-0000-0000-0000-" + sprintf12(i+1)),
				EntityID: ea, ScopeID: scope, Timestamp: advBase, Features: []float64{y},
			},
			chronos.EntityState{
				ID:       uuid.MustParse("10000000-0000-0000-0000-" + sprintf12(i+1)),
				EntityID: eb, ScopeID: scope, Timestamp: advBase, Features: []float64{y},
			},
		)
	}
	got := d.Detect(context.Background(), scope, states)
	if len(got) != 1 {
		t.Fatalf("got %d signals, want 1", len(got))
	}
	if !got[0].Window.Start.Equal(got[0].Window.End) {
		t.Errorf("window = %v..%v, want a degenerate point", got[0].Window.Start, got[0].Window.End)
	}
	if got[0].Metrics["aligned_samples"] != float64(len(ys)) {
		t.Errorf("aligned_samples = %v, want %d", got[0].Metrics["aligned_samples"], len(ys))
	}
	advAssertSane(t, got)
}

// --- Recurrence ------------------------------------------------------------

func advPeerStates(scope uuid.UUID, vectors [][]float64) []chronos.EntityState {
	out := make([]chronos.EntityState, len(vectors))
	for i, v := range vectors {
		out[i] = chronos.EntityState{
			ID:        uuid.New(),
			EntityID:  uuid.New(),
			ScopeID:   scope,
			Timestamp: advBase.Add(time.Duration(i) * time.Minute),
			Features:  append([]float64(nil), v...),
		}
	}
	return out
}

func TestRecurrence_Adversarial(t *testing.T) {
	scope := uuid.New()
	tests := []struct {
		name    string
		vectors [][]float64
		want    int
		why     string
	}{
		{"identical vectors", [][]float64{{1, 1, 1}, {1, 1, 1}, {1, 1, 1}}, 1,
			"cosine of identical vectors is 1; only the newest entity has two strictly-earlier peers"},
		{"all-zero vectors", [][]float64{{0, 0, 0}, {0, 0, 0}, {0, 0, 0}}, 0,
			"a zero vector has no direction, so cosine reports 0 similarity"},
		{"huge vectors", [][]float64{{advHuge, advHuge, advHuge}, {advHuge, advHuge, advHuge}, {advHuge, advHuge, advHuge}}, 0,
			"the dot product and both norms overflow to +Inf, leaving Inf/Inf"},
		{"denormal vectors", [][]float64{{advDenormal, advDenormal, advDenormal}, {advDenormal, advDenormal, advDenormal}, {advDenormal, advDenormal, advDenormal}}, 0,
			"squaring 5e-324 underflows to zero, so both norms are zero"},
		{"orthogonal vectors", [][]float64{{1, 0, 0}, {0, 1, 0}, {0, 0, 1}}, 0,
			"zero similarity is well below SimilarityThreshold 0.85"},
		{"one peer below MinSampleSize", [][]float64{{1, 1, 1}, {1, 1, 1}}, 0,
			"MinSampleSize is 2 and only one strictly-earlier peer exists"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := NewRecurrence(advCfg())
			got := d.Detect(context.Background(), scope, advPeerStates(scope, tc.vectors))
			if len(got) != tc.want {
				t.Fatalf("got %d signals, want %d (%s)", len(got), tc.want, tc.why)
			}
			advAssertSane(t, got)
		})
	}
}

func TestRecurrence_IdenticalVectorsStayInsideTheStrengthRange(t *testing.T) {
	// Cosine of two identical vectors divides out to 1+2ulp in binary
	// floating point. Averaged into Strength unclamped, that produced
	// 1.0000000000000002 and a signal that failed its own
	// domain.Signal.Validate — a detector breaking the contract the
	// Detector interface states it must satisfy.
	scope := uuid.New()
	d := NewRecurrence(advCfg())
	got := d.Detect(context.Background(), scope, advPeerStates(scope, [][]float64{{1, 2, 3}, {1, 2, 3}, {1, 2, 3}}))
	if len(got) != 1 {
		t.Fatalf("got %d signals, want 1", len(got))
	}
	if got[0].Strength > 1 {
		t.Errorf("Strength = %.20f, want <= 1", got[0].Strength)
	}
	if err := got[0].Validate(); err != nil {
		t.Errorf("Validate() = %v, want nil", err)
	}
}

// --- Anomaly ---------------------------------------------------------------

func TestAnomaly_Adversarial(t *testing.T) {
	scope := uuid.New()
	tests := []struct {
		name    string
		vectors [][]float64
		want    int
		why     string
	}{
		{"identical vectors", [][]float64{{1, 1, 1}, {1, 1, 1}, {1, 1, 1}}, 0,
			"every peer is at similarity 1, far above AnomalyMaxSimilarity 0.5"},
		{"all-zero vectors", [][]float64{{0, 0, 0}, {0, 0, 0}, {0, 0, 0}}, 0,
			"zero-norm vectors have no direction; Anomaly skips them rather than reading Cosine 0 as isolation"},
		{"huge vectors", [][]float64{{advHuge, advHuge, advHuge}, {advHuge, advHuge, advHuge}, {advHuge, advHuge, advHuge}}, 3,
			"overflow makes cosine undefined, which is also reported as 0 similarity"},
		{"orthogonal vectors", [][]float64{{1, 0, 0}, {0, 1, 0}, {0, 0, 1}}, 3,
			"mutually orthogonal entities are genuinely isolated"},
		{"one peer below AnomalyMinPeers", [][]float64{{1, 0, 0}, {0, 1, 0}}, 0,
			"AnomalyMinPeers is 2 and each subject sees only one peer"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := NewAnomaly(advCfg())
			got := d.Detect(context.Background(), scope, advPeerStates(scope, tc.vectors))
			if len(got) != tc.want {
				t.Fatalf("got %d signals, want %d (%s)", len(got), tc.want, tc.why)
			}
			advAssertSane(t, got)
		})
	}
}

// --- OutlierCluster --------------------------------------------------------

// advCohort builds n series in one scope, all carrying ys.
func advCohort(scope uuid.UUID, n int, step time.Duration, ys []float64) []chronos.EntityState {
	var out []chronos.EntityState
	for i := 0; i < n; i++ {
		out = append(out, advSeries(scope, uuid.New(), step, ys)...)
	}
	return out
}

func TestOutlierCluster_Adversarial(t *testing.T) {
	scope := uuid.New()
	tests := []struct {
		name   string
		series int
		ys     []float64
		step   time.Duration
		want   int
		why    string
	}{
		{"flat cohort", 4, advConst(5, 10), time.Minute, 0,
			"a constant series never differs from its own constant baseline"},
		{"below MinSeries", 2, []float64{1, 1, 1, 1, 1, 90, 1, 1}, time.Minute, 0,
			"OutlierClusterMinSeries is 3"},
		{"exactly at MinSeries", 3, []float64{1, 1.1, 0.9, 1, 1.05, 900, 1, 1}, time.Minute, 1,
			"three series is the documented floor"},
		{"huge constant cohort", 4, advConst(advHuge, 10), time.Minute, 0,
			"a constant series is flat whatever its magnitude"},
		{"two-observation series", 4, []float64{1, 900}, time.Minute, 0,
			"the detector needs more than three observations to form a baseline"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := NewOutlierCluster(advCfg())
			got := d.Detect(context.Background(), scope, advCohort(scope, tc.series, tc.step, tc.ys))
			if len(got) != tc.want {
				t.Fatalf("got %d signals, want %d (%s)", len(got), tc.want, tc.why)
			}
			advAssertSane(t, got)
		})
	}
}

func TestOutlierCluster_DuplicateTimestampsCollapseToOneBucket(t *testing.T) {
	// Every observation at one instant: the outlier events all hash
	// into the same time bucket, so at most one cluster can form and
	// the window is a point.
	scope := uuid.New()
	d := NewOutlierCluster(advCfg())
	got := d.Detect(context.Background(), scope, advCohort(scope, 4, 0, []float64{1, 1.1, 0.9, 1, 1.05, 900, 1, 1}))
	if len(got) > 1 {
		t.Fatalf("got %d signals, want at most 1 when every observation shares an instant", len(got))
	}
	for _, s := range got {
		if !s.Window.Start.Equal(s.Window.End) {
			t.Errorf("window = %v..%v, want a degenerate point", s.Window.Start, s.Window.End)
		}
	}
	advAssertSane(t, got)
}

// --- CrossScopeCorrelation -------------------------------------------------

func TestCrossScopeCorrelation_Adversarial(t *testing.T) {
	tests := []struct {
		name string
		a, b []float64
		want int
		why  string
	}{
		{"both constant", advConst(5, 8), advConst(9, 8), 0,
			"zero variance in both scopes leaves r undefined"},
		{"identical series", advRamp(8, 1, 1), advRamp(8, 1, 1), 1,
			"two scopes moving identically is exactly the pattern"},
		{"one below minimum", advRamp(4, 1, 1), advRamp(4, 1, 1), 0,
			"CrossScopeMinPoints is 5"},
		{"exactly at minimum", advRamp(5, 1, 1), advRamp(5, 1, 1), 1,
			"five aligned observations is the documented floor"},
		{"both huge", advConst(advHuge, 8), advConst(advHuge, 8), 0,
			"overflowing sums cannot establish a coefficient"},
		{"huge ramps", []float64{advHuge, advHuge / 2, advHuge / 3, advHuge / 4, advHuge / 5, advHuge / 6, advHuge / 7, advHuge / 8}, []float64{advHuge, advHuge / 2, advHuge / 3, advHuge / 4, advHuge / 5, advHuge / 6, advHuge / 7, advHuge / 8}, 0,
			"identical shapes at an unrepresentable magnitude"},
		{"denormal ramps", []float64{advDenormal, 2 * advDenormal, 3 * advDenormal, 4 * advDenormal, 5 * advDenormal, 6 * advDenormal, 7 * advDenormal, 8 * advDenormal}, []float64{advDenormal, 2 * advDenormal, 3 * advDenormal, 4 * advDenormal, 5 * advDenormal, 6 * advDenormal, 7 * advDenormal, 8 * advDenormal}, 0,
			"subnormal products underflow to zero variance"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := NewCrossScopeCorrelation(advCfg())
			sa, sb := uuid.New(), uuid.New()
			states := append(advSeries(sa, uuid.New(), time.Minute, tc.a), advSeries(sb, uuid.New(), time.Minute, tc.b)...)
			got := d.CrossDetect(context.Background(), states)
			if len(got) != tc.want {
				t.Fatalf("got %d signals, want %d (%s)", len(got), tc.want, tc.why)
			}
			advAssertSane(t, got)
		})
	}
}

func TestCrossScopeCorrelation_OutOfOrderInputIsSortedInternally(t *testing.T) {
	// Unlike the per-scope detectors, CrossDetect sorts each group by
	// timestamp itself, so descending input must still produce a
	// forward-running window.
	d := NewCrossScopeCorrelation(advCfg())
	sa, sb := uuid.New(), uuid.New()
	states := append(advSeries(sa, uuid.New(), -time.Minute, advRamp(8, 1, 1)), advSeries(sb, uuid.New(), -time.Minute, advRamp(8, 1, 1))...)
	got := d.CrossDetect(context.Background(), states)
	if len(got) != 1 {
		t.Fatalf("got %d signals, want 1", len(got))
	}
	if got[0].Window.End.Before(got[0].Window.Start) {
		t.Errorf("window = %v..%v, want End at or after Start", got[0].Window.Start, got[0].Window.End)
	}
	advAssertSane(t, got)
}

// --- Resolved findings -----------------------------------------------------
//
// Tests below used to pin incorrect behaviour under TestFinding_*.
// Each asserts the corrected contract so a regression is visible.

// TestCorrelationRefusesTwoPointCollinearity records that a
// CorrelationMinPoints floor below 3 is rejected by config.Validate —
// two points are always collinear, so |r| = 1 would be manufactured.
func TestCorrelationRefusesTwoPointCollinearity(t *testing.T) {
	cfg := advCfg()
	cfg.CorrelationMinPoints = 2
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected Validate to reject CorrelationMinPoints=2")
	}
	// Detector defence in depth: even a bypassed Validate must not emit.
	scope := uuid.New()
	states := append(advSeries(scope, uuid.New(), time.Minute, []float64{1, 2}),
		advSeries(scope, uuid.New(), time.Minute, []float64{3, 91})...)
	got := NewCorrelation(cfg).Detect(context.Background(), scope, states)
	if len(got) != 0 {
		t.Fatalf("got %d signals with MinPoints=2, want 0", len(got))
	}
}

// TestOutlierClusterIgnoresDenormalDeviation records that a one-ulp
// subnormal move off a zero baseline does not form a cohort signal.
func TestOutlierClusterIgnoresDenormalDeviation(t *testing.T) {
	scope := uuid.New()
	d := NewOutlierCluster(advCfg())
	ys := []float64{0, 0, 0, 0, 0, advDenormal, 0, 0}
	got := d.Detect(context.Background(), scope, advCohort(scope, 3, time.Minute, ys))
	if len(got) != 0 {
		t.Fatalf("got %d signals, want 0 — denormal noise is not a cluster", len(got))
	}
}

// TestOutlierClusterConfidenceTracksSupport records that confidence
// is strength × sampleFactor, not strength+0.3.
func TestOutlierClusterConfidenceTracksSupport(t *testing.T) {
	scope := uuid.New()
	d := NewOutlierCluster(advCfg())
	ys := []float64{1, 1, 1, 1, 1, 10, 1, 1}
	got := d.Detect(context.Background(), scope, advCohort(scope, 3, time.Minute, ys))
	if len(got) != 1 {
		t.Fatalf("got %d signals, want 1", len(got))
	}
	s := got[0]
	wantConf := clamp01(s.Strength * sampleFactor(3, 2*advCfg().OutlierClusterMinSeries))
	if s.Confidence != wantConf {
		t.Errorf("Confidence = %v, want %v (strength × sampleFactor)", s.Confidence, wantConf)
	}
	advAssertSane(t, got)
}

// TestChangePointPrefersCleanStepsOverNoisyOnes records that a
// perfectly constant-regime step wins over a noisy one: +Inf
// standardised shift is ranked as maximum evidence, not discarded.
func TestChangePointPrefersCleanStepsOverNoisyOnes(t *testing.T) {
	scope := uuid.New()
	clean := NewChangePoint(advCfg()).Detect(context.Background(), scope,
		advSeries(scope, uuid.New(), time.Minute, []float64{1, 1, 1, 1, 9, 9, 9, 9}))
	noisy := NewChangePoint(advCfg()).Detect(context.Background(), scope,
		advSeries(scope, uuid.New(), time.Minute, []float64{1, 1.01, 0.99, 1, 9, 9.01, 8.99, 9}))
	if len(clean) != 1 || len(noisy) != 1 {
		t.Fatalf("got %d clean and %d noisy signals, want 1 of each", len(clean), len(noisy))
	}
	if clean[0].Metrics["split_index"] != 4 {
		t.Errorf("clean split_index = %v, want 4 (the true split)", clean[0].Metrics["split_index"])
	}
	if noisy[0].Metrics["split_index"] != 4 {
		t.Errorf("noisy split_index = %v, want 4", noisy[0].Metrics["split_index"])
	}
	if clean[0].Strength < noisy[0].Strength {
		t.Errorf("clean Strength %v < noisy Strength %v: clean regimes must not score weaker",
			clean[0].Strength, noisy[0].Strength)
	}
	if clean[0].Metrics["shift"] != changePointInfiniteShift {
		t.Errorf("clean shift = %v, want the finite Inf sentinel %v", clean[0].Metrics["shift"], changePointInfiniteShift)
	}
	advAssertSane(t, clean)
	advAssertSane(t, noisy)
}

// TestAnomalySkipsZeroVectors records that zero-norm feature vectors
// are not compared: Cosine(0) means "undefined", not "isolated".
func TestAnomalySkipsZeroVectors(t *testing.T) {
	scope := uuid.New()
	got := NewAnomaly(advCfg()).Detect(context.Background(), scope, advPeerStates(scope, [][]float64{{0, 0, 0}, {0, 0, 0}, {0, 0, 0}}))
	if len(got) != 0 {
		t.Fatalf("got %d signals, want 0 — identical zero vectors are not anomalies", len(got))
	}
}

// TestFinding_ChangePointAndStallDisagreeOnTheSameSeries records that
// one eight-point series can be called both flat and regime-shifted
// when ChangePointMinDelta is left at its default of 0. Documented
// product behaviour: set CHRONOS_CHANGEPOINT_MIN_DELTA to require
// actionable absolute movement.
func TestFinding_ChangePointAndStallDisagreeOnTheSameSeries(t *testing.T) {
	scope := uuid.New()
	entity := uuid.New()
	ys := []float64{1.0000, 1.0001, 1.0000, 1.0001, 1.0100, 1.0101, 1.0100, 1.0101}
	states := advSeries(scope, entity, time.Minute, ys)

	cp := NewChangePoint(advCfg()).Detect(context.Background(), scope, states)
	st := NewStall(advCfg()).Detect(context.Background(), scope, states)
	if len(cp) != 1 {
		t.Fatalf("got %d change_point signals, want 1", len(cp))
	}
	if len(st) != 1 {
		t.Fatalf("got %d stall signals, want 1", len(st))
	}
	cfg := advCfg()
	cfg.ChangePointMinDelta = 0.05
	if gated := NewChangePoint(cfg).Detect(context.Background(), scope, states); len(gated) != 0 {
		t.Errorf("got %d change_point signals with MinDelta 0.05, want 0", len(gated))
	}
}

// TestTrendReflectsWallClockSpacing records that Trend regresses
// outcome against wall-clock hours (trend-v2), so irregular sampling
// changes the fitted slope relative to a regularly spaced series with
// the same outcome values.
func TestTrendReflectsWallClockSpacing(t *testing.T) {
	scope := uuid.New()
	ys := advRamp(8, 1, 1)
	regular := NewTrend(advCfg()).Detect(context.Background(), scope, advSeries(scope, uuid.New(), time.Hour, ys))
	// Mild irregularity: 0.5h, 1.5h, 0.75h, … — still a clear rising
	// line, but not uniform cadence.
	offsets := make([]time.Duration, 8)
	gaps := []time.Duration{30 * time.Minute, 90 * time.Minute, 45 * time.Minute, 75 * time.Minute}
	var acc time.Duration
	for i := range offsets {
		offsets[i] = acc
		acc += gaps[i%len(gaps)]
	}
	irregular := NewTrend(advCfg()).Detect(context.Background(), scope, advSeriesAt(scope, uuid.New(), offsets, ys))
	if len(regular) != 1 || len(irregular) != 1 {
		t.Fatalf("got %d regular and %d irregular signals, want 1 of each", len(regular), len(irregular))
	}
	if regular[0].Metrics["slope"] == irregular[0].Metrics["slope"] {
		t.Errorf("slope identical under irregular spacing (%v): wall-clock axis is not in effect", regular[0].Metrics["slope"])
	}
	// Regular hourly +1 outcome → slope ≈ 1.0 per hour.
	if math.Abs(regular[0].Metrics["slope"]-1) > 1e-9 {
		t.Errorf("regular slope = %v, want 1.0 (outcome units per hour)", regular[0].Metrics["slope"])
	}
}

// TestStallIsSilentWhenItsMetricsOverflow pins the resolution of a
// finding this file previously only recorded: the finiteness guards
// covered Strength and Confidence but not the Metrics map, so a stall
// over twelve MaxFloat64 observations emitted a signal that passed
// Validate carrying metrics["mean"] = +Inf.
//
// The cost was downstream and silent. Every SQL store did
// `metricsJSON, _ := json.Marshal(sig.Metrics)`; encoding/json refuses
// +Inf and returns zero bytes with the error, so the discarded error
// wrote an empty metrics column — the whole map lost, not the one key,
// and indistinguishable afterwards from a detector that surfaced no
// metrics at all.
//
// domain.Signal.Validate now rejects non-finite metrics, and detectors
// drop what will not validate rather than emit it. The stall is real —
// the normalised series is exactly flat — but the mean of the raw
// values is not a number this process can carry, so the correct output
// is no signal.
//
// The check on the raw arithmetic is kept: if mean() stops overflowing,
// this test is measuring nothing and should be re-derived rather than
// left passing for the wrong reason.
func TestStallIsSilentWhenItsMetricsOverflow(t *testing.T) {
	scope := uuid.New()
	ys := advConst(advHuge, 12)

	if !math.IsInf(mean(ys), 1) {
		t.Fatalf("mean of twelve MaxFloat64 values = %v, want +Inf — the overflow this test rests on is gone", mean(ys))
	}
	if _, err := json.Marshal(map[string]float64{"mean": mean(ys)}); err == nil {
		t.Fatal("json.Marshal accepted +Inf; the data loss this guard prevents no longer happens")
	}

	got := NewStall(advCfg()).Detect(context.Background(), scope, advSeries(scope, uuid.New(), time.Minute, ys))
	if len(got) != 0 {
		t.Fatalf("got %d signals, want 0 — metrics[mean] = %v cannot be persisted", len(got), got[0].Metrics["mean"])
	}
}

// TestFinding_DetectorsRequireChronologicalInput records what happens
// when Detect is handed descending timestamps.
//
// The Detector interface states the states slice is "guaranteed to be
// sorted by Timestamp ascending", and the engine does sort it, so this
// is a precondition violation rather than a bug. What it used to
// produce was a signal whose window ran backwards and whose feature
// evolution was non-monotonic — a signal failing its own Validate, the
// one thing the same interface comment says returned signals must not
// do. Detectors now drop what will not validate, so the violation
// costs the signal instead of corrupting it.
//
// It stays a finding because the loss is still silent: a caller
// feeding descending data gets an empty slice and no indication that
// its input, rather than its data, is the reason.
func TestFinding_DetectorsRequireChronologicalInput(t *testing.T) {
	scope := uuid.New()
	descending := advSeries(scope, uuid.New(), -time.Minute, advRamp(16, 1, 1))

	detectors := []Detector{
		NewTrend(advCfg()), NewStall(advCfg()), NewChangePoint(advCfg()), NewSeasonality(advCfg()),
		NewOscillation(advCfg()),
	}
	for _, d := range detectors {
		got := d.Detect(context.Background(), scope, descending)
		if len(got) != 0 {
			t.Errorf("%s: got %d signals from descending input, want 0 — "+
				"every signal it can build here has an inverted window", d.Pattern(), len(got))
		}
	}

	// The engine restores the precondition, so the same states routed
	// through Detect are perceived normally and validate.
	routed := NewEngine(advCfg()).WithCrossScopeDetectors(nil).Detect(context.Background(), descending)
	if len(routed) == 0 {
		t.Fatal("engine returned no signals for a clean ramp: the sort that restores the precondition is gone")
	}
	for _, s := range routed {
		if err := s.Validate(); err != nil {
			t.Errorf("engine-routed signal invalid: %v", err)
		}
	}
}

// --- Temporal semantics ----------------------------------------------------
//
// Pairwise detectors must not treat slice position as time. These cases
// are the regression net for that invariant: identical shapes on
// disjoint clocks are not a relationship, slopes are per hour, and a
// repeating sequence on a chaotic clock is not seasonality.

func TestCorrelation_MorningAndAfternoonAreNotRelated(t *testing.T) {
	scope := uuid.New()
	morning := time.Date(2026, 1, 1, 8, 0, 0, 0, time.UTC)
	afternoon := time.Date(2026, 1, 1, 14, 0, 0, 0, time.UTC)
	ys := []float64{1, 2, 3, 4, 5}
	offsets := []time.Duration{0, time.Minute, 2 * time.Minute, 3 * time.Minute, 4 * time.Minute}
	ea, eb := uuid.New(), uuid.New()
	states := append(
		advSeriesAt(scope, ea, shiftOffsets(offsets, morning.Sub(advBase)), ys),
		advSeriesAt(scope, eb, shiftOffsets(offsets, afternoon.Sub(advBase)), ys)...,
	)
	got := NewCorrelation(advCfg()).Detect(context.Background(), scope, states)
	if len(got) != 0 {
		t.Fatalf("got %d correlation signals from disjoint clocks, want 0", len(got))
	}
}

func TestCorrelation_OffsetWithinAndOutsideTolerance(t *testing.T) {
	scope := uuid.New()
	ys := []float64{1, 2, 3, 4, 5, 6}
	on := make([]time.Duration, len(ys))
	near := make([]time.Duration, len(ys))
	far := make([]time.Duration, len(ys))
	for i := range ys {
		on[i] = time.Duration(i) * time.Minute
		near[i] = on[i] + 20*time.Second
		far[i] = on[i] + 2*time.Hour
	}
	ea, eb := uuid.New(), uuid.New()
	base := advSeriesAt(scope, ea, on, ys)

	within := advCfg()
	within.AlignTolerance = time.Minute
	got := NewCorrelation(within).Detect(context.Background(), scope, append(append([]chronos.EntityState{}, base...), advSeriesAt(scope, eb, near, ys)...))
	if len(got) != 1 {
		t.Fatalf("within tolerance: got %d, want 1", len(got))
	}
	if got[0].Metrics["aligned_samples"] != float64(len(ys)) {
		t.Fatalf("aligned_samples = %v, want %d", got[0].Metrics["aligned_samples"], len(ys))
	}
	if got[0].Metrics["alignment_tolerance_seconds"] != time.Minute.Seconds() {
		t.Fatalf("tolerance metric = %v", got[0].Metrics["alignment_tolerance_seconds"])
	}
	advAssertSane(t, got)

	outside := NewCorrelation(within).Detect(context.Background(), scope, append(append([]chronos.EntityState{}, base...), advSeriesAt(scope, uuid.New(), far, ys)...))
	if len(outside) != 0 {
		t.Fatalf("outside tolerance: got %d, want 0", len(outside))
	}

	exact := NewCorrelation(advCfg()).Detect(context.Background(), scope, append(append([]chronos.EntityState{}, base...), advSeriesAt(scope, uuid.New(), near, ys)...))
	if len(exact) != 0 {
		t.Fatalf("exact alignment of a 20s offset: got %d, want 0", len(exact))
	}
}

func TestCorrelation_DifferentCadenceDoesNotInventPairs(t *testing.T) {
	scope := uuid.New()
	// A every minute, B halfway between those minutes. Exact alignment
	// shares no timestamps, so a perfect ordinal match is not evidence.
	ys := []float64{1, 2, 3, 4, 5, 6, 7, 8}
	aOff := make([]time.Duration, len(ys))
	bOff := make([]time.Duration, len(ys))
	for i := range ys {
		aOff[i] = time.Duration(i) * time.Minute
		bOff[i] = aOff[i] + 30*time.Second
	}
	states := append(
		advSeriesAt(scope, uuid.New(), aOff, ys),
		advSeriesAt(scope, uuid.New(), bOff, ys)...,
	)
	got := NewCorrelation(advCfg()).Detect(context.Background(), scope, states)
	if len(got) != 0 {
		t.Fatalf("got %d signals from interleaved cadences, want 0", len(got))
	}
}

func TestCorrelation_WindowIsContributingOverlap(t *testing.T) {
	scope := uuid.New()
	ys := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	ea, eb := uuid.New(), uuid.New()
	a := advSeries(scope, ea, time.Hour, ys) // hours 0..9
	b := advSeries(scope, eb, time.Hour, ys)
	for i := range b {
		b[i].Timestamp = b[i].Timestamp.Add(3 * time.Hour) // hours 3..12
	}
	got := NewCorrelation(advCfg()).Detect(context.Background(), scope, append(a, b...))
	if len(got) != 1 {
		t.Fatalf("got %d, want 1", len(got))
	}
	wantStart := advBase.Add(3 * time.Hour)
	wantEnd := advBase.Add(9 * time.Hour)
	if !got[0].Window.Start.Equal(wantStart) || !got[0].Window.End.Equal(wantEnd) {
		t.Fatalf("window = %s..%s, want overlap %s..%s (not the union of both histories)",
			got[0].Window.Start, got[0].Window.End, wantStart, wantEnd)
	}
	if got[0].Metrics["aligned_samples"] != 7 {
		t.Fatalf("aligned_samples = %v, want 7", got[0].Metrics["aligned_samples"])
	}
}

func TestCrossScopeCorrelation_DisjointClocksNoSignal(t *testing.T) {
	sa, sb := uuid.New(), uuid.New()
	ys := []float64{1, 2, 3, 4, 5, 6}
	morning := advSeries(sa, uuid.New(), time.Minute, ys)
	afternoon := advSeries(sb, uuid.New(), time.Minute, ys)
	for i := range afternoon {
		afternoon[i].Timestamp = afternoon[i].Timestamp.Add(8 * time.Hour)
	}
	got := NewCrossScopeCorrelation(advCfg()).CrossDetect(context.Background(), append(morning, afternoon...))
	if len(got) != 0 {
		t.Fatalf("got %d cross-scope signals from disjoint clocks, want 0", len(got))
	}
}

func TestDivergence_SlopeIsCadenceInvariant(t *testing.T) {
	// The same physical process — gap grows by 1 outcome unit per hour —
	// sampled hourly and every ten minutes must report approximately
	// the same per-hour slope. A per-step regression would report 1
	// versus 1/6.
	scope := uuid.New()
	hourly := gapProcess(scope, time.Hour, 6)
	dense := gapProcess(scope, 10*time.Minute, 31) // 0..5 hours inclusive
	h := NewDivergence(advCfg()).Detect(context.Background(), scope, hourly)
	d := NewDivergence(advCfg()).Detect(context.Background(), scope, dense)
	if len(h) != 1 || len(d) != 1 {
		t.Fatalf("hourly signals %d, dense signals %d, want 1 and 1", len(h), len(d))
	}
	hs, ds := h[0].Metrics["slope_per_hour"], d[0].Metrics["slope_per_hour"]
	if math.Abs(hs-1) > 1e-9 {
		t.Fatalf("hourly slope_per_hour = %v, want 1", hs)
	}
	if math.Abs(hs-ds) > 1e-6 {
		t.Fatalf("slopes differ by cadence: hourly %v dense %v", hs, ds)
	}
	if h[0].Metrics["slope"] != hs {
		t.Fatalf("slope %v and slope_per_hour %v diverged", h[0].Metrics["slope"], hs)
	}
	advAssertSane(t, h)
	advAssertSane(t, d)
}

func TestDivergence_ErraticGapBelowFitNoSignal(t *testing.T) {
	// Directional regression (R² ≈ 0.16) is not sustained divergence.
	scope := uuid.New()
	flat := advConst(0, 8)
	gaps := []float64{1, 50, 2, 60, 3, 70, 4, 80}
	states := append(
		advSeries(scope, uuid.New(), time.Hour, flat),
		advSeries(scope, uuid.New(), time.Hour, gaps)...,
	)
	got := NewDivergence(advCfg()).Detect(context.Background(), scope, states)
	if len(got) != 0 {
		t.Fatalf("got %d signals for an erratic gap (r2=%v), want 0", len(got), got[0].Metrics["r2"])
	}
}

func TestDivergence_ShortDramaticGapIsStrongButNotConfident(t *testing.T) {
	cfg := advCfg()
	cfg.DivergenceMinPoints = 3
	scope := uuid.New()
	// Gap 0, 100, 200 over three hours: slope 100/hour, perfect fit,
	// only three pairs.
	states := append(
		advSeries(scope, uuid.New(), time.Hour, []float64{0, 0, 0}),
		advSeries(scope, uuid.New(), time.Hour, []float64{0, 100, 200})...,
	)
	got := NewDivergence(cfg).Detect(context.Background(), scope, states)
	if len(got) != 1 {
		t.Fatalf("got %d, want 1", len(got))
	}
	if got[0].Strength < 0.9 {
		t.Fatalf("strength = %v, want a saturated shape", got[0].Strength)
	}
	// sampleFactor(3, 6) = 0.5, exact alignment quality = 1.
	if math.Abs(got[0].Confidence-0.5) > 1e-9 {
		t.Fatalf("confidence = %v, want 0.5 (sample size, not slope)", got[0].Confidence)
	}
	if got[0].Confidence >= got[0].Strength {
		t.Fatalf("confidence %v should stay below strength %v", got[0].Confidence, got[0].Strength)
	}
}

func TestConvergence_SlopeIsCadenceInvariant(t *testing.T) {
	scope := uuid.New()
	// Gap shrinks by 2 outcome units per hour.
	build := func(step time.Duration, n int) []chronos.EntityState {
		a := make([]float64, n)
		b := make([]float64, n)
		for i := range b {
			hours := (time.Duration(i) * step).Hours()
			b[i] = 12 - 2*hours
		}
		return append(
			advSeries(scope, uuid.New(), step, a),
			advSeries(scope, uuid.New(), step, b)...,
		)
	}
	hourly := NewConvergence(advCfg()).Detect(context.Background(), scope, build(time.Hour, 6))
	dense := NewConvergence(advCfg()).Detect(context.Background(), scope, build(10*time.Minute, 31))
	if len(hourly) != 1 || len(dense) != 1 {
		t.Fatalf("hourly %d dense %d, want 1 and 1", len(hourly), len(dense))
	}
	hs, ds := hourly[0].Metrics["slope_per_hour"], dense[0].Metrics["slope_per_hour"]
	if math.Abs(hs-(-2)) > 1e-9 || math.Abs(hs-ds) > 1e-6 {
		t.Fatalf("slopes hourly %v dense %v, want -2", hs, ds)
	}
}

func TestSeasonality_ChaoticClockIsNotAPeriod(t *testing.T) {
	scope := uuid.New()
	ys := make([]float64, 24)
	for i := range ys {
		ys[i] = math.Sin(float64(i) * math.Pi / 2) // ordinal period 4
	}
	regular := NewSeasonality(advCfg()).Detect(context.Background(), scope, advSeries(scope, uuid.New(), time.Minute, ys))
	if len(regular) != 1 {
		t.Fatalf("regular cadence: got %d, want 1", len(regular))
	}
	if regular[0].Metrics["period_samples"] != 4 {
		t.Fatalf("period_samples = %v, want 4", regular[0].Metrics["period_samples"])
	}
	if regular[0].Metrics["period"] != 4 {
		t.Fatalf("period = %v, want sample lag 4", regular[0].Metrics["period"])
	}
	if math.Abs(regular[0].Metrics["sampling_interval_seconds"]-60) > 1e-9 {
		t.Fatalf("sampling_interval_seconds = %v, want 60", regular[0].Metrics["sampling_interval_seconds"])
	}
	if math.Abs(regular[0].Metrics["period_seconds"]-240) > 1e-6 {
		t.Fatalf("period_seconds = %v, want 240", regular[0].Metrics["period_seconds"])
	}

	// Same values, timestamps that wander. Ordinal autocorrelation
	// would still see period 4; temporal seasonality must not.
	offsets := make([]time.Duration, len(ys))
	var acc time.Duration
	for i := range offsets {
		offsets[i] = acc
		acc += time.Duration(1+i*i) * time.Second
	}
	chaotic := NewSeasonality(advCfg()).Detect(context.Background(), scope, advSeriesAt(scope, uuid.New(), offsets, ys))
	if len(chaotic) != 0 {
		t.Fatalf("chaotic timestamps: got %d seasonality signals, want 0", len(chaotic))
	}
}

func TestOscillation_IgnoresCadence(t *testing.T) {
	scope := uuid.New()
	ys := []float64{1, 5, 1, 5, 1, 5, 1, 5}
	minute := NewOscillation(advCfg()).Detect(context.Background(), scope, advSeries(scope, uuid.New(), time.Minute, ys))
	hour := NewOscillation(advCfg()).Detect(context.Background(), scope, advSeries(scope, uuid.New(), time.Hour, ys))
	if len(minute) != 1 || len(hour) != 1 {
		t.Fatalf("minute %d hour %d, want 1 and 1", len(minute), len(hour))
	}
	if minute[0].Metrics["flip_rate"] != hour[0].Metrics["flip_rate"] {
		t.Fatalf("flip_rate changed with cadence: %v vs %v", minute[0].Metrics["flip_rate"], hour[0].Metrics["flip_rate"])
	}
}

// gapProcess builds a flat series and a partner whose outcome equals
// elapsed hours, so |a−b| grows by 1 per hour regardless of step.
func gapProcess(scope uuid.UUID, step time.Duration, n int) []chronos.EntityState {
	flat := make([]float64, n)
	rising := make([]float64, n)
	for i := range rising {
		rising[i] = (time.Duration(i) * step).Hours()
	}
	return append(
		advSeries(scope, uuid.New(), step, flat),
		advSeries(scope, uuid.New(), step, rising)...,
	)
}

func shiftOffsets(offsets []time.Duration, by time.Duration) []time.Duration {
	out := make([]time.Duration, len(offsets))
	for i, d := range offsets {
		out[i] = d + by
	}
	return out
}
