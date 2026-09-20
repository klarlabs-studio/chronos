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
// to the engine. Tests named TestFinding_* document behaviour that is
// currently questionable rather than asserting it is correct; each
// says why in its comment.
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
			"a perfect line whose slope is 5e-324, far below TrendMinSlope 0.05"},
		{"slope exactly at threshold", advRamp(8, 0, 0.05), 1,
			"|slope| == TrendMinSlope is not below it, so it emits"},
		{"slope just under threshold", advRamp(8, 0, 0.049), 0,
			"|slope| below TrendMinSlope stays silent"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := NewTrend(advCfg())
			got := d.Detect(context.Background(), scope, advSeries(scope, uuid.New(), time.Minute, tc.ys))
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
			"a signal is emitted, but not at the true split; see TestFinding_ChangePointPrefersNoisyStepsOverCleanOnes"},
		{"huge constant", advConst(advHuge, 12), 0,
			"no shift is computable when every regime statistic overflows"},
		{"huge step", append(advConst(1, 6), advConst(advHuge, 6)...), 0,
			"every candidate split puts MaxFloat64 values on one side, whose mean and spread overflow; no split yields a real shift"},
		{"denormal step", append(advConst(advDenormal, 6), advConst(2*advDenormal, 6)...), 0,
			"subnormal squared deviations underflow to zero, so every split has zero pooled spread and an infinite shift"},
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
		{"unequal lengths align on the tail", advRamp(20, 1, 1), advRamp(5, 1, 1), 1,
			"alignment is by ordinal index over the shorter series"},
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

func TestCorrelation_DuplicateTimestampsKeepWindowValid(t *testing.T) {
	// Two series whose observations all share one instant: the
	// overlap window collapses to a point, which must still satisfy
	// TimeWindow.Validate (End not before Start).
	scope := uuid.New()
	d := NewCorrelation(advCfg())
	ea, eb := uuid.New(), uuid.New()
	states := append(advSeries(scope, ea, 0, advRamp(8, 1, 1)), advSeries(scope, eb, 0, advRamp(8, 1, 1))...)
	got := d.Detect(context.Background(), scope, states)
	if len(got) != 1 {
		t.Fatalf("got %d signals, want 1", len(got))
	}
	if !got[0].Window.Start.Equal(got[0].Window.End) {
		t.Errorf("window = %v..%v, want a degenerate point", got[0].Window.Start, got[0].Window.End)
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
		{"all-zero vectors", [][]float64{{0, 0, 0}, {0, 0, 0}, {0, 0, 0}}, 3,
			"cosine reports 0 for a directionless vector, which reads as total isolation; see TestFinding_AnomalyCallsZeroVectorsIsolated"},
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

// --- Findings --------------------------------------------------------------
//
// The tests below document behaviour that is questionable but was
// left in place because changing it would change what the detectors
// mean, not just how they compute. They assert the behaviour that
// exists today so a future change to it is visible.

// TestFinding_SpikeConfidenceEqualsStrengthAtMinimumSamples records
// that Spike and Drop set Confidence = Strength, so six observations
// — the minimum the detector accepts — yield Confidence 1.0.
//
// Chronos states that strength describes the magnitude of the
// observed pattern and confidence the quality of the evidence, and
// that the two must remain distinct. Spike and Drop collapse them by
// design (see the type comment on Spike), so the thinnest admissible
// window can report maximum certainty. Every other detector scales
// confidence by a sample-size factor.
func TestFinding_SpikeConfidenceEqualsStrengthAtMinimumSamples(t *testing.T) {
	scope := uuid.New()
	d := NewSpike(advCfg())
	// Six observations: five of baseline plus the point under test.
	got := d.Detect(context.Background(), scope, advSeries(scope, uuid.New(), time.Minute, []float64{1, 1.1, 0.9, 1, 1.05, 900}))
	if len(got) != 1 {
		t.Fatalf("got %d signals, want 1", len(got))
	}
	s := got[0]
	if s.Confidence != s.Strength {
		t.Errorf("Confidence = %v, Strength = %v: this test exists because they are currently equal", s.Confidence, s.Strength)
	}
	if s.Confidence != 1 {
		t.Errorf("Confidence = %v, want 1 — the finding is that six samples produce maximum confidence", s.Confidence)
	}
	if s.ConfidenceClass != domain.ConfidenceClassTentative {
		t.Errorf("ConfidenceClass = %q, want %q: the class says tentative while the number says certain", s.ConfidenceClass, domain.ConfidenceClassTentative)
	}
}

// TestFinding_TwoPointCorrelationIsAlwaysPerfect records that
// Correlation accepts CorrelationMinPoints as low as 2, and any two
// points are exactly collinear, so r is always +/-1. The shipped
// default is 5, so this is reachable only by configuration, but the
// detector does not refuse it.
func TestFinding_TwoPointCorrelationIsAlwaysPerfect(t *testing.T) {
	cfg := advCfg()
	cfg.CorrelationMinPoints = 2
	scope := uuid.New()
	// Two series with nothing in common but their length.
	states := append(advSeries(scope, uuid.New(), time.Minute, []float64{1, 2}),
		advSeries(scope, uuid.New(), time.Minute, []float64{3, 91})...)
	got := NewCorrelation(cfg).Detect(context.Background(), scope, states)
	if len(got) != 1 {
		t.Fatalf("got %d signals, want 1", len(got))
	}
	if got[0].Metrics["r"] != 1 {
		t.Errorf("r = %v, want exactly 1 — two points are always collinear", got[0].Metrics["r"])
	}
	if got[0].Strength != 1 {
		t.Errorf("Strength = %v, want 1", got[0].Strength)
	}
}

// TestFinding_OutlierClusterSaturatesOnDenormalDeviation records that
// a change of one subnormal ulp off a zero baseline is scored as
// peak_z = 100 and forms a full cohort signal.
//
// The detector deliberately substitutes a saturated z when the
// baseline has zero variance (see the default branch in Detect),
// because a constant baseline plus any different value is "the
// strongest possible outlier" in relative terms. With no absolute
// floor, 5e-324 qualifies. Three such series produce a cluster with
// confidence 0.3 built entirely out of the smallest representable
// numbers.
func TestFinding_OutlierClusterSaturatesOnDenormalDeviation(t *testing.T) {
	scope := uuid.New()
	d := NewOutlierCluster(advCfg())
	ys := []float64{0, 0, 0, 0, 0, advDenormal, 0, 0}
	got := d.Detect(context.Background(), scope, advCohort(scope, 3, time.Minute, ys))
	if len(got) != 1 {
		t.Fatalf("got %d signals, want 1 (the finding is that this fires at all)", len(got))
	}
	s := got[0]
	if s.Evidence[0].Metrics["peak_z"] != 100 {
		t.Errorf("peak_z = %v, want the saturated 100", s.Evidence[0].Metrics["peak_z"])
	}
	if s.Confidence != 0.3 {
		t.Errorf("Confidence = %v, want 0.3 — the floor the detector adds to a bare-minimum cluster", s.Confidence)
	}
	if s.Metrics["member_count"] != 3 {
		t.Errorf("member_count = %v, want 3", s.Metrics["member_count"])
	}
	advAssertSane(t, got)
}

// TestFinding_ChangePointAndStallDisagreeOnTheSameSeries records that
// one eight-point series can be called both flat and regime-shifted.
//
// ChangePoint standardises the mean shift by the series' own
// variability, so a metric that moves in its fourth decimal place
// divides a third-decimal change out to hundreds of sigma.
// ChangePointMinDelta exists to require a minimum absolute movement
// but ships as 0, leaving the guard off by default. The result is a
// change_point at strength 1.0 and a stall at strength 0.90 over
// exactly the same observations.
func TestFinding_ChangePointAndStallDisagreeOnTheSameSeries(t *testing.T) {
	scope := uuid.New()
	entity := uuid.New()
	ys := []float64{1.0000, 1.0001, 1.0000, 1.0001, 1.0100, 1.0101, 1.0100, 1.0101}
	states := advSeries(scope, entity, time.Minute, ys)

	cp := NewChangePoint(advCfg()).Detect(context.Background(), scope, states)
	st := NewStall(advCfg()).Detect(context.Background(), scope, states)
	if len(cp) != 1 || len(st) != 1 {
		t.Fatalf("got %d change_point and %d stall signals, want 1 of each — the finding is that both fire", len(cp), len(st))
	}
	if delta := math.Abs(cp[0].Metrics["delta_mean"]); delta > 0.011 {
		t.Errorf("delta_mean = %v, want the ~0.01 absolute movement this finding is about", delta)
	}
	if cp[0].Metrics["shift"] < 100 {
		t.Errorf("shift = %v, want >= 100 sigma from a 0.01 absolute move", cp[0].Metrics["shift"])
	}
	if cp[0].Strength != 1 {
		t.Errorf("change_point Strength = %v, want 1", cp[0].Strength)
	}
	if st[0].Strength < 0.8 {
		t.Errorf("stall Strength = %v, want >= 0.8 — the same series is simultaneously called flat", st[0].Strength)
	}

	// With an absolute floor configured, the change_point is
	// suppressed and only the stall survives.
	cfg := advCfg()
	cfg.ChangePointMinDelta = 0.05
	if gated := NewChangePoint(cfg).Detect(context.Background(), scope, states); len(gated) != 0 {
		t.Errorf("got %d change_point signals with MinDelta 0.05, want 0", len(gated))
	}
}

// TestFinding_ChangePointPrefersNoisyStepsOverCleanOnes records that
// adding noise to a step change improves both the split index
// ChangePoint reports and the strength it assigns.
//
// standardisedMeanShift returns +Inf when the pooled spread is zero
// and the two means differ — a step between two perfectly constant
// regimes. Detect discards +Inf alongside NaN, so the true maximum
// is thrown away and a strictly worse split wins.
//
// Measured on eight observations:
//
//	{1,1,1,1,9,9,9,9}              -> split_index 5, shift 2.45, strength 0.63
//	{1,1.01,0.99,1,9,9.01,8.99,9}  -> split_index 4, shift 1131,  strength 1.00
//
// The first series is the second with the noise removed. Chronos
// reports the cleaner evidence as the weaker pattern and locates it
// one observation late.
func TestFinding_ChangePointPrefersNoisyStepsOverCleanOnes(t *testing.T) {
	scope := uuid.New()
	clean := NewChangePoint(advCfg()).Detect(context.Background(), scope,
		advSeries(scope, uuid.New(), time.Minute, []float64{1, 1, 1, 1, 9, 9, 9, 9}))
	noisy := NewChangePoint(advCfg()).Detect(context.Background(), scope,
		advSeries(scope, uuid.New(), time.Minute, []float64{1, 1.01, 0.99, 1, 9, 9.01, 8.99, 9}))
	if len(clean) != 1 || len(noisy) != 1 {
		t.Fatalf("got %d clean and %d noisy signals, want 1 of each", len(clean), len(noisy))
	}
	if noisy[0].Metrics["split_index"] != 4 {
		t.Errorf("noisy split_index = %v, want 4 (the true split)", noisy[0].Metrics["split_index"])
	}
	if clean[0].Metrics["split_index"] == 4 {
		t.Errorf("clean split_index = 4: the +Inf-discarding behaviour this test records is gone")
	}
	if clean[0].Strength >= noisy[0].Strength {
		t.Errorf("clean Strength %v >= noisy Strength %v: this test exists because the cleaner series scores lower",
			clean[0].Strength, noisy[0].Strength)
	}
	advAssertSane(t, clean)
	advAssertSane(t, noisy)
}

// TestFinding_TrendIgnoresTheTimeAxis records that Trend regresses
// the outcome against the ordinal index, not against the timestamp.
// A series sampled once a minute and a series whose gaps run from
// 17 milliseconds to 400 hours produce an identical slope, R2,
// strength and confidence. The detector documents the ordinal
// regression; the consequence — that "rate of change" carries no
// time unit and irregular sampling is invisible — is not documented.
func TestFinding_TrendIgnoresTheTimeAxis(t *testing.T) {
	scope := uuid.New()
	ys := advRamp(8, 1, 1)
	regular := NewTrend(advCfg()).Detect(context.Background(), scope, advSeries(scope, uuid.New(), time.Minute, ys))
	irregular := NewTrend(advCfg()).Detect(context.Background(), scope, advSeriesAt(scope, uuid.New(), advIrregularOffsets(8), ys))
	if len(regular) != 1 || len(irregular) != 1 {
		t.Fatalf("got %d regular and %d irregular signals, want 1 of each", len(regular), len(irregular))
	}
	for _, k := range []string{"slope", "r2", "intercept"} {
		if regular[0].Metrics[k] != irregular[0].Metrics[k] {
			t.Errorf("%s: regular %v, irregular %v — this test exists because they are currently identical", k, regular[0].Metrics[k], irregular[0].Metrics[k])
		}
	}
	if regular[0].Confidence != irregular[0].Confidence {
		t.Errorf("Confidence: regular %v, irregular %v", regular[0].Confidence, irregular[0].Confidence)
	}
}

// TestFinding_AnomalyCallsZeroVectorsIsolated records that a cohort
// of entities whose feature vectors are all zero is reported as three
// maximally isolated anomalies.
//
// similarity.Cosine returns 0 for a zero vector because it has no
// direction — "cannot be compared", not "compared and found
// dissimilar". Anomaly reads that 0 as maximum distance and emits
// strength 1.0. Entities that are numerically identical are reported
// as each other's opposites.
func TestFinding_AnomalyCallsZeroVectorsIsolated(t *testing.T) {
	scope := uuid.New()
	got := NewAnomaly(advCfg()).Detect(context.Background(), scope, advPeerStates(scope, [][]float64{{0, 0, 0}, {0, 0, 0}, {0, 0, 0}}))
	if len(got) != 3 {
		t.Fatalf("got %d signals, want 3 (the finding is that identical entities are all called anomalous)", len(got))
	}
	for i, s := range got {
		if s.Strength != 1 {
			t.Errorf("signal %d: Strength = %v, want 1 — maximum isolation from an identical peer", i, s.Strength)
		}
		if s.Metrics["max_peer_similarity"] != 0 {
			t.Errorf("signal %d: max_peer_similarity = %v, want 0", i, s.Metrics["max_peer_similarity"])
		}
	}
	advAssertSane(t, got)
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
