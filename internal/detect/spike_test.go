package detect

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/felixgeelhaar/chronos/internal/config"
	"github.com/felixgeelhaar/chronos/internal/domain"
	"github.com/google/uuid"
)

func spikeCfg() *config.Config {
	return &config.Config{
		MaxSignalsPerRun: 100,
		SpikeZScore:      2.5,
		DropZScore:       2.5,
		SpikeWindow:      5,
	}
}

func TestSpike_PositiveDeviationEmits(t *testing.T) {
	d := NewSpike(spikeCfg())
	scope := uuid.New()
	entity := uuid.New()
	now := time.Now()
	// Five baseline points around 10, then a 20 outlier.
	ys := []float64{10, 10.1, 9.9, 10.05, 9.95, 20}
	states := mkSeries(scope, entity, now, ys)

	got := d.Detect(context.Background(), scope, states)
	if len(got) != 1 {
		t.Fatalf("got %d spikes, want 1", len(got))
	}
	sig := got[0]
	if sig.Pattern != domain.PatternTypeSpike {
		t.Errorf("Pattern = %s", sig.Pattern)
	}
	if sig.Metrics["z"] <= spikeCfg().SpikeZScore {
		t.Errorf("z = %f, want > threshold %f", sig.Metrics["z"], spikeCfg().SpikeZScore)
	}
	if sig.Strength <= 0 || sig.Strength > 1 {
		t.Errorf("Strength = %f, out of range", sig.Strength)
	}
	if err := sig.Validate(); err != nil {
		t.Errorf("invalid signal: %v", err)
	}
	if sig.Explanation.DetectorVersion != detectorVersionSpike {
		t.Errorf("explanation version = %q, want %s", sig.Explanation.DetectorVersion, detectorVersionSpike)
	}
	if len(sig.Explanation.FeatureEvolution) == 0 {
		t.Error("explanation feature_evolution is empty")
	}
}

func TestSpike_NoBaselineNoSignal(t *testing.T) {
	d := NewSpike(spikeCfg())
	scope := uuid.New()
	entity := uuid.New()
	now := time.Now()
	// Only three points — fewer than window+1.
	states := mkSeries(scope, entity, now, []float64{1, 2, 100})
	got := d.Detect(context.Background(), scope, states)
	if len(got) != 0 {
		t.Errorf("got %d signals, want 0 (insufficient baseline)", len(got))
	}
}

func TestSpike_NegativeDeviationNotASpike(t *testing.T) {
	// A drop should NOT trigger Spike.
	d := NewSpike(spikeCfg())
	scope := uuid.New()
	entity := uuid.New()
	now := time.Now()
	states := mkSeries(scope, entity, now, []float64{10, 10, 10, 10, 10, -10})
	got := d.Detect(context.Background(), scope, states)
	if len(got) != 0 {
		t.Errorf("Spike fired on a drop: %d signals", len(got))
	}
}

func TestDrop_NegativeDeviationEmits(t *testing.T) {
	d := NewDrop(spikeCfg())
	scope := uuid.New()
	entity := uuid.New()
	now := time.Now()
	states := mkSeries(scope, entity, now, []float64{100, 100.5, 99.5, 100, 100.2, 50})
	got := d.Detect(context.Background(), scope, states)
	if len(got) != 1 {
		t.Fatalf("got %d drops, want 1", len(got))
	}
	if got[0].Pattern != domain.PatternTypeDrop {
		t.Errorf("Pattern = %s", got[0].Pattern)
	}
	if got[0].Metrics["z"] >= 0 {
		t.Errorf("drop z should be negative, got %f", got[0].Metrics["z"])
	}
}

func TestSpike_ZeroVarianceSkipped(t *testing.T) {
	// Constant baseline → stddev=0 → cannot compute z; must not panic
	// and must not emit.
	d := NewSpike(spikeCfg())
	scope := uuid.New()
	entity := uuid.New()
	now := time.Now()
	states := mkSeries(scope, entity, now, []float64{5, 5, 5, 5, 5, 100})
	got := d.Detect(context.Background(), scope, states)
	if len(got) != 0 {
		t.Errorf("got %d signals on zero-variance baseline, want 0", len(got))
	}
}

// --- Confidence ------------------------------------------------------------
//
// Confidence answers "how good is the evidence", Strength answers "how
// big is the deviation". The tests below pin that they are separate
// measurements: confidence moves with history, baseline quietness and
// threshold margin, and stops moving with magnitude well before
// strength does.

// spikeConfidenceCfg is spikeCfg plus the shipped confidence-class
// multipliers. spikeCfg leaves them at zero, which disables the class
// thresholds the sample-support term saturates against.
func spikeConfidenceCfg() *config.Config {
	c := spikeCfg()
	c.ConfidenceClassEstablished = 2.0
	c.ConfidenceClassStrong = 5.0
	return c
}

// spikeSeriesEndingIn repeats a baseline shape until the series holds
// n-1 observations, then appends last.
func spikeSeriesEndingIn(shape []float64, n int, last float64) []float64 {
	ys := make([]float64, 0, n)
	for len(ys) < n-1 {
		ys = append(ys, shape[len(ys)%len(shape)])
	}
	return append(ys, last)
}

// detectOneSpike runs Spike over ys and requires exactly one signal.
func detectOneSpike(t *testing.T, cfg *config.Config, ys []float64) domain.Signal {
	t.Helper()
	scope := uuid.New()
	got := NewSpike(cfg).Detect(context.Background(), scope, mkSeries(scope, uuid.New(), time.Now(), ys))
	if len(got) != 1 {
		t.Fatalf("got %d signals over %v, want 1", len(got), ys)
	}
	return got[0]
}

// TestSpike_MinimumSamplesReportConfidenceBelowStrength is the
// regression for the contradiction this formula replaced: six
// observations — the fewest the detector accepts — used to report
// Confidence 1.0 while ConfidenceClass said "tentative".
func TestSpike_MinimumSamplesReportConfidenceBelowStrength(t *testing.T) {
	sig := detectOneSpike(t, spikeConfidenceCfg(), []float64{1, 1.1, 0.9, 1, 1.05, 900})

	if sig.Strength != 1 {
		t.Fatalf("Strength = %v, want 1: a 13000-sigma jump saturates the magnitude measure", sig.Strength)
	}
	if sig.ConfidenceClass != domain.ConfidenceClassTentative {
		t.Fatalf("ConfidenceClass = %q, want %q at the minimum sample count", sig.ConfidenceClass, domain.ConfidenceClassTentative)
	}
	if got, want := sig.Confidence, 0.193837; math.Abs(got-want) > 1e-6 {
		t.Errorf("Confidence = %v, want %v (support 6/30 × quietness 0.969 × margin 1.0)", got, want)
	}
	if sig.Confidence >= sig.Strength {
		t.Errorf("Confidence = %v, Strength = %v: on the thinnest admissible evidence confidence must be strictly lower", sig.Confidence, sig.Strength)
	}
}

// TestSpike_ConfidenceRisesWithHistory pins the sample-support term:
// the same deviation, against the same baseline shape, is better
// evidence when more of the series has been observed. Strength is
// identical across every case — the deviation did not change.
func TestSpike_ConfidenceRisesWithHistory(t *testing.T) {
	cfg := spikeConfidenceCfg()
	shape := []float64{1, 1.1, 0.9, 1, 1.05}
	cases := []struct {
		n     int
		want  float64
		class domain.ConfidenceClass
	}{
		{6, 0.193837, domain.ConfidenceClassTentative},
		{12, 0.387674, domain.ConfidenceClassEstablished},
		{24, 0.775349, domain.ConfidenceClassEstablished},
		{30, 0.969186, domain.ConfidenceClassStrong},
		// Past the strong boundary the term saturates: more history at
		// the same baseline does not sharpen the measurement further.
		{60, 0.969186, domain.ConfidenceClassStrong},
	}
	var prev float64
	for _, c := range cases {
		sig := detectOneSpike(t, cfg, spikeSeriesEndingIn(shape, c.n, 900))
		if sig.Strength != 1 {
			t.Errorf("n=%d: Strength = %v, want 1 — history must not move the magnitude measure", c.n, sig.Strength)
		}
		if math.Abs(sig.Confidence-c.want) > 1e-6 {
			t.Errorf("n=%d: Confidence = %v, want %v", c.n, sig.Confidence, c.want)
		}
		if sig.ConfidenceClass != c.class {
			t.Errorf("n=%d: ConfidenceClass = %q, want %q", c.n, sig.ConfidenceClass, c.class)
		}
		if c.n <= 30 && sig.Confidence <= prev {
			t.Errorf("n=%d: Confidence = %v did not rise above %v", c.n, sig.Confidence, prev)
		}
		prev = sig.Confidence
	}
}

// TestSpike_ConfidenceStopsTrackingMagnitude is the property that
// separates confidence from strength. Holding the baseline and the
// history fixed and growing only the deviation, strength climbs to its
// saturation point at z=5 while confidence is already flat at z=3.125
// — 25% past the 2.5 threshold.
func TestSpike_ConfidenceStopsTrackingMagnitude(t *testing.T) {
	cfg := spikeConfidenceCfg()
	base := []float64{10, 10.1, 9.9, 10.05, 9.95}
	m := mean(base)
	sd := stddev(base, m)

	atZ := func(z float64) domain.Signal {
		return detectOneSpike(t, cfg, append(append([]float64{}, base...), m+z*sd))
	}

	flat := atZ(3.125)
	for _, z := range []float64{4, 5, 40} {
		sig := atZ(z)
		if math.Abs(sig.Confidence-flat.Confidence) > 1e-9 {
			t.Errorf("z=%v: Confidence = %v, want the z=3.125 value %v — confidence must not track magnitude past the margin saturation point", z, sig.Confidence, flat.Confidence)
		}
		if sig.Strength <= flat.Strength && z < 5 {
			t.Errorf("z=%v: Strength = %v did not rise above the z=3.125 value %v", z, sig.Strength, flat.Strength)
		}
	}
	if got, want := flat.Confidence, 0.199298; math.Abs(got-want) > 1e-6 {
		t.Errorf("saturated Confidence = %v, want %v", got, want)
	}
}

// TestSpike_BoundaryCrossingIsLessConfident pins the margin term. A
// deviation sitting just over the trigger threshold is one a
// fractionally different baseline would not have produced at all, so
// it is weaker evidence than the same-shaped detection well clear of
// the boundary.
func TestSpike_BoundaryCrossingIsLessConfident(t *testing.T) {
	cfg := spikeConfidenceCfg()
	base := []float64{10, 10.1, 9.9, 10.05, 9.95}
	m := mean(base)
	sd := stddev(base, m)

	boundary := detectOneSpike(t, cfg, append(append([]float64{}, base...), m+2.6*sd))
	wellClear := detectOneSpike(t, cfg, append(append([]float64{}, base...), m+5*sd))

	if boundary.Confidence >= wellClear.Confidence {
		t.Fatalf("boundary Confidence = %v, wellClear = %v: a detection on the threshold must be the weaker evidence", boundary.Confidence, wellClear.Confidence)
	}
	// z=2.6 is 4% past the 2.5 threshold, i.e. 16% of the way to the
	// 25% saturation point: margin = 0.5 + 0.5×0.16 = 0.58.
	if got, want := boundary.Confidence/wellClear.Confidence, 0.58; math.Abs(got-want) > 1e-9 {
		t.Errorf("margin ratio = %v, want %v", got, want)
	}
}

// TestSpike_NoisyBaselineIsLessConfident pins the quietness term. Both
// series carry the same z, the same sample count and the same
// threshold margin; only the spread of the baseline relative to its
// own level differs.
func TestSpike_NoisyBaselineIsLessConfident(t *testing.T) {
	cfg := spikeConfidenceCfg()
	atSameZ := func(base []float64) domain.Signal {
		m := mean(base)
		sd := stddev(base, m)
		return detectOneSpike(t, cfg, append(append([]float64{}, base...), m+4*sd))
	}

	quiet := atSameZ([]float64{10, 10.1, 9.9, 10.05, 9.95})
	noisy := atSameZ([]float64{10, 20, 1, 16, 3})

	if math.Abs(quiet.Metrics["z"]-noisy.Metrics["z"]) > 1e-9 {
		t.Fatalf("z differs (%v vs %v); the comparison is only meaningful at equal z", quiet.Metrics["z"], noisy.Metrics["z"])
	}
	if math.Abs(quiet.Strength-noisy.Strength) > 1e-12 {
		t.Fatalf("Strength differs (%v vs %v) at equal z", quiet.Strength, noisy.Strength)
	}
	if noisy.Confidence >= quiet.Confidence {
		t.Errorf("noisy Confidence = %v, quiet = %v: the same deviation against a volatile baseline is weaker evidence", noisy.Confidence, quiet.Confidence)
	}
}

// TestSpike_ConfidenceNeverOutrunsTheClassItShipsWith is the
// consistency invariant the old formula broke. ConfidenceClass is a
// claim about how much history backs the signal; the number must not
// claim more certainty than that class permits.
func TestSpike_ConfidenceNeverOutrunsTheClassItShipsWith(t *testing.T) {
	cfg := spikeConfidenceCfg()
	shape := []float64{1, 1.1, 0.9, 1, 1.05}
	// The class is a claim about sample support, so it sets a ceiling
	// on the number, not a floor: quietness and margin can only pull
	// confidence further down. Tentative means the series has fewer
	// than ESTABLISHED×minPoints observations, so support — and
	// therefore confidence — cannot exceed ESTABLISHED/STRONG.
	tentativeCeiling := cfg.ConfidenceClassEstablished / cfg.ConfidenceClassStrong
	var sawStrong bool
	for n := 6; n <= 45; n++ {
		sig := detectOneSpike(t, cfg, spikeSeriesEndingIn(shape, n, 900))
		switch sig.ConfidenceClass {
		case domain.ConfidenceClassTentative:
			if sig.Confidence >= tentativeCeiling {
				t.Errorf("n=%d: Confidence = %v but the class is tentative, which caps it at %v", n, sig.Confidence, tentativeCeiling)
			}
		case domain.ConfidenceClassEstablished:
			if sig.Confidence >= 1 {
				t.Errorf("n=%d: Confidence = %v but only a strong signal may approach 1", n, sig.Confidence)
			}
		case domain.ConfidenceClassStrong:
			sawStrong = true
			if sig.Confidence <= 0.9 {
				t.Errorf("n=%d: Confidence = %v for a strong signal against a quiet baseline", n, sig.Confidence)
			}
		default:
			t.Errorf("n=%d: unexpected ConfidenceClass %q", n, sig.ConfidenceClass)
		}
		if sig.Confidence < 0 || sig.Confidence > 1 || math.IsNaN(sig.Confidence) {
			t.Fatalf("n=%d: Confidence = %v out of range", n, sig.Confidence)
		}
	}
	if !sawStrong {
		t.Error("no strong signal in the sweep; the invariant was never exercised at the top of the range")
	}
}

// TestDrop_ConfidenceMirrorsSpike proves the two detectors share one
// confidence definition: a mirrored series is the same evidence.
func TestDrop_ConfidenceMirrorsSpike(t *testing.T) {
	cfg := spikeConfidenceCfg()
	ys := []float64{1, 1.1, 0.9, 1, 1.05, 900}
	mirrored := make([]float64, len(ys))
	for i, y := range ys {
		mirrored[i] = -y
	}

	scope := uuid.New()
	spike := detectOneSpike(t, cfg, ys)
	drops := NewDrop(cfg).Detect(context.Background(), scope, mkSeries(scope, uuid.New(), time.Now(), mirrored))
	if len(drops) != 1 {
		t.Fatalf("got %d drops, want 1", len(drops))
	}
	if math.Abs(spike.Confidence-drops[0].Confidence) > 1e-12 {
		t.Errorf("spike Confidence = %v, mirrored drop = %v", spike.Confidence, drops[0].Confidence)
	}
	// Pin the value on the drop path too, so the shared formula cannot
	// be collapsed back into Strength on one detector and not the other.
	if got, want := drops[0].Confidence, 0.193837; math.Abs(got-want) > 1e-6 {
		t.Errorf("drop Confidence = %v, want %v at the minimum sample count", got, want)
	}
	if drops[0].Confidence >= drops[0].Strength {
		t.Errorf("drop Confidence = %v, Strength = %v: they must stay distinct", drops[0].Confidence, drops[0].Strength)
	}
}

// TestFiniteConfidence_MapsNonRealToZero covers the guard that clamp01
// cannot provide. domain.Signal.Validate range-checks with
// `< 0 || > 1`; both comparisons are false for NaN, so a NaN
// confidence would validate cleanly and reach the wire.
func TestFiniteConfidence_MapsNonRealToZero(t *testing.T) {
	t.Parallel()
	for _, c := range []struct{ in, want float64 }{
		{math.NaN(), 0},
		{math.Inf(1), 0},
		{math.Inf(-1), 0},
		{-0.5, 0},
		{1.5, 1},
		{0.25, 0.25},
	} {
		if got := finiteConfidence(c.in); got != c.want {
			t.Errorf("finiteConfidence(%v) = %v, want %v", c.in, got, c.want)
		}
	}
	if got := clamp01(math.NaN()); !math.IsNaN(got) {
		t.Errorf("clamp01(NaN) = %v: this test's premise is that clamp01 passes NaN through", got)
	}
}

// TestBaselineConfidence_DegenerateInputs covers the arguments the
// detector cannot produce but the function must still answer for,
// including the fallback when no confidence-class thresholds are set.
func TestBaselineConfidence_DegenerateInputs(t *testing.T) {
	t.Parallel()
	cfg := spikeConfidenceCfg()
	if got := baselineConfidence(0, 6, 1, 0.1, 10, 2.5, cfg); got != 0 {
		t.Errorf("n=0 gave %v, want 0", got)
	}
	if got := baselineConfidence(6, 0, 1, 0.1, 10, 2.5, cfg); got != 0 {
		t.Errorf("minPoints=0 gave %v, want 0", got)
	}
	// nil cfg and both thresholds disabled fall back to 2×MIN_POINTS.
	want := baselineConfidence(6, 6, 1, 0.1, 10, 2.5, &config.Config{})
	if got := baselineConfidence(6, 6, 1, 0.1, 10, 2.5, nil); got != want {
		t.Errorf("nil cfg gave %v, want the disabled-threshold fallback %v", got, want)
	}
	// A zero threshold admits every crossing, so there is no margin to
	// measure and the term must be neutral rather than a division by
	// zero.
	got := baselineConfidence(30, 6, 1, 0.1, 10, 0, cfg)
	if !isFinite(got) || got <= 0 {
		t.Errorf("threshold=0 gave %v, want a positive real number", got)
	}
	if want := baselineConfidence(30, 6, 1, 0.1, 10, 2.5, cfg); math.Abs(got-want) > 1e-12 {
		t.Errorf("threshold=0 gave %v, want the same value as a fully-cleared margin %v", got, want)
	}
}
