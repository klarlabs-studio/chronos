package detect

import (
	"bytes"
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/felixgeelhaar/chronos"
	"github.com/felixgeelhaar/chronos/internal/config"
	"github.com/felixgeelhaar/chronos/internal/domain"
	"github.com/felixgeelhaar/chronos/internal/observability"
	"github.com/google/uuid"
)

// TestEngine_FanOutAcrossDetectors exercises the engine with the full
// default detector set against a synthetic dataset crafted to trigger
// at least three orthogonal pattern types in a single Detect call.
//
// The point is not to validate detector internals (that's done in each
// detector's own _test.go file) but to confirm the engine wires the
// detectors together correctly and the resulting signals carry the
// expected PatternType tags.
func TestEngine_FanOutAcrossDetectors(t *testing.T) {
	cfg := &config.Config{
		MaxSignalsPerRun: 100,

		// Recurrence: tight similarity to keep peer matches focused.
		SimilarityThreshold: 0.95,
		MinSampleSize:       2,

		// Trend: easy threshold for the rising series.
		TrendMinSlope:  0.05,
		TrendMinPoints: 4,

		// Spike/Drop: rolling baseline of 4 points, z>=2.5.
		SpikeZScore: 2.5,
		DropZScore:  2.5,
		SpikeWindow: 4,

		// Stall: very tight stddev window so a flat-ish series qualifies.
		StallMaxStdDev: 0.01,
		StallMinPoints: 4,

		// Anomaly: leave it disabled (max sim 1.0 = anything counts as
		// "not isolated") to keep this test focused.
		AnomalyMaxSimilarity: 1.0,
		AnomalyMinPeers:      2,

		// Seasonality: high bar, more points than we provide here.
		SeasonalityMinAutocorr: 0.99,
		SeasonalityMinPoints:   100,
		SeasonalityMinPeriod:   2,

		// Correlation: high bar so it doesn't fire incidentally.
		CorrelationMin:       0.99,
		CorrelationMinPoints: 100,
	}
	e := NewEngine(cfg)

	scope := uuid.New()
	rising := uuid.New()
	flat := uuid.New()
	spiker := uuid.New()
	now := time.Now()

	var states []chronos.EntityState

	// Rising series — should trigger Trend.
	for i := 0; i < 8; i++ {
		states = append(states, chronos.EntityState{
			ID: uuid.New(), EntityID: rising, ScopeID: scope,
			Timestamp: now.Add(time.Duration(i) * time.Hour),
			Features:  []float64{1, 2, 3, float64(i + 1)},
		})
	}

	// Flat series — should trigger Stall (relative stddev tiny).
	for i := 0; i < 6; i++ {
		states = append(states, chronos.EntityState{
			ID: uuid.New(), EntityID: flat, ScopeID: scope,
			Timestamp: now.Add(time.Duration(i) * time.Hour),
			Features:  []float64{1, 2, 3, 100.0 + float64(i)*0.0001},
		})
	}

	// Spiker series — flat baseline followed by an outlier; should
	// trigger Spike.
	for i := 0; i < 5; i++ {
		states = append(states, chronos.EntityState{
			ID: uuid.New(), EntityID: spiker, ScopeID: scope,
			Timestamp: now.Add(time.Duration(i) * time.Hour),
			Features:  []float64{1, 2, 3, 10.0 + float64(i)*0.01},
		})
	}
	states = append(states, chronos.EntityState{
		ID: uuid.New(), EntityID: spiker, ScopeID: scope,
		Timestamp: now.Add(5 * time.Hour),
		Features:  []float64{1, 2, 3, 50.0},
	})

	signals := e.Detect(context.Background(), states)
	if len(signals) == 0 {
		t.Fatal("no signals emitted")
	}

	patterns := make(map[domain.PatternType]bool)
	bySeries := make(map[domain.PatternType][]uuid.UUID)
	for _, s := range signals {
		patterns[s.Pattern] = true
		bySeries[s.Pattern] = append(bySeries[s.Pattern], s.Series)
	}

	if !patterns[domain.PatternTypeTrend] {
		t.Errorf("Trend not emitted (signals: %v)", patterns)
	} else {
		// Trend should be on the rising series specifically.
		seenRising := false
		for _, id := range bySeries[domain.PatternTypeTrend] {
			if id == rising {
				seenRising = true
			}
		}
		if !seenRising {
			t.Errorf("Trend did not fire on rising series; saw %v", bySeries[domain.PatternTypeTrend])
		}
	}
	if !patterns[domain.PatternTypeStall] {
		t.Errorf("Stall not emitted")
	}
	if !patterns[domain.PatternTypeSpike] {
		t.Errorf("Spike not emitted")
	}
}

// TestEngine_RespectsMaxSignalsCap ensures the global cap is applied
// across detectors, not just within one detector.
func TestEngine_RespectsMaxSignalsCap(t *testing.T) {
	cfg := &config.Config{
		MaxSignalsPerRun: 1,

		SimilarityThreshold: 0.5,
		MinSampleSize:       2,

		TrendMinSlope:  0.05,
		TrendMinPoints: 4,

		// Disable detectors that need lots of points or peers to keep
		// the test focused on the cap.
		SpikeWindow:            10,
		StallMinPoints:         100,
		AnomalyMaxSimilarity:   -2,
		AnomalyMinPeers:        100,
		SeasonalityMinPoints:   100,
		SeasonalityMinAutocorr: 2,
		CorrelationMinPoints:   100,
		CorrelationMin:         2,
	}
	e := NewEngine(cfg)

	scope := uuid.New()
	now := time.Now()
	var states []chronos.EntityState
	// Two rising series — Trend should match both, Recurrence may also
	// fire. We expect MaxSignalsPerRun=1 to cap the output to one.
	for _, ent := range []uuid.UUID{uuid.New(), uuid.New()} {
		for i := 0; i < 8; i++ {
			states = append(states, chronos.EntityState{
				ID: uuid.New(), EntityID: ent, ScopeID: scope,
				Timestamp: now.Add(time.Duration(i) * time.Hour),
				Features:  []float64{1, 2, 3, float64(i + 1)},
			})
		}
	}

	got := e.Detect(context.Background(), states)
	if len(got) != 1 {
		t.Errorf("MaxSignalsPerRun=1 returned %d signals", len(got))
	}
}

func TestEngine_PerceptionIDStableAcrossRuns(t *testing.T) {
	cfg := trendCfg()
	e := NewEngine(cfg, NewTrend(cfg)).WithCrossScopeDetectors(nil)
	scope := uuid.New()
	entity := uuid.New()
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	states := mkSeries(scope, entity, now, []float64{1, 2, 3, 4, 5, 6})

	a := e.Detect(context.Background(), states)
	b := e.Detect(context.Background(), states)
	if len(a) == 0 || len(b) == 0 {
		t.Fatal("expected trend signals")
	}
	byID := map[uuid.UUID]struct{}{}
	for _, s := range a {
		byID[s.ID] = struct{}{}
	}
	for _, s := range b {
		if _, ok := byID[s.ID]; !ok {
			t.Errorf("second detect minted a new id %s for pattern %s", s.ID, s.Pattern)
		}
	}
}

func TestEngine_RecordsDetectorMetrics(t *testing.T) {
	cfg := &config.Config{MaxSignalsPerRun: 1, TrendMinSlope: 0.05, TrendMinPoints: 4}
	m := observability.New()
	e := NewEngine(cfg, NewTrend(cfg)).WithCrossScopeDetectors(nil).WithMetrics(m)
	scope := uuid.New()
	now := time.Now()
	var states []chronos.EntityState
	for _, ent := range []uuid.UUID{uuid.New(), uuid.New()} {
		states = append(states, mkSeries(scope, ent, now, []float64{1, 2, 3, 4, 5, 6})...)
	}
	got := e.Detect(context.Background(), states)
	if len(got) != 1 {
		t.Fatalf("capped len = %d, want 1", len(got))
	}
	var buf bytes.Buffer
	if err := m.Render(&buf); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if !strings.Contains(out, `chronos_detector_signals_total{pattern="trend"}`) {
		t.Errorf("missing detector signals metric:\n%s", out)
	}
	if !strings.Contains(out, `chronos_signals_truncated_total{pattern="trend"}`) {
		t.Errorf("missing truncation metric:\n%s", out)
	}
}

// unvalidatedDetector emits whatever it is handed, valid or not. It
// stands in for an out-of-tree detector: Detector is an interface, so
// the engine cannot assume every implementation honours the contract
// its doc comment states.
type unvalidatedDetector struct {
	pattern domain.PatternType
	emit    []domain.Signal
}

func (d unvalidatedDetector) Pattern() domain.PatternType { return d.pattern }

func (d unvalidatedDetector) Detect(context.Context, uuid.UUID, []chronos.EntityState) []domain.Signal {
	return d.emit
}

// TestEngine_DropsSignalsThatFailValidate pins the engine's last gate
// before the pipeline persists and the API serves.
//
// The in-tree detectors filter their own output, so this needs a
// detector that does not. The engine drops the invalid signal and
// keeps the valid one: it does not repair the invalid one, because it
// has no basis on which to invent the quantity the detector failed to
// measure, and it does not pass it on, because the next stop is a
// store whose Save would reject it and a JSON encoder that cannot
// represent it.
func TestEngine_DropsSignalsThatFailValidate(t *testing.T) {
	cfg := &config.Config{MaxSignalsPerRun: 100}
	scope := uuid.New()
	series := uuid.New()
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)

	mk := func(metrics map[string]float64) domain.Signal {
		return domain.Signal{
			ScopeID:    scope,
			Series:     series,
			Pattern:    domain.PatternTypeStall,
			DetectedAt: now,
			Window:     domain.TimeWindow{Start: now.Add(-time.Hour), End: now},
			Strength:   0.9,
			Confidence: 0.8,
			Metrics:    metrics,
		}
	}
	good := mk(map[string]float64{"mean": 4})
	bad := mk(map[string]float64{"mean": math.Inf(1)})
	if err := bad.Validate(); err == nil {
		t.Fatal("the fixture meant to be invalid validates; the test proves nothing")
	}

	e := NewEngine(cfg, unvalidatedDetector{pattern: domain.PatternTypeStall, emit: []domain.Signal{good, bad}}).
		WithCrossScopeDetectors(nil)
	got := e.Detect(context.Background(), mkSeries(scope, series, now, []float64{1, 2, 3, 4}))

	if len(got) != 1 {
		t.Fatalf("got %d signals, want 1 — the invalid one should have been dropped", len(got))
	}
	if err := got[0].Validate(); err != nil {
		t.Errorf("surviving signal does not validate: %v", err)
	}
	if math.IsInf(got[0].Metrics["mean"], 0) {
		t.Error("the engine kept the signal with the infinite metric and dropped the finite one")
	}
}
