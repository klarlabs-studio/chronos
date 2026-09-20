package detect

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/felixgeelhaar/chronos"
	"github.com/felixgeelhaar/chronos/internal/config"
	"github.com/felixgeelhaar/chronos/internal/domain"
	"github.com/google/uuid"
)

// Detectors are documented as pure functions of their input plus a
// configuration snapshot. Purity is not only about values: the order
// of the returned slice is part of the output, because the engine's
// MaxSignalsPerRun cap truncates the tail and its final sort is
// stable, so ties keep whatever order the detector produced.
//
// Every detector that emits one signal per series or per entity used
// to range over a map to find them, which meant Go's randomised map
// iteration decided that order. The tests below run each detector
// many times over one fixed input and require byte-identical
// sequences.

// detRuns is how many repeats each determinism test performs. Go
// randomises map iteration per range statement, so a single repeat
// proves nothing; 200 makes an ordering leak over eight series
// essentially certain to show (measured: an unguarded Trend differed
// from its first run in 175 of 200 repeats).
const detRuns = 200

// detFingerprint renders the ordered, ID-independent content of a
// signal slice. Signal.ID is a fresh uuid.New() at detector level —
// the engine replaces it with a content-addressed PerceptionID — so
// it is excluded deliberately; everything else is compared.
func detFingerprint(sigs []domain.Signal) string {
	var b strings.Builder
	for _, s := range sigs {
		fmt.Fprintf(&b, "%s|%s|%s|%.17g|%.17g|%s|%d|%d|",
			s.ScopeID, s.Series, s.Pattern, s.Strength, s.Confidence,
			s.ConfidenceClass, s.Window.Start.UnixNano(), s.Window.End.UnixNano())
		for _, e := range s.Evidence {
			fmt.Fprintf(&b, "%s:%s:%.17g:%d,", e.Series, e.Kind, e.Score, e.Time.UnixNano())
		}
		b.WriteString(";")
	}
	return b.String()
}

// detCohort builds n series in one scope carrying ys, at a fixed
// cadence, with freshly minted entity IDs so their map ordering is
// not accidentally stable.
func detCohort(scope uuid.UUID, n int, ys []float64) []chronos.EntityState {
	var out []chronos.EntityState
	for i := 0; i < n; i++ {
		out = append(out, advSeries(scope, uuid.New(), time.Minute, ys)...)
	}
	return out
}

func TestDeterminism_PerSeriesDetectorsEmitInStableOrder(t *testing.T) {
	scope := uuid.New()

	tests := []struct {
		name   string
		newDet func(*config.Config) Detector
		states []chronos.EntityState
		want   int
	}{
		{"trend", func(c *config.Config) Detector { return NewTrend(c) },
			detCohort(scope, 8, advRamp(12, 1, 1)), 8},
		{"spike", func(c *config.Config) Detector { return NewSpike(c) },
			detCohort(scope, 8, []float64{1, 1.1, 0.9, 1, 1.05, 900}), 8},
		{"drop", func(c *config.Config) Detector { return NewDrop(c) },
			detCohort(scope, 8, []float64{1, 1.1, 0.9, 1, 1.05, -900}), 8},
		{"stall", func(c *config.Config) Detector { return NewStall(c) },
			detCohort(scope, 8, advConst(7, 12)), 8},
		{"seasonality", func(c *config.Config) Detector { return NewSeasonality(c) },
			detCohort(scope, 8, []float64{1, 5, 1, 5, 1, 5, 1, 5, 1, 5, 1, 5}), 8},
		{"change_point", func(c *config.Config) Detector { return NewChangePoint(c) },
			detCohort(scope, 8, []float64{1, 1.01, 0.99, 1, 9, 9.01, 8.99, 9}), 8},
		{"correlation", func(c *config.Config) Detector { return NewCorrelation(c) },
			detCohort(scope, 6, advRamp(12, 1, 1)), 15}, // 6 choose 2
		{"outlier_cluster", func(c *config.Config) Detector { return NewOutlierCluster(c) },
			detOutlierMultiBucket(scope, 5), 2},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := tc.newDet(advCfg())
			first := d.Detect(context.Background(), scope, tc.states)
			if len(first) != tc.want {
				t.Fatalf("got %d signals, want %d — the fixture must produce several so ordering is observable", len(first), tc.want)
			}
			want := detFingerprint(first)
			for i := 0; i < detRuns; i++ {
				if got := detFingerprint(d.Detect(context.Background(), scope, tc.states)); got != want {
					t.Fatalf("run %d produced a different signal sequence than run 0", i+1)
				}
			}
		})
	}
}

// detOutlierMultiBucket builds a cohort with two separate anomaly
// clusters an hour apart, so the detector produces two signals from
// two time buckets and their relative order is observable.
func detOutlierMultiBucket(scope uuid.UUID, n int) []chronos.EntityState {
	var out []chronos.EntityState
	for i := 0; i < n; i++ {
		entity := uuid.New()
		first := advSeries(scope, entity, time.Minute, []float64{1, 1.1, 0.9, 1, 1.05, 900, 1, 1})
		second := advSeries(scope, entity, time.Minute, []float64{1, 1.1, 0.9, 1, 1.05, 700, 1, 1})
		for j := range second {
			second[j].Timestamp = second[j].Timestamp.Add(time.Hour)
		}
		out = append(out, first...)
		out = append(out, second...)
	}
	return out
}

// detStates lays one observation per entity out in time order,
// minting a fresh entity ID for each so their map ordering is not
// accidentally stable.
func detStates(scope uuid.UUID, vectors [][]float64) []chronos.EntityState {
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

func TestDeterminism_PeerDetectorsEmitInStableOrder(t *testing.T) {
	scope := uuid.New()

	// Anomaly: six mutually orthogonal one-hot entities, so every one
	// of them is isolated from every other and all six emit.
	orthogonal := make([][]float64, 6)
	for i := range orthogonal {
		orthogonal[i] = make([]float64, 6)
		orthogonal[i][i] = 1
	}
	// Recurrence: eight entities in the same state, observed in
	// sequence. Each entity after the second has at least
	// MinSampleSize strictly-earlier peers, so six emit.
	identical := make([][]float64, 8)
	for i := range identical {
		identical[i] = []float64{1, 2, 3}
	}

	for _, tc := range []struct {
		name   string
		newDet func(*config.Config) Detector
		states []chronos.EntityState
		want   int
	}{
		{"anomaly", func(c *config.Config) Detector { return NewAnomaly(c) }, detStates(scope, orthogonal), 6},
		{"recurrence", func(c *config.Config) Detector { return NewRecurrence(c) }, detStates(scope, identical), 6},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := tc.newDet(advCfg())
			first := d.Detect(context.Background(), scope, tc.states)
			if len(first) != tc.want {
				t.Fatalf("got %d signals, want %d — the fixture must produce several so ordering is observable", len(first), tc.want)
			}
			want := detFingerprint(first)
			for i := 0; i < detRuns; i++ {
				if got := detFingerprint(d.Detect(context.Background(), scope, tc.states)); got != want {
					t.Fatalf("run %d produced a different signal sequence than run 0", i+1)
				}
			}
		})
	}
}

// TestDeterminism_EngineTruncatesTheSameTailEveryRun is the reason
// detector ordering matters. MaxSignalsPerRun drops everything past
// the cap, and the engine's final sort is stable, so signals that tie
// on DetectedAt and Confidence keep the order their detector emitted
// them in. If that order is a map's, the surviving set is random.
func TestDeterminism_EngineTruncatesTheSameTailEveryRun(t *testing.T) {
	scope := uuid.New()
	cfg := advCfg()
	cfg.MaxSignalsPerRun = 5
	// Sixteen identical stalled series: every signal carries the same
	// strength and confidence, so the cap can only be resolved by
	// emission order.
	states := detCohort(scope, 16, advConst(7, 12))

	fixed := advBase.Add(72 * time.Hour)
	stall := NewStall(cfg)
	stall.now = func() time.Time { return fixed }
	engine := NewEngine(cfg, stall).WithCrossScopeDetectors(nil)

	first := engine.Detect(context.Background(), states)
	if len(first) != 5 {
		t.Fatalf("got %d signals, want the cap of 5", len(first))
	}
	want := detFingerprint(first)
	for i := 0; i < detRuns; i++ {
		if got := detFingerprint(engine.Detect(context.Background(), states)); got != want {
			t.Fatalf("run %d kept a different set of signals past the cap than run 0", i+1)
		}
	}
}

// TestDeterminism_EngineVisitsScopesInStableOrder covers the engine's
// own map: Detect groups states by scope, and ranging that group
// directly put the scopes in a different order on every run.
func TestDeterminism_EngineVisitsScopesInStableOrder(t *testing.T) {
	cfg := advCfg()
	var states []chronos.EntityState
	for i := 0; i < 6; i++ {
		states = append(states, detCohort(uuid.New(), 2, advConst(7, 12))...)
	}

	fixed := advBase.Add(72 * time.Hour)
	stall := NewStall(cfg)
	stall.now = func() time.Time { return fixed }
	engine := NewEngine(cfg, stall).WithCrossScopeDetectors(nil)

	first := engine.Detect(context.Background(), states)
	if len(first) != 12 {
		t.Fatalf("got %d signals, want 12 (six scopes of two series)", len(first))
	}
	want := detFingerprint(first)
	for i := 0; i < detRuns; i++ {
		if got := detFingerprint(engine.Detect(context.Background(), states)); got != want {
			t.Fatalf("run %d visited the scopes in a different order than run 0", i+1)
		}
	}
}

// TestDeterminism_ParallelEngineMatchesSequential checks that turning
// on WithParallelDetectors does not change the output. The engine
// documents that the merge happens before the sort so the two paths
// agree; goroutine scheduling must not leak into the result.
func TestDeterminism_ParallelEngineMatchesSequential(t *testing.T) {
	scope := uuid.New()
	cfg := advCfg()
	states := detCohort(scope, 8, advConst(7, 12))
	states = append(states, detCohort(uuid.New(), 8, advRamp(12, 1, 1))...)

	fixed := advBase.Add(72 * time.Hour)
	build := func(parallel bool) *Engine {
		stall := NewStall(cfg)
		stall.now = func() time.Time { return fixed }
		trend := NewTrend(cfg)
		trend.now = func() time.Time { return fixed }
		return NewEngine(cfg, stall, trend).WithCrossScopeDetectors(nil).WithParallelDetectors(parallel)
	}

	sequential := detFingerprint(build(false).Detect(context.Background(), states))
	for i := 0; i < 50; i++ {
		if got := detFingerprint(build(true).Detect(context.Background(), states)); got != sequential {
			t.Fatalf("parallel run %d differs from the sequential result", i+1)
		}
	}
}

// TestDeterminism_CrossScopeCorrelationStableOrder covers the one
// cross-scope detector. It already sorted its (scope, series) keys;
// this pins that it keeps doing so.
func TestDeterminism_CrossScopeCorrelationStableOrder(t *testing.T) {
	var states []chronos.EntityState
	for i := 0; i < 5; i++ {
		states = append(states, advSeries(uuid.New(), uuid.New(), time.Minute, advRamp(12, 1, 1))...)
	}
	d := NewCrossScopeCorrelation(advCfg())
	first := d.CrossDetect(context.Background(), states)
	if len(first) != 10 { // 5 choose 2
		t.Fatalf("got %d signals, want 10", len(first))
	}
	want := detFingerprint(first)
	for i := 0; i < detRuns; i++ {
		if got := detFingerprint(d.CrossDetect(context.Background(), states)); got != want {
			t.Fatalf("run %d produced a different signal sequence than run 0", i+1)
		}
	}
}
