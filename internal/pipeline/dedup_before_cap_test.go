package pipeline

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/felixgeelhaar/chronos"
	"github.com/felixgeelhaar/chronos/internal/config"
	"github.com/felixgeelhaar/chronos/internal/detect"
	"github.com/felixgeelhaar/chronos/internal/domain"
	"github.com/felixgeelhaar/chronos/internal/ports"
	"github.com/felixgeelhaar/chronos/internal/store/memory"
	"github.com/google/uuid"
)

// fixedDetector re-emits the same set of signals on every Detect call,
// the way a real detector re-derives last hour's change point on every
// tick over a six-hour lookback: identical identity, identical window.
type fixedDetector struct {
	sigs func(scope uuid.UUID, states []chronos.EntityState) []domain.Signal
}

func (f fixedDetector) Pattern() domain.PatternType { return domain.PatternTypeChangePoint }

func (f fixedDetector) Detect(_ context.Context, scope uuid.UUID, states []chronos.EntityState) []domain.Signal {
	return f.sigs(scope, states)
}

func seriesID(name string) uuid.UUID { return uuid.NewSHA1(uuid.Nil, []byte(name)) }

func changePoint(scope uuid.UUID, states []chronos.EntityState, name string, conf float64) domain.Signal {
	return domain.Signal{
		ScopeID:    scope,
		Series:     seriesID(name),
		Pattern:    domain.PatternTypeChangePoint,
		DetectedAt: time.Now(),
		Window:     domain.TimeWindow{Start: states[0].Timestamp, End: states[len(states)-1].Timestamp},
		Strength:   0.5,
		Confidence: conf,
	}
}

// Production, 2026-09-26: after each restart chronos emitted one burst
// and then nothing, although observations kept arriving every five
// minutes. MaxSignalsPerRun is applied inside Engine.Detect, and the
// scheduler only drops already-persisted signals afterwards. When
// historical events that are already saved outrank new ones, they fill
// every slot on every tick, all of them are skipped as duplicates, and
// the new events are truncated before the duplicate check ever sees
// them. The cap was spending its budget on signals that could never be
// written.
func TestScheduler_NewSignalsAreNotStarvedByAlreadyPersistedOnes(t *testing.T) {
	cfg := config.Default()
	cfg.MaxSignalsPerRun = 3
	mem := memory.New()
	ctx := context.Background()

	scope, entity := uuid.New(), uuid.New()
	now := time.Now().Add(-10 * time.Minute)
	var states []chronos.EntityState
	for i := 0; i < 4; i++ {
		st := chronos.EntityState{
			ID: uuid.New(), EntityID: entity, ScopeID: scope,
			Timestamp: now.Add(time.Duration(i) * time.Minute),
			Features:  []float64{float64(i)},
		}
		if err := mem.EntityStates.Ingest(ctx, "test", st); err != nil {
			t.Fatalf("ingest: %v", err)
		}
		states = append(states, st)
	}

	old := make([]string, 5)
	for i := range old {
		old[i] = fmt.Sprintf("old-%d", i)
	}
	fresh := []string{"new-0", "new-1"}

	// Five confident historical events, already persisted.
	for _, name := range old {
		sig := changePoint(scope, states, name, 0.9)
		sig.ID = domain.PerceptionID(sig)
		if err := mem.Signals.Save(ctx, sig); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	det := fixedDetector{sigs: func(scope uuid.UUID, st []chronos.EntityState) []domain.Signal {
		var out []domain.Signal
		for _, name := range old {
			out = append(out, changePoint(scope, st, name, 0.9))
		}
		for _, name := range fresh {
			out = append(out, changePoint(scope, st, name, 0.5))
		}
		return out
	}}

	s := NewScheduler(mem.EntityStates, mem.Signals, detect.NewEngine(cfg, det), time.Minute, nil)
	s.tick(ctx)

	for _, name := range fresh {
		series := seriesID(name)
		n, err := mem.Signals.Count(ctx, ports.SignalFilter{ScopeID: scope, Series: &series})
		if err != nil {
			t.Fatalf("count: %v", err)
		}
		if n != 1 {
			t.Errorf("%s: persisted %d times, want 1 -- a new event was truncated by the cap in favour of events already saved", name, n)
		}
	}
	// And nothing already saved was written a second time.
	total, err := mem.Signals.Count(ctx, ports.SignalFilter{ScopeID: scope})
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if total != int64(len(old)+len(fresh)) {
		t.Errorf("store holds %d signals, want %d", total, len(old)+len(fresh))
	}
}

// The cap still bounds what one tick writes. Deduplicating first changes
// what the budget is spent on, not how large it is.
func TestScheduler_CapStillBoundsNovelSignalsPerTick(t *testing.T) {
	cfg := config.Default()
	cfg.MaxSignalsPerRun = 3
	mem := memory.New()
	ctx := context.Background()

	scope, entity := uuid.New(), uuid.New()
	now := time.Now().Add(-10 * time.Minute)
	for i := 0; i < 4; i++ {
		if err := mem.EntityStates.Ingest(ctx, "test", chronos.EntityState{
			ID: uuid.New(), EntityID: entity, ScopeID: scope,
			Timestamp: now.Add(time.Duration(i) * time.Minute),
			Features:  []float64{float64(i)},
		}); err != nil {
			t.Fatalf("ingest: %v", err)
		}
	}
	det := fixedDetector{sigs: func(scope uuid.UUID, st []chronos.EntityState) []domain.Signal {
		var out []domain.Signal
		for i := 0; i < 10; i++ {
			out = append(out, changePoint(scope, st, fmt.Sprintf("novel-%d", i), 0.7))
		}
		return out
	}}
	s := NewScheduler(mem.EntityStates, mem.Signals, detect.NewEngine(cfg, det), time.Minute, nil)
	s.tick(ctx)

	total, err := mem.Signals.Count(ctx, ports.SignalFilter{ScopeID: scope})
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if total != 3 {
		t.Errorf("one tick persisted %d novel signals, want the cap of 3", total)
	}
}
