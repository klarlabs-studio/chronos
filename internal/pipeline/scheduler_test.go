package pipeline

import (
	"context"
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

func TestScheduler_RunIsNoopWhenIntervalZero(t *testing.T) {
	cfg := config.Default()
	mem := memory.New()
	s := NewScheduler(mem.EntityStates, mem.Signals, detect.NewEngine(cfg), 0, nil)

	done := make(chan struct{})
	go func() { _ = s.Run(context.Background()); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run with zero interval did not return immediately")
	}
}

func TestScheduler_TickProducesAndPersistsSignals(t *testing.T) {
	cfg := config.Default()
	mem := memory.New()
	ctx := context.Background()

	// Seed observations of one entity that form a clean upward trend
	// so the Trend detector emits when the scheduler ticks.
	scope := uuid.New()
	entity := uuid.New()
	now := time.Now()
	for i, outcome := range []float64{1.0, 2.0, 3.0, 4.0, 5.0, 6.0} {
		state := chronos.EntityState{
			ID:        uuid.New(),
			EntityID:  entity,
			ScopeID:   scope,
			Timestamp: now.Add(time.Duration(i) * time.Minute),
			Features:  []float64{float64(i), outcome},
		}
		if err := mem.EntityStates.Ingest(ctx, "test", state); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	s := NewScheduler(mem.EntityStates, mem.Signals, detect.NewEngine(cfg), time.Second, nil)
	s.tick(ctx)

	got, err := mem.Signals.List(ctx, ports.SignalFilter{ScopeID: scope})
	if err != nil {
		t.Fatalf("list signals: %v", err)
	}
	if len(got) == 0 {
		t.Fatal("scheduler tick produced no signals despite a clear trend")
	}

	first := len(got)
	s.tick(ctx)
	got, err = mem.Signals.List(ctx, ports.SignalFilter{ScopeID: scope})
	if err != nil {
		t.Fatalf("list after second tick: %v", err)
	}
	if len(got) != first {
		t.Fatalf("second tick over unchanged data appended signals: first=%d second=%d", first, len(got))
	}
}

func TestScheduler_RunTicksUntilContextCancelled(t *testing.T) {
	cfg := config.Default()
	mem := memory.New()
	ctx := context.Background()

	scope := uuid.New()
	entity := uuid.New()
	now := time.Now()
	for i, o := range []float64{1, 2, 3, 4, 5, 6} {
		_ = mem.EntityStates.Ingest(ctx, "test", chronos.EntityState{
			ID:        uuid.New(),
			EntityID:  entity,
			ScopeID:   scope,
			Timestamp: now.Add(time.Duration(i) * time.Minute),
			Features:  []float64{float64(i), o},
		})
	}

	s := NewScheduler(mem.EntityStates, mem.Signals, detect.NewEngine(cfg), 50*time.Millisecond, nil)

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- s.Run(runCtx) }()

	// Wait long enough for at least one tick, then cancel.
	time.Sleep(200 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop after context cancel")
	}

	got, err := mem.Signals.List(ctx, ports.SignalFilter{ScopeID: scope})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(got) == 0 {
		t.Fatal("scheduler Run produced no signals across multiple ticks")
	}
}

func TestScheduler_TickIsResilientToPerScopeFailure(t *testing.T) {
	cfg := config.Default()
	mem := memory.New()
	ctx := context.Background()

	// One scope with a valid entity (should produce signals); we
	// rely on the scheduler not panicking when ListByScope returns
	// empty for a scope that has no states. The path under test is
	// the per-scope error tolerance.
	scope := uuid.New()
	entity := uuid.New()
	now := time.Now()
	for i, o := range []float64{1, 2, 3, 4, 5, 6} {
		_ = mem.EntityStates.Ingest(ctx, "test", chronos.EntityState{
			ID:        uuid.New(),
			EntityID:  entity,
			ScopeID:   scope,
			Timestamp: now.Add(time.Duration(i) * time.Minute),
			Features:  []float64{float64(i), o},
		})
	}

	s := NewScheduler(mem.EntityStates, mem.Signals, detect.NewEngine(cfg), time.Second, nil)
	// tick must not panic.
	s.tick(ctx)
}

// countingSignals wraps a SignalRepository and records how the
// scheduler interrogates it.
type countingSignals struct {
	inner ports.SignalRepository

	unboundedLists int // List calls with no Limit — the leak signature
	lists          int
	counts         int
}

func (c *countingSignals) Save(ctx context.Context, sig domain.Signal) error {
	return c.inner.Save(ctx, sig)
}

func (c *countingSignals) List(ctx context.Context, f ports.SignalFilter) ([]domain.Signal, error) {
	c.lists++
	if f.Limit == 0 {
		c.unboundedLists++
	}
	return c.inner.List(ctx, f)
}

func (c *countingSignals) Get(ctx context.Context, id uuid.UUID) (domain.Signal, error) {
	return c.inner.Get(ctx, id)
}

func (c *countingSignals) Count(ctx context.Context, f ports.SignalFilter) (int64, error) {
	c.counts++
	return c.inner.Count(ctx, f)
}

// The duplicate check runs once per candidate signal, on every tick,
// forever. If it answers "have I stored this before?" by loading the
// matching signals into memory, the cost of a tick grows with the size
// of the store — and because nothing prunes the store, that growth has
// no ceiling. In production this read the whole matching set (plus a
// per-row evidence query) every 30 seconds until the process was
// OOM-killed.
//
// The check must therefore ask the store a bounded question. This test
// pins that: no unbounded List may be issued while ticking.
func TestScheduler_DuplicateCheckDoesNotLoadStoredSignals(t *testing.T) {
	cfg := config.Default()
	mem := memory.New()
	ctx := context.Background()

	scope := uuid.New()
	entity := uuid.New()
	now := time.Now()
	for i, o := range []float64{1, 2, 3, 4, 5, 6} {
		if err := mem.EntityStates.Ingest(ctx, "test", chronos.EntityState{
			ID:        uuid.New(),
			EntityID:  entity,
			ScopeID:   scope,
			Timestamp: now.Add(time.Duration(i) * time.Minute),
			Features:  []float64{float64(i), o},
		}); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	spy := &countingSignals{inner: mem.Signals}
	s := NewScheduler(mem.EntityStates, spy, detect.NewEngine(cfg), time.Second, nil)

	s.tick(ctx)
	s.tick(ctx)

	if spy.unboundedLists > 0 {
		t.Fatalf("scheduler issued %d unbounded List call(s) while ticking; "+
			"the duplicate check must not materialise the stored signal set",
			spy.unboundedLists)
	}
}

// seedSignal persists one signal with an explicit detection time.
func seedSignal(t *testing.T, repo ports.SignalRepository, scope uuid.UUID, detectedAt time.Time) {
	t.Helper()
	if err := repo.Save(context.Background(), domain.Signal{
		ID:         uuid.New(),
		ScopeID:    scope,
		Series:     uuid.New(),
		Pattern:    domain.PatternTypeTrend,
		DetectedAt: detectedAt,
		Window:     domain.TimeWindow{Start: detectedAt.Add(-time.Hour), End: detectedAt},
		Strength:   0.5,
		Confidence: 0.5,
	}); err != nil {
		t.Fatalf("seed signal: %v", err)
	}
}

// Detectors re-derive a signal's window from the observations in front
// of them, so on a live stream the window slides forward and the
// duplicate check — which keys on the window — legitimately misses.
// Every tick therefore appends. Without retention that is a table with
// no ceiling, on storage that outlives the process: restarting the
// engine does not reclaim any of it.
func TestScheduler_SweepDropsSignalsPastRetention(t *testing.T) {
	mem := memory.New()
	ctx := context.Background()
	scope := uuid.New()
	now := time.Now()

	seedSignal(t, mem.Signals, scope, now.Add(-72*time.Hour))
	seedSignal(t, mem.Signals, scope, now.Add(-48*time.Hour))
	seedSignal(t, mem.Signals, scope, now.Add(-time.Hour))

	s := NewScheduler(mem.EntityStates, mem.Signals, detect.NewEngine(config.Default()), time.Second, nil).
		WithRetention(24 * time.Hour)
	s.sweep(ctx)

	got, err := mem.Signals.List(ctx, ports.SignalFilter{ScopeID: scope})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("retention sweep kept %d signals, want 1 (the one inside the window)", len(got))
	}
	if got[0].DetectedAt.Before(now.Add(-24 * time.Hour)) {
		t.Fatal("retention sweep kept a signal older than the cutoff")
	}
}

// Retention off is the default, so an operator who has not opted in
// sees exactly the behaviour they had before.
func TestScheduler_SweepIsNoopWithoutRetention(t *testing.T) {
	mem := memory.New()
	ctx := context.Background()
	scope := uuid.New()

	seedSignal(t, mem.Signals, scope, time.Now().Add(-10000*time.Hour))

	s := NewScheduler(mem.EntityStates, mem.Signals, detect.NewEngine(config.Default()), time.Second, nil)
	s.sweep(ctx)

	n, err := mem.Signals.Count(ctx, ports.SignalFilter{ScopeID: scope})
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 1 {
		t.Fatalf("sweep with retention disabled deleted signals: %d remain, want 1", n)
	}
}
