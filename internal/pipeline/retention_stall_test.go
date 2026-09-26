package pipeline

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/felixgeelhaar/chronos/internal/config"
	"github.com/felixgeelhaar/chronos/internal/detect"
	"github.com/felixgeelhaar/chronos/internal/store/memory"
	"github.com/google/uuid"
)

// stalledRetainer makes every retention batch block until its context
// ends: a DELETE waiting on a connection that will never answer. This is
// the shape that stopped retention in production for five days
// (2026-09-21 to 2026-09-26) without a single error line -- the sweep
// goroutine sat in one call, and the ticker dropped every tick it could
// not deliver.
type stalledRetainer struct {
	*memory.SignalRepository
	calls atomic.Int32
}

func (s *stalledRetainer) DeleteSignalsOlderThan(ctx context.Context, _ time.Time, _ int) (int64, error) {
	s.calls.Add(1)
	<-ctx.Done()
	return 0, ctx.Err()
}

// syncBuffer lets concurrent goroutines log into one buffer under -race.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (w *syncBuffer) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b.Write(p)
}

func (w *syncBuffer) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b.String()
}

func captureLogger() (*slog.Logger, *syncBuffer) {
	buf := &syncBuffer{}
	return slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})), buf
}

// A hung batch must cost at most one batch timeout, not the rest of the
// process's life.
func TestScheduler_SweepGivesUpOnAHungBatch(t *testing.T) {
	mem := memory.New()
	stalled := &stalledRetainer{SignalRepository: mem.Signals}
	s := NewScheduler(mem.EntityStates, stalled, detect.NewEngine(config.Default()), time.Second, nil).
		WithRetention(24 * time.Hour)
	s.batchTimeout = 50 * time.Millisecond

	done := make(chan struct{})
	go func() { s.sweep(context.Background()); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("sweep never returned from a hung batch; retention would stop for the life of the process")
	}
	if stalled.calls.Load() != 1 {
		t.Fatalf("expected exactly one attempted batch, got %d", stalled.calls.Load())
	}
}

// The alarm reads the data, not the sweeper's own account of itself:
// signals older than retention plus grace mean retention is not doing
// its job, whatever the reason.
func TestScheduler_RetentionCheckWarnsWhenSignalsOutliveRetention(t *testing.T) {
	mem := memory.New()
	scope := uuid.New()
	seedSignal(t, mem.Signals, scope, time.Now().Add(-72*time.Hour))
	seedSignal(t, mem.Signals, scope, time.Now().Add(-48*time.Hour))

	logger, buf := captureLogger()
	s := NewScheduler(mem.EntityStates, mem.Signals, detect.NewEngine(config.Default()), time.Second, logger).
		WithRetention(24 * time.Hour)
	s.checkRetention(context.Background())

	out := buf.String()
	if !strings.Contains(out, "retention is behind") || !strings.Contains(out, "overdue=2") {
		t.Fatalf("expected a retention-is-behind warning with overdue=2, got:\n%s", out)
	}
	if !strings.Contains(out, "level=WARN") {
		t.Fatalf("the alarm must be WARN so it can be alerted on, got:\n%s", out)
	}
}

func TestScheduler_RetentionCheckIsQuietWhenRetentionHolds(t *testing.T) {
	mem := memory.New()
	seedSignal(t, mem.Signals, uuid.New(), time.Now().Add(-time.Hour))

	logger, buf := captureLogger()
	s := NewScheduler(mem.EntityStates, mem.Signals, detect.NewEngine(config.Default()), time.Second, logger).
		WithRetention(24 * time.Hour)
	s.checkRetention(context.Background())

	if strings.Contains(buf.String(), "retention is behind") {
		t.Fatalf("warned although every signal is inside retention:\n%s", buf.String())
	}
}

// The property that matters most. A sweep cannot report its own hang, so
// the check has to run somewhere the hang cannot reach. With the
// retention goroutine stuck for the whole test, Run must still raise
// the alarm.
func TestScheduler_RetentionAlarmFiresWhileTheSweepIsStuck(t *testing.T) {
	mem := memory.New()
	seedSignal(t, mem.Signals, uuid.New(), time.Now().Add(-72*time.Hour))
	stalled := &stalledRetainer{SignalRepository: mem.Signals}

	logger, buf := captureLogger()
	s := NewScheduler(mem.EntityStates, stalled, detect.NewEngine(config.Default()), time.Hour, logger).
		WithRetention(24 * time.Hour).
		WithSweepInterval(20 * time.Millisecond)
	s.batchTimeout = time.Hour // the sweep stays hung for the whole test

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go func() { _ = s.Run(ctx) }()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(buf.String(), "retention is behind") {
			if stalled.calls.Load() == 0 {
				t.Fatal("alarm fired but the sweep never ran; the test is not exercising a stuck sweep")
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no alarm while the retention goroutine was stuck:\n%s", buf.String())
}
