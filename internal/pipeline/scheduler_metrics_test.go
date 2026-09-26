package pipeline

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/felixgeelhaar/chronos/internal/config"
	"github.com/felixgeelhaar/chronos/internal/detect"
	"github.com/felixgeelhaar/chronos/internal/observability"
	"github.com/felixgeelhaar/chronos/internal/store/memory"
	"github.com/google/uuid"
)

func render(t *testing.T, m *observability.Metrics) string {
	t.Helper()
	var buf bytes.Buffer
	if err := m.Render(&buf); err != nil {
		t.Fatalf("Render: %v", err)
	}
	return buf.String()
}

func TestScheduler_TickReportsItselfToMetrics(t *testing.T) {
	mem := memory.New()
	m := observability.New()
	s := NewScheduler(mem.EntityStates, mem.Signals, detect.NewEngine(config.Default()), time.Second, nil).
		WithMetrics(m)
	s.tick(context.Background())

	out := render(t, m)
	if !strings.Contains(out, "chronos_scheduler_ticks_total 1") {
		t.Errorf("tick not counted:\n%s", out)
	}
	if !strings.Contains(out, "\nchronos_scheduler_last_tick_timestamp_seconds ") {
		t.Errorf("last tick timestamp not set:\n%s", out)
	}
}

func TestScheduler_RetentionCheckExportsOverdueCount(t *testing.T) {
	mem := memory.New()
	scope := uuid.New()
	seedSignal(t, mem.Signals, scope, time.Now().Add(-72*time.Hour))
	seedSignal(t, mem.Signals, scope, time.Now().Add(-48*time.Hour))
	m := observability.New()
	s := NewScheduler(mem.EntityStates, mem.Signals, detect.NewEngine(config.Default()), time.Second, nil).
		WithRetention(24 * time.Hour).
		WithMetrics(m)

	s.checkRetention(context.Background())
	if out := render(t, m); !strings.Contains(out, "chronos_retention_overdue_signals 2") {
		t.Fatalf("overdue gauge not 2:\n%s", out)
	}

	// Caught up: the gauge must return to zero, or the alert never clears.
	s.sweep(context.Background())
	s.checkRetention(context.Background())
	out := render(t, m)
	if !strings.Contains(out, "chronos_retention_overdue_signals 0") {
		t.Fatalf("overdue gauge did not clear after the sweep:\n%s", out)
	}
	if !strings.Contains(out, "chronos_retention_signals_deleted_total 2") ||
		!strings.Contains(out, "\nchronos_retention_last_sweep_timestamp_seconds ") {
		t.Fatalf("sweep not reported:\n%s", out)
	}
}

// A sweep that fails must not refresh the last-sweep timestamp; the
// age of that timestamp is what the alert watches.
func TestScheduler_FailedSweepDoesNotCountAsASweep(t *testing.T) {
	mem := memory.New()
	seedSignal(t, mem.Signals, uuid.New(), time.Now().Add(-72*time.Hour))
	stalled := &stalledRetainer{SignalRepository: mem.Signals}
	m := observability.New()
	s := NewScheduler(mem.EntityStates, stalled, detect.NewEngine(config.Default()), time.Second, nil).
		WithRetention(24 * time.Hour).
		WithMetrics(m)
	s.batchTimeout = 20 * time.Millisecond

	s.sweep(context.Background())
	if out := render(t, m); strings.Contains(out, "\nchronos_retention_last_sweep_timestamp_seconds ") {
		t.Fatalf("a timed-out sweep was reported as successful:\n%s", out)
	}
}
