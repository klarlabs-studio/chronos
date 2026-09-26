package observability

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

// The scheduler's health has to be visible to Prometheus, not only in
// logs: detection once went silent for hours and retention stopped for
// five days, both with every line at INFO.
func TestMetrics_RendersSchedulerHealth(t *testing.T) {
	m := New()
	at := time.Unix(1_790_000_000, 0)
	m.ObserveTick(175, 2, at)
	m.ObserveTick(0, 0, at.Add(30*time.Second))
	m.SetRetentionOverdue(4858)
	m.ObserveRetentionDeleted(1000)
	m.ObserveRetentionSweep(at.Add(time.Minute))

	var buf bytes.Buffer
	if err := m.Render(&buf); err != nil {
		t.Fatalf("Render: %v", err)
	}
	out := buf.String()
	for _, w := range []string{
		"# TYPE chronos_scheduler_ticks_total counter",
		"chronos_scheduler_ticks_total 2",
		"chronos_scheduler_signals_saved_total 175",
		"chronos_scheduler_save_failures_total 2",
		"# TYPE chronos_scheduler_last_tick_timestamp_seconds gauge",
		"chronos_scheduler_last_tick_timestamp_seconds 1.79000003e+09",
		"# TYPE chronos_retention_overdue_signals gauge",
		"chronos_retention_overdue_signals 4858",
		"chronos_retention_signals_deleted_total 1000",
		"# TYPE chronos_retention_last_sweep_timestamp_seconds gauge",
		"chronos_retention_last_sweep_timestamp_seconds 1.79000006e+09",
	} {
		if !strings.Contains(out, w) {
			t.Errorf("missing %q\n--- output ---\n%s", w, out)
		}
	}
}

// Before the first tick or sweep there is no timestamp to report. A
// zero would read as "last success in 1970" and page immediately on
// every restart, so the gauges are absent until they mean something.
func TestMetrics_OmitsTimestampsBeforeFirstObservation(t *testing.T) {
	var buf bytes.Buffer
	if err := New().Render(&buf); err != nil {
		t.Fatalf("Render: %v", err)
	}
	for _, name := range []string{
		"\nchronos_scheduler_last_tick_timestamp_seconds ",
		"\nchronos_retention_last_sweep_timestamp_seconds ",
	} {
		if strings.Contains(buf.String(), name) {
			t.Errorf("%s rendered before any observation:\n%s", name, buf.String())
		}
	}
}
