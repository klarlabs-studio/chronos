package observability

import (
	"fmt"
	"io"
	"time"
)

// schedulerHealth is the detection scheduler's own account of whether
// it is working. Detection once went silent for hours and retention
// stopped for five days, each with every log line at INFO; these
// series let Prometheus notice what nobody reading logs did.
//
// The timestamps are the alerting surface: their age answers "when did
// this last succeed?", which stays correct when the process is wedged
// and nothing else is incremented.
type schedulerHealth struct {
	ticks            uint64
	signalsSaved     uint64
	saveFailures     uint64
	lastTick         time.Time
	retentionOverdue int64
	retentionDeleted uint64
	lastSweep        time.Time
}

// ObserveTick records one completed detection tick: how many signals it
// saved, how many saves failed, and when it finished.
func (m *Metrics) ObserveTick(saved, failed int, at time.Time) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.scheduler.ticks++
	m.scheduler.signalsSaved += uint64(max(saved, 0))
	m.scheduler.saveFailures += uint64(max(failed, 0))
	m.scheduler.lastTick = at
	m.mu.Unlock()
}

// SetRetentionOverdue records how many signals have outlived retention
// plus grace. Zero means retention is keeping up.
func (m *Metrics) SetRetentionOverdue(n int64) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.scheduler.retentionOverdue = n
	m.mu.Unlock()
}

// ObserveRetentionDeleted records signals removed by one retention
// batch. Batches commit individually, so a sweep that later fails has
// still deleted what its earlier batches removed.
func (m *Metrics) ObserveRetentionDeleted(n int64) {
	if m == nil || n <= 0 {
		return
	}
	m.mu.Lock()
	m.scheduler.retentionDeleted += uint64(n)
	m.mu.Unlock()
}

// ObserveRetentionSweep records a retention sweep that ran to
// completion. Failed or timed-out sweeps are deliberately not recorded.
func (m *Metrics) ObserveRetentionSweep(at time.Time) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.scheduler.lastSweep = at
	m.mu.Unlock()
}

// renderScheduler writes the scheduler families. Callers hold m.mu.
// Timestamp gauges are omitted until first observed: a zero would read
// as "last succeeded in 1970" and fire every staleness alert on restart.
func (m *Metrics) renderScheduler(w io.Writer) error {
	h := m.scheduler
	families := []struct {
		name, kind, help string
		value            string
		present          bool
	}{
		{"chronos_scheduler_ticks_total", "counter", "Detection ticks completed.", fmt.Sprintf("%d", h.ticks), true},
		{"chronos_scheduler_signals_saved_total", "counter", "Signals written by the detection scheduler.", fmt.Sprintf("%d", h.signalsSaved), true},
		{"chronos_scheduler_save_failures_total", "counter", "Signals the detection scheduler failed to write.", fmt.Sprintf("%d", h.saveFailures), true},
		{"chronos_scheduler_last_tick_timestamp_seconds", "gauge", "Unix time the last detection tick completed.", unixSeconds(h.lastTick), !h.lastTick.IsZero()},
		{"chronos_retention_overdue_signals", "gauge", "Signals older than retention plus grace; above zero means retention is behind.", fmt.Sprintf("%d", h.retentionOverdue), true},
		{"chronos_retention_signals_deleted_total", "counter", "Signals removed by retention.", fmt.Sprintf("%d", h.retentionDeleted), true},
		{"chronos_retention_last_sweep_timestamp_seconds", "gauge", "Unix time the last retention sweep completed without error.", unixSeconds(h.lastSweep), !h.lastSweep.IsZero()},
	}
	for _, f := range families {
		var samples []kvSample
		if f.present {
			samples = []kvSample{{value: f.value}}
		}
		if err := writeFamily(w, f.name, f.kind, f.help, samples); err != nil {
			return err
		}
	}
	return nil
}

func unixSeconds(t time.Time) string {
	return fmt.Sprintf("%g", float64(t.UnixNano())/1e9)
}
