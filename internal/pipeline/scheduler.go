package pipeline

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/felixgeelhaar/chronos/internal/detect"
	"github.com/felixgeelhaar/chronos/internal/domain"
	"github.com/felixgeelhaar/chronos/internal/ports"
)

// Scheduler runs detection in-process at a configurable cadence,
// without re-fetching observations from adapters. It is the bridge
// between the streaming Ingest path (POST /v1/ingest) and consumers
// of /v1/signals/stream — without a scheduler, signals only get
// produced by the compute CLI in a separate process and SSE clients
// would never see anything.
//
// Each tick the scheduler enumerates scopes that have observations,
// loads each scope's history, runs every detector over it, and
// persists the resulting signals through the configured (typically
// notifier-wrapped) SignalRepository. A second tick over the same
// observations is a no-op: the scheduler skips a candidate when a
// signal with the same (scope, series, pattern, window) already
// exists, so unchanged data does not append duplicate rows.
type Scheduler struct {
	states    ports.EntityStateRepository
	signals   ports.SignalRepository
	engine    *detect.Engine
	interval  time.Duration
	retention time.Duration
	logger    *slog.Logger
}

// NewScheduler builds a scheduler. interval == 0 produces a scheduler
// whose Run is a no-op (returns immediately) — convenient for the
// "scheduler off" config path so callers don't have to special-case
// the nil scheduler.
func NewScheduler(states ports.EntityStateRepository, signals ports.SignalRepository, engine *detect.Engine, interval time.Duration, logger *slog.Logger) *Scheduler {
	if logger == nil {
		logger = slog.Default()
	}
	return &Scheduler{
		states:   states,
		signals:  signals,
		engine:   engine,
		interval: interval,
		logger:   logger,
	}
}

// WithRetention enables periodic deletion of signals older than d, and
// returns the scheduler for chaining. Zero (the default) disables the
// sweep entirely, so a deployment that has not opted in behaves exactly
// as it did before retention existed.
//
// This is a setter rather than a NewScheduler parameter because the
// constructor already takes a time.Duration: two adjacent durations are
// two arguments a caller can transpose without the compiler noticing,
// and transposing these two turns a 30-second detection cadence into a
// 30-second retention window that deletes almost the entire table on
// its first pass.
func (s *Scheduler) WithRetention(d time.Duration) *Scheduler {
	s.retention = d
	return s
}

// Run blocks until ctx is cancelled, ticking detection every
// interval. interval <= 0 is treated as disabled and Run returns
// immediately. Each tick is bounded by ctx — if a scope load takes
// longer than the interval, the next tick still fires (no
// throttling) but the previous tick's writes are best-effort.
func (s *Scheduler) Run(ctx context.Context) error {
	if s.interval <= 0 {
		return nil
	}
	t := time.NewTicker(s.interval)
	defer t.Stop()

	// Retention runs on its own, far slower clock. Tying it to the
	// detection interval would issue a table-wide DELETE every few
	// seconds to reclaim the handful of rows that aged out since the
	// last one.
	sweeps := time.NewTicker(retentionSweepInterval)
	defer sweeps.Stop()

	s.logger.Info("detection scheduler started", "interval", s.interval, "signal_retention", s.retention)
	s.sweep(ctx)
	for {
		select {
		case <-ctx.Done():
			s.logger.Info("detection scheduler stopped")
			return nil
		case <-t.C:
			s.tick(ctx)
		case <-sweeps.C:
			s.sweep(ctx)
		}
	}
}

// retentionSweepInterval is how often Run prunes the signals table when
// retention is configured.
const retentionSweepInterval = time.Hour

// sweep deletes signals detected longer ago than the configured
// retention, and does nothing at all when retention is disabled.
//
// A store that cannot prune says so loudly. Retention is an operator
// setting whose whole effect is invisible — nobody watches rows fail to
// disappear — so a backend that does not implement SignalRetainer, or a
// decorator that forgets to forward it, would otherwise leave an
// operator believing their setting took effect while the table grew
// exactly as before.
func (s *Scheduler) sweep(ctx context.Context) {
	if s.retention <= 0 {
		return
	}
	retainer, ok := s.signals.(ports.SignalRetainer)
	if !ok {
		s.logger.Error("scheduler: signal retention configured but the store does not support it; signals will not be pruned",
			"retention", s.retention)
		return
	}
	cutoff := time.Now().Add(-s.retention)
	n, err := retainer.DeleteSignalsOlderThan(ctx, cutoff)
	switch {
	case errors.Is(err, ports.ErrNotImplemented):
		s.logger.Error("scheduler: signal retention configured but the store does not support it; signals will not be pruned",
			"retention", s.retention)
		return
	case err != nil:
		s.logger.Error("scheduler: signal retention sweep failed", "cutoff", cutoff, "err", err)
		return
	}
	if n > 0 {
		s.logger.Info("scheduler: pruned signals past retention", "deleted", n, "cutoff", cutoff)
	}
}

// tick performs one full detection pass across all scopes. Errors on
// any single scope are logged and do not abort the pass.
func (s *Scheduler) tick(ctx context.Context) {
	scopes, err := s.states.ListScopes(ctx)
	if err != nil {
		s.logger.Error("scheduler: list scopes failed", "err", err)
		return
	}
	for _, scopeID := range scopes {
		states, err := s.states.ListByScope(ctx, scopeID)
		if err != nil {
			s.logger.Error("scheduler: load scope failed", "scope_id", scopeID, "err", err)
			continue
		}
		if len(states) == 0 {
			continue
		}
		signals := s.engine.Detect(ctx, states)
		for _, sig := range signals {
			if s.alreadyPersisted(ctx, sig) {
				continue
			}
			if err := s.signals.Save(ctx, sig); err != nil {
				s.logger.Error("scheduler: signal save failed", "scope_id", scopeID, "signal_id", sig.ID, "err", err)
			}
		}
	}
}

// alreadyPersisted reports whether a signal with the same perception
// identity — (scope, series, pattern, window) — is already in the
// store. Detectors mint a fresh UUID every Detect call, so without
// this check the scheduler would append a duplicate row on every tick
// over unchanged observations. Lookup failures fail open (return
// false) so a transient store error cannot suppress a real emission.
//
// The whole identity goes into the filter and the store answers with a
// Count. It is tempting to ask the cheaper-looking question — fetch
// this series' signals, compare the windows here — but that reads
// every matching row into the process (and, on the SQL backends, one
// further query per row to hydrate its evidence) merely to compare two
// timestamps. Since nothing prunes the signals table, the cost of that
// read has no ceiling: it ran once per candidate per tick until the
// process was OOM-killed. Counting keeps a tick's memory flat no
// matter how large the store has grown.
func (s *Scheduler) alreadyPersisted(ctx context.Context, sig domain.Signal) bool {
	pat := sig.Pattern
	series := sig.Series
	window := sig.Window
	n, err := s.signals.Count(ctx, ports.SignalFilter{
		ScopeID: sig.ScopeID,
		Pattern: &pat,
		Series:  &series,
		Window:  &window,
	})
	if err != nil {
		return false
	}
	return n > 0
}
