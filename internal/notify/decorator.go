// Package notify implements outbound transports for newly-detected
// signals. Notification is decoupled from detection: any code path
// that calls SignalRepository.Save through a NotifyingSignalRepository
// triggers the configured ports.Notifier without the engine, the
// detectors, or the pipeline knowing or caring how delivery happens.
//
// Available transports:
//
//   - WebhookNotifier — HMAC-signed POST per signal to one or more URLs
//   - SSE — bounded in-memory broadcast for /v1/signals/stream clients
//   - Multi — composite that fans out to several Notifiers
//
// Delivery is at-most-once per consumer; consumers de-duplicate by
// Signal.ID. Persistence is the source of truth — push is a courtesy.
package notify

import (
	"context"
	"time"

	"github.com/felixgeelhaar/chronos/internal/domain"
	"github.com/felixgeelhaar/chronos/internal/ports"
	"github.com/google/uuid"
)

// NotifyingSignalRepository wraps a ports.SignalRepository and calls
// the configured Notifier *after* a successful Save. Failures from
// the inner repository propagate normally; the notifier is only
// invoked on the persistence happy path. A nil notifier disables
// notification (the wrapper still delegates Save/List/Get/Count, so
// it can be installed unconditionally and the configuration decides
// whether anything actually fires).
type NotifyingSignalRepository struct {
	inner    ports.SignalRepository
	notifier ports.Notifier
}

// WrapSignals returns a SignalRepository that calls notifier on each
// successful Save. Pass a nil notifier to disable push transparently.
func WrapSignals(inner ports.SignalRepository, notifier ports.Notifier) *NotifyingSignalRepository {
	return &NotifyingSignalRepository{inner: inner, notifier: notifier}
}

// Save persists the signal through the inner repository, then — only
// if persistence succeeded and a notifier is configured — pushes the
// signal to consumers. Notifier panics are recovered so a buggy
// transport cannot crash the pipeline.
func (n *NotifyingSignalRepository) Save(ctx context.Context, sig domain.Signal) error {
	if err := n.inner.Save(ctx, sig); err != nil {
		return err
	}
	if n.notifier == nil {
		return nil
	}
	defer func() {
		// Notifier failures must never propagate to the caller; the
		// signal is already persisted and that is the source of truth.
		_ = recover()
	}()
	n.notifier.Notify(ctx, sig)
	return nil
}

// List delegates to the inner repository.
func (n *NotifyingSignalRepository) List(ctx context.Context, filter ports.SignalFilter) ([]domain.Signal, error) {
	return n.inner.List(ctx, filter)
}

// Get delegates to the inner repository.
func (n *NotifyingSignalRepository) Get(ctx context.Context, id uuid.UUID) (domain.Signal, error) {
	return n.inner.Get(ctx, id)
}

// Count delegates to the inner repository.
func (n *NotifyingSignalRepository) Count(ctx context.Context, filter ports.SignalFilter) (int64, error) {
	return n.inner.Count(ctx, filter)
}

// DeleteSignalsOlderThan forwards retention to the inner repository when
// it supports it, and reports ports.ErrNotImplemented when it does not.
//
// This wrapper is installed unconditionally on the serve path, so it
// sits between the scheduler and the real store. A capability it fails
// to forward is a capability the scheduler cannot see — retention would
// silently do nothing on exactly the deployments whose store has grown
// large enough to need it.
func (n *NotifyingSignalRepository) DeleteSignalsOlderThan(ctx context.Context, cutoff time.Time, limit int) (int64, error) {
	retainer, ok := n.inner.(ports.SignalRetainer)
	if !ok {
		return 0, ports.ErrNotImplemented
	}
	return retainer.DeleteSignalsOlderThan(ctx, cutoff, limit)
}

// Multi composes several Notifiers into one. Failures in any single
// notifier do not block the others; each is invoked synchronously in
// slice order (transports are responsible for their own concurrency).
type Multi []ports.Notifier

// Notify fans the signal out to every configured notifier.
func (m Multi) Notify(ctx context.Context, sig domain.Signal) {
	for _, n := range m {
		if n == nil {
			continue
		}
		func() {
			defer func() { _ = recover() }()
			n.Notify(ctx, sig)
		}()
	}
}
