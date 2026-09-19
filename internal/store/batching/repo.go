// Package batching provides a write-coalescing decorator for the
// EntityStateRepository port. It collects Ingest calls into an
// in-memory buffer and flushes them to the underlying store as a
// single Save transaction whenever the buffer fills up, the deadline
// fires, or Close is invoked.
//
// Use case: streaming adapters that submit one observation per
// gRPC/HTTP call against a SQLite backend. Per-call commits cost one
// fsync each; batching coalesces N writes into one fsync, trading
// some per-write latency for substantially higher write throughput.
//
// The decorator is opt-in via the operator config; default is "no
// batching", preserving the original per-call commit semantics for
// deployments that prefer low latency over throughput.
package batching

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/felixgeelhaar/chronos"
	"github.com/felixgeelhaar/chronos/internal/ports"
	"github.com/google/uuid"
)

// Repo wraps an upstream EntityStateRepository with a buffered Ingest
// path. Reads and the explicit batch Save bypass the buffer and go
// straight to the upstream — this matches Chronos's design that
// reads should always see a consistent snapshot, while writes can
// tolerate small batching latency.
type Repo struct {
	upstream ports.EntityStateRepository

	maxBatch int
	maxWait  time.Duration

	mu      sync.Mutex
	buf     []bufferedItem
	timer   *time.Timer
	closing chan struct{}
	closed  bool
}

type bufferedItem struct {
	adapter string
	state   chronos.EntityState
}

// Config tunes the batcher. Both must be positive; otherwise New
// returns an error rather than silently disabling.
type Config struct {
	MaxBatch int
	MaxWait  time.Duration
}

// New wraps upstream with batched-Ingest semantics. Callers must call
// Close when shutting down to drain the buffer; otherwise the most
// recent buffered writes are lost.
func New(upstream ports.EntityStateRepository, cfg Config) (*Repo, error) {
	if upstream == nil {
		return nil, errors.New("batching: upstream required")
	}
	if cfg.MaxBatch <= 0 || cfg.MaxWait <= 0 {
		return nil, errors.New("batching: max_batch and max_wait must be positive")
	}
	return &Repo{
		upstream: upstream,
		maxBatch: cfg.MaxBatch,
		maxWait:  cfg.MaxWait,
		closing:  make(chan struct{}),
	}, nil
}

// Ingest buffers the state. The flush is triggered when the buffer
// hits maxBatch; otherwise a timer drains it at maxWait. Returns an
// error only when validation fails — successful buffering is
// reported as nil even though no row has been persisted yet.
func (r *Repo) Ingest(ctx context.Context, adapterName string, state chronos.EntityState) error {
	if err := state.Validate(); err != nil {
		return fmt.Errorf("batching: validate: %w", err)
	}

	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return errors.New("batching: repo closed")
	}
	r.buf = append(r.buf, bufferedItem{adapter: adapterName, state: state})
	full := len(r.buf) >= r.maxBatch
	if r.timer == nil {
		r.timer = time.AfterFunc(r.maxWait, func() {
			_ = r.Flush(context.Background())
		})
	}
	r.mu.Unlock()

	if full {
		return r.Flush(ctx)
	}
	return nil
}

// Flush drains the buffer immediately. Safe to call concurrently
// with Ingest; the timer is also driven through this path.
func (r *Repo) Flush(ctx context.Context) error {
	r.mu.Lock()
	if len(r.buf) == 0 {
		if r.timer != nil {
			r.timer.Stop()
			r.timer = nil
		}
		r.mu.Unlock()
		return nil
	}
	pending := r.buf
	r.buf = nil
	if r.timer != nil {
		r.timer.Stop()
		r.timer = nil
	}
	r.mu.Unlock()

	// Group by adapter so each underlying Save call shares the same
	// adapterName argument (the upstream takes a single name per
	// batch).
	byAdapter := map[string][]chronos.EntityState{}
	for _, it := range pending {
		byAdapter[it.adapter] = append(byAdapter[it.adapter], it.state)
	}
	for adapter, states := range byAdapter {
		if err := r.upstream.Save(ctx, adapter, states); err != nil {
			return fmt.Errorf("batching: flush adapter=%s: %w", adapter, err)
		}
	}
	return nil
}

// Close drains the buffer and prevents further Ingest calls. Reads
// remain available because they go straight to the upstream.
func (r *Repo) Close(ctx context.Context) error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	close(r.closing)
	r.mu.Unlock()
	return r.Flush(ctx)
}

// Save bypasses the buffer for callers that already have a batch.
func (r *Repo) Save(ctx context.Context, adapterName string, states []chronos.EntityState) error {
	return r.upstream.Save(ctx, adapterName, states)
}

// ListByScope passes through to upstream.
func (r *Repo) ListByScope(ctx context.Context, scopeID uuid.UUID) ([]chronos.EntityState, error) {
	return r.upstream.ListByScope(ctx, scopeID)
}

// ListByScopeSince passes through to upstream.
func (r *Repo) ListByScopeSince(ctx context.Context, scopeID uuid.UUID, cutoff time.Time) ([]chronos.EntityState, error) {
	return r.upstream.ListByScopeSince(ctx, scopeID, cutoff)
}

// ListByEntity passes through to upstream.
func (r *Repo) ListByEntity(ctx context.Context, entityID uuid.UUID) ([]chronos.EntityState, error) {
	return r.upstream.ListByEntity(ctx, entityID)
}

// DeleteOlderThan passes through to upstream.
func (r *Repo) DeleteOlderThan(ctx context.Context, cutoff time.Time, adapterName string) error {
	return r.upstream.DeleteOlderThan(ctx, cutoff, adapterName)
}

// Count passes through to upstream.
func (r *Repo) Count(ctx context.Context, adapterName string) (int64, error) {
	return r.upstream.Count(ctx, adapterName)
}

// ListScopes passes through to upstream.
func (r *Repo) ListScopes(ctx context.Context) ([]uuid.UUID, error) {
	return r.upstream.ListScopes(ctx)
}

// Compile-time interface satisfaction check.
var _ ports.EntityStateRepository = (*Repo)(nil)
