// Package ports declares the outbound interfaces ("ports") the engine drives.
//
// Two aggregates: entity-state observations and signals. Per the cognitive-
// stack vision Chronos does not own reviewer feedback (Mnemos's territory)
// or dismissal/decision lifecycle (agent runtimes) — those interfaces are
// deliberately absent.
//
// Per-aggregate repositories follow the Interface Segregation Principle:
// each backend only implements the slice it supports, and consumers depend
// on the narrowest interface they need.
package ports

import (
	"context"
	"errors"
	"time"

	"github.com/felixgeelhaar/chronos"
	"github.com/felixgeelhaar/chronos/internal/domain"
	"github.com/google/uuid"
)

// ErrNotImplemented is returned by a provider asked for a capability it
// does not support. Callers are expected to degrade rather than fail —
// but to degrade visibly, since a capability silently doing nothing is
// how an operator ends up believing a setting took effect.
var ErrNotImplemented = errors.New("capability not implemented by this provider")

// EntityStateRepository persists and loads time-series observations. The
// vision describes Chronos's intake as "Ingest(ctx, TimeSeriesPoint)";
// this interface offers both a single-point Ingest (for streaming
// adapters) and a batch Save (for pull-based adapters that fetch many
// points in one call).
type EntityStateRepository interface {
	// Ingest persists a single observation. Implementations should treat
	// repeated calls with the same ID as idempotent updates rather than
	// errors. This is the streaming entry point named to match the
	// cognitive-stack vocabulary.
	Ingest(ctx context.Context, adapterName string, state chronos.EntityState) error

	// Save persists a batch of observations. Equivalent in semantics to
	// calling Ingest once per state but transactionally guarded so a
	// partial write never leaves the store inconsistent.
	Save(ctx context.Context, adapterName string, states []chronos.EntityState) error

	// ListByScope returns all states for the given scope, most recent
	// first. Detectors load history through this method.
	ListByScope(ctx context.Context, scopeID uuid.UUID) ([]chronos.EntityState, error)

	// ListByScopeSince returns the scope's states observed at or after
	// cutoff, most recent first. This is the bounded form of
	// ListByScope and the one long-running callers must use.
	//
	// ListByScope has no limit and no window: it materialises every
	// observation ever recorded for the scope. Nothing prunes
	// entity_states -- DeleteOlderThan exists on this interface and had
	// no caller anywhere in the tree -- so on a live stream that result
	// grows without bound, and a detection loop calling it on a timer
	// allocates the whole table every tick. Measured on a 73-series
	// deployment: a flat 2Mi baseline, then ~1.9GB inside a single
	// 30-second tick, then OOM, repeating. The allocation is here,
	// before any detector runs, which is why disabling detectors did
	// not change it.
	ListByScopeSince(ctx context.Context, scopeID uuid.UUID, cutoff time.Time) ([]chronos.EntityState, error)

	// ListByEntity returns all observations of a single entity, most
	// recent first.
	ListByEntity(ctx context.Context, entityID uuid.UUID) ([]chronos.EntityState, error)

	// DeleteOlderThan removes states observed before the cutoff for the
	// given adapter. Used for retention.
	DeleteOlderThan(ctx context.Context, cutoff time.Time, adapterName string) error

	// Count returns the number of states recorded by the named adapter.
	Count(ctx context.Context, adapterName string) (int64, error)

	// ListScopes returns the set of distinct ScopeIDs that have at
	// least one observation. Order is unspecified. Used by the
	// in-process detection scheduler to know which scopes to detect
	// over without requiring callers to maintain a parallel index.
	ListScopes(ctx context.Context) ([]uuid.UUID, error)
}

// SignalFilter is a structured query against the signals store. All
// fields are optional; an empty filter matches everything in the scope
// (ScopeID is required by repositories that use this filter).
type SignalFilter struct {
	// ScopeID restricts results to a single scope. At least one of
	// ScopeID or ScopeIDs must be set unless the repo explicitly allows
	// unscoped queries.
	ScopeID uuid.UUID

	// ScopeIDs restricts results to a set of scopes (server-side
	// allowlist). Use this when fetching across N entities owned by one
	// consumer to avoid the N+1 round-trip pattern. May be combined
	// with ScopeID (ScopeID acts as a single additional allowed scope).
	ScopeIDs []uuid.UUID

	// Series, when set, restricts results to signals about a single
	// entity.
	Series *uuid.UUID

	// Pattern, when set, restricts results to a single PatternType.
	Pattern *domain.PatternType

	// Since and Until, when set, restrict results to signals detected
	// within the half-open interval [Since, Until).
	Since *time.Time
	Until *time.Time

	// MinConfidence, when set, drops signals below the threshold.
	MinConfidence *float64

	// Window, when set, restricts results to signals whose analysis
	// window matches exactly on both bounds. Together with ScopeID,
	// Series and Pattern this is a signal's perception identity, so a
	// filter carrying all four asks "have I detected this already?" —
	// a question a store can answer with Count instead of by returning
	// rows. Both bounds move together on purpose: half an identity is
	// not an identity, so a partial window is not expressible.
	Window *domain.TimeWindow

	// Limit caps the number of returned signals; 0 means no limit.
	Limit int
}

// Capability interfaces — optional features a provider may advertise.
//
// Per Mnemos ADR 0001 §4, not every backend can do every operation:
// memory has no real transactions, MySQL has no pgvector, etc. Rather
// than a lowest-common-denominator schema, we keep capabilities as
// distinct interfaces and let the engine type-assert. Providers
// implement what they can; callers fall back gracefully when an
// assertion fails.
//
// Today only Transactional has live consumers in Chronos (batch save
// semantics). TextSearcher and VectorSearcher are declared as
// forward-compatible plumbing — if/when a future detector wants
// natural-language similarity (e.g. clustering signals by their
// metadata) or semantic distance over an embedding, the provider
// surface is already there.

// Transactional advertises that a provider supports atomic multi-
// statement units of work. Callers obtain a child context that
// transparently routes repository writes through the same
// transaction; commit on nil-return-from-fn, rollback on error or
// panic.
//
// Implementations exist for postgres, sqlite, and mysql. The memory
// backend may return ErrNotImplemented (or implement best-effort
// rollback via state snapshots) — callers must handle that.
type Transactional interface {
	InTx(ctx context.Context, fn func(context.Context) error) error
}

// TextHit is a single result returned by [TextSearcher.SearchByText].
// Score is implementation-defined (BM25 / tsvector rank / FULLTEXT
// score) but always *higher = better* so consumers can sort uniformly.
type TextHit struct {
	SignalID uuid.UUID
	Score    float64
	Snippet  string // optional; may be empty
}

// TextSearcher advertises full-text search over a stored corpus
// (Mnemos searches Claims; Chronos's future use would be searching
// signal metadata, evidence kinds, or labels). Provider plans:
// postgres tsvector + GIN index, sqlite FTS5 virtual table, mysql
// FULLTEXT index, memory tokenised scan with cosine on token sets.
type TextSearcher interface {
	SearchByText(ctx context.Context, query string, limit int) ([]TextHit, error)
}

// VectorHit is a single result from [VectorSearcher.SearchByVector].
// Score is cosine similarity in [-1, 1]; consumers typically filter
// by a minimum threshold and sort descending.
type VectorHit struct {
	SignalID uuid.UUID
	Score    float64
}

// VectorSearcher advertises k-nearest-neighbour search over an
// embedding space. Provider plans: postgres pgvector, sqlite
// sqlite-vss, memory brute-force cosine. MySQL has no vector
// extension in the mainline server today; the MySQL backend is
// expected to either ship without VectorSearcher or fall back to
// the memory implementation.
type VectorSearcher interface {
	SearchByVector(ctx context.Context, query []float32, limit int) ([]VectorHit, error)
}

// Notifier is the outbound port for pushing newly-persisted signals to
// downstream consumers (webhooks, SSE clients, in-process listeners).
//
// Implementations are fire-and-forget: failures must NOT propagate to
// the persistence path, must NOT panic the caller, and must respect
// ctx cancellation. Delivery semantics are at-most-once per consumer;
// consumers de-duplicate by Signal.ID. Per the cognitive-stack vision,
// notification is a transport concern only — the decision of what a
// signal means lives in the consumer, never here.
type Notifier interface {
	Notify(ctx context.Context, sig domain.Signal)
}

// SignalRepository persists and queries signals. There is no Dismiss or
// IsActive concept here — once a signal is detected and persisted it is
// immutable. Decisions about whether to act on it (or to suppress it
// in a UI) live in the consumer.
type SignalRepository interface {
	// Save persists a single signal including its evidence. Idempotent
	// on Signal.ID.
	Save(ctx context.Context, sig domain.Signal) error

	// List returns signals matching filter, ordered detected-at
	// descending (then confidence descending) by convention.
	List(ctx context.Context, filter SignalFilter) ([]domain.Signal, error)

	// Get returns a single signal by ID. Returns
	// domain.ErrSignalNotFound when no row exists.
	Get(ctx context.Context, id uuid.UUID) (domain.Signal, error)

	// Count returns the number of signals matching filter.
	Count(ctx context.Context, filter SignalFilter) (int64, error)
}

// SignalRetainer advertises bounded retention over the signals table.
//
// It is a capability rather than part of SignalRepository because the
// signals table is otherwise append-only — a signal, once detected, is
// a historical fact and nothing in the system revises it. Retention is
// the one exception, and it is an operator's decision about storage,
// not a domain operation.
//
// Deleting a signal deletes its evidence with it; the SQL backends rely
// on ON DELETE CASCADE for that.
type SignalRetainer interface {
	// DeleteSignalsOlderThan removes at most limit signals detected
	// strictly before cutoff, across all scopes, and reports how many
	// rows went. A limit of zero or less means unbounded.
	//
	// The limit exists because the unbounded form is unusable on the
	// deployments that most need retention. A store that has been
	// accumulating before retention was switched on presents its entire
	// backlog to the first sweep, and evidence cascades: one real
	// deployment held 36,209 signals and 25,940,060 evidence rows, about
	// 716 per signal. Deleting that in a single statement ran for nine
	// minutes consuming ~64MB/min of WAL with nothing reclaimable until
	// commit, and would have exhausted the volume and rolled back,
	// achieving nothing. The same deletion in batches of 1000 moved free
	// space not at all, because WAL recycles between commits.
	//
	// Callers should pass a positive limit and loop until a sweep
	// returns fewer rows than it asked for.
	DeleteSignalsOlderThan(ctx context.Context, cutoff time.Time, limit int) (int64, error)
}
