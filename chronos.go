// Package chronos defines the public extension surface of the Chronos engine.
//
// Chronos is a data-source-agnostic pattern detection engine for time-series
// data. The engine is generic — it knows nothing about the domain it serves.
// All domain knowledge enters through adapters that implement [Source] and
// produce [EntityState]s. Detectors derive [Signal]s from those states; the
// HTTP, gRPC and MCP surfaces expose them to consumers.
//
// This package is the contract between the engine and adapter authors. It is
// deliberately small: an [EntityState] data type, a [Source] interface, and a
// process-wide registry. Internal domain logic, persistence, similarity, and
// detection live under internal/ and are not part of the public API.
//
// Adapters self-register via init():
//
//	package myadapter
//
//	import "github.com/felixgeelhaar/chronos"
//
//	func init() { chronos.Register(&Source{}) }
package chronos

import (
	"context"
	"errors"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// Errors returned by validation on public types.
var (
	// ErrMissingObservationID rejects a nil observation ID.
	//
	// Observation ID is the persistence idempotency key: a second Ingest
	// with the same ID is an update, not a new row. Leaving it nil collapses
	// every such observation onto one identity, so later writes silently
	// overwrite earlier ones. Wire transports (HTTP, gRPC, MCP) mint a UUID
	// when the caller omits id; the EntityState boundary still requires one.
	ErrMissingObservationID = errors.New("chronos: entity state missing observation ID")

	ErrMissingEntityID = errors.New("chronos: entity state missing entity ID")
	ErrMissingScopeID  = errors.New("chronos: entity state missing scope ID")
	ErrMissingFeatures = errors.New("chronos: entity state has no features")
	ErrLabelsMismatch  = errors.New("chronos: labels length does not match features length")

	// ErrNonFiniteFeature rejects NaN, +Inf and -Inf at the boundary.
	//
	// Every detector computes means, variances, slopes or correlations over
	// Features. A single NaN propagates silently through all of them: it is
	// not equal to itself, it poisons any sum it touches, and comparisons
	// against it are false, so threshold checks quietly fail open. The
	// result is not an error but something worse -- a signal with a
	// plausible shape and meaningless numbers.
	//
	// Three detectors previously guarded their own computed outputs against
	// this. That is the wrong place and the wrong time: it catches the
	// damage after the arithmetic, only in the detectors that remembered to
	// look, and says nothing about the ones that did not.
	ErrNonFiniteFeature = errors.New("chronos: entity state has a non-finite feature value (NaN or Inf)")

	// ErrMissingTimestamp rejects the zero time.
	//
	// Ordering, windowing and lookback are all computed from Timestamp. The
	// zero value is year 1, so an observation carrying it sorts before all
	// real data and falls outside every lookback window -- it does not
	// error, it silently never participates.
	//
	// This is an EntityState invariant, not a wire requirement: the HTTP,
	// gRPC and MCP layers still accept an omitted timestamp and default it
	// to now before an EntityState is constructed.
	ErrMissingTimestamp = errors.New("chronos: entity state missing timestamp")

	// ErrEmptyLabel rejects a blank feature name in a non-empty Labels slice.
	//
	// Labels name the features that evidence refers to in emitted signals. A
	// blank one produces evidence a consumer cannot attribute to a feature,
	// which defeats the point of carrying labels. Labels stay optional;
	// supplying them and leaving one blank does not.
	ErrEmptyLabel = errors.New("chronos: entity state has an empty feature label")
)

// EntityState is a single observation of an entity at a point in time, encoded
// as a vector of numeric features. Adapters map their domain-specific data
// into this generic shape.
//
// Conventions the engine relies on:
//   - The last element of Features is treated as the outcome metric.
//     Higher values are conventionally "better" for evidence metrics such as
//     outcome_diff and for the sign of Trend slopes.
//   - ScopeID is the grouping primitive: detectors compare entities only
//     against other entities sharing the same ScopeID (except
//     CrossScopeCorrelation, which is explicit about crossing scopes).
//   - Meta is opaque adapter metadata. It is not used for similarity
//     computation; it is preserved for downstream consumers.
type EntityState struct {
	ID        uuid.UUID         // unique observation ID (persistence idempotency key)
	EntityID  uuid.UUID         // the entity (athlete, server, sensor, …)
	ScopeID   uuid.UUID         // the scope (coach, team, tenant, …)
	Timestamp time.Time         // when this state was observed
	Features  []float64         // numeric feature vector; last element is the outcome
	Labels    []string          // optional human-readable feature names; len(Labels)==len(Features) when set
	Meta      map[string]string // adapter-specific metadata; not used for similarity
}

// Validate enforces the EntityState invariants. Adapters and stores must call
// it before returning or persisting an entity state.
func (s EntityState) Validate() error {
	if s.ID == uuid.Nil {
		return ErrMissingObservationID
	}
	if s.EntityID == uuid.Nil {
		return ErrMissingEntityID
	}
	if s.ScopeID == uuid.Nil {
		return ErrMissingScopeID
	}
	if len(s.Features) == 0 {
		return ErrMissingFeatures
	}
	if len(s.Labels) > 0 && len(s.Labels) != len(s.Features) {
		return ErrLabelsMismatch
	}
	// Checked after shape, before content: identity and arity problems are
	// structural and more useful to report first, and an observation whose
	// feature vector is malformed is not improved by learning its timestamp
	// is also absent.
	if s.Timestamp.IsZero() {
		return ErrMissingTimestamp
	}
	// Checked here, once, rather than in each detector. This is the
	// boundary the detectors are entitled to trust: past this point a
	// detector may assume every element of Features is a finite float64
	// and reason about sample counts and variance instead of re-deriving
	// whether arithmetic is safe at all.
	for _, f := range s.Features {
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return ErrNonFiniteFeature
		}
	}
	for _, l := range s.Labels {
		if strings.TrimSpace(l) == "" {
			return ErrEmptyLabel
		}
	}
	return nil
}

// Outcome returns the conventional outcome metric (the last feature). It
// returns zero when Features is empty; callers should validate first.
func (s EntityState) Outcome() float64 {
	if len(s.Features) == 0 {
		return 0
	}
	return s.Features[len(s.Features)-1]
}

// Source is the inbound contract for adapters. Implementations map external
// data into a slice of [EntityState]s. The cfg map carries adapter-specific
// parameters (for example "tenant_id" for a SaaS adapter); the engine
// passes through whatever was supplied at the CLI or API boundary.
type Source interface {
	// Name returns the stable adapter identifier (e.g. "ascend", "prometheus").
	// The name is used to register and look up the adapter and is persisted
	// alongside each EntityState.
	Name() string

	// Fetch retrieves entity states from the external source. Implementations
	// must respect ctx cancellation and should return wrapped errors.
	Fetch(ctx context.Context, cfg map[string]string) ([]EntityState, error)
}

// Closer is implemented by sources that own external resources (for example a
// database connection). The engine will call Close on any source that
// implements it during shutdown.
type Closer interface {
	Close() error
}

// Registry holds a set of adapters keyed by Source.Name(). Construct
// with [NewRegistry] when a process needs isolated Chronos engines
// (multiple independently configured registries in one binary). The
// package-level [Register] / [Get] / [Adapters] helpers delegate to
// [DefaultRegistry] for the common single-engine case.
type Registry struct {
	mu     sync.RWMutex
	byName map[string]Source
}

// NewRegistry returns an empty adapter registry.
func NewRegistry() *Registry {
	return &Registry{byName: make(map[string]Source)}
}

// Register adds src to the registry, keyed on src.Name().
// It panics if src or src.Name() is empty so registration mistakes
// surface at program start.
//
// Re-registering an adapter under the same name overwrites the previous
// entry (last-write-wins). This is intentional so a program that
// imports both the library surface and the cmd/chronos binary doesn't
// panic on duplicate init() registration; see ADR 0001.
func (r *Registry) Register(src Source) {
	if src == nil {
		panic("chronos: Register called with nil Source")
	}
	name := src.Name()
	if name == "" {
		panic("chronos: Source.Name() must be non-empty")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.byName[name] = src
}

// Get returns the adapter registered under name. The boolean is false
// when no such adapter has been registered.
func (r *Registry) Get(name string) (Source, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	src, ok := r.byName[name]
	return src, ok
}

// Adapters returns the names of all registered adapters in unspecified
// order.
func (r *Registry) Adapters() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.byName))
	for name := range r.byName {
		names = append(names, name)
	}
	return names
}

// defaultRegistry is the process-wide convenience registry. Prefer
// [NewRegistry] when embedding multiple independently configured
// Chronos engines in one process.
var defaultRegistry = NewRegistry()

// DefaultRegistry returns the process-wide adapter registry used by
// [Register], [Get], and [Adapters].
func DefaultRegistry() *Registry { return defaultRegistry }

// Register adds src to the default adapter registry. See
// [Registry.Register].
func Register(src Source) { defaultRegistry.Register(src) }

// Get returns the adapter registered under name on the default
// registry. See [Registry.Get].
func Get(name string) (Source, bool) { return defaultRegistry.Get(name) }

// Adapters returns the names of all adapters on the default registry.
// See [Registry.Adapters].
func Adapters() []string { return defaultRegistry.Adapters() }
