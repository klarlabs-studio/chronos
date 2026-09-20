// Package memory provides in-memory implementations of the persistence
// ports defined in internal/ports. It is the canonical backend used by
// tests; production code should use the SQLite or Postgres backends.
//
// Registers itself with the top-level store factory under the
// "memory" scheme. Per Mnemos ADR 0001 §3, ?namespace= is accepted
// but produces independent state per Open — there is no shared
// process-wide map, so each call to store.Open("memory://...")
// returns a fresh, empty backend regardless of namespace.
package memory

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/felixgeelhaar/chronos"
	"github.com/felixgeelhaar/chronos/internal/domain"
	"github.com/felixgeelhaar/chronos/internal/store"
	"github.com/google/uuid"
)

func init() {
	store.Register("memory", openProvider)
}

// openProvider is the store.OpenFunc that backs memory:// DSNs.
// State is per-Open: each call returns a fresh, empty Conn whose
// repositories share a brand-new state struct.
func openProvider(_ context.Context, dsn string) (*store.Conn, error) {
	if !strings.HasPrefix(dsn, "memory://") {
		return nil, fmt.Errorf("memory: not a memory dsn: %q", dsn)
	}
	mem := New()
	return &store.Conn{
		EntityStates: mem.EntityStates,
		Signals:      mem.Signals,
		Raw:          mem,
		Closer:       mem.Close,
	}, nil
}

// Conn bundles the in-memory repositories. The zero value is unusable;
// use [New].
type Conn struct {
	mu sync.RWMutex

	entityStates map[uuid.UUID][]storedState // keyed by ScopeID
	stateIndex   map[uuid.UUID]stateRef      // observation ID -> where it lives
	signals      []domain.Signal             // flat slice; queries scan

	EntityStates *EntityStateRepository
	Signals      *SignalRepository
}

// storedState couples an EntityState with the adapter that produced it.
// Adapter attribution stays out of the public chronos.EntityState shape.
type storedState struct {
	state   chronos.EntityState
	adapter string
}

// stateRef locates a stored observation: which scope bucket holds it
// and where in that bucket it sits. It exists so that Ingest can be
// idempotent on observation ID — the contract every SQL backend
// implements with ON CONFLICT (id) — without scanning a bucket on
// every write.
type stateRef struct {
	scope uuid.UUID
	idx   int
}

// New creates a fresh in-memory Conn with all repositories wired up.
func New() *Conn {
	c := &Conn{
		entityStates: make(map[uuid.UUID][]storedState),
		stateIndex:   make(map[uuid.UUID]stateRef),
	}
	c.EntityStates = &EntityStateRepository{conn: c}
	c.Signals = &SignalRepository{conn: c}
	return c
}

// Close is a no-op; the in-memory store has no resources to release.
// It satisfies io.Closer so the same factory wiring works as for the
// SQL backends.
func (c *Conn) Close() error { return nil }
