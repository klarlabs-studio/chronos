// Package conformance defines the behavioural contract every Chronos
// storage backend must satisfy, and runs it as a table of subtests
// against any backend that can hand over the repository ports.
//
// Storage is infrastructure, not behaviour. The observable behaviour of
// Chronos must not change because an operator selected the in-memory
// store, SQLite, PostgreSQL, MySQL/MariaDB or libSQL. Backend-specific
// optimisations are welcome; backend-specific semantics are not.
//
// Two rules keep the suite honest:
//
//  1. No assertion is written loosely enough to pass on every backend
//     when the backends actually disagree. Where they disagree, the
//     disagreement is declared on the [Backend] as a [Quirk] and the
//     corresponding assertion is inverted — the suite then asserts the
//     divergence still reproduces exactly as documented, so repairing
//     the backend fails the suite and forces the declaration, the
//     contract and the release notes to move together.
//
//  2. Anything the contract leaves undefined (the relative order of
//     rows with equal timestamps, for example) is named as undefined in
//     a comment next to the weaker assertion that replaces it, rather
//     than silently assumed.
package conformance

import (
	"slices"
	"testing"
	"time"

	"github.com/felixgeelhaar/chronos"
	"github.com/felixgeelhaar/chronos/internal/ports"
	"github.com/google/uuid"
)

// Store is the set of repositories a backend hands to the suite. Both
// fields are required: a backend that cannot supply one of them is not
// a Chronos backend.
type Store struct {
	// EntityStates persists time-series observations.
	EntityStates ports.EntityStateRepository
	// Signals persists detected patterns. When it also implements
	// [ports.SignalRetainer] the retention contract is exercised too;
	// when it does not, those subtests skip with an explicit reason.
	Signals ports.SignalRepository
}

// Quirk names a contract point a backend is known to violate today.
//
// A quirk is not a licence to diverge: it is a pinned, reproducible
// description of a divergence that has been measured and written down,
// so that it cannot quietly persist and cannot quietly disappear.
type Quirk string

// QuirkLexicalSubSecondTime declares that the backend compares
// timestamps as strings produced by [time.RFC3339Nano], which trims
// trailing zeros from the fractional second. Because the trimmed forms
// have different lengths, byte order stops matching chronological order
// below one second: "12:00:00.1Z" sorts after "12:00:00.12Z" because
// 'Z' (0x5A) is greater than '2' (0x32).
//
// The effect reaches every predicate and ORDER BY over a time column on
// such a backend: entity-state ordering, the ListByScopeSince cutoff,
// signal ordering, the Since/Until filter and retention cutoffs.
// Timestamps on a whole second carry no fractional part at all and are
// therefore unaffected, which is why every other fixture in this suite
// sits on a whole second.
const QuirkLexicalSubSecondTime Quirk = "lexical-sub-second-time"

// Backend describes one storage implementation under test.
type Backend struct {
	// Name identifies the backend in failure messages.
	Name string

	// TimestampPrecision is the finest interval the backend stores.
	// It is declared rather than discovered: the suite asserts both
	// that the backend keeps everything at this precision and that it
	// loses everything below it, so a driver or schema change that
	// moves the precision fails here instead of silently changing what
	// queries return.
	TimestampPrecision time.Duration

	// Quirks lists the contract points this backend violates today.
	// See [Quirk].
	Quirks []Quirk

	// New returns a fresh, empty store. It is called once per subtest,
	// so the store must be isolated from every other store the suite
	// opened: retention, ListScopes and Count are global operations and
	// a shared store would let subtests see each other's rows.
	New func(t *testing.T) Store
}

// Has reports whether the backend declares q.
func (b Backend) Has(q Quirk) bool {
	for _, got := range b.Quirks {
		if got == q {
			return true
		}
	}
	return false
}

// Run executes the full conformance suite against b.
func Run(t *testing.T, b Backend) {
	t.Helper()
	switch {
	case b.Name == "":
		t.Fatal("conformance: Backend.Name is empty")
	case b.New == nil:
		t.Fatal("conformance: Backend.New is nil")
	case b.TimestampPrecision <= 0:
		t.Fatalf("conformance: Backend.TimestampPrecision must be positive, got %v", b.TimestampPrecision)
	}
	for _, g := range groups() {
		t.Run(g.name, func(t *testing.T) { g.run(t, b) })
	}
}

type group struct {
	name string
	run  func(t *testing.T, b Backend)
}

func groups() []group {
	out := entityStateGroups()
	return append(out, signalGroups()...)
}

// open returns a fresh store and fails the test if the backend handed
// back an incomplete one.
func open(t *testing.T, b Backend) Store {
	t.Helper()
	s := b.New(t)
	if s.EntityStates == nil {
		t.Fatalf("%s: Store.EntityStates is nil", b.Name)
	}
	if s.Signals == nil {
		t.Fatalf("%s: Store.Signals is nil", b.Name)
	}
	return s
}

// base is the reference instant every fixture derives from. It sits on
// a whole second so that no fixture accidentally depends on sub-second
// comparison behaviour; the subtests that do test sub-second behaviour
// build their own timestamps and say so.
var base = time.Date(2024, 3, 1, 12, 0, 0, 0, time.UTC)

// observation builds a valid [chronos.EntityState]. Every fixture in
// this suite carries a non-zero Timestamp and finite features, because
// the zero time and non-finite features are rejected at the
// EntityState boundary.
func observation(scope, entity uuid.UUID, ts time.Time, features ...float64) chronos.EntityState {
	if len(features) == 0 {
		features = []float64{1, 2, 3}
	}
	return chronos.EntityState{
		ID:        uuid.New(),
		EntityID:  entity,
		ScopeID:   scope,
		Timestamp: ts,
		Features:  features,
	}
}

// stateIDs projects the observation IDs of states, in order.
func stateIDs(states []chronos.EntityState) []uuid.UUID {
	out := make([]uuid.UUID, len(states))
	for i, s := range states {
		out[i] = s.ID
	}
	return out
}

// sameIDs reports whether two ID sequences are equal element by
// element, order included.
func sameIDs(a, b []uuid.UUID) bool {
	return slices.Equal(a, b)
}

// sameIDSet reports whether two ID sequences contain the same IDs,
// ignoring order. Used where the contract fixes membership but not
// order.
func sameIDSet(a, b []uuid.UUID) bool {
	if len(a) != len(b) {
		return false
	}
	seen := make(map[uuid.UUID]int, len(a))
	for _, id := range a {
		seen[id]++
	}
	for _, id := range b {
		seen[id]--
		if seen[id] < 0 {
			return false
		}
	}
	return true
}

// requireTimestampsNonIncreasing asserts the "most recent first"
// ordering guarantee that every List method on the entity-state port
// documents.
func requireTimestampsNonIncreasing(t *testing.T, states []chronos.EntityState, what string) {
	t.Helper()
	for i := 1; i < len(states); i++ {
		if states[i].Timestamp.After(states[i-1].Timestamp) {
			t.Errorf("%s: not ordered most-recent-first at index %d: %s then %s",
				what, i,
				states[i-1].Timestamp.Format(time.RFC3339Nano),
				states[i].Timestamp.Format(time.RFC3339Nano))
			return
		}
	}
}
