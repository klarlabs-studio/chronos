package conformance

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/felixgeelhaar/chronos"
	"github.com/google/uuid"
)

func entityStateGroups() []group {
	return []group{
		{"EntityState/RoundTrip", entityStateRoundTrip},
		{"EntityState/BatchSave", entityStateBatchSave},
		{"EntityState/Ordering", entityStateOrdering},
		{"EntityState/OrderingSubSecond", entityStateOrderingSubSecond},
		{"EntityState/DuplicateObservationID", entityStateDuplicateID},
		{"EntityState/DuplicateTimestamps", entityStateDuplicateTimestamps},
		{"EntityState/ScopeIsolation", entityStateScopeIsolation},
		{"EntityState/ListByEntity", entityStateListByEntity},
		{"EntityState/ListByScopeSinceBoundary", entityStateSinceBoundary},
		{"EntityState/ListByScopeSinceSubSecondCutoff", entityStateSinceSubSecond},
		{"EntityState/EmptyResultsAreNotErrors", entityStateEmptyNotError},
		{"EntityState/CountIsPerAdapter", entityStateCountPerAdapter},
		{"EntityState/ListScopes", entityStateListScopes},
		{"EntityState/DeleteOlderThan", entityStateDeleteOlderThan},
		{"EntityState/TimestampFidelity", entityStateTimestampFidelity},
		{"EntityState/Concurrency", entityStateConcurrency},
	}
}

// entityStateRoundTrip pins the basic persistence contract: everything
// an adapter puts in comes back out.
func entityStateRoundTrip(t *testing.T, b Backend) {
	s := open(t, b)
	ctx := context.Background()
	scope, entity := uuid.New(), uuid.New()

	want := observation(scope, entity, base, 1.5, -2.25, 0, 3e8)
	want.Labels = []string{"load", "latency", "errors", "throughput"}
	want.Meta = map[string]string{"region": "eu-central-1", "adapter_version": "2"}
	if err := s.EntityStates.Ingest(ctx, "adapter-a", want); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	got, err := s.EntityStates.ListByScope(ctx, scope)
	if err != nil {
		t.Fatalf("ListByScope: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("ListByScope returned %d states, want 1", len(got))
	}
	requireStateEqual(t, want, got[0], b.TimestampPrecision)

	n, err := s.EntityStates.Count(ctx, "adapter-a")
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if n != 1 {
		t.Errorf("Count(adapter-a) = %d, want 1", n)
	}

	// A state ingested without labels or meta comes back carrying no
	// labels and no meta. Whether that is a nil map or an empty one is
	// NOT part of the contract and does differ: the SQLite family
	// decodes an absent meta object into an empty map, the others
	// leave it nil. Consumers must therefore test len(), not nil.
	bare := observation(scope, entity, base.Add(-time.Minute))
	if err := s.EntityStates.Ingest(ctx, "adapter-a", bare); err != nil {
		t.Fatalf("Ingest bare: %v", err)
	}
	got, err = s.EntityStates.ListByScope(ctx, scope)
	if err != nil {
		t.Fatalf("ListByScope: %v", err)
	}
	for _, st := range got {
		if st.ID != bare.ID {
			continue
		}
		if len(st.Labels) != 0 {
			t.Errorf("bare state came back with labels %v, want none", st.Labels)
		}
		if len(st.Meta) != 0 {
			t.Errorf("bare state came back with meta %v, want none", st.Meta)
		}
	}
}

// entityStateBatchSave pins Save's equivalence to a loop of Ingest.
func entityStateBatchSave(t *testing.T, b Backend) {
	s := open(t, b)
	ctx := context.Background()
	scope, entity := uuid.New(), uuid.New()

	batch := []chronos.EntityState{
		observation(scope, entity, base),
		observation(scope, entity, base.Add(time.Minute)),
		observation(scope, uuid.New(), base.Add(2*time.Minute)),
	}
	if err := s.EntityStates.Save(ctx, "adapter-a", batch); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, err := s.EntityStates.ListByScope(ctx, scope)
	if err != nil {
		t.Fatalf("ListByScope: %v", err)
	}
	if !sameIDSet(stateIDs(got), stateIDs(batch)) {
		t.Errorf("ListByScope after Save = %v, want the 3 saved IDs %v", stateIDs(got), stateIDs(batch))
	}
	n, err := s.EntityStates.Count(ctx, "adapter-a")
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if n != 3 {
		t.Errorf("Count after Save of 3 = %d, want 3", n)
	}

	// Saving an empty batch is a no-op, not an error.
	if err := s.EntityStates.Save(ctx, "adapter-a", nil); err != nil {
		t.Errorf("Save(nil batch): %v, want nil", err)
	}

	// Save is all-or-nothing: the port documents it as "transactionally
	// guarded so a partial write never leaves the store inconsistent".
	// One invalid observation rejects the batch and writes none of it,
	// including the valid rows that precede the bad one.
	rejected := []chronos.EntityState{
		observation(scope, entity, base.Add(time.Hour)),
		observation(scope, entity, base.Add(2*time.Hour)),
		{ID: uuid.New(), ScopeID: scope, Timestamp: base, Features: []float64{1}}, // no EntityID
	}
	if err := s.EntityStates.Save(ctx, "adapter-a", rejected); err == nil {
		t.Error("Save(batch containing an invalid state) = nil, want an error")
	}
	n, err = s.EntityStates.Count(ctx, "adapter-a")
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if n != 3 {
		t.Errorf("Count after a rejected batch = %d, want the 3 rows from before it: "+
			"a batch that fails validation must write none of its rows", n)
	}
}

// entityStateOrdering pins the "most recent first" guarantee against
// out-of-order arrival, and pins that the order does not change between
// two identical queries.
func entityStateOrdering(t *testing.T, b Backend) {
	s := open(t, b)
	ctx := context.Background()
	scope, entity := uuid.New(), uuid.New()

	// Arrival order is deliberately not chronological order.
	arrival := []chronos.EntityState{
		observation(scope, entity, base.Add(2*time.Hour)),
		observation(scope, entity, base),
		observation(scope, entity, base.Add(3*time.Hour)),
		observation(scope, entity, base.Add(time.Hour)),
	}
	for _, st := range arrival {
		if err := s.EntityStates.Ingest(ctx, "adapter-a", st); err != nil {
			t.Fatalf("Ingest: %v", err)
		}
	}
	wantOrder := []uuid.UUID{arrival[2].ID, arrival[0].ID, arrival[3].ID, arrival[1].ID}

	got, err := s.EntityStates.ListByScope(ctx, scope)
	if err != nil {
		t.Fatalf("ListByScope: %v", err)
	}
	if !sameIDs(stateIDs(got), wantOrder) {
		t.Errorf("ListByScope order = %v, want %v (most recent first, regardless of arrival order)",
			stateIDs(got), wantOrder)
	}
	requireTimestampsNonIncreasing(t, got, "ListByScope")

	// Ordering is a property of the query, not of the call: the same
	// query twice returns the same sequence.
	again, err := s.EntityStates.ListByScope(ctx, scope)
	if err != nil {
		t.Fatalf("ListByScope (repeat): %v", err)
	}
	if !sameIDs(stateIDs(again), stateIDs(got)) {
		t.Errorf("ListByScope is not deterministic: %v then %v", stateIDs(got), stateIDs(again))
	}

	byEntity, err := s.EntityStates.ListByEntity(ctx, entity)
	if err != nil {
		t.Fatalf("ListByEntity: %v", err)
	}
	if !sameIDs(stateIDs(byEntity), wantOrder) {
		t.Errorf("ListByEntity order = %v, want %v", stateIDs(byEntity), wantOrder)
	}

	since, err := s.EntityStates.ListByScopeSince(ctx, scope, base.Add(time.Hour))
	if err != nil {
		t.Fatalf("ListByScopeSince: %v", err)
	}
	if !sameIDs(stateIDs(since), wantOrder[:3]) {
		t.Errorf("ListByScopeSince order = %v, want %v", stateIDs(since), wantOrder[:3])
	}
}

// entityStateOrderingSubSecond is the same guarantee below one second.
// The three fixtures are 100ms, 120ms and 200ms past a whole second:
// chronologically .1 < .12 < .2, but as RFC3339Nano strings ".1Z" sorts
// after ".12Z" because 'Z' > '2'.
func entityStateOrderingSubSecond(t *testing.T, b Backend) {
	s := open(t, b)
	ctx := context.Background()
	scope, entity := uuid.New(), uuid.New()

	at100 := observation(scope, entity, base.Add(100*time.Millisecond))
	at120 := observation(scope, entity, base.Add(120*time.Millisecond))
	at200 := observation(scope, entity, base.Add(200*time.Millisecond))
	for _, st := range []chronos.EntityState{at100, at120, at200} {
		if err := s.EntityStates.Ingest(ctx, "adapter-a", st); err != nil {
			t.Fatalf("Ingest: %v", err)
		}
	}

	got, err := s.EntityStates.ListByScope(ctx, scope)
	if err != nil {
		t.Fatalf("ListByScope: %v", err)
	}
	chronological := []uuid.UUID{at200.ID, at120.ID, at100.ID}

	if b.Has(QuirkLexicalSubSecondTime) {
		// Pinned divergence, not an accepted one: byte order over the
		// trimmed fractional second puts .1 ahead of .12. If this
		// assertion starts failing the backend has been repaired —
		// drop QuirkLexicalSubSecondTime from its declaration and
		// delete this branch.
		lexical := []uuid.UUID{at200.ID, at100.ID, at120.ID}
		if !sameIDs(stateIDs(got), lexical) {
			t.Errorf("%s declares %s but ordered %v; lexical order is %v, chronological order is %v",
				b.Name, QuirkLexicalSubSecondTime, stateIDs(got), lexical, chronological)
		}
		return
	}
	if !sameIDs(stateIDs(got), chronological) {
		t.Errorf("sub-second ListByScope order = %v, want %v (most recent first)",
			stateIDs(got), chronological)
	}
}

// entityStateDuplicateID pins what a second Ingest of the same
// observation ID does. The port documents it as an idempotent update.
//
// The mutable column set is the one the SQL backends' ON CONFLICT
// clauses carry: features, labels, meta and adapter are replaced;
// id, entity_id, scope_id and timestamp are not. Re-ingesting an
// observation is a correction of its payload, never a move of the
// observation in time.
func entityStateDuplicateID(t *testing.T, b Backend) {
	s := open(t, b)
	ctx := context.Background()
	scope, entity := uuid.New(), uuid.New()

	first := observation(scope, entity, base, 1, 2, 3)
	first.Meta = map[string]string{"v": "1"}
	if err := s.EntityStates.Ingest(ctx, "adapter-a", first); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	second := first
	second.Timestamp = base.Add(time.Hour)
	second.Features = []float64{9, 8, 7}
	second.Meta = map[string]string{"v": "2"}
	if err := s.EntityStates.Ingest(ctx, "adapter-b", second); err != nil {
		t.Fatalf("Ingest (duplicate ID): %v", err)
	}

	got, err := s.EntityStates.ListByScope(ctx, scope)
	if err != nil {
		t.Fatalf("ListByScope: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("after re-ingesting observation %s the scope holds %d states, want 1 (Ingest is idempotent on ID)",
			first.ID, len(got))
	}
	if !sameFeatures(got[0].Features, second.Features) {
		t.Errorf("features = %v, want the re-ingested %v", got[0].Features, second.Features)
	}
	if got[0].Meta["v"] != "2" {
		t.Errorf("meta = %v, want the re-ingested {v:2}", got[0].Meta)
	}
	if !got[0].Timestamp.Equal(first.Timestamp) {
		t.Errorf("timestamp = %s, want the original %s: re-ingest replaces the payload, not the observation time",
			got[0].Timestamp.Format(time.RFC3339Nano), first.Timestamp.Format(time.RFC3339Nano))
	}

	// The adapter attribution moves with the update, so the row is
	// counted against the adapter that wrote it last.
	if n, err := s.EntityStates.Count(ctx, "adapter-a"); err != nil {
		t.Fatalf("Count(adapter-a): %v", err)
	} else if n != 0 {
		t.Errorf("Count(adapter-a) = %d, want 0 after the row was re-ingested by adapter-b", n)
	}
	if n, err := s.EntityStates.Count(ctx, "adapter-b"); err != nil {
		t.Fatalf("Count(adapter-b): %v", err)
	} else if n != 1 {
		t.Errorf("Count(adapter-b) = %d, want 1", n)
	}
}

// entityStateDuplicateTimestamps pins that two distinct observations
// sharing one timestamp are both kept and both returned.
func entityStateDuplicateTimestamps(t *testing.T, b Backend) {
	s := open(t, b)
	ctx := context.Background()
	scope := uuid.New()

	a := observation(scope, uuid.New(), base, 1)
	c := observation(scope, uuid.New(), base, 2)
	d := observation(scope, uuid.New(), base.Add(time.Hour), 3)
	for _, st := range []chronos.EntityState{a, c, d} {
		if err := s.EntityStates.Ingest(ctx, "adapter-a", st); err != nil {
			t.Fatalf("Ingest: %v", err)
		}
	}

	got, err := s.EntityStates.ListByScope(ctx, scope)
	if err != nil {
		t.Fatalf("ListByScope: %v", err)
	}
	if !sameIDSet(stateIDs(got), []uuid.UUID{a.ID, c.ID, d.ID}) {
		t.Errorf("ListByScope = %v, want all three observations", stateIDs(got))
	}
	if len(got) > 0 && got[0].ID != d.ID {
		t.Errorf("first result = %s, want the newest observation %s", got[0].ID, d.ID)
	}
	requireTimestampsNonIncreasing(t, got, "ListByScope")

	// The relative order of the two rows sharing a timestamp is NOT
	// part of the contract today and is not the same on every backend:
	// the in-memory store sorts stably and so preserves arrival order,
	// while the SQL backends return whatever the plan produces. The
	// assertion above is therefore membership plus non-increasing
	// timestamps, and the tie order is only logged.
	t.Logf("%s: tie order for equal timestamps was %v (arrival order was %v)",
		b.Name, stateIDs(got)[1:], []uuid.UUID{a.ID, c.ID})

	again, err := s.EntityStates.ListByScope(ctx, scope)
	if err != nil {
		t.Fatalf("ListByScope (repeat): %v", err)
	}
	if !sameIDs(stateIDs(again), stateIDs(got)) {
		t.Errorf("tie order is not stable between identical queries: %v then %v",
			stateIDs(got), stateIDs(again))
	}
}

// entityStateScopeIsolation pins that a scope query never leaks rows
// from another scope.
func entityStateScopeIsolation(t *testing.T, b Backend) {
	s := open(t, b)
	ctx := context.Background()
	mine, theirs := uuid.New(), uuid.New()

	a := observation(mine, uuid.New(), base)
	c := observation(theirs, uuid.New(), base.Add(time.Hour))
	for _, st := range []chronos.EntityState{a, c} {
		if err := s.EntityStates.Ingest(ctx, "adapter-a", st); err != nil {
			t.Fatalf("Ingest: %v", err)
		}
	}
	got, err := s.EntityStates.ListByScope(ctx, mine)
	if err != nil {
		t.Fatalf("ListByScope: %v", err)
	}
	if !sameIDs(stateIDs(got), []uuid.UUID{a.ID}) {
		t.Errorf("ListByScope(mine) = %v, want only %v", stateIDs(got), a.ID)
	}
	since, err := s.EntityStates.ListByScopeSince(ctx, mine, base.Add(-time.Hour))
	if err != nil {
		t.Fatalf("ListByScopeSince: %v", err)
	}
	if !sameIDs(stateIDs(since), []uuid.UUID{a.ID}) {
		t.Errorf("ListByScopeSince(mine) = %v, want only %v", stateIDs(since), a.ID)
	}
}

// entityStateListByEntity pins that an entity query spans scopes and
// excludes other entities.
func entityStateListByEntity(t *testing.T, b Backend) {
	s := open(t, b)
	ctx := context.Background()
	entity, other := uuid.New(), uuid.New()
	scopeA, scopeB := uuid.New(), uuid.New()

	inA := observation(scopeA, entity, base)
	inB := observation(scopeB, entity, base.Add(time.Hour))
	noise := observation(scopeA, other, base.Add(2*time.Hour))
	for _, st := range []chronos.EntityState{inA, inB, noise} {
		if err := s.EntityStates.Ingest(ctx, "adapter-a", st); err != nil {
			t.Fatalf("Ingest: %v", err)
		}
	}
	got, err := s.EntityStates.ListByEntity(ctx, entity)
	if err != nil {
		t.Fatalf("ListByEntity: %v", err)
	}
	if !sameIDs(stateIDs(got), []uuid.UUID{inB.ID, inA.ID}) {
		t.Errorf("ListByEntity = %v, want %v (both scopes, most recent first)",
			stateIDs(got), []uuid.UUID{inB.ID, inA.ID})
	}
}

// entityStateSinceBoundary pins the cutoff of ListByScopeSince as
// inclusive: the port documents "at or after cutoff".
func entityStateSinceBoundary(t *testing.T, b Backend) {
	s := open(t, b)
	ctx := context.Background()
	scope, entity := uuid.New(), uuid.New()

	before := observation(scope, entity, base.Add(-time.Second))
	at := observation(scope, entity, base)
	after := observation(scope, entity, base.Add(time.Second))
	for _, st := range []chronos.EntityState{before, at, after} {
		if err := s.EntityStates.Ingest(ctx, "adapter-a", st); err != nil {
			t.Fatalf("Ingest: %v", err)
		}
	}
	got, err := s.EntityStates.ListByScopeSince(ctx, scope, base)
	if err != nil {
		t.Fatalf("ListByScopeSince: %v", err)
	}
	if !sameIDs(stateIDs(got), []uuid.UUID{after.ID, at.ID}) {
		t.Errorf("ListByScopeSince(cutoff=base) = %v, want %v: the cutoff is inclusive and the older row is excluded",
			stateIDs(got), []uuid.UUID{after.ID, at.ID})
	}

	// A cutoff past every observation is an empty result, not an error.
	empty, err := s.EntityStates.ListByScopeSince(ctx, scope, base.Add(time.Hour))
	if err != nil {
		t.Fatalf("ListByScopeSince (future cutoff): %v", err)
	}
	if len(empty) != 0 {
		t.Errorf("ListByScopeSince (future cutoff) = %v, want empty", stateIDs(empty))
	}
}

// entityStateSinceSubSecond is the same cutoff below one second.
func entityStateSinceSubSecond(t *testing.T, b Backend) {
	s := open(t, b)
	ctx := context.Background()
	scope, entity := uuid.New(), uuid.New()

	at100 := observation(scope, entity, base.Add(100*time.Millisecond))
	at120 := observation(scope, entity, base.Add(120*time.Millisecond))
	at200 := observation(scope, entity, base.Add(200*time.Millisecond))
	for _, st := range []chronos.EntityState{at100, at120, at200} {
		if err := s.EntityStates.Ingest(ctx, "adapter-a", st); err != nil {
			t.Fatalf("Ingest: %v", err)
		}
	}
	got, err := s.EntityStates.ListByScopeSince(ctx, scope, base.Add(120*time.Millisecond))
	if err != nil {
		t.Fatalf("ListByScopeSince: %v", err)
	}

	if b.Has(QuirkLexicalSubSecondTime) {
		// ".1Z" >= ".12Z" is true as bytes and false in time, so the
		// row 20ms before the cutoff is returned as well. Pinned so
		// that repairing the backend fails here.
		if len(got) != 3 {
			t.Errorf("%s declares %s but returned %d rows for a sub-second cutoff; the lexical comparison returns all 3",
				b.Name, QuirkLexicalSubSecondTime, len(got))
		}
		return
	}
	if !sameIDSet(stateIDs(got), []uuid.UUID{at120.ID, at200.ID}) {
		t.Errorf("ListByScopeSince(cutoff=base+120ms) = %v, want the rows at +120ms and +200ms",
			stateIDs(got))
	}
}

// entityStateEmptyNotError pins that "nothing matched" is an empty
// result on every read path — the entity-state port has no not-found
// error and no backend may invent one.
func entityStateEmptyNotError(t *testing.T, b Backend) {
	s := open(t, b)
	ctx := context.Background()

	got, err := s.EntityStates.ListByScope(ctx, uuid.New())
	if err != nil {
		t.Errorf("ListByScope(unknown scope): %v, want no error", err)
	}
	if len(got) != 0 {
		t.Errorf("ListByScope(unknown scope) returned %d states, want 0", len(got))
	}
	if got, err := s.EntityStates.ListByScopeSince(ctx, uuid.New(), base); err != nil {
		t.Errorf("ListByScopeSince(unknown scope): %v, want no error", err)
	} else if len(got) != 0 {
		t.Errorf("ListByScopeSince(unknown scope) returned %d states, want 0", len(got))
	}
	if got, err := s.EntityStates.ListByEntity(ctx, uuid.New()); err != nil {
		t.Errorf("ListByEntity(unknown entity): %v, want no error", err)
	} else if len(got) != 0 {
		t.Errorf("ListByEntity(unknown entity) returned %d states, want 0", len(got))
	}
	if n, err := s.EntityStates.Count(ctx, "no-such-adapter"); err != nil {
		t.Errorf("Count(unknown adapter): %v, want no error", err)
	} else if n != 0 {
		t.Errorf("Count(unknown adapter) = %d, want 0", n)
	}
	if scopes, err := s.EntityStates.ListScopes(ctx); err != nil {
		t.Errorf("ListScopes(empty store): %v, want no error", err)
	} else if len(scopes) != 0 {
		t.Errorf("ListScopes(empty store) = %v, want none", scopes)
	}
	// Deleting from an empty store is a no-op, not an error.
	if err := s.EntityStates.DeleteOlderThan(ctx, base, "no-such-adapter"); err != nil {
		t.Errorf("DeleteOlderThan(empty store): %v, want no error", err)
	}
}

// entityStateCountPerAdapter pins that Count is scoped to one adapter.
func entityStateCountPerAdapter(t *testing.T, b Backend) {
	s := open(t, b)
	ctx := context.Background()
	scope := uuid.New()

	for i := 0; i < 3; i++ {
		st := observation(scope, uuid.New(), base.Add(time.Duration(i)*time.Minute))
		if err := s.EntityStates.Ingest(ctx, "adapter-a", st); err != nil {
			t.Fatalf("Ingest: %v", err)
		}
	}
	if err := s.EntityStates.Ingest(ctx, "adapter-b", observation(scope, uuid.New(), base)); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if n, err := s.EntityStates.Count(ctx, "adapter-a"); err != nil {
		t.Fatalf("Count: %v", err)
	} else if n != 3 {
		t.Errorf("Count(adapter-a) = %d, want 3", n)
	}
	if n, err := s.EntityStates.Count(ctx, "adapter-b"); err != nil {
		t.Fatalf("Count: %v", err)
	} else if n != 1 {
		t.Errorf("Count(adapter-b) = %d, want 1", n)
	}
}

// entityStateListScopes pins that ListScopes reports each scope that
// holds at least one observation, exactly once. The port leaves the
// order unspecified, so the assertion is on the set.
func entityStateListScopes(t *testing.T, b Backend) {
	s := open(t, b)
	ctx := context.Background()
	scopeA, scopeB := uuid.New(), uuid.New()

	for _, st := range []chronos.EntityState{
		observation(scopeA, uuid.New(), base),
		observation(scopeA, uuid.New(), base.Add(time.Minute)),
		observation(scopeB, uuid.New(), base),
	} {
		if err := s.EntityStates.Ingest(ctx, "adapter-a", st); err != nil {
			t.Fatalf("Ingest: %v", err)
		}
	}
	got, err := s.EntityStates.ListScopes(ctx)
	if err != nil {
		t.Fatalf("ListScopes: %v", err)
	}
	if !sameIDSet(got, []uuid.UUID{scopeA, scopeB}) {
		t.Errorf("ListScopes = %v, want exactly {%v, %v} with no repeats", got, scopeA, scopeB)
	}
}

// entityStateDeleteOlderThan pins retention over observations: the
// cutoff is strict, and the sweep touches only the named adapter.
func entityStateDeleteOlderThan(t *testing.T, b Backend) {
	s := open(t, b)
	ctx := context.Background()
	scope, entity := uuid.New(), uuid.New()

	old := observation(scope, entity, base.Add(-time.Hour))
	atCutoff := observation(scope, entity, base)
	recent := observation(scope, entity, base.Add(time.Hour))
	for _, st := range []chronos.EntityState{old, atCutoff, recent} {
		if err := s.EntityStates.Ingest(ctx, "adapter-a", st); err != nil {
			t.Fatalf("Ingest: %v", err)
		}
	}
	otherAdapter := observation(scope, entity, base.Add(-2*time.Hour))
	if err := s.EntityStates.Ingest(ctx, "adapter-b", otherAdapter); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	if err := s.EntityStates.DeleteOlderThan(ctx, base, "adapter-a"); err != nil {
		t.Fatalf("DeleteOlderThan: %v", err)
	}
	got, err := s.EntityStates.ListByScope(ctx, scope)
	if err != nil {
		t.Fatalf("ListByScope: %v", err)
	}
	want := []uuid.UUID{recent.ID, atCutoff.ID, otherAdapter.ID}
	if !sameIDSet(stateIDs(got), want) {
		t.Errorf("after DeleteOlderThan(base, adapter-a) the scope holds %v, want %v: "+
			"the cutoff is strict (the row at the cutoff survives) and other adapters are untouched",
			stateIDs(got), want)
	}
}

// entityStateTimestampFidelity pins what survives the trip through the
// driver: the instant, the declared precision, and UTC on the way back.
func entityStateTimestampFidelity(t *testing.T, b Backend) {
	s := open(t, b)
	ctx := context.Background()
	scope, entity := uuid.New(), uuid.New()

	// A timestamp carrying nanoseconds, supplied in a zone two hours
	// ahead of UTC, and one deliberately old observation from before
	// the Unix epoch.
	offset := time.FixedZone("plus-two", 2*60*60)
	precise := observation(scope, entity, time.Date(2024, 3, 1, 14, 0, 0, 123456789, offset))
	ancient := observation(scope, entity, time.Date(1969, 12, 31, 23, 59, 59, 0, time.UTC))
	for _, st := range []chronos.EntityState{precise, ancient} {
		if err := s.EntityStates.Ingest(ctx, "adapter-a", st); err != nil {
			t.Fatalf("Ingest %s: %v", st.Timestamp, err)
		}
	}
	got, err := s.EntityStates.ListByScope(ctx, scope)
	if err != nil {
		t.Fatalf("ListByScope: %v", err)
	}
	byID := map[uuid.UUID]chronos.EntityState{}
	for _, st := range got {
		byID[st.ID] = st
	}
	if len(byID) != 2 {
		t.Fatalf("ListByScope returned %d states, want 2", len(byID))
	}

	// Reads return UTC. The instant is what matters to a detector, but
	// the zone is what a consumer renders, and a consumer must not see
	// the wall clock move because the operator changed backend.
	for id, st := range byID {
		if st.Timestamp.Location() != time.UTC {
			t.Errorf("state %s came back in zone %v, want UTC", id, st.Timestamp.Location())
		}
	}

	wantPrecise := precise.Timestamp.Truncate(b.TimestampPrecision)
	gotPrecise := byID[precise.ID].Timestamp
	if !gotPrecise.Equal(wantPrecise) {
		t.Errorf("timestamp round-trip = %s, want %s (declared precision %v)",
			gotPrecise.Format(time.RFC3339Nano), wantPrecise.Format(time.RFC3339Nano), b.TimestampPrecision)
	}
	// The declared precision is a claim in both directions: a backend
	// that declares microseconds must actually lose the nanoseconds,
	// otherwise the declaration (and the divergence table it feeds) is
	// stale.
	if b.TimestampPrecision > time.Nanosecond && gotPrecise.Equal(precise.Timestamp) {
		t.Errorf("%s declares %v precision but kept the full nanosecond timestamp %s; update the declaration",
			b.Name, b.TimestampPrecision, gotPrecise.Format(time.RFC3339Nano))
	}
	if gotAncient := byID[ancient.ID].Timestamp; !gotAncient.Equal(ancient.Timestamp) {
		t.Errorf("pre-epoch timestamp round-trip = %s, want %s",
			gotAncient.Format(time.RFC3339Nano), ancient.Timestamp.Format(time.RFC3339Nano))
	}

	// Two observations one precision unit apart stay two observations,
	// in order.
	tick := uuid.New()
	early := observation(tick, entity, base.Add(500*time.Millisecond))
	late := observation(tick, entity, base.Add(500*time.Millisecond+b.TimestampPrecision))
	for _, st := range []chronos.EntityState{early, late} {
		if err := s.EntityStates.Ingest(ctx, "adapter-a", st); err != nil {
			t.Fatalf("Ingest: %v", err)
		}
	}
	ticked, err := s.EntityStates.ListByScope(ctx, tick)
	if err != nil {
		t.Fatalf("ListByScope: %v", err)
	}
	if len(ticked) != 2 {
		t.Fatalf("two observations %v apart collapsed into %d row(s)", b.TimestampPrecision, len(ticked))
	}
	if ticked[0].Timestamp.Equal(ticked[1].Timestamp) {
		t.Errorf("two observations %v apart came back with the same timestamp %s",
			b.TimestampPrecision, ticked[0].Timestamp.Format(time.RFC3339Nano))
	}
}

// entityStateConcurrency pins that the repositories are safe to use
// from several goroutines at once. Run under -race it is the only
// assertion that matters here; the row count afterwards confirms no
// write was dropped.
func entityStateConcurrency(t *testing.T, b Backend) {
	s := open(t, b)
	ctx := context.Background()
	scope := uuid.New()

	const writers, perWriter, readers = 4, 20, 4
	errCh := make(chan error, writers*perWriter+readers)
	var wg sync.WaitGroup

	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			entity := uuid.New()
			for i := 0; i < perWriter; i++ {
				st := observation(scope, entity, base.Add(time.Duration(w*perWriter+i)*time.Second))
				if err := s.EntityStates.Ingest(ctx, "adapter-a", st); err != nil {
					errCh <- fmt.Errorf("writer %d: ingest: %w", w, err)
					return
				}
			}
		}(w)
	}
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				states, err := s.EntityStates.ListByScope(ctx, scope)
				if err != nil {
					errCh <- fmt.Errorf("reader %d: list: %w", r, err)
					return
				}
				requireTimestampsNonIncreasing(t, states, "concurrent ListByScope")
			}
		}(r)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}

	n, err := s.EntityStates.Count(ctx, "adapter-a")
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if n != writers*perWriter {
		t.Errorf("Count after %d concurrent ingests = %d", writers*perWriter, n)
	}
}

func requireStateEqual(t *testing.T, want, got chronos.EntityState, precision time.Duration) {
	t.Helper()
	if got.ID != want.ID {
		t.Errorf("ID = %v, want %v", got.ID, want.ID)
	}
	if got.EntityID != want.EntityID {
		t.Errorf("EntityID = %v, want %v", got.EntityID, want.EntityID)
	}
	if got.ScopeID != want.ScopeID {
		t.Errorf("ScopeID = %v, want %v", got.ScopeID, want.ScopeID)
	}
	if wantTS := want.Timestamp.Truncate(precision); !got.Timestamp.Equal(wantTS) {
		t.Errorf("Timestamp = %s, want %s", got.Timestamp.Format(time.RFC3339Nano), wantTS.Format(time.RFC3339Nano))
	}
	if !sameFeatures(got.Features, want.Features) {
		t.Errorf("Features = %v, want %v", got.Features, want.Features)
	}
	if len(got.Labels) != len(want.Labels) {
		t.Errorf("Labels = %v, want %v", got.Labels, want.Labels)
	} else {
		for i := range want.Labels {
			if got.Labels[i] != want.Labels[i] {
				t.Errorf("Labels = %v, want %v", got.Labels, want.Labels)
				break
			}
		}
	}
	if len(got.Meta) != len(want.Meta) {
		t.Errorf("Meta = %v, want %v", got.Meta, want.Meta)
	} else {
		for k, v := range want.Meta {
			if got.Meta[k] != v {
				t.Errorf("Meta = %v, want %v", got.Meta, want.Meta)
				break
			}
		}
	}
}

func sameFeatures(a, b []float64) bool {
	return slices.Equal(a, b)
}
