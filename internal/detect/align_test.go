package detect

import (
	"testing"
	"time"

	"github.com/felixgeelhaar/chronos"
	"github.com/google/uuid"
)

func alignState(id, entity string, ts time.Time, y float64) chronos.EntityState {
	return chronos.EntityState{
		ID:        uuid.MustParse(id),
		EntityID:  uuid.MustParse(entity),
		ScopeID:   uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
		Timestamp: ts,
		Features:  []float64{y},
	}
}

func TestAlignNearest_Exact(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	a := []chronos.EntityState{
		alignState("00000000-0000-0000-0000-000000000001", "10000000-0000-0000-0000-000000000001", t0, 1),
		alignState("00000000-0000-0000-0000-000000000002", "10000000-0000-0000-0000-000000000001", t0.Add(time.Minute), 2),
	}
	b := []chronos.EntityState{
		alignState("00000000-0000-0000-0000-000000000003", "10000000-0000-0000-0000-000000000002", t0.Add(time.Minute), 20),
		alignState("00000000-0000-0000-0000-000000000004", "10000000-0000-0000-0000-000000000002", t0, 10),
	}
	got := AlignNearest(a, b, 0)
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2", len(got))
	}
	if got[0].A.Outcome() != 1 || got[0].B.Outcome() != 10 {
		t.Fatalf("first pair outcomes = %v,%v want 1,10", got[0].A.Outcome(), got[0].B.Outcome())
	}
	if got[1].A.Outcome() != 2 || got[1].B.Outcome() != 20 {
		t.Fatalf("second pair outcomes = %v,%v want 2,20", got[1].A.Outcome(), got[1].B.Outcome())
	}
}

func TestAlignNearest_DisjointTimelines(t *testing.T) {
	morning := time.Date(2026, 1, 1, 8, 0, 0, 0, time.UTC)
	afternoon := time.Date(2026, 1, 1, 16, 0, 0, 0, time.UTC)
	a := []chronos.EntityState{
		alignState("00000000-0000-0000-0000-000000000001", "10000000-0000-0000-0000-000000000001", morning, 1),
	}
	b := []chronos.EntityState{
		alignState("00000000-0000-0000-0000-000000000002", "10000000-0000-0000-0000-000000000002", afternoon, 1),
	}
	if got := AlignNearest(a, b, 0); len(got) != 0 {
		t.Fatalf("exact align of disjoint timelines = %d pairs, want 0", len(got))
	}
	if got := AlignNearest(a, b, time.Hour); len(got) != 0 {
		t.Fatalf("1h tolerance still disjoint = %d pairs, want 0", len(got))
	}
}

func TestAlignNearest_WithinTolerance(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	a := []chronos.EntityState{
		alignState("00000000-0000-0000-0000-000000000001", "10000000-0000-0000-0000-000000000001", t0, 1),
	}
	b := []chronos.EntityState{
		alignState("00000000-0000-0000-0000-000000000002", "10000000-0000-0000-0000-000000000002", t0.Add(30*time.Second), 2),
	}
	if got := AlignNearest(a, b, 0); len(got) != 0 {
		t.Fatal("offset outside exact tolerance paired")
	}
	got := AlignNearest(a, b, time.Minute)
	if len(got) != 1 {
		t.Fatalf("within 1m = %d pairs, want 1", len(got))
	}
}

func TestAlignNearest_EqualDistancePrefersEarlier(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	a := []chronos.EntityState{
		alignState("00000000-0000-0000-0000-000000000001", "10000000-0000-0000-0000-000000000001", t0, 1),
	}
	earlier := alignState("00000000-0000-0000-0000-00000000000a", "10000000-0000-0000-0000-000000000002", t0.Add(-time.Hour), 2)
	later := alignState("00000000-0000-0000-0000-00000000000b", "10000000-0000-0000-0000-000000000002", t0.Add(time.Hour), 3)
	// Feed the later candidate first so a non-deterministic or
	// first-seen match would pick it.
	got := AlignNearest(a, []chronos.EntityState{later, earlier}, 2*time.Hour)
	if len(got) != 1 {
		t.Fatalf("len = %d, want 1", len(got))
	}
	if got[0].B.ID != earlier.ID {
		t.Fatalf("matched %s, want earlier candidate %s", got[0].B.ID, earlier.ID)
	}
}

func TestAlignNearest_EachObservationOnce(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	a := []chronos.EntityState{
		alignState("00000000-0000-0000-0000-000000000001", "10000000-0000-0000-0000-000000000001", t0, 1),
		alignState("00000000-0000-0000-0000-000000000002", "10000000-0000-0000-0000-000000000001", t0.Add(time.Second), 2),
	}
	b := []chronos.EntityState{
		alignState("00000000-0000-0000-0000-000000000003", "10000000-0000-0000-0000-000000000002", t0, 9),
	}
	got := AlignNearest(a, b, time.Minute)
	if len(got) != 1 {
		t.Fatalf("len = %d, want 1 (one B cannot serve two A's)", len(got))
	}
	if got[0].A.Outcome() != 1 {
		t.Fatalf("claimed A outcome = %v, want the earlier A", got[0].A.Outcome())
	}
}

func TestAlignNearest_DeterministicUnderReorder(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	mk := func(n int, entity string, shift time.Duration) []chronos.EntityState {
		out := make([]chronos.EntityState, n)
		for i := range out {
			id := uuid.MustParse("00000000-0000-0000-0000-" + sprintf12(i+1))
			out[i] = chronos.EntityState{
				ID: id, EntityID: uuid.MustParse(entity),
				ScopeID:   uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
				Timestamp: t0.Add(time.Duration(i)*time.Minute + shift),
				Features:  []float64{float64(i)},
			}
		}
		return out
	}
	a := mk(5, "10000000-0000-0000-0000-000000000001", 0)
	b := mk(5, "10000000-0000-0000-0000-000000000002", 20*time.Second)
	forward := AlignNearest(a, b, time.Minute)
	// Reverse both inputs.
	rev := func(s []chronos.EntityState) []chronos.EntityState {
		out := append([]chronos.EntityState(nil), s...)
		for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
			out[i], out[j] = out[j], out[i]
		}
		return out
	}
	backward := AlignNearest(rev(a), rev(b), time.Minute)
	if len(forward) != len(backward) {
		t.Fatalf("len forward %d backward %d", len(forward), len(backward))
	}
	for i := range forward {
		if forward[i].A.ID != backward[i].A.ID || forward[i].B.ID != backward[i].B.ID {
			t.Fatalf("pair %d differs under reorder", i)
		}
	}
}

func TestAlignNearest_DuplicateTimestampPrefersSmallerID(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	a := []chronos.EntityState{
		alignState("00000000-0000-0000-0000-000000000001", "10000000-0000-0000-0000-000000000001", t0, 1),
	}
	low := alignState("00000000-0000-0000-0000-000000000002", "10000000-0000-0000-0000-000000000002", t0, 2)
	high := alignState("00000000-0000-0000-0000-000000000003", "10000000-0000-0000-0000-000000000002", t0, 3)
	got := AlignNearest(a, []chronos.EntityState{high, low}, 0)
	if len(got) != 1 {
		t.Fatalf("len = %d, want 1", len(got))
	}
	if got[0].B.ID != low.ID {
		t.Fatalf("matched %s, want smaller id %s", got[0].B.ID, low.ID)
	}
}

func TestAlignNearest_MultipleCandidatesTakesNearest(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	a := []chronos.EntityState{
		alignState("00000000-0000-0000-0000-000000000001", "10000000-0000-0000-0000-000000000001", t0, 1),
	}
	near := alignState("00000000-0000-0000-0000-00000000000b", "10000000-0000-0000-0000-000000000002", t0.Add(10*time.Second), 2)
	far := alignState("00000000-0000-0000-0000-00000000000a", "10000000-0000-0000-0000-000000000002", t0.Add(50*time.Second), 3)
	// Smaller ID on the farther candidate, so an ID-first matcher would miss.
	got := AlignNearest(a, []chronos.EntityState{far, near}, time.Minute)
	if len(got) != 1 || got[0].B.ID != near.ID {
		t.Fatalf("matched %+v, want the 10s candidate", got)
	}
}

func TestAlignNearest_DifferentCadenceKeepsSharedInstants(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	var a, b []chronos.EntityState
	for i := 0; i < 6; i++ {
		a = append(a, alignState(
			"00000000-0000-0000-0000-"+sprintf12(i+1),
			"10000000-0000-0000-0000-000000000001",
			t0.Add(time.Duration(i)*time.Minute),
			float64(i),
		))
	}
	for i := 0; i < 3; i++ {
		b = append(b, alignState(
			"00000000-0000-0000-0000-"+sprintf12(i+10),
			"10000000-0000-0000-0000-000000000002",
			t0.Add(time.Duration(i*2)*time.Minute),
			float64(i*2),
		))
	}
	got := AlignNearest(a, b, 0)
	if len(got) != 3 {
		t.Fatalf("len = %d, want the three shared minutes", len(got))
	}
	for i, p := range got {
		if !p.A.Timestamp.Equal(p.B.Timestamp) {
			t.Fatalf("pair %d timestamps differ: %s vs %s", i, p.A.Timestamp, p.B.Timestamp)
		}
	}
}

func sprintf12(n int) string {
	const hex = "0123456789abcdef"
	s := make([]byte, 12)
	for i := 11; i >= 0; i-- {
		s[i] = hex[n%16]
		n /= 16
	}
	return string(s)
}
