package conformance

import (
	"context"
	"testing"
	"time"

	"github.com/felixgeelhaar/chronos/internal/domain"
	"github.com/felixgeelhaar/chronos/internal/ports"
	"github.com/google/uuid"
)

// signalCursorPaging pins ports.SignalFilter.After: strictly after the
// cursor in (detected_at, id) order, oldest first, with Limit applied
// after the cursor rather than before it.
//
// Ties are the point. The scheduler stamps every signal of one run with
// the same DetectedAt, so a production tick is hundreds of rows sharing a
// timestamp. A cursor that compared time alone, or a limit applied before
// the tie-break, loses or repeats rows at exactly that boundary -- and a
// consumer paging without a limit instead is what timed out on every poll
// in production once its backlog passed ~50k signals.
func signalCursorPaging(t *testing.T, b Backend) {
	s := open(t, b)
	ctx := context.Background()
	scope := uuid.New()

	// 3 ticks of 4 signals each, every tick sharing one timestamp, plus a
	// signal in another scope that must never appear.
	var saved []domain.Signal
	for tick := 0; tick < 3; tick++ {
		at := base.Add(time.Duration(tick) * time.Minute)
		for i := 0; i < 4; i++ {
			sig := signalFixture(scope, uuid.New(), at, 0.5)
			sig.Evidence[0].Score = float64(tick*10 + i) // identifies the owner
			if err := s.Signals.Save(ctx, sig); err != nil {
				t.Fatalf("Save: %v", err)
			}
			saved = append(saved, sig)
		}
	}
	if err := s.Signals.Save(ctx, signalFixture(uuid.New(), uuid.New(), base, 0.5)); err != nil {
		t.Fatalf("Save other scope: %v", err)
	}
	byID := make(map[uuid.UUID]domain.Signal, len(saved))
	for _, sig := range saved {
		byID[sig.ID] = sig
	}

	// Page from before the first tick with a limit that splits ticks.
	cursor := &ports.SignalCursor{DetectedAt: base.Add(-time.Second)}
	seen := map[uuid.UUID]bool{}
	var prev *domain.Signal
	for page := 0; page < 10; page++ {
		got, err := s.Signals.List(ctx, ports.SignalFilter{ScopeID: scope, After: cursor, Limit: 5})
		if err != nil {
			t.Fatalf("List page %d: %v", page, err)
		}
		for i := range got {
			sig := got[i]
			if seen[sig.ID] {
				t.Fatalf("page %d repeats %s: the cursor is not strict", page, sig.ID)
			}
			seen[sig.ID] = true
			if prev != nil && afterCursor(*prev, sig) {
				t.Fatalf("page %d: %s at %v comes before %s at %v; After must page oldest first",
					page, sig.ID, sig.DetectedAt, prev.ID, prev.DetectedAt)
			}
			prev = &got[i]
			// Batched evidence must still land on its own signal.
			want := byID[sig.ID]
			if len(sig.Evidence) != 1 || sig.Evidence[0].Score != want.Evidence[0].Score {
				t.Fatalf("signal %s carries evidence %+v, want its own (score %v)", sig.ID, sig.Evidence, want.Evidence[0].Score)
			}
		}
		if len(got) < 5 {
			break
		}
		last := got[len(got)-1]
		cursor = &ports.SignalCursor{DetectedAt: last.DetectedAt, ID: last.ID}
	}
	if len(seen) != len(saved) {
		t.Fatalf("paging returned %d of %d signals: rows at a shared timestamp were skipped", len(seen), len(saved))
	}

	// A cursor at the newest row is an empty page, not the newest row again.
	got, err := s.Signals.List(ctx, ports.SignalFilter{ScopeID: scope, After: &ports.SignalCursor{DetectedAt: prev.DetectedAt, ID: prev.ID}, Limit: 5})
	if err != nil {
		t.Fatalf("List past the end: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("List past the end = %v, want an empty page", signalIDs(got))
	}
}

// afterCursor reports whether a sorts strictly after b in (DetectedAt, ID) order.
func afterCursor(a, b domain.Signal) bool {
	if !a.DetectedAt.Equal(b.DetectedAt) {
		return a.DetectedAt.After(b.DetectedAt)
	}
	return a.ID.String() > b.ID.String()
}
