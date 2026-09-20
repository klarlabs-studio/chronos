package conformance

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/felixgeelhaar/chronos/internal/domain"
	"github.com/felixgeelhaar/chronos/internal/ports"
	"github.com/google/uuid"
)

func signalGroups() []group {
	return []group{
		{"Signal/RoundTrip", signalRoundTrip},
		{"Signal/ListMatchesGet", signalListMatchesGet},
		{"Signal/NotFoundVersusEmpty", signalNotFoundVersusEmpty},
		{"Signal/Ordering", signalOrdering},
		{"Signal/OrderingSubSecond", signalOrderingSubSecond},
		{"Signal/DuplicateID", signalDuplicateID},
		{"Signal/Filters", signalFilters},
		{"Signal/ScopeIDWithScopeIDs", signalScopeIDWithScopeIDs},
		{"Signal/LimitAndPaging", signalLimitAndPaging},
		{"Signal/Retention", signalRetention},
		{"Signal/Concurrency", signalConcurrency},
	}
}

// signalFixture builds a valid signal with every optional field
// populated, so that a backend dropping one of them is visible.
func signalFixture(scope, series uuid.UUID, detectedAt time.Time, confidence float64) domain.Signal {
	return domain.Signal{
		ID:         uuid.New(),
		ScopeID:    scope,
		Series:     series,
		Pattern:    domain.PatternTypeTrend,
		DetectedAt: detectedAt,
		Window:     domain.TimeWindow{Start: detectedAt.Add(-time.Hour), End: detectedAt},
		Strength:   0.5,
		Confidence: confidence,
		Metrics:    map[string]float64{"slope": 1.25, "r2": 0.9},
		Evidence: []domain.Evidence{{
			Series:  series,
			Time:    detectedAt.Add(-30 * time.Minute),
			Kind:    "baseline_deviation",
			Score:   2.5,
			Metrics: map[string]float64{"z_score": 2.5},
		}},
		Explanation: domain.Explanation{
			FeatureEvolution: []domain.FeatureSample{
				{At: detectedAt.Add(-time.Hour), Value: 1},
				{At: detectedAt, Value: 4},
			},
			ComparablePeers:    7,
			BaselineWindowDays: 14,
			ThresholdUsed:      2.0,
			DetectorVersion:    "trend/v3",
		},
		ConfidenceClass: domain.ConfidenceClassEstablished,
	}
}

func signalIDs(sigs []domain.Signal) []uuid.UUID {
	out := make([]uuid.UUID, len(sigs))
	for i, s := range sigs {
		out[i] = s.ID
	}
	return out
}

// signalRoundTrip pins that everything a detector emits survives a
// save and a Get, including evidence, metrics, the explanation value
// object and the confidence class.
func signalRoundTrip(t *testing.T, b Backend) {
	s := open(t, b)
	ctx := context.Background()
	want := signalFixture(uuid.New(), uuid.New(), base, 0.8)

	if err := s.Signals.Save(ctx, want); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, err := s.Signals.Get(ctx, want.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	requireSignalEqual(t, want, got, b.TimestampPrecision)
}

// signalListMatchesGet pins that the two read paths return the same
// signal. They are separate queries on every SQL backend, and a column
// missing from one of them is a silent, backend-specific data loss.
func signalListMatchesGet(t *testing.T, b Backend) {
	s := open(t, b)
	ctx := context.Background()
	scope := uuid.New()
	want := signalFixture(scope, uuid.New(), base, 0.8)

	if err := s.Signals.Save(ctx, want); err != nil {
		t.Fatalf("Save: %v", err)
	}
	listed, err := s.Signals.List(ctx, ports.SignalFilter{ScopeID: scope})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(listed) != 1 {
		t.Fatalf("List returned %d signals, want 1", len(listed))
	}
	requireSignalEqual(t, want, listed[0], b.TimestampPrecision)
}

// signalNotFoundVersusEmpty pins the distinction the ports draw and
// every backend must draw identically: Get of an absent ID is
// domain.ErrSignalNotFound, a query that matches nothing is an empty
// result and no error.
func signalNotFoundVersusEmpty(t *testing.T, b Backend) {
	s := open(t, b)
	ctx := context.Background()

	_, err := s.Signals.Get(ctx, uuid.New())
	if !errors.Is(err, domain.ErrSignalNotFound) {
		t.Errorf("Get(absent id) error = %v, want domain.ErrSignalNotFound", err)
	}
	got, err := s.Signals.List(ctx, ports.SignalFilter{ScopeID: uuid.New()})
	if err != nil {
		t.Errorf("List(no match): %v, want no error", err)
	}
	if len(got) != 0 {
		t.Errorf("List(no match) returned %d signals, want 0", len(got))
	}
	if n, err := s.Signals.Count(ctx, ports.SignalFilter{ScopeID: uuid.New()}); err != nil {
		t.Errorf("Count(no match): %v, want no error", err)
	} else if n != 0 {
		t.Errorf("Count(no match) = %d, want 0", n)
	}
	// An unfiltered query against an empty store is empty, not an error.
	if got, err := s.Signals.List(ctx, ports.SignalFilter{}); err != nil {
		t.Errorf("List(empty filter, empty store): %v, want no error", err)
	} else if len(got) != 0 {
		t.Errorf("List(empty filter, empty store) returned %d signals, want 0", len(got))
	}
}

// signalOrdering pins the documented total order: detected-at
// descending, then confidence descending.
func signalOrdering(t *testing.T, b Backend) {
	s := open(t, b)
	ctx := context.Background()
	scope, series := uuid.New(), uuid.New()

	// Saved in an order that is neither the detection order nor the
	// result order.
	oldWeak := signalFixture(scope, series, base, 0.2)
	newStrong := signalFixture(scope, series, base.Add(time.Hour), 0.9)
	newWeak := signalFixture(scope, series, base.Add(time.Hour), 0.4)
	for _, sig := range []domain.Signal{newWeak, oldWeak, newStrong} {
		if err := s.Signals.Save(ctx, sig); err != nil {
			t.Fatalf("Save: %v", err)
		}
	}
	got, err := s.Signals.List(ctx, ports.SignalFilter{ScopeID: scope})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	want := []uuid.UUID{newStrong.ID, newWeak.ID, oldWeak.ID}
	if !sameIDs(signalIDs(got), want) {
		t.Errorf("List order = %v, want %v (detected-at desc, then confidence desc)",
			signalIDs(got), want)
	}
	again, err := s.Signals.List(ctx, ports.SignalFilter{ScopeID: scope})
	if err != nil {
		t.Fatalf("List (repeat): %v", err)
	}
	if !sameIDs(signalIDs(again), signalIDs(got)) {
		t.Errorf("List is not deterministic: %v then %v", signalIDs(got), signalIDs(again))
	}
}

// signalOrderingSubSecond is the signal-side instance of the same
// sub-second comparison contract the entity states have.
func signalOrderingSubSecond(t *testing.T, b Backend) {
	s := open(t, b)
	ctx := context.Background()
	scope, series := uuid.New(), uuid.New()

	at100 := signalFixture(scope, series, base.Add(100*time.Millisecond), 0.5)
	at120 := signalFixture(scope, series, base.Add(120*time.Millisecond), 0.5)
	at200 := signalFixture(scope, series, base.Add(200*time.Millisecond), 0.5)
	for _, sig := range []domain.Signal{at100, at120, at200} {
		if err := s.Signals.Save(ctx, sig); err != nil {
			t.Fatalf("Save: %v", err)
		}
	}
	got, err := s.Signals.List(ctx, ports.SignalFilter{ScopeID: scope})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	chronological := []uuid.UUID{at200.ID, at120.ID, at100.ID}

	if b.Has(QuirkLexicalSubSecondTime) {
		lexical := []uuid.UUID{at200.ID, at100.ID, at120.ID}
		if !sameIDs(signalIDs(got), lexical) {
			t.Errorf("%s declares %s but ordered %v; lexical order is %v, chronological order is %v",
				b.Name, QuirkLexicalSubSecondTime, signalIDs(got), lexical, chronological)
		}
		return
	}
	if !sameIDs(signalIDs(got), chronological) {
		t.Errorf("sub-second List order = %v, want %v", signalIDs(got), chronological)
	}
}

// signalDuplicateID pins Save's idempotency on Signal.ID.
//
// A signal is a historical fact: re-saving one revises the detector's
// quantities (strength, confidence, metrics, explanation, class) and
// replaces its evidence, but does not move the perception itself —
// pattern, series, scope, detected-at and the analysis window are the
// signal's identity and the SQL backends' ON CONFLICT clauses leave
// them alone.
func signalDuplicateID(t *testing.T, b Backend) {
	s := open(t, b)
	ctx := context.Background()
	scope, series := uuid.New(), uuid.New()

	first := signalFixture(scope, series, base, 0.4)
	if err := s.Signals.Save(ctx, first); err != nil {
		t.Fatalf("Save: %v", err)
	}

	second := signalFixture(scope, series, base.Add(time.Hour), 0.95)
	second.ID = first.ID
	second.Pattern = domain.PatternTypeSpike
	second.Strength = 0.75
	second.Metrics = map[string]float64{"z_score": 4}
	second.ConfidenceClass = domain.ConfidenceClassStrong
	second.Evidence = []domain.Evidence{{
		Series: series, Time: base, Kind: "revised", Score: 9,
	}}
	if err := s.Signals.Save(ctx, second); err != nil {
		t.Fatalf("Save (duplicate ID): %v", err)
	}

	all, err := s.Signals.List(ctx, ports.SignalFilter{ScopeID: scope})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(all) != 1 {
		t.Fatalf("after re-saving signal %s the store holds %d signals, want 1 (Save is idempotent on ID)",
			first.ID, len(all))
	}
	got := all[0]
	if got.Confidence != second.Confidence {
		t.Errorf("Confidence = %v, want the re-saved %v", got.Confidence, second.Confidence)
	}
	if got.Strength != second.Strength {
		t.Errorf("Strength = %v, want the re-saved %v", got.Strength, second.Strength)
	}
	if got.ConfidenceClass != second.ConfidenceClass {
		t.Errorf("ConfidenceClass = %q, want the re-saved %q", got.ConfidenceClass, second.ConfidenceClass)
	}
	if got.Metrics["z_score"] != 4 {
		t.Errorf("Metrics = %v, want the re-saved {z_score:4}", got.Metrics)
	}
	if len(got.Evidence) != 1 || got.Evidence[0].Kind != "revised" {
		t.Errorf("Evidence = %+v, want exactly the re-saved evidence (evidence is replaced, not appended)", got.Evidence)
	}
	if !got.DetectedAt.Equal(first.DetectedAt.Truncate(b.TimestampPrecision)) {
		t.Errorf("DetectedAt = %s, want the original %s: re-saving revises the detector's quantities, not the perception's identity",
			got.DetectedAt.Format(time.RFC3339Nano), first.DetectedAt.Format(time.RFC3339Nano))
	}
	if got.Pattern != first.Pattern {
		t.Errorf("Pattern = %q, want the original %q", got.Pattern, first.Pattern)
	}
	if !got.Window.Start.Equal(first.Window.Start.Truncate(b.TimestampPrecision)) {
		t.Errorf("Window.Start = %s, want the original %s",
			got.Window.Start.Format(time.RFC3339Nano), first.Window.Start.Format(time.RFC3339Nano))
	}
}

// signalFilters pins every predicate on ports.SignalFilter, including
// the half-open [Since, Until) interval and the exact-match window.
func signalFilters(t *testing.T, b Backend) {
	s := open(t, b)
	ctx := context.Background()
	scopeA, scopeB := uuid.New(), uuid.New()
	seriesA, seriesB := uuid.New(), uuid.New()

	a := signalFixture(scopeA, seriesA, base, 0.9)
	bb := signalFixture(scopeA, seriesB, base.Add(time.Hour), 0.3)
	c := signalFixture(scopeB, seriesA, base.Add(2*time.Hour), 0.6)
	bb.Pattern = domain.PatternTypeSpike
	for _, sig := range []domain.Signal{a, bb, c} {
		if err := s.Signals.Save(ctx, sig); err != nil {
			t.Fatalf("Save: %v", err)
		}
	}

	pattern := domain.PatternTypeSpike
	minConf := 0.6
	since := base.Add(time.Hour)
	until := base.Add(2 * time.Hour)
	window := domain.TimeWindow{Start: base.Add(-time.Hour), End: base}

	cases := []struct {
		name   string
		filter ports.SignalFilter
		want   []uuid.UUID
	}{
		{"scope", ports.SignalFilter{ScopeID: scopeA}, []uuid.UUID{bb.ID, a.ID}},
		{"scope set", ports.SignalFilter{ScopeIDs: []uuid.UUID{scopeA, scopeB}}, []uuid.UUID{c.ID, bb.ID, a.ID}},
		{"series", ports.SignalFilter{Series: &seriesA}, []uuid.UUID{c.ID, a.ID}},
		{"pattern", ports.SignalFilter{Pattern: &pattern}, []uuid.UUID{bb.ID}},
		{"min confidence", ports.SignalFilter{MinConfidence: &minConf}, []uuid.UUID{c.ID, a.ID}},
		// [Since, Until): the signal at Since is in, the one at Until is out.
		{"since inclusive until exclusive", ports.SignalFilter{Since: &since, Until: &until}, []uuid.UUID{bb.ID}},
		{"since only", ports.SignalFilter{Since: &since}, []uuid.UUID{c.ID, bb.ID}},
		{"until only", ports.SignalFilter{Until: &until}, []uuid.UUID{bb.ID, a.ID}},
		// The window is a signal's perception identity together with
		// scope, series and pattern; both bounds must match exactly.
		{"window exact", ports.SignalFilter{Window: &window}, []uuid.UUID{a.ID}},
		{"empty filter matches everything", ports.SignalFilter{}, []uuid.UUID{c.ID, bb.ID, a.ID}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := s.Signals.List(ctx, tc.filter)
			if err != nil {
				t.Fatalf("List: %v", err)
			}
			if !sameIDs(signalIDs(got), tc.want) {
				t.Errorf("List = %v, want %v", signalIDs(got), tc.want)
			}
			n, err := s.Signals.Count(ctx, tc.filter)
			if err != nil {
				t.Fatalf("Count: %v", err)
			}
			if n != int64(len(tc.want)) {
				t.Errorf("Count = %d, want %d (Count and List must agree on the same filter)", n, len(tc.want))
			}
		})
	}

	// A window that matches only one of the two bounds matches nothing.
	half := domain.TimeWindow{Start: window.Start, End: window.End.Add(time.Minute)}
	if got, err := s.Signals.List(ctx, ports.SignalFilter{Window: &half}); err != nil {
		t.Fatalf("List: %v", err)
	} else if len(got) != 0 {
		t.Errorf("List(window with one bound off) = %v, want nothing", signalIDs(got))
	}
}

// signalScopeIDWithScopeIDs pins what happens when both scope
// predicates are set at once.
func signalScopeIDWithScopeIDs(t *testing.T, b Backend) {
	s := open(t, b)
	ctx := context.Background()
	scopeA, scopeB, scopeC := uuid.New(), uuid.New(), uuid.New()

	a := signalFixture(scopeA, uuid.New(), base, 0.5)
	bb := signalFixture(scopeB, uuid.New(), base.Add(time.Hour), 0.5)
	c := signalFixture(scopeC, uuid.New(), base.Add(2*time.Hour), 0.5)
	for _, sig := range []domain.Signal{a, bb, c} {
		if err := s.Signals.Save(ctx, sig); err != nil {
			t.Fatalf("Save: %v", err)
		}
	}

	// Every backend intersects the two predicates: ScopeID AND
	// ScopeIDs. The port's doc comment says the opposite ("ScopeID acts
	// as a single additional allowed scope", i.e. a union). The
	// behaviour is uniform across backends, so this suite pins the
	// behaviour; the doc comment is the thing that is wrong, and it is
	// reported as such rather than silently satisfied by a looser
	// assertion here.
	got, err := s.Signals.List(ctx, ports.SignalFilter{ScopeID: scopeA, ScopeIDs: []uuid.UUID{scopeA, scopeB}})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if !sameIDs(signalIDs(got), []uuid.UUID{a.ID}) {
		t.Errorf("List(ScopeID=A, ScopeIDs={A,B}) = %v, want only %v: the two predicates intersect",
			signalIDs(got), a.ID)
	}
	// Disjoint predicates therefore match nothing.
	got, err = s.Signals.List(ctx, ports.SignalFilter{ScopeID: scopeC, ScopeIDs: []uuid.UUID{scopeA, scopeB}})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("List(ScopeID=C, ScopeIDs={A,B}) = %v, want nothing", signalIDs(got))
	}
}

// signalLimitAndPaging pins the only paging primitive the port has.
//
// SignalFilter carries a Limit and no offset or cursor, so a page is
// always the head of the ordering: a consumer can ask for the newest N
// and cannot ask for the next N. The boundaries that do exist —
// zero, negative, exact, over-large, and a limit against an empty
// result — must behave the same everywhere.
func signalLimitAndPaging(t *testing.T, b Backend) {
	s := open(t, b)
	ctx := context.Background()
	scope, series := uuid.New(), uuid.New()

	const total = 5
	saved := make([]domain.Signal, 0, total)
	for i := 0; i < total; i++ {
		sig := signalFixture(scope, series, base.Add(time.Duration(i)*time.Hour), 0.5)
		if err := s.Signals.Save(ctx, sig); err != nil {
			t.Fatalf("Save: %v", err)
		}
		saved = append(saved, sig)
	}
	newestFirst := make([]uuid.UUID, 0, total)
	for i := total - 1; i >= 0; i-- {
		newestFirst = append(newestFirst, saved[i].ID)
	}

	cases := []struct {
		name  string
		limit int
		want  []uuid.UUID
	}{
		{"zero means unlimited", 0, newestFirst},
		{"negative means unlimited", -1, newestFirst},
		{"one returns the newest", 1, newestFirst[:1]},
		{"partial page", 3, newestFirst[:3]},
		{"exactly the row count", total, newestFirst},
		{"larger than the row count", total + 4, newestFirst},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := s.Signals.List(ctx, ports.SignalFilter{ScopeID: scope, Limit: tc.limit})
			if err != nil {
				t.Fatalf("List: %v", err)
			}
			if !sameIDs(signalIDs(got), tc.want) {
				t.Errorf("List(limit=%d) = %v, want %v", tc.limit, signalIDs(got), tc.want)
			}
			// Limit truncates the result, it does not narrow the
			// query: Count reports the matching rows either way.
			n, err := s.Signals.Count(ctx, ports.SignalFilter{ScopeID: scope, Limit: tc.limit})
			if err != nil {
				t.Fatalf("Count: %v", err)
			}
			if n != total {
				t.Errorf("Count(limit=%d) = %d, want %d: Limit must not affect Count", tc.limit, n, total)
			}
		})
	}

	// A limit over an empty result is an empty page, not an error.
	got, err := s.Signals.List(ctx, ports.SignalFilter{ScopeID: uuid.New(), Limit: 3})
	if err != nil {
		t.Fatalf("List(limit over empty result): %v", err)
	}
	if len(got) != 0 {
		t.Errorf("List(limit over empty result) = %v, want an empty page", signalIDs(got))
	}
}

// signalRetention pins ports.SignalRetainer: a strict cutoff, a bounded
// batch that takes the oldest rows first, an honest row count, and
// evidence that goes with the signal.
func signalRetention(t *testing.T, b Backend) {
	s := open(t, b)
	ctx := context.Background()
	retainer, ok := s.Signals.(ports.SignalRetainer)
	if !ok {
		t.Skipf("%s: signal repository does not implement ports.SignalRetainer", b.Name)
	}
	scope, series := uuid.New(), uuid.New()

	cutoff := base.Add(10 * time.Hour)
	// Four rows strictly before the cutoff, one exactly on it, one after.
	var old []domain.Signal
	for i := 0; i < 4; i++ {
		sig := signalFixture(scope, series, base.Add(time.Duration(i)*time.Hour), 0.5)
		old = append(old, sig)
		if err := s.Signals.Save(ctx, sig); err != nil {
			t.Fatalf("Save: %v", err)
		}
	}
	atCutoff := signalFixture(scope, series, cutoff, 0.5)
	after := signalFixture(scope, series, cutoff.Add(time.Hour), 0.5)
	for _, sig := range []domain.Signal{atCutoff, after} {
		if err := s.Signals.Save(ctx, sig); err != nil {
			t.Fatalf("Save: %v", err)
		}
	}

	// A bounded sweep deletes exactly limit rows and takes the oldest
	// first, so repeated sweeps drain the backlog from the far end
	// rather than nibbling an arbitrary subset.
	n, err := retainer.DeleteSignalsOlderThan(ctx, cutoff, 2)
	if err != nil {
		t.Fatalf("DeleteSignalsOlderThan(limit=2): %v", err)
	}
	if n != 2 {
		t.Errorf("DeleteSignalsOlderThan(limit=2) = %d, want 2", n)
	}
	remaining, err := s.Signals.List(ctx, ports.SignalFilter{ScopeID: scope})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	want := []uuid.UUID{after.ID, atCutoff.ID, old[3].ID, old[2].ID}
	if !sameIDs(signalIDs(remaining), want) {
		t.Errorf("after a sweep of 2 the store holds %v, want %v (the two oldest went)",
			signalIDs(remaining), want)
	}
	if _, err := s.Signals.Get(ctx, old[0].ID); !errors.Is(err, domain.ErrSignalNotFound) {
		t.Errorf("Get(swept signal) error = %v, want domain.ErrSignalNotFound", err)
	}

	// An unbounded sweep takes the rest of the backlog and stops at the
	// cutoff, which is strict: the signal detected exactly at the
	// cutoff survives.
	n, err = retainer.DeleteSignalsOlderThan(ctx, cutoff, 0)
	if err != nil {
		t.Fatalf("DeleteSignalsOlderThan(limit=0): %v", err)
	}
	if n != 2 {
		t.Errorf("DeleteSignalsOlderThan(limit=0) = %d, want the 2 remaining old rows", n)
	}
	remaining, err = s.Signals.List(ctx, ports.SignalFilter{ScopeID: scope})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if !sameIDs(signalIDs(remaining), []uuid.UUID{after.ID, atCutoff.ID}) {
		t.Errorf("after the unbounded sweep the store holds %v, want the rows at and after the cutoff",
			signalIDs(remaining))
	}

	// A sweep with nothing to do reports zero rather than failing.
	if n, err := retainer.DeleteSignalsOlderThan(ctx, cutoff, 100); err != nil {
		t.Errorf("DeleteSignalsOlderThan (nothing to delete): %v", err)
	} else if n != 0 {
		t.Errorf("DeleteSignalsOlderThan (nothing to delete) = %d, want 0", n)
	}

	// Evidence goes with the signal. The port has no evidence query, so
	// the observable form of the check is that re-saving the swept
	// signal does not resurrect the old evidence rows alongside the new.
	revived := old[0]
	revived.Evidence = []domain.Evidence{{Series: series, Time: base, Kind: "fresh", Score: 1}}
	if err := s.Signals.Save(ctx, revived); err != nil {
		t.Fatalf("Save (after sweep): %v", err)
	}
	got, err := s.Signals.Get(ctx, revived.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if len(got.Evidence) != 1 {
		t.Errorf("re-saved signal carries %d evidence rows, want 1: the swept signal's evidence was deleted with it",
			len(got.Evidence))
	}
}

// signalConcurrency pins that the signal repository is safe to use from
// several goroutines at once. Meaningful under -race.
func signalConcurrency(t *testing.T, b Backend) {
	s := open(t, b)
	ctx := context.Background()
	scope, series := uuid.New(), uuid.New()

	const writers, perWriter, readers = 4, 10, 4
	errCh := make(chan error, writers+readers)
	var wg sync.WaitGroup

	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				sig := signalFixture(scope, series, base.Add(time.Duration(w*perWriter+i)*time.Second), 0.5)
				if err := s.Signals.Save(ctx, sig); err != nil {
					errCh <- fmt.Errorf("writer %d: save: %w", w, err)
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
				if _, err := s.Signals.List(ctx, ports.SignalFilter{ScopeID: scope, Limit: 5}); err != nil {
					errCh <- fmt.Errorf("reader %d: list: %w", r, err)
					return
				}
			}
		}(r)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}

	n, err := s.Signals.Count(ctx, ports.SignalFilter{ScopeID: scope})
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if n != writers*perWriter {
		t.Errorf("Count after %d concurrent saves = %d", writers*perWriter, n)
	}
}

func requireSignalEqual(t *testing.T, want, got domain.Signal, precision time.Duration) {
	t.Helper()
	if got.ID != want.ID {
		t.Errorf("ID = %v, want %v", got.ID, want.ID)
	}
	if got.ScopeID != want.ScopeID {
		t.Errorf("ScopeID = %v, want %v", got.ScopeID, want.ScopeID)
	}
	if got.Series != want.Series {
		t.Errorf("Series = %v, want %v", got.Series, want.Series)
	}
	if got.Pattern != want.Pattern {
		t.Errorf("Pattern = %q, want %q", got.Pattern, want.Pattern)
	}
	requireTimeEqual(t, "DetectedAt", got.DetectedAt, want.DetectedAt, precision)
	requireTimeEqual(t, "Window.Start", got.Window.Start, want.Window.Start, precision)
	requireTimeEqual(t, "Window.End", got.Window.End, want.Window.End, precision)
	if got.Strength != want.Strength {
		t.Errorf("Strength = %v, want %v", got.Strength, want.Strength)
	}
	if got.Confidence != want.Confidence {
		t.Errorf("Confidence = %v, want %v", got.Confidence, want.Confidence)
	}
	if got.ConfidenceClass != want.ConfidenceClass {
		t.Errorf("ConfidenceClass = %q, want %q", got.ConfidenceClass, want.ConfidenceClass)
	}
	if len(got.Metrics) != len(want.Metrics) {
		t.Errorf("Metrics = %v, want %v", got.Metrics, want.Metrics)
	} else {
		for k, v := range want.Metrics {
			if got.Metrics[k] != v {
				t.Errorf("Metrics[%q] = %v, want %v", k, got.Metrics[k], v)
			}
		}
	}
	if len(got.Evidence) != len(want.Evidence) {
		t.Errorf("Evidence has %d rows, want %d", len(got.Evidence), len(want.Evidence))
	} else {
		for i := range want.Evidence {
			w, g := want.Evidence[i], got.Evidence[i]
			if g.Series != w.Series || g.Kind != w.Kind || g.Score != w.Score {
				t.Errorf("Evidence[%d] = %+v, want %+v", i, g, w)
			}
			requireTimeEqual(t, fmt.Sprintf("Evidence[%d].Time", i), g.Time, w.Time, precision)
			for k, v := range w.Metrics {
				if g.Metrics[k] != v {
					t.Errorf("Evidence[%d].Metrics[%q] = %v, want %v", i, k, g.Metrics[k], v)
				}
			}
		}
	}
	if got.Explanation.ComparablePeers != want.Explanation.ComparablePeers ||
		got.Explanation.BaselineWindowDays != want.Explanation.BaselineWindowDays ||
		got.Explanation.ThresholdUsed != want.Explanation.ThresholdUsed ||
		got.Explanation.DetectorVersion != want.Explanation.DetectorVersion {
		t.Errorf("Explanation = %+v, want %+v", got.Explanation, want.Explanation)
	}
	if len(got.Explanation.FeatureEvolution) != len(want.Explanation.FeatureEvolution) {
		t.Errorf("Explanation.FeatureEvolution has %d samples, want %d",
			len(got.Explanation.FeatureEvolution), len(want.Explanation.FeatureEvolution))
	} else {
		for i := range want.Explanation.FeatureEvolution {
			w, g := want.Explanation.FeatureEvolution[i], got.Explanation.FeatureEvolution[i]
			if g.Value != w.Value {
				t.Errorf("Explanation.FeatureEvolution[%d].Value = %v, want %v", i, g.Value, w.Value)
			}
			requireTimeEqual(t, fmt.Sprintf("Explanation.FeatureEvolution[%d].At", i), g.At, w.At, precision)
		}
	}
}

func requireTimeEqual(t *testing.T, field string, got, want time.Time, precision time.Duration) {
	t.Helper()
	if truncated := want.Truncate(precision); !got.Equal(truncated) {
		t.Errorf("%s = %s, want %s", field, got.Format(time.RFC3339Nano), truncated.Format(time.RFC3339Nano))
	}
	if got.Location() != time.UTC {
		t.Errorf("%s came back in zone %v, want UTC", field, got.Location())
	}
}
