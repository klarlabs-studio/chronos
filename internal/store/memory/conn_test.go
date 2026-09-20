package memory

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/felixgeelhaar/chronos"
	"github.com/felixgeelhaar/chronos/internal/domain"
	"github.com/felixgeelhaar/chronos/internal/ports"
	"github.com/google/uuid"
)

func mkState(scope, entity uuid.UUID, ts time.Time) chronos.EntityState {
	return chronos.EntityState{
		ID: uuid.New(), EntityID: entity, ScopeID: scope, Timestamp: ts,
		Features: []float64{1, 2, 3, 4},
	}
}

func TestEntityStateRepository_IngestAndBatchSave(t *testing.T) {
	c := New()
	ctx := context.Background()
	scope := uuid.New()
	a := uuid.New()
	b := uuid.New()
	now := time.Now()

	if err := c.EntityStates.Ingest(ctx, "test", mkState(scope, a, now)); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if err := c.EntityStates.Save(ctx, "test", []chronos.EntityState{mkState(scope, b, now.Add(-time.Hour))}); err != nil {
		t.Fatalf("Save: %v", err)
	}

	got, err := c.EntityStates.ListByScope(ctx, scope)
	if err != nil {
		t.Fatalf("ListByScope: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("ListByScope length = %d, want 2", len(got))
	}
	if !got[0].Timestamp.After(got[1].Timestamp) {
		t.Errorf("ListByScope must be most-recent-first")
	}

	count, _ := c.EntityStates.Count(ctx, "test")
	if count != 2 {
		t.Errorf("Count = %d, want 2", count)
	}
}

func TestEntityStateRepository_RejectsInvalid(t *testing.T) {
	c := New()
	err := c.EntityStates.Ingest(context.Background(), "ad", chronos.EntityState{Timestamp: time.Now(), Features: []float64{1}})
	if !errors.Is(err, chronos.ErrMissingEntityID) {
		t.Fatalf("Ingest invalid = %v, want ErrMissingEntityID", err)
	}
}

func mkSignal(scope, series uuid.UUID, pattern domain.PatternType, conf float64, t time.Time) domain.Signal {
	return domain.Signal{
		ID:         uuid.New(),
		ScopeID:    scope,
		Series:     series,
		Pattern:    pattern,
		DetectedAt: t,
		Window:     domain.TimeWindow{Start: t.Add(-time.Hour), End: t},
		Strength:   conf,
		Confidence: conf,
	}
}

func TestSignalRepository_FiltersAndOrdering(t *testing.T) {
	c := New()
	ctx := context.Background()
	scope := uuid.New()
	otherScope := uuid.New()
	now := time.Now()

	s1 := mkSignal(scope, uuid.New(), domain.PatternTypeRecurrence, 0.9, now)
	s2 := mkSignal(scope, uuid.New(), domain.PatternTypeRecurrence, 0.7, now.Add(-time.Hour))
	s3 := mkSignal(scope, uuid.New(), domain.PatternTypeTrend, 0.6, now.Add(-2*time.Hour))
	s4 := mkSignal(otherScope, uuid.New(), domain.PatternTypeRecurrence, 0.95, now)

	for _, s := range []domain.Signal{s1, s2, s3, s4} {
		if err := c.Signals.Save(ctx, s); err != nil {
			t.Fatalf("Save: %v", err)
		}
	}

	// Filter by scope
	got, err := c.Signals.List(ctx, ports.SignalFilter{ScopeID: scope})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("scope filter returned %d, want 3", len(got))
	}
	// detected-at desc
	if !got[0].DetectedAt.After(got[1].DetectedAt) {
		t.Errorf("ordering wrong")
	}

	// Filter by pattern
	pat := domain.PatternTypeRecurrence
	got, _ = c.Signals.List(ctx, ports.SignalFilter{ScopeID: scope, Pattern: &pat})
	if len(got) != 2 {
		t.Errorf("pattern filter returned %d, want 2", len(got))
	}

	// Filter by min confidence
	conf := 0.8
	got, _ = c.Signals.List(ctx, ports.SignalFilter{ScopeID: scope, MinConfidence: &conf})
	if len(got) != 1 {
		t.Errorf("min confidence filter returned %d, want 1", len(got))
	}

	// Limit
	got, _ = c.Signals.List(ctx, ports.SignalFilter{ScopeID: scope, Limit: 1})
	if len(got) != 1 {
		t.Errorf("limit returned %d, want 1", len(got))
	}

	// Count
	n, _ := c.Signals.Count(ctx, ports.SignalFilter{ScopeID: scope})
	if n != 3 {
		t.Errorf("Count = %d, want 3", n)
	}
}

func TestSignalRepository_CohortLevelOutlierClusterPersists(t *testing.T) {
	c := New()
	ctx := context.Background()
	now := time.Now()
	sig := domain.Signal{
		ID:         uuid.New(),
		ScopeID:    uuid.New(),
		Series:     uuid.Nil,
		Pattern:    domain.PatternTypeOutlierCluster,
		DetectedAt: now,
		Window:     domain.TimeWindow{Start: now.Add(-time.Minute), End: now},
		Strength:   0.4,
		Confidence: 0.7,
		Metrics:    map[string]float64{"member_count": 3},
	}
	if err := c.Signals.Save(ctx, sig); err != nil {
		t.Fatalf("Save cohort-level signal: %v", err)
	}
	got, err := c.Signals.Get(ctx, sig.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Series != uuid.Nil {
		t.Errorf("Series = %v, want nil", got.Series)
	}
	if got.Pattern != domain.PatternTypeOutlierCluster {
		t.Errorf("Pattern = %q", got.Pattern)
	}
}

func TestSignalRepository_GetMissing(t *testing.T) {
	c := New()
	if _, err := c.Signals.Get(context.Background(), uuid.New()); !errors.Is(err, domain.ErrSignalNotFound) {
		t.Errorf("Get missing = %v, want ErrSignalNotFound", err)
	}
}

// A bounded retention delete removes the OLDEST rows first.
//
// Order matters, not just count. A bounded sweep that deleted an
// arbitrary subset would leave the oldest rows behind on every pass and
// never drain them, so retention would appear to run forever while the
// rows it exists to remove stayed put.
func TestMemory_DeleteSignalsOlderThanHonoursLimitOldestFirst(t *testing.T) {
	c := New()
	ctx := context.Background()
	scope := uuid.New()
	base := time.Now().UTC().Add(-72 * time.Hour)

	// Saved newest-first so a correct implementation cannot pass by
	// accident of insertion order.
	var want []time.Time
	for i := 4; i >= 0; i-- {
		at := base.Add(time.Duration(i) * time.Minute)
		if i >= 2 {
			want = append(want, at)
		}
		if err := c.Signals.Save(ctx, mkSignal(scope, uuid.New(), domain.PatternTypeRecurrence, 0.9, at)); err != nil {
			t.Fatalf("seed %d: %v", i, err)
		}
	}

	n, err := c.Signals.DeleteSignalsOlderThan(ctx, time.Now().UTC(), 2)
	if err != nil {
		t.Fatalf("bounded delete: %v", err)
	}
	if n != 2 {
		t.Fatalf("deleted %d, want 2 — limit not honoured", n)
	}

	left, err := c.Signals.List(ctx, ports.SignalFilter{ScopeID: scope})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(left) != 3 {
		t.Fatalf("%d remain, want 3", len(left))
	}
	for _, sig := range left {
		if sig.DetectedAt.Equal(base) || sig.DetectedAt.Equal(base.Add(time.Minute)) {
			t.Errorf("oldest signal at %v survived; the sweep deleted a newer subset and will never drain the backlog", sig.DetectedAt)
		}
	}
}
