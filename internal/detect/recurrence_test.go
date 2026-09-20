package detect

import (
	"context"
	"testing"
	"time"

	"github.com/felixgeelhaar/chronos"
	"github.com/felixgeelhaar/chronos/internal/config"
	"github.com/felixgeelhaar/chronos/internal/domain"
	"github.com/google/uuid"
)

func defaultCfg() *config.Config {
	return &config.Config{
		SimilarityThreshold: 0.8,
		MinSampleSize:       2,
		MaxSignalsPerRun:    10,
	}
}

func TestRecurrence_EmitsSignalForSimilarPeers(t *testing.T) {
	d := NewRecurrence(defaultCfg())

	scope := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	a := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	b := uuid.MustParse("33333333-3333-3333-3333-333333333333")
	c := uuid.MustParse("44444444-4444-4444-4444-444444444444")
	now := time.Now()

	states := []chronos.EntityState{
		{ID: uuid.New(), EntityID: a, ScopeID: scope, Timestamp: now, Features: []float64{1.0, 2.0, 3.0, 5.0}},
		{ID: uuid.New(), EntityID: b, ScopeID: scope, Timestamp: now.Add(-24 * time.Hour), Features: []float64{1.1, 2.1, 3.1, 7.0}},
		{ID: uuid.New(), EntityID: c, ScopeID: scope, Timestamp: now.Add(-48 * time.Hour), Features: []float64{1.05, 2.05, 3.05, 6.5}},
	}

	got := d.Detect(context.Background(), scope, states)
	if len(got) == 0 {
		t.Fatal("expected at least one signal")
	}
	sig := got[0]
	if sig.Pattern != domain.PatternTypeRecurrence {
		t.Errorf("Pattern = %s", sig.Pattern)
	}
	if sig.Series != a {
		t.Errorf("Series = %v, want %v", sig.Series, a)
	}
	if sig.Confidence <= 0 || sig.Confidence > 1 {
		t.Errorf("Confidence = %f, out of range", sig.Confidence)
	}
	if sig.Strength <= 0 || sig.Strength > 1 {
		t.Errorf("Strength = %f, out of range", sig.Strength)
	}
	if len(sig.Evidence) < defaultCfg().MinSampleSize {
		t.Errorf("evidence = %d, want >= %d", len(sig.Evidence), defaultCfg().MinSampleSize)
	}
	for _, e := range sig.Evidence {
		if e.Kind != "similar_state" {
			t.Errorf("evidence kind = %q", e.Kind)
		}
	}
	if err := sig.Validate(); err != nil {
		t.Errorf("emitted signal invalid: %v", err)
	}
}

func TestRecurrence_EmptyAndSingleEntity(t *testing.T) {
	d := NewRecurrence(defaultCfg())
	scope := uuid.New()
	a := uuid.New()

	cases := [][]chronos.EntityState{
		nil,
		{},
		{{ID: uuid.New(), EntityID: a, ScopeID: scope, Timestamp: time.Now(), Features: []float64{1, 2, 3, 5}}},
	}
	for i, states := range cases {
		got := d.Detect(context.Background(), scope, states)
		if len(got) != 0 {
			t.Errorf("case %d: got %d signals, want 0", i, len(got))
		}
	}
}

func TestRecurrence_NoCrossEntityComparison(t *testing.T) {
	// A single entity with multiple historical states must not produce
	// recurrence signals — recurrence requires *other* entities' history.
	d := NewRecurrence(defaultCfg())
	scope := uuid.New()
	a := uuid.New()
	now := time.Now()
	states := []chronos.EntityState{
		{ID: uuid.New(), EntityID: a, ScopeID: scope, Timestamp: now, Features: []float64{1, 2, 3, 5}},
		{ID: uuid.New(), EntityID: a, ScopeID: scope, Timestamp: now.Add(-time.Hour), Features: []float64{1.1, 2.1, 3.1, 5.1}},
		{ID: uuid.New(), EntityID: a, ScopeID: scope, Timestamp: now.Add(-2 * time.Hour), Features: []float64{1.05, 2.05, 3.05, 5.05}},
	}
	got := d.Detect(context.Background(), scope, states)
	if len(got) != 0 {
		t.Errorf("got %d signals, want 0", len(got))
	}
}

func TestEngine_GroupsByScopeAndCaps(t *testing.T) {
	cfg := &config.Config{SimilarityThreshold: 0.8, MinSampleSize: 2, MaxSignalsPerRun: 1}
	e := NewEngine(cfg, NewRecurrence(cfg))

	mk := func(scope, ent uuid.UUID, ts time.Time, outcome float64) chronos.EntityState {
		return chronos.EntityState{ID: uuid.New(), EntityID: ent, ScopeID: scope, Timestamp: ts, Features: []float64{1, 2, 3, outcome}}
	}
	s1, s2 := uuid.New(), uuid.New()
	now := time.Now()
	a, b, c := uuid.New(), uuid.New(), uuid.New()
	states := []chronos.EntityState{
		mk(s1, a, now, 5),
		mk(s1, b, now.Add(-time.Hour), 7),
		mk(s1, c, now.Add(-2*time.Hour), 6.5),
		mk(s2, a, now, 5),
		mk(s2, b, now.Add(-time.Hour), 7),
		mk(s2, c, now.Add(-2*time.Hour), 6.5),
	}
	got := e.Detect(context.Background(), states)
	if len(got) != 1 {
		t.Errorf("MaxSignalsPerRun=1 returned %d signals", len(got))
	}
}

func TestRecurrence_SkipsRaggedFeatureVectors(t *testing.T) {
	d := NewRecurrence(defaultCfg())
	scope := uuid.New()
	a, b, c := uuid.New(), uuid.New(), uuid.New()
	now := time.Now()
	// Peers b and c are cosine-similar to a but have a different feature
	// arity — Cosine would return 0; fail-closed means skip, not emit.
	states := []chronos.EntityState{
		{ID: uuid.New(), EntityID: a, ScopeID: scope, Timestamp: now, Features: []float64{1, 2, 3, 5}},
		{ID: uuid.New(), EntityID: b, ScopeID: scope, Timestamp: now.Add(-time.Hour), Features: []float64{1.1, 2.1}},
		{ID: uuid.New(), EntityID: c, ScopeID: scope, Timestamp: now.Add(-2 * time.Hour), Features: []float64{1.05, 2.05}},
	}
	got := d.Detect(context.Background(), scope, states)
	if len(got) != 0 {
		t.Fatalf("got %d signals from ragged peers, want 0", len(got))
	}
}

func TestRecurrence_SkipsZeroNormVectors(t *testing.T) {
	d := NewRecurrence(defaultCfg())
	scope := uuid.New()
	a, b, c := uuid.New(), uuid.New(), uuid.New()
	now := time.Now()
	states := []chronos.EntityState{
		{ID: uuid.New(), EntityID: a, ScopeID: scope, Timestamp: now, Features: []float64{0, 0, 0, 0}},
		{ID: uuid.New(), EntityID: b, ScopeID: scope, Timestamp: now.Add(-time.Hour), Features: []float64{1, 2, 3, 5}},
		{ID: uuid.New(), EntityID: c, ScopeID: scope, Timestamp: now.Add(-2 * time.Hour), Features: []float64{1.1, 2.1, 3.1, 5.1}},
	}
	got := d.Detect(context.Background(), scope, states)
	if len(got) != 0 {
		t.Fatalf("got %d signals for zero-norm subject, want 0", len(got))
	}
}

func TestMostRecentByEntity_EqualTimestampPrefersLargerID(t *testing.T) {
	entity := uuid.New()
	scope := uuid.New()
	ts := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	lo := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	hi := uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff")
	states := []chronos.EntityState{
		{ID: lo, EntityID: entity, ScopeID: scope, Timestamp: ts, Features: []float64{1}},
		{ID: hi, EntityID: entity, ScopeID: scope, Timestamp: ts, Features: []float64{2}},
	}
	got := mostRecentByEntity(states)
	if got[entity].ID != hi {
		t.Fatalf("most recent ID = %s, want %s (larger ID wins equal-timestamp ties)", got[entity].ID, hi)
	}
	// Reverse input order — result must be stable.
	states[0], states[1] = states[1], states[0]
	got = mostRecentByEntity(states)
	if got[entity].ID != hi {
		t.Fatalf("after reverse, most recent ID = %s, want %s", got[entity].ID, hi)
	}
}

func TestSortByTimestampAsc_equalTimestampBreaksByID(t *testing.T) {
	entity := uuid.New()
	scope := uuid.New()
	ts := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	lo := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	hi := uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff")
	states := []chronos.EntityState{
		{ID: hi, EntityID: entity, ScopeID: scope, Timestamp: ts, Features: []float64{2}},
		{ID: lo, EntityID: entity, ScopeID: scope, Timestamp: ts, Features: []float64{1}},
	}
	sortByTimestampAsc(states)
	if states[0].ID != lo || states[1].ID != hi {
		t.Fatalf("order = [%s, %s], want [%s, %s]", states[0].ID, states[1].ID, lo, hi)
	}
}
