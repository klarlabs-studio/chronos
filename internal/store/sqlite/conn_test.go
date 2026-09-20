package sqlite

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

func openTestConn(t *testing.T) *Conn {
	t.Helper()
	c, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestSQLite_EntityStateRoundTrip(t *testing.T) {
	c := openTestConn(t)
	ctx := context.Background()
	scope := uuid.New()
	a := uuid.New()
	b := uuid.New()
	now := time.Now()

	if err := c.EntityStates.Ingest(ctx, "test", chronos.EntityState{ID: uuid.New(), EntityID: a, ScopeID: scope, Timestamp: now, Features: []float64{1, 2, 3, 5}}); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if err := c.EntityStates.Save(ctx, "test", []chronos.EntityState{
		{ID: uuid.New(), EntityID: b, ScopeID: scope, Timestamp: now.Add(-time.Hour), Features: []float64{1.1, 2.1, 3.1, 7}},
	}); err != nil {
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
		Metrics:    map[string]float64{"avg_similarity": conf},
	}
}

func TestSQLite_SignalRoundTripWithEvidence(t *testing.T) {
	c := openTestConn(t)
	ctx := context.Background()
	scope := uuid.New()
	series := uuid.New()
	now := time.Now()

	sig := mkSignal(scope, series, domain.PatternTypeRecurrence, 0.85, now)
	sig.Evidence = []domain.Evidence{
		{Series: uuid.New(), Time: now.Add(-time.Hour), Kind: "similar_state", Score: 0.92, Metrics: map[string]float64{"outcome_diff": 1.5}},
	}
	if err := c.Signals.Save(ctx, sig); err != nil {
		t.Fatalf("Save: %v", err)
	}

	got, err := c.Signals.Get(ctx, sig.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Pattern != domain.PatternTypeRecurrence {
		t.Errorf("Pattern lost: %s", got.Pattern)
	}
	if len(got.Evidence) != 1 {
		t.Fatalf("evidence count = %d", len(got.Evidence))
	}
	if got.Evidence[0].Kind != "similar_state" {
		t.Errorf("evidence kind = %q", got.Evidence[0].Kind)
	}
	if got.Evidence[0].Metrics["outcome_diff"] != 1.5 {
		t.Errorf("evidence metric lost: %v", got.Evidence[0].Metrics)
	}

	sig.Evidence = []domain.Evidence{
		{Series: uuid.New(), Time: now, Kind: "similar_state", Score: 0.5, Metrics: map[string]float64{"outcome_diff": 0.1}},
	}
	if err := c.Signals.Save(ctx, sig); err != nil {
		t.Fatalf("Save upsert: %v", err)
	}
	got, err = c.Signals.Get(ctx, sig.ID)
	if err != nil {
		t.Fatalf("Get after upsert: %v", err)
	}
	if len(got.Evidence) != 1 {
		t.Fatalf("upsert duplicated evidence: %d rows", len(got.Evidence))
	}
	if got.Evidence[0].Score != 0.5 {
		t.Errorf("upsert evidence score = %v, want 0.5", got.Evidence[0].Score)
	}
}

// TestSQLite_SignalExplanationRoundTrip pins the persistence contract
// for the detector explainability payload: a Signal saved with an
// Explanation is loaded back with the same FeatureEvolution,
// ComparablePeers, BaselineWindowDays, ThresholdUsed and
// DetectorVersion. A signal saved WITHOUT an explanation loads back
// with the zero value (no panic, no spurious data).
func TestSQLite_SignalExplanationRoundTrip(t *testing.T) {
	c := openTestConn(t)
	ctx := context.Background()
	scope := uuid.New()
	series := uuid.New()
	now := time.Now().UTC().Truncate(time.Microsecond)

	sig := mkSignal(scope, series, domain.PatternTypeTrend, 0.8, now)
	sig.Explanation = domain.Explanation{
		FeatureEvolution: []domain.FeatureSample{
			{At: now.Add(-2 * time.Hour), Value: 18.0},
			{At: now.Add(-time.Hour), Value: 22.0},
			{At: now, Value: 26.0},
		},
		ComparablePeers:    12,
		BaselineWindowDays: 90,
		ThresholdUsed:      2.5,
		DetectorVersion:    "trend-v2",
	}
	if err := c.Signals.Save(ctx, sig); err != nil {
		t.Fatalf("Save: %v", err)
	}

	got, err := c.Signals.Get(ctx, sig.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Explanation.DetectorVersion != "trend-v2" {
		t.Errorf("detector_version = %q, want trend-v2", got.Explanation.DetectorVersion)
	}
	if got.Explanation.ComparablePeers != 12 {
		t.Errorf("comparable_peers = %d, want 12", got.Explanation.ComparablePeers)
	}
	if got.Explanation.BaselineWindowDays != 90 {
		t.Errorf("baseline_window_days = %d, want 90", got.Explanation.BaselineWindowDays)
	}
	if got.Explanation.ThresholdUsed != 2.5 {
		t.Errorf("threshold_used = %v, want 2.5", got.Explanation.ThresholdUsed)
	}
	if len(got.Explanation.FeatureEvolution) != 3 {
		t.Fatalf("feature_evolution len = %d, want 3", len(got.Explanation.FeatureEvolution))
	}
	if got.Explanation.FeatureEvolution[2].Value != 26.0 {
		t.Errorf("feature_evolution[2].value = %v, want 26.0", got.Explanation.FeatureEvolution[2].Value)
	}
}

// TestSQLite_SignalConfidenceClassRoundTrip pins the persistence
// contract for the qualitative grade: a Signal saved with
// ConfidenceClass loads back with the same value, and a signal saved
// without one loads back with the empty string (the explicit "no
// claim about strength" state).
func TestSQLite_SignalConfidenceClassRoundTrip(t *testing.T) {
	c := openTestConn(t)
	ctx := context.Background()
	scope := uuid.New()
	series := uuid.New()
	now := time.Now().UTC()

	sig := mkSignal(scope, series, domain.PatternTypeTrend, 0.8, now)
	sig.ConfidenceClass = domain.ConfidenceClassEstablished
	if err := c.Signals.Save(ctx, sig); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, err := c.Signals.Get(ctx, sig.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.ConfidenceClass != domain.ConfidenceClassEstablished {
		t.Errorf("ConfidenceClass = %q, want established", got.ConfidenceClass)
	}

	// Unclassified signal stays unclassified.
	plain := mkSignal(scope, uuid.New(), domain.PatternTypeRecurrence, 0.7, now)
	if err := c.Signals.Save(ctx, plain); err != nil {
		t.Fatalf("Save plain: %v", err)
	}
	gotPlain, _ := c.Signals.Get(ctx, plain.ID)
	if gotPlain.ConfidenceClass != "" {
		t.Errorf("unclassified signal loaded with class %q", gotPlain.ConfidenceClass)
	}
}

// TestSQLite_SignalNoExplanationLoadsZero confirms a signal saved
// before the explanation column existed (or saved with the zero value)
// loads back with an empty Explanation. Avoids regressions on the
// optional-by-design contract.
func TestSQLite_SignalNoExplanationLoadsZero(t *testing.T) {
	c := openTestConn(t)
	ctx := context.Background()
	scope := uuid.New()
	series := uuid.New()
	now := time.Now().UTC()

	sig := mkSignal(scope, series, domain.PatternTypeRecurrence, 0.7, now)
	if err := c.Signals.Save(ctx, sig); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, err := c.Signals.Get(ctx, sig.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !got.Explanation.IsZero() {
		t.Errorf("expected zero Explanation, got %+v", got.Explanation)
	}
}

func TestSQLite_SignalListFilters(t *testing.T) {
	c := openTestConn(t)
	ctx := context.Background()
	scope := uuid.New()
	other := uuid.New()
	now := time.Now()

	in := []domain.Signal{
		mkSignal(scope, uuid.New(), domain.PatternTypeRecurrence, 0.9, now),
		mkSignal(scope, uuid.New(), domain.PatternTypeRecurrence, 0.7, now.Add(-time.Hour)),
		mkSignal(scope, uuid.New(), domain.PatternTypeTrend, 0.6, now.Add(-2*time.Hour)),
		mkSignal(other, uuid.New(), domain.PatternTypeRecurrence, 0.95, now),
	}
	for _, s := range in {
		if err := c.Signals.Save(ctx, s); err != nil {
			t.Fatalf("Save: %v", err)
		}
	}

	got, _ := c.Signals.List(ctx, ports.SignalFilter{ScopeID: scope})
	if len(got) != 3 {
		t.Errorf("scope filter returned %d, want 3", len(got))
	}
	if !got[0].DetectedAt.After(got[1].DetectedAt) {
		t.Errorf("ordering must be detected-at desc")
	}

	pat := domain.PatternTypeRecurrence
	got, _ = c.Signals.List(ctx, ports.SignalFilter{ScopeID: scope, Pattern: &pat})
	if len(got) != 2 {
		t.Errorf("pattern filter returned %d, want 2", len(got))
	}

	conf := 0.8
	got, _ = c.Signals.List(ctx, ports.SignalFilter{ScopeID: scope, MinConfidence: &conf})
	if len(got) != 1 {
		t.Errorf("min confidence filter returned %d, want 1", len(got))
	}

	got, _ = c.Signals.List(ctx, ports.SignalFilter{ScopeID: scope, Limit: 1})
	if len(got) != 1 {
		t.Errorf("limit returned %d, want 1", len(got))
	}

	n, _ := c.Signals.Count(ctx, ports.SignalFilter{ScopeID: scope})
	if n != 3 {
		t.Errorf("Count = %d, want 3", n)
	}
}

func TestSQLite_SignalCohortLevelOutlierClusterPersists(t *testing.T) {
	c := openTestConn(t)
	ctx := context.Background()
	now := time.Now().UTC()
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
}

func TestSQLite_SignalGetMissing(t *testing.T) {
	c := openTestConn(t)
	if _, err := c.Signals.Get(context.Background(), uuid.New()); !errors.Is(err, domain.ErrSignalNotFound) {
		t.Errorf("Get missing = %v, want ErrSignalNotFound", err)
	}
}

// A bounded retention delete must remove only `limit` rows, oldest
// first, and leave the rest for the next batch.
//
// sqlite needs a subquery for this: stock builds are not compiled with
// SQLITE_ENABLE_UPDATE_DELETE_LIMIT, so `DELETE ... LIMIT` does not
// parse. A backend that silently ignored the limit would delete the
// whole backlog in one transaction, which is the failure this bound
// exists to prevent.
func TestSQLite_DeleteSignalsOlderThanHonoursLimit(t *testing.T) {
	c := openTestConn(t)
	ctx := context.Background()
	scope := uuid.New()
	base := time.Now().UTC().Add(-72 * time.Hour)

	for i := 0; i < 5; i++ {
		sig := mkSignal(scope, uuid.New(), domain.PatternTypeRecurrence, 0.9,
			base.Add(time.Duration(i)*time.Minute))
		if err := c.Signals.Save(ctx, sig); err != nil {
			t.Fatalf("seed %d: %v", i, err)
		}
	}

	cutoff := time.Now().UTC()
	n, err := c.Signals.DeleteSignalsOlderThan(ctx, cutoff, 2)
	if err != nil {
		t.Fatalf("bounded delete: %v", err)
	}
	if n != 2 {
		t.Fatalf("deleted %d rows, want 2 — the limit was not honoured", n)
	}

	left, err := c.Signals.List(ctx, ports.SignalFilter{ScopeID: scope})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(left) != 3 {
		t.Fatalf("%d signals remain, want 3", len(left))
	}

	// A limit of zero means unbounded, which must still work.
	n, err = c.Signals.DeleteSignalsOlderThan(ctx, cutoff, 0)
	if err != nil {
		t.Fatalf("unbounded delete: %v", err)
	}
	if n != 3 {
		t.Fatalf("unbounded delete removed %d, want 3", n)
	}
}
