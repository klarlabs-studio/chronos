package embed_test

import (
	"context"
	"testing"
	"time"

	"github.com/felixgeelhaar/chronos"
	"github.com/felixgeelhaar/chronos/embed"
	"github.com/google/uuid"
)

// TestTemporalContract_OOOIngestDetectDeterminism exercises the
// temporal contract end-to-end on the embed path: out-of-order ingest,
// duplicate observation-ID upsert, duplicate timestamps, then Detect.
// Two independent engines receiving the same observations (in different
// arrival orders) must emit the same patterns with the same windows.
func TestTemporalContract_OOOIngestDetectDeterminism(t *testing.T) {
	t.Parallel()
	scope := uuid.New()
	entity := uuid.New()
	base := time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC)

	// A clear stall: flat outcome over enough points.
	mk := func(i int) chronos.EntityState {
		return chronos.EntityState{
			ID:        uuid.New(),
			EntityID:  entity,
			ScopeID:   scope,
			Timestamp: base.Add(time.Duration(i) * time.Minute),
			Features:  []float64{float64(i), 11.0},
		}
	}
	chrono := make([]chronos.EntityState, 7)
	for i := range chrono {
		chrono[i] = mk(i)
	}

	// Duplicate-ID correction: re-ingest observation 3 with a corrected
	// feature payload; timestamp must not move.
	corrected := chrono[3]
	corrected.Features = []float64{3, 11.0}

	// Duplicate timestamp with a distinct ID (allowed; order among ties
	// is undefined, but Detect must still be deterministic after Engine sort).
	dupTS := chronos.EntityState{
		ID:        uuid.New(),
		EntityID:  entity,
		ScopeID:   scope,
		Timestamp: chrono[5].Timestamp,
		Features:  []float64{5.5, 11.0},
	}

	run := func(t *testing.T, order []chronos.EntityState) []chronos.Signal {
		t.Helper()
		eng, err := embed.New(embed.WithStorage("memory://?namespace=temporal-" + uuid.New().String()))
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		defer func() { _ = eng.Close() }()
		ctx := context.Background()
		for _, s := range order {
			if err := eng.Process(ctx, s); err != nil {
				t.Fatalf("Process: %v", err)
			}
		}
		if err := eng.Process(ctx, corrected); err != nil {
			t.Fatalf("Process correction: %v", err)
		}
		if err := eng.Process(ctx, dupTS); err != nil {
			t.Fatalf("Process dup timestamp: %v", err)
		}
		sigs, err := eng.Detect(ctx, []uuid.UUID{scope})
		if err != nil {
			t.Fatalf("Detect: %v", err)
		}
		return sigs
	}

	forward := append([]chronos.EntityState(nil), chrono...)
	reverse := make([]chronos.EntityState, len(chrono))
	for i := range chrono {
		reverse[i] = chrono[len(chrono)-1-i]
	}

	a := run(t, forward)
	b := run(t, reverse)
	if len(a) == 0 {
		t.Fatal("expected at least one signal (stall) from a flat series")
	}
	if len(a) != len(b) {
		t.Fatalf("signal count differs by ingest order: forward=%d reverse=%d", len(a), len(b))
	}
	for i := range a {
		if a[i].Pattern != b[i].Pattern {
			t.Errorf("signal[%d] pattern: forward %s, reverse %s", i, a[i].Pattern, b[i].Pattern)
		}
		if a[i].Window != b[i].Window {
			t.Errorf("signal[%d] window: forward %+v, reverse %+v", i, a[i].Window, b[i].Window)
		}
		if a[i].Strength != b[i].Strength || a[i].Confidence != b[i].Confidence {
			t.Errorf("signal[%d] strength/confidence differ by ingest order", i)
		}
	}
}
