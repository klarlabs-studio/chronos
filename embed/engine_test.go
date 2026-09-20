package embed_test

import (
	"context"
	"testing"
	"time"

	"github.com/felixgeelhaar/chronos"
	"github.com/felixgeelhaar/chronos/embed"
	"github.com/felixgeelhaar/chronos/internal/store"
	"github.com/google/uuid"
)

// TestNew_ZeroConfig_BootsMemory verifies the default constructor
// returns a working engine without any options.
func TestNew_ZeroConfig_BootsMemory(t *testing.T) {
	t.Parallel()
	eng, err := embed.New()
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = eng.Close() }()
}

// TestProcess_Query_Roundtrip writes states + detects + queries them
// back. Uses the spike detector via the default detector set.
func TestProcess_Query_Roundtrip(t *testing.T) {
	t.Parallel()
	eng, err := embed.New(embed.WithStorage("memory://?namespace=chronos_embed_test"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = eng.Close() }()

	scopeID := uuid.New()
	entityID := uuid.New()
	base := time.Date(2026, 5, 31, 10, 0, 0, 0, time.UTC)
	ctx := context.Background()

	// Baseline observations + one obvious spike at the end.
	values := []float64{1.0, 1.1, 0.9, 1.0, 1.2, 1.1, 50.0}
	states := make([]chronos.EntityState, len(values))
	for i, v := range values {
		states[i] = chronos.EntityState{
			ID:        uuid.New(),
			EntityID:  entityID,
			ScopeID:   scopeID,
			Timestamp: base.Add(time.Duration(i) * time.Minute),
			Features:  []float64{v},
			Labels:    []string{"value"},
		}
	}
	if err := eng.ProcessBatch(ctx, states); err != nil {
		t.Fatalf("ProcessBatch: %v", err)
	}

	signals, err := eng.Detect(ctx, []uuid.UUID{scopeID})
	if err != nil {
		t.Fatalf("Detect: %v", err)
	}

	queried, err := eng.Query(ctx, embed.QueryOpts{ScopeID: scopeID})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(queried) != len(signals) {
		t.Errorf("Query returned %d signals, Detect returned %d; mismatch", len(queried), len(signals))
	}
}

// TestProcess_RejectsInvalidState verifies the validation path. A state
// missing EntityID must be rejected without touching the store.
func TestProcess_RejectsInvalidState(t *testing.T) {
	t.Parallel()
	eng, err := embed.New(embed.WithStorage("memory://?namespace=chronos_embed_validate_test"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = eng.Close() }()

	bad := chronos.EntityState{
		ID: uuid.New(),
		// EntityID intentionally zero
		ScopeID:   uuid.New(),
		Timestamp: time.Now(),
		Features:  []float64{1.0},
	}
	if err := eng.Process(context.Background(), bad); err == nil {
		t.Fatal("Process accepted invalid state; expected validation error")
	}
}

// TestClose_Idempotent verifies repeated Close calls don't panic and
// return nil after the first invocation.
func TestClose_Idempotent(t *testing.T) {
	t.Parallel()
	eng, err := embed.New()
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := eng.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := eng.Close(); err != nil {
		t.Errorf("second Close: %v (expected nil)", err)
	}
}

// TestWithStoreRegistry_UsesIsolatedProviders verifies an Engine can
// open storage against a cloned registry without touching the default.
func TestWithStoreRegistry_UsesIsolatedProviders(t *testing.T) {
	t.Parallel()
	reg := store.DefaultRegistry().Clone()
	eng, err := embed.New(
		embed.WithStorage("memory://?namespace=chronos_embed_registry_test"),
		embed.WithStoreRegistry(reg),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = eng.Close() }()

	scope := uuid.New()
	state := chronos.EntityState{
		ID:        uuid.New(),
		EntityID:  uuid.New(),
		ScopeID:   scope,
		Timestamp: time.Now().UTC(),
		Features:  []float64{1.0},
	}
	if err := eng.Process(context.Background(), state); err != nil {
		t.Fatalf("Process: %v", err)
	}
}
