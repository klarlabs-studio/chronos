package chronos_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/felixgeelhaar/chronos"
	"github.com/google/uuid"
)

func TestEntityState_Validate(t *testing.T) {
	entityID := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	scopeID := uuid.MustParse("11111111-1111-1111-1111-111111111111")

	obsID := uuid.MustParse("33333333-3333-3333-3333-333333333333")
	ts := time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name    string
		state   chronos.EntityState
		wantErr error
	}{
		{
			name: "valid",
			state: chronos.EntityState{
				ID:       obsID,
				EntityID: entityID,
				ScopeID:  scopeID,
				Features: []float64{1, 2, 3},
				// Timestamp became an invariant rather than an optional
				// field. The wire still defaults an omitted timestamp to
				// now (internal/api/dto.go), so the tightening applies at
				// the EntityState boundary only.
				Timestamp: ts,
			},
		},
		{
			name: "missing observation ID",
			state: chronos.EntityState{
				EntityID:  entityID,
				ScopeID:   scopeID,
				Features:  []float64{1, 2, 3},
				Timestamp: ts,
			},
			wantErr: chronos.ErrMissingObservationID,
		},
		{
			name: "missing entity ID",
			state: chronos.EntityState{
				ID:       obsID,
				ScopeID:  scopeID,
				Features: []float64{1, 2, 3},
			},
			wantErr: chronos.ErrMissingEntityID,
		},
		{
			name: "missing scope ID",
			state: chronos.EntityState{
				ID:       obsID,
				EntityID: entityID,
				Features: []float64{1, 2, 3},
			},
			wantErr: chronos.ErrMissingScopeID,
		},
		{
			name: "no features",
			state: chronos.EntityState{
				ID:       obsID,
				EntityID: entityID,
				ScopeID:  scopeID,
			},
			wantErr: chronos.ErrMissingFeatures,
		},
		{
			name: "labels length mismatch",
			state: chronos.EntityState{
				ID:       obsID,
				EntityID: entityID,
				ScopeID:  scopeID,
				Features: []float64{1, 2, 3},
				Labels:   []string{"a", "b"},
			},
			wantErr: chronos.ErrLabelsMismatch,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.state.Validate()
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Validate() = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestEntityState_Outcome(t *testing.T) {
	s := chronos.EntityState{Features: []float64{1, 2, 3, 9}}
	if got := s.Outcome(); got != 9 {
		t.Fatalf("Outcome() = %v, want 9", got)
	}

	empty := chronos.EntityState{}
	if got := empty.Outcome(); got != 0 {
		t.Fatalf("Outcome() on empty features = %v, want 0", got)
	}
}

type stubSource struct{ name string }

func (s *stubSource) Name() string { return s.name }
func (s *stubSource) Fetch(context.Context, map[string]string) ([]chronos.EntityState, error) {
	return nil, nil
}

func TestRegistry_RoundTrip(t *testing.T) {
	name := "test-adapter-" + time.Now().Format("150405.000000")
	src := &stubSource{name: name}

	chronos.Register(src)

	got, ok := chronos.Get(name)
	if !ok {
		t.Fatalf("Get(%q) ok = false, want true", name)
	}
	if got.Name() != name {
		t.Fatalf("Get(%q).Name() = %q, want %q", name, got.Name(), name)
	}

	var found bool
	for _, n := range chronos.Adapters() {
		if n == name {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("Adapters() does not contain %q", name)
	}
}

func TestRegistry_IsolatedInstances(t *testing.T) {
	a := chronos.NewRegistry()
	b := chronos.NewRegistry()
	name := "isolated-" + time.Now().Format("150405.000000")
	a.Register(&stubSource{name: name})
	if _, ok := b.Get(name); ok {
		t.Fatal("registry B must not see adapters registered on A")
	}
	if _, ok := a.Get(name); !ok {
		t.Fatal("registry A must see its own adapter")
	}
	if _, ok := chronos.Get(name); ok {
		t.Fatal("default registry must not see an isolated registry's adapters")
	}
}

func TestRegister_PanicsOnNil(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic for nil Source")
		}
	}()
	chronos.Register(nil)
}

func TestRegister_PanicsOnEmptyName(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic for empty name")
		}
	}()
	chronos.Register(&stubSource{name: ""})
}
