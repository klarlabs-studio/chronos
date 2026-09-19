package batching

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/felixgeelhaar/chronos"
	"github.com/google/uuid"
)

// fakeUpstream records Save / Ingest calls for assertion.
type fakeUpstream struct {
	mu      sync.Mutex
	saves   [][]chronos.EntityState
	ingests int32
}

func (f *fakeUpstream) Ingest(_ context.Context, _ string, s chronos.EntityState) error {
	atomic.AddInt32(&f.ingests, 1)
	return nil
}

func (f *fakeUpstream) Save(_ context.Context, _ string, states []chronos.EntityState) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := make([]chronos.EntityState, len(states))
	copy(cp, states)
	f.saves = append(f.saves, cp)
	return nil
}

func (f *fakeUpstream) ListByScopeSince(_ context.Context, _ uuid.UUID, _ time.Time) ([]chronos.EntityState, error) {
	return nil, nil
}

func (f *fakeUpstream) ListByScope(_ context.Context, _ uuid.UUID) ([]chronos.EntityState, error) {
	return nil, nil
}
func (f *fakeUpstream) ListByEntity(_ context.Context, _ uuid.UUID) ([]chronos.EntityState, error) {
	return nil, nil
}
func (f *fakeUpstream) DeleteOlderThan(_ context.Context, _ time.Time, _ string) error { return nil }
func (f *fakeUpstream) Count(_ context.Context, _ string) (int64, error)               { return 0, nil }
func (f *fakeUpstream) ListScopes(_ context.Context) ([]uuid.UUID, error)              { return nil, nil }

func mkState() chronos.EntityState {
	return chronos.EntityState{
		ID:        uuid.New(),
		EntityID:  uuid.New(),
		ScopeID:   uuid.New(),
		Timestamp: time.Now(),
		Features:  []float64{1.0},
	}
}

func TestRepo_FlushOnSizeBoundary(t *testing.T) {
	t.Parallel()
	up := &fakeUpstream{}
	r, err := New(up, Config{MaxBatch: 3, MaxWait: time.Hour})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	for i := 0; i < 3; i++ {
		if err := r.Ingest(context.Background(), "adapter", mkState()); err != nil {
			t.Fatalf("ingest %d: %v", i, err)
		}
	}
	up.mu.Lock()
	defer up.mu.Unlock()
	if len(up.saves) != 1 {
		t.Errorf("save count = %d, want 1", len(up.saves))
	}
	if len(up.saves[0]) != 3 {
		t.Errorf("batch size = %d, want 3", len(up.saves[0]))
	}
}

func TestRepo_FlushOnDeadline(t *testing.T) {
	t.Parallel()
	up := &fakeUpstream{}
	r, err := New(up, Config{MaxBatch: 100, MaxWait: 50 * time.Millisecond})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if err := r.Ingest(context.Background(), "adapter", mkState()); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	time.Sleep(150 * time.Millisecond)
	up.mu.Lock()
	defer up.mu.Unlock()
	if len(up.saves) != 1 {
		t.Errorf("save count = %d, want 1", len(up.saves))
	}
}

func TestRepo_CloseDrainsBuffer(t *testing.T) {
	t.Parallel()
	up := &fakeUpstream{}
	r, err := New(up, Config{MaxBatch: 100, MaxWait: time.Hour})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if err := r.Ingest(context.Background(), "adapter", mkState()); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if err := r.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}
	up.mu.Lock()
	defer up.mu.Unlock()
	if len(up.saves) != 1 {
		t.Errorf("save count = %d, want 1", len(up.saves))
	}
	// Post-close ingest must error.
	if err := r.Ingest(context.Background(), "adapter", mkState()); err == nil {
		t.Error("ingest after close should error")
	}
}

func TestRepo_GroupsByAdapter(t *testing.T) {
	t.Parallel()
	up := &fakeUpstream{}
	r, err := New(up, Config{MaxBatch: 4, MaxWait: time.Hour})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	for _, ad := range []string{"a", "b", "a", "b"} {
		if err := r.Ingest(context.Background(), ad, mkState()); err != nil {
			t.Fatalf("ingest %s: %v", ad, err)
		}
	}
	up.mu.Lock()
	defer up.mu.Unlock()
	if len(up.saves) != 2 {
		t.Errorf("save count = %d, want 2 (one per adapter)", len(up.saves))
	}
}

func TestRepo_RejectsBadConfig(t *testing.T) {
	t.Parallel()
	if _, err := New(&fakeUpstream{}, Config{}); err == nil {
		t.Error("zero config should error")
	}
	if _, err := New(nil, Config{MaxBatch: 1, MaxWait: time.Second}); err == nil {
		t.Error("nil upstream should error")
	}
}

func TestRepo_SaveBypassesBuffer(t *testing.T) {
	t.Parallel()
	up := &fakeUpstream{}
	r, err := New(up, Config{MaxBatch: 100, MaxWait: time.Hour})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if err := r.Save(context.Background(), "adapter", []chronos.EntityState{mkState()}); err != nil {
		t.Fatalf("save: %v", err)
	}
	up.mu.Lock()
	defer up.mu.Unlock()
	if len(up.saves) != 1 {
		t.Errorf("save count = %d, want 1", len(up.saves))
	}
}
