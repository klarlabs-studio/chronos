package notify

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/felixgeelhaar/chronos/internal/domain"
	"github.com/felixgeelhaar/chronos/internal/ports"
	"github.com/google/uuid"
)

// fakeRepo is a minimal SignalRepository used by these tests. Behaviour
// is configurable per-call via the saveErr field; List/Get/Count are
// delegated to record-keeping so we can verify pass-through.
type fakeRepo struct {
	saveErr  error
	saved    []domain.Signal
	listResp []domain.Signal
	getResp  domain.Signal
}

func (f *fakeRepo) Save(_ context.Context, sig domain.Signal) error {
	if f.saveErr != nil {
		return f.saveErr
	}
	f.saved = append(f.saved, sig)
	return nil
}

func (f *fakeRepo) List(_ context.Context, _ ports.SignalFilter) ([]domain.Signal, error) {
	return f.listResp, nil
}

func (f *fakeRepo) Get(_ context.Context, _ uuid.UUID) (domain.Signal, error) {
	return f.getResp, nil
}

func (f *fakeRepo) Count(_ context.Context, _ ports.SignalFilter) (int64, error) {
	return int64(len(f.listResp)), nil
}

// recordingNotifier captures every Notify call so the test can assert
// what the wrapper passed through.
type recordingNotifier struct {
	calls []domain.Signal
	panic bool
}

func (r *recordingNotifier) Notify(_ context.Context, sig domain.Signal) {
	if r.panic {
		panic("intentional test panic")
	}
	r.calls = append(r.calls, sig)
}

func sampleSignal() domain.Signal {
	return domain.Signal{
		ID:         uuid.New(),
		ScopeID:    uuid.New(),
		Series:     uuid.New(),
		Pattern:    domain.PatternTypeRecurrence,
		DetectedAt: time.Now(),
		Window:     domain.TimeWindow{Start: time.Now().Add(-time.Hour), End: time.Now()},
		Strength:   0.8,
		Confidence: 0.7,
	}
}

func TestWrapSignals_NotifiesAfterSuccessfulSave(t *testing.T) {
	repo := &fakeRepo{}
	notifier := &recordingNotifier{}
	wrapped := WrapSignals(repo, notifier)

	sig := sampleSignal()
	if err := wrapped.Save(context.Background(), sig); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if len(repo.saved) != 1 || repo.saved[0].ID != sig.ID {
		t.Fatalf("inner Save not invoked")
	}
	if len(notifier.calls) != 1 || notifier.calls[0].ID != sig.ID {
		t.Fatalf("notifier not called for persisted signal")
	}
}

func TestWrapSignals_DoesNotNotifyOnSaveError(t *testing.T) {
	repo := &fakeRepo{saveErr: errors.New("disk full")}
	notifier := &recordingNotifier{}
	wrapped := WrapSignals(repo, notifier)

	if err := wrapped.Save(context.Background(), sampleSignal()); err == nil {
		t.Fatal("expected Save error to propagate")
	}
	if len(notifier.calls) != 0 {
		t.Fatalf("notifier was called despite Save error: %d call(s)", len(notifier.calls))
	}
}

func TestWrapSignals_NilNotifierIsSafe(t *testing.T) {
	repo := &fakeRepo{}
	wrapped := WrapSignals(repo, nil)
	if err := wrapped.Save(context.Background(), sampleSignal()); err != nil {
		t.Fatalf("Save with nil notifier: %v", err)
	}
	if len(repo.saved) != 1 {
		t.Fatal("inner Save should still run with nil notifier")
	}
}

func TestWrapSignals_NotifierPanicDoesNotEscape(t *testing.T) {
	repo := &fakeRepo{}
	notifier := &recordingNotifier{panic: true}
	wrapped := WrapSignals(repo, notifier)

	// A buggy notifier must not crash the persistence path. Save
	// returns nil because the inner Save succeeded.
	if err := wrapped.Save(context.Background(), sampleSignal()); err != nil {
		t.Fatalf("notifier panic propagated as error: %v", err)
	}
	if len(repo.saved) != 1 {
		t.Fatal("Save should have completed before notifier panicked")
	}
}

func TestWrapSignals_DelegatesReadMethods(t *testing.T) {
	want := []domain.Signal{sampleSignal(), sampleSignal()}
	repo := &fakeRepo{listResp: want, getResp: want[0]}
	wrapped := WrapSignals(repo, nil)

	got, err := wrapped.List(context.Background(), ports.SignalFilter{})
	if err != nil || len(got) != len(want) {
		t.Fatalf("List delegate: got %d signals, err=%v", len(got), err)
	}
	one, err := wrapped.Get(context.Background(), uuid.New())
	if err != nil || one.ID != want[0].ID {
		t.Fatalf("Get delegate: got %v err=%v", one.ID, err)
	}
	n, err := wrapped.Count(context.Background(), ports.SignalFilter{})
	if err != nil || n != 2 {
		t.Fatalf("Count delegate: got %d err=%v", n, err)
	}
}

func TestMulti_FansOutToAllNotifiers(t *testing.T) {
	a := &recordingNotifier{}
	b := &recordingNotifier{}
	multi := Multi{a, b}
	sig := sampleSignal()
	multi.Notify(context.Background(), sig)
	if len(a.calls) != 1 || len(b.calls) != 1 {
		t.Fatalf("multi did not fan out: a=%d b=%d", len(a.calls), len(b.calls))
	}
}

func TestMulti_PanicInOneDoesNotBlockOthers(t *testing.T) {
	a := &recordingNotifier{panic: true}
	b := &recordingNotifier{}
	multi := Multi{a, b}
	multi.Notify(context.Background(), sampleSignal())
	if len(b.calls) != 1 {
		t.Fatalf("second notifier missed delivery after first panicked: %d", len(b.calls))
	}
}

func TestMulti_NilEntriesIgnored(t *testing.T) {
	a := &recordingNotifier{}
	multi := Multi{nil, a, nil}
	multi.Notify(context.Background(), sampleSignal())
	if len(a.calls) != 1 {
		t.Fatalf("nil entries should be skipped, but real notifier got %d", len(a.calls))
	}
}

// retainingRepo is a fakeRepo that also advertises SignalRetainer.
type retainingRepo struct {
	fakeRepo
	cutoff  time.Time
	deleted int64
	called  bool
}

func (r *retainingRepo) DeleteSignalsOlderThan(_ context.Context, cutoff time.Time, _ int) (int64, error) {
	r.called = true
	r.cutoff = cutoff
	return r.deleted, nil
}

// The serve path installs this wrapper unconditionally, so it sits
// between the scheduler and the real store. The scheduler discovers
// retention by type-asserting the repository it was handed — which is
// the wrapper — so a wrapper that does not forward the capability makes
// retention a silent no-op on every deployment that has a notifier
// configured, which is all of them.
func TestNotifyingSignalRepository_ForwardsRetention(t *testing.T) {
	inner := &retainingRepo{deleted: 7}
	repo := WrapSignals(inner, &recordingNotifier{})

	if _, ok := ports.SignalRepository(repo).(ports.SignalRetainer); !ok {
		t.Fatal("wrapper does not advertise SignalRetainer; the scheduler's type assertion would fail")
	}

	cutoff := time.Now().Add(-24 * time.Hour)
	n, err := repo.DeleteSignalsOlderThan(context.Background(), cutoff, 0)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if !inner.called {
		t.Fatal("wrapper did not reach the inner repository")
	}
	if !inner.cutoff.Equal(cutoff) {
		t.Fatalf("cutoff %v forwarded as %v", cutoff, inner.cutoff)
	}
	if n != 7 {
		t.Fatalf("deleted count %d, want 7 (the inner repository's answer)", n)
	}
}

// A store that genuinely cannot prune must say so rather than report a
// successful deletion of zero rows, which reads identically to "nothing
// was old enough".
func TestNotifyingSignalRepository_RetentionUnsupportedByInner(t *testing.T) {
	repo := WrapSignals(&fakeRepo{}, &recordingNotifier{})

	_, err := repo.DeleteSignalsOlderThan(context.Background(), time.Now(), 0)
	if !errors.Is(err, ports.ErrNotImplemented) {
		t.Fatalf("err = %v, want ports.ErrNotImplemented", err)
	}
}
