package memory

import (
	"context"
	"sort"
	"time"

	"github.com/felixgeelhaar/chronos/internal/domain"
	"github.com/felixgeelhaar/chronos/internal/ports"
	"github.com/google/uuid"
)

// SignalRepository implements ports.SignalRepository in memory.
type SignalRepository struct{ conn *Conn }

// Save appends or replaces a signal by ID.
func (r *SignalRepository) Save(_ context.Context, sig domain.Signal) error {
	if err := sig.Validate(); err != nil {
		return err
	}
	r.conn.mu.Lock()
	defer r.conn.mu.Unlock()
	for i, existing := range r.conn.signals {
		if existing.ID == sig.ID {
			r.conn.signals[i] = sig
			return nil
		}
	}
	r.conn.signals = append(r.conn.signals, sig)
	return nil
}

// List returns signals matching the filter, ordered detected-at desc
// then confidence desc.
func (r *SignalRepository) List(_ context.Context, filter ports.SignalFilter) ([]domain.Signal, error) {
	r.conn.mu.RLock()
	defer r.conn.mu.RUnlock()

	var out []domain.Signal
	for _, sig := range r.conn.signals {
		if !matches(sig, filter) {
			continue
		}
		out = append(out, sig)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if !out[i].DetectedAt.Equal(out[j].DetectedAt) {
			return out[i].DetectedAt.After(out[j].DetectedAt)
		}
		return out[i].Confidence > out[j].Confidence
	})
	if filter.Limit > 0 && len(out) > filter.Limit {
		out = out[:filter.Limit]
	}
	return out, nil
}

// Get returns a single signal by ID.
func (r *SignalRepository) Get(_ context.Context, id uuid.UUID) (domain.Signal, error) {
	r.conn.mu.RLock()
	defer r.conn.mu.RUnlock()
	for _, sig := range r.conn.signals {
		if sig.ID == id {
			return sig, nil
		}
	}
	return domain.Signal{}, domain.ErrSignalNotFound
}

// Count returns the number of signals matching filter.
func (r *SignalRepository) Count(_ context.Context, filter ports.SignalFilter) (int64, error) {
	r.conn.mu.RLock()
	defer r.conn.mu.RUnlock()
	var n int64
	for _, sig := range r.conn.signals {
		if matches(sig, filter) {
			n++
		}
	}
	return n, nil
}

// DeleteSignalsOlderThan removes signals detected before cutoff.
func (r *SignalRepository) DeleteSignalsOlderThan(_ context.Context, cutoff time.Time, limit int) (int64, error) {
	r.conn.mu.Lock()
	defer r.conn.mu.Unlock()
	// Oldest first, so a bounded sweep makes progress from the far end
	// rather than deleting an arbitrary subset and leaving the oldest
	// rows behind forever.
	order := make([]int, 0, len(r.conn.signals))
	for i, sig := range r.conn.signals {
		if sig.DetectedAt.Before(cutoff) {
			order = append(order, i)
		}
	}
	sort.SliceStable(order, func(a, b int) bool {
		return r.conn.signals[order[a]].DetectedAt.Before(r.conn.signals[order[b]].DetectedAt)
	})
	if limit > 0 && len(order) > limit {
		order = order[:limit]
	}
	doomed := make(map[int]struct{}, len(order))
	for _, i := range order {
		doomed[i] = struct{}{}
	}
	kept := r.conn.signals[:0]
	var removed int64
	for i, sig := range r.conn.signals {
		if _, ok := doomed[i]; ok {
			removed++
			continue
		}
		kept = append(kept, sig)
	}
	r.conn.signals = kept
	return removed, nil
}

func matches(sig domain.Signal, f ports.SignalFilter) bool {
	if f.ScopeID != uuid.Nil && sig.ScopeID != f.ScopeID {
		return false
	}
	if len(f.ScopeIDs) > 0 {
		found := false
		for _, allowed := range f.ScopeIDs {
			if sig.ScopeID == allowed {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	if f.Series != nil && sig.Series != *f.Series {
		return false
	}
	if f.Pattern != nil && sig.Pattern != *f.Pattern {
		return false
	}
	if f.Since != nil && sig.DetectedAt.Before(*f.Since) {
		return false
	}
	if f.Until != nil && !sig.DetectedAt.Before(*f.Until) {
		return false
	}
	if f.MinConfidence != nil && sig.Confidence < *f.MinConfidence {
		return false
	}
	if f.Window != nil &&
		(!sig.Window.Start.Equal(f.Window.Start) || !sig.Window.End.Equal(f.Window.End)) {
		return false
	}
	return true
}
