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

// Save appends a signal, or updates one already stored under the same
// ID.
//
// The update replaces what a re-run of the detector can revise — the
// quantities, the explanation, the confidence class and the evidence —
// and leaves the perception's identity alone: scope, series, pattern,
// detected-at and the analysis window stay as first recorded. That is
// the ON CONFLICT (id) column set every SQL backend uses, and a signal
// is a historical fact, not a mutable row.
func (r *SignalRepository) Save(_ context.Context, sig domain.Signal) error {
	if err := sig.Validate(); err != nil {
		return err
	}
	sig = normaliseSignalTimes(sig)
	r.conn.mu.Lock()
	defer r.conn.mu.Unlock()
	for i, existing := range r.conn.signals {
		if existing.ID == sig.ID {
			existing.Strength = sig.Strength
			existing.Confidence = sig.Confidence
			existing.Metrics = sig.Metrics
			existing.Explanation = sig.Explanation
			existing.ConfidenceClass = sig.ConfidenceClass
			existing.Evidence = sig.Evidence
			r.conn.signals[i] = existing
			return nil
		}
	}
	r.conn.signals = append(r.conn.signals, sig)
	return nil
}

// normaliseSignalTimes converts every timestamp on a signal to UTC.
// The SQL backends do this on the way through their drivers, so the
// in-memory store does it explicitly rather than handing back whatever
// zone (and monotonic reading) the detector happened to carry.
// Evidence and feature-evolution slices are copied before being
// rewritten so the caller's own slice is left untouched.
func normaliseSignalTimes(sig domain.Signal) domain.Signal {
	sig.DetectedAt = sig.DetectedAt.UTC()
	sig.Window.Start = sig.Window.Start.UTC()
	sig.Window.End = sig.Window.End.UTC()
	if len(sig.Evidence) > 0 {
		evidence := make([]domain.Evidence, len(sig.Evidence))
		copy(evidence, sig.Evidence)
		for i := range evidence {
			evidence[i].Time = evidence[i].Time.UTC()
		}
		sig.Evidence = evidence
	}
	if len(sig.Explanation.FeatureEvolution) > 0 {
		samples := make([]domain.FeatureSample, len(sig.Explanation.FeatureEvolution))
		copy(samples, sig.Explanation.FeatureEvolution)
		for i := range samples {
			samples[i].At = samples[i].At.UTC()
		}
		sig.Explanation.FeatureEvolution = samples
	}
	return sig
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
