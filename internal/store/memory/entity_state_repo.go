package memory

import (
	"context"
	"sort"
	"time"

	"github.com/felixgeelhaar/chronos"
	"github.com/google/uuid"
)

// EntityStateRepository implements ports.EntityStateRepository in memory.
type EntityStateRepository struct{ conn *Conn }

// Ingest persists a single observation.
func (r *EntityStateRepository) Ingest(_ context.Context, adapterName string, state chronos.EntityState) error {
	if err := state.Validate(); err != nil {
		return err
	}
	r.conn.mu.Lock()
	defer r.conn.mu.Unlock()
	r.conn.put(adapterName, state)
	return nil
}

// put stores or updates one observation. The caller holds c.mu.
//
// A repeated write of the same observation ID is an idempotent update,
// matching the ON CONFLICT (id) clause every SQL backend uses:
// features, labels, meta and the adapter attribution are replaced; the
// identity columns (id, entity_id, scope_id) and the observation
// timestamp are not. Re-ingesting corrects an observation's payload; it
// does not move the observation in time.
func (c *Conn) put(adapter string, s chronos.EntityState) {
	// Reads return UTC on every backend — the SQL stores normalise on
	// the way through their drivers — so this one normalises on the
	// way in. It also drops the monotonic reading, which a timestamp
	// that has been through a database never carries.
	s.Timestamp = s.Timestamp.UTC()
	if ref, ok := c.stateIndex[s.ID]; ok {
		updated := c.entityStates[ref.scope][ref.idx].state
		updated.Features = s.Features
		updated.Labels = s.Labels
		updated.Meta = s.Meta
		c.entityStates[ref.scope][ref.idx] = storedState{state: updated, adapter: adapter}
		return
	}
	bucket := c.entityStates[s.ScopeID]
	c.entityStates[s.ScopeID] = append(bucket, storedState{state: s, adapter: adapter})
	c.stateIndex[s.ID] = stateRef{scope: s.ScopeID, idx: len(bucket)}
}

// reindex rebuilds stateIndex after a delete has moved positions. The
// caller holds c.mu.
func (c *Conn) reindex() {
	c.stateIndex = make(map[uuid.UUID]stateRef, len(c.stateIndex))
	for scopeID, list := range c.entityStates {
		for i, s := range list {
			c.stateIndex[s.state.ID] = stateRef{scope: scopeID, idx: i}
		}
	}
}

// Save persists a batch of observations. Validation runs over the
// whole batch before anything is written, so a rejected observation
// leaves the store exactly as it was — the same all-or-nothing
// guarantee the SQL backends get from wrapping the batch in a
// transaction.
func (r *EntityStateRepository) Save(_ context.Context, adapterName string, states []chronos.EntityState) error {
	for _, s := range states {
		if err := s.Validate(); err != nil {
			return err
		}
	}
	r.conn.mu.Lock()
	defer r.conn.mu.Unlock()
	for _, s := range states {
		r.conn.put(adapterName, s)
	}
	return nil
}

// ListByScope returns a defensive copy of all states recorded under
// scopeID, most recent first.
func (r *EntityStateRepository) ListByScope(_ context.Context, scopeID uuid.UUID) ([]chronos.EntityState, error) {
	r.conn.mu.RLock()
	defer r.conn.mu.RUnlock()
	stored := r.conn.entityStates[scopeID]
	out := make([]chronos.EntityState, 0, len(stored))
	for _, s := range stored {
		out = append(out, s.state)
	}
	sortByTimestampDesc(out)
	return out, nil
}

// ListByScopeSince returns a defensive copy of the scope's states
// observed at or after cutoff, most recent first.
func (r *EntityStateRepository) ListByScopeSince(_ context.Context, scopeID uuid.UUID, cutoff time.Time) ([]chronos.EntityState, error) {
	r.conn.mu.RLock()
	defer r.conn.mu.RUnlock()
	stored := r.conn.entityStates[scopeID]
	out := make([]chronos.EntityState, 0, len(stored))
	for _, s := range stored {
		if s.state.Timestamp.Before(cutoff) {
			continue
		}
		out = append(out, s.state)
	}
	sortByTimestampDesc(out)
	return out, nil
}

// ListByEntity scans all scopes for observations of entityID, most
// recent first.
func (r *EntityStateRepository) ListByEntity(_ context.Context, entityID uuid.UUID) ([]chronos.EntityState, error) {
	r.conn.mu.RLock()
	defer r.conn.mu.RUnlock()
	var out []chronos.EntityState
	for _, scope := range r.conn.entityStates {
		for _, s := range scope {
			if s.state.EntityID == entityID {
				out = append(out, s.state)
			}
		}
	}
	sortByTimestampDesc(out)
	return out, nil
}

// DeleteOlderThan removes states observed before cutoff for the named
// adapter.
func (r *EntityStateRepository) DeleteOlderThan(_ context.Context, cutoff time.Time, adapterName string) error {
	r.conn.mu.Lock()
	defer r.conn.mu.Unlock()
	for scopeID, list := range r.conn.entityStates {
		kept := list[:0]
		for _, s := range list {
			if s.adapter == adapterName && s.state.Timestamp.Before(cutoff) {
				continue
			}
			kept = append(kept, s)
		}
		r.conn.entityStates[scopeID] = kept
	}
	r.conn.reindex()
	return nil
}

// ListScopes returns the distinct ScopeIDs that have at least one
// observation. Order is unspecified. The scheduler uses this to know
// which scopes to detect over.
func (r *EntityStateRepository) ListScopes(_ context.Context) ([]uuid.UUID, error) {
	r.conn.mu.RLock()
	defer r.conn.mu.RUnlock()
	out := make([]uuid.UUID, 0, len(r.conn.entityStates))
	for scopeID, list := range r.conn.entityStates {
		if len(list) > 0 {
			out = append(out, scopeID)
		}
	}
	return out, nil
}

// Count returns the total number of states recorded by adapterName.
func (r *EntityStateRepository) Count(_ context.Context, adapterName string) (int64, error) {
	r.conn.mu.RLock()
	defer r.conn.mu.RUnlock()
	var n int64
	for _, list := range r.conn.entityStates {
		for _, s := range list {
			if s.adapter == adapterName {
				n++
			}
		}
	}
	return n, nil
}

func sortByTimestampDesc(states []chronos.EntityState) {
	sort.SliceStable(states, func(i, j int) bool { return states[i].Timestamp.After(states[j].Timestamp) })
}
