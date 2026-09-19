package sqlite

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/felixgeelhaar/chronos"
	"github.com/felixgeelhaar/chronos/internal/store/sqlite/sqlcgen"
	"github.com/google/uuid"
)

// EntityStateRepository persists entity states in SQLite. Features,
// Labels, and Meta are JSON-encoded TEXT to keep adapter-specific schema
// out of the engine's tables.
type EntityStateRepository struct{ conn *Conn }

// Ingest persists a single observation.
func (r *EntityStateRepository) Ingest(ctx context.Context, adapterName string, state chronos.EntityState) error {
	if err := state.Validate(); err != nil {
		return fmt.Errorf("entity_state ingest: validate: %w", err)
	}
	return r.insert(ctx, r.conn.q, adapterName, state)
}

// Save persists a batch of observations transactionally.
func (r *EntityStateRepository) Save(ctx context.Context, adapterName string, states []chronos.EntityState) error {
	tx, err := r.conn.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("entity_state save: begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	q := r.conn.q.WithTx(tx)
	for _, s := range states {
		if err := s.Validate(); err != nil {
			return fmt.Errorf("entity_state save: validate %s: %w", s.ID, err)
		}
		if err := r.insert(ctx, q, adapterName, s); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("entity_state save: commit: %w", err)
	}
	return nil
}

func (r *EntityStateRepository) insert(ctx context.Context, q *sqlcgen.Queries, adapterName string, s chronos.EntityState) error {
	featuresJSON, _ := json.Marshal(s.Features)
	labelsJSON, _ := json.Marshal(s.Labels)
	metaJSON, _ := json.Marshal(s.Meta)
	now := formatTime(time.Now())
	if err := q.InsertEntityState(ctx, sqlcgen.InsertEntityStateParams{
		ID:        s.ID.String(),
		EntityID:  s.EntityID.String(),
		ScopeID:   s.ScopeID.String(),
		Timestamp: formatTime(s.Timestamp),
		Features:  string(featuresJSON),
		Labels:    string(labelsJSON),
		Meta:      string(metaJSON),
		Adapter:   adapterName,
		CreatedAt: now,
	}); err != nil {
		return fmt.Errorf("entity_state insert %s: %w", s.ID, err)
	}
	return nil
}

// ListByScope returns the scope's states, most recent first.
func (r *EntityStateRepository) ListByScope(ctx context.Context, scopeID uuid.UUID) ([]chronos.EntityState, error) {
	rows, err := r.conn.q.GetEntityStatesByScope(ctx, scopeID.String())
	if err != nil {
		return nil, fmt.Errorf("entity_state list by scope: %w", err)
	}
	return decodeEntityStates(rows)
}

// ListByScopeSince returns the scope's states observed at or after
// cutoff, most recent first. Cutoff is encoded with formatTime, the
// same encoding used on write and by DeleteOlderThan, so the TEXT
// comparison orders correctly.
func (r *EntityStateRepository) ListByScopeSince(ctx context.Context, scopeID uuid.UUID, cutoff time.Time) ([]chronos.EntityState, error) {
	rows, err := r.conn.q.GetEntityStatesByScopeSince(ctx, sqlcgen.GetEntityStatesByScopeSinceParams{
		ScopeID:   scopeID.String(),
		Timestamp: formatTime(cutoff),
	})
	if err != nil {
		return nil, fmt.Errorf("entity_state list by scope since: %w", err)
	}
	return decodeEntityStates(rows)
}

// ListByEntity returns all observations of an entity, most recent first.
func (r *EntityStateRepository) ListByEntity(ctx context.Context, entityID uuid.UUID) ([]chronos.EntityState, error) {
	rows, err := r.conn.q.GetEntityStatesByEntity(ctx, entityID.String())
	if err != nil {
		return nil, fmt.Errorf("entity_state list by entity: %w", err)
	}
	return decodeEntityStates(rows)
}

// DeleteOlderThan removes states observed before cutoff for adapterName.
func (r *EntityStateRepository) DeleteOlderThan(ctx context.Context, cutoff time.Time, adapterName string) error {
	if err := r.conn.q.DeleteOldEntityStates(ctx, sqlcgen.DeleteOldEntityStatesParams{
		Timestamp: formatTime(cutoff),
		Adapter:   adapterName,
	}); err != nil {
		return fmt.Errorf("entity_state delete old: %w", err)
	}
	return nil
}

// ListScopes returns the distinct ScopeIDs that have at least one
// observation. Used by the in-process detection scheduler. The query
// is direct SQL rather than sqlc-generated to keep the surface
// minimal — there is exactly one query and it has no parameters.
func (r *EntityStateRepository) ListScopes(ctx context.Context) ([]uuid.UUID, error) {
	rows, err := r.conn.DB.QueryContext(ctx, `SELECT DISTINCT scope_id FROM entity_states`)
	if err != nil {
		return nil, fmt.Errorf("entity_state list scopes: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []uuid.UUID
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, fmt.Errorf("entity_state list scopes scan: %w", err)
		}
		id, err := uuid.Parse(s)
		if err != nil {
			return nil, fmt.Errorf("entity_state list scopes parse %q: %w", s, err)
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// Count returns the number of states stored under adapterName.
func (r *EntityStateRepository) Count(ctx context.Context, adapterName string) (int64, error) {
	n, err := r.conn.q.CountEntityStates(ctx, adapterName)
	if err != nil {
		return 0, fmt.Errorf("entity_state count: %w", err)
	}
	return n, nil
}

func decodeEntityStates(rows []sqlcgen.EntityState) ([]chronos.EntityState, error) {
	out := make([]chronos.EntityState, 0, len(rows))
	for _, row := range rows {
		s, err := decodeEntityState(row)
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, nil
}

func decodeEntityState(row sqlcgen.EntityState) (chronos.EntityState, error) {
	id, err := uuid.Parse(row.ID)
	if err != nil {
		return chronos.EntityState{}, fmt.Errorf("decode id: %w", err)
	}
	entityID, err := uuid.Parse(row.EntityID)
	if err != nil {
		return chronos.EntityState{}, fmt.Errorf("decode entity_id: %w", err)
	}
	scopeID, err := uuid.Parse(row.ScopeID)
	if err != nil {
		return chronos.EntityState{}, fmt.Errorf("decode scope_id: %w", err)
	}
	ts, err := parseTime(row.Timestamp)
	if err != nil {
		return chronos.EntityState{}, fmt.Errorf("decode timestamp: %w", err)
	}
	var features []float64
	if row.Features != "" {
		if err := json.Unmarshal([]byte(row.Features), &features); err != nil {
			return chronos.EntityState{}, fmt.Errorf("decode features: %w", err)
		}
	}
	var labels []string
	if row.Labels != "" {
		_ = json.Unmarshal([]byte(row.Labels), &labels)
	}
	meta := map[string]string{}
	if row.Meta != "" {
		_ = json.Unmarshal([]byte(row.Meta), &meta)
	}
	return chronos.EntityState{
		ID:        id,
		EntityID:  entityID,
		ScopeID:   scopeID,
		Timestamp: ts,
		Features:  features,
		Labels:    labels,
		Meta:      meta,
	}, nil
}
