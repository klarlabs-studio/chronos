package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/felixgeelhaar/chronos"
	"github.com/google/uuid"
)

// EntityStateRepository persists entity states in MySQL. Mirrors the
// Postgres implementation; differences are all dialect-level
// (placeholder syntax `?`, ON DUPLICATE KEY UPDATE, CHAR(36) UUIDs).
type EntityStateRepository struct{ conn *Conn }

// Ingest persists a single observation.
func (r *EntityStateRepository) Ingest(ctx context.Context, adapterName string, state chronos.EntityState) error {
	if err := state.Validate(); err != nil {
		return fmt.Errorf("entity_state ingest: validate: %w", err)
	}
	return r.upsert(ctx, r.conn.DB, adapterName, state)
}

// Save upserts a batch of observations inside a single transaction.
func (r *EntityStateRepository) Save(ctx context.Context, adapterName string, states []chronos.EntityState) error {
	tx, err := r.conn.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("entity_state save: begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	for _, s := range states {
		if err := s.Validate(); err != nil {
			return fmt.Errorf("entity_state save: validate %s: %w", s.ID, err)
		}
		if err := r.upsert(ctx, tx, adapterName, s); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("entity_state save: commit: %w", err)
	}
	return nil
}

type execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func (r *EntityStateRepository) upsert(ctx context.Context, ex execer, adapterName string, s chronos.EntityState) error {
	featuresJSON, _ := json.Marshal(s.Features)
	labelsJSON, _ := json.Marshal(s.Labels)
	metaJSON, _ := json.Marshal(s.Meta)
	_, err := ex.ExecContext(ctx, `
		INSERT INTO entity_states (id, entity_id, scope_id, timestamp, features, labels, meta, adapter, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			features = VALUES(features),
			labels = VALUES(labels),
			meta = VALUES(meta),
			adapter = VALUES(adapter)
	`,
		s.ID.String(), s.EntityID.String(), s.ScopeID.String(),
		s.Timestamp.UTC(),
		string(featuresJSON), string(labelsJSON), string(metaJSON),
		adapterName, time.Now().UTC(),
	)
	if err != nil {
		return fmt.Errorf("entity_state upsert %s: %w", s.ID, err)
	}
	return nil
}

// ListByScope returns all entity states for a scope, most recent first.
func (r *EntityStateRepository) ListByScope(ctx context.Context, scopeID uuid.UUID) ([]chronos.EntityState, error) {
	rows, err := r.conn.DB.QueryContext(ctx, `
		SELECT id, entity_id, scope_id, timestamp, features, labels, meta
		FROM entity_states
		WHERE scope_id = ?
		ORDER BY timestamp DESC
	`, scopeID.String())
	if err != nil {
		return nil, fmt.Errorf("entity_state list by scope: %w", err)
	}
	return scanEntityStates(rows)
}

// ListByScopeSince returns the scope's states observed at or after
// cutoff, most recent first.
func (r *EntityStateRepository) ListByScopeSince(ctx context.Context, scopeID uuid.UUID, cutoff time.Time) ([]chronos.EntityState, error) {
	rows, err := r.conn.DB.QueryContext(ctx, `
		SELECT id, entity_id, scope_id, timestamp, features, labels, meta
		FROM entity_states
		WHERE scope_id = ? AND timestamp >= ?
		ORDER BY timestamp DESC
	`, scopeID.String(), cutoff)
	if err != nil {
		return nil, fmt.Errorf("entity_state list by scope since: %w", err)
	}
	return scanEntityStates(rows)
}

// ListByEntity returns all observations of an entity, most recent first.
func (r *EntityStateRepository) ListByEntity(ctx context.Context, entityID uuid.UUID) ([]chronos.EntityState, error) {
	rows, err := r.conn.DB.QueryContext(ctx, `
		SELECT id, entity_id, scope_id, timestamp, features, labels, meta
		FROM entity_states
		WHERE entity_id = ?
		ORDER BY timestamp DESC
	`, entityID.String())
	if err != nil {
		return nil, fmt.Errorf("entity_state list by entity: %w", err)
	}
	return scanEntityStates(rows)
}

// DeleteOlderThan removes states observed before cutoff for adapterName.
func (r *EntityStateRepository) DeleteOlderThan(ctx context.Context, cutoff time.Time, adapterName string) error {
	if _, err := r.conn.DB.ExecContext(ctx,
		`DELETE FROM entity_states WHERE timestamp < ? AND adapter = ?`,
		cutoff.UTC(), adapterName,
	); err != nil {
		return fmt.Errorf("entity_state delete old: %w", err)
	}
	return nil
}

// Count returns the number of states stored under adapterName.
func (r *EntityStateRepository) Count(ctx context.Context, adapterName string) (int64, error) {
	var n int64
	if err := r.conn.DB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM entity_states WHERE adapter = ?`, adapterName,
	).Scan(&n); err != nil {
		return 0, fmt.Errorf("entity_state count: %w", err)
	}
	return n, nil
}

// ListScopes returns the distinct ScopeIDs that have at least one
// observation.
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

func scanEntityStates(rows *sql.Rows) ([]chronos.EntityState, error) {
	defer func() { _ = rows.Close() }()
	var out []chronos.EntityState
	for rows.Next() {
		var s chronos.EntityState
		var idStr, entityStr, scopeStr string
		var features, labels, meta []byte
		if err := rows.Scan(&idStr, &entityStr, &scopeStr, &s.Timestamp, &features, &labels, &meta); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		var err error
		if s.ID, err = uuid.Parse(idStr); err != nil {
			return nil, fmt.Errorf("parse id %q: %w", idStr, err)
		}
		if s.EntityID, err = uuid.Parse(entityStr); err != nil {
			return nil, fmt.Errorf("parse entity_id %q: %w", entityStr, err)
		}
		if s.ScopeID, err = uuid.Parse(scopeStr); err != nil {
			return nil, fmt.Errorf("parse scope_id %q: %w", scopeStr, err)
		}
		_ = json.Unmarshal(features, &s.Features)
		if len(labels) > 0 {
			_ = json.Unmarshal(labels, &s.Labels)
		}
		if len(meta) > 0 {
			_ = json.Unmarshal(meta, &s.Meta)
		}
		out = append(out, s)
	}
	return out, rows.Err()
}
