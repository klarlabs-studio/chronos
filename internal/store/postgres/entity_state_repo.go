package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/felixgeelhaar/chronos"
	"github.com/google/uuid"
)

// EntityStateRepository persists entity states in PostgreSQL.
type EntityStateRepository struct{ conn *Conn }

// Ingest persists a single observation.
func (r *EntityStateRepository) Ingest(ctx context.Context, adapterName string, state chronos.EntityState) error {
	if err := state.Validate(); err != nil {
		return fmt.Errorf("entity_state ingest: validate: %w", err)
	}
	return r.upsert(ctx, r.conn.DB, adapterName, state)
}

// Save upserts a batch of observations inside a single transaction.
// Above [BulkSaveThreshold] rows the batch switches to a multi-row
// INSERT (chunks of [bulkChunkSize]) instead of one statement per
// row. This avoids the round-trip overhead of per-row INSERT against
// the wire protocol while staying portable across every Postgres-
// wire-compatible engine (CockroachDB, YugabyteDB, Neon, etc.) —
// including engines whose pgx COPY support is incomplete.
func (r *EntityStateRepository) Save(ctx context.Context, adapterName string, states []chronos.EntityState) error {
	if len(states) >= BulkSaveThreshold {
		return r.bulkSave(ctx, adapterName, states)
	}
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

// BulkSaveThreshold is the row count above which Save switches to
// the chunked multi-row INSERT path.
const BulkSaveThreshold = 200

// bulkChunkSize caps the rows per multi-row INSERT statement so we
// never exceed Postgres's 65535 placeholder limit (9 columns × 7280
// rows = 65520).
const bulkChunkSize = 1000

// bulkSave emits multi-row INSERT…VALUES…ON CONFLICT statements in
// chunks so a 100k-row batch becomes 100 round-trips instead of
// 100k. All chunks run in a single transaction so partial failure
// rolls back the whole batch.
func (r *EntityStateRepository) bulkSave(ctx context.Context, adapterName string, states []chronos.EntityState) error {
	for _, s := range states {
		if err := s.Validate(); err != nil {
			return fmt.Errorf("entity_state bulksave: validate %s: %w", s.ID, err)
		}
	}
	tx, err := r.conn.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("entity_state bulksave: begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	now := time.Now()
	for start := 0; start < len(states); start += bulkChunkSize {
		end := start + bulkChunkSize
		if end > len(states) {
			end = len(states)
		}
		chunk := states[start:end]
		if err := r.bulkInsertChunk(ctx, tx, adapterName, chunk, now); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("entity_state bulksave: commit: %w", err)
	}
	return nil
}

func (r *EntityStateRepository) bulkInsertChunk(ctx context.Context, tx *sql.Tx, adapterName string, chunk []chronos.EntityState, now time.Time) error {
	const cols = 9
	args := make([]any, 0, len(chunk)*cols)
	placeholders := make([]string, 0, len(chunk))
	for i, s := range chunk {
		base := i*cols + 1
		placeholders = append(placeholders, fmt.Sprintf(
			"($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)",
			base, base+1, base+2, base+3, base+4, base+5, base+6, base+7, base+8,
		))
		featuresJSON, _ := json.Marshal(s.Features)
		labelsJSON, _ := json.Marshal(s.Labels)
		metaJSON, _ := json.Marshal(s.Meta)
		args = append(args,
			s.ID, s.EntityID, s.ScopeID,
			s.Timestamp,
			featuresJSON, labelsJSON, metaJSON,
			adapterName, now,
		)
	}
	// G202 false positive: placeholders is a fixed string list ("$1, $2, ...")
	// generated programmatically; no user input is concatenated into the SQL.
	q := "INSERT INTO entity_states (id, entity_id, scope_id, timestamp, features, labels, meta, adapter, created_at) VALUES " + //nolint:gosec
		strings.Join(placeholders, ", ") +
		" ON CONFLICT (id) DO UPDATE SET features = EXCLUDED.features, labels = EXCLUDED.labels, meta = EXCLUDED.meta, adapter = EXCLUDED.adapter"
	if _, err := tx.ExecContext(ctx, q, args...); err != nil {
		return fmt.Errorf("entity_state bulksave: chunk: %w", err)
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
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (id) DO UPDATE SET
			features = EXCLUDED.features,
			labels = EXCLUDED.labels,
			meta = EXCLUDED.meta,
			adapter = EXCLUDED.adapter
	`,
		s.ID, s.EntityID, s.ScopeID,
		s.Timestamp,
		featuresJSON, labelsJSON, metaJSON,
		adapterName, time.Now(),
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
		WHERE scope_id = $1
		ORDER BY timestamp DESC
	`, scopeID)
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
		WHERE scope_id = $1 AND timestamp >= $2
		ORDER BY timestamp DESC
	`, scopeID, cutoff)
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
		WHERE entity_id = $1
		ORDER BY timestamp DESC
	`, entityID)
	if err != nil {
		return nil, fmt.Errorf("entity_state list by entity: %w", err)
	}
	return scanEntityStates(rows)
}

// DeleteOlderThan removes states observed before cutoff for adapterName.
func (r *EntityStateRepository) DeleteOlderThan(ctx context.Context, cutoff time.Time, adapterName string) error {
	if _, err := r.conn.DB.ExecContext(ctx,
		`DELETE FROM entity_states WHERE timestamp < $1 AND adapter = $2`,
		cutoff, adapterName,
	); err != nil {
		return fmt.Errorf("entity_state delete old: %w", err)
	}
	return nil
}

// Count returns the number of states stored under adapterName.
func (r *EntityStateRepository) Count(ctx context.Context, adapterName string) (int64, error) {
	var n int64
	if err := r.conn.DB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM entity_states WHERE adapter = $1`, adapterName,
	).Scan(&n); err != nil {
		return 0, fmt.Errorf("entity_state count: %w", err)
	}
	return n, nil
}

// ListScopes returns the distinct ScopeIDs that have at least one
// observation. Used by the in-process detection scheduler.
func (r *EntityStateRepository) ListScopes(ctx context.Context) ([]uuid.UUID, error) {
	rows, err := r.conn.DB.QueryContext(ctx, `SELECT DISTINCT scope_id FROM entity_states`)
	if err != nil {
		return nil, fmt.Errorf("entity_state list scopes: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("entity_state list scopes scan: %w", err)
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
		var features, labels, meta []byte
		if err := rows.Scan(&s.ID, &s.EntityID, &s.ScopeID, &s.Timestamp, &features, &labels, &meta); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
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
