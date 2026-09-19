-- name: InsertEntityState :exec
INSERT INTO entity_states (id, entity_id, scope_id, timestamp, features, labels, meta, adapter, created_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
    features = excluded.features,
    labels = excluded.labels,
    meta = excluded.meta,
    adapter = excluded.adapter;

-- name: GetEntityStatesByScope :many
SELECT * FROM entity_states
WHERE scope_id = ?
ORDER BY timestamp DESC;

-- name: GetEntityStatesByScopeSince :many
SELECT * FROM entity_states
WHERE scope_id = ? AND timestamp >= ?
ORDER BY timestamp DESC;

-- name: GetEntityStatesByEntity :many
SELECT * FROM entity_states
WHERE entity_id = ?
ORDER BY timestamp DESC;

-- name: DeleteOldEntityStates :exec
DELETE FROM entity_states
WHERE timestamp < ? AND adapter = ?;

-- name: CountEntityStates :one
SELECT COUNT(*) FROM entity_states WHERE adapter = ?;

-- name: InsertSignal :exec
INSERT INTO signals (id, scope_id, series_id, pattern, detected_at, window_start, window_end, strength, confidence, metrics, explanation, confidence_class)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
    strength = excluded.strength,
    confidence = excluded.confidence,
    metrics = excluded.metrics,
    explanation = excluded.explanation,
    confidence_class = excluded.confidence_class;

-- name: InsertSignalEvidence :exec
INSERT INTO signal_evidence (signal_id, series_id, time, kind, score, metrics)
VALUES (?, ?, ?, ?, ?, ?);

-- name: GetSignalByID :one
SELECT * FROM signals WHERE id = ?;

-- name: GetSignalEvidence :many
SELECT * FROM signal_evidence
WHERE signal_id = ?
ORDER BY score DESC;
