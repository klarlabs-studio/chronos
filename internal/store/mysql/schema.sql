-- MySQL / MariaDB schema for Chronos.
--
-- Conventions:
--   * UUIDs are stored as CHAR(36) (MySQL has no native UUID type).
--   * Timestamps are DATETIME(6) so we keep microsecond precision; the
--     driver is configured with parseTime=true&loc=UTC to round-trip
--     time.Time correctly.
--   * Engine is implicit (InnoDB on every modern install) so we don't
--     pin it and break MariaDB / TiDB / PlanetScale, which all use
--     compatible defaults.
--   * Charset is implicit utf8mb4 — all string content is UTF-8.

CREATE TABLE IF NOT EXISTS entity_states (
    id          CHAR(36)     NOT NULL PRIMARY KEY,
    entity_id   CHAR(36)     NOT NULL,
    scope_id    CHAR(36)     NOT NULL,
    timestamp   DATETIME(6)  NOT NULL,
    features    JSON         NOT NULL,
    labels      JSON,
    meta        JSON,
    adapter     VARCHAR(255) NOT NULL,
    created_at  DATETIME(6)  NOT NULL,
    INDEX idx_entity_states_scope    (scope_id),
    INDEX idx_entity_states_entity   (entity_id),
    INDEX idx_entity_states_time     (timestamp),
    INDEX idx_entity_states_adapter  (adapter)
);

CREATE TABLE IF NOT EXISTS signals (
    id            CHAR(36)     NOT NULL PRIMARY KEY,
    scope_id      CHAR(36)     NOT NULL,
    series_id     CHAR(36)     NOT NULL,
    pattern       VARCHAR(64)  NOT NULL,
    detected_at   DATETIME(6)  NOT NULL,
    window_start  DATETIME(6)  NOT NULL,
    window_end    DATETIME(6)  NOT NULL,
    strength      DOUBLE       NOT NULL,
    confidence    DOUBLE       NOT NULL,
    metrics       JSON         NOT NULL,
    explanation   JSON         NOT NULL DEFAULT ('{}'),
    confidence_class VARCHAR(32) NOT NULL DEFAULT '',
    INDEX idx_signals_scope_time     (scope_id, detected_at),
    INDEX idx_signals_scope_pattern  (scope_id, pattern, detected_at),
    INDEX idx_signals_series         (series_id, detected_at),
    -- The scheduler's duplicate check, once per candidate signal per
    -- tick, forever. Its predicate is a signal's full perception
    -- identity, so the index covers all five columns and the lookup is
    -- a probe rather than a scan over everything ever detected for the
    -- series.
    INDEX idx_signals_identity       (scope_id, series_id, pattern, window_start, window_end)
);

-- Existing deployments created the table before explanation /
-- confidence_class landed. ADD COLUMN IF NOT EXISTS is a no-op when
-- CREATE TABLE already included them (fresh Open).
ALTER TABLE signals ADD COLUMN IF NOT EXISTS explanation JSON NOT NULL DEFAULT ('{}');
ALTER TABLE signals ADD COLUMN IF NOT EXISTS confidence_class VARCHAR(32) NOT NULL DEFAULT '';
ALTER TABLE signals ADD INDEX IF NOT EXISTS idx_signals_identity (scope_id, series_id, pattern, window_start, window_end);

CREATE TABLE IF NOT EXISTS signal_evidence (
    signal_id  CHAR(36)     NOT NULL,
    series_id  CHAR(36)     NOT NULL,
    time       DATETIME(6)  NOT NULL,
    kind       VARCHAR(64)  NOT NULL,
    score      DOUBLE       NOT NULL,
    metrics    JSON         NOT NULL,
    INDEX idx_signal_evidence (signal_id),
    CONSTRAINT fk_signal_evidence FOREIGN KEY (signal_id) REFERENCES signals(id) ON DELETE CASCADE
);
