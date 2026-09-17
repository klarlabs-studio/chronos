-- Schema migration 001: Chronos signal store (cognitive-stack alignment)
--
-- Two aggregates: time-series observations (entity_states) and detected
-- patterns (signals + signal_evidence). No insights, no feedback, no
-- dismissal: those concerns belong to agent runtimes and Mnemos in the broader
-- cognitive stack.

CREATE TABLE entity_states (
    id TEXT PRIMARY KEY,
    entity_id TEXT NOT NULL,
    scope_id TEXT NOT NULL,
    timestamp TEXT NOT NULL,           -- RFC3339Nano
    features TEXT NOT NULL,            -- JSON array of float64
    labels TEXT,                       -- JSON array of feature names
    meta TEXT,                         -- JSON object of adapter metadata
    adapter TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX idx_entity_states_scope ON entity_states(scope_id);
CREATE INDEX idx_entity_states_entity ON entity_states(entity_id);
CREATE INDEX idx_entity_states_time ON entity_states(timestamp);
CREATE INDEX idx_entity_states_adapter ON entity_states(adapter);

CREATE TABLE signals (
    id TEXT PRIMARY KEY,
    scope_id TEXT NOT NULL,
    series_id TEXT NOT NULL,           -- the entity the pattern was detected in
    pattern TEXT NOT NULL,             -- PatternType enum (recurrence, trend, ...)
    detected_at TEXT NOT NULL,
    window_start TEXT NOT NULL,
    window_end TEXT NOT NULL,
    strength REAL NOT NULL CHECK (strength >= 0 AND strength <= 1),
    confidence REAL NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
    metrics TEXT NOT NULL DEFAULT '{}', -- JSON map<string,float64>
    explanation TEXT NOT NULL DEFAULT '{}', -- JSON Explanation value object
    confidence_class TEXT NOT NULL DEFAULT ''  -- "" | tentative | established | strong
);

CREATE INDEX idx_signals_scope_time   ON signals(scope_id, detected_at DESC);
CREATE INDEX idx_signals_scope_pattern ON signals(scope_id, pattern, detected_at DESC);
CREATE INDEX idx_signals_series       ON signals(series_id, detected_at DESC);

-- The scheduler's duplicate check, once per candidate signal per tick,
-- forever. Its predicate is a signal's full perception identity, so the
-- index covers all five columns and the lookup is a probe rather than a
-- scan over everything ever detected for the series.
CREATE INDEX idx_signals_identity ON signals(scope_id, series_id, pattern, window_start, window_end);

CREATE TABLE signal_evidence (
    signal_id TEXT NOT NULL,
    series_id TEXT NOT NULL,
    time TEXT NOT NULL,
    kind TEXT NOT NULL,
    score REAL NOT NULL,
    metrics TEXT NOT NULL DEFAULT '{}',
    FOREIGN KEY (signal_id) REFERENCES signals(id) ON DELETE CASCADE
);

CREATE INDEX idx_signal_evidence ON signal_evidence(signal_id);

PRAGMA user_version = 1;
