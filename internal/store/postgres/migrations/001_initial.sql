CREATE TABLE IF NOT EXISTS entity_states (
    id UUID PRIMARY KEY,
    entity_id UUID NOT NULL,
    scope_id UUID NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    features JSONB NOT NULL,
    labels JSONB,
    meta JSONB,
    adapter TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_entity_states_scope   ON entity_states(scope_id);
CREATE INDEX IF NOT EXISTS idx_entity_states_entity  ON entity_states(entity_id);
CREATE INDEX IF NOT EXISTS idx_entity_states_time    ON entity_states(timestamp);
CREATE INDEX IF NOT EXISTS idx_entity_states_adapter ON entity_states(adapter);

CREATE TABLE IF NOT EXISTS signals (
    id UUID PRIMARY KEY,
    scope_id UUID NOT NULL,
    series_id UUID NOT NULL,
    pattern TEXT NOT NULL,
    detected_at TIMESTAMPTZ NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    strength DOUBLE PRECISION NOT NULL CHECK (strength >= 0 AND strength <= 1),
    confidence DOUBLE PRECISION NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
    metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
    explanation JSONB NOT NULL DEFAULT '{}'::jsonb,
    confidence_class TEXT NOT NULL DEFAULT ''
);

-- Defensive ALTERs for existing deployments where signals predated
-- a newer column. Safe to run on fresh installs (no-op via IF NOT EXISTS).
ALTER TABLE signals ADD COLUMN IF NOT EXISTS explanation JSONB NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE signals ADD COLUMN IF NOT EXISTS confidence_class TEXT NOT NULL DEFAULT '';
CREATE INDEX IF NOT EXISTS idx_signals_scope_time    ON signals(scope_id, detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_signals_scope_pattern ON signals(scope_id, pattern, detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_signals_series        ON signals(series_id, detected_at DESC);

-- The scheduler's duplicate check, once per candidate signal per tick,
-- forever. Its predicate is a signal's full perception identity, so the
-- index covers all five columns and the lookup is a probe rather than a
-- scan over everything ever detected for the series.
CREATE INDEX IF NOT EXISTS idx_signals_identity       ON signals(scope_id, series_id, pattern, window_start, window_end);

-- Retention deletes across every scope at once, so the leading
-- scope_id of idx_signals_scope_time puts that index out of reach and
-- the sweep would degrade into a full scan of the table it exists to
-- keep small.
CREATE INDEX IF NOT EXISTS idx_signals_detected_at    ON signals(detected_at);

CREATE TABLE IF NOT EXISTS signal_evidence (
    signal_id UUID NOT NULL REFERENCES signals(id) ON DELETE CASCADE,
    series_id UUID NOT NULL,
    time TIMESTAMPTZ NOT NULL,
    kind TEXT NOT NULL,
    score DOUBLE PRECISION NOT NULL,
    metrics JSONB NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_signal_evidence ON signal_evidence(signal_id);
