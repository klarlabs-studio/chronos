package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/felixgeelhaar/chronos/internal/domain"
	"github.com/felixgeelhaar/chronos/internal/ports"
	"github.com/google/uuid"
)

// SignalRepository persists signals and their evidence in MySQL.
type SignalRepository struct{ conn *Conn }

// Save writes a signal and its evidence atomically.
func (r *SignalRepository) Save(ctx context.Context, sig domain.Signal) error {
	if err := sig.Validate(); err != nil {
		return err
	}
	tx, err := r.conn.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("signal save: begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	metricsJSON, _ := json.Marshal(sig.Metrics)
	if metricsJSON == nil {
		metricsJSON = []byte(`{}`)
	}
	explanationJSON, err := encodeExplanation(sig.Explanation)
	if err != nil {
		return fmt.Errorf("signal save: encode explanation: %w", err)
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO signals (id, scope_id, series_id, pattern, detected_at, window_start, window_end, strength, confidence, metrics, explanation, confidence_class)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			strength = VALUES(strength),
			confidence = VALUES(confidence),
			metrics = VALUES(metrics),
			explanation = VALUES(explanation),
			confidence_class = VALUES(confidence_class)
	`, sig.ID.String(), sig.ScopeID.String(), sig.Series.String(), string(sig.Pattern),
		sig.DetectedAt.UTC(), sig.Window.Start.UTC(), sig.Window.End.UTC(),
		sig.Strength, sig.Confidence, string(metricsJSON), explanationJSON,
		string(sig.ConfidenceClass),
	)
	if err != nil {
		return fmt.Errorf("signal save: insert: %w", err)
	}

	// Replace evidence for idempotency under upsert.
	if _, err := tx.ExecContext(ctx, `DELETE FROM signal_evidence WHERE signal_id = ?`, sig.ID.String()); err != nil {
		return fmt.Errorf("signal save: clear evidence: %w", err)
	}
	for _, e := range sig.Evidence {
		evMetrics, _ := json.Marshal(e.Metrics)
		if evMetrics == nil {
			evMetrics = []byte(`{}`)
		}
		_, err := tx.ExecContext(ctx, `
			INSERT INTO signal_evidence (signal_id, series_id, time, kind, score, metrics)
			VALUES (?, ?, ?, ?, ?, ?)
		`, sig.ID.String(), e.Series.String(), e.Time.UTC(), e.Kind, e.Score, string(evMetrics))
		if err != nil {
			return fmt.Errorf("signal save: insert evidence: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("signal save: commit: %w", err)
	}
	return nil
}

// List returns signals matching filter, ordered detected-at desc then
// confidence desc.
func (r *SignalRepository) List(ctx context.Context, filter ports.SignalFilter) ([]domain.Signal, error) {
	query, args := buildListQuery(filter)
	rows, err := r.conn.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("signal list: %w", err)
	}
	signals, err := scanSignals(rows)
	if err != nil {
		return nil, err
	}
	for i := range signals {
		ev, err := r.loadEvidence(ctx, signals[i].ID)
		if err != nil {
			return nil, err
		}
		signals[i].Evidence = ev
	}
	return signals, nil
}

// Get returns a single signal by ID, including its evidence.
func (r *SignalRepository) Get(ctx context.Context, id uuid.UUID) (domain.Signal, error) {
	row := r.conn.DB.QueryRowContext(ctx, `
		SELECT id, scope_id, series_id, pattern, detected_at, window_start, window_end, strength, confidence, metrics, explanation, confidence_class
		FROM signals WHERE id = ?
	`, id.String())
	sig, err := scanOneSignal(row)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Signal{}, domain.ErrSignalNotFound
	}
	if err != nil {
		return domain.Signal{}, fmt.Errorf("signal get: %w", err)
	}
	ev, err := r.loadEvidence(ctx, id)
	if err != nil {
		return domain.Signal{}, err
	}
	sig.Evidence = ev
	return sig, nil
}

// Count returns the number of signals matching filter.
func (r *SignalRepository) Count(ctx context.Context, filter ports.SignalFilter) (int64, error) {
	query, args := buildCountQuery(filter)
	var n int64
	if err := r.conn.DB.QueryRowContext(ctx, query, args...).Scan(&n); err != nil {
		return 0, fmt.Errorf("signal count: %w", err)
	}
	return n, nil
}

// DeleteSignalsOlderThan removes signals detected before cutoff.
// signal_evidence goes with them via ON DELETE CASCADE.
func (r *SignalRepository) DeleteSignalsOlderThan(ctx context.Context, cutoff time.Time) (int64, error) {
	res, err := r.conn.DB.ExecContext(ctx,
		`DELETE FROM signals WHERE detected_at < ?`, cutoff.UTC())
	if err != nil {
		return 0, fmt.Errorf("signal retention: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("signal retention: rows affected: %w", err)
	}
	return n, nil
}

func (r *SignalRepository) loadEvidence(ctx context.Context, id uuid.UUID) ([]domain.Evidence, error) {
	rows, err := r.conn.DB.QueryContext(ctx, `
		SELECT series_id, time, kind, score, metrics
		FROM signal_evidence
		WHERE signal_id = ?
		ORDER BY score DESC
	`, id.String())
	if err != nil {
		return nil, fmt.Errorf("signal evidence: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []domain.Evidence
	for rows.Next() {
		var e domain.Evidence
		var seriesStr string
		var metrics []byte
		if err := rows.Scan(&seriesStr, &e.Time, &e.Kind, &e.Score, &metrics); err != nil {
			return nil, fmt.Errorf("scan evidence: %w", err)
		}
		series, err := uuid.Parse(seriesStr)
		if err != nil {
			return nil, fmt.Errorf("parse evidence series_id: %w", err)
		}
		e.Series = series
		if len(metrics) > 0 {
			_ = json.Unmarshal(metrics, &e.Metrics)
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func buildListQuery(f ports.SignalFilter) (string, []any) {
	const base = `SELECT id, scope_id, series_id, pattern, detected_at, window_start, window_end, strength, confidence, metrics, explanation, confidence_class FROM signals`
	where, args := buildWhere(f)
	q := base + where + " ORDER BY detected_at DESC, confidence DESC"
	if f.Limit > 0 {
		q += fmt.Sprintf(" LIMIT %d", f.Limit)
	}
	return q, args
}

func buildCountQuery(f ports.SignalFilter) (string, []any) {
	const base = `SELECT COUNT(*) FROM signals`
	where, args := buildWhere(f)
	return base + where, args
}

// buildWhere uses the `?` placeholder shared by all MySQL-compatible
// drivers. The Postgres provider's $N placeholder isn't portable here.
func buildWhere(f ports.SignalFilter) (string, []any) {
	var clauses []string
	var args []any
	add := func(predicate string, val any) {
		args = append(args, val)
		clauses = append(clauses, fmt.Sprintf("%s ?", predicate))
	}
	if f.ScopeID != uuid.Nil {
		add("scope_id =", f.ScopeID.String())
	}
	if len(f.ScopeIDs) > 0 {
		placeholders := make([]string, len(f.ScopeIDs))
		for i, id := range f.ScopeIDs {
			args = append(args, id.String())
			placeholders[i] = "?"
		}
		clauses = append(clauses, "scope_id IN ("+strings.Join(placeholders, ",")+")")
	}
	if f.Series != nil {
		add("series_id =", f.Series.String())
	}
	if f.Pattern != nil {
		add("pattern =", string(*f.Pattern))
	}
	if f.Since != nil {
		add("detected_at >=", f.Since.UTC())
	}
	if f.Until != nil {
		add("detected_at <", f.Until.UTC())
	}
	if f.MinConfidence != nil {
		add("confidence >=", *f.MinConfidence)
	}
	if f.Window != nil {
		add("window_start =", f.Window.Start.UTC())
		add("window_end =", f.Window.End.UTC())
	}
	if len(clauses) == 0 {
		return "", args
	}
	return " WHERE " + strings.Join(clauses, " AND "), args
}

func scanSignals(rows *sql.Rows) ([]domain.Signal, error) {
	defer func() { _ = rows.Close() }()
	var out []domain.Signal
	for rows.Next() {
		sig, err := scanSignalRow(rows.Scan)
		if err != nil {
			return nil, err
		}
		out = append(out, sig)
	}
	return out, rows.Err()
}

func scanOneSignal(row *sql.Row) (domain.Signal, error) {
	return scanSignalRow(row.Scan)
}

func scanSignalRow(scan func(...any) error) (domain.Signal, error) {
	var (
		sig                        domain.Signal
		idStr, scopeStr, seriesStr string
		patternStr                 string
		metricsJSON                []byte
		explanationJSON            []byte
		confidenceClassStr         string
	)
	if err := scan(
		&idStr, &scopeStr, &seriesStr, &patternStr,
		&sig.DetectedAt, &sig.Window.Start, &sig.Window.End,
		&sig.Strength, &sig.Confidence, &metricsJSON,
		&explanationJSON, &confidenceClassStr,
	); err != nil {
		return domain.Signal{}, err
	}
	var err error
	if sig.ID, err = uuid.Parse(idStr); err != nil {
		return domain.Signal{}, fmt.Errorf("parse signal id: %w", err)
	}
	if sig.ScopeID, err = uuid.Parse(scopeStr); err != nil {
		return domain.Signal{}, fmt.Errorf("parse signal scope_id: %w", err)
	}
	if sig.Series, err = uuid.Parse(seriesStr); err != nil {
		return domain.Signal{}, fmt.Errorf("parse signal series_id: %w", err)
	}
	sig.Pattern = domain.PatternType(patternStr)
	sig.ConfidenceClass = domain.ConfidenceClass(confidenceClassStr)
	if len(metricsJSON) > 0 {
		_ = json.Unmarshal(metricsJSON, &sig.Metrics)
	}
	expl, err := decodeExplanation(string(explanationJSON))
	if err != nil {
		return domain.Signal{}, fmt.Errorf("decode signal explanation: %w", err)
	}
	sig.Explanation = expl
	return sig, nil
}

// explanationWire matches the sqlite/postgres JSON shape so a signal
// saved on one backend can be loaded on another without re-encoding.
type explanationWire struct {
	FeatureEvolution   []featureSampleWire `json:"feature_evolution,omitempty"`
	ComparablePeers    int                 `json:"comparable_peers,omitempty"`
	BaselineWindowDays int                 `json:"baseline_window_days,omitempty"`
	ThresholdUsed      float64             `json:"threshold_used,omitempty"`
	DetectorVersion    string              `json:"detector_version,omitempty"`
}

type featureSampleWire struct {
	At    string  `json:"at"`
	Value float64 `json:"value"`
}

func encodeExplanation(e domain.Explanation) (string, error) {
	if e.IsZero() {
		return "{}", nil
	}
	w := explanationWire{
		ComparablePeers:    e.ComparablePeers,
		BaselineWindowDays: e.BaselineWindowDays,
		ThresholdUsed:      e.ThresholdUsed,
		DetectorVersion:    e.DetectorVersion,
	}
	for _, fs := range e.FeatureEvolution {
		w.FeatureEvolution = append(w.FeatureEvolution, featureSampleWire{
			At:    fs.At.UTC().Format("2006-01-02T15:04:05.999999999Z07:00"),
			Value: fs.Value,
		})
	}
	b, err := json.Marshal(w)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func decodeExplanation(raw string) (domain.Explanation, error) {
	if raw == "" || raw == "{}" {
		return domain.Explanation{}, nil
	}
	var w explanationWire
	if err := json.Unmarshal([]byte(raw), &w); err != nil {
		return domain.Explanation{}, err
	}
	out := domain.Explanation{
		ComparablePeers:    w.ComparablePeers,
		BaselineWindowDays: w.BaselineWindowDays,
		ThresholdUsed:      w.ThresholdUsed,
		DetectorVersion:    w.DetectorVersion,
	}
	for _, fs := range w.FeatureEvolution {
		t, err := time.Parse("2006-01-02T15:04:05.999999999Z07:00", fs.At)
		if err != nil {
			return domain.Explanation{}, fmt.Errorf("decode feature_evolution time: %w", err)
		}
		out.FeatureEvolution = append(out.FeatureEvolution, domain.FeatureSample{
			At: t, Value: fs.Value,
		})
	}
	return out, nil
}
