package sqlite

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
	"github.com/felixgeelhaar/chronos/internal/store/sqlite/sqlcgen"
	"github.com/google/uuid"
)

// SignalRepository persists signals and their evidence in SQLite.
//
// Filtering uses dynamic SQL because sqlc cannot express optional
// predicates compactly. Inserts go through the sqlc-generated code so
// the same query pipeline carries them as the rest of the schema.
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
	q := r.conn.q.WithTx(tx)

	metricsJSON, _ := json.Marshal(sig.Metrics)
	explanationJSON, err := encodeExplanation(sig.Explanation)
	if err != nil {
		return fmt.Errorf("signal save: encode explanation: %w", err)
	}
	if err := q.InsertSignal(ctx, sqlcgen.InsertSignalParams{
		ID:              sig.ID.String(),
		ScopeID:         sig.ScopeID.String(),
		SeriesID:        sig.Series.String(),
		Pattern:         string(sig.Pattern),
		DetectedAt:      formatTime(sig.DetectedAt),
		WindowStart:     formatTime(sig.Window.Start),
		WindowEnd:       formatTime(sig.Window.End),
		Strength:        sig.Strength,
		Confidence:      sig.Confidence,
		Metrics:         string(metricsJSON),
		Explanation:     explanationJSON,
		ConfidenceClass: string(sig.ConfidenceClass),
	}); err != nil {
		return fmt.Errorf("signal save: insert: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM signal_evidence WHERE signal_id = ?`, sig.ID.String()); err != nil {
		return fmt.Errorf("signal save: clear evidence: %w", err)
	}
	for _, e := range sig.Evidence {
		evMetrics, _ := json.Marshal(e.Metrics)
		if err := q.InsertSignalEvidence(ctx, sqlcgen.InsertSignalEvidenceParams{
			SignalID: sig.ID.String(),
			SeriesID: e.Series.String(),
			Time:     formatTime(e.Time),
			Kind:     e.Kind,
			Score:    e.Score,
			Metrics:  string(evMetrics),
		}); err != nil {
			return fmt.Errorf("signal save: insert evidence: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("signal save: commit: %w", err)
	}
	return nil
}

// List returns signals matching the filter, ordered detected-at desc
// then confidence desc.
func (r *SignalRepository) List(ctx context.Context, filter ports.SignalFilter) ([]domain.Signal, error) {
	query, args := buildListQuery(filter)
	rows, err := r.conn.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("signal list: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []domain.Signal
	for rows.Next() {
		sig, err := scanSignal(rows.Scan)
		if err != nil {
			return nil, err
		}
		out = append(out, sig)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("signal list: %w", err)
	}
	for i := range out {
		ev, err := r.loadEvidence(ctx, out[i].ID)
		if err != nil {
			return nil, err
		}
		out[i].Evidence = ev
	}
	return out, nil
}

// Get returns a single signal by ID, including its evidence.
func (r *SignalRepository) Get(ctx context.Context, id uuid.UUID) (domain.Signal, error) {
	row, err := r.conn.q.GetSignalByID(ctx, id.String())
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Signal{}, domain.ErrSignalNotFound
	}
	if err != nil {
		return domain.Signal{}, fmt.Errorf("signal get: %w", err)
	}
	sig, err := decodeSignal(row)
	if err != nil {
		return domain.Signal{}, err
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
		`DELETE FROM signals WHERE detected_at < ?`, formatTime(cutoff))
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
	rows, err := r.conn.q.GetSignalEvidence(ctx, id.String())
	if err != nil {
		return nil, fmt.Errorf("signal evidence: %w", err)
	}
	out := make([]domain.Evidence, 0, len(rows))
	for _, row := range rows {
		series, err := uuid.Parse(row.SeriesID)
		if err != nil {
			return nil, fmt.Errorf("decode evidence series: %w", err)
		}
		t, err := parseTime(row.Time)
		if err != nil {
			return nil, fmt.Errorf("decode evidence time: %w", err)
		}
		var metrics map[string]float64
		if row.Metrics != "" {
			_ = json.Unmarshal([]byte(row.Metrics), &metrics)
		}
		out = append(out, domain.Evidence{
			Series:  series,
			Time:    t,
			Kind:    row.Kind,
			Score:   row.Score,
			Metrics: metrics,
		})
	}
	return out, nil
}

// buildListQuery composes the dynamic SELECT for SignalFilter. Order is
// always (detected_at DESC, confidence DESC) so callers get newest +
// strongest first; Limit is applied as the SQL LIMIT clause.
func buildListQuery(f ports.SignalFilter) (string, []any) {
	const base = `SELECT id, scope_id, series_id, pattern, detected_at, window_start, window_end, strength, confidence, metrics FROM signals`
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

func buildWhere(f ports.SignalFilter) (string, []any) {
	var clauses []string
	var args []any
	if f.ScopeID != uuid.Nil {
		clauses = append(clauses, "scope_id = ?")
		args = append(args, f.ScopeID.String())
	}
	if len(f.ScopeIDs) > 0 {
		placeholders := make([]string, len(f.ScopeIDs))
		for i, id := range f.ScopeIDs {
			placeholders[i] = "?"
			args = append(args, id.String())
		}
		clauses = append(clauses, "scope_id IN ("+strings.Join(placeholders, ",")+")")
	}
	if f.Series != nil {
		clauses = append(clauses, "series_id = ?")
		args = append(args, f.Series.String())
	}
	if f.Pattern != nil {
		clauses = append(clauses, "pattern = ?")
		args = append(args, string(*f.Pattern))
	}
	if f.Since != nil {
		clauses = append(clauses, "detected_at >= ?")
		args = append(args, formatTime(*f.Since))
	}
	if f.Until != nil {
		clauses = append(clauses, "detected_at < ?")
		args = append(args, formatTime(*f.Until))
	}
	if f.MinConfidence != nil {
		clauses = append(clauses, "confidence >= ?")
		args = append(args, *f.MinConfidence)
	}
	if f.Window != nil {
		clauses = append(clauses, "window_start = ?", "window_end = ?")
		args = append(args, formatTime(f.Window.Start), formatTime(f.Window.End))
	}
	if len(clauses) == 0 {
		return "", args
	}
	return " WHERE " + strings.Join(clauses, " AND "), args
}

func scanSignal(scan func(...any) error) (domain.Signal, error) {
	var row sqlcgen.Signal
	if err := scan(
		&row.ID, &row.ScopeID, &row.SeriesID, &row.Pattern, &row.DetectedAt,
		&row.WindowStart, &row.WindowEnd, &row.Strength, &row.Confidence, &row.Metrics,
	); err != nil {
		return domain.Signal{}, fmt.Errorf("scan signal: %w", err)
	}
	return decodeSignal(row)
}

func decodeSignal(row sqlcgen.Signal) (domain.Signal, error) {
	id, err := uuid.Parse(row.ID)
	if err != nil {
		return domain.Signal{}, fmt.Errorf("decode signal id: %w", err)
	}
	scope, err := uuid.Parse(row.ScopeID)
	if err != nil {
		return domain.Signal{}, fmt.Errorf("decode signal scope: %w", err)
	}
	series, err := uuid.Parse(row.SeriesID)
	if err != nil {
		return domain.Signal{}, fmt.Errorf("decode signal series: %w", err)
	}
	det, err := parseTime(row.DetectedAt)
	if err != nil {
		return domain.Signal{}, fmt.Errorf("decode signal detected_at: %w", err)
	}
	wStart, err := parseTime(row.WindowStart)
	if err != nil {
		return domain.Signal{}, fmt.Errorf("decode window start: %w", err)
	}
	wEnd, err := parseTime(row.WindowEnd)
	if err != nil {
		return domain.Signal{}, fmt.Errorf("decode window end: %w", err)
	}
	var metrics map[string]float64
	if row.Metrics != "" {
		_ = json.Unmarshal([]byte(row.Metrics), &metrics)
	}
	expl, err := decodeExplanation(row.Explanation)
	if err != nil {
		return domain.Signal{}, fmt.Errorf("decode signal explanation: %w", err)
	}
	return domain.Signal{
		ID:              id,
		ScopeID:         scope,
		Series:          series,
		Pattern:         domain.PatternType(row.Pattern),
		DetectedAt:      det,
		Window:          domain.TimeWindow{Start: wStart, End: wEnd},
		Strength:        row.Strength,
		Confidence:      row.Confidence,
		Metrics:         metrics,
		Explanation:     expl,
		ConfidenceClass: domain.ConfidenceClass(row.ConfidenceClass),
	}, nil
}

// explanationWire is the JSONB shape persisted in the explanation
// column. Domain time.Time → RFC3339Nano string for stable storage.
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

// encodeExplanation serialises an Explanation to its JSONB column
// representation. Zero value becomes the literal "{}" string so the
// column's NOT NULL constraint holds without forcing every signal to
// carry an explanation.
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
			At:    formatTime(fs.At),
			Value: fs.Value,
		})
	}
	b, err := json.Marshal(w)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// decodeExplanation parses the JSONB column back into a domain
// Explanation. Empty / "{}" strings yield the zero value.
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
		t, err := parseTime(fs.At)
		if err != nil {
			return domain.Explanation{}, fmt.Errorf("decode feature_evolution time: %w", err)
		}
		out.FeatureEvolution = append(out.FeatureEvolution, domain.FeatureSample{
			At: t, Value: fs.Value,
		})
	}
	return out, nil
}
