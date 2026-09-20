// Package domain holds the engine's private domain model.
//
// Chronos's role in the cognitive stack (Mnemos / Chronos / agent runtimes)
// is *Time / Pattern Perception*: it ingests time-series observations and
// emits structured Signals describing patterns. It does NOT interpret what
// signals mean, decide actions, or store reviewer feedback — those are
// agent-runtime and Mnemos responsibilities respectively.
//
// As a result, this package contains only the perceptual primitives:
// Signal, Evidence, PatternType, TimeWindow. There is no Title, no
// Suggestion, no DismissedAt — Chronos is signals, not opinions.
package domain

import (
	"errors"
	"math"
	"time"

	"github.com/google/uuid"
)

// Sentinel errors returned by validation on domain types.
var (
	ErrMissingScopeID     = errors.New("domain: signal missing scope ID")
	ErrMissingSeriesID    = errors.New("domain: signal missing series ID")
	ErrMissingPattern     = errors.New("domain: signal missing pattern type")
	ErrInvalidConfidence  = errors.New("domain: confidence must be in [0,1]")
	ErrInvalidStrength    = errors.New("domain: strength must be in [0,1]")
	ErrInvalidWindow      = errors.New("domain: window end must not precede window start")
	ErrSignalNotFound     = errors.New("domain: signal not found")
	ErrInvalidExplanation = errors.New("domain: explanation has invalid component (negative count or non-monotonic feature evolution)")

	// ErrNonFiniteMetric rejects NaN, +Inf and -Inf anywhere in the
	// numeric payload a signal carries: Signal.Metrics, each
	// Evidence's Score and Metrics, and the Explanation's
	// ThresholdUsed and feature-evolution values.
	//
	// The loss this prevents is silent and total. Every SQL store
	// persists the metric bags as a JSON column, and encoding/json
	// refuses non-finite floats: json.Marshal of
	// map[string]float64{"mean": math.Inf(1), "n": 12} returns zero
	// bytes and "json: unsupported value: +Inf". The stores discarded
	// that error, so a single unrepresentable key did not cost one
	// key — it wrote the whole map as an empty column, and the signal
	// read back carrying no metrics at all rather than a metric
	// flagged as bad. Nothing in the pipeline could tell that apart
	// from a detector that surfaced no metrics.
	//
	// Rejecting here keeps the loss from arising. A detector whose
	// arithmetic overflowed has not measured anything, and "no
	// signal" is a valid result: a signal whose metrics cannot be
	// represented is not a signal worth emitting.
	ErrNonFiniteMetric = errors.New("domain: signal has a non-finite metric value (NaN or Inf)")
)

// PatternType is a typed enum for the kinds of patterns Chronos can
// detect. Each detector emits Signals tagged with one PatternType so
// downstream consumers can filter and route by perception kind
// without parsing free text.
type PatternType string

const (
	// PatternTypeRecurrence — the subject is in a state that other
	// entities have been in before. Detected via cross-entity similarity
	// over historical states.
	PatternTypeRecurrence PatternType = "recurrence"

	// PatternTypeTrend — the outcome metric exhibits a sustained
	// directional movement over the analysis window.
	PatternTypeTrend PatternType = "trend"

	// PatternTypeSpike — a sharp positive deviation from the recent
	// baseline.
	PatternTypeSpike PatternType = "spike"

	// PatternTypeDrop — a sharp negative deviation from the recent
	// baseline.
	PatternTypeDrop PatternType = "drop"

	// PatternTypeStall — the outcome metric shows little to no variance
	// over the analysis window.
	PatternTypeStall PatternType = "stall"

	// PatternTypeAnomaly — a generic out-of-distribution observation;
	// reserved for future detectors that do not fit Spike/Drop/Stall.
	PatternTypeAnomaly PatternType = "anomaly"

	// PatternTypeSeasonality — a periodic structure detected over a
	// long-enough window via autocorrelation peaks.
	PatternTypeSeasonality PatternType = "seasonality"

	// PatternTypeCorrelation — two or more series in the same scope
	// move together (pairwise Pearson).
	PatternTypeCorrelation PatternType = "correlation"

	// PatternTypeChangePoint — the series exhibits a step change: a
	// sustained shift in mean that does not look like Spike/Drop (which
	// are short-lived deviations). Detected via best-split mean-shift
	// test over the analysis window.
	PatternTypeChangePoint PatternType = "change_point"

	// PatternTypeOutlierCluster — multiple series in the same scope go
	// anomalous around the same time. Distinct from per-series Anomaly:
	// this is the cohort-level signal that something systemic is going
	// on (a deploy gone wrong, an upstream dependency incident, etc.).
	PatternTypeOutlierCluster PatternType = "outlier_cluster"

	// PatternTypeCrossScopeCorrelation — two series in DIFFERENT scopes
	// move together. The within-scope correlation detector misses these
	// because it groups by scope; this detector looks across scopes.
	PatternTypeCrossScopeCorrelation PatternType = "cross_scope_correlation"

	// PatternTypeOscillation — the outcome repeatedly reverses direction
	// (high sign-flip rate among successive differences). Distinct from
	// Seasonality, which requires a periodic autocorrelation peak.
	PatternTypeOscillation PatternType = "oscillation"

	// PatternTypeDivergence — two series in the same scope whose
	// absolute outcome gap is growing over the aligned window.
	PatternTypeDivergence PatternType = "divergence"

	// PatternTypeConvergence — two series in the same scope whose
	// absolute outcome gap is shrinking over the aligned window.
	PatternTypeConvergence PatternType = "convergence"
)

// TimeWindow describes the analysis window over which a signal was
// detected. End >= Start is enforced by Validate.
type TimeWindow struct {
	Start time.Time
	End   time.Time
}

// Validate enforces that End is not before Start.
func (w TimeWindow) Validate() error {
	if w.End.Before(w.Start) {
		return ErrInvalidWindow
	}
	return nil
}

// Evidence is one piece of supporting data backing a signal. The Kind tag
// disambiguates evidence shapes across detector types: "similar_state"
// for Recurrence, "baseline_deviation" for Spike/Drop, etc.
type Evidence struct {
	// Series is the entity this evidence comes from. For Recurrence this
	// is a peer entity; for single-series detectors (Trend/Spike/...) it
	// is the same as the Signal's Series.
	Series uuid.UUID
	// Time is when the evidence was observed.
	Time time.Time
	// Kind disambiguates the structural shape of this evidence.
	Kind string
	// Score is a unit-free numeric measurement whose meaning depends on
	// Kind (similarity for "similar_state"; z-score for
	// "baseline_deviation"; …).
	Score float64
	// Metrics carries detector-specific numeric measurements that are
	// useful at the API boundary but not generic enough to deserve named
	// fields. Keys should be lowercase snake_case.
	Metrics map[string]float64
}

// Validate enforces that the evidence's numeric payload is
// representable. Score and every Metrics value must be finite; see
// [ErrNonFiniteMetric] for what a non-finite one costs on the way to
// storage.
func (e Evidence) Validate() error {
	if !isFinite(e.Score) {
		return ErrNonFiniteMetric
	}
	return validateMetricBag(e.Metrics)
}

// Signal is a structured description of a detected pattern. It is
// presentation-neutral: no Title, no Summary, no Suggestion. Downstream
// consumers interpret Signals into actions; Chronos only perceives.
type Signal struct {
	ID         uuid.UUID
	ScopeID    uuid.UUID
	Series     uuid.UUID // the entity ("series") the pattern was detected in
	Pattern    PatternType
	DetectedAt time.Time
	Window     TimeWindow

	// Strength describes the intensity of the pattern as observed
	// (e.g. average similarity, regression slope normalised to a
	// reference scale, peak-to-baseline ratio). Range: [0, 1].
	Strength float64

	// Confidence describes how sure the detector is that the pattern is
	// real given the evidence available (e.g. similarity * sample
	// factor, p-value transform, ...). Range: [0, 1].
	Confidence float64

	// Evidence is the list of underlying observations supporting the
	// signal. May be empty if the detector does not surface line-level
	// evidence.
	Evidence []Evidence

	// Metrics is a free-form bag of detector-specific measurements (e.g.
	// "avg_similarity", "slope", "z_score"). Keys should be lowercase
	// snake_case so downstream filters can reason about them. Values
	// must be finite — see [ErrNonFiniteMetric].
	Metrics map[string]float64

	// Explanation carries detector-side context for narration: the
	// feature evolution that fired the pattern, the comparable-peer
	// count, the baseline window used, the threshold applied, and the
	// detector's own version tag. Zero value (default) means the
	// detector did not surface an explanation; consumers handle absent.
	Explanation Explanation

	// ConfidenceClass is the qualitative bucket a downstream narrator
	// uses to phrase the signal: a pattern just over the MIN_POINTS
	// floor speaks softly ("a possible trend"), one with 5× the floor
	// speaks plainly ("a clear trend"). Empty string means the
	// detector did not classify — consumers treat absent as "no claim
	// about strength". Stable values are defined as constants below.
	ConfidenceClass ConfidenceClass
}

// ConfidenceClass is a coarse qualitative grade derived from sample
// size relative to the detector's MIN_POINTS floor. Three buckets,
// chosen so a narration consumer can always say "a possible X",
// "a X", or "a clear X" without inventing extra adjectives.
type ConfidenceClass string

const (
	// ConfidenceClassTentative — sample size at or just above the
	// MIN_POINTS floor. Narration should hedge.
	ConfidenceClassTentative ConfidenceClass = "tentative"
	// ConfidenceClassEstablished — sample size ≥ established
	// multiplier × MIN_POINTS. Narration can state the pattern
	// without hedging.
	ConfidenceClassEstablished ConfidenceClass = "established"
	// ConfidenceClassStrong — sample size ≥ strong multiplier ×
	// MIN_POINTS. Narration can speak with emphasis ("a clear X").
	ConfidenceClassStrong ConfidenceClass = "strong"
)

// FeatureSample is one observation in an Explanation's feature
// evolution series.
type FeatureSample struct {
	At    time.Time
	Value float64
}

// Explanation is the value object that detectors attach to a Signal so
// downstream consumers can narrate WHY the pattern fired without
// re-deriving the supporting data. All fields are optional; the zero
// value is the explicit "no explanation surfaced" state.
type Explanation struct {
	// FeatureEvolution is the time-ordered series the detector looked
	// at (monotonic non-decreasing timestamps). Empty when the
	// detector does not surface line-level evolution.
	FeatureEvolution []FeatureSample

	// ComparablePeers is the number of comparable entities the
	// detector considered when ranking this signal. 0 means
	// "not applicable" for detectors that don't operate over peers.
	ComparablePeers int

	// BaselineWindowDays is the rolling baseline window (in days) the
	// detector applied. 0 means "not applicable".
	BaselineWindowDays int

	// ThresholdUsed is the threshold value the detector compared
	// against (e.g. z-score cutoff, slope minimum). 0 means
	// "not applicable".
	ThresholdUsed float64

	// DetectorVersion is the detector's own version tag. Useful for
	// drift analysis when detector logic evolves.
	DetectorVersion string
}

// IsZero reports whether the Explanation carries no information.
// Persistence layers use this to decide whether to store the column.
func (e Explanation) IsZero() bool {
	return len(e.FeatureEvolution) == 0 &&
		e.ComparablePeers == 0 &&
		e.BaselineWindowDays == 0 &&
		e.ThresholdUsed == 0 &&
		e.DetectorVersion == ""
}

// Validate enforces invariants on the explanation value object.
// Zero value is always valid (means "no explanation surfaced").
func (e Explanation) Validate() error {
	if e.ComparablePeers < 0 {
		return ErrInvalidExplanation
	}
	if e.BaselineWindowDays < 0 {
		return ErrInvalidExplanation
	}
	if !isFinite(e.ThresholdUsed) {
		return ErrNonFiniteMetric
	}
	for i := range e.FeatureEvolution {
		if !isFinite(e.FeatureEvolution[i].Value) {
			return ErrNonFiniteMetric
		}
	}
	for i := 1; i < len(e.FeatureEvolution); i++ {
		if e.FeatureEvolution[i].At.Before(e.FeatureEvolution[i-1].At) {
			return ErrInvalidExplanation
		}
	}
	return nil
}

// Validate enforces invariants on a Signal. Detectors call Validate before
// returning, and repositories call Validate before persisting.
func (s Signal) Validate() error {
	if s.ScopeID == uuid.Nil {
		return ErrMissingScopeID
	}
	// OutlierCluster is a cohort-level signal: Series is uuid.Nil by
	// contract (see docs/wire-contract.md). Every other pattern is
	// entity-level and must name a series.
	if s.Series == uuid.Nil && s.Pattern != PatternTypeOutlierCluster {
		return ErrMissingSeriesID
	}
	if s.Pattern == "" {
		return ErrMissingPattern
	}
	// !isFinite first, and not folded into the range test: NaN < 0 and
	// NaN > 1 are both false, so a range check on its own reports a NaN
	// strength as in [0,1].
	if !isFinite(s.Strength) || s.Strength < 0 || s.Strength > 1 {
		return ErrInvalidStrength
	}
	if !isFinite(s.Confidence) || s.Confidence < 0 || s.Confidence > 1 {
		return ErrInvalidConfidence
	}
	if err := s.Window.Validate(); err != nil {
		return err
	}
	if err := validateMetricBag(s.Metrics); err != nil {
		return err
	}
	for _, ev := range s.Evidence {
		if err := ev.Validate(); err != nil {
			return err
		}
	}
	return s.Explanation.Validate()
}

// validateMetricBag rejects a metric map holding a value encoding/json
// cannot represent. Map iteration order decides which key is found
// first; the sentinel carries no key, so the result is deterministic
// either way.
func validateMetricBag(m map[string]float64) error {
	for _, v := range m {
		if !isFinite(v) {
			return ErrNonFiniteMetric
		}
	}
	return nil
}

// isFinite reports whether x is a real number — neither NaN nor an
// infinity.
func isFinite(x float64) bool {
	return !math.IsNaN(x) && !math.IsInf(x, 0)
}
