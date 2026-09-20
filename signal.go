package chronos

import "github.com/felixgeelhaar/chronos/internal/domain"

// Public type aliases re-exported from internal/domain. Field additions
// are backward compatible; field removals constitute a major bump.
//
// See [domain.Signal] for the authoritative definition.
type (
	// Signal is an inference the detection engine emits when it sees a
	// pattern in a series of entity states (spike, drop, trend, etc.).
	Signal = domain.Signal

	// PatternType names the kind of pattern a Signal represents.
	PatternType = domain.PatternType

	// TimeWindow bounds the period of observation a Signal references.
	TimeWindow = domain.TimeWindow

	// Evidence is the per-feature observation slice supporting a Signal.
	Evidence = domain.Evidence

	// FeatureSample is one numeric observation within a TimeWindow,
	// carried inside Evidence for explainability.
	FeatureSample = domain.FeatureSample

	// Explanation carries the detector's per-Signal reasoning artefacts
	// (feature evolution, baseline references, contributing factors).
	Explanation = domain.Explanation

	// ConfidenceClass classifies a Signal's confidence into tentative /
	// established / strong tiers for downstream filters.
	ConfidenceClass = domain.ConfidenceClass
)

// PatternType constants re-exported for callers that want to filter
// queries by pattern without importing internal/domain.
const (
	PatternTypeRecurrence            = domain.PatternTypeRecurrence
	PatternTypeTrend                 = domain.PatternTypeTrend
	PatternTypeSpike                 = domain.PatternTypeSpike
	PatternTypeDrop                  = domain.PatternTypeDrop
	PatternTypeStall                 = domain.PatternTypeStall
	PatternTypeAnomaly               = domain.PatternTypeAnomaly
	PatternTypeSeasonality           = domain.PatternTypeSeasonality
	PatternTypeCorrelation           = domain.PatternTypeCorrelation
	PatternTypeChangePoint           = domain.PatternTypeChangePoint
	PatternTypeOutlierCluster        = domain.PatternTypeOutlierCluster
	PatternTypeCrossScopeCorrelation = domain.PatternTypeCrossScopeCorrelation
	PatternTypeOscillation           = domain.PatternTypeOscillation
	PatternTypeDivergence            = domain.PatternTypeDivergence
	PatternTypeConvergence           = domain.PatternTypeConvergence
)

// ConfidenceClass constants re-exported.
const (
	ConfidenceClassTentative   = domain.ConfidenceClassTentative
	ConfidenceClassEstablished = domain.ConfidenceClassEstablished
	ConfidenceClassStrong      = domain.ConfidenceClassStrong
)
