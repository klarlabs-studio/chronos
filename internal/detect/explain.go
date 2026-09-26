package detect

import (
	"github.com/felixgeelhaar/chronos"
	"github.com/felixgeelhaar/chronos/internal/domain"
)

// Detector version tags are stable strings consumers can use to detect
// logic drift. Bump the suffix when the detector's math or evidence
// shape changes in a way that would invalidate prior explanations.
const (
	detectorVersionRecurrence            = "recurrence-v2"
	detectorVersionTrend                 = "trend-v2"
	detectorVersionSpike                 = "spike-v2"
	detectorVersionDrop                  = "drop-v2"
	detectorVersionStall                 = "stall-v1"
	detectorVersionAnomaly               = "anomaly-v1"
	detectorVersionSeasonality           = "seasonality-v2"
	detectorVersionCorrelation           = "correlation-v2"
	detectorVersionChangePoint           = "changepoint-v1"
	detectorVersionOutlierCluster        = "outlier_cluster-v1"
	detectorVersionCrossScopeCorrelation = "cross_scope_correlation-v2"
	detectorVersionOscillation           = "oscillation-v1"
	detectorVersionDivergence            = "divergence-v2"
	detectorVersionConvergence           = "convergence-v2"
)

// explainSeries builds the explainability payload for a detector that
// inspected a chronological observation window. peers is 0 when the
// detector is not peer-based. BaselineWindowDays is left zero — Chronos
// windows are counted in observations, not calendar days.
func explainSeries(observations []chronos.EntityState, peers int, threshold float64, version string) domain.Explanation {
	samples := make([]domain.FeatureSample, 0, len(observations))
	for _, o := range observations {
		samples = append(samples, domain.FeatureSample{At: o.Timestamp, Value: o.Outcome()})
	}
	return domain.Explanation{
		FeatureEvolution: samples,
		ComparablePeers:  peers,
		ThresholdUsed:    threshold,
		DetectorVersion:  version,
	}
}
