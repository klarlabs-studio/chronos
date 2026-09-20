package grpc

import (
	"time"

	chronosv1 "github.com/felixgeelhaar/chronos/api/proto/chronos/v1"
	"github.com/felixgeelhaar/chronos/internal/domain"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// patternTypeToDomain maps proto PatternType to domain.PatternType.
func patternTypeToDomain(p chronosv1.PatternType) domain.PatternType {
	switch p {
	case chronosv1.PatternType_PATTERN_TYPE_RECURRENCE:
		return domain.PatternTypeRecurrence
	case chronosv1.PatternType_PATTERN_TYPE_TREND:
		return domain.PatternTypeTrend
	case chronosv1.PatternType_PATTERN_TYPE_SPIKE:
		return domain.PatternTypeSpike
	case chronosv1.PatternType_PATTERN_TYPE_DROP:
		return domain.PatternTypeDrop
	case chronosv1.PatternType_PATTERN_TYPE_STALL:
		return domain.PatternTypeStall
	case chronosv1.PatternType_PATTERN_TYPE_ANOMALY:
		return domain.PatternTypeAnomaly
	case chronosv1.PatternType_PATTERN_TYPE_SEASONALITY:
		return domain.PatternTypeSeasonality
	case chronosv1.PatternType_PATTERN_TYPE_CORRELATION:
		return domain.PatternTypeCorrelation
	case chronosv1.PatternType_PATTERN_TYPE_CHANGE_POINT:
		return domain.PatternTypeChangePoint
	case chronosv1.PatternType_PATTERN_TYPE_OUTLIER_CLUSTER:
		return domain.PatternTypeOutlierCluster
	case chronosv1.PatternType_PATTERN_TYPE_CROSS_SCOPE_CORRELATION:
		return domain.PatternTypeCrossScopeCorrelation
	case chronosv1.PatternType_PATTERN_TYPE_OSCILLATION:
		return domain.PatternTypeOscillation
	case chronosv1.PatternType_PATTERN_TYPE_DIVERGENCE:
		return domain.PatternTypeDivergence
	case chronosv1.PatternType_PATTERN_TYPE_CONVERGENCE:
		return domain.PatternTypeConvergence
	default:
		return ""
	}
}

// patternTypeFromDomain maps domain.PatternType to proto PatternType.
func patternTypeFromDomain(p domain.PatternType) chronosv1.PatternType {
	switch p {
	case domain.PatternTypeRecurrence:
		return chronosv1.PatternType_PATTERN_TYPE_RECURRENCE
	case domain.PatternTypeTrend:
		return chronosv1.PatternType_PATTERN_TYPE_TREND
	case domain.PatternTypeSpike:
		return chronosv1.PatternType_PATTERN_TYPE_SPIKE
	case domain.PatternTypeDrop:
		return chronosv1.PatternType_PATTERN_TYPE_DROP
	case domain.PatternTypeStall:
		return chronosv1.PatternType_PATTERN_TYPE_STALL
	case domain.PatternTypeAnomaly:
		return chronosv1.PatternType_PATTERN_TYPE_ANOMALY
	case domain.PatternTypeSeasonality:
		return chronosv1.PatternType_PATTERN_TYPE_SEASONALITY
	case domain.PatternTypeCorrelation:
		return chronosv1.PatternType_PATTERN_TYPE_CORRELATION
	case domain.PatternTypeChangePoint:
		return chronosv1.PatternType_PATTERN_TYPE_CHANGE_POINT
	case domain.PatternTypeOutlierCluster:
		return chronosv1.PatternType_PATTERN_TYPE_OUTLIER_CLUSTER
	case domain.PatternTypeCrossScopeCorrelation:
		return chronosv1.PatternType_PATTERN_TYPE_CROSS_SCOPE_CORRELATION
	case domain.PatternTypeOscillation:
		return chronosv1.PatternType_PATTERN_TYPE_OSCILLATION
	case domain.PatternTypeDivergence:
		return chronosv1.PatternType_PATTERN_TYPE_DIVERGENCE
	case domain.PatternTypeConvergence:
		return chronosv1.PatternType_PATTERN_TYPE_CONVERGENCE
	default:
		return chronosv1.PatternType_PATTERN_TYPE_UNSPECIFIED
	}
}

// fromDomainSignal converts a domain.Signal to a proto Signal.
func fromDomainSignal(s domain.Signal) *chronosv1.Signal {
	evidence := make([]*chronosv1.Evidence, 0, len(s.Evidence))
	for _, e := range s.Evidence {
		evidence = append(evidence, &chronosv1.Evidence{
			Series:  e.Series.String(),
			Time:    timestamppb.New(e.Time),
			Kind:    e.Kind,
			Score:   e.Score,
			Metrics: e.Metrics,
		})
	}
	return &chronosv1.Signal{
		Id:         s.ID.String(),
		ScopeId:    s.ScopeID.String(),
		Series:     s.Series.String(),
		Pattern:    patternTypeFromDomain(s.Pattern),
		DetectedAt: timestamppb.New(s.DetectedAt),
		Window: &chronosv1.TimeWindow{
			Start: timestamppb.New(s.Window.Start),
			End:   timestamppb.New(s.Window.End),
		},
		Strength:        s.Strength,
		Confidence:      s.Confidence,
		Metrics:         s.Metrics,
		Evidence:        evidence,
		Explanation:     explanationToProto(s.Explanation),
		ConfidenceClass: string(s.ConfidenceClass),
	}
}

// explanationToProto maps domain.Explanation to its proto wire shape.
// Returns nil when the Explanation is zero-valued so consumers can
// distinguish "no explanation surfaced" from "explanation with default
// values".
func explanationToProto(e domain.Explanation) *chronosv1.Explanation {
	if e.IsZero() {
		return nil
	}
	samples := make([]*chronosv1.FeatureSample, 0, len(e.FeatureEvolution))
	for _, fs := range e.FeatureEvolution {
		samples = append(samples, &chronosv1.FeatureSample{
			At:    timestamppb.New(fs.At),
			Value: fs.Value,
		})
	}
	return &chronosv1.Explanation{
		FeatureEvolution:   samples,
		ComparablePeers:    int32(e.ComparablePeers),    //nolint:gosec // detector-bound
		BaselineWindowDays: int32(e.BaselineWindowDays), //nolint:gosec // detector-bound
		ThresholdUsed:      e.ThresholdUsed,
		DetectorVersion:    e.DetectorVersion,
	}
}

// parseUUID safely parses a UUID string; returns uuid.Nil on error.
func parseUUID(s string) uuid.UUID {
	id, err := uuid.Parse(s)
	if err != nil {
		return uuid.Nil
	}
	return id
}

// protoTime converts a protobuf Timestamp to time.Time, returning zero on nil.
func protoTime(t *timestamppb.Timestamp) time.Time {
	if t == nil {
		return time.Time{}
	}
	return t.AsTime()
}
