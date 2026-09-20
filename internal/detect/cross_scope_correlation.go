package detect

import (
	"context"
	"math"
	"sort"
	"time"

	"github.com/felixgeelhaar/chronos"
	"github.com/felixgeelhaar/chronos/internal/config"
	"github.com/felixgeelhaar/chronos/internal/domain"
	"github.com/google/uuid"
)

// CrossScopeCorrelation detects PatternTypeCrossScopeCorrelation:
// two series in DIFFERENT scopes whose outcome metrics move together
// (positive r) or opposite (negative r). The within-scope Correlation
// detector misses these by design — it groups by scope. This detector
// runs once across the full state list as a CrossScopeDetector.
//
// Method: align observations by ordinal index (mirroring the
// within-scope detector's assumption that adapters with similar
// cadence are comparable). Pearson correlation gated on
// CHRONOS_CROSS_SCOPE_MIN and CHRONOS_CROSS_SCOPE_MIN_POINTS.
//
// Cost is O(N²) in series count *globally*, so the threshold is
// stricter than the within-scope default (0.8 vs 0.7) to keep noise
// down. Operators with many scopes should tighten further.
//
// Each emitted signal is owned by the lex-smaller (scope, series)
// pair, with the partner pair carried in evidence. The signal's
// ScopeID is the lex-smaller scope.
type CrossScopeCorrelation struct {
	cfg *config.Config
	now func() time.Time
}

// NewCrossScopeCorrelation wires the detector from configuration.
func NewCrossScopeCorrelation(cfg *config.Config) *CrossScopeCorrelation {
	return &CrossScopeCorrelation{cfg: cfg, now: time.Now}
}

// Pattern reports the PatternType this detector emits.
func (c *CrossScopeCorrelation) Pattern() domain.PatternType {
	return domain.PatternTypeCrossScopeCorrelation
}

// scopedSeriesKey identifies a (scope, series) tuple.
type scopedSeriesKey struct {
	scope, series uuid.UUID
}

// CrossDetect computes pairwise correlations across every (scope,
// series) pair and emits one signal per pair above threshold.
func (c *CrossScopeCorrelation) CrossDetect(_ context.Context, states []chronos.EntityState) []domain.Signal {
	if c.cfg.CrossScopeMinPoints < 3 || c.cfg.CrossScopeMin <= 0 {
		return nil
	}

	// Group states by (scope, series) and sort each group by time.
	groups := map[scopedSeriesKey][]chronos.EntityState{}
	for _, s := range states {
		k := scopedSeriesKey{scope: s.ScopeID, series: s.EntityID}
		groups[k] = append(groups[k], s)
	}
	keys := make([]scopedSeriesKey, 0, len(groups))
	for k := range groups {
		sort.SliceStable(groups[k], func(i, j int) bool {
			return beforeByTimeThenID(groups[k][i], groups[k][j])
		})
		if len(groups[k]) >= c.cfg.CrossScopeMinPoints {
			keys = append(keys, k)
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].scope != keys[j].scope {
			return keys[i].scope.String() < keys[j].scope.String()
		}
		return keys[i].series.String() < keys[j].series.String()
	})

	var signals []domain.Signal
	for i := 0; i < len(keys); i++ {
		for j := i + 1; j < len(keys); j++ {
			a, b := keys[i], keys[j]
			if a.scope == b.scope {
				continue // within-scope correlation handles this
			}
			ya := outcomes(groups[a])
			yb := outcomes(groups[b])
			n := minInt(len(ya), len(yb))
			if n < c.cfg.CrossScopeMinPoints {
				continue
			}
			ya, yb = ya[len(ya)-n:], yb[len(yb)-n:]
			r := pearsonCorrelation(ya, yb)
			if math.IsNaN(r) {
				continue
			}
			absR := math.Abs(r)
			if absR < c.cfg.CrossScopeMin {
				continue
			}
			signals = append(signals, c.build(a, b, r, n, groups[a], groups[b]))
		}
	}
	return keepValid(signals)
}

func (c *CrossScopeCorrelation) build(a, b scopedSeriesKey, r float64, n int, sa, sb []chronos.EntityState) domain.Signal {
	absR := math.Abs(r)
	direction := 0.0
	if r > 0 {
		direction = 1
	} else if r < 0 {
		direction = -1
	}
	metrics := map[string]float64{
		"r":         r,
		"abs_r":     absR,
		"n":         float64(n),
		"direction": direction,
	}
	earliest := earliestTime(sa[0].Timestamp, sb[0].Timestamp)
	latest := latestTime(sa[len(sa)-1].Timestamp, sb[len(sb)-1].Timestamp)

	// The lex-smaller (scope, series) tuple owns the signal; the
	// other half rides in evidence. Anonymization replaces both
	// halves with deterministic UUIDv5 hashes so the cross-tenant
	// statistical perception stays useful without identifying which
	// tenants paired up.
	emittedScope, emittedSeries, emittedPartner := a.scope, a.series, b.series
	if c.cfg.AnonymizeCrossScope {
		emittedScope = anonymizeID(a.scope)
		emittedSeries = anonymizeID(a.series)
		emittedPartner = anonymizeID(b.series)
		metrics["anonymized"] = 1
	}
	return domain.Signal{
		ID:              uuid.New(),
		ScopeID:         emittedScope,
		Series:          emittedSeries,
		Pattern:         domain.PatternTypeCrossScopeCorrelation,
		DetectedAt:      c.now(),
		Window:          domain.TimeWindow{Start: earliest, End: latest},
		Strength:        absR,
		Confidence:      clamp01(absR * sampleFactor(n, 2*c.cfg.CrossScopeMinPoints)),
		ConfidenceClass: ClassifyConfidence(n, c.cfg.CrossScopeMinPoints, c.cfg),
		Metrics:         metrics,
		Explanation:     explainSeries(sa[max(0, len(sa)-n):], 1, c.cfg.CrossScopeMin, detectorVersionCrossScopeCorrelation),
		Evidence: []domain.Evidence{{
			Series:  emittedPartner,
			Time:    latest,
			Kind:    "cross_scope_pair",
			Score:   absR,
			Metrics: map[string]float64{"partner_scope_id_lex_max": 1, "r": r, "n": float64(n)},
		}},
	}
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func earliestTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

func latestTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}
