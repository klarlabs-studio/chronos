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
// Method: temporally align observations ([AlignNearest], tolerance
// CHRONOS_ALIGN_TOLERANCE) and compute Pearson r on the aligned
// outcomes. Same contract as within-scope Correlation — a different
// scope does not relax the requirement that the observations were
// contemporaneous. Minimum sample size counts aligned pairs. The
// signal window spans only those pairs, not the union of the two
// histories.
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
			pairs := AlignNearest(groups[a], groups[b], c.cfg.AlignTolerance)
			n := len(pairs)
			if n < c.cfg.CrossScopeMinPoints {
				continue
			}
			ya := make([]float64, n)
			yb := make([]float64, n)
			alignedA := make([]chronos.EntityState, n)
			for k, p := range pairs {
				ya[k] = p.A.Outcome()
				yb[k] = p.B.Outcome()
				alignedA[k] = p.A
			}
			r := pearsonCorrelation(ya, yb)
			if math.IsNaN(r) || !isFinite(r) {
				continue
			}
			absR := math.Abs(r)
			if absR < c.cfg.CrossScopeMin {
				continue
			}
			signals = append(signals, c.build(a, b, r, pairs, alignedA))
		}
	}
	return keepValid(signals)
}

func (c *CrossScopeCorrelation) build(a, b scopedSeriesKey, r float64, pairs []AlignedPair, alignedA []chronos.EntityState) domain.Signal {
	n := len(pairs)
	absR := math.Abs(r)
	direction := 0.0
	if r > 0 {
		direction = 1
	} else if r < 0 {
		direction = -1
	}
	start, end := alignedWindow(pairs)
	metrics := map[string]float64{
		"r":                           r,
		"abs_r":                       absR,
		"n":                           float64(n),
		"aligned_samples":             float64(n),
		"alignment_tolerance_seconds": c.cfg.AlignTolerance.Seconds(),
		"direction":                   direction,
	}

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
		Window:          domain.TimeWindow{Start: start, End: end},
		Strength:        absR,
		Confidence:      clamp01(absR * sampleFactor(n, 2*c.cfg.CrossScopeMinPoints)),
		ConfidenceClass: ClassifyConfidence(n, c.cfg.CrossScopeMinPoints, c.cfg),
		Metrics:         metrics,
		Explanation:     explainSeries(alignedA, 1, c.cfg.CrossScopeMin, detectorVersionCrossScopeCorrelation),
		Evidence: []domain.Evidence{{
			Series: emittedPartner,
			Time:   end,
			Kind:   "cross_scope_pair",
			Score:  absR,
			Metrics: map[string]float64{
				"partner_scope_id_lex_max":    1,
				"r":                           r,
				"n":                           float64(n),
				"aligned_samples":             float64(n),
				"alignment_tolerance_seconds": c.cfg.AlignTolerance.Seconds(),
			},
		}},
	}
}
