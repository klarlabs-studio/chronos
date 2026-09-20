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

// OutlierCluster detects PatternTypeOutlierCluster: a temporal window
// in which several series in the same scope go anomalous together.
// Distinct from per-series Anomaly (cross-entity): this is the
// cohort-level signal that something systemic is happening.
//
// Method: for each series, compute z-scores against the series's own
// rolling baseline (last [SpikeWindow] points; reuses the Spike
// detector's window so configuration stays consistent). A
// per-(series, time) "outlier event" is emitted when |z| ≥
// CHRONOS_OUTLIER_CLUSTER_Z. Outlier events are then grouped into
// time buckets of width CHRONOS_OUTLIER_CLUSTER_WINDOW; any bucket
// containing distinct-series count ≥ CHRONOS_OUTLIER_CLUSTER_MIN_SERIES
// becomes a signal. The signal's Series is uuid.Nil (cohort-level,
// not entity-level); evidence carries one row per participating
// series with its peak |z|.
type OutlierCluster struct {
	cfg *config.Config
	now func() time.Time
}

// NewOutlierCluster wires an OutlierCluster detector from configuration.
func NewOutlierCluster(cfg *config.Config) *OutlierCluster {
	return &OutlierCluster{cfg: cfg, now: time.Now}
}

// Pattern reports the PatternType this detector emits.
func (o *OutlierCluster) Pattern() domain.PatternType { return domain.PatternTypeOutlierCluster }

// outlierEvent is an internal record of a single anomalous
// observation: a series went |z| ≥ threshold at time t.
type outlierEvent struct {
	series uuid.UUID
	t      time.Time
	z      float64
}

// Detect groups outlier events into time windows and emits one
// signal per qualifying cluster.
func (o *OutlierCluster) Detect(_ context.Context, scopeID uuid.UUID, states []chronos.EntityState) []domain.Signal {
	if o.cfg.OutlierClusterMinSeries < 2 || o.cfg.OutlierClusterZ <= 0 {
		return nil
	}
	window := o.cfg.OutlierClusterTimeWindow
	if window <= 0 {
		window = 5 * time.Minute
	}

	// Pass 1: per-series outlier events.
	var events []outlierEvent
	ids, grouped := seriesInOrder(states)
	for _, series := range ids {
		observations := grouped[series]
		ys := outcomes(observations)
		if len(ys) < 3 {
			continue
		}
		baseline := o.cfg.SpikeWindow
		if baseline < 3 {
			baseline = 3
		}
		for i := baseline; i < len(ys); i++ {
			ref := ys[i-baseline : i]
			m := mean(ref)
			s := stddev(ref, m)
			var z float64
			switch {
			case s > 0:
				z = math.Abs(ys[i]-m) / s
			case ys[i] == m:
				continue // truly flat — not an outlier
			default:
				// Constant baseline + a different value is the
				// strongest possible outlier. Use a saturated z so
				// the row counts.
				z = 100
			}
			if z >= o.cfg.OutlierClusterZ {
				events = append(events, outlierEvent{series: series, t: observations[i].Timestamp, z: z})
			}
		}
	}
	if len(events) < o.cfg.OutlierClusterMinSeries {
		return nil
	}

	// Pass 2: time-bucket events by floor(t / window).
	sort.Slice(events, func(i, j int) bool { return events[i].t.Before(events[j].t) })
	bucketStarts := map[int64][]outlierEvent{}
	for _, e := range events {
		key := e.t.UnixNano() / int64(window)
		bucketStarts[key] = append(bucketStarts[key], e)
	}
	// Emit buckets oldest-first. Ranging the map directly would order
	// signals by Go's randomised map iteration, and the engine's
	// MaxSignalsPerRun cap truncates the tail.
	bucketKeys := make([]int64, 0, len(bucketStarts))
	for k := range bucketStarts {
		bucketKeys = append(bucketKeys, k)
	}
	sort.Slice(bucketKeys, func(i, j int) bool { return bucketKeys[i] < bucketKeys[j] })

	var signals []domain.Signal
	for _, key := range bucketKeys {
		bucket := bucketStarts[key]
		// Distinct-series count.
		seen := map[uuid.UUID]float64{}
		for _, e := range bucket {
			if z, ok := seen[e.series]; !ok || e.z > z {
				seen[e.series] = e.z
			}
		}
		if len(seen) < o.cfg.OutlierClusterMinSeries {
			continue
		}
		// Build the signal.
		earliest, latest := bucket[0].t, bucket[0].t
		for _, e := range bucket {
			if e.t.Before(earliest) {
				earliest = e.t
			}
			if e.t.After(latest) {
				latest = e.t
			}
		}
		ev := make([]domain.Evidence, 0, len(seen))
		for _, sid := range sortedUUIDKeys(seen) {
			ev = append(ev, domain.Evidence{
				Series:  sid,
				Time:    latest,
				Kind:    "outlier_member",
				Score:   seen[sid],
				Metrics: map[string]float64{"peak_z": seen[sid]},
			})
		}
		seriesCount := float64(len(seen))
		strength := clamp01((seriesCount - float64(o.cfg.OutlierClusterMinSeries)) / float64(o.cfg.OutlierClusterMinSeries+1))
		confidence := clamp01(strength + 0.3) // a clear cluster is meaningful even at the floor
		signals = append(signals, domain.Signal{
			ID:              uuid.New(),
			ScopeID:         scopeID,
			Series:          uuid.Nil, // cohort-level
			Pattern:         domain.PatternTypeOutlierCluster,
			DetectedAt:      o.now(),
			Window:          domain.TimeWindow{Start: earliest, End: latest},
			Strength:        strength,
			Confidence:      confidence,
			ConfidenceClass: ClassifyConfidence(len(seen), o.cfg.OutlierClusterMinSeries, o.cfg),
			Explanation: domain.Explanation{
				ComparablePeers: len(seen),
				ThresholdUsed:   o.cfg.OutlierClusterZ,
				DetectorVersion: detectorVersionOutlierCluster,
			},
			Metrics: map[string]float64{
				"member_count":   seriesCount,
				"window_seconds": window.Seconds(),
			},
			Evidence: ev,
		})
	}
	return signals
}
