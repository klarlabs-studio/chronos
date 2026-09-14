package detect

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/felixgeelhaar/chronos"
	"github.com/felixgeelhaar/chronos/internal/config"
	"github.com/felixgeelhaar/chronos/internal/domain"
	"github.com/google/uuid"
)

func cpCfg() *config.Config {
	return &config.Config{
		ChangePointMinShift:  1.5,
		ChangePointMinPoints: 8,
	}
}

func cpSeries(scope, series uuid.UUID, values []float64) []chronos.EntityState {
	out := make([]chronos.EntityState, len(values))
	t0 := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	for i, v := range values {
		out[i] = chronos.EntityState{
			ID:        uuid.New(),
			EntityID:  series,
			ScopeID:   scope,
			Timestamp: t0.Add(time.Duration(i) * time.Hour),
			Features:  []float64{v}, // last feature = outcome
		}
	}
	return out
}

func TestChangePoint_DetectsStepChange(t *testing.T) {
	t.Parallel()
	scope := uuid.New()
	series := uuid.New()
	// Mean shifts from ~10 to ~20 at index 6.
	values := []float64{10, 10.2, 9.8, 10.1, 10, 9.9, 20, 20.1, 19.9, 20, 20.2, 19.8}
	d := NewChangePoint(cpCfg())
	signals := d.Detect(context.Background(), scope, cpSeries(scope, series, values))
	if len(signals) != 1 {
		t.Fatalf("len = %d, want 1; got %+v", len(signals), signals)
	}
	s := signals[0]
	if s.Pattern != domain.PatternTypeChangePoint {
		t.Errorf("pattern = %q", s.Pattern)
	}
	if got := s.Metrics["split_index"]; got != 6 {
		t.Errorf("split_index = %v, want 6", got)
	}
	if s.Metrics["mean_before"] >= s.Metrics["mean_after"] {
		t.Errorf("mean_before %v should be < mean_after %v",
			s.Metrics["mean_before"], s.Metrics["mean_after"])
	}
	if got := s.Metrics["shift"]; got < 1.5 {
		t.Errorf("shift = %v, want ≥ 1.5", got)
	}
	if len(s.Evidence) != 2 {
		t.Errorf("evidence kinds = %d", len(s.Evidence))
	}
}

func TestChangePoint_NoSignalForFlatSeries(t *testing.T) {
	t.Parallel()
	scope := uuid.New()
	values := []float64{10, 10.1, 9.9, 10, 10.2, 9.8, 10, 10.1}
	d := NewChangePoint(cpCfg())
	signals := d.Detect(context.Background(), scope, cpSeries(scope, uuid.New(), values))
	if len(signals) != 0 {
		t.Errorf("len = %d, want 0", len(signals))
	}
}

func TestChangePoint_NoSignalBelowMinPoints(t *testing.T) {
	t.Parallel()
	scope := uuid.New()
	values := []float64{10, 10, 20, 20}
	d := NewChangePoint(cpCfg())
	signals := d.Detect(context.Background(), scope, cpSeries(scope, uuid.New(), values))
	if len(signals) != 0 {
		t.Errorf("len = %d, want 0 (below ChangePointMinPoints)", len(signals))
	}
}

func TestChangePoint_PicksLargestShift(t *testing.T) {
	t.Parallel()
	scope := uuid.New()
	// Two candidate splits — only the larger crosses threshold.
	values := []float64{10, 10.2, 11, 10.8, 12, 12.1, 12, 30, 29.5, 30.5, 29.8, 30.2}
	d := NewChangePoint(cpCfg())
	signals := d.Detect(context.Background(), scope, cpSeries(scope, uuid.New(), values))
	if len(signals) != 1 {
		t.Fatalf("len = %d", len(signals))
	}
	if got := signals[0].Metrics["split_index"]; got != 7 {
		t.Errorf("split_index = %v, want 7", got)
	}
}

// TestChangePoint_MinDeltaSuppressesQuietSeries covers the case a
// purely standardised threshold cannot: an outcome so stable that the
// pooled stddev approaches zero, where any movement at all divides out
// to a large shift.
//
// Taken from a production series. Health headroom moved from 0.9050 to
// 0.9072 — an improvement of two tenths of one percent — and scored
// 7.98 sigma at confidence 1.00, because the series varies in the
// fourth decimal place. Statistically correct; operationally noise.
func TestChangePoint_MinDeltaSuppressesQuietSeries(t *testing.T) {
	t.Parallel()
	scope := uuid.New()
	series := uuid.New()

	// Two regimes 0.002 apart, each varying by ~0.0001.
	values := []float64{
		0.9050, 0.9051, 0.9050, 0.9049, 0.9050, 0.9051,
		0.9071, 0.9072, 0.9071, 0.9070, 0.9072, 0.9071,
	}

	t.Run("fires without a delta floor", func(t *testing.T) {
		cfg := cpCfg()
		signals := NewChangePoint(cfg).Detect(context.Background(), scope, cpSeries(scope, series, values))
		if len(signals) != 1 {
			t.Fatalf("got %d signals, want 1 — the standardised shift is large here", len(signals))
		}
		if got := signals[0].Metrics["shift"]; got < 3 {
			t.Errorf("shift = %.2f, expected a large standardised shift on so quiet a series", got)
		}
	})

	t.Run("suppressed by a delta floor", func(t *testing.T) {
		cfg := cpCfg()
		cfg.ChangePointMinDelta = 0.01 // a full percentage point of headroom
		signals := NewChangePoint(cfg).Detect(context.Background(), scope, cpSeries(scope, series, values))
		if len(signals) != 0 {
			t.Fatalf("got %d signals, want none: |delta_mean| ~0.002 is below the 0.01 floor", len(signals))
		}
	})
}

// TestChangePoint_MinDeltaKeepsMaterialShifts: the floor must not
// suppress a change that is both significant and large.
func TestChangePoint_MinDeltaKeepsMaterialShifts(t *testing.T) {
	t.Parallel()
	scope := uuid.New()
	series := uuid.New()

	// Headroom falls off a cliff: 0.90 -> 0.10.
	values := []float64{
		0.90, 0.91, 0.90, 0.89, 0.90, 0.91,
		0.10, 0.11, 0.10, 0.09, 0.10, 0.11,
	}

	cfg := cpCfg()
	cfg.ChangePointMinDelta = 0.01
	signals := NewChangePoint(cfg).Detect(context.Background(), scope, cpSeries(scope, series, values))
	if len(signals) != 1 {
		t.Fatalf("got %d signals, want 1: a 0.8 drop is well past the floor", len(signals))
	}
	if got := math.Abs(signals[0].Metrics["delta_mean"]); got < 0.7 {
		t.Errorf("delta_mean = %.3f, want the full regime change", got)
	}
}

// TestChangePoint_MinDeltaDefaultsOff keeps existing deployments
// behaving exactly as before.
func TestChangePoint_MinDeltaDefaultsOff(t *testing.T) {
	t.Parallel()
	if got := config.Default().ChangePointMinDelta; got != 0 {
		t.Errorf("ChangePointMinDelta default = %v, want 0 (disabled)", got)
	}
}
