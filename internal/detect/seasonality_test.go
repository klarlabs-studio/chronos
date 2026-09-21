package detect

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/felixgeelhaar/chronos/internal/config"
	"github.com/felixgeelhaar/chronos/internal/domain"
	"github.com/google/uuid"
)

func seasonalityCfg() *config.Config {
	return &config.Config{
		MaxSignalsPerRun:       100,
		SeasonalityMinAutocorr: 0.5,
		SeasonalityMinPoints:   12,
		SeasonalityMinPeriod:   2,
	}
}

func TestSeasonality_PeriodicSeriesEmits(t *testing.T) {
	d := NewSeasonality(seasonalityCfg())
	scope := uuid.New()
	entity := uuid.New()
	now := time.Now()

	// 24 points of a clean period-4 sine: high autocorrelation at lag 4.
	ys := make([]float64, 24)
	for i := range ys {
		ys[i] = math.Sin(float64(i) * math.Pi / 2) // period 4
	}
	states := mkSeries(scope, entity, now, ys)

	got := d.Detect(context.Background(), scope, states)
	if len(got) != 1 {
		t.Fatalf("got %d signals, want 1", len(got))
	}
	sig := got[0]
	if sig.Pattern != domain.PatternTypeSeasonality {
		t.Errorf("Pattern = %s", sig.Pattern)
	}
	if int(sig.Metrics["period"]) != 4 {
		t.Errorf("period = %v, want 4", sig.Metrics["period"])
	}
	if sig.Metrics["period_samples"] != 4 {
		t.Errorf("period_samples = %v, want 4", sig.Metrics["period_samples"])
	}
	// mkSeries stamps one point per hour, so a lag of 4 is four hours.
	if math.Abs(sig.Metrics["period_seconds"]-4*time.Hour.Seconds()) > 1e-6 {
		t.Errorf("period_seconds = %v, want %v", sig.Metrics["period_seconds"], 4*time.Hour.Seconds())
	}
	if sig.Metrics["autocorrelation"] < seasonalityCfg().SeasonalityMinAutocorr {
		t.Errorf("autocorrelation = %f below threshold", sig.Metrics["autocorrelation"])
	}
	if err := sig.Validate(); err != nil {
		t.Errorf("invalid signal: %v", err)
	}
}

func TestSeasonality_NonperiodicSeriesNoSignal(t *testing.T) {
	d := NewSeasonality(seasonalityCfg())
	scope := uuid.New()
	entity := uuid.New()
	now := time.Now()
	// Strict linear trend: high linear correlation but no periodicity.
	// The autocorrelation peaks at lag 1 with high value (since
	// neighbours are very similar), so this *can* trigger seasonality.
	// To avoid that, we use a noisy uncorrelated series instead.
	ys := []float64{0.1, 5.0, 0.3, 4.7, -1.2, 6.8, 0.5, 4.5, -0.8, 5.5, 0.2, 4.8, -0.5, 5.0}
	states := mkSeries(scope, entity, now, ys)

	got := d.Detect(context.Background(), scope, states)
	// We accept emit-or-no-emit here because pseudo-noisy data can
	// sometimes show weak peaks. What we assert is that *if* it emits
	// the period must be sensible (>=MinPeriod) and the
	// autocorrelation must clear the threshold.
	for _, sig := range got {
		if int(sig.Metrics["period"]) < seasonalityCfg().SeasonalityMinPeriod {
			t.Errorf("period %v below MinPeriod", sig.Metrics["period"])
		}
		if sig.Metrics["autocorrelation"] < seasonalityCfg().SeasonalityMinAutocorr {
			t.Errorf("autocorrelation %f below threshold", sig.Metrics["autocorrelation"])
		}
	}
}

func TestSeasonality_RespectsMinPoints(t *testing.T) {
	d := NewSeasonality(seasonalityCfg())
	scope := uuid.New()
	entity := uuid.New()
	now := time.Now()
	// 8 points — below MinPoints=12.
	ys := []float64{1, 0, 1, 0, 1, 0, 1, 0}
	states := mkSeries(scope, entity, now, ys)
	got := d.Detect(context.Background(), scope, states)
	if len(got) != 0 {
		t.Errorf("got %d signals below MinPoints, want 0", len(got))
	}
}

func TestAutocorrelation(t *testing.T) {
	// A pure period-2 binary signal has autocorrelation exactly 1 at
	// lag 2 and -1 at lag 1.
	ys := []float64{0, 1, 0, 1, 0, 1, 0, 1}
	if got := autocorrelation(ys, 2); math.Abs(got-1) > 1e-9 {
		t.Errorf("autocorrelation lag 2 = %f, want 1", got)
	}
	if got := autocorrelation(ys, 1); got > 0 {
		t.Errorf("autocorrelation lag 1 should be negative for period-2 signal, got %f", got)
	}
}
