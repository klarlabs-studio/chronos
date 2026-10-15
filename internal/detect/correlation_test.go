package detect

import (
	"context"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/felixgeelhaar/chronos"
	"github.com/felixgeelhaar/chronos/internal/config"
	"github.com/felixgeelhaar/chronos/internal/domain"
	"github.com/google/uuid"
)

func correlationCfg() *config.Config {
	return &config.Config{
		MaxSignalsPerRun:     100,
		CorrelationMin:       0.7,
		CorrelationMinPoints: 5,
	}
}

func mkParallelSeries(scope, entity uuid.UUID, start time.Time, ys []float64) []chronos.EntityState {
	out := make([]chronos.EntityState, len(ys))
	for i, y := range ys {
		out[i] = chronos.EntityState{
			ID: uuid.New(), EntityID: entity, ScopeID: scope,
			Timestamp: start.Add(time.Duration(i) * time.Hour),
			Features:  []float64{1, 2, 3, y},
		}
	}
	return out
}

func TestCorrelation_PositivelyCorrelatedPairEmits(t *testing.T) {
	d := NewCorrelation(correlationCfg())
	scope := uuid.New()
	a := uuid.New()
	b := uuid.New()
	now := time.Now()

	// b is a*2 + small offset — perfect correlation.
	statesA := mkParallelSeries(scope, a, now, []float64{1, 2, 3, 4, 5, 6})
	statesB := mkParallelSeries(scope, b, now, []float64{2.1, 4.0, 6.1, 8.0, 10.1, 12.0})

	all := append(append([]chronos.EntityState{}, statesA...), statesB...)
	got := d.Detect(context.Background(), scope, all)
	if len(got) != 1 {
		t.Fatalf("got %d signals, want 1", len(got))
	}
	sig := got[0]
	if sig.Pattern != domain.PatternTypeCorrelation {
		t.Errorf("Pattern = %s", sig.Pattern)
	}
	if sig.Metrics["r"] <= 0.9 {
		t.Errorf("r = %f, want very high positive correlation", sig.Metrics["r"])
	}
	if sig.Metrics["direction"] != 1 {
		t.Errorf("direction = %v, want +1 for positive correlation", sig.Metrics["direction"])
	}
	if len(sig.Evidence) != 1 || sig.Evidence[0].Kind != "pair_correlation" {
		t.Errorf("evidence wrong: %+v", sig.Evidence)
	}
	// Series should be the lexicographically-smaller ID.
	expected := a
	if a.String() > b.String() {
		expected = b
	}
	if sig.Series != expected {
		t.Errorf("Series = %v, want %v (lex-smaller)", sig.Series, expected)
	}
	if err := sig.Validate(); err != nil {
		t.Errorf("invalid signal: %v", err)
	}
}

func TestCorrelation_AntiCorrelatedPairEmitsNegative(t *testing.T) {
	d := NewCorrelation(correlationCfg())
	scope := uuid.New()
	a := uuid.New()
	b := uuid.New()
	now := time.Now()

	statesA := mkParallelSeries(scope, a, now, []float64{1, 2, 3, 4, 5, 6})
	statesB := mkParallelSeries(scope, b, now, []float64{6, 5, 4, 3, 2, 1})
	all := append(append([]chronos.EntityState{}, statesA...), statesB...)

	got := d.Detect(context.Background(), scope, all)
	if len(got) != 1 {
		t.Fatalf("got %d signals, want 1", len(got))
	}
	if got[0].Metrics["r"] >= 0 {
		t.Errorf("r = %f, want negative for anti-correlation", got[0].Metrics["r"])
	}
	if got[0].Metrics["direction"] != -1 {
		t.Errorf("direction = %v, want -1", got[0].Metrics["direction"])
	}
	// Strength is |r|, so still high.
	if got[0].Strength < 0.9 {
		t.Errorf("Strength = %f, want high for clean anti-correlation", got[0].Strength)
	}
}

func TestCorrelation_UncorrelatedPairNoSignal(t *testing.T) {
	d := NewCorrelation(correlationCfg())
	scope := uuid.New()
	now := time.Now()

	statesA := mkParallelSeries(scope, uuid.New(), now, []float64{1, 5, 2, 6, 3, 7, 4})
	statesB := mkParallelSeries(scope, uuid.New(), now, []float64{4, 1, 6, 2, 5, 3, 7})
	all := append(append([]chronos.EntityState{}, statesA...), statesB...)

	got := d.Detect(context.Background(), scope, all)
	if len(got) != 0 {
		t.Errorf("got %d signals on uncorrelated pair, want 0", len(got))
	}
}

func TestCorrelation_RespectsMinPoints(t *testing.T) {
	d := NewCorrelation(correlationCfg())
	scope := uuid.New()
	now := time.Now()
	statesA := mkParallelSeries(scope, uuid.New(), now, []float64{1, 2, 3, 4})
	statesB := mkParallelSeries(scope, uuid.New(), now, []float64{2, 4, 6, 8})
	all := append(append([]chronos.EntityState{}, statesA...), statesB...)
	got := d.Detect(context.Background(), scope, all)
	if len(got) != 0 {
		t.Errorf("got %d signals below MinPoints, want 0", len(got))
	}
}

func TestPearsonCorrelation(t *testing.T) {
	xs := []float64{1, 2, 3, 4, 5}
	ys := []float64{2, 4, 6, 8, 10}
	r := pearsonCorrelation(xs, ys)
	if r < 0.999999 {
		t.Errorf("r = %f, want ~1", r)
	}
	r = pearsonCorrelation(xs, []float64{5, 4, 3, 2, 1})
	if r > -0.999999 {
		t.Errorf("r = %f, want ~-1", r)
	}
	if r := pearsonCorrelation([]float64{1, 1, 1}, []float64{1, 2, 3}); r != 0 {
		t.Errorf("zero variance produced r=%f, want 0", r)
	}
}

// A health-headroom series sits near 0.97 and barely moves. The one-pass
// formula subtracts n*Σx² from (Σx)² there, two numbers that agree to
// almost every digit, and the rounding left over was reported as the
// correlation: Vorhut saw r 1.15 to 1.67 in production, and this sweep
// reached 206.
func TestPearsonCorrelation_StaysACorrelationOnANearlyFlatSeries(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for _, eps := range []float64{1e-7, 1e-8, 1e-9, 1e-10} {
		for trial := 0; trial < 2000; trial++ {
			xs, ys := make([]float64, 69), make([]float64, 69)
			base := 0.9 + rng.Float64()*0.09
			for i := range xs {
				xs[i] = base + rng.NormFloat64()*eps
				ys[i] = base + 0.5*(xs[i]-base) + rng.NormFloat64()*eps*0.3
			}
			if r := pearsonCorrelation(xs, ys); math.Abs(r) > 1 {
				t.Fatalf("eps %g trial %d: r = %v, outside [-1, 1]", eps, trial, r)
			}
		}
	}
}
