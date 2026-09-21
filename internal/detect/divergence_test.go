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

func divergenceCfg() *config.Config {
	return &config.Config{
		MaxSignalsPerRun:     100,
		DivergenceMinSlope:   0.05,
		DivergenceMinPoints:  5,
		DivergenceMinR2:      0.5,
		ConvergenceMinSlope:  0.05,
		ConvergenceMinPoints: 5,
		ConvergenceMinR2:     0.5,
	}
}

func TestDivergence_GrowingGapEmits(t *testing.T) {
	d := NewDivergence(divergenceCfg())
	scope := uuid.New()
	a := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	b := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	now := time.Now()
	// a flat at 10; b rises 1,2,3,4,5,6 — gap grows 9→4... wait that's shrinking.
	// a flat at 0; b rises 1..6 — gap grows 1→6.
	statesA := mkParallelSeries(scope, a, now, []float64{0, 0, 0, 0, 0, 0})
	statesB := mkParallelSeries(scope, b, now, []float64{1, 2, 3, 4, 5, 6})
	all := append(append([]chronos.EntityState{}, statesA...), statesB...)
	got := d.Detect(context.Background(), scope, all)
	if len(got) != 1 {
		t.Fatalf("got %d signals, want 1", len(got))
	}
	sig := got[0]
	if sig.Pattern != domain.PatternTypeDivergence {
		t.Errorf("Pattern = %s", sig.Pattern)
	}
	if sig.Metrics["slope"] <= 0 {
		t.Errorf("slope = %v, want positive", sig.Metrics["slope"])
	}
	// Hourly samples, gap grows by 1 per hour.
	if math.Abs(sig.Metrics["slope_per_hour"]-1) > 1e-9 {
		t.Errorf("slope_per_hour = %v, want 1", sig.Metrics["slope_per_hour"])
	}
	if sig.Metrics["r_squared"] < 0.99 {
		t.Errorf("r_squared = %v, want a clean line", sig.Metrics["r_squared"])
	}
	if sig.Series != a {
		t.Errorf("Series = %v, want lex-smaller %v", sig.Series, a)
	}
	if len(sig.Evidence) != 1 || sig.Evidence[0].Kind != "pair_divergence" {
		t.Errorf("evidence = %+v", sig.Evidence)
	}
	if err := sig.Validate(); err != nil {
		t.Errorf("invalid: %v", err)
	}
}

func TestDivergence_ParallelNoSignal(t *testing.T) {
	d := NewDivergence(divergenceCfg())
	scope := uuid.New()
	now := time.Now()
	statesA := mkParallelSeries(scope, uuid.New(), now, []float64{1, 2, 3, 4, 5, 6})
	statesB := mkParallelSeries(scope, uuid.New(), now, []float64{2, 3, 4, 5, 6, 7}) // constant gap 1
	all := append(append([]chronos.EntityState{}, statesA...), statesB...)
	got := d.Detect(context.Background(), scope, all)
	if len(got) != 0 {
		t.Fatalf("got %d signals on constant gap, want 0", len(got))
	}
}

func TestConvergence_ShrinkingGapEmits(t *testing.T) {
	d := NewConvergence(divergenceCfg())
	scope := uuid.New()
	a := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	b := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	now := time.Now()
	statesA := mkParallelSeries(scope, a, now, []float64{0, 0, 0, 0, 0, 0})
	statesB := mkParallelSeries(scope, b, now, []float64{10, 8, 6, 4, 2, 0})
	all := append(append([]chronos.EntityState{}, statesA...), statesB...)
	got := d.Detect(context.Background(), scope, all)
	if len(got) != 1 {
		t.Fatalf("got %d signals, want 1", len(got))
	}
	sig := got[0]
	if sig.Pattern != domain.PatternTypeConvergence {
		t.Errorf("Pattern = %s", sig.Pattern)
	}
	if sig.Metrics["slope"] >= 0 {
		t.Errorf("slope = %v, want negative", sig.Metrics["slope"])
	}
	if len(sig.Evidence) != 1 || sig.Evidence[0].Kind != "pair_convergence" {
		t.Errorf("evidence = %+v", sig.Evidence)
	}
	if err := sig.Validate(); err != nil {
		t.Errorf("invalid: %v", err)
	}
}

func TestConvergence_GrowingGapNoSignal(t *testing.T) {
	d := NewConvergence(divergenceCfg())
	scope := uuid.New()
	now := time.Now()
	statesA := mkParallelSeries(scope, uuid.New(), now, []float64{0, 0, 0, 0, 0, 0})
	statesB := mkParallelSeries(scope, uuid.New(), now, []float64{1, 2, 3, 4, 5, 6})
	all := append(append([]chronos.EntityState{}, statesA...), statesB...)
	got := d.Detect(context.Background(), scope, all)
	if len(got) != 0 {
		t.Fatalf("got %d convergence signals on growing gap, want 0", len(got))
	}
}
