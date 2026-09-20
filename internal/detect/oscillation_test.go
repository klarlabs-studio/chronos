package detect

import (
	"context"
	"testing"
	"time"

	"github.com/felixgeelhaar/chronos"
	"github.com/felixgeelhaar/chronos/internal/config"
	"github.com/felixgeelhaar/chronos/internal/domain"
	"github.com/google/uuid"
)

func oscillationCfg() *config.Config {
	return &config.Config{
		MaxSignalsPerRun:       100,
		OscillationMinFlipRate: 0.55,
		OscillationMinPoints:   6,
	}
}

func TestOscillation_ZigZagEmits(t *testing.T) {
	d := NewOscillation(oscillationCfg())
	scope := uuid.New()
	entity := uuid.New()
	now := time.Now()
	// Alternating high/low — every consecutive meaningful diff flips sign.
	ys := []float64{1, 5, 1, 5, 1, 5, 1, 5}
	states := make([]chronos.EntityState, len(ys))
	for i, y := range ys {
		states[i] = chronos.EntityState{
			ID: uuid.New(), EntityID: entity, ScopeID: scope,
			Timestamp: now.Add(time.Duration(i) * time.Hour),
			Features:  []float64{y},
		}
	}
	got := d.Detect(context.Background(), scope, states)
	if len(got) != 1 {
		t.Fatalf("got %d signals, want 1", len(got))
	}
	sig := got[0]
	if sig.Pattern != domain.PatternTypeOscillation {
		t.Errorf("Pattern = %s", sig.Pattern)
	}
	if sig.Metrics["flip_rate"] < 0.9 {
		t.Errorf("flip_rate = %v, want near 1", sig.Metrics["flip_rate"])
	}
	if len(sig.Evidence) != 1 || sig.Evidence[0].Kind != "sign_flip_rate" {
		t.Errorf("evidence = %+v", sig.Evidence)
	}
	if err := sig.Validate(); err != nil {
		t.Errorf("invalid signal: %v", err)
	}
}

func TestOscillation_MonotoneNoSignal(t *testing.T) {
	d := NewOscillation(oscillationCfg())
	scope := uuid.New()
	entity := uuid.New()
	now := time.Now()
	ys := []float64{1, 2, 3, 4, 5, 6, 7, 8}
	states := make([]chronos.EntityState, len(ys))
	for i, y := range ys {
		states[i] = chronos.EntityState{
			ID: uuid.New(), EntityID: entity, ScopeID: scope,
			Timestamp: now.Add(time.Duration(i) * time.Hour),
			Features:  []float64{y},
		}
	}
	got := d.Detect(context.Background(), scope, states)
	if len(got) != 0 {
		t.Fatalf("got %d signals on monotone series, want 0", len(got))
	}
}

func TestOscillation_FlatNoSignal(t *testing.T) {
	d := NewOscillation(oscillationCfg())
	scope := uuid.New()
	entity := uuid.New()
	now := time.Now()
	ys := []float64{1, 1, 1, 1, 1, 1, 1, 1}
	states := make([]chronos.EntityState, len(ys))
	for i, y := range ys {
		states[i] = chronos.EntityState{
			ID: uuid.New(), EntityID: entity, ScopeID: scope,
			Timestamp: now.Add(time.Duration(i) * time.Hour),
			Features:  []float64{y},
		}
	}
	got := d.Detect(context.Background(), scope, states)
	if len(got) != 0 {
		t.Fatalf("got %d signals on flat series, want 0", len(got))
	}
}
