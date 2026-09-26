package detect

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/felixgeelhaar/chronos"
	"github.com/felixgeelhaar/chronos/internal/config"
	"github.com/felixgeelhaar/chronos/internal/domain"
	"github.com/google/uuid"
)

// burstDetector emits n Validate-clean signals of one pattern, stamped
// DetectedAt = time.Now() at the moment it runs -- exactly as every
// shipped detector does. That is the property that matters: a detector
// registered later runs later and therefore stamps later timestamps.
type burstDetector struct {
	pattern    domain.PatternType
	n          int
	confidence float64
}

func (b burstDetector) Pattern() domain.PatternType { return b.pattern }

func (b burstDetector) Detect(_ context.Context, scopeID uuid.UUID, states []chronos.EntityState) []domain.Signal {
	start := states[0].Timestamp
	end := states[len(states)-1].Timestamp
	out := make([]domain.Signal, 0, b.n)
	for i := 0; i < b.n; i++ {
		out = append(out, domain.Signal{
			ScopeID:    scopeID,
			Series:     uuid.NewSHA1(uuid.Nil, []byte(fmt.Sprintf("%s-%d", b.pattern, i))),
			Pattern:    b.pattern,
			DetectedAt: time.Now(),
			Window:     domain.TimeWindow{Start: start, End: end},
			Strength:   0.5,
			Confidence: b.confidence,
		})
	}
	return out
}

func capStates() []chronos.EntityState {
	scope, entity := uuid.New(), uuid.New()
	base := time.Date(2026, 9, 26, 10, 0, 0, 0, time.UTC)
	var out []chronos.EntityState
	for i := 0; i < 3; i++ {
		out = append(out, chronos.EntityState{
			ID: uuid.New(), EntityID: entity, ScopeID: scope,
			Timestamp: base.Add(time.Duration(i) * time.Minute),
			Features:  []float64{1},
		})
	}
	return out
}

func countByPattern(sigs []domain.Signal) map[domain.PatternType]int {
	m := map[domain.PatternType]int{}
	for _, s := range sigs {
		m[s.Pattern]++
	}
	return m
}

// Reproduces the production incident of 2026-09-26. With the cap at 200,
// a detector registered after the others and emitting more than 200
// signals took every slot: registration order decided survival, because
// the sort key is DetectedAt and DetectedAt is wall-clock at run time.
// The classic detector, registered first, lost all of its signals.
func TestCap_RegistrationOrderDoesNotDecideSurvival(t *testing.T) {
	cfg := config.Default()
	cfg.MaxSignalsPerRun = 200
	e := NewEngine(cfg,
		burstDetector{pattern: domain.PatternTypeSpike, n: 5, confidence: 0.9},
		burstDetector{pattern: domain.PatternTypeDivergence, n: 300, confidence: 0.9},
	)
	got := countByPattern(e.Detect(context.Background(), capStates()))
	if got[domain.PatternTypeSpike] != 5 {
		t.Fatalf("spike kept %d of 5 under the cap (divergence kept %d); a detector registered first must not lose its output to one registered last",
			got[domain.PatternTypeSpike], got[domain.PatternTypeDivergence])
	}
}

// The subtler half. Normalising timestamps alone makes confidence the
// real sort key -- and the pairwise detectors emit at ~0.98 while the
// classic ones sit between 0.3 and 0.9 in production. A high-volume
// detector must not starve a low-volume one merely by also being more
// confident: the cap bounds memory, it is not a ranking across patterns.
func TestCap_HighVolumeHighConfidenceDoesNotStarveOthers(t *testing.T) {
	cfg := config.Default()
	cfg.MaxSignalsPerRun = 200
	e := NewEngine(cfg,
		burstDetector{pattern: domain.PatternTypeTrend, n: 8, confidence: 0.31},
		burstDetector{pattern: domain.PatternTypeDrop, n: 13, confidence: 0.83},
		burstDetector{pattern: domain.PatternTypeDivergence, n: 400, confidence: 0.989},
		burstDetector{pattern: domain.PatternTypeConvergence, n: 400, confidence: 0.983},
	)
	sigs := e.Detect(context.Background(), capStates())
	got := countByPattern(sigs)
	if len(sigs) != 200 {
		t.Fatalf("cap not honoured: %d signals, want 200", len(sigs))
	}
	if got[domain.PatternTypeTrend] != 8 || got[domain.PatternTypeDrop] != 13 {
		t.Fatalf("low-volume patterns starved: trend %d/8 drop %d/13 (divergence %d, convergence %d)",
			got[domain.PatternTypeTrend], got[domain.PatternTypeDrop],
			got[domain.PatternTypeDivergence], got[domain.PatternTypeConvergence])
	}
}

// Within one pattern the cap still keeps the most confident signals.
// Fairness is across patterns, not a replacement for ranking inside one.
func TestCap_WithinAPatternKeepsTheMostConfident(t *testing.T) {
	cfg := config.Default()
	cfg.MaxSignalsPerRun = 3
	low := burstDetector{pattern: domain.PatternTypeSpike, n: 5, confidence: 0.2}
	high := burstDetector{pattern: domain.PatternTypeSpike, n: 5, confidence: 0.9}
	e := NewEngine(cfg, low, high)
	for _, s := range e.Detect(context.Background(), capStates()) {
		if s.Confidence != 0.9 {
			t.Fatalf("kept a 0.2-confidence spike while 0.9-confidence ones were truncated")
		}
	}
}

// Every signal in one Detect call carries the same DetectedAt. Within a
// run, wall-clock differences between detectors are execution-order
// noise, not information, and they were deciding the sort.
func TestCap_OneDetectedAtPerRun(t *testing.T) {
	cfg := config.Default()
	e := NewEngine(cfg,
		burstDetector{pattern: domain.PatternTypeSpike, n: 3, confidence: 0.5},
		burstDetector{pattern: domain.PatternTypeDrop, n: 3, confidence: 0.5},
	)
	sigs := e.Detect(context.Background(), capStates())
	for _, s := range sigs[1:] {
		if !s.DetectedAt.Equal(sigs[0].DetectedAt) {
			t.Fatalf("DetectedAt varies within one run: %v vs %v", s.DetectedAt, sigs[0].DetectedAt)
		}
	}
}
