package domain

import (
	"encoding/json"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestNormalise(t *testing.T) {
	tests := []struct {
		name     string
		input    []float64
		expected []float64
	}{
		{"simple values", []float64{1, 2, 3, 4, 5}, []float64{-1.4142, -0.7071, 0, 0.7071, 1.4142}},
		{"single value", []float64{5}, []float64{0}},
		{"empty", []float64{}, nil},
		{"all same", []float64{3, 3, 3}, []float64{0, 0, 0}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Normalise(tt.input)
			if len(got) != len(tt.expected) {
				t.Fatalf("Normalise() length = %d, want %d", len(got), len(tt.expected))
			}
			for i := range got {
				if math.Abs(got[i]-tt.expected[i]) > 0.001 {
					t.Errorf("Normalise()[%d] = %v, want %v", i, got[i], tt.expected[i])
				}
			}
		})
	}
}

func TestSignal_Validate(t *testing.T) {
	scope := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	series := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	now := time.Now()

	base := Signal{
		ID:         uuid.New(),
		ScopeID:    scope,
		Series:     series,
		Pattern:    PatternTypeRecurrence,
		DetectedAt: now,
		Window:     TimeWindow{Start: now.Add(-time.Hour), End: now},
		Strength:   0.9,
		Confidence: 0.8,
	}
	if err := base.Validate(); err != nil {
		t.Fatalf("valid signal rejected: %v", err)
	}

	tests := []struct {
		name string
		mut  func(*Signal)
		want error
	}{
		{"missing scope", func(s *Signal) { s.ScopeID = uuid.Nil }, ErrMissingScopeID},
		{"missing series", func(s *Signal) { s.Series = uuid.Nil }, ErrMissingSeriesID},
		{"invalid explanation", func(s *Signal) { s.Explanation.ComparablePeers = -1 }, ErrInvalidExplanation},
		{"missing pattern", func(s *Signal) { s.Pattern = "" }, ErrMissingPattern},
		{"strength > 1", func(s *Signal) { s.Strength = 1.2 }, ErrInvalidStrength},
		{"strength < 0", func(s *Signal) { s.Strength = -0.1 }, ErrInvalidStrength},
		{"confidence > 1", func(s *Signal) { s.Confidence = 1.5 }, ErrInvalidConfidence},
		{"confidence < 0", func(s *Signal) { s.Confidence = -0.1 }, ErrInvalidConfidence},
		{"window inverted", func(s *Signal) { s.Window = TimeWindow{Start: now, End: now.Add(-time.Hour)} }, ErrInvalidWindow},
		// NaN passes a bare range check: NaN < 0 and NaN > 1 are both
		// false. These two cases exist so the finiteness test cannot be
		// deleted as redundant with the range test above.
		{"strength NaN", func(s *Signal) { s.Strength = math.NaN() }, ErrInvalidStrength},
		{"confidence NaN", func(s *Signal) { s.Confidence = math.NaN() }, ErrInvalidConfidence},
		{"metric +Inf", func(s *Signal) { s.Metrics = map[string]float64{"mean": math.Inf(1), "n": 12} }, ErrNonFiniteMetric},
		{"metric -Inf", func(s *Signal) { s.Metrics = map[string]float64{"slope": math.Inf(-1)} }, ErrNonFiniteMetric},
		{"metric NaN", func(s *Signal) { s.Metrics = map[string]float64{"r2": math.NaN()} }, ErrNonFiniteMetric},
		{"evidence score NaN", func(s *Signal) {
			s.Evidence = []Evidence{{Series: series, Time: now, Kind: "baseline_deviation", Score: math.NaN()}}
		}, ErrNonFiniteMetric},
		{"evidence metric +Inf", func(s *Signal) {
			s.Evidence = []Evidence{{Series: series, Time: now, Kind: "baseline_deviation", Score: 2.5,
				Metrics: map[string]float64{"z_score": math.Inf(1)}}}
		}, ErrNonFiniteMetric},
		{"explanation threshold NaN", func(s *Signal) { s.Explanation.ThresholdUsed = math.NaN() }, ErrNonFiniteMetric},
		{"explanation feature value +Inf", func(s *Signal) {
			s.Explanation.FeatureEvolution = []FeatureSample{{At: now, Value: math.Inf(1)}}
		}, ErrNonFiniteMetric},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cp := base
			tt.mut(&cp)
			err := cp.Validate()
			if !errors.Is(err, tt.want) {
				t.Fatalf("Validate() = %v, want %v", err, tt.want)
			}
		})
	}
}

// TestExplanation_Validate pins the value object contract for the
// detector explainability payload. An empty Explanation is valid
// (consumer absent → field absent). Populated values must be sane:
// ComparablePeers ≥ 0, BaselineWindowDays ≥ 0, FeatureEvolution
// timestamps monotonic non-decreasing.
func TestExplanation_Validate(t *testing.T) {
	now := time.Now()

	t.Run("zero value is valid", func(t *testing.T) {
		if err := (Explanation{}).Validate(); err != nil {
			t.Errorf("zero Explanation rejected: %v", err)
		}
	})

	t.Run("populated explanation is valid", func(t *testing.T) {
		ex := Explanation{
			FeatureEvolution: []FeatureSample{
				{At: now.Add(-2 * time.Hour), Value: 18.0},
				{At: now.Add(-time.Hour), Value: 22.0},
				{At: now, Value: 26.0},
			},
			ComparablePeers:    12,
			BaselineWindowDays: 90,
			ThresholdUsed:      2.5,
			DetectorVersion:    "trend-v2",
		}
		if err := ex.Validate(); err != nil {
			t.Errorf("populated explanation rejected: %v", err)
		}
	})

	t.Run("negative comparable peers rejected", func(t *testing.T) {
		ex := Explanation{ComparablePeers: -1}
		if err := ex.Validate(); !errors.Is(err, ErrInvalidExplanation) {
			t.Errorf("got %v, want ErrInvalidExplanation", err)
		}
	})

	t.Run("negative baseline window rejected", func(t *testing.T) {
		ex := Explanation{BaselineWindowDays: -5}
		if err := ex.Validate(); !errors.Is(err, ErrInvalidExplanation) {
			t.Errorf("got %v, want ErrInvalidExplanation", err)
		}
	})

	t.Run("non-monotonic feature evolution rejected", func(t *testing.T) {
		ex := Explanation{
			FeatureEvolution: []FeatureSample{
				{At: now, Value: 1.0},
				{At: now.Add(-time.Hour), Value: 2.0}, // out of order
			},
		}
		if err := ex.Validate(); !errors.Is(err, ErrInvalidExplanation) {
			t.Errorf("got %v, want ErrInvalidExplanation", err)
		}
	})
}

// TestExplanation_IsZero pins the persistence-layer hook: an
// Explanation with no populated fields reports zero, and any single
// populated field flips it to non-zero. Persistence checks IsZero
// to skip writing empty JSON blobs.
func TestExplanation_IsZero(t *testing.T) {
	if !(Explanation{}).IsZero() {
		t.Error("default Explanation should be IsZero")
	}
	for _, c := range []struct {
		name string
		ex   Explanation
	}{
		{"has feature evolution", Explanation{FeatureEvolution: []FeatureSample{{At: time.Now()}}}},
		{"has comparable peers", Explanation{ComparablePeers: 1}},
		{"has baseline window", Explanation{BaselineWindowDays: 1}},
		{"has threshold", Explanation{ThresholdUsed: 0.1}},
		{"has detector version", Explanation{DetectorVersion: "v1"}},
	} {
		c := c
		t.Run(c.name, func(t *testing.T) {
			if c.ex.IsZero() {
				t.Errorf("non-zero Explanation reported IsZero: %+v", c.ex)
			}
		})
	}
}

func TestSignal_Validate_OutlierClusterAllowsNilSeries(t *testing.T) {
	now := time.Now()
	sig := Signal{
		ID:         uuid.New(),
		ScopeID:    uuid.MustParse("11111111-1111-1111-1111-111111111111"),
		Series:     uuid.Nil,
		Pattern:    PatternTypeOutlierCluster,
		DetectedAt: now,
		Window:     TimeWindow{Start: now.Add(-time.Minute), End: now},
		Strength:   0.4,
		Confidence: 0.7,
	}
	if err := sig.Validate(); err != nil {
		t.Fatalf("cohort-level outlier_cluster rejected: %v", err)
	}
}

func TestTimeWindow_Validate(t *testing.T) {
	now := time.Now()
	if err := (TimeWindow{Start: now, End: now}).Validate(); err != nil {
		t.Errorf("zero-length window rejected: %v", err)
	}
	if err := (TimeWindow{Start: now, End: now.Add(-time.Second)}).Validate(); !errors.Is(err, ErrInvalidWindow) {
		t.Errorf("inverted window not rejected: %v", err)
	}
}

// TestSignal_Validate_NonFiniteMetricsAreUnrepresentable states the
// reason the finiteness invariant exists, next to the invariant, so a
// future reader does not read it as fussiness about float hygiene.
//
// encoding/json refuses non-finite floats and returns no bytes when it
// does. Every SQL store writes Metrics as a JSON column, so one
// unrepresentable key does not cost that key — it costs the map. The
// test asserts both halves: json.Marshal fails on exactly the bag that
// Validate now rejects, and the bytes it hands back are empty.
func TestSignal_Validate_NonFiniteMetricsAreUnrepresentable(t *testing.T) {
	metrics := map[string]float64{"mean": math.Inf(1), "n": 12}

	b, err := json.Marshal(metrics)
	if err == nil {
		t.Fatal("json.Marshal accepted +Inf; the invariant guards a failure that no longer happens")
	}
	if len(b) != 0 {
		t.Errorf("json.Marshal returned %d bytes alongside its error, want 0 — "+
			"a partial encoding would have lost only the bad key, not the map", len(b))
	}

	now := time.Now()
	sig := Signal{
		ID:         uuid.New(),
		ScopeID:    uuid.New(),
		Series:     uuid.New(),
		Pattern:    PatternTypeStall,
		DetectedAt: now,
		Window:     TimeWindow{Start: now.Add(-time.Hour), End: now},
		Strength:   0.9,
		Confidence: 0.8,
		Metrics:    metrics,
	}
	if err := sig.Validate(); !errors.Is(err, ErrNonFiniteMetric) {
		t.Fatalf("Validate() = %v, want ErrNonFiniteMetric", err)
	}
}
