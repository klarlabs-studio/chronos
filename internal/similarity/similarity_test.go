package similarity

import (
	"math"
	"testing"
)

func TestCosine(t *testing.T) {
	tests := []struct {
		name     string
		a        []float64
		b        []float64
		expected float64
	}{
		{
			name:     "identical vectors",
			a:        []float64{1, 2, 3},
			b:        []float64{1, 2, 3},
			expected: 1.0,
		},
		{
			name:     "orthogonal vectors",
			a:        []float64{1, 0, 0},
			b:        []float64{0, 1, 0},
			expected: 0.0,
		},
		{
			name:     "opposite vectors",
			a:        []float64{1, 2, 3},
			b:        []float64{-1, -2, -3},
			expected: -1.0,
		},
		{
			name:     "similar vectors",
			a:        []float64{1, 2, 3},
			b:        []float64{2, 3, 4},
			expected: 0.992583, // approximately
		},
		{
			name:     "different lengths",
			a:        []float64{1, 2},
			b:        []float64{1, 2, 3},
			expected: 0.0,
		},
		{
			name:     "empty vectors",
			a:        []float64{},
			b:        []float64{},
			expected: 0.0,
		},
		{
			name:     "zero vector",
			a:        []float64{0, 0, 0},
			b:        []float64{1, 2, 3},
			expected: 0.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Cosine(tt.a, tt.b)
			diff := math.Abs(got - tt.expected)
			if diff > 0.0001 {
				t.Errorf("Cosine() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestWeightedCosine(t *testing.T) {
	a := []float64{1, 2, 3}
	b := []float64{2, 3, 4}
	weights := []float64{1, 1, 1}

	// With equal weights, should equal regular cosine
	weighted := WeightedCosine(a, b, weights)
	regular := Cosine(a, b)

	if math.Abs(weighted-regular) > 0.0001 {
		t.Errorf("WeightedCosine with equal weights should equal Cosine: got %v, want %v", weighted, regular)
	}

	// With different weights
	weights2 := []float64{2, 1, 0.5}
	weighted2 := WeightedCosine(a, b, weights2)
	if weighted2 == regular {
		t.Error("WeightedCosine with different weights should differ from regular Cosine")
	}
}

// TestCosine_StaysInRangeUnderNumericExtremes pins the [-1, 1]
// contract at the boundaries where the arithmetic leaves it.
// Callers threshold on the result and feed it straight into a
// signal's Strength, where domain.Signal.Validate rejects anything
// outside [0, 1] — but accepts NaN, because NaN fails both of its
// comparisons.
func TestCosine_StaysInRangeUnderNumericExtremes(t *testing.T) {
	const huge = math.MaxFloat64
	const denormal = 5e-324

	tests := []struct {
		name string
		a, b []float64
		want float64
		why  string
	}{
		{"identical vectors", []float64{1, 2, 3}, []float64{1, 2, 3}, 1,
			"the unclamped quotient is 1.0000000000000002, two ulp above the contract"},
		{"identical unit vectors", []float64{1, 1, 1}, []float64{1, 1, 1}, 1,
			"same rounding, different magnitude"},
		{"opposite vectors", []float64{1, 2, 3}, []float64{-1, -2, -3}, -1,
			"the negative end of the range rounds the same way"},
		{"both at MaxFloat64", []float64{huge, huge, huge}, []float64{huge, huge, huge}, 0,
			"the dot product and both norms overflow to +Inf, leaving Inf/Inf = NaN"},
		{"one at MaxFloat64", []float64{huge, huge, huge}, []float64{1, 1, 1}, 0,
			"the two vectors point in exactly the same direction, but the dot product and one norm both overflow to +Inf, so the identical direction is reported as no similarity at all"},
		{"both subnormal", []float64{denormal, denormal}, []float64{denormal, denormal}, 0,
			"squaring 5e-324 underflows to zero, so both norms are zero"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Cosine(tt.a, tt.b)
			if math.IsNaN(got) || math.IsInf(got, 0) {
				t.Fatalf("Cosine() = %v, want a real number (%s)", got, tt.why)
			}
			if got < -1 || got > 1 {
				t.Fatalf("Cosine() = %.20f, outside [-1, 1] (%s)", got, tt.why)
			}
			if got != tt.want {
				t.Errorf("Cosine() = %.20f, want exactly %v (%s)", got, tt.want, tt.why)
			}
		})
	}
}

func TestEuclidean(t *testing.T) {
	a := []float64{1, 2, 3}
	b := []float64{4, 5, 6}

	got := Euclidean(a, b)
	expected := math.Sqrt(27) // sqrt((3)^2 + (3)^2 + (3)^2)

	if math.Abs(got-expected) > 0.0001 {
		t.Errorf("Euclidean() = %v, want %v", got, expected)
	}
}
