// Package similarity provides generic similarity computation for feature vectors.
package similarity

import "math"

// Cosine computes the cosine similarity between two vectors.
// Returns a value in [-1, 1]. For pattern detection, values near 1.0 indicate high similarity.
//
// The range is enforced, not merely intended. Two identical vectors
// divide out to 1+2ulp in binary floating point, and vectors near
// math.MaxFloat64 square to +Inf in both the dot product and the
// norms, leaving Inf/Inf = NaN. Callers threshold on the result, and
// every comparison against NaN is false, so an out-of-range value
// propagates into a signal's Strength. A result that is not a real
// number is reported as 0 — no evidence of similarity — and every
// other result is clamped to [-1, 1].
func Cosine(a, b []float64) float64 {
	if len(a) != len(b) || len(a) == 0 {
		return 0
	}

	dot := 0.0
	normA := 0.0
	normB := 0.0

	for i := range a {
		dot += a[i] * b[i]
		normA += a[i] * a[i]
		normB += b[i] * b[i]
	}

	if normA == 0 || normB == 0 {
		return 0
	}

	return clampUnit(dot / (math.Sqrt(normA) * math.Sqrt(normB)))
}

// clampUnit squashes x into [-1, 1], mapping NaN to 0.
func clampUnit(x float64) float64 {
	switch {
	case math.IsNaN(x):
		return 0
	case x > 1:
		return 1
	case x < -1:
		return -1
	default:
		return x
	}
}

// WeightedCosine computes cosine similarity with per-dimension weights.
func WeightedCosine(a, b, weights []float64) float64 {
	if len(a) != len(b) || len(a) != len(weights) || len(a) == 0 {
		return 0
	}

	dot := 0.0
	normA := 0.0
	normB := 0.0

	for i := range a {
		wa := a[i] * weights[i]
		wb := b[i] * weights[i]
		dot += wa * wb
		normA += wa * wa
		normB += wb * wb
	}

	if normA == 0 || normB == 0 {
		return 0
	}

	return dot / (math.Sqrt(normA) * math.Sqrt(normB))
}

// Euclidean computes the Euclidean distance between two vectors.
func Euclidean(a, b []float64) float64 {
	if len(a) != len(b) {
		return math.Inf(1)
	}

	sum := 0.0
	for i := range a {
		d := a[i] - b[i]
		sum += d * d
	}

	return math.Sqrt(sum)
}
