package detect

import (
	"math"
	"sort"

	"github.com/felixgeelhaar/chronos"
	"github.com/google/uuid"
)

// bySeries groups states by entity ID. The Engine sorts by timestamp
// ascending before invoking detectors, so the slices returned here are
// already in chronological order.
//
// Ranging over the returned map directly leaks Go's randomised map
// iteration order into the order of emitted signals; detectors that
// emit one signal per series use [seriesInOrder] instead.
func bySeries(states []chronos.EntityState) map[uuid.UUID][]chronos.EntityState {
	out := make(map[uuid.UUID][]chronos.EntityState)
	for _, s := range states {
		out[s.EntityID] = append(out[s.EntityID], s)
	}
	return out
}

// seriesInOrder groups states by entity ID like [bySeries] and returns
// the entity IDs in ascending lexicographic order alongside the map.
// Detectors iterate the returned ID slice so that repeated runs over
// identical input emit signals in an identical order — the engine's
// MaxSignalsPerRun cap truncates the tail, so ordering decides which
// signals survive.
func seriesInOrder(states []chronos.EntityState) ([]uuid.UUID, map[uuid.UUID][]chronos.EntityState) {
	grouped := bySeries(states)
	return sortedUUIDKeys(grouped), grouped
}

// sortedUUIDKeys returns the keys of m in ascending lexicographic
// order. Detectors keyed on a map of entities use it for the same
// reason as [seriesInOrder]: emitted signal order must not depend on
// Go's randomised map iteration.
func sortedUUIDKeys[V any](m map[uuid.UUID]V) []uuid.UUID {
	ids := make([]uuid.UUID, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i].String() < ids[j].String() })
	return ids
}

// isFinite reports whether x is a real number — neither NaN nor an
// infinity. Detectors use it to reject derived statistics that
// overflowed: every comparison against NaN is false, so an
// unguarded NaN slips through *all* threshold gates and lands in a
// signal's Strength or Confidence, where domain.Signal.Validate also
// accepts it (NaN < 0 and NaN > 1 are both false).
func isFinite(x float64) bool {
	return !math.IsNaN(x) && !math.IsInf(x, 0)
}

// outcomes extracts the outcome metric (last feature) from each
// observation in chronological order.
func outcomes(states []chronos.EntityState) []float64 {
	out := make([]float64, len(states))
	for i, s := range states {
		out[i] = s.Outcome()
	}
	return out
}

// mean returns the arithmetic mean of xs. Returns 0 for an empty slice.
func mean(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	sum := 0.0
	for _, x := range xs {
		sum += x
	}
	return sum / float64(len(xs))
}

// stddev returns the population standard deviation of xs given its
// mean. Returns 0 for an empty slice or all-equal values.
func stddev(xs []float64, m float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	var s float64
	for _, x := range xs {
		d := x - m
		s += d * d
	}
	return math.Sqrt(s / float64(len(xs)))
}

// linearRegression fits y = slope*x + intercept by ordinary least
// squares. r2 is the coefficient of determination (clamped to [0,1]);
// it is 0 when the fit is undefined (constant x or y).
//
// All three results are 0 when the fit is not a real number. Inputs
// near math.MaxFloat64 overflow the running sums to ±Inf, and the
// resulting Inf/Inf divisions yield NaN — an undefined fit, not a
// fit of unknown quality.
func linearRegression(xs, ys []float64) (slope, intercept, r2 float64) {
	n := float64(len(xs))
	if n < 2 {
		return 0, 0, 0
	}
	var sumX, sumY, sumXY, sumXX, sumYY float64
	for i := range xs {
		sumX += xs[i]
		sumY += ys[i]
		sumXY += xs[i] * ys[i]
		sumXX += xs[i] * xs[i]
		sumYY += ys[i] * ys[i]
	}
	denX := n*sumXX - sumX*sumX
	if denX == 0 || !isFinite(denX) {
		return 0, 0, 0
	}
	slope = (n*sumXY - sumX*sumY) / denX
	intercept = (sumY - slope*sumX) / n
	if !isFinite(slope) || !isFinite(intercept) {
		return 0, 0, 0
	}
	denY := n*sumYY - sumY*sumY
	if denY <= 0 || !isFinite(denY) {
		return slope, intercept, 0
	}
	num := n*sumXY - sumX*sumY
	r2 = (num * num) / (denX * denY)
	switch {
	case !isFinite(r2):
		r2 = 0
	case r2 < 0:
		r2 = 0
	case r2 > 1:
		r2 = 1
	}
	return slope, intercept, r2
}

// clamp01 squashes x into [0, 1].
func clamp01(x float64) float64 {
	if x < 0 {
		return 0
	}
	if x > 1 {
		return 1
	}
	return x
}

// pearsonCorrelation returns the Pearson correlation coefficient
// between xs and ys, or 0 when either series is degenerate (length
// mismatch, fewer than two points, zero variance, or sums that
// overflow to ±Inf and leave the coefficient undefined).
func pearsonCorrelation(xs, ys []float64) float64 {
	if len(xs) != len(ys) || len(xs) < 2 {
		return 0
	}
	n := float64(len(xs))
	var sumX, sumY, sumXY, sumXX, sumYY float64
	for i := range xs {
		sumX += xs[i]
		sumY += ys[i]
		sumXY += xs[i] * ys[i]
		sumXX += xs[i] * xs[i]
		sumYY += ys[i] * ys[i]
	}
	num := n*sumXY - sumX*sumY
	denX := n*sumXX - sumX*sumX
	denY := n*sumYY - sumY*sumY
	if denX <= 0 || denY <= 0 || !isFinite(denX) || !isFinite(denY) {
		return 0
	}
	r := num / math.Sqrt(denX*denY)
	if !isFinite(r) {
		return 0
	}
	return r
}

// autocorrelation returns the Pearson correlation between a series and
// itself shifted by lag observations. Lags must be in [1, len(ys)-1].
func autocorrelation(ys []float64, lag int) float64 {
	if lag < 1 || lag >= len(ys) {
		return 0
	}
	n := len(ys) - lag
	return pearsonCorrelation(ys[:n], ys[lag:lag+n])
}
