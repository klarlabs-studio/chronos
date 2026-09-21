package detect

import (
	"math"
	"sort"
	"time"

	"github.com/felixgeelhaar/chronos"
)

// AlignedPair is one temporally corresponding observation from each of
// two series. A and B are real observations — alignment never
// interpolates a value that was not recorded.
//
// Time is not a third timestamp. The pair's evidence span is
// [earlier, later] of the two observation timestamps. Callers that need
// a single anchor (regression x-axis) use [AlignedPair.Anchor].
type AlignedPair struct {
	A chronos.EntityState
	B chronos.EntityState
}

// Anchor is the earlier of the two observation timestamps. Equal
// timestamps yield that instant. Used as the regression x-axis so a
// pair has one temporal coordinate.
func (p AlignedPair) Anchor() time.Time {
	if p.B.Timestamp.Before(p.A.Timestamp) {
		return p.B.Timestamp
	}
	return p.A.Timestamp
}

// AlignNearest pairs observations from a and b whose timestamps fall
// within tol of each other. Each observation is used in at most one
// pair. tol == 0 requires exact timestamp equality (no nearest-neighbor
// slack). Negative tol is treated as exact.
//
// Matching is greedy and deterministic:
//
//  1. Both series are sorted by (timestamp, observation ID).
//  2. Each left-hand observation, in that order, claims the unused
//     right-hand observation with the smallest |Δt| that is ≤ tol.
//  3. Equal distances prefer the earlier timestamp, then the smaller
//     observation ID.
//
// Input order and map iteration never affect the result. Values are
// never synthesized between observations.
func AlignNearest(a, b []chronos.EntityState, tol time.Duration) []AlignedPair {
	if tol < 0 {
		tol = 0
	}
	as := append([]chronos.EntityState(nil), a...)
	bs := append([]chronos.EntityState(nil), b...)
	sort.SliceStable(as, func(i, j int) bool { return beforeByTimeThenID(as[i], as[j]) })
	sort.SliceStable(bs, func(i, j int) bool { return beforeByTimeThenID(bs[i], bs[j]) })

	used := make([]bool, len(bs))
	out := make([]AlignedPair, 0, min(len(as), len(bs)))
	for _, left := range as {
		best := -1
		var bestDist time.Duration
		for j, right := range bs {
			if used[j] {
				continue
			}
			d := absDuration(left.Timestamp.Sub(right.Timestamp))
			if d > tol {
				continue
			}
			if best < 0 || d < bestDist || (d == bestDist && beforeByTimeThenID(right, bs[best])) {
				best = j
				bestDist = d
			}
		}
		if best < 0 {
			continue
		}
		used[best] = true
		out = append(out, AlignedPair{A: left, B: bs[best]})
	}
	return out
}

func absDuration(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}

// alignedWindow is the span of observations that actually participated
// in the pairs: earliest timestamp through latest, across both series.
// Observations excluded by alignment do not expand the window.
func alignedWindow(pairs []AlignedPair) (start, end time.Time) {
	start = pairs[0].A.Timestamp
	end = start
	consider := func(t time.Time) {
		if t.Before(start) {
			start = t
		}
		if t.After(end) {
			end = t
		}
	}
	for _, p := range pairs {
		consider(p.A.Timestamp)
		consider(p.B.Timestamp)
	}
	return start, end
}

// pairAnchorHours is hours elapsed since the first pair's anchor.
// Same time basis as Trend's wall-clock regression (outcome or gap
// units per hour). Equal anchors collapse the x-axis.
func pairAnchorHours(pairs []AlignedPair) []float64 {
	xs := make([]float64, len(pairs))
	if len(pairs) == 0 {
		return xs
	}
	start := pairs[0].Anchor()
	for i, p := range pairs {
		xs[i] = p.Anchor().Sub(start).Hours()
	}
	return xs
}

// alignmentQuality is 1 for exact matches (tol == 0, or every pair
// coincident). Otherwise it falls from 1 toward 0 as the mean |Δt|
// approaches the tolerance. It is an evidence-quality term, not a
// similarity score.
func alignmentQuality(pairs []AlignedPair, tol time.Duration) float64 {
	if len(pairs) == 0 {
		return 0
	}
	if tol <= 0 {
		return 1
	}
	var sum float64
	for _, p := range pairs {
		sum += math.Abs(p.A.Timestamp.Sub(p.B.Timestamp).Seconds())
	}
	mean := sum / float64(len(pairs))
	return clamp01(1 - mean/tol.Seconds())
}
