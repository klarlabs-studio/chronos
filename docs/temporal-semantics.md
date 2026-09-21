# Temporal semantics

Chronos perceives patterns in time. A detector that claims a relationship, a rate, or a period must use timestamps as data, not as labels attached after an ordinal calculation.

Observation order is not a substitute for temporal alignment.

This document is the taxonomy later detectors should follow. The wire keys are in [`wire-contract.md`](wire-contract.md). Ordering, duplicates, and irregular series at the store boundary are in [`temporal-contract.md`](temporal-contract.md).

## Four kinds of pattern

| Kind | Question | Time enters as | Detectors |
|---|---|---|---|
| Ordinal | What does the sequence of recorded values do? | Sort order only | Oscillation |
| Temporal-rate | How fast is a quantity changing on the clock? | Elapsed wall-clock time | Trend, Divergence, Convergence |
| Temporally relational | Did these series move together *when they were observed together*? | Aligned timestamps | Correlation, CrossScopeCorrelation, Divergence, Convergence |
| Temporal-periodic | Does the series repeat on a clock? | A regular sampling cadence | Seasonality |

Divergence and Convergence are both temporal-rate and temporally relational: the gap is only defined at aligned times, and the slope of that gap is per hour.

Slice position is valid evidence only for an ordinal pattern. Using it for any of the other three is a detector bug.

## Alignment

Pairwise detectors share one implementation, `detect.AlignNearest`.

An aligned pair is two real observations. Alignment never interpolates a value that was not recorded. Insufficient overlap is no signal.

Modes, selected by `CHRONOS_ALIGN_TOLERANCE`:

- **Exact** (default, tolerance `0`). `A @ t` pairs only with `B @ t`.
- **Nearest-within**. An observation may match the nearest unused observation on the other series when `abs(tA − tB) ≤ tolerance`.

Each observation is used in at most one pair. Matching is greedy after both series are sorted by `(timestamp, observation ID)`:

1. Walk the left series in that order.
2. Claim the unused right-hand observation with the smallest `|Δt|` that is still inside the tolerance.
3. Equal distances prefer the earlier timestamp, then the smaller observation ID.

Input order and map iteration do not affect the result. A negative tolerance is treated as exact.

The minimum sample count of a pairwise detector is a count of aligned pairs. One hundred observations on each series and two pairs is two relational observations.

## Signal windows

`Signal.Window` is the span of observations that support that signal.

For a pairwise signal that is `Start` = earliest timestamp among the aligned observations and `End` = latest. Points excluded by alignment do not widen the window. The union of the two input histories is not the evidence.

## Rates

Trend regresses outcome against hours since the first observation in its window. Slope is outcome units per hour. That is the reference convention for rate-like metrics.

Divergence and Convergence regress `gap(t) = |A(t) − B(t)|` against hours since the first aligned pair's anchor (the earlier timestamp of that pair). Slope is gap units per hour. Sampling a process more often does not make it diverge faster.

Equal timestamps collapse the x-axis. Trend, Divergence, and Convergence then emit no signal.

## Fit is part of the claim

A regression coefficient with a sign is not, by itself, sustained structure. Divergence requires `slope ≥ CHRONOS_DIVERGENCE_MIN_SLOPE` and `R² ≥ CHRONOS_DIVERGENCE_MIN_R2` (default `0.5`). Convergence is the mirror (`slope ≤ −minSlope`, same R² shape, its own knob defaulting to `0.5`).

`0.5` is not Trend's `0.3`. Trend's floor is for a single series' outcome. A pairwise gap is a derived series; half the variance left unexplained is not yet a sustained divergence or convergence. `0` disables the fit gate.

Strength is the shape: slope magnitude, saturating at twice the slope floor, discounted by R². Confidence is the evidence: aligned sample size and how tight the alignment was. A large slope on three pairs can be strong and still tentative.

## Seasonality

Seasonality claims a period in wall-clock time. Ordinal autocorrelation is computed only when successive intervals are positive and their coefficient of variation is at most `CHRONOS_SEASONALITY_MAX_INTERVAL_CV`. The default `0` accepts only equal spacing. Duplicate timestamps, reversed time, and chaotic spacing emit no signal.

There is no resampling. A future explicit grid would have to say how missing samples are filled and would have to mark synthetic points as synthetic. Until then, Chronos prefers no seasonality signal over a period invented by interpolation.

Evidence keeps `period` as the lag in samples and also reports `period_samples`, `period_seconds` (lag × median interval), and `sampling_interval_seconds`.

## Oscillation stays ordinal

Oscillation asks whether successive observations reverse direction. It does not ask how many reversals occurred per hour. Two series with the same values in the same order produce the same oscillation result even when their timestamps differ. That is intentional.

A later oscillation-frequency detector would be a new pattern, not a silent change to this one.

## What this change drops

Inputs that used to emit a signal because two slices shared an index, or because a repeating sequence sat on an irregular clock, may now emit nothing. That is a correctness improvement. The detector version tags are `correlation-v2`, `cross_scope_correlation-v2`, `divergence-v2`, `convergence-v2`, and `seasonality-v2`. Trend remains `trend-v2`. Oscillation remains `oscillation-v1`.
