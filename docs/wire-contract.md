# Wire contract

This document is the authoritative list of strings consumers may rely on when reading Chronos signals: the `Pattern` enum values, every `Evidence.Kind` a detector emits, and every key that may appear in `Signal.Metrics` or `Evidence.Metrics`. Renaming any of these is a breaking change.

The wire shape itself (field names, JSON tags, types) is in `client/types.go` and `internal/api/dto.go`; this document covers only the string-valued fields whose stability matters to consumers that branch on them.

## Transport parity (HTTP and gRPC)

The same domain shape ships over both transports. Evidence.Kind strings and metric keys are identical regardless of transport. The gRPC schema lives at [`api/proto/chronos/v1/chronos.proto`](../api/proto/chronos/v1/chronos.proto). Field-name conventions across the two:

- HTTP returns JSON shaped by `internal/api/dto.go` with `snake_case` keys (e.g. `"pattern": "recurrence"`).
- gRPC returns proto messages shaped by `chronos/v1/chronos.proto`. The `pattern` field is a typed `PatternType` enum (`PATTERN_TYPE_RECURRENCE`, `PATTERN_TYPE_TREND`, ...) — the wire integer is what travels, but generated clients expose the named constants. Consumers should switch on the typed enum, not the raw integer.
- Mapping HTTP string ↔ gRPC enum is one-to-one; conversion lives in `internal/api/grpc/convert.go`.
- Metric keys (`avg_similarity`, `slope`, `z_score`, ...) appear in `map<string, double>` fields in proto and `Record<string, number>` in HTTP JSON; the keys themselves are identical.

Adding a new transport without updating this document is a contract bug.

gRPC RPCs match the HTTP surface additively: unary `Ingest` + `IngestBatch`, `ListSignals` (including `since_cursor` / `next_cursor`), `GetSignal`, server-streaming `StreamSignals` (frames are `StreamSignalsResponse`, not bare `Signal`, so the type stays unique from `GetSignal`), `ValidateConfig`, and `ExportFederation`. Unary `Ingest` was not changed to client-streaming.

## Explanation

`Signal.Explanation` is numeric/structured context so downstream narrators can say *why* a detector fired without Chronos emitting prose. Detectors populate it; empty/`omit` means the detector did not surface one (legacy rows). Fields:

| JSON key | Meaning |
|---|---|
| `feature_evolution` | Time-ordered `{at, value}` samples of the outcome the detector inspected. Omitted for cohort-level `outlier_cluster`. |
| `comparable_peers` | Peer count considered. `0` / omitted = not a peer detector. |
| `baseline_window_days` | Reserved; Chronos windows are observation counts, so this stays `0`. |
| `threshold_used` | The configured cutoff the detector compared against. |
| `detector_version` | Stable tag. Bump the suffix when math or evidence shape changes. |

Current `detector_version` values: `recurrence-v2`, `trend-v2`, `spike-v2`, `drop-v2`, `stall-v1`, `anomaly-v1`, `seasonality-v2`, `correlation-v2`, `changepoint-v1`, `outlier_cluster-v1`, `cross_scope_correlation-v2`, `oscillation-v1`, `divergence-v2`, `convergence-v2`.

Which detectors treat time as order, as a rate, as a join, or as a period is in [`temporal-semantics.md`](temporal-semantics.md).

## Pattern enum

`Signal.Pattern` is one of:

| Value           | Constant                          | Detector       |
|-----------------|-----------------------------------|----------------|
| `recurrence`    | `client.PatternTypeRecurrence`    | Recurrence     |
| `trend`         | `client.PatternTypeTrend`         | Trend          |
| `spike`         | `client.PatternTypeSpike`         | Spike          |
| `drop`          | `client.PatternTypeDrop`          | Drop           |
| `stall`         | `client.PatternTypeStall`         | Stall          |
| `anomaly`       | `client.PatternTypeAnomaly`       | Anomaly        |
| `seasonality`   | `client.PatternTypeSeasonality`   | Seasonality    |
| `correlation`   | `client.PatternTypeCorrelation`   | Correlation    |
| `change_point`  | `client.PatternTypeChangePoint`   | ChangePoint    |
| `outlier_cluster` | `client.PatternTypeOutlierCluster` | OutlierCluster |
| `cross_scope_correlation` | `client.PatternTypeCrossScopeCorrelation` | CrossScopeCorrelation |
| `oscillation`   | `client.PatternTypeOscillation`   | Oscillation    |
| `divergence`    | `client.PatternTypeDivergence`    | Divergence     |
| `convergence`   | `client.PatternTypeConvergence`   | Convergence    |

Consumers should switch on the `client.PatternType*` constants. New patterns will be added with new string values; consumers using a closed switch on a typed enum will surface unknown patterns naturally.

## Evidence kinds and metric keys per detector

Each detector emits a stable `Evidence.Kind` (single string) and a stable set of keys in `Signal.Metrics` and `Evidence.Metrics`. Future evolutions add keys; renames or removals are breaking changes.

Every numeric value a signal carries — `Strength`, `Confidence`, `Evidence.Score`, and each value in `Signal.Metrics` and `Evidence.Metrics` — is a finite JSON number. A detector whose arithmetic overflows to `NaN` or an infinity emits no signal rather than an unrepresentable one, so a consumer never sees `null`, a string sentinel, or an empty metric bag standing in for a value that could not be encoded.

## Strength and Confidence (all detectors)

Strength and Confidence are distinct (Intent §7):

- **Strength** — magnitude of the observed pattern in `[0, 1]`.
- **Confidence** — quality / certainty of the evidence supporting the detection in `[0, 1]`.
- **ConfidenceClass** — qualitative grade (`tentative` / `established` / `strong`) from sample size vs each detector's `MIN_POINTS` and the `CHRONOS_CONFIDENCE_*` multipliers.

Unless noted otherwise, Confidence is `strength × sampleFactor(n, saturate)`, where `sampleFactor` ramps linearly to 1.0 at `saturate` supporting samples. Spike/Drop are the exception: they use an evidence-quality product that is intentionally flat in `|z|` past a margin.

| Pattern | Strength | Confidence | ConfidenceClass sample basis |
|---|---|---|---|
| `recurrence` | mean peer cosine similarity | `strength × sampleFactor(peers, 5)` | peer count vs `MinSampleSize` |
| `trend` | R² of wall-clock OLS | `strength × sampleFactor(n, 2×TrendMinPoints)` | `n` vs `TrendMinPoints` |
| `spike` / `drop` | `min(\|z\|/5, 1)` | `support × quietness × margin` (see below) | `n` vs `SpikeWindow+1` |
| `stall` | `1 − normalised_stddev / StallMaxStdDev` | `strength × sampleFactor(n, 2×StallMinPoints)` | `n` vs `StallMinPoints` |
| `anomaly` | `1 − max_peer_similarity` | `strength × sampleFactor(peers, 5)` | peer count vs `AnomalyMinPeers` |
| `seasonality` | peak autocorrelation, only after a regular cadence check | `strength × sampleFactor(n, 2×SeasonalityMinPoints)` | `n` vs `SeasonalityMinPoints` |
| `correlation` | `\|r\|` on temporally aligned pairs | `strength × sampleFactor(n, 2×CorrelationMinPoints)` | aligned `n` vs `CorrelationMinPoints` (≥ 3) |
| `change_point` | scaled standardised shift (Inf → 1.0) | `strength × sampleFactor(n, 2×ChangePointMinPoints)` | `n` vs `ChangePointMinPoints` |
| `outlier_cluster` | how far `member_count` exceeds the floor | `strength × sampleFactor(members, 2×OutlierClusterMinSeries)` | member count vs `OutlierClusterMinSeries` |
| `cross_scope_correlation` | `\|r\|` | `\|r\| × sampleFactor(n, 2×CrossScopeMinPoints)` | aligned `n` vs `CrossScopeMinPoints` (≥ 3) |
| `oscillation` | sign-flip rate among meaningful first-differences | `strength × sampleFactor(n, 2×OscillationMinPoints)` | `n` vs `OscillationMinPoints` |
| `divergence` | scaled positive gap slope (per hour) × R² | `sampleFactor(aligned n, 2×DivergenceMinPoints) × alignment quality` — not × strength | aligned `n` vs `DivergenceMinPoints` (≥ 3) |
| `convergence` | scaled \|negative\| gap slope (per hour) × R² | `sampleFactor(aligned n, 2×ConvergenceMinPoints) × alignment quality` — not × strength | aligned `n` vs `ConvergenceMinPoints` (≥ 3) |

**Trend axis.** Trend regresses outcome against **wall-clock hours since window start** (`trend-v2`). Slope units are outcome-units per hour. This is the reference convention for rate-like detectors. Irregular sampling therefore changes the fitted slope (and typically R²) relative to a regularly spaced series with the same outcome values. Equal timestamps collapse the x-axis and yield no signal. Do not read Trend's slope as a per-sample increment.

**Anomaly zero vectors.** Subjects or peers with zero L2 norm are skipped: cosine similarity against a directionless vector is undefined, not “maximally isolated”.

## Evidence kinds and metric keys per detector

Each detector emits a stable `Evidence.Kind` (single string) and a stable set of keys in `Signal.Metrics` and `Evidence.Metrics`. Future evolutions add keys; renames or removals are breaking changes.

### Recurrence — `Pattern: "recurrence"`

- **Evidence.Kind**: `similar_state` — one per peer state above the similarity threshold.
- **Evidence.Score**: cosine similarity to the peer (`[0, 1]`).
- **Evidence.Metrics**:
  - `outcome_diff` — `peer.outcome - subject.outcome`. Higher means the peer's outcome was better than the subject's at that observation.
- **Signal.Metrics**:
  - `avg_similarity` — mean of evidence scores.
  - `sample_size` — number of peer cases.
  - `avg_outcome_diff` — mean of evidence `outcome_diff`.
- **Strength / Confidence**: see table above.

### Trend — `Pattern: "trend"`

- **Evidence.Kind**: `regression_summary` — exactly one per signal.
- **Evidence.Score**: R² of the regression.
- **Evidence.Metrics** *(equal to Signal.Metrics)*:
  - `slope` — OLS slope of outcome vs. wall-clock hours since window start (outcome units per hour).
  - `intercept` — OLS intercept.
  - `r2` — coefficient of determination.
  - `n` — number of observations in the window.
- **Strength / Confidence**: see table above.

### Spike / Drop — `Pattern: "spike" | "drop"`

Spike and Drop share the same evidence shape; sign of `z` distinguishes them.

- **Evidence.Kind**: `baseline_deviation` — exactly one per signal.
- **Evidence.Score**: `|z|`.
- **Evidence.Metrics**:
  - `z` — z-score of the latest outcome against the rolling baseline (signed).
  - `baseline_mean` — mean of the previous `SpikeWindow` outcomes.
  - `baseline_stddev` — sample stddev of the baseline.
- **Signal.Metrics** *(superset of evidence)*:
  - `z`, `baseline_mean`, `baseline_stddev` — same as evidence.
  - `observed_outcome` — the latest outcome value.
  - `window` — `SpikeWindow` size (number of baseline points).
- **Confidence**: quality of the evidence, not the size of the deviation. `support × quietness × margin` — see [`architecture.md`](architecture.md) for the terms. It is flat in `|z|` once the deviation is 25% past the trigger threshold, so consumers ranking by *how big* a spike was must read `strength` (or `z`), not `confidence`. Emitted values changed in `spike-v2` / `drop-v2`; the same input produces a lower number than it did under `spike-v1` / `drop-v1`, where confidence was a copy of strength.

### Stall — `Pattern: "stall"`

- **Evidence.Kind**: `variance_window` — exactly one per signal.
- **Evidence.Score**: normalised stddev.
- **Evidence.Metrics** *(equal to Signal.Metrics)*:
  - `normalised_stddev` — stddev divided by a non-zero baseline (first value, or mean if first is zero).
  - `mean` — mean outcome over the window.
  - `n` — number of observations.

### Anomaly — `Pattern: "anomaly"`

- **Evidence.Kind**: `peer_distance` — one per peer (sorted similarity-descending so `evidence[0]` is the closest peer).
- **Evidence.Score**: cosine similarity to the peer (`[0, 1]`).
- **Evidence.Metrics**: empty.
- **Signal.Metrics**:
  - `max_peer_similarity` — highest similarity to any peer (the subject is isolated when this is below `AnomalyMaxSimilarity`).
  - `peer_count` — number of peers compared.
- **Window invariant**: `Window.Start == Window.End == subject.Timestamp`. Anomaly is a snapshot, not an interval; consumers computing duration must special-case this.

### Seasonality — `Pattern: "seasonality"`

Autocorrelation is interpreted as a wall-clock period only when the sampling cadence is regular (`seasonality-v2`). Inter-observation intervals must be positive and their coefficient of variation must be at most `CHRONOS_SEASONALITY_MAX_INTERVAL_CV` (default `0`, exact spacing). Irregular, duplicate, or reversed timestamps emit no signal. There is no resampling.

- **Evidence.Kind**: `autocorrelation_peak` — exactly one per signal.
- **Evidence.Score**: autocorrelation value at the peak lag.
- **Evidence.Metrics** *(equal to Signal.Metrics)*:
  - `period` — lag at which autocorrelation peaks, **in samples**. Unchanged from `seasonality-v1`. Multiply by `sampling_interval_seconds` (or read `period_seconds`) for the clock period. Do not treat this number as seconds.
  - `period_samples` — same value as `period`, named so the unit is explicit.
  - `period_seconds` — `period_samples × sampling_interval_seconds` (median positive interval).
  - `sampling_interval_seconds` — median inter-observation interval, in seconds.
  - `autocorrelation` — the peak Pearson autocorrelation.
  - `n` — number of observations.

### Correlation — `Pattern: "correlation"`

Pearson `r` is computed on temporally aligned pairs (`correlation-v2`), not on slice indexes. `CHRONOS_ALIGN_TOLERANCE` is `0` by default (timestamps must be equal). `n` counts aligned pairs. A morning series and an afternoon series with the same values produce no signal when no pair falls inside the tolerance.

One signal per pair, deterministically owned by the lex-smaller series ID; the partner appears in evidence. The window runs from the earliest aligned timestamp to the latest, not across observations that were excluded.

- **Evidence.Kind**: `pair_correlation` — exactly one per signal, pointing at the partner series.
- **Evidence.Score**: `|r|`.
- **Evidence.Metrics** *(equal to Signal.Metrics)*:
  - `r` — signed Pearson correlation of the aligned outcomes.
  - `abs_r` — `|r|`.
  - `n` — number of aligned pairs (not the raw length of either series).
  - `aligned_samples` — same count as `n`, named so consumers do not read it as raw history length.
  - `alignment_tolerance_seconds` — the tolerance that was in force (`0` = exact).
  - `direction` — `+1` for positive `r`, `-1` for negative, `0` for zero.

### ChangePoint — `Pattern: "change_point"`

Detects a step change in the mean of the outcome metric — a sustained shift between two regimes (distinct from Spike/Drop, which are short-lived deviations).

- **Evidence.Kind**: two evidence rows per signal, in this order: `regime_before` and `regime_after`. Both carry `mean`, `stddev`, `n`.
- **Evidence.Score**: regime mean (so consumers can read the before / after means without joining metrics).
- **Signal.Metrics**:
  - `shift` — `|mean_before − mean_after| / pooled_stddev` (always positive).
    Note this is standardised by the series' own variability, so a series
    that barely moves yields a large shift for a small change. Pair with
    `CHRONOS_CHANGEPOINT_MIN_DELTA` when the outcome is bounded and small
    changes are not actionable.
  - `split_index` — index of the first observation in the post-change regime (0-based).
  - `mean_before`, `mean_after` — the two regime means.
  - `delta_mean` — signed change (`mean_after − mean_before`).
  - `n_before`, `n_after` — observation counts on each side.

### OutlierCluster — `Pattern: "outlier_cluster"`

Cohort-level signal: multiple series in the same scope went anomalous around the same time.

- **Series**: `uuid.Nil` (cohort-level, not entity-level).
- **Evidence.Kind**: `outlier_member` — one row per participating series, sorted by series ID for deterministic ordering.
- **Evidence.Score**: peak |z| of that series within the cluster window.
- **Evidence.Metrics**: `peak_z`.
- **Signal.Metrics**:
  - `member_count` — number of distinct series in the cluster.
  - `window_seconds` — cluster bucket width (`CHRONOS_OUTLIER_CLUSTER_WINDOW`).

### CrossScopeCorrelation — `Pattern: "cross_scope_correlation"`

Two series in DIFFERENT scopes that move together at corresponding times. Same alignment contract as `correlation` (`cross_scope_correlation-v2`): scope boundaries do not relax the requirement that the observations were contemporaneous. Same-scope pairs are handled by `correlation`.

- **ScopeID**: lex-smaller of the two participating scopes.
- **Series**: lex-smaller series within the chosen scope.
- **Evidence.Kind**: `cross_scope_pair` — exactly one row, pointing at the partner series.
- **Evidence.Score**: `|r|`.
- **Signal.Metrics**: `r`, `abs_r`, `n`, `aligned_samples`, `alignment_tolerance_seconds`, `direction`. The window spans the aligned pairs only.
- **Evidence.Metrics**: `partner_scope_id_lex_max`, `r`, `n`, `aligned_samples`, `alignment_tolerance_seconds`.

### Oscillation — `Pattern: "oscillation"`

Repeated direction reversals across successive observations. This is an **ordinal** pattern (`oscillation-v1`): identical value order yields the same result at any timestamp spacing. It is not a frequency. Seasonality is the detector that claims a clock period, and only when cadence is regular.

- **Evidence.Kind**: `sign_flip_rate` — one record for the analysis window.
- **Evidence.Score**: flip rate in `[0, 1]`.
- **Signal.Metrics** / **Evidence.Metrics**:
  - `flip_rate` — flips / consecutive meaningful first-difference pairs.
  - `flips` — count of sign changes.
  - `pairs` — number of consecutive meaningful difference pairs considered.
  - `n` — observation count.

### Divergence — `Pattern: "divergence"`

Two series in the same scope whose absolute outcome gap is growing over wall-clock time (`divergence-v2`).

Pipeline: align the series (`CHRONOS_ALIGN_TOLERANCE`), form `gap = |A − B|` on each pair, regress that gap against **hours since the first pair's anchor**. Emit only when `slope ≥ CHRONOS_DIVERGENCE_MIN_SLOPE` (gap units per hour) and `R² ≥ CHRONOS_DIVERGENCE_MIN_R2` (default `0.5`). A directional but erratic gap emits nothing. The window covers only the aligned observations.

**Unit change from `divergence-v1`.** `slope`, `abs_slope`, and Evidence.Score were per ordinal step. They are now gap units per hour, the same time basis as Trend. `slope_per_hour` carries that value under an explicit name (equal to `slope`). The per-step number is not preserved: it was the wrong quantity, and sampling faster must not inflate it. Consumers comparing `slope` to a per-step threshold must retune. The `detector_version` bump is the signal.

- **Series**: lex-smaller entity ID.
- **Evidence.Kind**: `pair_divergence` — one row pointing at the partner.
- **Evidence.Score**: `|slope|` in gap units per hour (finite; not required to lie in `[0, 1]`).
- **Signal.Metrics** / **Evidence.Metrics**:
  - `slope` — signed OLS slope of the absolute gap, gap units per hour (positive).
  - `slope_per_hour` — same value as `slope`.
  - `abs_slope` — `|slope|`.
  - `r2` — coefficient of determination of the gap fit.
  - `r_squared` — same value as `r2`.
  - `start_gap`, `end_gap` — absolute gap at the first and last aligned pair in pair order.
  - `n` — aligned pair count.
  - `aligned_samples` — same count as `n`.
  - `alignment_tolerance_seconds` — tolerance in force.
- **Strength**: `clamp01(clamp01(|slope| / (2 × minSlope)) × R²)`. Shape, not sample size.
- **Confidence**: `sampleFactor(n, 2 × minPoints) × alignmentQuality`. Not multiplied by strength. Exact alignment has quality 1; nearest-within quality falls as the mean `|Δt|` approaches the tolerance.

### Convergence — `Pattern: "convergence"`

Mirror of Divergence (`convergence-v2`): absolute outcome gap is shrinking. `slope` is negative and must be `≤ −CHRONOS_CONVERGENCE_MIN_SLOPE`, with `R² ≥ CHRONOS_CONVERGENCE_MIN_R2` (default `0.5`). Same unit change as Divergence: `slope` is gap units per hour, not per step.

- **Series**: lex-smaller entity ID.
- **Evidence.Kind**: `pair_convergence` — one row pointing at the partner.
- **Evidence.Score**: `|slope|` in gap units per hour.
- **Signal.Metrics** / **Evidence.Metrics**: same keys as Divergence (`slope` is negative).
- **Strength / Confidence**: same formulas as Divergence, using the convergence knobs.

## Sort order

`SignalRepository.List` and the HTTP `/v1/signals` endpoint return signals sorted by `detected_at` descending, then `confidence` descending. Within a single compute run the engine emits in the same order; persistence preserves it.

## Push transports

Webhook bodies and SSE event payloads use the **same JSON shape** as `/v1/signals` responses (`SignalDTO`). Anything described above applies to push consumers identically.

Webhook headers carry the only push-specific contract:

| Header | Stable? | Meaning |
|---|---|---|
| `X-Chronos-Event` | yes — currently always `signal.detected` | event kind; future versions may add `signal.batch` etc. Switch defensively. |
| `X-Chronos-Delivery` | yes | UUID v4 unique per send attempt; idempotency key for retries. |
| `X-Chronos-Signature` | yes — `sha256=<hex>` | HMAC-SHA256 of the raw body keyed on `CHRONOS_WEBHOOK_SECRET`. Absent when no secret is configured. |

SSE frames use the SSE event name `signal`, an `id:` line carrying the `Signal.ID` UUID, and a `data:` line containing `SignalDTO` JSON. The endpoint sends an initial `: connected` comment line for connection-readiness signalling.

**Replay.** Clients may resume after a disconnect by re-issuing the request with the standard `Last-Event-ID` HTTP header set to the last `Signal.ID` they received. Environments where browsers strip the header (some service-worker setups) can use the `?last_event_id=<uuid>` query parameter as a fallback. On reconnect the server queries the persistence layer for signals detected at or after the cursor's `detected_at` (filtered by the same `scope_id` and `pattern`) and emits them before continuing with the live stream. The cursor signal itself is never re-emitted. If the cursor ID is unknown (the row was deleted, the client reconnected to a different deployment), replay is skipped and the live stream begins as if no header was set.

## Stability policy

- **Adding a new key** to `Signal.Metrics` or `Evidence.Metrics` is non-breaking. Consumers must tolerate unknown keys.
- **Adding a new `Pattern` value** is non-breaking. Consumers using a closed switch will surface unknowns naturally.
- **Adding a new `Evidence.Kind`** under an existing detector is reserved as a future evolution path. Consumers branching on `Kind` should default-case unknowns rather than panic.
- **Renaming or removing** any of the strings above is a breaking change and requires an `/v2` API.
- **Changing the unit or meaning of an existing metric value** is a breaking semantic change even when the key stays. It is recorded in `CHANGELOG.md` and signaled by a `detector_version` bump. `divergence-v2` / `convergence-v2` changed `slope` from per ordinal step to gap units per hour; `slope_per_hour` is the explicit-unit alias added in the same change.
