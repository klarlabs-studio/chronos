# Changelog

All notable changes to Chronos are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

The wire contract documented in [`docs/wire-contract.md`](docs/wire-contract.md) is the stability boundary. Renaming any documented Pattern, Evidence.Kind, or metric key is a major-version change.

## [Unreleased]

## [0.18.0] - 2026-09-20

### Added
- **[`docs/intent.md`](docs/intent.md)** — authoritative product Intent:
  signals-not-opinions, shape-not-state, input invariants, temporal
  semantics, numerical robustness, explainability, confidence semantics,
  storage-as-infrastructure, first-class embedding, wire contracts as
  public API, detector acceptance criteria, and hardening priorities.
- **[`docs/temporal-contract.md`](docs/temporal-contract.md)** —
  deterministic behaviour for observation IDs, timestamps, ordering,
  duplicate IDs/timestamps, sparse/irregular series, and signal immutability.
- **Injectable `chronos.Registry`** — `NewRegistry` / method receivers;
  package-level `Register` / `Get` / `Adapters` delegate to
  `DefaultRegistry()` so multiple engines can coexist in one process.
- **Injectable `store.Registry`** — `NewRegistry` / `Clone` / method
  receivers; package-level `Register` / `Open` / `SupportedSchemes`
  delegate to `DefaultRegistry()`. Duplicate schemes still panic.
  `embed.WithStoreRegistry` opens storage against an isolated registry.
- **Oscillation, Divergence, Convergence detectors** — Intent shape
  catalog completion. Proto enum values 12–14; wire evidence kinds
  `sign_flip_rate`, `pair_divergence`, `pair_convergence`. Plateaus
  remain Stall (documented).
- **Per-detector Strength / Confidence table** in
  [`docs/wire-contract.md`](docs/wire-contract.md).
- **Embed temporal-contract integration test** — out-of-order ingest,
  duplicate ID upsert, duplicate timestamps → Detect determinism.
- **`embed.WithAdapterName` / `DetectStates` / `SetSignalRepository`** —
  so `cmd/chronos compute` dogfoods the embed surface while preserving
  batch-only detect and webhook wrapping.

### Changed
- **BREAKING for embedded callers: `EntityState.Validate` rejects a nil
  observation ID** (`ErrMissingObservationID`). Observation ID is the
  persistence idempotency key; leaving it nil collapsed every such write
  onto one row. HTTP, gRPC and MCP already mint a UUID when the wire
  omits `id`. Callers constructing an `EntityState` directly (including
  `embed.Engine.Process`) must set `ID` — typically `uuid.New()`.
- Package and adapter docs drop residual “insight” vocabulary in favour
  of Observation → Detection → Signal, matching the Intent.
- **`config.Validate` requires `CorrelationMinPoints` and
  `CrossScopeMinPoints` ≥ 3** — two aligned points are always collinear,
  so a floor of 2 manufactured perfect `|r| = 1`. Detectors also refuse
  floors below 3 as defence in depth. Shipped defaults remain 5.
- **ADR 0001** updated to describe the shipped `embed/` package and
  injectable registries (was drafted as root-package `chronos.Engine`).
- **`cmd/chronos compute` constructs `embed.Engine`** instead of wiring
  `store.Open` + `pipeline.Compute` directly.
- **BREAKING wire: Trend is `trend-v2` — OLS against wall-clock hours
  since window start**, not ordinal index. `slope` is outcome-units per
  hour. Irregular sampling changes the fitted slope. Equal timestamps
  yield no signal. `CHRONOS_TREND_MIN_SLOPE` (default 0.05) is interpreted
  in those units.
- **SQLite / libSQL timestamps use fixed-width UTC TEXT** (nine
  fractional digits). Removes `QuirkLexicalSubSecondTime`. Existing DBs
  are rewritten on Open via `normalizeTimestampText`.
- **Engine sorts observations by `(timestamp, observation ID)`** before
  detection. Equal-timestamp ties are broken by lexicographic ID;
  `mostRecentByEntity` uses the same total order.

### Fixed
- **ChangePoint ranks clean constant-regime steps as maximum evidence**
  instead of discarding `+Inf` standardised shifts and preferring noisy
  splits. The wire metric stores a finite sentinel (`1e12`).
- **Anomaly skips zero-norm (and length-mismatched) feature vectors** —
  cosine similarity against a directionless vector is undefined, not
  “maximally isolated”.
- **Adversarial coverage extended to Oscillation, Divergence, and
  Convergence** — shared single-series matrix plus dedicated tables;
  huge / denormal / below-floor cases must stay silent or emit
  Validate-clean signals.
- **OutlierCluster ignores denormal absolute deviations** off a
  zero-variance baseline and derives Confidence as
  `strength × sampleFactor`, not `strength + 0.3`.
- **SQLite / libSQL sub-second ORDER BY** matches chronology (was
  broken by RFC3339Nano trimmed fractional zeros).

### Changed (spike/drop confidence — prior unreleased)
- **Spike and Drop derive Confidence from the quality of the evidence
  instead of copying Strength.** This changes the confidence number
  emitted for every spike and drop signal, so it wants a minor version
  bump: consumers thresholding or ranking on these values will see
  different results for identical input. `detector_version` moves to
  `spike-v2` / `drop-v2` so a consumer can tell the two formulas apart.

  The old behaviour was `Confidence = Strength`, which made the two
  fields one measurement under two names. It contradicted itself at the
  bottom of the range: at six observations — the fewest the detector
  accepts, with the shipped window of 5 — a large jump reported
  `Confidence = 1.0` beside `ConfidenceClass = "tentative"`. The signal
  claimed maximum certainty and admitted to being tentative in the same
  breath. Chronos's own rule is that strength describes the magnitude of
  the observed pattern and confidence the quality of the evidence behind
  it, and that the two stay distinct.

  Confidence is now `support × quietness × margin`:
  - **support** — `min(n / (CONFIDENCE_STRONG × (SpikeWindow+1)), 1)`.
    It saturates exactly at the boundary where `ConfidenceClass` says
    `strong`, which is what makes the number and the class agree: a
    `tentative` signal is now capped at `ESTABLISHED/STRONG` (0.4 on the
    shipped 2× / 5× defaults), and only a `strong` one can approach 1.
  - **quietness** — `1 − ½·stddev/(|mean| + stddev)`. The same z against
    a baseline whose spread rivals its own level is weaker evidence than
    one against a quiet baseline. Capped at a halving: noise weakens
    evidence, it does not erase it.
  - **margin** — `½` at the trigger threshold, reaching `1` once `|z|`
    is 25% past it. A detection sitting on the decision boundary is one
    a fractionally different baseline would not have made. Past that
    point confidence is flat in magnitude, which is what keeps it
    distinct from strength.

  Concrete values, shipped defaults (window 5, z ≥ 2.5, established 2×,
  strong 5×). Six observations `[1, 1.1, 0.9, 1, 1.05, 900]`, z ≈ 13553:
  strength stays `1.0`, confidence `1.0 → 0.1938`, class `tentative` as
  before. The same jump after 30 observations of that baseline:
  confidence `1.0 → 0.9692`, class `strong`. Holding history and
  baseline fixed and varying only the deviation, z = 3.125, 4, 5 and 40
  all yield confidence `0.1993` while strength runs 0.625 → 1.0 — the
  flatness is the point.

  Strength is unchanged, and sample count is deliberately not folded
  into it. Consumers ranking spikes by *how big* they were should read
  `strength` or the `z` metric; `confidence` now answers how much the
  evidence is worth.

### Fixed
- **A non-real spike/drop confidence can no longer reach the wire.**
  `domain.Signal.Validate` range-checks with `< 0 || > 1` and both
  comparisons are false for `NaN`, so a `NaN` confidence validates
  cleanly — the failure mode 0.17.0 fixed in the detectors' arithmetic.
  The new confidence passes through an explicit finiteness guard rather
  than `clamp01`, which returns `NaN` unchanged.

### Changed
- **BREAKING: `Signal.Validate` rejects non-finite numbers anywhere in a
  signal's numeric payload** — `Signal.Metrics`, `Evidence.Score`,
  `Evidence.Metrics`, `Explanation.ThresholdUsed` and the feature-evolution
  values — with a new `domain.ErrNonFiniteMetric`. `Strength` and
  `Confidence` are covered too: their range check reads
  `Strength < 0 || Strength > 1`, and both comparisons are false for `NaN`,
  so the same hole 0.17.0 closed inside the detectors was still open at the
  domain boundary the detectors hand to.

  The loss this prevents is silent and total, not partial. All three SQL
  stores wrote the metric bags with
  `metricsJSON, _ := json.Marshal(sig.Metrics)`. `encoding/json` refuses
  non-finite floats and returns zero bytes when it does — measured:
  `json.Marshal(map[string]float64{"mean": math.Inf(1), "n": 12})` returns
  `bytes=""`, `err="json: unsupported value: +Inf"` — so the discarded error
  wrote an empty metrics column. One unrepresentable key did not cost that
  key; it cost the map, and afterwards nothing could tell that signal apart
  from one whose detector surfaced no metrics at all.

  Reachable from legal input. Twelve observations of `math.MaxFloat64` are
  a flat series, so Stall fired: the normalised spread was exactly 0, and
  `metrics["mean"]` — the mean of the raw values — was `+Inf`. The signal
  passed `Validate` and reached the stores with its metrics erased.
  `TestFinding_NonFiniteMetricsSurviveIntoSignals` recorded that in 0.17.0
  rather than fixing it, because the fix changes the `Signal` contract.
  This is that change; the test is now
  `TestStallIsSilentWhenItsMetricsOverflow` and asserts the silence.

  Finite extremes stay valid — the invariant is representability, not
  magnitude. Embedded callers constructing a `domain.Signal` by hand must
  keep its numbers finite; out-of-tree detectors that previously emitted an
  overflowed metric now have their signal dropped rather than persisted
  empty.

### Fixed
- **Detectors stay silent instead of emitting a signal that fails its own
  `Validate`.** The `Detector` interface has always said returned signals
  must validate; the promise rested on each detector's arithmetic guards,
  and a gap in one produced a signal rather than silence. All eleven
  detectors now filter their output through one helper, and
  `Engine.Detect` repeats the check before the pipeline persists and the
  API serves, because `Detector` is an interface and an out-of-tree
  implementation is under the same contract with nothing else checking it.
  This follows the rule the rest of the engine already keeps: a detector
  that cannot establish evidence produces no signal rather than
  manufactured confidence, and "no signal" is a valid result.

  Side effect worth knowing: descending input, which the `Detector`
  interface documents as a precondition violation, now yields an empty
  slice instead of signals with inverted windows.
  `TestFinding_DetectorsRequireChronologicalInput` still records the
  caveat — the loss remains silent — but records silence rather than
  corruption.
- **The three SQL stores no longer discard the `json.Marshal` error** when
  encoding `Signal.Metrics` and `Evidence.Metrics`
  (`internal/store/{sqlite,postgres,mysql}/signal_repo.go`; libSQL reuses
  the SQLite repositories). Each now goes through an `encodeMetrics` helper
  that returns the error, and `Save` fails loudly rather than writing an
  empty column. The invariant above means nothing can currently reach that
  path — `Save` validates first — which is exactly why the error needed
  returning: the previous behaviour was invisible to every test, and the
  next unmarshalable value to appear in a metric bag would have been
  written as emptiness in the same way. MySQL also carried
  `if metricsJSON == nil { metricsJSON = []byte("{}") }`, a guard that
  could only fire when the discarded error was non-nil; it is gone.

## [0.17.0] - 2026-09-20

### Changed
- **BREAKING: `EntityState.Validate` enforces the numerical and temporal
  invariants.** It previously checked identity, feature presence and label
  arity and said nothing about the values themselves, so `NaN`, `+Inf`,
  `-Inf` and the zero timestamp all reached the detectors. Three new
  errors reject them at the boundary: `ErrNonFiniteFeature`,
  `ErrMissingTimestamp` and `ErrEmptyLabel`.

  `NaN` is why this is a correctness problem rather than a tidiness one.
  It is not equal to itself and every comparison against it is false, so a
  threshold gate does not reject it — it fails open, and the detector
  emits a signal with a plausible shape and meaningless numbers instead of
  staying silent.

  The wire is unaffected: HTTP, gRPC and MCP already default an omitted
  timestamp to now before an `EntityState` is constructed. Finite extremes
  — `MaxFloat64`, denormals, zero — remain valid; the invariant is
  finiteness, not magnitude. Embedded callers constructing an
  `EntityState` directly with a zero `Timestamp` must now set one.

### Added
- **Adversarial numerical coverage for all eleven detectors** — 195
  subtests over zero variance, constant series, sample counts at and below
  each detector's documented minimum, `MaxFloat64` and denormal
  magnitudes, identical vectors, duplicate timestamps, out-of-order
  arrival, irregular intervals and sparse series, plus per-detector
  determinism checks. Behaviour that is questionable but whose correction
  would be a contract change is pinned by a `TestFinding_*` test recording
  the concrete input and output, rather than quietly asserted as correct.
- **A storage backend conformance suite** (`internal/store/conformance`),
  run by all five backends — memory, SQLite, libSQL, PostgreSQL,
  MySQL/MariaDB — from their own packages. 27 contract groups cover
  persistence, ordering, filtering, duplicate IDs, duplicate timestamps,
  out-of-order arrival, limits, timestamp fidelity, not-found versus
  empty, retention and concurrency. Postgres and MySQL run it under the
  existing `TEST_POSTGRES_DSN` / `TEST_MYSQL_DSN` gates and CI jobs.

  Where backends disagree and the fix is not obviously safe, the suite
  declares the divergence as a `Quirk` on the backend and asserts that
  the divergence still reproduces exactly, so it can neither persist
  unnoticed nor be repaired unnoticed.

### Fixed
- **`NaN` strength and confidence escaped four detectors and passed
  `Signal.Validate`.** The range check reads
  `Strength < 0 || Strength > 1`, and both comparisons are false for
  `NaN`, so an invalid signal validated cleanly. Reachable from entirely
  legal input: twelve observations of `MaxFloat64` gave trend, spike and
  drop `Strength=NaN, Confidence=NaN, Validate()=nil`. Guards now live in
  `linearRegression`, `pearsonCorrelation` and `zScoreSignal`, each of
  which already promised the behaviour in its own doc comment. Note this
  is a distinct problem from the input invariants above: finite input can
  still produce non-finite output, so guarding the boundary alone was not
  sufficient.
- **Recurrence emitted `Strength = 1.0000000000000002`** for identical
  feature vectors — a signal that fails its own `Validate`, which the
  `Detector` contract forbids. `similarity.Cosine` returned two ulp above
  its documented `[-1, 1]` range; it is clamped there.
- **Signal order was decided by map iteration** in eight detectors and in
  the engine's scope grouping, on both the sequential and parallel paths.
  Trend over eight series differed from its first run in 175 of 200
  repeats. Because the final sort is stable, this silently decided which
  signals survived `MaxSignalsPerRun` — the same input could yield
  different retained signals run to run. Keys are now sorted before use.
- **PostgreSQL lost its namespace on every connection but the first.**
  The schema was selected with `SET search_path`, which is a property of
  one session, while the pool opens up to 25. Whichever connection
  served `Open` could see the tables; every later one started on the
  default search_path and failed with
  `relation "entity_states" does not exist`. Single-threaded use hid it
  because the pool kept handing back the one warm connection — in the
  conformance suite's concurrency group, four writers and four readers
  produced the error immediately. The namespace now travels as a
  `search_path` startup parameter on the DSN, so every connection the
  pool opens carries it.
- **MySQL/MariaDB deadlocked under concurrent signal saves.** `Save`
  clears a signal's evidence before re-inserting it, and under the
  server's default REPEATABLE READ a `DELETE ... WHERE signal_id = ?`
  matching no rows still takes a next-key lock on the gap it searched —
  the same gap every other writer is inserting into. Measured on
  MariaDB 11: 40 concurrent saves of unrelated signals, **30 lost** to
  `Error 1213: Deadlock found`. The transaction now runs at READ
  COMMITTED, which does not take those gap locks and which is
  sufficient for a statement touching one signal and its own evidence.
- **SQLite dropped `explanation` and `confidence_class` from `List`.**
  Both columns were selected by `Get` and not by `List`, so the same
  signal carried its explanation when fetched by ID and lost it when
  returned in a list.
- **libSQL in local-file mode dropped writes under concurrency.** The
  provider reuses the SQLite repositories but not the single-connection
  pool they were written for, so concurrent writers hit `SQLITE_BUSY`
  and the write was lost rather than retried: 80 concurrent ingests
  landed 20 rows. Local-file mode now caps the pool at one connection,
  as the SQLite provider does. Remote databases keep the default pool.
- **The in-memory store now matches the SQL backends** on three points
  where it was the odd one out: a repeated `Ingest` of the same
  observation ID is an idempotent update rather than a second row; a
  repeated `Save` of the same signal ID revises the detector's
  quantities and evidence rather than rewriting the perception's
  identity; and a batch `Save` containing an invalid observation now
  writes none of the batch instead of everything before the bad row.
- **Timestamps come back in UTC from every backend.** pgx returns
  `TIMESTAMPTZ` in the client's local zone and the in-memory store
  returned whatever zone the caller supplied, so the same instant
  rendered differently depending on backend and on the machine's `TZ`.

## [0.16.0] - 2026-09-20

### Added
- **`CHRONOS_RETENTION_SWEEP_INTERVAL`**, defaulting to **`5m`** where the
  sweep cadence was previously a hard-coded hour.

  The interval *is* the overshoot: whatever ages out between sweeps is
  still on disk, so an hourly sweep holds up to an hour of data beyond
  the retention window. On a fleet producing 2,430 signals/hour at ~716
  evidence rows each that is ~245 MB of slack, against ~20 MB at five
  minutes.

  It also scaled wrongly against short windows. `CHRONOS_SIGNAL_RETENTION=1h`
  swept hourly holds two hours — the overshoot is 100% of the window. An
  hour is not small relative to anything an incident engine is likely to
  configure.

  The hourly cadence was justified by a DELETE that no longer exists: it
  was table-wide and unbounded, so running it often was genuinely
  wasteful. Since 0.14.0 the delete is bounded and indexed, and since
  0.15.0 retention runs off the detection goroutine — so a sweep with
  nothing to do is a probe returning zero rows, and one with work to do
  cannot delay a tick. The reason for the hour was removed by those two
  changes without the hour being revisited.

## [0.15.0] - 2026-09-20

### Fixed
- **Retention no longer blocks detection.** `Run` called `sweep` inline —
  once before the loop, then on a second ticker inside the same
  `select` — so a slow sweep produced exactly as much dead time as it
  took, and the worst sweep of all (the first, facing whatever
  accumulated before retention was enabled) sat directly in the startup
  path, making a restart look like a hang.

  Observed on a deployment carrying ~716 cascading evidence rows per
  signal: one batch spent **23 minutes** in `IO/DataFileRead` without
  committing, and the engine produced no detections for the whole of it.
  Cancelling that single statement restored detection within seconds —
  98 signals in the next three minutes — which is how the coupling was
  found.

  Detection and retention share nothing but the goroutine they ran on:
  one reads observations, the other deletes aged signals. Retention now
  runs on its own goroutine, and a test pins it by wedging a sweep open
  and asserting a tick still completes.

### Changed
- **`retentionBatchSize` 1000 → 200.** The number that matters is not the
  batch but what it multiplies into, and the engine cannot know the
  ratio: at ~716 evidence rows per signal, 1000 signals is 716,000
  cascaded deletions. 200 keeps that deployment near 143,000, which
  commits in seconds. Smaller batches cost round trips and nothing else —
  WAL recycles between commits either way.

## [0.14.0] - 2026-09-20

### Fixed
- **The retention sweep no longer deletes the whole backlog in one
  transaction.** `DeleteSignalsOlderThan` issued a single unbounded
  `DELETE`, and `Run` calls `sweep` immediately on start — so the
  failure landed precisely where retention is most needed: a deployment
  that enables it *after* accumulating, during a rolling restart.

  Measured on a live deployment holding 36,209 signals and 25,940,060
  evidence rows (~716 per signal, cascading via `ON DELETE CASCADE`):
  26,807 eligible signals as one statement ran for nine minutes,
  consuming ~64 MB/min of WAL with nothing reclaimable until commit. It
  was minutes from exhausting the volume, at which point it would have
  rolled back having deleted nothing, leaving the database read-only.

  The identical deletion in batches of 1000 did not move free space at
  all, because WAL recycles between commits.

  `SignalRetainer.DeleteSignalsOlderThan` now takes a `limit`, honoured
  by memory, sqlite, postgres and mysql — sqlite through a subquery,
  since stock builds lack `SQLITE_ENABLE_UPDATE_DELETE_LIMIT`, and mysql
  through `ORDER BY … LIMIT`. The scheduler loops until a batch returns
  short, so a large first sweep finishes rather than leaving the
  remainder for an hour later when it is an hour larger. Progress is
  logged per batch, because the sweep an operator most wants to watch is
  exactly the one that would otherwise say nothing for minutes. A
  cancelled sweep stops at a batch boundary.

  Two regression tests, both verified red: one fails if any delete is
  issued unbounded, one if a cancelled sweep keeps going.

## [0.13.0] - 2026-09-19

### Changed
- **`CHRONOS_DETECTION_LOOKBACK` now defaults to `24h`, not `168h`.**

  The 0.12.0 default was sized for seasonality — the detector with the
  longest memory, which needs more than one cycle of a daily or weekly
  rhythm to tell it from a trend. That is the right consideration for
  detection quality and the wrong one for a default. The deployment it
  was written for OOM-killed on it immediately: 1804Mi against a 2Gi
  limit, because a week of 73 series at a five-minute cadence is ~1.8GB
  once decoded into `[]EntityState`. The bound was working — the startup
  line reported `detection_lookback=168h0m0s` — the value was simply too
  large. It is the same failure as the bug 0.12.0 fixed, one step
  smaller: that code loaded everything, this default loaded more than
  fits.

  `24h` is 288 points at a five-minute cadence against a largest
  detector requirement of 24 (`CHRONOS_SEASONALITY_MIN_POINTS`) —
  twelve times the floor, and enough for a daily rhythm.

  **Weekly seasonality does not resolve inside a 24h window.** A
  deployment that needs it should raise the lookback deliberately.
  `docs/configuration.md` now carries the sizing arithmetic (~10.7MB per
  hour of history on that fleet) so the number can be computed rather
  than inherited, and a test pins the default so widening it is a
  deliberate edit against a failing assertion.

## [0.12.0] - 2026-09-19

### Fixed
- **The detection scheduler no longer loads a scope's entire history on
  every tick.** `tick` called `ListByScope`, which has no limit and no
  window, once per scope per interval. Nothing prunes `entity_states` —
  `DeleteOlderThan` has existed on `EntityStateRepository` since the
  beginning and had no caller anywhere in the tree — so that result grew
  for as long as a deployment ingested, and a scheduler on a 30-second
  timer re-materialised the whole table every 30 seconds.

  Measured on a 73-series deployment: a flat 2 MiB baseline, ~1.9 GiB
  allocated inside a single tick, OOMKill, repeating. It tracks table
  size rather than ingest rate, so raising the memory limit lengthened
  the cycle instead of stopping it, and restarting reclaimed nothing.

  This is a **second** unbounded read, distinct from the one fixed in
  0.11.0. That one was the per-tick duplicate check against the
  `signals` table; this is the history load against `entity_states`, and
  it dominates. 0.11.0 alone did not stop the OOMKills.

  The load happens before `Detect` is called, which is why disabling
  individual detectors — including correlation, the only `O(N²)` one —
  did not move it. Detectors only consult their own analysis window, so
  everything behind it was loaded and then ignored.

  `ports.EntityStateRepository` gains `ListByScopeSince`, implemented
  across memory, sqlite, postgres and mysql and forwarded by the
  batching decorator. `CHRONOS_DETECTION_LOOKBACK` (default `168h`)
  bounds the window. A regression test fails if a tick ever issues an
  unbounded scope load again, and a second asserts that a zero lookback
  falls back to the default rather than producing a zero cutoff — which
  as a predicate matches every row ever written.

- **`serve` logged the wrong storage backend.** The startup line took
  `store` from `cfg.DBType`, the legacy selector, which defaults to
  `sqlite` whether or not it is in use. `resolveDSN` prefers
  `CHRONOS_DB_DSN` and only falls back to `DBType`, so any deployment
  configured the modern way — the only way to pass a `?namespace=`
  parameter — announced `store=sqlite` while running on Postgres. It now
  reports the scheme of the DSN that actually selected the backend.
  Only the scheme: the DSN carries credentials and never reaches a log.

- **A transient failure opening the store is no longer fatal.** Starting
  under an orchestrator regularly loses a race against the process's own
  networking — the container runs before service routing into its
  namespace is programmed, so the first dial is refused and every
  subsequent one succeeds. Observed on every rollout, with identical
  start and finish timestamps against a database that had not moved in
  nine hours. `serve` now retries the initial connection five times at
  two-second intervals. A misconfigured DSN still fails, because it
  fails identically on every attempt.

### Added
- **`CHRONOS_DETECTION_LOOKBACK`** — how much history each detection
  tick loads per scope, defaulting to `168h`.

  Defaulting to unbounded was considered and rejected: unbounded is the
  defect, so that default would leave every deployment on the broken
  path until it opted out. Seven days is sized for seasonality, the
  detector with the longest memory, which needs more than one cycle of a
  daily or weekly rhythm to tell it from a trend.

## [0.11.0] - 2026-09-19

### Fixed
- **The detection scheduler no longer reads the signals table to ask
  whether it has seen a signal before.** `alreadyPersisted` fetched every
  stored signal for the candidate's `(scope, series, pattern)` — with no
  limit, and on the SQL backends one further query per row to hydrate
  evidence — in order to compare two timestamps, once per candidate per
  tick. Nothing prunes that table, so the read had no ceiling: a
  deployment running 73 series at a 30-second cadence accumulated 24,471
  rows in 19 hours and was OOM-killed 35 times. `CHRONOS_MAX_SIGNALS`
  does not help, because it caps what a run *emits*, not what the store
  holds.

  The scheduler now asks the store to `Count` the full perception
  identity, which keeps a tick's memory flat however large the store has
  grown. `SignalFilter` gained a `Window` field to express it, and all
  three SQL schemas gained `idx_signals_identity` so the check is an
  indexed probe. A regression test fails if the scheduler ever issues an
  unbounded `List` again.

- **`go.opentelemetry.io/otel/sdk` to v1.45.0**, clearing
  GHSA-8wmf-6v46-5gfg / CVE-2026-81870 (exporter config logging may leak
  endpoint URLs into info logs). Low severity, and nox could not establish
  whether the affected symbol is reached — it resolved `affected_version`
  and stopped at `symbol_used` — so this is taken on version alone. The
  bump carries `otel`, `otel/metric` and `otel/trace` to 1.45.0 with it.

### Added
- **`CHRONOS_SIGNAL_RETENTION`** — deletes signals detected longer ago
  than the given duration, swept hourly. Defaults to `0` (keep
  everything), so nothing changes for a deployment that does not opt in.

  The fix above bounds the *memory* a tick costs, not the table. A
  detector derives its analysis window from the observations in front of
  it, so on a live stream that window slides forward and the duplicate
  check legitimately misses — every tick appends a row, and because the
  store outlives the process, restarting the engine reclaims none of it.
  Any deployment whose scheduler runs continuously should set this.

  Retention is an optional store capability (`ports.SignalRetainer`)
  rather than a `SignalRepository` method: the signals table is otherwise
  append-only, and pruning it is an operator's decision about storage,
  not a domain operation. All four bundled backends implement it, the
  notifier decorator forwards it, and a store that cannot prune makes the
  scheduler log an error on every sweep rather than silently do nothing.

## [0.10.0] - 2026-09-14

### Added
- **`CHRONOS_CHANGEPOINT_MIN_DELTA`** — an optional floor on
  `|mean_before − mean_after|` in the outcome's own units, applied
  alongside the standardised shift rather than instead of it. Defaults to
  `0` (disabled), so existing behaviour is unchanged.

  The standardised test divides by pooled stddev, which is the series' own
  variability, so on a metric that barely moves any change at all scores
  as many sigma. Seen in a real deployment: health headroom went from
  0.9050 to 0.9072 — an improvement of two tenths of one percent — and
  reported 7.98 sigma at confidence 1.00. Correct, and far too small to
  act on. The floor lets a deployment say how much movement is worth a
  signal. Opt-in because only the adapter knows the outcome's scale.

### Fixed
- **`google.golang.org/grpc` to v1.83.2**, clearing GHSA-vp52-pcj8-j9qc
  (heap memory exhaustion via HTTP/2 DATA frame fragmentation) and
  GHSA-2v4p-qf9q-27wj (xDS server DoS). Note the second advisory carries a
  fix version per release line — 1.82.2 on 1.82.x, 1.83.2 on 1.83.x — so
  1.83.1 clears the first and remains inside the second.

## [Unreleased]

> **Audit note (2026-09-19).** The bookkeeping gap flagged in #67 is now
> resolved against the tags rather than by guesswork. Every entry below the
> old note was probed against the v0.5.0–v0.10.0 trees for the artefact it
> describes; sixteen first appear in the v0.9.0 tree and have been moved to
> a dated `[0.9.0]` section. What remains here genuinely does not belong to
> a later release: both entries describe code already present at v0.5.0, the
> earliest tag still in this file, so they shipped at or before 0.5.0 and
> cannot be placed more precisely from the tags alone. They are left rather
> than reassigned, for the reason the original note gave — a wrong date is
> worse than an acknowledged gap in the file this project calls its
> stability record.

### Changed
- **MySQL backend** stores `explanation` and `confidence_class` and honours
  `SignalFilter.ScopeIDs`, matching sqlite/postgres/memory.
- **Detection scheduler** skips a candidate when a signal with the same
  `(scope, series, pattern, window)` already exists, so a second tick over
  unchanged observations does not append duplicate rows (and does not
  re-notify). A later tick that grows `window.End` (new points) still emits.
  SQLite and Postgres now replace evidence on ID upsert (MySQL already did).
- Agent and user docs (README, architecture, configuration, CLAUDE, AGENTS)
  list all eleven detectors, the MySQL/libSQL backends, and the HTTP auth /
  extra `/v1` routes that were already in the binary.



## [0.9.0] - 2026-08-17

### Changed
- **No Homebrew cask.** Chronos is a Go library (optional `cmd/chronos` for
  demos and ops), not a brew-installable product. GoReleaser no longer
  publishes to the Homebrew tap; the release workflow no longer requires
  `HOMEBREW_TAP_TOKEN`.
- **GHCR images publish to `ghcr.io/klarlabs-studio/chronos`.** The v0.9.0
  tag built artefacts then failed pushing `ghcr.io/felixgeelhaar/chronos`
  (`permission_denied: The requested installation does not exist`) because
  the workflow token belongs to `klarlabs-studio`. The Go module path is
  unchanged.


### Added
- **`chronos health` subcommand** — probes `GET /health` on
  `http://127.0.0.1:$CHRONOS_HTTP_PORT` and exits non-zero when the server does
  not report `healthy`. Override with `-addr` / `-timeout`. Exists so the
  runtime image can declare a `HEALTHCHECK`: the distroless base ships no shell
  and no `curl`, so the binary has to probe itself.
- **Container `HEALTHCHECK`** in the Dockerfile, wired to `chronos health`.
- **Detector `Explanation` payloads** — every detector now fills
  `Signal.Explanation` with numeric feature evolution (where a series exists),
  comparable-peer count, threshold used, and a stable `detector_version` tag
  (`trend-v1`, `spike-v1`, …). Still no Title/Summary/Suggestion.
- **Public client HTTP parity** — `Explanation` / `ConfidenceClass` on
  `client.Signal`; `ListPage` (`next_cursor` / `since_cursor`); `Scopes`
  (`scope_in`); `IngestBatch`; `FederationExport`.
- **gRPC HTTP parity (additive RPCs)** — `IngestBatch`, `StreamSignals`
  (server-streaming), `ValidateConfig`, `ExportFederation`, plus
  `since_cursor` / `next_cursor` on `ListSignals`. Unary `Ingest` is
  unchanged. The public `client.GRPCClient` mirrors the new RPCs.
- **Content-addressed signal IDs** — `domain.PerceptionID` (UUID v5 of
  scope, series, pattern, window, pairwise partner). The engine stamps
  every emission so a second detect over the same window upserts.
- **Per-detector metrics** — `chronos_detector_duration_seconds_*`,
  `chronos_detector_signals_total`, `chronos_detector_skips_total`,
  `chronos_signals_truncated_total{pattern}`.


### Changed
- **Nous is archived.** Living docs, agent files, issue/PR templates, and
  package comments no longer treat Nous as a live decision layer.
  Interpretation belongs to agent runtimes; risk + intervention scoring
  lives in [decisionkit](https://github.com/felixgeelhaar/decisionkit).
  See [Mnemos ADR 0005](https://github.com/felixgeelhaar/mnemos/blob/main/docs/adr/0005-archive-nous.md).
- **Dockerfile hardening** — explicit `USER 65532:65532` (the distroless
  `nonroot` default, now stated rather than inherited, so it survives a base
  image retag), `COPY --chown=root:root --chmod=0555` so a compromised process
  cannot rewrite its own entrypoint, and a `maintainer` label.
- **`Signal.Validate`** allows `Series == uuid.Nil` for `outlier_cluster`
  (cohort-level by contract). Those signals can now persist instead of being
  silently dropped on save.
- **`pipeline.Compute` `SignalsCreated`** counts signals that actually saved,
  not detections that failed validation.
- **Default `CHRONOS_MAX_SIGNALS`** is `100` (was `10`). `0` remains unlimited.

### Security
- **`golang.org/x/mod` 0.37.0 → 0.40.0** — GO-2026-6179 / CVE-2026-56865
  (sumdb/tlog) and GO-2026-6180 / CVE-2026-56864 (module zip hashes). Indirect;
  not reachable from Chronos's own code.
- **`golang.org/x/text` 0.38.0 → 0.41.0** — GO-2026-5970 / CVE-2026-56852,
  infinite loop on invalid input in `golang.org/x/text/unicode/norm`. Reachable
  transitively; no API change.


## [0.8.0] - 2026-07-11

Maintenance release; no functional change. Reconstructed from the tag range
during the 2026-09-19 audit, because no entry in `[Unreleased]` described it.

### Changed
- **`go.klarlabs.de/mcp` to v1.21.0, then v1.22.0** (#43, #46).
- **nox 0.10.1 → 1.7.0**, with baseline adoption (#45).

## [0.7.0] - 2026-07-03

Largely build, supply-chain and dependency work. Reconstructed from the tag
range during the 2026-09-19 audit.

### Added
- **Public per-backend shims for durable storage** (#41).

### Changed
- **Klarlabs library dependencies moved to `go.klarlabs.de` vanity paths.**
- **The mnemos image is pulled from `ghcr.io/klarlabs-studio`**, which does
  not redirect on GHCR.
- Dependency and action bumps: goreleaser-action 6 → 7 (#34),
  docker/login-action 3 → 4 (#33), actions/deploy-pages 4 → 5 (#32), and the
  go-deps group (#37).

### Fixed
- **Eight false-positive nox findings suppressed**, goreleaser pinned to
  `~> v2.15` (#36), and IAC-013 handled with a VEX waiver plus
  `-fail-on-unwaived` (#33, #34).

## [0.6.0] - 2026-05-31

Embeddable engine release. Adds a public in-process Go API so consumers
(notably Mnemos, which now bundles Chronos to power its temporal memory)
can drive Chronos as a library instead of as a CLI / HTTP service. Adds
ADR 0001 establishing the contract.

### Added
- **`chronos/embed` package** — embeddable in-process engine.
  `embed.New(opts...)` returns an `Engine` with `Process`, `ProcessBatch`,
  `Detect`, `Query`, and `Close`. Defaults to a memory storage backend
  so zero-config usage works in tests and demos; SQL providers are
  blank-imported by callers as before.
- **`chronos/signal.go`** — public type aliases re-exported from
  `internal/domain`: `Signal`, `PatternType`, `TimeWindow`, `Evidence`,
  `FeatureSample`, `Explanation`, `ConfidenceClass`. Pattern and
  confidence constants are also re-exported.
- **ADR 0001 (`docs/adr/0001-embeddable-engine-api.md`)** — documents
  the embeddable-engine decision, stability contract, and alternatives.
- `embed.WithStorage`, `embed.WithLogger`, `embed.WithDetectionConfig`,
  `embed.WithDetectors`, `embed.WithParallelDetectors` option builders.

### Changed
- `chronos.Register` now documents its last-write-wins semantics. The
  behaviour itself is unchanged; the doc clarifies that re-registering
  an adapter under the same name overwrites the previous entry, which
  is intentional for processes that import both the library surface and
  the `cmd/chronos` binary.

## [0.5.0] - 2026-05-24

Cognitive-stack alignment release. Twelve issues land that turn
chronos from a passive perception engine into a tenant-safe,
agent-facing service that mnemos and downstream LLM hosts can wire
into directly. Highlights: explainability, MCP transport, CLI
schema visibility, federation hook.

### Added
- **Pattern explainability payload on `Signal`** (#21) — optional
  `Explanation` value object surfaces the feature evolution series
  the detector inspected, the comparable-peer count, the baseline
  window, the threshold applied, and a stable detector version
  string. Persisted via a new `explanation` JSONB / TEXT column on
  the signals table; surfaced symmetrically on the HTTP DTO
  (`explanation` field, omitempty) and the gRPC `Signal.explanation`
  message (tag 11).
- **Anonymize-cross-scope mode** (#20) — `CHRONOS_ANONYMIZE_CROSS_SCOPE=true`
  flips `CrossScopeCorrelation` to emit deterministic UUIDv5 hashes
  in place of the real scope/series ids on emitted signals.
  Statistical payload (`r`, `n`, `direction`, strength, confidence)
  survives; `metrics["anonymized"]=1` tells downstream consumers
  the ids are opaque. Multi-tenant deployments can finally enable
  the cross-tenant detector without crossing the data boundary.
- **`POST /v1/ingest/batch`** (#23) — up to `MaxIngestBatchSize=1000`
  observations per call, single repository write per adapter group
  via the existing `Save([])` path. All-or-nothing validation;
  `defer_detection` accepted (and echoed) for forward-compat.
  Backfills drop from N round-trips to one.
- **`POST /v1/config/validate`** (#26) — dry-run a candidate env-var
  map and get back a per-detector report (`enabled` /
  `disabled-with-reason` / thresholds / warnings). Loads via the
  same `config.Default()` path the live server uses; env mutations
  are mutex-guarded and restored on every code path so a validate
  call cannot leak overrides into the running process. The
  cross-scope row additionally warns when `AnonymizeCrossScope=false`
  in a multi-tenant deployment.
- **Opaque cursor pagination on `/v1/signals`** (#28) —
  `since_cursor` + `next_cursor` use a base64-wrapped
  `(DetectedAt, ID)` tuple so polling for "new since last check"
  tie-breaks on signal id when timestamps collide. Empty response
  omits `next_cursor` so clients can detect "nothing new" without
  a sentinel. Bad cursors return 400 — a paste error no longer
  silently degrades into an unfiltered list.
- **`scope_in` allowlist on `/v1/signals/stream`** (#25) —
  multi-scope SSE subscriptions for per-user UIs. The server holds
  the allowlist for the lifetime of the connection; a client
  cannot widen its own filter post-handshake. `SSEBroadcaster.Subscribe`
  now takes `[]uuid.UUID`; nil = any scope, single-element slice =
  legacy per-scope stream, multi-element = the new allowlist path.
  An empty (non-nil) slice fails closed.
- **`confidence_class` on `Signal`** (#24) — qualitative grade
  (`tentative` / `established` / `strong`) derived from sample
  size vs `MIN_POINTS`. Env-tunable multipliers
  `CHRONOS_CONFIDENCE_ESTABLISHED` (default 2.0) and
  `CHRONOS_CONFIDENCE_STRONG` (default 5.0). Persisted via a new
  `confidence_class` column; surfaced on HTTP DTO and gRPC proto
  (tag 12). Lets downstream narrators say "a possible trend" vs "a
  clear trend" without reverse-engineering the sample size.
- **`chronos mcp` subcommand** (#22) — MCP stdio server exposing
  three tools: `list_signals` (single scope or scope_ids
  allowlist), `ingest` (single observation), `describe_detector`
  (delegates to the same `BuildConfigReport` /v1/config/validate
  uses). Companion to the mnemos MCP server so MCP-aware hosts
  (Claude Code, Letta, Anthropic Desktop) discover the whole
  cognitive stack natively.
- **`chronos migrate` CLI** (#27) — `migrate status` reports the
  declared version ladder, current vs latest, and pending/applied
  steps. `migrate up` opens the store (triggers the existing
  auto-apply path) then prints status. `migrate down` is an
  explicit usage error — migrations are forward-only by design;
  per-step `.down.sql` files would have to land first.
- **`GET /v1/federation/export`** (#30) — opt-in
  (`CHRONOS_FEDERATION_ENABLED=true`) anonymized pattern
  statistics: per-pattern count, avg/min/max strength + confidence,
  mean sample size, per-confidence-class histogram. NO scope_ids,
  NO series ids, NO evidence rows in the payload — community-grade
  insight without crossing the tenant boundary. Stable
  alphabetical ordering so two consecutive exports diff cleanly.
- **buf CI guard on every PR** (#29) — `buf lint` (STANDARD ruleset
  minus the response-naming opinion) plus `buf breaking`
  (WIRE_JSON) against main. JSON-over-HTTP is a first-class
  transport so JSON-shape renames matter as much as wire-format
  breaks. Skipped on main itself.
- **Joint chronos+mnemos integration harness** (#31) —
  `test/integration/docker-compose.yml` stands the stack up;
  `test/integration/smoke_test.go` (//go:build integration) pins
  the cross-talk surface: health on both services, chronos ingest
  → list signals, mnemos events append → list. A nightly +
  repository_dispatch CI workflow re-runs the smoke against the
  sister-repo's `main` so version-skew bugs surface within 24h.
- **Postgres bulk-load path** — `EntityStateRepository.Save` switches to chunked multi-row `INSERT…ON CONFLICT` above `BulkSaveThreshold` (200 rows). Chunks of 1 000 rows keep us under the 65 535 placeholder limit. Backfills now batch-commit; per-row UPSERT path retained for small batches where round-trip overhead amortises poorly. — `EntityStateRepository.Save` switches to chunked multi-row `INSERT…ON CONFLICT` above `BulkSaveThreshold` (200 rows). Chunks of 1 000 rows keep us under the 65 535 placeholder limit. Backfills now batch-commit; per-row UPSERT path retained for small batches where round-trip overhead amortises poorly.
- **Durable webhook outbox** — `notify.OutboxConfig.PersistencePath` opts the in-memory outbox into JSON-on-disk persistence. Enqueue / flush snapshot to atomic-rename file; startup re-loads pending deliveries. Survives process restart.
- **Detector parallelism** — `Engine.WithParallelDetectors(true)` runs every (scope, detector) pair in its own goroutine. Off by default; flip on via `CHRONOS_DETECTOR_PARALLELISM=1`. Race tests pass.
- **At-least-once webhook outbox** (`internal/notify/outbox.go`) — `Outbox` wraps an `AckingNotifier` with retry+exponential-backoff (default 5 attempts, 1s→30s). Failed deliveries reschedule until success or max-attempts. In-memory only; restart loses pending. Operators wanting durable delivery wire their own persistent notifier.
- **OutlierCluster detector** (`PatternTypeOutlierCluster`) — detects time windows where multiple series in a scope go anomalous together. Cohort-level, not per-series. Tunable via `CHRONOS_OUTLIER_CLUSTER_MIN_SERIES` (default 3), `CHRONOS_OUTLIER_CLUSTER_Z` (default 2.5), `CHRONOS_OUTLIER_CLUSTER_WINDOW` (default 5m). Signals carry `member_count` and one `outlier_member` evidence row per participating series.
- **CrossScopeCorrelation detector** (`PatternTypeCrossScopeCorrelation`) — pairwise Pearson correlation across (scope, series) pairs in DIFFERENT scopes. Tunable via `CHRONOS_CROSS_SCOPE_MIN` (default 0.8) and `CHRONOS_CROSS_SCOPE_MIN_POINTS` (default 5). Drops same-scope pairs (handled by within-scope `Correlation`).
- **`CrossScopeDetector` interface** — parallel to `Detector`; runs once over the full state list rather than per-scope. Engine picks up `DefaultCrossScopeDetectors` automatically.
- **Write-coalescing repository decorator** (`internal/store/batching`) — opt-in `EntityStateRepository` wrapper that batches Ingest calls into a single Save transaction. Tunable via `MaxBatch` and `MaxWait`. `Close` drains the buffer on shutdown.
- **ChangePoint detector** (`PatternTypeChangePoint`) — best-split mean-shift test detects step changes in the outcome metric. Distinct from Spike/Drop; emits `regime_before` / `regime_after` evidence with `mean`, `stddev`, `n` per regime; `shift` and `split_index` in signal metrics. Tunable via `CHRONOS_CHANGEPOINT_MIN_SHIFT` (default 1.5) and `CHRONOS_CHANGEPOINT_MIN_POINTS` (default 8). Wired into `DefaultDetectors`.
- **SSE `Last-Event-ID` replay** — clients reconnect with the standard `Last-Event-ID` HTTP header (or `?last_event_id=<uuid>` query fallback) and the server replays signals detected at or after the cursor before continuing the live stream. Each SSE frame now carries an `id:` line.
- `ROADMAP.md` — public roadmap with status, next steps, non-goals, semver policy.
- `docs/DEPLOYMENT.md` — operator runbook (k8s shape, Prometheus scrape, Grafana import, ops procedures, known limitations).
- `docs/SLOs.md` — formal availability / latency / error-rate / freshness SLOs with error-budget burn-rate tiers.
- `deploy/grafana/dashboards/chronos-overview.json` — reference Grafana dashboard (signals, observations, HTTP, webhooks, adapters).
- `docs/wire-contract.md` — "Transport parity" section explaining HTTP-string ↔ gRPC enum mapping and metric-key parity; `change_point` Pattern entry; SSE replay protocol.
- gRPC: new `PATTERN_TYPE_CHANGE_POINT = 9` enum value in `api/proto/chronos/v1/chronos.proto`; `client.PatternTypeChangePoint` constant.
- Config: `CHRONOS_CHANGEPOINT_MIN_SHIFT`, `CHRONOS_CHANGEPOINT_MIN_POINTS`.
- `CHANGELOG.md`.

### Changed
- `docs/configuration.md` — added `CHRONOS_GRPC_PORT` / `CHRONOS_GRPC_HOST` rows; clarified that `CHRONOS_API_TOKEN` gates both transports; documented `--grpc-port` flag override.
- `docs/backlog.md` — gRPC moved to "Recently shipped"; stale work-in-progress stub removed.
- `README.md` — API section split into HTTP and gRPC subsections; quickstart shows `--grpc-port`.

## [Released via GoReleaser]

Prior releases are tracked as GitHub Releases. Notable feature waves:

- **Eight detectors GA**: Recurrence, Trend, Spike, Drop, Stall, Anomaly, Seasonality, Correlation.
- **Multi-backend storage** per [Mnemos ADR-0001](https://github.com/felixgeelhaar/Mnemos/blob/main/docs/adr/0001-multi-backend-storage.md): `memory`, `sqlite`, `postgres`, `mysql`/`mariadb`, `libsql`. Wire-compatible engines (CockroachDB, YugabyteDB, Neon, Crunchy Bridge, TimescaleDB, AlloyDB Omni; PlanetScale, TiDB, Vitess; Turso) work through the native providers unchanged.
- **gRPC transport** alongside HTTP, with `Ingest` (client-streaming) and `ListSignals`. Schema in `api/proto/chronos/v1/chronos.proto`. Bearer auth shared with HTTP.
- **Push notifications**: webhooks (HMAC-SHA256-signed) and Server-Sent Events.
- **In-process detection scheduler** (`CHRONOS_DETECTION_INTERVAL`) — required for SSE to receive events.
- **VEX security audit** complete; nox baseline tracked in-tree.
