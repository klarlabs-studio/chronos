# Architecture

This document describes the engine's layout, the contracts at each boundary, and the design principles that hold the codebase together. It complements [`AGENTS.md`](../AGENTS.md) (working conventions), [`CLAUDE.md`](../CLAUDE.md) (machine-oriented guidance), and [`docs/cognitive-stack.md`](cognitive-stack.md) (Chronos's role next to Mnemos and agent runtimes).

## Intent

Chronos is the **Time / Pattern Perception** layer of the cognitive stack. It accepts time-series observations from any domain via an adapter, runs them through a fan-out of detectors, and emits structured signals.

The full Intent — principles, detector acceptance criteria, hardening priorities, and non-goals — lives in [`intent.md`](intent.md). Temporal ordering and duplicate semantics are in [`temporal-contract.md`](temporal-contract.md).

Two design rules everything else follows:

1. **Signals, not opinions.** Chronos perceives; agent runtimes and other downstream consumers interpret. There is no Title/Summary/Suggestion, no dismissal, no feedback. Domain types carry only structured perception.
2. **The engine knows nothing about the domain it serves.** All domain knowledge enters through `chronos.Source` implementations that live in their own repositories. `internal/domain` has no imports outside the standard library, `github.com/google/uuid`, and `chronos` itself.

## Layered design (DDD / hexagonal)

Layers run from inside (pure) to outside (I/O):

```
                                ┌────────────────────────────┐
                                │       cmd/chronos          │  CLI / process boundary
                                │  (errors, slog, exit codes)│
                                └────────────────────────────┘
                                              │
                ┌─────────────────────────────┼─────────────────────────────┐
                ▼                             ▼                             ▼
        ┌───────────────┐           ┌───────────────────┐           ┌──────────────┐
        │  (out-of-tree)│           │ internal/api      │           │  client/     │
        │  chronos.Source│          │ HTTP transport    │           │  HTTP SDK    │
        │  adapters     │           │ + DTO conversion  │           └──────────────┘
        └───────────────┘           └───────────────────┘
                │                             │
                ▼                             ▼
                       ┌──────────────────────────────┐
                       │ internal/pipeline            │  orchestration
                       │  Compute(ctx, ComputeInput)  │
                       └──────────────────────────────┘
                                       │
              ┌───────────────────────┬┴──────────────────────────┐
              ▼                       ▼                           ▼
   ┌─────────────────────┐  ┌─────────────────────┐   ┌────────────────────────┐
   │ internal/detect     │  │ internal/similarity │   │ internal/store         │
   │ Engine + Detector   │  │ Cosine, weighted,   │   │ Open(dsn)              │
   │ (recurrence, ...)   │  │ Euclidean (math)    │   │ memory/sqlite/pg/mysql │
   └─────────────────────┘  └─────────────────────┘   └────────────────────────┘
                                       │                           │
                                       └─────────────┬─────────────┘
                                                     ▼
                                       ┌──────────────────────────┐
                                       │ internal/ports           │
                                       │ EntityStateRepository    │
                                       │   (Ingest + Save)        │
                                       │ SignalRepository         │
                                       │   (Save/List/Get/Count)  │
                                       │ Source                   │
                                       └──────────────────────────┘
                                                     │
                                                     ▼
                                       ┌──────────────────────────┐
                                       │ internal/domain          │
                                       │ Signal, Evidence,        │
                                       │ TimeWindow, PatternType, │
                                       │ Validate, Normalise      │
                                       └──────────────────────────┘
                                                     ▲
                                                     │ (re-uses)
                                                     │
                                       ┌──────────────────────────┐
                                       │ chronos (top-level pkg)  │
                                       │ EntityState, Source,     │
                                       │ Register, Get, Adapters  │
                                       └──────────────────────────┘
```

### Two public packages, two audiences

- **`chronos`** is the *adapter SDK*. It exposes `EntityState` (the input shape) and `Source` (the inbound port) plus a process-wide registry.
- **`client`** is the *HTTP SDK* for consumers (agent runtimes, dashboards) reading signals from a running server.

`internal/domain.Signal` is deliberately private — wire shape lives in `internal/api.SignalDTO` and `client.Signal`. The engine's representation can evolve without breaking either audience.

### Per-aggregate repositories

Persistence is split by aggregate per the Interface Segregation Principle. There are two ports:

- `EntityStateRepository` — `Ingest` (single point, for streaming sources), `Save` (batch, transactional), plus `ListByScope`, `ListByEntity`, `DeleteOlderThan`, `Count`.
- `SignalRepository` — `Save`, `List(filter)`, `Get`, `Count`. **No `Dismiss`, no `Active`** — once detected, signals are immutable.

There is **no `FeedbackRepository`** in Chronos. Reviewer feedback lives in Mnemos.

### Why the API does not render prose

The cognitive-stack vision is explicit: Chronos emits signals, consumers interpret them, presentation surfaces vary. Putting prose at the API boundary would couple Chronos to a single language, audience, and surface. Instead, the wire shape carries structured fields (`Pattern`, `Strength`, `Confidence`, `Metrics`, numeric `Explanation`); consumers compose copy as they see fit.

## Engine semantics

### Detector contract

Each detector implements:

```go
type Detector interface {
    Pattern() domain.PatternType
    Detect(ctx context.Context, scopeID uuid.UUID, states []chronos.EntityState) []domain.Signal
}
```

The Engine groups input states by scope, sorts each group ascending by timestamp, and calls every registered detector. Each emission is stamped with a content-addressed `PerceptionID` (UUID v5 of scope, series, pattern, window, and pairwise partner) so a second detect over the same window upserts. Output is sorted by `detected-at` descending, then `confidence` descending, and capped at `cfg.MaxSignalsPerRun`. Truncation and per-detector latency/skip/emit counts are recorded on the optional metrics registry. `internal/ports.SignalRepository.List` returns results in the same order.

### Recurrence (available)

- For each entity's most recent state, look at *strictly historical* states of *other* entities in the same scope.
- Cosine similarity ≥ `SimilarityThreshold` qualifies as a similar peer.
- At least `MinSampleSize` peers are required to emit a signal.
- `Strength = avg(peer similarity)`.
- `Confidence = strength × min(samples / 5, 1)` — diminishing returns above five samples are intentional.
- Evidence kind `similar_state`, with `metrics["outcome_diff"] = peer.outcome - subject.outcome`.

### Other available detectors

- `Trend` — OLS linear regression on outcome vs ordinal index. Trigger: `|slope| > TrendMinSlope ∧ R² > 0.3 ∧ n >= TrendMinPoints`. Strength = R²; metrics carry slope, intercept, R², n. Evidence kind `regression_summary`.
- `Spike` / `Drop` — z-score of the most recent outcome against the previous `SpikeWindow` points. Trigger: `|z| >= threshold` in the configured direction. Strength = `min(|z|/5, 1)`. Confidence is derived from the evidence rather than the magnitude: `support × quietness × margin`, where support is `min(n / (CONFIDENCE_STRONG × (SpikeWindow+1)), 1)`, quietness is `1 − ½·stddev/(|mean| + stddev)`, and margin ramps from `½` at the trigger threshold to `1` once `|z|` is 25% past it. Support saturates exactly where `ConfidenceClass` says `strong`, so the number and the class cannot contradict each other. Metrics carry z, baseline mean/stddev. Evidence kind `baseline_deviation`.
- `Stall` — normalised stddev of outcomes below `StallMaxStdDev` over at least `StallMinPoints`. Strength reflects flatness (1 - normalised_stddev / threshold). Evidence kind `variance_window`.
- `Anomaly` — the cross-entity dual of Recurrence. For each entity's most recent state, cosine-compare to peers' most recent states; emit when the *highest* peer similarity is below `AnomalyMaxSimilarity` (subject is isolated). Strength = `1 - max_similarity`. Evidence kind `peer_distance`, one per peer. Window is degenerate: `Start == End == subject.Timestamp`, since Anomaly is a snapshot in time across peers rather than an interval. Consumers computing window duration must special-case this pattern.
- `Seasonality` — autocorrelation peaks. Computes Pearson autocorrelation at lags `[MinPeriod, n/2]` and emits when the largest peak exceeds `SeasonalityMinAutocorr`. Strength = the peak value; metrics carry the period (lag). Evidence kind `autocorrelation_peak`.
- `Correlation` — pairwise Pearson on aligned outcome series within a scope. One signal per pair, deterministically owned by the lex-smaller series ID; the other appears as evidence. Strength = `|r|`; metrics carry r, direction (+1/0/-1). Evidence kind `pair_correlation`. Cost is O(N²) in series count per scope.
- `ChangePoint` — best-split mean-shift. Distinct from Spike/Drop (short-lived deviations). Emits `regime_before` / `regime_after` evidence and `shift` / `delta_mean` metrics.
- `OutlierCluster` — cohort-level: several series in the same scope go anomalous in the same time bucket. `Series` is `uuid.Nil` by contract; members live in evidence (`outlier_member`).
- `CrossScopeCorrelation` — pairwise Pearson across (scope, series) pairs in *different* scopes. Implements `CrossScopeDetector`; the engine runs it once after per-scope detectors. Optional anonymize mode replaces scope/series ids with UUIDv5 hashes.

Each detector defines its own `Evidence.Kind` and `Metrics` keys; the schema is uniform but the semantics are detector-specific. The full list of stable string keys consumers may rely on is in [`wire-contract.md`](wire-contract.md).

## Persistence

| Backend | Driver | When to use |
|---|---|---|
| `memory` | none | Tests, ephemeral exploration |
| `sqlite` | `modernc.org/sqlite` (pure Go) | Single-binary, embedded, local dev |
| `postgres` | `jackc/pgx/v5` | Multi-process production deployments |
| `mysql` / `mariadb` | `go-sql-driver/mysql` | Environments where MySQL is the data plane |
| `libsql` | `tursodatabase/libsql-client-go` | Turso remote or local libSQL files |

### Schema (single migration)

Two aggregates plus evidence:

- `entity_states` — observations from adapters.
- `signals` — detected patterns with `pattern`, `series_id`, `window_*`, `strength`, `confidence`, `metrics` (JSON).
- `signal_evidence` — supporting observations with `kind`, `score`, `metrics` (JSON), foreign-keyed to `signals`.

There is no `insights` table, no `insight_feedback`, no `similarities`, no FTS5 vestige — Tier-A simplifications.

### SQLite

- DSN encodes PRAGMAs `foreign_keys(1)`, `journal_mode(wal)`, `busy_timeout(5000)`.
- Migration is embedded via `go:embed migrations/001_initial.sql` and is also the input to sqlc.
- `internal/store/sqlite/sqlcgen/` is generated. To change SQL: edit `sql/sqlite/queries.sql` and/or the migration, then `make sqlc`.

### Postgres

- Hand-written queries (Postgres is the secondary backend).
- Schema is embedded via `go:embed` from `internal/store/postgres/migrations/001_initial.sql`.
- Filters are composed dynamically; predicates are positional placeholders for parametrised execution.

## CLI

The CLI in `cmd/chronos/` is hand-rolled (no Cobra). Each subcommand lives in its own file. Failures return `*ChronosError{Code, Message, Cause, Hint}`; `main` translates them into stderr output and exit codes. `CHRONOS_VERBOSE=1` reveals the underlying cause chain.

Adapters are activated by blank imports in `cmd/chronos/main.go`. Because adapters live in their own repositories, the CLI ships with no adapters baked in; downstream binaries blank-import the adapter packages they need.

## Test strategy

- Standard library `testing` only.
- In-memory SQLite (`Open(":memory:")`) for store integration tests so tests exercise the real driver.
- Domain, similarity, and detector packages are pure — fast unit tests, table-driven where useful.
- API tests use `httptest.Server` over the in-memory store to cover the transport+filter path end-to-end.

## What is *not* here (and why)

- **No interpretation in the engine.** No "this is a problem" or "consider doing X." Those are decisions; agent runtimes own decisions.
- **No reviewer feedback or dismissal.** Feedback updates knowledge; that lives in Mnemos. Dismissal is a UI/orchestration concern that lives in the consumer.
- **No Cobra**; the CLI is small enough that a hand-rolled dispatch is clearer.
- **No DI container**; constructors take their dependencies directly; main wires everything.
- **No event sourcing or CQRS**; the engine's writes are simple aggregates; reads are direct queries.
- **No third-party logger**; `log/slog` is sufficient. Libraries return errors instead of logging.
