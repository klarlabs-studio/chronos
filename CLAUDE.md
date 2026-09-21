# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Common Commands

```bash
make build         # Build ./bin/chronos with version/commit/buildDate ldflags
make test          # go test -race -count=1 ./...
make check         # fmt + vet + test
make lint          # golangci-lint run ./...
make sqlc          # Regenerate internal/store/sqlite/sqlcgen from sql/sqlite/queries.sql
make clean         # Remove ./bin and *.db

# Run a single test
go test -race -run TestRecurrence ./internal/detect
go test -race -run '^TestSQLite_' ./internal/store/sqlite
```

CI (`.github/workflows/ci.yml`) runs `go test -race -count=1`, `golangci-lint`, and `make build` on Go 1.25. The codebase targets Go 1.25 (`go.mod`); match locally before pushing.

## Project intent

Chronos is the **Time / Pattern Perception** layer of the cognitive stack (Mnemos → Chronos → agent runtimes). It ingests time-series observations and emits structured **signals** — `Recurrence`, `Trend`, `Spike`, `Drop`, `Stall`, `Anomaly`, `Seasonality`, `Correlation`, `ChangePoint`, `OutlierCluster`, `CrossScopeCorrelation`, `Oscillation`, `Divergence`, `Convergence`. Each signal carries Pattern, Strength (intensity), Confidence (sureness), Window, Evidence, and Metrics.

Two non-negotiable rules:

1. **Signals, not opinions.** Chronos perceives; agent runtimes interpret. No Title/Summary/Suggestion in domain or wire types. No dismissal, no feedback, no IsActive. Those are agent-runtime and Mnemos concerns.
2. **The core engine knows nothing about the domain.** Domain knowledge enters only through adapters that produce `chronos.EntityState`. If domain-specific code starts leaking into `internal/` or the top-level `chronos` package, that is a design break.

See [`docs/intent.md`](docs/intent.md) for the full Intent (principles, detector acceptance criteria, hardening priorities). Temporal semantics: [`docs/temporal-contract.md`](docs/temporal-contract.md). Cognitive-stack composition: [`docs/cognitive-stack.md`](docs/cognitive-stack.md).

## Architecture

Public surface (stable):

- **Top-level `chronos` package** — `EntityState`, `Source` interface, registry (`Register`, `Get`, `Adapters`). Adapter authors import this.
- **`client/`** — HTTP-API SDK for consumers of a running Chronos server. Functional options (`WithToken`, `WithLogger`, `WithTimeout`, `WithUserAgent`) and a fluent builder (`c.Signals().Scope(id).Pattern(...).List(ctx)`).

Private (everything under `internal/`, may change without notice):

```
(out-of-tree      →  internal/pipeline   →  internal/store/...   →  internal/api
 chronos.Source)     orchestrator wired       per-aggregate           HTTP transport
 from cmd/chronos         repositories             + DTO conversion
       ↓
 internal/detect.Engine
       ↓
 detectors (one per PatternType)
```

Layering, in dependency order (inner → outer):

1. `internal/domain` — pure types: `Signal`, `Evidence`, `TimeWindow`, `PatternType`, plus `Validate` / `Normalise`. No I/O. No prose.
2. `internal/ports` — outbound interfaces in one file: `EntityStateRepository` (with both `Ingest` and batch `Save`), `SignalRepository` (`Save`/`List(filter)`/`Get`/`Count`). No `Dismiss`, no `Active`, no `Feedback`.
3. `internal/similarity` — pure math (cosine, weighted, Euclidean).
4. `internal/detect` — detectors + Engine. One file per pattern (`recurrence.go`, `trend.go`, `spike.go`, `drop.go`, `stall.go`, `anomaly.go`, `seasonality.go`, `correlation.go`, `changepoint.go`, `outlier_cluster.go`, `cross_scope_correlation.go`).
5. `internal/store/{memory,sqlite,postgres,mysql,libsql}` — port implementations. SQLite uses sqlc; Postgres/MySQL are hand-written; libSQL reuses the SQLite repositories.
6. `internal/store` (top of subtree) — `Open(ctx, dsn)` factory returns a uniform port-typed `*Conn`.
7. `internal/pipeline` — orchestration: `Compute(ctx, ComputeInput)` runs fetch → save observations → detect → save signals. `Scheduler` ticks detection for the HTTP ingest path.
8. `internal/api` — HTTP transport + `SignalDTO` / `IngestRequest`. gRPC lives in `internal/api/grpc`. Strictly transport — no rendering of human-readable copy.
9. `cmd/chronos` — flat CLI: `main.go` + one file per subcommand + `errors.go` (`ChronosError{Code,Message,Cause,Hint}`).

### Invariants worth knowing

- **Last feature is the outcome metric. Higher is better.** Recurrence emits an `outcome_diff = peer.outcome - subject.outcome` per evidence row; Tier-B detectors will reuse this convention for slope and z-score sign.
- **Scope is the grouping primitive.** Detectors operate within a `ScopeID`. The Engine groups states by scope before fan-out.
- **Self-comparison and forward-looking comparisons are excluded** by the Recurrence detector.
- **Adapter registration is `init()`-driven.** `chronos.Register(src)` panics on a nil source or empty name, so wiring mistakes surface at program start. The CLI imports adapter packages with `_` so their `init()` runs.

### Persistence layout

- **SQLite**: pure-Go `modernc.org/sqlite` driver. PRAGMAs (`foreign_keys`, `journal_mode=wal`, `busy_timeout`) encoded in the DSN. Migrations are embedded via `go:embed migrations/001_initial.sql`. sqlc-generated code lives in `internal/store/sqlite/sqlcgen/`. Two aggregates: `entity_states` and `signals` + `signal_evidence`.
- **Postgres**: hand-written queries. Schema embedded via `go:embed` from `internal/store/postgres/migrations/001_initial.sql`. Same two-aggregate shape.
- **MySQL / MariaDB**: hand-written queries; namespace is a database, not a schema.
- **libSQL**: Turso remote or local files; reuses the SQLite repositories.
- **Memory**: thread-safe in-memory backend used for tests.

To change SQL: edit `sql/sqlite/queries.sql` and/or the relevant migration file, then `make sqlc`.

### Configuration

All env-var driven. Defaults in `config.Default()` (`internal/config/config.go`):

| Variable | Default | Notes |
|---|---|---|
| `CHRONOS_DB_DSN` | unset | Primary DSN (`sqlite:///…`, `postgres://…`, `mysql://…`, `libsql://…`) |
| `CHRONOS_DB_TYPE` | `sqlite` | Legacy: `sqlite` / `postgres` / `memory` |
| `CHRONOS_DB_CONN` | `chronos.db` | Path or DSN |
| `CHRONOS_MAX_SIGNALS` | `100` | Cap per detect run (`0` = unlimited) |
| `CHRONOS_JOB_TIMEOUT` | `10m` | Compute timeout |
| `CHRONOS_SIM_THRESHOLD` | `0.85` | Recurrence: min cosine similarity |
| `CHRONOS_MIN_SAMPLE` | `2` | Recurrence: min peer cases |
| `CHRONOS_TREND_MIN_SLOPE` | `0.05` | Trend: minimum \|slope\| in outcome units per hour |
| `CHRONOS_TREND_MIN_POINTS` | `4` | Trend: min observations (Tier B) |
| `CHRONOS_SPIKE_Z` | `2.5` | Spike: |z-score| threshold (Tier B) |
| `CHRONOS_DROP_Z` | `2.5` | Drop: |z-score| threshold (Tier B) |
| `CHRONOS_SPIKE_WINDOW` | `5` | Spike/Drop rolling baseline size (Tier B) |
| `CHRONOS_STALL_MAX_STDDEV` | `0.05` | Stall: max stddev of normalised outcome (Tier B) |
| `CHRONOS_STALL_MIN_POINTS` | `4` | Stall: min observations (Tier B) |
| `CHRONOS_ANOMALY_MAX_SIM` | `0.5` | Anomaly: max similarity to nearest peer to count as isolated |
| `CHRONOS_ANOMALY_MIN_PEERS` | `2` | Anomaly: minimum peers for cross-entity comparison |
| `CHRONOS_SEASONALITY_MIN_AUTOCORR` | `0.5` | Seasonality: minimum autocorrelation at any lag |
| `CHRONOS_SEASONALITY_MIN_POINTS` | `12` | Seasonality: minimum observations |
| `CHRONOS_SEASONALITY_MIN_PERIOD` | `2` | Seasonality: minimum lag (period) considered, in samples |
| `CHRONOS_SEASONALITY_MAX_INTERVAL_CV` | `0` | Seasonality: max CV of inter-observation intervals; `0` = exact spacing |
| `CHRONOS_ALIGN_TOLERANCE` | `0` | Pairwise alignment tolerance; `0` = exact timestamps |
| `CHRONOS_CORRELATION_MIN` | `0.7` | Correlation: minimum |Pearson r| to emit |
| `CHRONOS_CORRELATION_MIN_POINTS` | `5` | Correlation: minimum aligned pairs |
| `CHRONOS_CHANGEPOINT_MIN_SHIFT` | `1.5` | ChangePoint: minimum standardised mean shift |
| `CHRONOS_CHANGEPOINT_MIN_POINTS` | `8` | ChangePoint: minimum observations |
| `CHRONOS_OUTLIER_CLUSTER_MIN_SERIES` | `3` | OutlierCluster: minimum distinct series in a bucket |
| `CHRONOS_OUTLIER_CLUSTER_Z` | `2.5` | OutlierCluster: per-series \|z\| threshold |
| `CHRONOS_OUTLIER_CLUSTER_WINDOW` | `5m` | OutlierCluster: time-bucket width |
| `CHRONOS_CROSS_SCOPE_MIN` | `0.8` | CrossScopeCorrelation: minimum \|r\| |
| `CHRONOS_CROSS_SCOPE_MIN_POINTS` | `5` | CrossScopeCorrelation: minimum aligned observations |
| `CHRONOS_ANONYMIZE_CROSS_SCOPE` | `false` | Hash scope/series ids on cross-scope signals |
| `CHRONOS_OSCILLATION_MIN_FLIP_RATE` | `0.55` | Oscillation: minimum sign-flip rate |
| `CHRONOS_OSCILLATION_MIN_POINTS` | `6` | Oscillation: minimum observations |
| `CHRONOS_DIVERGENCE_MIN_SLOPE` | `0.05` | Divergence: minimum positive \|a−b\| slope, gap units per hour |
| `CHRONOS_DIVERGENCE_MIN_POINTS` | `5` | Divergence: minimum aligned pairs |
| `CHRONOS_DIVERGENCE_MIN_R2` | `0.5` | Divergence: minimum R² of the gap regression |
| `CHRONOS_CONVERGENCE_MIN_SLOPE` | `0.05` | Convergence: minimum \|negative\| \|a−b\| slope, gap units per hour |
| `CHRONOS_CONVERGENCE_MIN_POINTS` | `5` | Convergence: minimum aligned pairs |
| `CHRONOS_CONVERGENCE_MIN_R2` | `0.5` | Convergence: minimum R² of the gap regression |
| `CHRONOS_CONFIDENCE_ESTABLISHED` | `2.0` | MIN_POINTS multiplier for `established` |
| `CHRONOS_CONFIDENCE_STRONG` | `5.0` | MIN_POINTS multiplier for `strong` |
| `CHRONOS_DETECTOR_PARALLELISM` | `false` | Parallel per-scope detectors |
| `CHRONOS_HTTP_PORT` | `7778` | Serve port |
| `CHRONOS_HTTP_HOST` | `127.0.0.1` | Serve host |
| `CHRONOS_API_TOKEN` | unset | Bearer token; empty disables auth |
| `CHRONOS_GRPC_PORT` | `0` | gRPC port; `0` disables |
| `CHRONOS_DETECTION_INTERVAL` | `0` | In-process scheduler cadence; `0` disables |
| `CHRONOS_VERBOSE` | unset | When set, the CLI prints error causes |

`serve` flags `--port` / `--host` override env. `compute` accepts `--scope-id` (preferred) or `--coach-id` (alias retained for the original Ascend wiring).

## Adding a detector

1. New file in `internal/detect/<pattern>.go`. Implement `Detector`.
2. Add the `PatternType` constant if not already reserved in `internal/domain/types.go`.
3. Register in `detect.DefaultDetectors`.
4. Add config knobs to `internal/config/config.go`.
5. Document evidence `Kind` and `Metrics` keys in [`docs/wire-contract.md`](docs/wire-contract.md).
6. Tests in `internal/detect/<pattern>_test.go` covering trigger + no-trigger.
7. If the pattern compares series, estimates a rate, or claims a period, follow [`docs/temporal-semantics.md`](docs/temporal-semantics.md). Slice position is not a timestamp.

## Conventions

- Go 1.25+. Deps: `google/uuid`, `jackc/pgx/v5`, `go-sql-driver/mysql`, `modernc.org/sqlite`.
- Conventional Commits.
- Tests are stdlib-only and table-driven where useful.
- Wrap errors with `%w`. The CLI uses `*ChronosError` for exit-code routing.
- Logging is `log/slog`. Libraries return errors; only `cmd/` and `internal/api` log.
