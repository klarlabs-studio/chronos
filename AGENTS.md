# AGENTS.md

## Project intent

Chronos is the **Time / Pattern Perception** layer of the cognitive stack ([Mnemos / **Chronos** / agent runtimes](docs/cognitive-stack.md)). It ingests time-series observations from any source and emits **signals** — structured records describing patterns: `Recurrence`, `Trend`, `Spike`, `Drop`, `Stall`, `Anomaly`, `Seasonality`, `Correlation`, `ChangePoint`, `OutlierCluster`, `CrossScopeCorrelation`, `Oscillation`, `Divergence`, `Convergence`.

The single hardest rule: **signals, not opinions.** Chronos perceives; it does not interpret. There is no Title, no Summary, no Suggestion, no dismissal workflow, no feedback. Those concerns belong to agent runtimes (decisions) and Mnemos (knowledge), respectively.

The second hardest rule: **the core engine knows nothing about the domain.** Athletes, servers, sensors — all enter through `chronos.Source` as undifferentiated `EntityState` records.

Full Intent (principles, detector acceptance criteria, hardening priorities, non-goals): [`docs/intent.md`](docs/intent.md). Temporal semantics: [`docs/temporal-contract.md`](docs/temporal-contract.md).

## Architecture

```
chronos/                       Public adapter SDK: EntityState, Source, registry
client/                        Public HTTP-API SDK for consumers (agent runtimes, dashboards)
embed/                         In-process engine API (Process / Detect / Query)
cmd/chronos/                   CLI: main.go + one file per subcommand + errors.go
internal/
  domain/                      Private domain model: Signal, Evidence, TimeWindow,
                               PatternType (Recurrence/Trend/Spike/Drop/Stall/...),
                               Validate, Normalise
  ports/                       Outbound interfaces (one file): Source,
                               EntityStateRepository, SignalRepository
  config/                      Env-var driven config + Validate
  similarity/                  Cosine, weighted cosine, Euclidean (pure math)
  detect/                      Detectors + Engine that fans observations out
  pipeline/                    Orchestration: fetch → save observations → detect → save signals
  api/                         HTTP REST + gRPC transport + DTO conversion
  notify/                      Webhooks, SSE, durable outbox
  store/                       Persistence factory (Open dispatches on dbType)
    memory/                    In-memory backend (test default)
    sqlite/                    modernc.org/sqlite-backed; sqlcgen subpkg holds generated code
    postgres/                  PostgreSQL backend (hand-written queries)
    mysql/                     MySQL / MariaDB backend
    libsql/                    Turso / local libSQL (reuses sqlite repos)
sql/sqlite/                    sqlc query file
docs/                          Intent, temporal contract, architecture, cognitive-stack,
                               adapter authoring, configuration, wire contract
```

## Public surface

- **`chronos`** — adapter authors import `chronos.EntityState` and implement `chronos.Source`. Adapters self-register via `init()` calling `chronos.Register(src)`.
- **`client`** — consumers (agent runtimes, dashboards) import `client.New(...)` and use the fluent builder (`c.Signals().Scope(id).Pattern(client.PatternTypeRecurrence).MinConfidence(0.7).List(ctx)`).

Anything under `internal/` is private and may change without notice.

## Conventions

- **Go 1.25+**, minimal external deps: `google/uuid`, `jackc/pgx/v5`, `go-sql-driver/mysql`, `modernc.org/sqlite` (pure Go — no CGO).
- Numeric features are always `[]float64`. Timestamps are `time.Time` (RFC3339Nano on disk). IDs are `uuid.UUID`.
- **Last feature is the outcome metric**, higher is better. Adapters that violate this convention will produce signals whose evidence metrics (`outcome_diff`, `slope`) are inverted.
- `internal/store/sqlite/sqlcgen/` is sqlc-generated; **do not hand-edit** under normal circumstances. To change SQL: edit `sql/sqlite/queries.sql` and/or `internal/store/sqlite/migrations/001_initial.sql`, then `make sqlc`.
- Tests: standard library only; in-memory SQLite for store integration tests via `Open(":memory:")`. Table-driven where it helps.
- Errors: wrap with `fmt.Errorf("...: %w", err)` in libraries; the CLI returns `*ChronosError` carrying an `ExitCode` and optional `Hint`.
- Logging: `log/slog`. Libraries do not log; only the CLI and HTTP server emit log lines.

## Building

```bash
make build      # ./bin/chronos with version/commit/buildDate baked in
make test       # go test -race -count=1 ./...
make check      # fmt + vet + test
make sqlc       # regenerate internal/store/sqlite/sqlcgen
```

## Adding a pattern detector

1. Add a file under `internal/detect/` (e.g. `trend.go`) implementing the `Detector` interface.
2. Add the corresponding `PatternType` constant in `internal/domain/types.go` (most are reserved already).
3. Wire the detector into `detect.DefaultDetectors`.
4. Add per-detector config knobs in `internal/config/config.go` (`CHRONOS_<DETECTOR>_*`).
5. Document the detector's evidence shape (the `Kind` value and `Metrics` keys) in [`docs/wire-contract.md`](docs/wire-contract.md).
6. Add unit tests covering the trigger and the no-trigger paths.

## Adding an adapter

Adapters live **out-of-tree** in their own repositories. They import `github.com/felixgeelhaar/chronos` as a library.

1. Create a new Go module (e.g. `github.com/felixgeelhaar/ascend`).
2. Implement `chronos.Source` (`Name`, `Fetch`).
3. Call `chronos.Register(&Source{})` in `init()`.
4. In your own `main.go` (or the Chronos CLI fork you ship), add a blank import so `init()` runs:
   ```go
   import _ "github.com/felixgeelhaar/ascend"
   ```
5. Document the required `cfg` keys (e.g. `coach_id`).

See [`felixgeelhaar/ascend`](https://github.com/felixgeelhaar/ascend) for a complete example.
