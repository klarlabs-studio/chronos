# ADR 0001: Expose an embeddable Engine API

- **Status:** Accepted (implemented as `embed/`)
- **Date:** 2026-05-31
- **Updated:** 2026-09-20
- **Deciders:** Felix Geelhaar
- **Scope:** Chronos public Go package surface.

## Context

Chronos today is shaped as a CLI + HTTP service. The public Go package
(`chronos.go`) intentionally exposes the adapter contract:
`EntityState`, `Source` interface, and an adapter registry. All
engine logic — pattern detection, signal generation, persistence — lives
under `internal/` and is reachable only via the HTTP API or the
`cmd/chronos compute` CLI subcommand.

That shape worked when Chronos was always deployed as its own process.
It does not work for the cognitive-stack direction: Mnemos and other Go
consumers need an in-process API without spinning up HTTP or importing
`internal/`.

## Decision

Chronos exposes an embeddable engine as the **`embed` package**
(`github.com/felixgeelhaar/chronos/embed`), not as types on the root
`chronos` package. The root package stays the adapter SDK; `embed` is
the in-process product surface.

Public surface:

- `embed.Engine` — the embeddable engine handle.
- `embed.New(opts ...Option) (*Engine, error)` — constructor.
- `Engine.Process(ctx, EntityState) error` — ingest one observation.
- `Engine.ProcessBatch(ctx, []EntityState) error` — ingest a batch.
- `Engine.Detect(ctx, scopeIDs []uuid.UUID) ([]chronos.Signal, error)` —
  run detection synchronously.
- `Engine.Query(ctx, QueryOpts) ([]chronos.Signal, error)` — fetch stored
  signals filtered by scope / pattern / window / confidence.
- `Engine.Close() error` — release storage handle.
- Option builders: `WithStorage(dsn)`, `WithDetectionConfig(cfg)`,
  `WithLogger(*slog.Logger)`, `WithDetectors(...)`, `WithParallelDetectors()`.
- Public type aliases: `chronos.Signal`, `PatternType`, `TimeWindow`,
  `Evidence` re-exported from `internal/domain` via `chronos/signal.go`.

The new types and methods are thin wrappers around existing internal
packages. No detector or storage logic moves; only the construction
surface is publicised.

### Adapter registry isolation (Intent P2)

`chronos.Registry` is an injectable value type (`NewRegistry`,
`Register` / `Get` / `Adapters` methods). Package-level
`chronos.Register` / `Get` / `Adapters` delegate to
`chronos.DefaultRegistry()` for convenience. Multiple independently
configured Chronos embeddings in one process should each own a
`Registry` rather than sharing the default.

## Consequences

**Positive:**

- Mnemos and other Go runtimes can embed Chronos without HTTP overhead.
- The public API stabilises against the `cognitive-stack/library-first`
  direction.
- Isolated registries avoid process-global adapter collisions.

**Negative / risks:**

- The public API is a versioned contract. Future detector or storage
  changes need to preserve `embed.Engine` semantics.
- `Register` is last-write-wins on duplicate names (ADR decision).
- `Engine` does not support multiple writers concurrently against the
  same scope. Documented single-writer expectation.

## Public API contract

Covered by semantic versioning:

1. `embed.New`, `Process`, `ProcessBatch`, `Detect`, `Query`, `Close`
   and their parameter shapes are stable.
2. `Option` and the `With*` builders may grow but won't be removed
   without a major bump.
3. `Signal`, `PatternType`, `TimeWindow`, `Evidence` re-exports follow
   the internal source of truth; field additions are backward compatible,
   field removals are major bumps.
4. `internal/*` is **not** part of the public API.

## Implementation status

| Step | Status |
|---|---|
| `chronos/signal.go` re-exports | Done |
| `embed.Engine` + options | Done |
| `Register` last-write-wins | Done |
| Injectable `chronos.Registry` | Done |
| Lifecycle tests against memory store | Done |
| Refactor `cmd/chronos compute` to dogfood `embed` | Open |
| Store provider registry isolation | Open (lower priority) |

## Alternatives Considered

**1. Keep the public API minimal; require HTTP for all engine
interaction.** Rejected. Forces every Go consumer to run an in-process
HTTP server.

**2. Expose `internal/*` via replace directives.** Rejected. Brittle.

**3. Put `Engine` on the root `chronos` package.** Rejected in favour of
`embed/` so the root package stays the small adapter SDK and embedding
dependencies (storage providers, config) stay opt-in.

## Related Work

- [`docs/intent.md`](../intent.md) — embedding is first-class (principle 9)
- [Mnemos ADR 0003-0006 (cognitive-stack simplification)](https://github.com/klarlabs-studio/mnemos/blob/main/docs/adr/)
