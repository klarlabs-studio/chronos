# Chronos Roadmap

Chronos is the **time / pattern perception** layer of the cognitive stack (Mnemos → Chronos → agent runtimes). The engine is feature-complete for the v1 contract; this roadmap covers what comes next.

The authoritative product Intent — principles, detector acceptance criteria, and hardening priorities — is [`docs/intent.md`](docs/intent.md).

## Status (September 2026)

### ✅ Shipped

**Core engine.**
- `internal/domain` — `Signal`, `Evidence`, `TimeWindow`, `PatternType`, validation, normalization.
- `internal/detect` Engine + eleven detectors: Recurrence, Trend, Spike, Drop, Stall, Anomaly, Seasonality, Correlation, ChangePoint, OutlierCluster, plus CrossScopeCorrelation.
- `internal/pipeline.Compute` — orchestration: fetch → save → detect → save signals.
- `internal/similarity` — cosine, weighted, Euclidean.

**Hardening (Intent P0 / P1).**
- `EntityState.Validate` rejects nil observation ID, nil entity/scope, zero timestamp, non-finite features, and blank labels.
- Adversarial numerical coverage for all eleven detectors (`internal/detect/adversarial_test.go`).
- Storage backend conformance suite (`internal/store/conformance`) run by memory, SQLite, libSQL, Postgres, and MySQL.
- Temporal contract documented in [`docs/temporal-contract.md`](docs/temporal-contract.md).

**Storage (per [Mnemos ADR-0001](https://github.com/felixgeelhaar/Mnemos/blob/main/docs/adr/0001-multi-backend-storage.md)).**
- `memory://` — in-process backend for tests.
- `sqlite://` — pure-Go (`modernc.org/sqlite`), default for single-process deployments.
- `postgres://` — production multi-process backend; verified compat with CockroachDB, YugabyteDB, Neon, Crunchy Bridge, TimescaleDB, AlloyDB Omni.
- `mysql://` / `mariadb://` — verified compat with PlanetScale, TiDB, MariaDB, Vitess.
- `libsql://` — Turso remote and local libSQL files.

**Transports.**
- HTTP REST (`internal/api`) — `/v1/ingest`, `/v1/signals`, `/v1/signals/{id}`, `/v1/signals/stream` (SSE), `/health`, `/metrics`.
- gRPC (`internal/api/grpc`) — `Ingest` (unary), `IngestBatch`, `ListSignals` (`since_cursor`), `GetSignal`, `StreamSignals`, `ValidateConfig`, `ExportFederation`. Schema in `api/proto/chronos/v1/chronos.proto`. Runs alongside HTTP on a separate port.
- Webhook push (`CHRONOS_WEBHOOK_URLS`) with HMAC-SHA256 signing.

**Infra.**
- Bearer-token auth on HTTP and gRPC (shares `CHRONOS_API_TOKEN`).
- Conventional Commits, golangci-lint clean, race-tests green on Go 1.25 (CI matrix).
- GoReleaser — Docker images and GitHub Release archives.
- coverctl per-domain coverage gating; nox security baseline.

**Adapters.**
- The engine is domain-agnostic. `chronos.Source` is the seam; adapters live in the repo that owns the domain. Verified integration: [`felixgeelhaar/ascend`](https://github.com/felixgeelhaar/ascend) (athlete training weeks).

## Next

Shipped items from earlier roadmap slices stay checked in git history; they are no longer open work. Remaining scope:

### 1. Intent P2 — contract consistency

Keep README, package comments, ADRs, CI claims, and [`docs/wire-contract.md`](docs/wire-contract.md) aligned with executable behavior. Treat documentation drift as a defect. Prefer Observation → Detection → Signal language; do not reintroduce “insight / alert / recommendation” vocabulary in engine docs.

### 2. Intent P2 — embeddability

Injectable `chronos.Registry` is shipped. Remaining: dogfood `embed.Engine`
from `cmd/chronos compute`, and optionally isolate the store-provider
registry the same way. See [`docs/adr/0001-embeddable-engine-api.md`](docs/adr/0001-embeddable-engine-api.md).

### 3. Capability ports

`ports.TextSearcher` and `ports.VectorSearcher` remain unused. Implement them only when a detector actually needs FTS or embeddings (Intent: statistical methods remain the default).

### 4. Adapter ecosystem (community-driven)

The point of the no-adapters-in-Chronos rule is that adapters live close to their domain. Anticipated near-term integrations from neighbouring projects:

- **Mnemos action+outcome stream** — feed Mnemos's recorded outcomes into Chronos as a metric series; pattern detection on the outcome.
- **decisionkit risk score over time** — feed [decisionkit](https://github.com/felixgeelhaar/decisionkit) risk-score time series; detect "risk piling up" patterns. (Nous owned this when it was a live service; it is archived.)

Both are out-of-tree adapters. This roadmap tracks them only as expected use cases — implementation belongs to the consuming repo.

### 5. New detectors

Only after the hardening track above. Each candidate must meet the [detector acceptance criteria](docs/intent.md#detector-acceptance-criteria) in the Intent. “No signal” under degenerate input is required.

## Non-goals

- Becoming a TSDB. Chronos persists what it must to detect; it is not a Prometheus / VictoriaMetrics replacement.
- Becoming an alerting system. Chronos emits signals; alerting is downstream.
- Adding domain-specific detectors. The engine is domain-agnostic by design — detectors must work generically over `EntityState` features.
- Absorbing RCA, remediation, dashboards, LLM reasoning, or recommendation engines. Those may consume Chronos; they do not belong in it. Full list: [`docs/intent.md`](docs/intent.md#non-goals).

## Versioning policy

Chronos follows [Semantic Versioning](https://semver.org/). The wire contract documented in `docs/wire-contract.md` is the stability boundary; renaming any documented Pattern, Evidence.Kind, or metric key is a major-version change.
