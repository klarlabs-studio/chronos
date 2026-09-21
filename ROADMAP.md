# Chronos Roadmap

Chronos is the **time / pattern perception** layer of the cognitive stack (Mnemos → Chronos → agent runtimes). The engine is feature-complete for the v1 contract; this roadmap covers what comes next.

The authoritative product Intent — principles, detector acceptance criteria, and hardening priorities — is [`docs/intent.md`](docs/intent.md).

## Status (September 2026)

### ✅ Shipped

**Core engine.**
- `internal/domain` — `Signal`, `Evidence`, `TimeWindow`, `PatternType`, validation, normalization.
- `internal/detect` Engine + fourteen detectors: Recurrence, Trend, Spike, Drop, Stall, Anomaly, Seasonality, Correlation, ChangePoint, OutlierCluster, Oscillation, Divergence, Convergence, plus CrossScopeCorrelation.
- `internal/pipeline.Compute` — orchestration: fetch → save → detect → save signals.
- `internal/similarity` — cosine, weighted, Euclidean.

**Hardening (Intent P0 / P1).**
- `EntityState.Validate` rejects nil observation ID, nil entity/scope, zero timestamp, non-finite features, and blank labels.
- Adversarial numerical coverage for all fourteen detectors (`internal/detect/adversarial_test.go`).
- Storage backend conformance suite (`internal/store/conformance`) run by memory, SQLite, libSQL, Postgres, and MySQL.
- Temporal contract documented in [`docs/temporal-contract.md`](docs/temporal-contract.md). Detector time taxonomy in [`docs/temporal-semantics.md`](docs/temporal-semantics.md): pairwise detectors align on timestamps; Divergence/Convergence slopes are per hour; Seasonality requires a regular cadence.

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

Shipped Intent hardening and the shape-catalog detectors are on `main`
(#90–#92). Remaining Chronos-side scope is thin:

### 1. Contract consistency (ongoing)

Keep README, package comments, ADRs, CI claims, and
[`docs/wire-contract.md`](docs/wire-contract.md) aligned with executable
behavior. Treat documentation drift as a defect. Prefer Observation →
Detection → Signal language.

### 2. Capability ports (deferred)

`ports.TextSearcher` and `ports.VectorSearcher` remain unused. Implement
them only when a detector actually needs FTS or embeddings (Intent:
statistical methods remain the default).

### 3. Further detectors

Any new pattern must meet the
[detector acceptance criteria](docs/intent.md#detector-acceptance-criteria).
“No signal” under degenerate input is required. Plateaus are covered by
Stall; Oscillation / Divergence / Convergence are shipped.

## Non-goals

- Becoming a TSDB. Chronos persists what it must to detect; it is not a Prometheus / VictoriaMetrics replacement.
- Becoming an alerting system. Chronos emits signals; alerting is downstream.
- Adding domain-specific detectors. The engine is domain-agnostic by design — detectors must work generically over `EntityState` features.
- Absorbing RCA, remediation, dashboards, LLM reasoning, or recommendation engines. Those may consume Chronos; they do not belong in it. Full list: [`docs/intent.md`](docs/intent.md#non-goals).

## Versioning policy

Chronos follows [Semantic Versioning](https://semver.org/). The wire contract documented in `docs/wire-contract.md` is the stability boundary; renaming any documented Pattern, Evidence.Kind, or metric key is a major-version change.
