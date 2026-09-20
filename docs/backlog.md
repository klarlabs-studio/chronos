# Backlog

Open items not yet scoped into a release. Closed items move out of this file once shipped.

Authoritative Intent and hardening priorities: [`intent.md`](intent.md).

## Recently shipped

- **Chronos Intent** — [`docs/intent.md`](intent.md) as the product north star; [`docs/temporal-contract.md`](temporal-contract.md) for ordering and duplicate semantics; `EntityState.Validate` rejects nil observation IDs.
- **Injectable `store.Registry`** — isolated provider sets via `NewRegistry` / `Clone` / `embed.WithStoreRegistry`; package-level `Register` / `Open` still panic on duplicate schemes.
- **Equal-timestamp total order** — Engine and peer detectors sort / pick "most recent" by `(timestamp, observation ID)`.
- **Recurrence fail-closed on ragged / zero-norm vectors** — same rule as Anomaly.
- **Oscillation, Divergence, Convergence** — Intent shape-catalog detectors; plateaus covered by Stall.
- **Adversarial coverage** for all fourteen detectors (including Oscillation / Divergence / Convergence).
- **gRPC transport parity** — Additive RPCs: `IngestBatch`, `StreamSignals`, `ValidateConfig`, `ExportFederation`, plus `since_cursor` / `next_cursor` on `ListSignals`. Unary `Ingest` unchanged. Schema in `api/proto/chronos/v1/chronos.proto`.
- **Detector explainability** — all detectors populate `Signal.Explanation`.
- **Scheduler same-window skip** plus **content-addressed `PerceptionID`** (UUID v5). Unchanged windows upsert; growing `window.End` still emits a new row.
- **Public Go client HTTP and gRPC parity** — `ListPage`, `Scopes`, `IngestBatch`, `FederationExport`, `ValidateConfig`, `Stream`.
- **Default `CHRONOS_MAX_SIGNALS=100`**.
- **Per-detector observability** — latency, emit, skip, and truncation counters labelled by pattern.
- **OutlierCluster persist** and MySQL explanation / `ScopeIDs` parity.
- **Input invariants, store conformance** (0.17.0).

## Open

### Capability ports unused

`ports.TextSearcher` and `ports.VectorSearcher` are declared for forward compatibility and have no implementations or callers. Leave them until a detector actually needs full-text or embedding search; do not add unused store implementations.
