# Temporal contract

This document defines how Chronos treats imperfect time-series input. Behavior here must not depend on which storage backend is selected, on map iteration order, or on accidental detector implementation details.

It implements Intent principle 4 ([`intent.md`](intent.md)).

## Vocabulary

| Term | Meaning |
|---|---|
| Observation | One `chronos.EntityState` — a feature vector at a point in time for one entity in one scope |
| Observation ID | `EntityState.ID` — the idempotency key for persistence |
| Series | All observations sharing the same `EntityID` (within a scope for per-scope detectors) |
| Window | The `[Start, End]` interval a detector analysed when emitting a signal |

## Observation identity

- `EntityState.ID` is required and must be non-nil. `Validate` rejects `uuid.Nil`.
- HTTP, gRPC, and MCP mint a new UUID when the wire omits `id`, then construct the `EntityState`. Embedded callers must set `ID` themselves (or generate one before `Process` / `ProcessBatch`).
- Re-ingesting the same observation ID is an **idempotent update** of the mutable payload (`features`, `labels`, `meta`, adapter label). `entity_id`, `scope_id`, and `timestamp` are not relocated by a re-ingest. See the store conformance suite (`EntityState/DuplicateObservationID`).

## Timestamps

- Timestamps are required and must be non-zero. Wire transports that omit it default to `time.Now().UTC()` before constructing an `EntityState`.
- Chronos does **not** reject “extremely old” observations at ingest. Retention (`DeleteOlderThan`) and detection lookback (`ListByScopeSince`) bound what detectors see; age alone is not a validation error.
- Sub-second timestamp fidelity and ordering are part of the store contract. SQLite and libSQL store UTC timestamps as fixed-width TEXT (nine fractional digits) so lexical `ORDER BY` matches chronology. Backends that cannot preserve sub-second order must declare a conformance `Quirk`.

## Ordering

### Persistence → retrieval

- `ListByScope` / `ListByScopeSince` / `ListByEntity` return observations **most recent first** (descending timestamp).
- Relative order among rows that share an identical timestamp is **undefined** at the store layer unless a backend documents a stronger guarantee. The detection Engine re-establishes a total order (see below), so detectors must not depend on store tie-breaking.

### Detection

- The Engine groups observations by `ScopeID`, then sorts each group **ascending by `(timestamp, observation ID)`** before calling detectors. Equal timestamps are broken by lexicographic observation ID so input order is deterministic regardless of ingest or retrieval order.
- Detectors that select a "most recent" observation per entity (`mostRecentByEntity`) use the same total order: equal timestamps prefer the larger observation ID (last in the ascending sort).
- Detectors that key internal maps by series ID (or any other unordered key) must sort those keys before emitting so signal order is deterministic across runs.
- Final Engine output is sorted by `detected-at` descending, then `confidence` descending, then capped at `MaxSignalsPerRun`. Because that sort is stable, non-deterministic emission order would silently change which signals survive truncation — determinism is therefore a correctness property, not a nicety.

### Out-of-order arrival

- Observations may be ingested in any order. Stores persist them; list methods and the Engine re-establish chronological order. Detectors must not assume ingest order.
- Adversarial coverage (`internal/detect/adversarial_test.go`) includes out-of-order and irregular-interval series; detectors either remain correct after the Engine’s sort or emit no signal.

## Duplicate timestamps

- Multiple observations for the same entity at the same timestamp are allowed at the store layer (distinct observation IDs).
- Detectors must not invent semantics for ties. Prefer algorithms that are well-defined on equal timestamps (Trend collapses equal times to a zero-width x-axis and emits no signal) or decline to emit when the situation is ambiguous for that detector’s math.
- Duplicate *observation IDs* are not duplicates in time — they are corrections (see above).

## Sparse and irregular series

- Irregular sampling intervals are first-class. Detectors that assume fixed cadence must document that assumption and fail closed (no signal) when it does not hold, or operate on ordinal index / wall-clock explicitly.
- Sparse series and insufficient history are “no signal” cases. Each detector’s minimum sample count is a config knob; falling short never manufactures confidence.

## Missing features and malformed numerics

- An observation with an empty `Features` slice is rejected by `Validate` (`ErrMissingFeatures`).
- Partial / ragged feature sets across a series are adapter concerns: Chronos does not impute. Peer-comparison detectors (Recurrence, Anomaly) **fail closed** on length-mismatched or zero-norm vectors — cosine similarity is undefined in those cases, so the peer is skipped rather than treated as dissimilar (`Cosine` returning 0). Detectors that operate only on the outcome convention (last feature) are unaffected.
- `NaN`, `+Inf`, and `-Inf` never enter the pipeline (`ErrNonFiniteFeature`). Finite extremes (`MaxFloat64`, denormals, zero) are valid.

## Signals over time

- Detected signals are immutable once persisted. There is no dismissal, edit, or “is active” flag in Chronos.
- Content-addressed `PerceptionID` (UUID v5 of scope, series, pattern, window, and pairwise partner) makes a second detect over the **same** analysis window upsert rather than append.
- On a live stream the analysis window typically slides forward with new observations, so successive ticks produce new rows. Retention and consumer de-duplication are how growth is managed — not in-place revision of past signals.

## Backend independence

Any behavior described here that differs by storage backend is a defect unless declared as a conformance `Quirk` with an inverted assertion in `internal/store/conformance`. Optimisations are welcome; divergent semantics are not.
