# Chronos — Intent

Chronos is a typed time-series perception engine.

Its job is to observe temporal data, identify statistically meaningful patterns, and emit structured, explainable signals that other systems can reason about.

Chronos answers:

> What shape is the data taking over time?

It does not decide what that shape means operationally, whether someone should be alerted, or what action should be taken.

The core boundary is:

```
Source
  ↓
EntityState
  ↓
Detection
  ↓
Signal
  ↓
Downstream interpretation
```

Chronos owns everything through Signal.

Interpretation, policy, alerting, diagnosis, remediation, and automation belong downstream.

---

## Core Principles

### 1. Signals, not opinions

Chronos reports observations about time-series behavior.

A signal should describe:

- the detected pattern;
- the evidence supporting it;
- its strength;
- its confidence;
- the relevant entities, features, and time window.

It should not contain conclusions such as:

- “this is bad”;
- “the service is unhealthy”;
- “this caused the incident”;
- “scale the deployment”;
- “restart the database”.

Those conclusions require context Chronos intentionally does not own.

Chronos produces evidence.

Consumers decide what the evidence means.

### 2. Shape, not state

Chronos is not a monitoring system, alert manager, TSDB, incident-management system, or rules engine.

It detects temporal structure such as:

- trends;
- spikes and drops;
- change points;
- anomalies;
- oscillation;
- seasonality;
- correlations;
- divergence;
- convergence;
- plateaus;
- other statistically defensible temporal patterns.

The distinction matters.

A value being 95 is state.

A value increasing steadily from 40 to 95 is shape.

Chronos is interested in the second.

### 3. Input invariants are part of correctness

Detection quality cannot exceed input quality.

`EntityState` is therefore not merely a transport structure. It is the boundary at which Chronos establishes the mathematical invariants required by its detectors.

Accepted observations must be safe for downstream numerical operations.

At minimum:

- entity identity must be valid;
- scope identity must be valid where required;
- observation identity must have explicit semantics;
- timestamps must be valid;
- feature names must be valid;
- feature values must be finite;
- NaN, +Inf, and -Inf must not enter the detection pipeline.

If Chronos accepts an observation, detectors should be able to assume those invariants hold.

Validation belongs at the boundary rather than being repeatedly rediscovered inside individual detectors.

### 4. Temporal semantics must be explicit

Time-series systems inevitably encounter imperfect temporal data.

Chronos must define deterministic behavior for:

- observations arriving out of order;
- duplicate timestamps;
- duplicate observation IDs;
- sparse series;
- irregular sampling intervals;
- missing features;
- insufficient history;
- extremely old observations;
- multiple observations for the same entity and timestamp.

These behaviors must not depend accidentally on storage implementation, query ordering, map iteration, or detector implementation.

Where ordering matters, ordering must be explicit.

Where ambiguity cannot be resolved safely, Chronos should reject or decline to produce a signal rather than silently invent semantics.

See [`temporal-contract.md`](temporal-contract.md).

### 5. Numerical robustness over detector count

Chronos should prefer a smaller collection of trustworthy detectors over a large collection of superficially impressive ones.

Before adding additional detection algorithms, existing detectors should behave predictably under adversarial numerical conditions including:

- zero variance;
- constant series;
- extremely small values;
- extremely large values;
- floating-point precision boundaries;
- insufficient samples;
- identical vectors;
- empty vectors;
- sparse observations;
- duplicate timestamps;
- irregular intervals.

A detector that cannot establish sufficient evidence should produce no signal rather than manufacture confidence.

“No signal” is a valid result.

### 6. Explainability is a feature

Signals should be inspectable without reproducing the detector internally.

Evidence should make it possible to understand why a signal exists.

Detector output should therefore favor:

- explicit metrics;
- meaningful statistical quantities;
- identifiable observation windows;
- stable evidence kinds;
- documented units and semantics;
- deterministic results where practical.

Opaque scoring should be avoided when an understandable statistical representation can express the same information.

Chronos should make downstream reasoning easier, not require consumers to reverse-engineer its algorithms.

### 7. Confidence has semantics

Confidence must not become a generic “how much we like this result” number.

Every detector producing confidence must have a defensible interpretation for that value.

Likewise, Strength and Confidence represent different concepts and must remain distinct.

Broadly:

- strength describes the magnitude of the observed pattern;
- confidence describes the quality or certainty of the evidence supporting the detection.

Individual detectors may derive these differently, but their meaning must remain compatible with the public contract.

### 8. Storage is infrastructure, not behavior

Chronos supports multiple storage implementations.

The observable behavior of Chronos must not change because a consumer selected SQLite, PostgreSQL, MySQL/MariaDB, libSQL, or another supported backend.

All stores should satisfy the same behavioral contract for:

- persistence;
- retrieval;
- ordering;
- filtering;
- duplicate handling;
- pagination;
- timestamps;
- errors;
- concurrency expectations.

Backend-specific optimizations are welcome.

Backend-specific semantics are not.

A shared conformance suite (`internal/store/conformance`) defines this contract and runs against every supported implementation.

### 9. Embedding is first-class

Chronos is both a service and a Go library.

The embeddable API is therefore a product surface, not an implementation detail.

Library consumers should be able to compose Chronos without depending on:

- process-global mutable configuration;
- HTTP;
- gRPC;
- environment variables;
- hidden initialization ordering.

Convenience globals may exist where useful, but core components should trend toward explicit construction and dependency injection.

In particular, registries and other extensibility mechanisms should eventually support isolated instances so multiple independently configured Chronos engines can coexist safely in one process.

See [`adr/0001-embeddable-engine-api.md`](adr/0001-embeddable-engine-api.md).

### 10. Wire contracts are public API

Chronos’s public contract extends beyond exported Go symbols.

Values crossing HTTP, gRPC, MCP, storage, webhook, or event boundaries are API.

This includes concepts such as:

- pattern identifiers;
- evidence kinds;
- metric names;
- serialized field semantics;
- signal structure.

Changing these can break consumers even when Go compilation succeeds.

They must therefore receive the same compatibility discipline as exported Go APIs.

Stable wire semantics are governed by semantic versioning and documented in [`wire-contract.md`](wire-contract.md).

---

## Extension Philosophy

Chronos should remain extensible without becoming framework-heavy.

Extensions should have:

- small interfaces;
- explicit contracts;
- minimal lifecycle assumptions;
- deterministic registration;
- strong validation;
- clear ownership boundaries.

Capabilities should be introduced when a real detector or integration requires them.

Chronos should not add abstractions merely because they might someday be useful.

In particular, search, vector, embedding, ML, or LLM capabilities should enter the core only when they solve a concrete perception problem better than simpler statistical techniques.

Statistical methods remain the default when they are sufficient.

---

## Detector Acceptance Criteria

A new detector should not be merged solely because it recognizes another named pattern.

It should demonstrate:

1. A clearly defined temporal phenomenon.
2. A defensible algorithm for identifying that phenomenon.
3. Explicit minimum data requirements.
4. Defined behavior for degenerate inputs.
5. Defined strength semantics.
6. Defined confidence semantics.
7. Explainable evidence.
8. Deterministic behavior where practical.
9. Adversarial numerical tests.
10. No unnecessary interpretation of operational meaning.

The burden is not proving that a detector can emit a signal.

The burden is proving that the emitted signal deserves to exist.

---

## Testing Philosophy

Tests should protect behavioral contracts rather than mirror implementation details.

Particular emphasis belongs on:

- numerical edge cases;
- temporal ordering;
- concurrent execution;
- detector determinism;
- serialization;
- wire compatibility;
- store conformance;
- malformed input;
- boundary conditions.

Race testing is part of correctness.

Integration testing is required where behavior depends on an external storage engine.

Every supported storage backend should eventually run the same conformance suite against the real engine.

---

## Documentation Discipline

Documentation must describe the system that exists today.

Historical terminology should not survive after architectural concepts change.

In particular, Chronos consistently uses the language of:

> Observation → Detection → Signal

rather than treating signals as insights, diagnoses, recommendations, or alerts.

README documentation, package comments, API documentation, ADRs, examples, and roadmap claims should agree with executable behavior and CI configuration.

Documentation drift is treated as a defect.

---

## Current Engineering Priorities

Before significantly expanding Chronos’s detector catalog, prioritize hardening the existing engine.

| Priority | Theme | Status |
|---|---|---|
| **P0** | Input invariants — `EntityState` validation; observation IDs; timestamps; finite numerics; malformed feature sets | Done |
| **P1** | Numerical robustness — adversarial coverage; resolve correctness findings (ChangePoint Inf, Anomaly zero-vec, OutlierCluster denormal, two-point correlation) | Done |
| **P1** | Temporal contract — deterministic ordering and duplicate semantics | Done ([`temporal-contract.md`](temporal-contract.md) + embed OOO integration test) |
| **P1** | Storage conformance — reusable backend contract suite on every backend | Done (`internal/store/conformance`) |
| **P2** | Contract consistency — terminology and documentation drift | Done for Intent adoption; ongoing maintenance |
| **P2** | Embeddability — injectable `chronos.Registry`; injectable `store.Registry` (+ `embed.WithStoreRegistry`); `cmd/chronos compute` dogfoods `embed` | Done |

See [`../ROADMAP.md`](../ROADMAP.md) for near-term scope beyond this hardening track.

---

## Non-Goals

Chronos should resist becoming:

- a TSDB;
- an alert manager;
- an incident-management platform;
- a root-cause-analysis engine;
- an autonomous remediation system;
- a generic rules engine;
- a dashboard platform;
- an LLM reasoning layer;
- a recommendation engine.

Those systems may consume Chronos.

They should not be absorbed into it.

---

## Decision Rule

When considering a new capability, ask:

> Does this make Chronos better at reliably perceiving temporal shape?

If yes, it may belong in Chronos.

If it primarily interprets the consequences of that shape, decides what someone should do about it, or orchestrates an operational response, it belongs downstream.

---

## Long-Term Direction

Chronos should become a small, dependable temporal perception primitive.

Its value should come from being:

- mathematically trustworthy;
- deterministic;
- explainable;
- composable;
- backend-independent;
- operationally boring;
- difficult to misuse.

A mature Chronos does not need to know why a pattern matters.

It needs to be exceptionally good at determining that the pattern is there.
