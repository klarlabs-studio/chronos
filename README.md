# Chronos

<p align="center">
  <strong>Time & Pattern Perception in the Cognitive Stack</strong>
</p>

<p align="center">
  <a href="https://github.com/felixgeelhaar/chronos/actions"><img src="https://github.com/felixgeelhaar/chronos/workflows/CI/badge.svg" alt="CI Status"></a>
  <a href="https://goreportcard.com/report/github.com/felixgeelhaar/chronos"><img src="https://goreportcard.com/badge/github.com/felixgeelhaar/chronos" alt="Go Report Card"></a>
  <a href="https://pkg.go.dev/github.com/felixgeelhaar/chronos"><img src="https://pkg.go.dev/badge/github.com/felixgeelhaar/chronos.svg" alt="Go Reference"></a>
  <a href="https://github.com/felixgeelhaar/chronos/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="License"></a>
</p>

---

Chronos ingests time-series observations from any source and emits structured **signals** describing the patterns it sees — recurrences, trends, spikes, drops, stalls, anomalies, seasonality, correlations, change-points, outlier clusters, cross-scope correlations. It does not decide, act, or render prose. Signals are perception, not opinion.

Chronos sits next to **Mnemos** (memory). Downstream **agent runtimes** interpret signals and act. See [`docs/cognitive-stack.md`](docs/cognitive-stack.md) for how the systems compose. [Nous](https://github.com/felixgeelhaar/nous) is archived (2026-05-31); risk + intervention scoring lives in [decisionkit](https://github.com/felixgeelhaar/decisionkit).

---

## 5-minute demo

From zero to a real Stall signal, no external services, just `chronos` and `curl`. Copy-paste each block in order.

```bash
# 1. Install — single static binary, no CGO. (~10 seconds)
go install github.com/felixgeelhaar/chronos/cmd/chronos@latest

# 2. Run a server with the in-process detection scheduler ticking
#    every 5 seconds. SQLite at /tmp/demo.db; no setup needed.
export CHRONOS_DB_DSN="sqlite:///tmp/chronos-demo.db"
export CHRONOS_DETECTION_INTERVAL=5s
chronos serve --port 7778 &  # add --grpc-port 7779 to also expose gRPC
SERVER_PID=$!
sleep 1

# 3. Push seven flat observations for one entity. The outcome metric
#    (last feature) hovers at 11 → Chronos's Stall detector should fire
#    once it has enough samples.
SCOPE="11111111-1111-1111-1111-111111111111"
ENTITY="22222222-2222-2222-2222-222222222222"
for i in $(seq 0 6); do
  curl -s -X POST http://localhost:7778/v1/ingest \
    -H 'Content-Type: application/json' \
    -d "{\"entity_id\":\"$ENTITY\",\"scope_id\":\"$SCOPE\",\"timestamp\":\"$(date -u -v+${i}M +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -d "+$i minutes" +%Y-%m-%dT%H:%M:%SZ)\",\"features\":[$i,11.0],\"labels\":[\"i\",\"outcome\"]}" > /dev/null
done

# 4. Wait for one detection tick.
sleep 6

# 5. See the Stall signal.
curl -s "http://localhost:7778/v1/signals?scope_id=$SCOPE&pattern=stall" | jq
# {
#   "signals": [{
#     "id": "...",
#     "scope_id": "111...",
#     "series":   "222...",
#     "pattern":  "stall",
#     "detected_at": "...",
#     "window": { "start": "...", "end": "..." },
#     "strength":   1.0,
#     "confidence": 0.875,
#     "metrics":  { "normalised_stddev": 0, "mean": 11.0, "n": 7 },
#     "evidence": [{ "kind": "variance_window", ... }]
#   }],
#   "count": 1
# }

# 6. (Optional) Subscribe to live signals via Server-Sent Events.
#    curl -N "http://localhost:7778/v1/signals/stream?scope_id=$SCOPE"

# Cleanup.
kill $SERVER_PID
```

That's the whole loop: ingest a series → detection runs in-process → signals are queryable and streamable. The full Mnemos → Chronos → agent walkthrough lives in [`docs/cognitive-stack-example.md`](docs/cognitive-stack-example.md).

## Why Chronos?

Most observability tools answer *"is this metric outside its range?"* Chronos answers *"what shape is this series?"* and exposes the answer as a typed signal a downstream system (an agent, a workflow, a dashboard) can switch on.

| | Chronos | Prometheus + Alertmanager | Grafana | Build it yourself |
|---|---|---|---|---|
| **Output** | Typed signal (`Pattern` enum + structured `Metrics`) | Threshold alert (string) | Visual chart | Whatever you write |
| **Audience** | Systems (agents, schedulers) | Humans (oncall) | Humans (looking) | You |
| **Detection** | 14 detectors out of the box (recurrence, trend, spike, drop, stall, anomaly, seasonality, correlation, change_point, outlier_cluster, cross_scope_correlation, oscillation, divergence, convergence) | Threshold + rate + absent | n/a (visualisation) | What you implement |
| **Storage** | memory / sqlite / postgres / mysql / libsql; namespace-isolated | TSDB | Reads other stores | Yours |
| **Footprint** | Single static binary, no CGO, ~2 MB Docker image | TSDB cluster | Java/JS app | Depends |
| **Stable wire** | Yes — `Pattern`, `Evidence.Kind`, `Metrics` keys; SDK in Go | Yes (Prometheus exposition) | n/a | You decide |

Chronos doesn't replace any of these. It complements them by giving you a layer that perceives shape, not state — so your decision systems read structured perception instead of parsing alerts or scraping metrics.

## Design principles

Authoritative Intent: [`docs/intent.md`](docs/intent.md). Temporal semantics: [`docs/temporal-contract.md`](docs/temporal-contract.md).

- **Signals, not opinions.** Each signal carries Pattern, Strength, Confidence, Window, and Evidence. There is no Title, no Summary, no Suggestion. Interpretation is the consumer's job.
- **Shape, not state.** Chronos detects temporal structure (trends, spikes, change points, …), not threshold breaches alone.
- **Domain-agnostic.** Athletes, servers, sensors, stocks — all flow through the `chronos.Source` adapter port.
- **Loosely coupled.** Chronos works standalone. The stack composes through stable contracts, not internal coupling.
- **Lightweight.** Single Go binary. Pure-Go SQLite (no CGO). Five backends; pick one per deployment.

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Adapter    │     │   Engine    │     │  API + SDK  │
│  (Source)   │ ──▶ │ (detectors) │ ──▶ │  (signals)  │
│             │     │             │     │             │
│ Out of tree │     │ • Detect    │     │ • REST      │
│ Yours       │     │ • Score     │     │ • SSE       │
│             │     │ • Persist   │     │ • Webhooks  │
└─────────────┘     └─────────────┘     └─────────────┘
                          │
            ┌─────────────┼─────────────┐
            ▼             ▼             ▼
       memory://    sqlite://     postgres://
                                  mysql://, mariadb://
                                  libsql://
```

Detailed layering and invariants: [`docs/architecture.md`](docs/architecture.md). Cognitive-stack context: [`docs/cognitive-stack.md`](docs/cognitive-stack.md).

## Documentation

| Document | What's in it |
|---|---|
| [`docs/cognitive-stack.md`](docs/cognitive-stack.md) | Chronos's role next to Mnemos and agent runtimes; the boundaries that make the stack composable. |
| [`docs/cognitive-stack-example.md`](docs/cognitive-stack-example.md) | End-to-end walkthrough with real curl + JSON + Go: a commitment goes stale, Chronos detects it, an agent interprets. |
| [`docs/architecture.md`](docs/architecture.md) | Internal layering (DDD / hexagonal), per-aggregate repos, detector contract. |
| [`docs/adapters.md`](docs/adapters.md) | How to write a `chronos.Source` adapter for your own data. |
| [`docs/configuration.md`](docs/configuration.md) | All `CHRONOS_*` env vars; DSN syntax, namespace contract, backend matrix, push-notification setup. |
| [`docs/wire-contract.md`](docs/wire-contract.md) | Authoritative list of stable strings consumers may rely on (`Pattern`, `Evidence.Kind`, every `Metrics` key per detector) plus the stability policy. |

## Pattern detectors

| Pattern         | What it detects                                                            | Evidence kind          |
|-----------------|----------------------------------------------------------------------------|------------------------|
| `recurrence`    | Subject is in a state other entities have been in before (cosine peers)    | `similar_state`        |
| `trend`         | Sustained directional movement of the outcome metric (linear regression)   | `regression_summary`   |
| `spike`         | Sharp positive deviation from the rolling baseline (z-score)               | `baseline_deviation`   |
| `drop`          | Sharp negative deviation from the rolling baseline (z-score)               | `baseline_deviation`   |
| `stall`         | Outcome variance falls below threshold over a window (normalised stddev)   | `variance_window`      |
| `anomaly`       | Subject is unlike its peers' current states (cross-entity dual of `recurrence`) | `peer_distance`   |
| `seasonality`   | Periodic structure in the outcome series (autocorrelation peak)            | `autocorrelation_peak` |
| `correlation`   | Two series in the same scope move together (pairwise Pearson)              | `pair_correlation`     |
| `change_point`  | Sustained mean shift between two regimes (best-split test)                 | `regime_before` / `regime_after` |
| `outlier_cluster` | Several series in a scope go anomalous around the same time (cohort-level; `series` is the nil UUID) | `outlier_member` |
| `cross_scope_correlation` | Two series in *different* scopes move together                         | `cross_scope_pair`     |

Tunable via `CHRONOS_*` env vars per detector (see [`docs/configuration.md`](docs/configuration.md)).

## Persistence backends

Chronos supports five native backends and inherits any wire-protocol-compatible alternative. DSN-driven; one URL picks the provider.

| Scheme(s)                 | Wire-protocol compatibles also supported                                    |
|---------------------------|------------------------------------------------------------------------------|
| `memory://`               | —                                                                            |
| `sqlite://` / `sqlite3://`| —                                                                            |
| `postgres://` / `postgresql://` | CockroachDB, YugabyteDB, Neon, Crunchy Bridge, TimescaleDB, AlloyDB Omni |
| `mysql://` / `mariadb://` | MariaDB, PlanetScale, TiDB, Vitess                                           |
| `libsql://`               | Turso (remote), local-file libSQL                                            |

Every DSN accepts a `?namespace=` query parameter so multiple cognitive-stack tools (Mnemos + Chronos) can share one database with isolated schemas. See [`docs/configuration.md`](docs/configuration.md) for syntax and the namespace contract.

## Install

Chronos is a Go library. Import the module (adapters) or the HTTP/gRPC client. The `cmd/chronos` binary is optional — demos, `serve`, and ops probes.

```go
import "github.com/felixgeelhaar/chronos"          // adapter SDK
import "github.com/felixgeelhaar/chronos/client"   // HTTP / gRPC consumer SDK
```

```bash
go get github.com/felixgeelhaar/chronos@latest     # requires Go 1.25+
```

**Optional CLI** (`go install` or a release archive) if you want to run `chronos serve` locally:

```bash
go install github.com/felixgeelhaar/chronos/cmd/chronos@latest
```

**Docker (any OCI runtime)** — for operators running the HTTP/gRPC server:

```bash
docker run --rm -p 7778:7778 ghcr.io/klarlabs-studio/chronos:latest
# Multi-arch image: linux/amd64 + linux/arm64. Distroless, ~2 MB.
```

**Linux packages**

```bash
# Replace <version> and <arch> (amd64|arm64) with the desired release.
curl -fsSL -o chronos.deb \
  https://github.com/felixgeelhaar/chronos/releases/download/v<version>/chronos_<version>_linux_<arch>.deb
sudo dpkg -i chronos.deb
```

`.rpm` (RHEL / Fedora) and `.apk` (Alpine) ship for the same OS/arch matrix; substitute the file extension.

**Prebuilt binary archive**

```bash
curl -fsSL -o chronos.tar.gz \
  https://github.com/felixgeelhaar/chronos/releases/download/v<version>/chronos_<version>_<os>_<arch>.tar.gz
curl -fsSL -O \
  https://github.com/felixgeelhaar/chronos/releases/download/v<version>/checksums.txt
shasum -a 256 -c checksums.txt --ignore-missing
tar -xzf chronos.tar.gz && sudo install -m 0755 chronos /usr/local/bin/chronos
```

**Source build**

```bash
git clone https://github.com/felixgeelhaar/chronos.git
cd chronos
make build   # binary lands in ./bin/chronos with version/commit/buildDate ldflags
```

**Supported targets** (optional CLI / container, not a Homebrew product)

| OS      | amd64 | arm64 | Distribution channels                     |
|---------|:-----:|:-----:|-------------------------------------------|
| linux   |  ✓    |  ✓    | Docker, .deb, .rpm, .apk, tar.gz, source  |
| darwin  |  ✓    |  ✓    | tar.gz, source                            |
| windows |  ✓    |  —    | zip archive, source                       |

## Writing an adapter

```go
package myadapter

import (
    "context"

    "github.com/felixgeelhaar/chronos"
)

type Source struct{}

func (s *Source) Name() string { return "my-source" }

func (s *Source) Fetch(ctx context.Context, cfg map[string]string) ([]chronos.EntityState, error) {
    // Map your domain into chronos.EntityState. Last feature is the outcome metric.
    return states, nil
}

func init() { chronos.Register(&Source{}) }
```

Adapters self-register. Add a blank import in your binary so `init()` fires:

```go
import _ "example.com/myadapter"
```

Full guide: [`docs/adapters.md`](docs/adapters.md).

## Reading signals

```go
import "github.com/felixgeelhaar/chronos/client"

c, _ := client.New("http://chronos.local:7778",
    client.WithToken(os.Getenv("CHRONOS_TOKEN")),
    client.WithTimeout(10*time.Second),
)

// Pull: recent recurrence signals for a scope (page includes next_cursor).
page, err := c.Signals().
    Scope(scopeID).
    Pattern(client.PatternTypeRecurrence).
    MinConfidence(0.7).
    Limit(20).
    ListPage(ctx)
signals := page.Signals

// Resume after the opaque cursor from the previous page.
page, err = c.Signals().Scope(scopeID).SinceCursor(page.NextCursor).ListPage(ctx)
```

For low-latency consumers, subscribe to live signals via SSE instead of polling:

```go
ctx, cancel := context.WithCancel(ctx)
defer cancel()

events, err := c.Signals().
    Scope(scopeID).
    Pattern(client.PatternTypeRecurrence).
    Stream(ctx)
if err != nil { return err }

for sig := range events {
    handle(sig)   // sig is client.Signal — same shape as List returns
}
// channel closes on ctx cancel, server EOF, or fatal protocol error
```

Streaming requires the server to run an in-process detection scheduler (`CHRONOS_DETECTION_INTERVAL > 0`); otherwise the endpoint returns 501. Delivery is at-most-once — pair with a `Since`-keyed `List` call for gap recovery and de-duplicate by `Signal.ID`.

For streaming sources you can ingest single observations, or a batch:

```go
_, err := c.Ingest(ctx, client.IngestRequest{
    EntityID:  entityID,
    ScopeID:   scopeID,
    Timestamp: time.Now(),
    Features:  []float64{f1, f2, f3, outcome},
    Adapter:   "my-source",
})

_, err = c.IngestBatch(ctx, []client.IngestRequest{ /* ... */ })
```

## API

### HTTP

```
GET  /health                              Liveness/readiness
GET  /metrics                             Prometheus exposition
POST /v1/ingest                           Stream a single observation
POST /v1/ingest/batch                     Batch observations (up to 1000)
POST /v1/config/validate                  Dry-run a candidate env-var map
GET  /v1/federation/export                Opt-in anonymized pattern statistics
GET  /v1/signals                          List signals (filter by scope/pattern/series/since/until/min_confidence/limit/since_cursor)
GET  /v1/signals/<id>                     Fetch a single signal with evidence
GET  /v1/signals/stream                   Server-Sent Events feed (requires scheduler enabled)
```

### gRPC

The gRPC service is defined in [`api/proto/chronos/v1/chronos.proto`](api/proto/chronos/v1/chronos.proto) and runs alongside the HTTP server on a separate port when `CHRONOS_GRPC_PORT` (or `--grpc-port`) is set:

| Method | Description |
|---|---|
| `Ingest` | Push a single observation. Unary — batch ingest is `IngestBatch`. |
| `IngestBatch` | Persist many observations (all-or-nothing, same cap as HTTP). |
| `ListSignals` | Filter by scope/pattern/series/since/until/min_confidence/limit/`since_cursor`. Returns `next_cursor`. |
| `GetSignal` | Fetch a single signal by ID |
| `StreamSignals` | Server-streaming live feed (requires scheduler; Unimplemented otherwise) |
| `ValidateConfig` | Dry-run a candidate `CHRONOS_*` env map |
| `ExportFederation` | Anonymized pattern stats (`CHRONOS_FEDERATION_ENABLED=true`) |

Bearer-token auth via the `authorization` metadata header reuses `CHRONOS_API_TOKEN` (unary and streaming). HTTP and gRPC return the same domain shape — see [`docs/wire-contract.md`](docs/wire-contract.md) for the canonical contract.

Wire shape and stability policy: [`docs/wire-contract.md`](docs/wire-contract.md). Roadmap: [`ROADMAP.md`](ROADMAP.md).

## Adapters

Chronos itself ships with no adapters — by design. The engine is domain-agnostic; every adapter lives in the repo that owns the domain it bridges. Build a custom binary that imports `chronos` plus the adapters you need:

```go
package main

import (
    _ "example.com/your-adapter"   // registers itself via init()
    _ "github.com/felixgeelhaar/chronos/internal/store/sqlite"
    // ... etc.
)

// re-use chronos's CLI subcommands or write your own main()
```

Known integrations:

| Repo | Domain |
|---|---|
| [`felixgeelhaar/ascend`](https://github.com/felixgeelhaar/ascend) | Ascend weightlifting coaching platform — maps athlete training weeks into `chronos.EntityState`. |

## Development

```bash
make test          # go test -race -count=1 ./...
make check         # fmt + vet + test
make sqlc          # Regenerate SQLite query code
make build         # Builds with version/commit/buildDate ldflags
make coverctl-check # Enforce per-domain coverage policy
make nox-scan      # Security scan (baseline-gated)
```

### Pre-commit hooks

Pre-commit catches style and lint failures locally, before CI. One-time setup per clone:

```bash
pip install pre-commit                   # or: brew install pre-commit
make precommit-install                   # installs pre-commit + commit-msg hooks
make precommit                           # run all hooks against the working tree
```

The hook set (gofmt, go vet, go mod tidy, golangci-lint, file hygiene, Conventional Commits) is a strict subset of CI; passing locally guarantees CI will not reject on style or lint.

Working conventions for human and agent contributors: [`AGENTS.md`](AGENTS.md). Contribution guidelines: [`CONTRIBUTING.md`](CONTRIBUTING.md).

## License

MIT — see [`LICENSE`](LICENSE).

## Companion projects

- **[Mnemos](https://github.com/felixgeelhaar/Mnemos)** — Memory & Knowledge ("what happened, what do we know")
- **Chronos** — Time & Pattern Perception ("what is changing, what's emerging")
- **[decisionkit](https://github.com/felixgeelhaar/decisionkit)** — deterministic risk + intervention scoring for agent consumers
- **[Nous](https://github.com/felixgeelhaar/nous)** — archived 2026-05-31 (`v0.3.1-final`); reasoning moved to agent runtimes. See [Mnemos ADR 0005](https://github.com/felixgeelhaar/mnemos/blob/main/docs/adr/0005-archive-nous.md).
