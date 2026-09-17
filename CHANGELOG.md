# Changelog

All notable changes to Chronos are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

The wire contract documented in [`docs/wire-contract.md`](docs/wire-contract.md) is the stability boundary. Renaming any documented Pattern, Evidence.Kind, or metric key is a major-version change.

## [0.10.0] - 2026-09-14

### Added
- **`CHRONOS_CHANGEPOINT_MIN_DELTA`** — an optional floor on
  `|mean_before − mean_after|` in the outcome's own units, applied
  alongside the standardised shift rather than instead of it. Defaults to
  `0` (disabled), so existing behaviour is unchanged.

  The standardised test divides by pooled stddev, which is the series' own
  variability, so on a metric that barely moves any change at all scores
  as many sigma. Seen in a real deployment: health headroom went from
  0.9050 to 0.9072 — an improvement of two tenths of one percent — and
  reported 7.98 sigma at confidence 1.00. Correct, and far too small to
  act on. The floor lets a deployment say how much movement is worth a
  signal. Opt-in because only the adapter knows the outcome's scale.

### Fixed
- **`google.golang.org/grpc` to v1.83.2**, clearing GHSA-vp52-pcj8-j9qc
  (heap memory exhaustion via HTTP/2 DATA frame fragmentation) and
  GHSA-2v4p-qf9q-27wj (xDS server DoS). Note the second advisory carries a
  fix version per release line — 1.82.2 on 1.82.x, 1.83.2 on 1.83.x — so
  1.83.1 clears the first and remains inside the second.

## [Unreleased]

> **Bookkeeping note.** The older entries in this section — everything
> from the first `### Changed` down — were written against
> `[Unreleased]` and never moved when 0.7.0, 0.8.0 and 0.9.0 were tagged;
> several of them verifiably shipped in those releases (content-addressed
> `PerceptionID` and the `outlier_cluster` nil-series validation are both
> present in the v0.9.0 tree). They are left in place rather than
> reassigned, because guessing which release each landed in would put
> false history into the file this project calls its stability record.
> Worth an audit against the tags.

### Fixed
- **The detection scheduler no longer reads the signals table to ask
  whether it has seen a signal before.** `alreadyPersisted` fetched every
  stored signal for the candidate's `(scope, series, pattern)` — with no
  limit, and on the SQL backends one further query per row to hydrate
  evidence — in order to compare two timestamps, once per candidate per
  tick. Nothing prunes that table, so the read had no ceiling: a
  deployment running 73 series at a 30-second cadence accumulated 24,471
  rows in 19 hours and was OOM-killed 35 times. `CHRONOS_MAX_SIGNALS`
  does not help, because it caps what a run *emits*, not what the store
  holds.

  The scheduler now asks the store to `Count` the full perception
  identity, which keeps a tick's memory flat however large the store has
  grown. `SignalFilter` gained a `Window` field to express it, and all
  three SQL schemas gained `idx_signals_identity` so the check is an
  indexed probe. A regression test fails if the scheduler ever issues an
  unbounded `List` again.

### Added
- **`CHRONOS_SIGNAL_RETENTION`** — deletes signals detected longer ago
  than the given duration, swept hourly. Defaults to `0` (keep
  everything), so nothing changes for a deployment that does not opt in.

  The fix above bounds the *memory* a tick costs, not the table. A
  detector derives its analysis window from the observations in front of
  it, so on a live stream that window slides forward and the duplicate
  check legitimately misses — every tick appends a row, and because the
  store outlives the process, restarting the engine reclaims none of it.
  Any deployment whose scheduler runs continuously should set this.

  Retention is an optional store capability (`ports.SignalRetainer`)
  rather than a `SignalRepository` method: the signals table is otherwise
  append-only, and pruning it is an operator's decision about storage,
  not a domain operation. All four bundled backends implement it, the
  notifier decorator forwards it, and a store that cannot prune makes the
  scheduler log an error on every sweep rather than silently do nothing.

### Changed
- **No Homebrew cask.** Chronos is a Go library (optional `cmd/chronos` for
  demos and ops), not a brew-installable product. GoReleaser no longer
  publishes to the Homebrew tap; the release workflow no longer requires
  `HOMEBREW_TAP_TOKEN`.
- **GHCR images publish to `ghcr.io/klarlabs-studio/chronos`.** The v0.9.0
  tag built artefacts then failed pushing `ghcr.io/felixgeelhaar/chronos`
  (`permission_denied: The requested installation does not exist`) because
  the workflow token belongs to `klarlabs-studio`. The Go module path is
  unchanged.

### Added
- **`chronos health` subcommand** — probes `GET /health` on
  `http://127.0.0.1:$CHRONOS_HTTP_PORT` and exits non-zero when the server does
  not report `healthy`. Override with `-addr` / `-timeout`. Exists so the
  runtime image can declare a `HEALTHCHECK`: the distroless base ships no shell
  and no `curl`, so the binary has to probe itself.
- **Container `HEALTHCHECK`** in the Dockerfile, wired to `chronos health`.
- **Detector `Explanation` payloads** — every detector now fills
  `Signal.Explanation` with numeric feature evolution (where a series exists),
  comparable-peer count, threshold used, and a stable `detector_version` tag
  (`trend-v1`, `spike-v1`, …). Still no Title/Summary/Suggestion.
- **Public client HTTP parity** — `Explanation` / `ConfidenceClass` on
  `client.Signal`; `ListPage` (`next_cursor` / `since_cursor`); `Scopes`
  (`scope_in`); `IngestBatch`; `FederationExport`.
- **gRPC HTTP parity (additive RPCs)** — `IngestBatch`, `StreamSignals`
  (server-streaming), `ValidateConfig`, `ExportFederation`, plus
  `since_cursor` / `next_cursor` on `ListSignals`. Unary `Ingest` is
  unchanged. The public `client.GRPCClient` mirrors the new RPCs.
- **Content-addressed signal IDs** — `domain.PerceptionID` (UUID v5 of
  scope, series, pattern, window, pairwise partner). The engine stamps
  every emission so a second detect over the same window upserts.
- **Per-detector metrics** — `chronos_detector_duration_seconds_*`,
  `chronos_detector_signals_total`, `chronos_detector_skips_total`,
  `chronos_signals_truncated_total{pattern}`.

### Changed
- **Nous is archived.** Living docs, agent files, issue/PR templates, and
  package comments no longer treat Nous as a live decision layer.
  Interpretation belongs to agent runtimes; risk + intervention scoring
  lives in [decisionkit](https://github.com/felixgeelhaar/decisionkit).
  See [Mnemos ADR 0005](https://github.com/felixgeelhaar/mnemos/blob/main/docs/adr/0005-archive-nous.md).
- **Dockerfile hardening** — explicit `USER 65532:65532` (the distroless
  `nonroot` default, now stated rather than inherited, so it survives a base
  image retag), `COPY --chown=root:root --chmod=0555` so a compromised process
  cannot rewrite its own entrypoint, and a `maintainer` label.
- **`Signal.Validate`** allows `Series == uuid.Nil` for `outlier_cluster`
  (cohort-level by contract). Those signals can now persist instead of being
  silently dropped on save.
- **MySQL backend** stores `explanation` and `confidence_class` and honours
  `SignalFilter.ScopeIDs`, matching sqlite/postgres/memory.
- **`pipeline.Compute` `SignalsCreated`** counts signals that actually saved,
  not detections that failed validation.
- **Detection scheduler** skips a candidate when a signal with the same
  `(scope, series, pattern, window)` already exists, so a second tick over
  unchanged observations does not append duplicate rows (and does not
  re-notify). A later tick that grows `window.End` (new points) still emits.
  SQLite and Postgres now replace evidence on ID upsert (MySQL already did).
- **Default `CHRONOS_MAX_SIGNALS`** is `100` (was `10`). `0` remains unlimited.
- Agent and user docs (README, architecture, configuration, CLAUDE, AGENTS)
  list all eleven detectors, the MySQL/libSQL backends, and the HTTP auth /
  extra `/v1` routes that were already in the binary.

### Security
- **`golang.org/x/mod` 0.37.0 → 0.40.0** — GO-2026-6179 / CVE-2026-56865
  (sumdb/tlog) and GO-2026-6180 / CVE-2026-56864 (module zip hashes). Indirect;
  not reachable from Chronos's own code.
- **`golang.org/x/text` 0.38.0 → 0.41.0** — GO-2026-5970 / CVE-2026-56852,
  infinite loop on invalid input in `golang.org/x/text/unicode/norm`. Reachable
  transitively; no API change.

## [0.6.0] - 2026-05-31

Embeddable engine release. Adds a public in-process Go API so consumers
(notably Mnemos, which now bundles Chronos to power its temporal memory)
can drive Chronos as a library instead of as a CLI / HTTP service. Adds
ADR 0001 establishing the contract.

### Added
- **`chronos/embed` package** — embeddable in-process engine.
  `embed.New(opts...)` returns an `Engine` with `Process`, `ProcessBatch`,
  `Detect`, `Query`, and `Close`. Defaults to a memory storage backend
  so zero-config usage works in tests and demos; SQL providers are
  blank-imported by callers as before.
- **`chronos/signal.go`** — public type aliases re-exported from
  `internal/domain`: `Signal`, `PatternType`, `TimeWindow`, `Evidence`,
  `FeatureSample`, `Explanation`, `ConfidenceClass`. Pattern and
  confidence constants are also re-exported.
- **ADR 0001 (`docs/adr/0001-embeddable-engine-api.md`)** — documents
  the embeddable-engine decision, stability contract, and alternatives.
- `embed.WithStorage`, `embed.WithLogger`, `embed.WithDetectionConfig`,
  `embed.WithDetectors`, `embed.WithParallelDetectors` option builders.

### Changed
- `chronos.Register` now documents its last-write-wins semantics. The
  behaviour itself is unchanged; the doc clarifies that re-registering
  an adapter under the same name overwrites the previous entry, which
  is intentional for processes that import both the library surface and
  the `cmd/chronos` binary.

## [0.5.0] - 2026-05-24

Cognitive-stack alignment release. Twelve issues land that turn
chronos from a passive perception engine into a tenant-safe,
agent-facing service that mnemos and downstream LLM hosts can wire
into directly. Highlights: explainability, MCP transport, CLI
schema visibility, federation hook.

### Added
- **Pattern explainability payload on `Signal`** (#21) — optional
  `Explanation` value object surfaces the feature evolution series
  the detector inspected, the comparable-peer count, the baseline
  window, the threshold applied, and a stable detector version
  string. Persisted via a new `explanation` JSONB / TEXT column on
  the signals table; surfaced symmetrically on the HTTP DTO
  (`explanation` field, omitempty) and the gRPC `Signal.explanation`
  message (tag 11).
- **Anonymize-cross-scope mode** (#20) — `CHRONOS_ANONYMIZE_CROSS_SCOPE=true`
  flips `CrossScopeCorrelation` to emit deterministic UUIDv5 hashes
  in place of the real scope/series ids on emitted signals.
  Statistical payload (`r`, `n`, `direction`, strength, confidence)
  survives; `metrics["anonymized"]=1` tells downstream consumers
  the ids are opaque. Multi-tenant deployments can finally enable
  the cross-tenant detector without crossing the data boundary.
- **`POST /v1/ingest/batch`** (#23) — up to `MaxIngestBatchSize=1000`
  observations per call, single repository write per adapter group
  via the existing `Save([])` path. All-or-nothing validation;
  `defer_detection` accepted (and echoed) for forward-compat.
  Backfills drop from N round-trips to one.
- **`POST /v1/config/validate`** (#26) — dry-run a candidate env-var
  map and get back a per-detector report (`enabled` /
  `disabled-with-reason` / thresholds / warnings). Loads via the
  same `config.Default()` path the live server uses; env mutations
  are mutex-guarded and restored on every code path so a validate
  call cannot leak overrides into the running process. The
  cross-scope row additionally warns when `AnonymizeCrossScope=false`
  in a multi-tenant deployment.
- **Opaque cursor pagination on `/v1/signals`** (#28) —
  `since_cursor` + `next_cursor` use a base64-wrapped
  `(DetectedAt, ID)` tuple so polling for "new since last check"
  tie-breaks on signal id when timestamps collide. Empty response
  omits `next_cursor` so clients can detect "nothing new" without
  a sentinel. Bad cursors return 400 — a paste error no longer
  silently degrades into an unfiltered list.
- **`scope_in` allowlist on `/v1/signals/stream`** (#25) —
  multi-scope SSE subscriptions for per-user UIs. The server holds
  the allowlist for the lifetime of the connection; a client
  cannot widen its own filter post-handshake. `SSEBroadcaster.Subscribe`
  now takes `[]uuid.UUID`; nil = any scope, single-element slice =
  legacy per-scope stream, multi-element = the new allowlist path.
  An empty (non-nil) slice fails closed.
- **`confidence_class` on `Signal`** (#24) — qualitative grade
  (`tentative` / `established` / `strong`) derived from sample
  size vs `MIN_POINTS`. Env-tunable multipliers
  `CHRONOS_CONFIDENCE_ESTABLISHED` (default 2.0) and
  `CHRONOS_CONFIDENCE_STRONG` (default 5.0). Persisted via a new
  `confidence_class` column; surfaced on HTTP DTO and gRPC proto
  (tag 12). Lets downstream narrators say "a possible trend" vs "a
  clear trend" without reverse-engineering the sample size.
- **`chronos mcp` subcommand** (#22) — MCP stdio server exposing
  three tools: `list_signals` (single scope or scope_ids
  allowlist), `ingest` (single observation), `describe_detector`
  (delegates to the same `BuildConfigReport` /v1/config/validate
  uses). Companion to the mnemos MCP server so MCP-aware hosts
  (Claude Code, Letta, Anthropic Desktop) discover the whole
  cognitive stack natively.
- **`chronos migrate` CLI** (#27) — `migrate status` reports the
  declared version ladder, current vs latest, and pending/applied
  steps. `migrate up` opens the store (triggers the existing
  auto-apply path) then prints status. `migrate down` is an
  explicit usage error — migrations are forward-only by design;
  per-step `.down.sql` files would have to land first.
- **`GET /v1/federation/export`** (#30) — opt-in
  (`CHRONOS_FEDERATION_ENABLED=true`) anonymized pattern
  statistics: per-pattern count, avg/min/max strength + confidence,
  mean sample size, per-confidence-class histogram. NO scope_ids,
  NO series ids, NO evidence rows in the payload — community-grade
  insight without crossing the tenant boundary. Stable
  alphabetical ordering so two consecutive exports diff cleanly.
- **buf CI guard on every PR** (#29) — `buf lint` (STANDARD ruleset
  minus the response-naming opinion) plus `buf breaking`
  (WIRE_JSON) against main. JSON-over-HTTP is a first-class
  transport so JSON-shape renames matter as much as wire-format
  breaks. Skipped on main itself.
- **Joint chronos+mnemos integration harness** (#31) —
  `test/integration/docker-compose.yml` stands the stack up;
  `test/integration/smoke_test.go` (//go:build integration) pins
  the cross-talk surface: health on both services, chronos ingest
  → list signals, mnemos events append → list. A nightly +
  repository_dispatch CI workflow re-runs the smoke against the
  sister-repo's `main` so version-skew bugs surface within 24h.
- **Postgres bulk-load path** — `EntityStateRepository.Save` switches to chunked multi-row `INSERT…ON CONFLICT` above `BulkSaveThreshold` (200 rows). Chunks of 1 000 rows keep us under the 65 535 placeholder limit. Backfills now batch-commit; per-row UPSERT path retained for small batches where round-trip overhead amortises poorly. — `EntityStateRepository.Save` switches to chunked multi-row `INSERT…ON CONFLICT` above `BulkSaveThreshold` (200 rows). Chunks of 1 000 rows keep us under the 65 535 placeholder limit. Backfills now batch-commit; per-row UPSERT path retained for small batches where round-trip overhead amortises poorly.
- **Durable webhook outbox** — `notify.OutboxConfig.PersistencePath` opts the in-memory outbox into JSON-on-disk persistence. Enqueue / flush snapshot to atomic-rename file; startup re-loads pending deliveries. Survives process restart.
- **Detector parallelism** — `Engine.WithParallelDetectors(true)` runs every (scope, detector) pair in its own goroutine. Off by default; flip on via `CHRONOS_DETECTOR_PARALLELISM=1`. Race tests pass.
- **At-least-once webhook outbox** (`internal/notify/outbox.go`) — `Outbox` wraps an `AckingNotifier` with retry+exponential-backoff (default 5 attempts, 1s→30s). Failed deliveries reschedule until success or max-attempts. In-memory only; restart loses pending. Operators wanting durable delivery wire their own persistent notifier.
- **OutlierCluster detector** (`PatternTypeOutlierCluster`) — detects time windows where multiple series in a scope go anomalous together. Cohort-level, not per-series. Tunable via `CHRONOS_OUTLIER_CLUSTER_MIN_SERIES` (default 3), `CHRONOS_OUTLIER_CLUSTER_Z` (default 2.5), `CHRONOS_OUTLIER_CLUSTER_WINDOW` (default 5m). Signals carry `member_count` and one `outlier_member` evidence row per participating series.
- **CrossScopeCorrelation detector** (`PatternTypeCrossScopeCorrelation`) — pairwise Pearson correlation across (scope, series) pairs in DIFFERENT scopes. Tunable via `CHRONOS_CROSS_SCOPE_MIN` (default 0.8) and `CHRONOS_CROSS_SCOPE_MIN_POINTS` (default 5). Drops same-scope pairs (handled by within-scope `Correlation`).
- **`CrossScopeDetector` interface** — parallel to `Detector`; runs once over the full state list rather than per-scope. Engine picks up `DefaultCrossScopeDetectors` automatically.
- **Write-coalescing repository decorator** (`internal/store/batching`) — opt-in `EntityStateRepository` wrapper that batches Ingest calls into a single Save transaction. Tunable via `MaxBatch` and `MaxWait`. `Close` drains the buffer on shutdown.
- **ChangePoint detector** (`PatternTypeChangePoint`) — best-split mean-shift test detects step changes in the outcome metric. Distinct from Spike/Drop; emits `regime_before` / `regime_after` evidence with `mean`, `stddev`, `n` per regime; `shift` and `split_index` in signal metrics. Tunable via `CHRONOS_CHANGEPOINT_MIN_SHIFT` (default 1.5) and `CHRONOS_CHANGEPOINT_MIN_POINTS` (default 8). Wired into `DefaultDetectors`.
- **SSE `Last-Event-ID` replay** — clients reconnect with the standard `Last-Event-ID` HTTP header (or `?last_event_id=<uuid>` query fallback) and the server replays signals detected at or after the cursor before continuing the live stream. Each SSE frame now carries an `id:` line.
- `ROADMAP.md` — public roadmap with status, next steps, non-goals, semver policy.
- `docs/DEPLOYMENT.md` — operator runbook (k8s shape, Prometheus scrape, Grafana import, ops procedures, known limitations).
- `docs/SLOs.md` — formal availability / latency / error-rate / freshness SLOs with error-budget burn-rate tiers.
- `deploy/grafana/dashboards/chronos-overview.json` — reference Grafana dashboard (signals, observations, HTTP, webhooks, adapters).
- `docs/wire-contract.md` — "Transport parity" section explaining HTTP-string ↔ gRPC enum mapping and metric-key parity; `change_point` Pattern entry; SSE replay protocol.
- gRPC: new `PATTERN_TYPE_CHANGE_POINT = 9` enum value in `api/proto/chronos/v1/chronos.proto`; `client.PatternTypeChangePoint` constant.
- Config: `CHRONOS_CHANGEPOINT_MIN_SHIFT`, `CHRONOS_CHANGEPOINT_MIN_POINTS`.
- `CHANGELOG.md`.

### Changed
- `docs/configuration.md` — added `CHRONOS_GRPC_PORT` / `CHRONOS_GRPC_HOST` rows; clarified that `CHRONOS_API_TOKEN` gates both transports; documented `--grpc-port` flag override.
- `docs/backlog.md` — gRPC moved to "Recently shipped"; stale work-in-progress stub removed.
- `README.md` — API section split into HTTP and gRPC subsections; quickstart shows `--grpc-port`.

## [Released via GoReleaser]

Prior releases are tracked as GitHub Releases. Notable feature waves:

- **Eight detectors GA**: Recurrence, Trend, Spike, Drop, Stall, Anomaly, Seasonality, Correlation.
- **Multi-backend storage** per [Mnemos ADR-0001](https://github.com/felixgeelhaar/Mnemos/blob/main/docs/adr/0001-multi-backend-storage.md): `memory`, `sqlite`, `postgres`, `mysql`/`mariadb`, `libsql`. Wire-compatible engines (CockroachDB, YugabyteDB, Neon, Crunchy Bridge, TimescaleDB, AlloyDB Omni; PlanetScale, TiDB, Vitess; Turso) work through the native providers unchanged.
- **gRPC transport** alongside HTTP, with `Ingest` (client-streaming) and `ListSignals`. Schema in `api/proto/chronos/v1/chronos.proto`. Bearer auth shared with HTTP.
- **Push notifications**: webhooks (HMAC-SHA256-signed) and Server-Sent Events.
- **In-process detection scheduler** (`CHRONOS_DETECTION_INTERVAL`) — required for SSE to receive events.
- **VEX security audit** complete; nox baseline tracked in-tree.
