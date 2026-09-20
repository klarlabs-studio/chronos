# Configuration

Chronos is configured exclusively through `CHRONOS_*` environment variables. The CLI accepts a small set of flags that override the corresponding env vars at runtime.

## Reference

| Variable | Default | Used by | Description |
|---|---|---|---|
| `CHRONOS_DB_DSN` | unset | both | Persistence DSN. Primary entry point. Examples: `sqlite:///chronos.db`, `postgres://user:pw@host/db?namespace=chronos`, `mysql://user:pw@host:3306/?namespace=chronos`, `libsql://my-db.turso.io?authToken=...`. When set, takes precedence over the legacy pair below. |
| `CHRONOS_DB_TYPE` | `sqlite` | both | **Legacy**: `sqlite`, `postgres`, or `memory`. Translated internally to a DSN; new deployments should set `CHRONOS_DB_DSN`. |
| `CHRONOS_DB_CONN` | `chronos.db` | both | **Legacy**: SQLite path (or `:memory:`) or full Postgres URL. Used together with `CHRONOS_DB_TYPE`. |
| `CHRONOS_MAX_SIGNALS` | `100` | `compute` | Cap on signals produced per detect run. `0` = unlimited. |
| `CHRONOS_JOB_TIMEOUT` | `10m` | `compute` | Overall compute timeout (Go duration syntax). |
| `CHRONOS_SIM_THRESHOLD` | `0.85` | Recurrence | Minimum cosine similarity for a peer to count. |
| `CHRONOS_MIN_SAMPLE` | `2` | Recurrence | Minimum peer cases required to emit. |
| `CHRONOS_TREND_MIN_SLOPE` | `0.05` | Trend (Tier B) | Minimum absolute regression slope. |
| `CHRONOS_TREND_MIN_POINTS` | `4` | Trend (Tier B) | Minimum observations to consider a trend. |
| `CHRONOS_SPIKE_Z` | `2.5` | Spike (Tier B) | Absolute z-score threshold for a spike. |
| `CHRONOS_DROP_Z` | `2.5` | Drop (Tier B) | Absolute z-score threshold for a drop. |
| `CHRONOS_SPIKE_WINDOW` | `5` | Spike/Drop (Tier B) | Rolling baseline size in points. |
| `CHRONOS_STALL_MAX_STDDEV` | `0.05` | Stall | Max stddev of normalised outcome to qualify. |
| `CHRONOS_STALL_MIN_POINTS` | `4` | Stall | Minimum observations to qualify. |
| `CHRONOS_ANOMALY_MAX_SIM` | `0.5` | Anomaly | Max cosine similarity to nearest peer to count as isolated. |
| `CHRONOS_ANOMALY_MIN_PEERS` | `2` | Anomaly | Minimum peers required for cross-entity comparison. |
| `CHRONOS_SEASONALITY_MIN_AUTOCORR` | `0.5` | Seasonality | Minimum autocorrelation at any lag. |
| `CHRONOS_SEASONALITY_MIN_POINTS` | `12` | Seasonality | Minimum observations to consider. |
| `CHRONOS_SEASONALITY_MIN_PERIOD` | `2` | Seasonality | Minimum lag (period) considered. |
| `CHRONOS_CORRELATION_MIN` | `0.7` | Correlation | Minimum |Pearson r| between two series to emit. |
| `CHRONOS_CORRELATION_MIN_POINTS` | `5` | Correlation | Minimum aligned observations between two series. |
| `CHRONOS_CHANGEPOINT_MIN_SHIFT` | `1.5` | ChangePoint | Minimum standardised mean shift (`|Δmean| / pooled_stddev`) to emit. |
| `CHRONOS_CHANGEPOINT_MIN_POINTS` | `8` | ChangePoint | Minimum observations required (split needs ≥ 2 each side). |
| `CHRONOS_CHANGEPOINT_MIN_DELTA` | `0` | ChangePoint | Minimum `|mean_before − mean_after|` in the outcome's own units, required in addition to the standardised shift. `0` (default) disables it. Use on a bounded outcome where a very stable series makes any movement score as many sigma. |
| `CHRONOS_OUTLIER_CLUSTER_MIN_SERIES` | `3` | OutlierCluster | Minimum distinct series in a single time bucket to emit. |
| `CHRONOS_OUTLIER_CLUSTER_Z` | `2.5` | OutlierCluster | Per-series \|z-score\| threshold for an observation to count as an outlier. |
| `CHRONOS_OUTLIER_CLUSTER_WINDOW` | `5m` | OutlierCluster | Sliding-window width for "around the same time". |
| `CHRONOS_CROSS_SCOPE_MIN` | `0.8` | CrossScopeCorrelation | Minimum \|Pearson r\| across scopes. |
| `CHRONOS_CROSS_SCOPE_MIN_POINTS` | `5` | CrossScopeCorrelation | Minimum aligned observations between two series. |
| `CHRONOS_ANONYMIZE_CROSS_SCOPE` | `false` | CrossScopeCorrelation | Replace scope/series ids with UUIDv5 hashes on cross-scope signals. |
| `CHRONOS_CONFIDENCE_ESTABLISHED` | `2.0` | all detectors | MIN_POINTS multiplier for `confidence_class=established`. |
| `CHRONOS_CONFIDENCE_STRONG` | `5.0` | all detectors | MIN_POINTS multiplier for `confidence_class=strong`. |
| `CHRONOS_DETECTOR_PARALLELISM` | `false` | Engine | Run per-scope detectors in parallel goroutines. Off by default (deterministic ordering); flip on for many-scope deployments. |
| `CHRONOS_HTTP_PORT` | `7778` | `serve` | HTTP listen port. |
| `CHRONOS_HTTP_HOST` | `127.0.0.1` | `serve` | HTTP listen host. Use `0.0.0.0` to bind all interfaces. |
| `CHRONOS_API_TOKEN` | unset | `serve` | Bearer token for HTTP and gRPC. Empty disables auth. |
| `CHRONOS_GRPC_PORT` | `0` | `serve` | gRPC listen port. `0` disables gRPC; HTTP and gRPC servers run concurrently when both are set. |
| `CHRONOS_GRPC_HOST` | unset | `serve` | gRPC listen host. Empty binds all interfaces. |
| `CHRONOS_FEDERATION_ENABLED` | `false` | `serve` | Opt-in `GET /v1/federation/export`. |
| `CHRONOS_WEBHOOK_URLS` | unset | both | Comma-separated POST endpoints; empty disables webhooks. |
| `CHRONOS_WEBHOOK_SECRET` | unset | both | HMAC-SHA256 key for `X-Chronos-Signature`. Empty omits the header. |
| `CHRONOS_WEBHOOK_TIMEOUT` | `5s` | both | Per-request HTTP client timeout (Go duration). |
| `CHRONOS_WEBHOOK_RETRIES` | `1` | both | Best-effort retries on 5xx. No retry on 2xx or 4xx. |
| `CHRONOS_DETECTION_INTERVAL` | `0` | `serve` | Background detection cadence; `0` disables. Required for SSE to receive signals. |
| `CHRONOS_DETECTION_LOOKBACK` | `24h` | `serve` | How much history each detection tick loads per scope. Bounds a tick's memory; see below. |
| `CHRONOS_RETENTION_SWEEP_INTERVAL` | `5m` | `serve` | How often retention runs. This is the overshoot — see below. |
| `CHRONOS_SIGNAL_RETENTION` | `0` | `serve` | Delete signals detected longer ago than this; `0` keeps them forever. Set it on any long-running scheduler — see below. |
| `CHRONOS_VERBOSE` | unset | CLI | When set to any non-empty value, prints the cause chain on errors. |

`serve` flags `--port`, `--host`, and `--grpc-port` override their env counterparts. `compute` accepts `--scope-id` (preferred) or `--coach-id` (legacy alias).

## Precedence

CLI flags > environment variables > defaults baked into `internal/config/config.Default()`.

## Tuning detectors

Each detector has its own knob namespace (`CHRONOS_<DETECTOR>_*`) so you can tune one without affecting others. All eleven detectors in `DefaultDetectors` / `DefaultCrossScopeDetectors` are live; set a detector's min-points / threshold knobs out of range to effectively disable it (the detector returns no signals).

- **Recurrence** (`SIM_THRESHOLD`, `MIN_SAMPLE`) — raise threshold for fewer, more specific peers; lower it for more candidates. Below ~0.7 admits noise. `MIN_SAMPLE` of 2 is the smallest defensible value; five is the saturation point of the confidence sample-factor.
- **Trend / Spike / Drop / Stall** thresholds influence trigger sensitivity; smaller windows react faster but produce more noise.
- **Anomaly** (`MAX_SIM`, `MIN_PEERS`) — lower `MAX_SIM` makes anomalies harder to qualify (only truly isolated entities trigger); higher `MIN_PEERS` requires more cohort coverage.
- **Seasonality** (`MIN_AUTOCORR`, `MIN_POINTS`, `MIN_PERIOD`) — `MIN_POINTS` should be at least two full periods; raising `MIN_PERIOD` past 2 avoids labelling noisy near-flat series as periodic.
- **Correlation** (`MIN`, `MIN_POINTS`) — pairwise correlation cost is O(N²) in series count per scope. Tighten `MIN` and `MIN_POINTS` for noisy data.

## Backend choice

Chronos's persistence layer follows the cognitive-stack contract documented in [Mnemos ADR 0001](https://github.com/felixgeelhaar/Mnemos/blob/main/docs/adr/0001-multi-backend-storage.md): every provider is selected by URL scheme, every DSN accepts a `?namespace=` query parameter, and providers translate the namespace into their native isolation primitive. This is shared across Mnemos, Chronos, and the rest of the cognitive stack so a company can run one Postgres for all four tools and keep each one's data isolated by schema.

### DSN syntax

```
memory://?namespace=chronos
sqlite:///var/lib/chronos/chronos.db
sqlite://:memory:
postgres://user:pw@host:5432/cogstack?namespace=chronos
postgresql://user:pw@host/cogstack?sslmode=require&namespace=chronos
mysql://user:pw@host:3306/?namespace=chronos
mariadb://user:pw@host:3306/?namespace=chronos
libsql://my-db.turso.io?authToken=eyJ...
libsql:///absolute/path/to/local.db
```

`namespace` defaults to `chronos`; valid identifiers match `^[a-z][a-z0-9_]{0,62}$` so the same value is safe across every dialect without quoting.

### Native providers

| Scheme(s)              | Driver                                       | Namespace translation                                                       |
|------------------------|----------------------------------------------|-----------------------------------------------------------------------------|
| `memory`               | none                                         | per-Open state — each call returns a fresh in-memory backend                |
| `sqlite` / `sqlite3`   | `modernc.org/sqlite` (pure Go, no CGO)       | accepted but not enforced — single-tenant by file                           |
| `postgres` / `postgresql` | `github.com/jackc/pgx/v5/stdlib`         | `CREATE SCHEMA IF NOT EXISTS <ns>` + `SET search_path TO <ns>`              |
| `mysql` / `mariadb`    | `github.com/go-sql-driver/mysql`             | `CREATE DATABASE IF NOT EXISTS <ns>`; reconnect with `<ns>` selected         |
| `libsql`               | `github.com/tursodatabase/libsql-client-go`  | accepted but not enforced — each remote DB is already a tenant boundary     |

### Wire-protocol compatibles (no extra Chronos code)

These databases speak the same wire protocol as one of the native providers and have been verified to work with Chronos through the same driver:

| Native provider | Compatibles                                                                              |
|-----------------|-------------------------------------------------------------------------------------------|
| `postgres`      | CockroachDB, YugabyteDB, Neon, Crunchy Bridge, TimescaleDB, AlloyDB Omni                  |
| `mysql`         | MariaDB (also via `mariadb://`), PlanetScale, TiDB, Vitess                                |
| `libsql`        | Turso (remote), local-file libSQL                                                         |

If your target speaks one of these wire protocols, point `CHRONOS_DB_DSN` at it and Chronos will treat it like the native provider — no extra builds, no extra dependencies.

### When to use which

- **`memory`** — tests and one-off exploration. State is lost on process exit; each `Open` returns a fresh backend.
- **`sqlite`** — single-process or embedded deployments. No CGO required. Use `:memory:` for ephemeral, a path for persistent.
- **`postgres`** — multi-process and production. Cluster-shared with the rest of the cognitive stack via per-tool `?namespace=`.
- **`mysql` / `mariadb`** — environments where MySQL is the standard data plane. Namespace is a database, not a schema (MySQL has no schemas).
- **`libsql`** — managed remote SQLite (Turso) or local libSQL files. SQLite-compatible at the SQL level; reuses the SQLite repository implementations.

### Legacy `CHRONOS_DB_TYPE` + `CHRONOS_DB_CONN`

For back-compat the older two-variable form is still accepted and translated internally to the new DSN. Mappings:

```
CHRONOS_DB_TYPE=memory                     -> memory://
CHRONOS_DB_TYPE=sqlite, CHRONOS_DB_CONN=p  -> sqlite://p
CHRONOS_DB_TYPE=sqlite, CHRONOS_DB_CONN=:memory:  -> sqlite://:memory:
CHRONOS_DB_TYPE=postgres, CHRONOS_DB_CONN=postgres://...  -> passthrough
```

`CHRONOS_DB_DSN`, when set, takes precedence over the legacy pair. New deployments should use the DSN form; the legacy pair will be removed in a future major version.

## Operations

- The HTTP server installs a `SIGINT`/`SIGTERM` handler that drains in-flight requests for up to 10 seconds before shutdown.
- The compute pipeline applies `CHRONOS_JOB_TIMEOUT` as a `context.WithTimeout` covering fetch, save, detect, and persist. Long-running upstreams should respect the supplied `context.Context`.
- `POST /v1/ingest` is the streaming entry point. Compute does not run automatically on ingest; run `chronos compute` (or call detection from a scheduler) at the cadence appropriate for your patterns.
- Logs are emitted via `log/slog` to stderr with a text handler at info level.

## Push notifications

Chronos can push newly-detected signals to consumers in addition to (or instead of) the polling API. Two transports ship today; both implement `internal/ports.Notifier` and fan out off the same `SignalRepository.Save` boundary.

### Webhooks

Configured via `CHRONOS_WEBHOOK_URLS` (comma-separated). Each signal becomes a POST with these headers:

- `Content-Type: application/json`
- `X-Chronos-Event: signal.detected`
- `X-Chronos-Delivery: <uuid-v4>` — unique per send attempt; consumers de-duplicate by this when retrying.
- `X-Chronos-Signature: sha256=<hex hmac>` — only when `CHRONOS_WEBHOOK_SECRET` is set. Computed over the raw body.

Body shape is identical to `/v1/signals` responses (see [`wire-contract.md`](wire-contract.md)). Best-effort delivery: `CHRONOS_WEBHOOK_RETRIES` retries on 5xx with 1s linear backoff, no retry on 2xx (success) or 4xx (consumer rejected). Failures are logged and counted in the `chronos_webhook_deliveries_total` metric.

**De-duplication is the consumer's responsibility.** Treat `Signal.ID` as the idempotency key.

### Server-Sent Events (SSE)

Available at `GET /v1/signals/stream?scope_id=<uuid>&pattern=<optional>`. Set `CHRONOS_DETECTION_INTERVAL` to a non-zero duration to enable the in-process detection scheduler — without it, `serve` only ingests, so the SSE stream would never produce events.

### Signal retention

A running scheduler appends; it never revises. Each detector derives its analysis window from the observations in front of it, so on a live stream that window slides forward with every tick and the scheduler's duplicate check — which keys on `(scope, series, pattern, window)` — correctly finds no match. The signals table therefore grows for as long as the scheduler runs, and because the store outlives the process, restarting the engine reclaims none of it.

`CHRONOS_SIGNAL_RETENTION` bounds that growth: once an hour the scheduler deletes every signal detected longer ago than the configured duration (evidence rows go with them via `ON DELETE CASCADE`). The default of `0` keeps everything, which is the historical behaviour; **set it on any deployment whose scheduler runs continuously**. A week (`168h`) is a reasonable starting point for a store that exists to feed live consumers.

## Bounding a detection tick

`CHRONOS_DETECTION_LOOKBACK` caps how much history each tick loads per scope. It defaults to `168h`.

Before this existed the scheduler loaded a scope's **entire recorded history** on every tick, through a query with no limit and no window. Nothing prunes `entity_states`, so that result grew for as long as the deployment ingested, and a scheduler on a 30-second timer re-materialised the whole table every 30 seconds. On a 73-series deployment the process sat at a flat 2 MiB, allocated roughly 1.9 GiB inside one tick, and was OOM-killed — repeating indefinitely.

Two properties of that failure are worth knowing, because both mislead:

- **It tracks table size, not ingest rate.** Raising the memory limit lengthens the cycle instead of stopping it, and restarting reclaims nothing because the store outlives the process.
- **It is not any detector.** The load happens before `Detect` is called, so disabling detectors — including the only `O(N²)` one — does not move it.

Detectors only ever consult their own analysis window, so history older than the lookback was loaded and then ignored.

**Sizing it.** The cost is roughly linear in the window, and the engine cannot compute it for you — it depends on `window × series × observation rate × row size`, and it knows none of those at config time. Measure instead: on a 73-series fleet at a five-minute cadence, a tick cost about **10.7 MB per hour of history**, so

| window | that fleet |
|---|---|
| `168h` | ~1.8 GB — OOM at a 2 GiB limit |
| `24h` (default) | ~257 MiB |
| `6h` | ~64 MiB |

The default is deliberately conservative. It shipped once at `168h`, sized for weekly seasonality, and OOM-killed the deployment it was written for on the first tick. **Weekly seasonality does not resolve inside 24h** — if you need it, raise this having done the arithmetic above, rather than assuming the default already accounts for your data. Set it to the longest window any enabled detector needs: seasonality needs more than one cycle of the rhythm it is looking for, which is what the `168h` default is sized for. Shorter is cheaper, and the cost is linear in the window.

**The sweep interval is the overshoot.** Whatever ages out between sweeps is still on disk, so the store holds up to `CHRONOS_RETENTION_SWEEP_INTERVAL` of data beyond the retention window. Keep it small relative to the window it enforces: sweeping hourly against `CHRONOS_SIGNAL_RETENTION=1h` holds two hours. On a fleet producing 2,430 signals/hour at ~716 evidence rows each, an hourly sweep is ~245 MB of slack and a five-minute sweep is ~20 MB.

Frequent sweeps are cheap. The delete is bounded and indexed, so a sweep with nothing to do is a probe returning zero rows, and retention runs on its own goroutine — it cannot delay detection however long it takes.

Retention is a store capability, not a requirement. If the configured backend cannot prune, the scheduler logs an error on every sweep rather than quietly doing nothing.

Each event has the form:

```
event: signal
data: {SignalDTO JSON}
```

Per-client buffer is bounded; slow consumers are silently dropped. The endpoint sends a `: connected\n\n` heartbeat comment immediately after subscription so clients can detect a successful connection before the first signal.

### Reliability

Both transports are at-most-once. The persistence record (`SignalRepository`) is the source of truth — push is a courtesy. Consumers needing replay should pair the live stream with a `/v1/signals` query keyed on the last-seen `detected_at`.

## Authentication

When `CHRONOS_API_TOKEN` is set, both HTTP (`Authorization: Bearer <token>` via `api.BearerAuth`) and gRPC (`authorization` metadata) reject unauthenticated calls. Leave it empty to skip auth in development. The `client.WithToken(...)` option sends the standard bearer header.

The gRPC server, when enabled (`CHRONOS_GRPC_PORT > 0`), checks the `authorization` metadata header against `CHRONOS_API_TOKEN`. Calls without a matching bearer token are rejected with `Unauthenticated`.
