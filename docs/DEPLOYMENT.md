# Deployment Runbook

Operator-facing notes for running Chronos in production. See `README.md` for the configuration table and `docs/wire-contract.md` for the stable wire shape consumers can branch on.

## Topology

```
           ┌──────────────┐
HTTP/gRPC ─┤  chronos     ├──── webhook consumers (push)
SSE       ─┤  serve       ├──── SSE clients (per-scope stream)
           └──────┬───────┘
                  │
       per-DSN backend (sqlite | postgres | mysql | libsql | memory)
```

`serve` runs HTTP and gRPC concurrently from the same process when both ports are set. The detection scheduler runs in-process when `CHRONOS_DETECTION_INTERVAL > 0`.

## Container

The image is `gcr.io/distroless/static:nonroot` based, pinned to digest `sha256:e3f9...0a39`. Built via GoReleaser:

```bash
goreleaser release --clean
```

For local Docker builds (no GoReleaser):

```bash
make docker-build
```

CGO is disabled in the build (`modernc.org/sqlite`). Refresh the digest with `docker buildx imagetools inspect gcr.io/distroless/static:nonroot` and update the Dockerfile in the same PR as the image bump.

The image declares a `HEALTHCHECK` that runs `chronos health`, which probes
`GET /health` on `http://127.0.0.1:$CHRONOS_HTTP_PORT` and exits non-zero when
the server does not report `healthy`. The probe is the binary itself because
distroless ships no shell and no `curl`. `/health` is exempt from bearer auth,
so the check works with `CHRONOS_API_TOKEN` set. Override the target with
`chronos health -addr <url> -timeout <duration>`. On Kubernetes, prefer the
native HTTP probes below — Kubernetes ignores the image's `HEALTHCHECK`.

## Kubernetes outline

A reference manifest is intentionally not committed. Minimum requirements:

- **Workload**: `Deployment` (stateless except for SQLite — use Postgres or libSQL/Turso for replicated deployments).
- **Replicas**: `≥ 2` for HA when using a multi-process backend; SQLite forces single-replica.
- **Probes**: `livenessProbe` and `readinessProbe` against `GET /health`.
- **Resources**: start at `requests: 100m CPU / 128Mi`, `limits: 1 CPU / 512Mi`. Detector cost scales with scopes × observations per scope.
- **Security context**: `runAsNonRoot: true`, `readOnlyRootFilesystem: true`, `allowPrivilegeEscalation: false`, drop all capabilities. The image runs as UID 65532 (`nonroot`).
- **Secrets**: `CHRONOS_DB_DSN`, `CHRONOS_API_TOKEN`, `CHRONOS_WEBHOOK_SECRET`. Mount as env vars from a `Secret`.
- **Graceful shutdown**: process honours SIGTERM; HTTP drains for up to 10 s, gRPC stops gracefully, scheduler stops via context cancellation. Set `terminationGracePeriodSeconds: 30`.

## Configuration cheat-sheet

```bash
# Bind both transports
CHRONOS_HTTP_HOST=0.0.0.0
CHRONOS_HTTP_PORT=7778
CHRONOS_GRPC_HOST=0.0.0.0
CHRONOS_GRPC_PORT=7779

# Postgres backend (multi-tenant via namespace)
CHRONOS_DB_DSN="postgres://user:pw@db:5432/cogstack?namespace=chronos"

# In-process detection scheduler (required for SSE)
CHRONOS_DETECTION_INTERVAL=30s

# Bound the signals table. A continuously-running scheduler appends on
# every tick and nothing else prunes, so leaving this at 0 means the
# store grows for as long as the process runs — and survives its restart.
CHRONOS_SIGNAL_RETENTION=168h

# Auth (HTTP + gRPC share this token)
CHRONOS_API_TOKEN=<bearer-secret>

# Webhook fan-out (optional)
CHRONOS_WEBHOOK_URLS=https://agent.example.com/v1/chronos-signal
CHRONOS_WEBHOOK_SECRET=<hmac-secret>
CHRONOS_WEBHOOK_RETRIES=2
```

Full reference: [`docs/configuration.md`](configuration.md).

## Observability

### Prometheus

Scrape `/metrics`:

```yaml
scrape_configs:
  - job_name: chronos
    static_configs:
      - targets: ['chronos:7778']
    metrics_path: /metrics
    scrape_interval: 15s
```

Key metrics:

- `chronos_signals_emitted_total{pattern}` — detector throughput per pattern.
- `chronos_observations_ingested_total` — ingest rate.
- `chronos_compute_duration_seconds_bucket` — Compute latency histogram.
- `chronos_webhook_deliveries_total{status}` — webhook fan-out outcomes.
- `chronos_sse_clients` — current SSE subscribers.

### Grafana

Import [`deploy/grafana/dashboards/chronos-overview.json`](../deploy/grafana/dashboards/chronos-overview.json). The dashboard panels:

- Signals emitted /sec (all patterns) and per-pattern rate.
- Observations ingested /sec (all adapters) and per-adapter rate.
- HTTP request rate, mean latency, status mix, 5xx ratio.
- Webhook deliveries by status.

### Logging

`log/slog` text handler to stderr at info level. Wrap with a JSON shipper at the orchestrator (Vector, Fluent Bit, etc.) for centralised aggregation.

## Operational procedures

### First-time bring-up

1. Provision storage. For Postgres / MySQL: pre-create the database, no schema work — Chronos auto-migrates on first connect.
2. Run a one-off pod with `CHRONOS_DB_DSN` set to verify migration.
3. Smoke-test: `curl -fsS -H "Authorization: Bearer $TOKEN" http://<host>:7778/health` returns the version block.
4. Push a sample observation: `curl -X POST -H "Authorization: Bearer $TOKEN" http://<host>:7778/v1/ingest -d '{...}'`.
5. List signals: `curl -H "Authorization: Bearer $TOKEN" 'http://<host>:7778/v1/signals?scope_id=<uuid>'`.

### Rolling out a new release

1. Bump image tag/digest; `kubectl rollout restart deployment/chronos`.
2. Watch p99 latency and 5xx rate for 5 min.
3. SSE clients reconnect automatically; webhook consumers see no gap if you keep the previous replica until the new one is Ready.

### Detector outage / runaway

1. Drop the detector by tightening its threshold env var temporarily (e.g. `CHRONOS_RECURRENCE_MIN_SAMPLE=99999`) and rolling.
2. Investigate without traffic pressure.
3. Restore the threshold once root cause is understood.

### Database migration

Migrations live in `internal/store/<backend>/migrations/` and run on connect. Older binary against newer schema is fine; reverse is not. Always roll forward.

### SSE client lag

SSE buffers are bounded per-client; slow consumers are silently dropped. The Grafana panel shows `chronos_sse_drops_total`. Pair the live stream with a `/v1/signals?since=...` query for gap recovery.

## Backup and restore

- **Postgres / MySQL / libSQL remote**: standard tooling for the backing engine.
- **SQLite**: stop the pod, copy the `.db` file out of the persistent volume.
- **libSQL local file**: same as SQLite.

Persisted records are immutable per the wire contract; replays of stale observations are safe (idempotent on `EntityState.id`).

## Known limitations

- HTTP and gRPC bearer tokens are static (`CHRONOS_API_TOKEN`); JWT-based auth is on the roadmap.
- Single-replica only on SQLite.
- Detection scheduler is in-process; multi-replica deployments must lease scope ownership externally to avoid duplicate detection.
- SSE delivery is at-most-once.
