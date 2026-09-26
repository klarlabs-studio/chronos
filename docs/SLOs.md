# Service Level Objectives

These are the formal SLOs Chronos targets in production. Alerting rules in [`deploy/grafana/dashboards/chronos-overview.json`](../deploy/grafana/dashboards/chronos-overview.json) surface shorter-window manifestations of each SLO; this document is the canonical statement.

## Headline SLOs

| SLO | Target | Window | Source signal |
|---|---|---|---|
| Availability | 99.9 % | 30 days, rolling | `up{job="chronos"}` from Prometheus blackbox or external probe |
| HTTP p99 latency (read) | < 150 ms | 30 days, rolling | `chronos_http_request_duration_seconds_*` for `GET /v1/signals*` |
| HTTP p99 latency (write) | < 250 ms | 30 days, rolling | same metric for `POST /v1/ingest` |
| HTTP error rate | < 0.1 % | 30 days, rolling | `chronos_http_requests_total{status=~"5.."}` |
| Signal-emission freshness | < 2 × `CHRONOS_DETECTION_INTERVAL` | 30 days, rolling | `time() - chronos_scheduler_last_tick_timestamp_seconds` |

## Error budgets

A 30-day budget at 99.9 % availability allows **43 minutes** of unavailability per window. Burn-rate alerts (multi-window, multi-burn-rate) fire when the budget is on track to be consumed faster than allowed:

- **Fast burn** — 14.4× budget burn over 1 h. Page immediately.
- **Slow burn** — 6× budget burn over 6 h. Ticket / next-business-day.
- **Trickle burn** — 3× budget burn over 3 d. Track in weekly review.

## Latency methodology

p99 is computed over 5-minute buckets, then aggregated to the 30-day window with a percentile-of-percentiles. This is approximate; for compliance reporting use the per-bucket values rather than a derived per-window number.

Endpoints excluded from the latency SLO:

- `/v1/signals/stream` (SSE; long-lived connection by design).
- `POST /v1/ingest` when streaming a large batch (latency is a function of batch size).

## Error classification

A request counts toward the error budget when it returns 5xx **or** when an internal middleware records an unhandled error in the metric pipeline. 4xx responses are explicitly **not** SLO-impacting — they are caller errors.

Webhook delivery failures are counted in `chronos_webhook_deliveries_total{status="failure"}` and tracked separately; webhook reliability is at-most-once by contract and does not consume the headline error budget.

## Detector freshness

The in-process detection scheduler ticks every `CHRONOS_DETECTION_INTERVAL`. A signal that should have been emitted at tick T must appear in the persistence layer by T + 2 × interval. Operators alerting on freshness should alert on `time() - chronos_scheduler_last_tick_timestamp_seconds`, which keeps working when no signals are produced; the latest `signals.detected_at` cannot tell a stalled scheduler from a quiet fleet.

This SLO does not apply to one-shot `chronos compute` invocations — those are batch operations whose latency is a function of dataset size.

## Reporting

Import [`deploy/grafana/dashboards/chronos-overview.json`](../deploy/grafana/dashboards/chronos-overview.json) and add an SLO row showing current attainment and remaining budget. The shipped dashboard covers headline rate panels; SLO attainment overlays are operator-specific and not committed.

Quarterly review: any SLO breached for two consecutive quarters triggers a service-level objective revision (either tighten the operational practice or relax the target with stakeholder sign-off).
