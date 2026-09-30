---
title: "Mercure hub Prometheus metrics reference"
description: "Every Prometheus metric the Mercure hub adds to Caddy's: names, types, labels, what increments each one, and how rejections map to HTTP statuses."
---

# Mercure hub metrics reference

The hub exposes Caddy's built-in metrics plus the `mercure_*` series below. Enable the endpoint with the `metrics` global option, as described in [Health checks and monitoring](health-monitoring.md#prometheus-metrics). The Redis/Valkey transport adds its own `mercure_redis_*` series, documented in the [Redis transport README](../../redistransport/README.md#metrics).

The `reason` and `outcome` labels use closed sets. Binding and claim labels come from configuration; build labels describe the running binary. The `value` label of `mercure_subscribers_by_binding_value` is an accepted request header value authorized by the token issuer, so its cardinality follows the number of granted values. Prometheus adds scrape labels such as `job` and `instance`; those are not collector labels.

Hubs sharing a metrics registry adopt the same collectors, so their counts are aggregated without a hub-name label. Vectors expose no series until the corresponding label values have been observed; an absent rejection series does not mean the collector is unregistered.

## Restarts and config reloads

Every applied Caddy config reload swaps in a fresh metrics registry (Caddy keeps one registry per config). Counters therefore restart at 0 on each applied reload, not only when the process restarts. `rate()` and `increase()` tolerate the reset, but increments between the last scrape and the reload are lost. The subscriber gauges, `mercure_subscribers_connected` and `mercure_subscribers_by_binding_value`, start empty in the new registry and exclude subscribers held over from the previous config until they disconnect. A deployment that replaces tasks instead of reloading the config is unaffected.

## Metric summary

| Metric | Type | Collector labels |
| --- | --- | --- |
| `mercure_subscribers_connected` | Gauge | none |
| `mercure_subscribers_total` | Counter | none |
| `mercure_updates_total` | Counter | none |
| `mercure_updates_failed_total` | Counter | `reason` |
| `mercure_subscriber_disconnects_total` | Counter | `reason` |
| `mercure_subscribe_write_flush_seconds` | Histogram | `outcome` (`le` on buckets) |
| `mercure_claim_header_rejected_total` | Counter | `binding`, `reason` |
| `mercure_claim_value_rejected_total` | Counter | `claim`, `reason` |
| `mercure_subscribers_by_binding_value` | Gauge | `binding`, `value` |
| `mercure_version_info` | Gauge | `version`, `built_at`, `commit`, `go_version`, `os`, `architecture`, `upstream_version` |

## Subscribers and updates

`mercure_subscribers_connected` is the number of subscribers connected now: it increases on each accepted connection and decreases on disconnection; after an applied config reload it excludes subscribers held over from the previous config (see [Restarts and config reloads](#restarts-and-config-reloads)). `mercure_subscribers_total` counts every subscriber the hub has accepted since startup, including those that have since disconnected; its rate is the connection churn. `mercure_updates_total` counts updates the hub accepted for dispatch since startup. It counts at publish time, not per subscriber delivery.

## `mercure_updates_failed_total`

Counts publishes that did not complete, by `reason`. Pair it with `mercure_updates_total` for a success and failure ratio.

| `reason`     | Incremented when                                                                                                                                                                                                                                                      | HTTP status                              |
| ------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------- |
| `validation` | The update's content is rejected before dispatch: no topic, too many topics, an invalid topic, an invalid `retry`, or an update that fails validation. Also an update whose encoding is too large for the transport to store. A client error; nothing was stored.     | `400`; `413` for an oversized encoding    |
| `transport`  | Dispatch fails for any reason other than `publish_timeout`: backend unreachable, write or codec error, a transport closed during shutdown, or a deadline error not caused by `publish_timeout` (for example a cancelled parent context).                               | `500`                                    |
| `timeout`    | Dispatch is aborted by `publish_timeout`. The update may already be stored.                                                                                                                                                                                           | `504`                                    |

Not counted: authorization rejections, and request bodies that cannot be read or exceed the size limit (`400` or `413` before the update exists).

## `mercure_subscriber_disconnects_total`

Counts subscriber disconnections by `reason`, so you can tell routine recycling from failures. Each disconnection increments exactly one reason.

| `reason`          | Incremented when                                                                                                                                                                                                                                                                                                         |
| ----------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `client_closed`   | The client closed the connection: the request context was cancelled (tab closed, network drop, proxy dropped the connection). The usual healthy reason.                                                                                                                                                                 |
| `write_timeout`   | The hub closed the connection one `dispatch_timeout` before its write deadline (at the deadline itself when that is nearer than `dispatch_timeout`). The deadline is randomised to 80-100% of `write_timeout`, so about 475-595 s with the defaults; when the token's `exp` falls before that deadline, it is randomised to 80-100% of the time left until `exp` instead. With `write_timeout 0` it fires only for a token with `exp`. A clean periodic recycle, not an error.                                                                                                    |
| `hub_shutdown`    | The hub's root context was cancelled. Only with `write_timeout 0`, where it ends every subscriber at a stop and at every applied config reload, even a forced reload of an unchanged config; with a non-zero `write_timeout`, subscribers drain on their deadlines instead. Never visible in Prometheus: the disconnect is counted in the previous config's registry (reload) or after the metrics listeners have closed (stop). Read it from the `Subscriber disconnected` log line's `reason` attribute. See [Rolling updates](rolling-updates.md).                                                                                                                                                      |
| `write_failed`    | A write to the response stream failed: broken pipe, or the per-write deadline (`dispatch_timeout`) exceeded mid-write. Distinct from `write_timeout`, where the deadline was reached cleanly before another write.                                                                                                        |
| `transport_ended` | The transport closed the subscriber's update channel while the connection was healthy: the transport was closed (a reload that replaces it, or a stop once Caddy's `grace_period` ends), or the subscriber could not keep up with its buffer. Only the slow-subscriber case reaches Prometheus: a reload or stop is counted in the previous config's registry or after the metrics listeners have closed, so it shows only in the `Subscriber disconnected` log line.                                                                            |
| `transport_error` | Recorded only when the subscribe loop exits without a classified reason (in practice a panic) and removing the subscriber from the transport then fails with an error other than a closed transport. A removal error on a normal exit keeps the loop's reason. The bundled transports never fail this way; a non-zero count means a panicking subscribe loop together with a failing, possibly third-party, transport. Paired with a `Failed to remove subscriber on shutdown` error log.                                          |
| `unknown`         | Defensive fallback when no reason was recorded. It should never fire; a non-zero rate is a bug to report.                                                                                                                                                                                                                 |

## `mercure_subscribe_write_flush_seconds`

A histogram of the time spent on each SSE write: set the write deadline, write, flush, reset the deadline. It observes every write to a subscriber, updates and heartbeat comments alike, labeled `outcome`:

- `ok`: setting the deadline, writing, flushing and resetting the deadline all succeeded.
- `failed`: any step returned an error. Failed writes keep their latency, so a slow broken pipe stays distinguishable from a slow healthy write. A failed write also ends the connection with `write_failed` in `mercure_subscriber_disconnects_total`.

The finite bucket boundaries in seconds are `0.0001`, `0.0005`, `0.001`, `0.0025`, `0.005`, `0.01`, `0.025`, `0.05`, `0.1`, `0.25`, `1`, `2.5`, `5` and `10`, plus the implicit `+Inf` bucket. The 5 s boundary matches the default `dispatch_timeout`. Prometheus exposes `mercure_subscribe_write_flush_seconds_bucket` (with `le`), `mercure_subscribe_write_flush_seconds_sum` (elapsed seconds) and `mercure_subscribe_write_flush_seconds_count` (observations), each labeled `outcome`. Query `outcome="ok"` for latency percentiles so failures do not distort them.

## Claim bindings

These counters are the per-binding signal that a misconfigured [claim binding](../deployment/configuration.md#claim-bindings) is rejecting every request that carries one header value. Each rejection is also logged at info level. Both counters are defined for the hub's metrics implementation; the hub warns at startup if a binding is configured and the implementation cannot count it.

When several bindings fail in the same tier, the first configured one is counted. The tiers are bound-header faults (`400`), unusable bound-claim types (`401`), then claim absence or mismatch (`403`). The `malformed` header-binding series combines header and claim faults; it cannot distinguish `400` from `401` on its own:

| `reason`        | Meaning                                                                                                                                                              | HTTP status                                                      |
| --------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------- |
| `header_absent` | The bound header was not present (`require_claim_header` only).                                                                                                      | `400`                                                            |
| `malformed`     | Invalid presence. Header side: an empty or repeated bound header. Claim side: a claim that is an object, number or boolean, an array holding a non-string or empty element, an array over 1000 entries, or a shape the match mode forbids. | `400` for the bound header, `401` (`invalid_token`) for the claim |
| `claim_absent`  | The bound claim is missing, null or empty.                                                                                                                           | `403` (`insufficient_scope`)                                     |
| `mismatch`      | Both sides are well formed, but the claim does not authorize the header value (or, for a value binding, any configured value).                                        | `403` (`insufficient_scope`)                                     |

### `mercure_claim_header_rejected_total`

Requests rejected by a `require_claim_header` binding. Labels: `binding`, the operator-configured `claim:Canonical-Header` pair (for example `groups:Group-Id`), and `reason`, one of `header_absent`, `claim_absent`, `mismatch`, `malformed`.

A sustained `claim_absent` or `header_absent` rate points at a token issuer or edge misconfiguration. A `mismatch` spike can indicate token replay.

### `mercure_claim_value_rejected_total`

Requests rejected by a `require_claim_value` binding. Labels: `claim` and `reason`, one of `claim_absent`, `mismatch`, `malformed`. A value binding reads no header, so `header_absent` never occurs. These rejections are not counted in `mercure_claim_header_rejected_total`.

## `mercure_subscribers_by_binding_value`

Connected subscribers per binding value. Labels: `binding`, the `claim:Canonical-Header` pair of the `require_claim_header` binding that sets `count_subscribers` (the same string as the `binding` label of `mercure_claim_header_rejected_total`, for example `groups:Group-Id`), and `value`, the header value that binding matched. A subscriber is counted only when that binding matched, so an anonymous subscriber, or one whose binding `on_missing allow` skipped, is not counted and `value` only takes values the issuer granted. A series disappears when its last subscriber disconnects, and there are no series until a subscriber is counted.

The gauge increases when a counted subscriber connects and decreases when it disconnects. After an applied config reload it excludes subscribers held over from the previous config (see [Restarts and config reloads](#restarts-and-config-reloads)). Hubs that share a metrics registry, as all hubs in one Caddy configuration do, share these counts without a hub-name label.

## `mercure_version_info`

A gauge fixed at `1`, labeled with the build: `version`, `built_at`, `commit`, `go_version`, `os`, `architecture` and `upstream_version`. Use it to spot mixed versions during a rolling update.

## Dashboard

The repository ships a Grafana dashboard for these series in `grafana/dashboards/hub.json`. Besides the `mercure_*` series it charts uptime, config-reload status and the Go runtime, process, network (Linux only) and Caddy series. Select the scrape job and instance to scope the charts. The binding-value panel stays empty until `count_subscribers` is configured and a subscriber with a matching header value connects.

## Next steps

- [Health checks and monitoring](health-monitoring.md): health endpoints, alerts and canaries.
- [Rolling updates](rolling-updates.md): how subscribers drain on `write_timeout` during a deploy.
- [Configuration](../deployment/configuration.md): `require_claim_header`, `require_claim_value` and `count_subscribers`.
