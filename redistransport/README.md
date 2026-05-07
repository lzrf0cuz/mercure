# Redis Transport for Mercure (Redis / Valkey)

[![Go Reference](https://pkg.go.dev/badge/github.com/lzrf0cuz/mercure/redistransport.svg)](https://pkg.go.dev/github.com/lzrf0cuz/mercure/redistransport)
[![Go Report Card](https://goreportcard.com/badge/github.com/lzrf0cuz/mercure/redistransport)](https://goreportcard.com/report/github.com/lzrf0cuz/mercure/redistransport)
[![Latest release](https://img.shields.io/github/v/release/lzrf0cuz/mercure?filter=redistransport%2Fv*&label=release)](https://github.com/lzrf0cuz/mercure/releases?q=redistransport)
[![License: AGPL-3.0](https://img.shields.io/badge/license-AGPL--3.0-blue)](LICENSE)

A [Redis Streams](https://redis.io/docs/latest/develop/data-types/streams/)–based transport
for the [Mercure](https://github.com/dunglas/mercure) hub. Targets
**Redis 6.2+** and **[Valkey](https://valkey.io) 7.2+** (see
[Compatibility](#compatibility) for version detection details).

## Quick Start

### Prerequisites

- [Task](https://taskfile.dev) (`brew install go-task` on macOS) — the canonical
  task runner used throughout the repo.
- Docker Compose v2 — bundled with Docker Desktop, OrbStack, or any modern
  Docker install.
- Go 1.26+ — only required for `task dev:redis` (native hub mode). Container
  paths build the hub in-image.

All commands below run from the repo root.

### 1. Start a Redis-compatible server

Pick one — Valkey and Redis are alternatives, not stacked. Both ship with
[RedisInsight](https://redis.io/insight/) on `http://localhost:5540` via the
shared `compose.redisinsight.yaml`, and both can opt into Prometheus +
Grafana with `--profile metrics` (see [Observability](#observability)).

**Option A: Valkey** (recommended — BSD-licensed)

```sh
task rt:valkey:up
# or from redistransport/:
# docker compose -f compose.valkey.yaml up -d
```

**Option B: Redis**

```sh
task rt:redis:up
# or from redistransport/:
# docker compose -f compose.redis.yaml up -d
```

### 2. Run the hub

Run the dev loop (debug UI, demo flow, redis transport, live-rebuild on
`.go` changes):

```sh
task dev:redis    # one-shot — brings up the redis stack (idempotent),
                  # runs the hub natively against local.Caddyfile with
                  # the redis transport injected via .env.dev.redis
```

To also tee the hub's stdout+stderr to a file (useful for capturing
logs from a long-running dev loop alongside the terminal stream), pass
`LOG_FILE=path` as a go-task variable:

```sh
task dev:redis LOG_FILE=/tmp/mercure.log
```

The flag threads through `dev:full` and `dev:full:tracing` as well.

The HS256 JWT keys come from `.env.dev` (the dev defaults — never use the
shipped `!ChangeThisMercureHubJWTSecretKey!` value in production). For
real secrets in dev, put them in `.env.local` (gitignored).

`redis-test.Caddyfile` is the transport-only testing scaffold (used by
`task go:run:redis` for in-package smoke + by conformance tests). It does
NOT enable the debug UI / demo flow — use `task dev:redis` for the dev
loop with UI + demo.

### 3. Open the Mercure debug UI

```text
https://localhost/.well-known/mercure/ui/
```

`task dev:redis` runs the hub against `local.Caddyfile`, which binds to
`:443` (HTTPS) — NOT `:4000`. The `:4000` default is specific to
`redis-test.Caddyfile` (testing scaffold). The debug UI auto-redirects
from `/` so plain `https://localhost/` works too. The first load shows a
self-signed-cert warning; run `task tls:trust` (after first hub start
generates the local CA) to silence it.

### 4. Publish your first event

The fastest path is the bundled debug UI's built-in publish form: open
the UI, Subscribe to `https://example.com/books/1`, then Publish to the
same topic and watch the event arrive.

For programmatic integration, mint an HS256 publisher JWT carrying the
`mercure.publish` claim and POST to `/.well-known/mercure`:

```sh
# Mint a publisher JWT (Python ≥ 3.x with PyJWT):
JWT=$(python3 -c "import jwt; print(jwt.encode({'mercure':{'publish':['*']}}, \
  '!ChangeThisMercureHubJWTSecretKey!', algorithm='HS256'))")

# Publish (–k accepts the dev self-signed cert):
curl -k -X POST https://localhost/.well-known/mercure \
  -H "Authorization: Bearer $JWT" \
  --data-urlencode "topic=https://example.com/books/1" \
  --data-urlencode 'data={"hello":"world"}'
```

In another terminal, subscribe via curl to confirm the round-trip:

```sh
curl -k -N "https://localhost/.well-known/mercure?topic=https://example.com/books/1"
```

You should see the event stream out as `data: {"hello":"world"}`.

## RedisInsight

Both compose stacks include [RedisInsight](https://redis.io/insight/) at
[http://localhost:5540](http://localhost:5540) (`task rt:redis:open` /
`task rt:valkey:open`). A one-shot `redisinsight-bootstrap` sidecar
POSTs the `Local Redis` / `Local Valkey` connection to the RedisInsight
API once the UI healthcheck passes.

## Observability

Both stacks support an opt-in Prometheus + Grafana add-on for inspecting
the redis transport's `mercure_redis_*` metrics during development. It is
gated behind the Compose `metrics` profile so the lightweight default
`up -d` doesn't pull the prom/grafana images for ad-hoc iteration.

| Service | URL | Notes |
|---|---|---|
| Prometheus | [http://localhost:9090](http://localhost:9090) | Scrapes `host.docker.internal:9091` (the natively-running hub) and `redis_exporter:9121`. |
| Grafana | [http://localhost:3000](http://localhost:3000) | Anonymous Editor enabled; admin password `admin`. Prometheus pre-provisioned as default datasource. |
| Redis Exporter | [http://localhost:9121/metrics](http://localhost:9121/metrics) | [`oliver006/redis_exporter`](https://github.com/oliver006/redis_exporter) — server-side metrics (memory, evictions, slow log, command throughput) so operators can distinguish "hub is slow" from "Redis is slow" during incidents. |

To bring up the stack with metrics:

```sh
task rt:metrics:up:redis    # against Redis stack
# or:
task rt:metrics:up:valkey   # against Valkey stack
```

For traces (Jaeger), add the matching tracing-profile task — Jaeger comes
up on the same compose project as Grafana so the Grafana trace dashboard's
datasource resolves (`http://jaeger:16686` in-cluster):

```sh
task rt:tracing:up:redis    # against Redis stack (or `task dev:full:tracing`)
# or:
task rt:tracing:up:valkey   # against Valkey stack
```

The hub itself runs natively (`task dev:redis` or `task go:run:redis`);
Prometheus scrapes it across the host bridge via
`host.docker.internal:9091`. The hub must already be running for
Prometheus to have anything to scrape.

> **Mutual exclusion with the root compose stack.** The container names
> `prometheus` and `grafana` are unique and conflict with the root
> `compose.yaml` if both are running. `task up:build` (root) and the
> `metrics` profile here are mutually exclusive — bring one down before
> bringing the other up.

### Server-side metrics

The transport's `mercure_redis_*` metrics report what the hub sees; the
Redis Exporter sidecar reports what the Redis/Valkey server sees. The
two together separate "hub is slow" from "Redis is slow" during load
testing and incidents. The "Redis server" row on the transport dashboard
covers the load-testing-relevant signals:

| Signal | Source metric | Reads as |
|---|---|---|
| Memory pressure | `redis_memory_used_bytes` / `redis_memory_max_bytes` | When `used / max` approaches 1, the next write evicts (LRU/LFU) or returns OOM (`noeviction`) — depending on `maxmemory-policy`. |
| Evictions | `rate(redis_evicted_keys_total[5m])` | Non-zero only when `maxmemory-policy` is an eviction policy. Dev `compose.redis.yaml` sets `allkeys-lru` (this counter rises and presence / `:lastEventID` keys are at risk); dev `compose.valkey.yaml` sets `noeviction` (counter stays at 0; pressure surfaces instead as failed XADD/SET). |
| Rejected connections | `rate(redis_rejected_connections_total[5m])` | `maxclients` saturated — also the dominant write-failure mode under `noeviction` once memory caps. |
| Throughput | `rate(redis_commands_processed_total[5m])` | Aggregate command rate. Compare against hub publish/dispatch counters to spot non-hub traffic. |
| Network | `rate(redis_net_input_bytes_total[5m])`, `rate(redis_net_output_bytes_total[5m])` | Output » input is normal during fan-out; sustained input bursts track publish storms. |
| CPU | `rate(redis_cpu_user_seconds_total[5m])` + sys | `1.0` = one full core. Sustained `> 0.7` means Redis itself is the bottleneck. |
| Per-command latency | `rate(redis_commands_duration_seconds_total[5m]) / rate(redis_commands_total[5m])` per `cmd` | Watch `evalsha` for Lua publish slowdowns and `xrange`/`xreadgroup` for history-replay or listener stalls. |
| Slow log | `redis_slowlog_length` | Commands above `slowlog-log-slower-than`. Pair with hub-side dispatch p99 to triage. |
| Memory fragmentation | `redis_mem_fragmentation_ratio`, `redis_memory_used_rss_bytes - redis_memory_used_bytes` | Ratio ~1.0–1.5 is normal; sustained `> 1.5` under churn = allocator fragmentation. The RSS overshoot sets the headroom needed beyond `maxmemory` to avoid OS-level OOM. |

> The dashboard panels substitute Grafana's `$__rate_interval` for the
> `[5m]` window above; both forms are valid — pick `[5m]` (or your
> preferred window) for ad-hoc Prometheus queries.

Replication / persistence series (`redis_connected_slave_*`, `redis_aof_*`,
`redis_rdb_*`) are exposed at `http://localhost:9121/metrics` but not
dashboarded — dev stacks run `--save "" --appendonly no` without replicas.

Reset state between load tests with `task rt:redis:flush` (FLUSHALL,
keeps Prometheus history) or `task rt:redis:down` (clean slate, drops
volumes). Valkey equivalents: `task rt:valkey:flush`, `task rt:valkey:down`.

### Tracing

The transport emits one OpenTelemetry span — `mercure.transport.history`
(`SpanKind=Internal`), from history replay — and tags the active parent
span on every `Dispatch`/`AddSubscriber`. Spans nest under whatever
parent the caller has active — typically the hub's `mercure.publish` or
`mercure.subscribe` span when [Caddy's `tracing` directive](https://caddyserver.com/docs/caddyfile/directives/tracing)
is enabled, otherwise the OpenTelemetry no-op tracer (no exporter cost,
no globals touched).

`mercure.transport.history` carries these attributes:

| Attribute | Type | Meaning |
|-----------|------|---------|
| `mercure.transport` | string | Always `redis` — the per-backend discriminator (the BoltDB transport sets `bolt`). |
| `mercure.subscriber.id` | string | The subscriber whose reconnect triggered the replay. |
| `mercure.last_event_id.requested` | string | The client's requested `Last-Event-ID`, length-capped (80 chars) so a single oversized header can't bloat trace storage. |
| `mercure.history.events_replayed` | int | Count of matched updates **delivered** to the subscriber. A dispatch the subscriber refused (gone / buffer full) is not counted. |
| `mercure.history.future_dated` | bool | The future-ID guard tripped: the requested ID post-dates the hub's clock, so both replay passes were skipped. |
| `mercure.history.full_scan` | bool | Pass 1 (UUIDv7 seek) missed, so a Pass 2 full-stream rescan was entered (it may itself be cut short — see `truncated`). |
| `mercure.history.truncated` | bool | The backlog was **not** fully replayed: a matched update went undelivered because the subscriber disconnected (for any reason — client close, shutdown) or its buffer was full. |

The four `mercure.history.*` attributes report the replay outcome —
detail the BoltDB transport's single `db.View` read can't. `full_scan`
is set after a Pass 2 that ran without an XRANGE error, agreeing with
the `mercure_redis_history_replay_fallback_total` metric. **`truncated`
is a completeness signal, not an error**: the span stays `Status=OK`
(a gone subscriber isn't a transport failure), so alert on the
attribute itself, not span status — and expect it during normal client
churn and rolling restarts. For a true backpressure signal use the
`subscribers_lost{reason="backpressure"}` metric.

In addition, `Dispatch` and `AddSubscriber` annotate the **active parent
span** (`mercure.publish` / `mercure.subscribe`) with
`mercure.transport="redis"` on entry. This makes those traces filterable
by backend without a wall-clock-echo child span — the attribute costs
one `SetAttributes` call, gated by `span.IsRecording()`.

Span name, `mercure.subscriber.id` / `mercure.last_event_id.requested`
attributes, and failure-status semantics match the BoltDB transport.
The history span is flipped to `codes.Error` via
`recordSpanError` on any failure path (XRANGE error, semaphore-acquire
context cancellation, etc.), matching the BoltDB transport's
`dispatchHistory`, so cross-backend dashboards filtering on
`mercure.transport=redis` vs `bolt` or on `span.Status` see Redis
history failures the same way they see Bolt ones. The full list of
hub-emitted parent spans (`mercure.publish`, `mercure.subscribe`, …)
lives in [the hub's tracing docs](https://github.com/dunglas/mercure/blob/main/docs/hub/tracing.md).

The XREADGROUP listener and presence-GC heartbeat goroutines are
unspanned — their latency lives in the `mercure_redis_xreadgroup_*`
and `mercure_redis_*` histograms. Spanning each cycle would add
high-cardinality short spans with no diagnostic signal beyond what
the histograms already provide, and they're long-running background
loops rather than request-scoped operations.

For per-Redis-command tracing (e.g. timing individual `EVALSHA` or
`XRANGE` calls), wrap the go-redis client with
[`redisotel`](https://pkg.go.dev/github.com/redis/go-redis/extra/redisotel/v9)
hooks before handing it to the transport. The transport accepts a
pre-built `redis.UniversalClient`, so instrumentation can live in the
embedding Caddy module without library buy-in here.

## Metrics

Every metric carries two const labels:

- `transport_type="redis"` — the transport **family** (always `"redis"`
  for this implementation). Lets dashboards namespace cleanly when
  multiple Mercure transports coexist in the same registry.
- `backend_type` — the detected **backend server**: `"redis"`, `"valkey"`,
  or `"unknown"`. Sourced from `INFO server` at startup (the
  `valkey_version:` field wins when both are present — Valkey emits
  `redis_version:` for wire-protocol compatibility). Lets dashboards
  split deployment fleets by backend identity while still grouping all
  redis-streams transports under `transport_type`.

Names are stable and follow the `mercure_redis_<subsystem>_<unit>`
convention. The full list is grouped below by subsystem with the
actionable signal for each.

**Dashboard bookmark + rolling-upgrade notes:** the bundled Grafana
dashboard's `backend_type` template variable defaults to `$__all`
(regex `.*`), so legacy bookmark URLs (without `?var-backend_type=...`)
keep working. Two semantic shifts to know about:

- A dashboard URL captured before a backend was added implicitly
  filtered to "the one backend that exists." After a second backend
  joins, the same URL now shows multi-backend mixed series (since
  `.*` matches both). Re-capture bookmarks after backend topology
  changes.
- During a rolling upgrade where some hub instances are pre-this-version
  (no `backend_type` label) and some are post-, both kinds of series
  match `=~".*"`. Pre-upgrade series render legends with an empty
  prefix (`{{backend_type}}` interpolates to `""`); post-upgrade series
  render correctly. Self-heals once the upgrade completes.

**Caddy hot-reload caveat:** Prometheus collector identity includes
ConstLabel **values**, not just names. On a reload that swaps backends
(e.g., Redis URL → Valkey URL within one Caddy process without a
restart), the new transport instance registers a **second, separate set**
of collectors stamped `backend_type="valkey"` — and the original
`backend_type="redis"` collectors stay on the registry, owned by the
now-closed transport, frozen at their last observed values. Operator
impact: dashboards filtered by `backend_type="redis"` show stale data
from the prior backend until the hub process restarts. Filtering by
the new backend shows fresh data. Same-backend reloads adopt cleanly
via `AlreadyRegisteredError` (no drift). The pool-collector mismatch
Warn in `RegisterMetricsWith` is a programmer-error guard for the
same-identity-different-field-value case; the cross-backend ghost-
series accumulation is detected by the divergent identity itself and
is intentional pre-1.0 behavior. Restart the hub after a backend swap
for clean attribution.

**Transparent-backend-swap caveat:** the same staleness exists for
the rarer case where the data store behind a stable endpoint is
swapped (e.g., a transparent proxy like Envoy or HAProxy routes
`redis.internal:6379` at a Valkey replica during a live migration).
The transport detects backend identity at `NewRedisTransport` by
parsing `INFO server`. The `backend_type` label is then a Prometheus
ConstLabel for the lifetime of the process; later `INFO server`
results do not update it. **Restart the hub after the endpoint flips
to the new engine** so the label reflects reality. Polling `INFO` to
auto-correct was rejected: Prometheus const labels cannot change at
runtime, and a "current backend" gauge would not surface in existing
alerts.

### Publish path

| Metric | Collector | Labels | Reads as |
|---|---|---|---|
| `mercure_redis_publish_duration_seconds` | Histogram | — | End-to-end `Dispatch()` latency (Lua script: XADD + last-event-ID SET). p99 > 50ms in steady state usually means Redis under pressure. |
| `mercure_redis_publish_payload_bytes` | Histogram | — | Per-event payload size after codec encoding (XADD message body). Pair with `Network throughput (Redis)` and `publish_duration_seconds` p99 to predict bandwidth scaling and to spot publish-latency regressions caused by payload-size shifts. |
| `mercure_redis_publish_errors_total` | CounterVec | `kind` (`encode`/`publish`) | `Dispatch()` failures. Divide by `publish_duration_seconds_count` for an error rate. |
| `mercure_redis_publisher_rate_limited_total` | Counter | — | Publishes delayed by `WithPublisherRateLimit`. Sustained non-zero indicates the rate cap is the bottleneck. |
| `mercure_redis_stream_length` | Gauge | — | Sampled during health-check ticks. Watch for unbounded growth when neither `max_length` nor `event_ttl` is set. |

### Dispatch + fan-out

| Metric | Collector | Labels | Reads as |
|---|---|---|---|
| `mercure_redis_dispatch_duration_seconds` | HistogramVec | `shard` | Per-shard worker time (MatchAny + Dispatch). High values across all shards = local subscriber processing is the bottleneck. |
| `mercure_redis_dispatch_subscribers_matched` | Histogram | — | Matched-subscriber count per message (recorded once per message after fan-out). |
| `mercure_redis_shard_subscribers` | GaugeVec | `shard` | Live subscriber count per dispatch shard. Imbalance = degenerate hash distribution. |

### Stream listener (XREADGROUP)

| Metric | Collector | Labels | Reads as |
|---|---|---|---|
| `mercure_redis_xreadgroup_latency_seconds` | Histogram | — | Time from XREADGROUP call to result, including BLOCK wait. Bimodal: fast = stream had data, slow ≈ `xread_block` = stream was idle. |
| `mercure_redis_xreadgroup_batch_size` | Histogram | — | Entries per batch. Spikes near `xread_count` mean the listener is the bottleneck. |
| `mercure_redis_xreadgroup_errors_total` | CounterVec | `kind` (`nogroup`/`nogroup_recreate_failed`/`other`/`xack`) | `nogroup` = a recoverable blip after consumer-group destruction; `other` = connectivity or ACL problem (alert). |
| `mercure_redis_stream_decode_errors_total` | CounterVec | `kind` (`data_type`/`unmarshal`/`forbidden_sse_chars`) | Stream entries dropped on the consumer side before dispatch. `data_type`/`unmarshal`: payload decode failure — non-zero often signals a mixed-codec stream (see codec migration in the runbook). `forbidden_sse_chars`: the decoded update carried a CR/LF/NUL in `id`/`type` that would forge SSE frames (CWE-93) — a defense-in-depth drop against an unvalidated or tampered entry (an old/non-compliant hub, or direct stream tampering). Should be zero; a sustained rate is a security signal warranting a higher-priority alert than the codec kinds. The per-entry drop log is rate-sampled (a few lines then ~1/sec) so a flood can't drown the logs — this counter, not the log, is the authoritative rate. |

### History replay

| Metric | Collector | Labels | Reads as |
|---|---|---|---|
| `mercure_redis_history_replay_duration_seconds` | Histogram | — | Per-replay wall time (paginated XRANGE). |
| `mercure_redis_history_replay_concurrent` | Gauge | — | In-flight replays. Cap at `WithHistoryReplayConcurrency` (default 20); reaching the cap means subscribers are queuing for a slot. |

### Subscriber lifecycle

| Metric | Collector | Labels | Reads as |
|---|---|---|---|
| `mercure_redis_subscriber_add_total` | Counter | — | Total `AddSubscriber` calls. |
| `mercure_redis_subscriber_remove_total` | Counter | — | Total `RemoveSubscriber` calls. |
| `mercure_redis_add_subscriber_duration_seconds` | Histogram | — | End-to-end AddSubscriber latency (rate-limit + lock + shard insert + history replay setup). |
| `mercure_redis_remove_subscriber_duration_seconds` | Histogram | — | RemoveSubscriber latency (lock + shard remove). p99 spikes during mass disconnects are expected. |
| `mercure_redis_subscriber_rate_limited_total` | Counter | — | AddSubscriber calls delayed by `WithSubscriberRateLimit`. |
| `mercure_redis_subscribers_lost_total` | CounterVec | `reason` | Subscribers dropped with operational cause; at-most-one increment per subscriber across racing paths (first-attribution wins). `reason="shutdown"` (forced disconnect during `Close`, not in `subscriber_remove_total`). `reason="history_replay_failed"` (XRANGE error during `AddSubscriber`, not in `subscriber_remove_total`). `reason="backpressure"` (`LocalSubscriber.Dispatch` returned false during live dispatch — dominant cause is slow-consumer `handleFullChan`; also counts the rare race where another path disconnected the subscriber in the same dispatch loop. The SSE handler ultimately observes the closed channel, exits, and triggers `RemoveSubscriber`, so backpressure events appear in `subscriber_remove_total` too). |
| `mercure_redis_consumer_lag` | Gauge | — | Stream entries this node's consumer group has not yet consumed (XINFO GROUPS lag). Sustained non-zero = hub falling behind real-time. Reports `-1` when Redis cannot compute lag deterministically (entries XDEL'd from the middle, or pre-Redis-7.0 server). Sampled at the health-check cadence. |
| `mercure_redis_consumer_pending` | Gauge | — | Messages this node XREADGROUP'd but has not XACK'd yet (PEL size). Sustained growth indicates dispatch/ack pipeline backup beyond normal batch churn. |
| `mercure_redis_consumer_groups` | Gauge | — | Number of consumer groups attached to the stream (XINFO GROUPS length). One group per hub instance plus any not-yet-GC'd zombies. Gates the misconfiguration alert `publish_rate > 0 AND groups == 0` — catches "publisher writing to a stream nobody reads" (typo'd `stream` directive, wrong tenant). Reports `-1` on XINFO error (distinct from "Redis returned zero groups"). Sampled at the health-check cadence. |
| `mercure_redis_clock_drift_seconds` | Gauge | — | Absolute clock drift between hub and Redis server at connect time. Drift > `clockSkewMargin` (default 5s) breaks UUIDv7 timestamp ordering for history replay. |
| `mercure_redis_history_window_seconds` | Gauge | — | Elapsed wall-clock seconds between the stream's first and last entries. Proactive counterpart to `history_replay_fallback_total` — alert when this falls below the expected client reconnect duration to catch retention-too-tight regressions before clients start missing. Reports `0` on empty stream; `-1` when head/tail stream IDs cannot be parsed. Sampled at the health-check cadence. |
| `mercure_redis_history_replay_fallback_total` | Counter | — | Replays that fell through Pass 1 because the requested Last-Event-ID was past retention, triggering a Pass 2 full-stream replay. Sustained rate means retention is too tight for the reconnect window your clients exercise. |
| `mercure_redis_history_replay_truncated_total` | Counter | — | Replays cut short before delivering the full backlog because a matched update couldn't be handed off (subscriber disconnected, for any reason, or buffer full). Aggregate companion to the `mercure.history.truncated` span attribute; a sustained rate means reconnecting clients aren't getting their complete history (slow consumers or restart churn). |
| `mercure_redis_history_cursor_rejected_total` | Counter | — | History-replay entries whose `eventID` stream metadata carried forbidden SSE chars (CR/LF/NUL), so the `Last-Event-Id` cursor was not advanced to it (the entry was still delivered with its validated `update.ID`). Should be zero; non-zero indicates a tampered stream or a non-compliant hub. The Warn log for this event is rate-sampled, so this counter — not the log — is authoritative. |
| `mercure_redis_dispatch_zero_match_total` | Counter | — | XREADGROUP'd events that fanned out to zero local subscribers. Multiplied by replica count, surfaces fleet-wide fan-out cost of chatty publishers whose topics nobody subscribes to. |
| `mercure_redis_ttl_cleanup_errors_total` | Counter | — | Failures of the periodic XTRIM-by-MINID call that enforces `event_ttl`. Persistent non-zero means stream growth is unbounded despite configured TTL. |

### Presence

| Metric | Collector | Labels | Reads as |
|---|---|---|---|
| `mercure_redis_presence_payload_bytes` | Histogram | — | SET payload size. Crossing `presence_detail_threshold` (default 1 000) flips to summary mode (count-only). |
| `mercure_redis_presence_read_errors_total` | CounterVec | `kind` (`get`/`unmarshal`) | Presence-key read failures in `GetSubscribers`. `kind=unmarshal`: one corrupt value skipped, the rest of the response still returned. `kind=get`: an MGET batch failed, so the whole call aborts with an error and returns NO results (count reflects the failed batch's keys, not the whole fleet). |
| `mercure_redis_presence_read_skipped_total` | Counter | — | Presence keys skipped due to expected transient states (missing key / empty value). Background noise; not actionable. |
| `mercure_redis_presence_read_capped_total` | Counter | — | `GetSubscribers` calls refused because the subscriber count exceeded `subscriptions_max_subscribers` (returns 503, no partial results). Non-zero = the `/subscriptions` endpoint is unusable at the current fleet size; raise the cap or stop polling it. |
| `mercure_redis_presence_heartbeat_errors_total` | Counter | — | Failed presence-key SET calls. Sustained non-zero risks `presence_ttl` expiry → the node gets zombie-GC'd by peers. |
| `mercure_redis_presence_fallback_total` | CounterVec | `reason` (`byte_budget`/`summary_marshal_failed`) | Presence-encoding decisions that fell back to summary mode after detail-mode encoding crossed `presence_detail_byte_threshold`. Counted at decision time, BEFORE the Redis SET — a downstream SET failure is tracked by `mercure_redis_presence_heartbeat_errors_total`, not by this counter. `reason=byte_budget` is the normal byte-gate path; `reason=summary_marshal_failed` means the summary fallback itself failed and the over-budget detail payload was shipped as a last-resort liveness signal — sustained non-zero on that label indicates payload shapes the codec cannot handle. |

### Health & GC

| Metric | Collector | Labels | Reads as |
|---|---|---|---|
| `mercure_redis_healthy` | Gauge | — | `1` = healthy, `0` = unhealthy. Drives the `/mercure/health/ready` 503 response after `health_threshold` consecutive PING failures. |
| `mercure_redis_zombie_gc_errors_total` | Counter | — | Per-group failures during zombie consumer-group cleanup. Persistent non-zero may indicate zombie accumulation that needs operator attention. |
| `mercure_redis_build_info` | GaugeVec | `version`, `revision`, `go_version` | Constant `1` per process. Distinguishes transport versions across nodes during a rolling deploy. `version` resolves via build-time ldflag (`RT_VERSION`) with a `runtime/debug.ReadBuildInfo` fallback; `revision` is the short VCS SHA (empty under `go test` / `go run`, or any `-buildvcs=false` build — including the hub image, whose Dockerfile sets it). |
| `mercure_redis_pre_binding_metric_drops_total` | Counter (read-through) | — | Metric-emission attempts that landed before `RegisterMetricsWith` bound the collector. Non-zero means the late-binding window covered live traffic — bind metrics earlier (direct API: `WithPrometheusRegisterer`; Caddy: parent `metrics_registry` reaches the transport via `bindTransportMetrics` inside `Provision`). Read-through, so a scrape always reflects the running atomic total even after binding. Rebinds across Caddy reloads so a new transport instance against a persistent registry reports its own drops, not a prior instance's. |

### go-redis client pool (read-through)

The transport registers a Prometheus collector that read-throughs go-redis's
`PoolStats()` on every scrape, so the pool surface is observable without
periodic polling.

| Metric | Collector | Reads as |
|---|---|---|
| `mercure_redis_client_pool_hits_total` | Counter | Connection-pool hits (a pooled conn was available). |
| `mercure_redis_client_pool_misses_total` | Counter | Pool misses (a new conn was opened). High miss rate = pool size too small. |
| `mercure_redis_client_pool_timeouts_total` | Counter | Connections that waited longer than `pool_timeout` for a free slot. Non-zero = pool exhaustion; raise `PoolSize` or lower `WithHistoryReplayConcurrency`. |
| `mercure_redis_client_pool_total_conns` | Gauge | Currently-open connections. |
| `mercure_redis_client_pool_idle_conns` | Gauge | Currently-idle connections. |
| `mercure_redis_client_pool_stale_conns_total` | Counter | Idle conns reaped by go-redis's stale-conn check. |

### Suggested starter alerts

- `rate(mercure_redis_publish_errors_total[5m]) > 0` — sustained publish failures.
- `rate(mercure_redis_xreadgroup_errors_total{kind!="nogroup"}[5m]) > 0` — non-recoverable listener errors.
- `mercure_redis_healthy == 0 for 1m` — sustained Redis outage.
- `rate(mercure_redis_client_pool_timeouts_total[5m]) > 0` — pool exhaustion.
- `histogram_quantile(0.99, sum by (le) (rate(mercure_redis_dispatch_duration_seconds_bucket[5m]))) > 0.05` — dispatch p99 regression past 50ms.
- `mercure_redis_consumer_lag == -1 for 5m` — lag sampler hit non-deterministic case (warning, not page).
- `mercure_redis_consumer_pending > <threshold> for 5m` — PEL growth; threshold 10–100× steady state.
- `mercure_redis_consumer_groups == 0 AND rate(mercure_redis_publish_duration_seconds_count[5m]) > 0 for 5m` — publishes succeed but no hub reads. Almost always a `stream` directive typo or a tenant deployed against the wrong Redis. Page.
- `abs(mercure_redis_clock_drift_seconds) > 4` — early warning before default 5s `clockSkewMargin`. NaN means the `TIME` probe failed.
- `mercure_redis_history_window_seconds < <expected_reconnect_seconds> for 5m` — retention is tightening; clients reconnecting at the expected gap are about to miss. Proactive companion to the `history_replay_fallback` rate alert. Choose the threshold from your client reconnect-backoff ceiling.
- `rate(mercure_redis_history_replay_fallback_total[15m]) > 0.01` — chronic Pass-1 misses (full XRANGE scans).
- `rate(mercure_redis_history_replay_truncated_total[15m]) > 0.01` — reconnecting clients are losing backlog mid-replay (slow consumers or restart churn); correlate with `subscribers_lost{reason="backpressure"}`.
- `rate(mercure_redis_dispatch_zero_match_total[15m]) / rate(mercure_redis_publish_duration_seconds_count[15m]) > 0.5 for 1h` — over half of published events match no local subscriber on this replica. Often means a publisher emitting to dead topics — investigate before the cost shows up in Redis CPU and hub fan-out.
- `rate(mercure_redis_ttl_cleanup_errors_total[5m]) > 0` — XTRIM MINID failing; stream grows unbounded.

## Compose Stacks

| Stack | File | Services | Task |
|---|---|---|---|
| **Valkey** | `compose.valkey.yaml` | Valkey 8, RedisInsight, (+ optional Prometheus/Grafana via `--profile metrics`) | `task rt:valkey:up` |
| **Redis** | `compose.redis.yaml` | Redis 7, RedisInsight, (+ optional Prometheus/Grafana via `--profile metrics`) | `task rt:redis:up` |
| **Redis Cluster** | `compose.cluster.yaml` | Redis 7 (3 masters; plain 7000-7002, TLS 8000-8002) | `task rt:cluster:up` |

Shared service definitions live in their own files and are pulled into
the parent stacks via Compose's `include:` directive — one source of
truth per service:

| Shared file | Provides | Pulled into |
|---|---|---|
| `compose.redisinsight.yaml` | RedisInsight UI on `:5540` | redis, valkey |
| `compose.observability.yaml` | Prometheus on `:9090` + Grafana on `:3000` (gated by `metrics` profile) | redis, valkey |

```sh
# Valkey stack
task rt:valkey:up          # Start Valkey + RedisInsight
task rt:valkey:down        # Stop and remove volumes
task rt:valkey:logs        # Follow logs
task rt:valkey:open        # Open RedisInsight in browser

# Redis stack
task rt:redis:up           # Start Redis + RedisInsight
task rt:redis:down         # Stop and remove volumes
task rt:redis:logs         # Follow logs
task rt:redis:open         # Open RedisInsight in browser

# Redis Cluster stack (3 masters, cluster_state=ok at startup)
task rt:cluster:up         # Start cluster
task rt:cluster:down       # Stop and remove volumes
task rt:cluster:logs       # Follow logs
```

The cluster stack runs three `redis-server` processes in a single container
with `--cluster-announce-ip 127.0.0.1`, so host clients connecting to any
seed (`127.0.0.1:7000-7002`) receive a fully host-routable cluster topology.
This single-container layout is structurally required: with bridge-network
multi-container setups, the `cluster-announce-ip` value used for client
routing also drives inter-node gossip — fragmenting them needs a custom
client Dialer rewrite, which is the kind of test-infrastructure hack we
opt out of.

Each node also listens on a TLS port (data port + 1000 → 8000-8002) using
a self-signed cert generated at startup. The TLS-on-Cluster integration
test connects to the TLS plane with `InsecureSkipVerify`, mirroring the
production pattern documented for clusters whose nodes do not share a
single SAN.

## Caddyfile Configuration

```caddyfile
mercure {
    transport redis {
        url               redis://localhost:6379
        stream            mercure
        max_length        10000
        encoding          json
        event_ttl         24h
        cleanup_interval  5m
    }
}
```

### Redis Cluster (multi-node, sharded)

Use `addresses` instead of `url` to give the client multiple seed nodes
for slot-routing discovery. `url` and `addresses` are mutually exclusive.

```caddyfile
transport redis {
    addresses redis-1:6379 redis-2:6379 redis-3:6379
    username  myuser
    password  {env.REDIS_PASSWORD}
    db        0
    tls
    stream    mercure
}
```

### Redis Sentinel (HA failover)

Set `master_name` alongside `addresses` (the sentinel seed addresses) to
select Sentinel topology — the client tracks the current master and
fails over automatically.

```caddyfile
transport redis {
    addresses    sentinel-1:26379 sentinel-2:26379 sentinel-3:26379
    master_name  mymaster
    password     {env.REDIS_PASSWORD}
}
```

### Choosing a topology

> **Scaling ceiling per stream.** Hash-tagged keys for a `stream` name
> route to a single Redis Cluster slot, which lives on a single primary.
> Per-stream throughput is bounded by what one Redis node sustains
> (order of 10⁴ XADD/XREADGROUP ops/sec; benchmark your own hardware).
> Adding Cluster nodes does not raise a single stream's ceiling; it
> enables running additional independent streams in parallel.
>
> **To scale past one slot:** run multiple `stream` names (one per
> tenant or partition axis). Each name hashes to a different slot, so
> N streams give N× the headroom on an N-shard Cluster. Mercure hubs
> are configured per-stream via the Caddyfile, so this is a deployment
> decision, not a transport-level dynamic shard.

The right choice depends on how many independent **streams** you run.
Topic-level multi-tenancy (many topics on one stream) does NOT benefit
from Cluster — only distinct `stream` names hash to different slots.

| Use case | Recommended topology | Caddyfile |
|---|---|---|
| Single mercure hub fleet, one transport stream, many topics (e.g. per-user or per-channel topics) | Single instance with replicas, OR Sentinel for self-managed HA. ElastiCache "cluster mode disabled" (CMD) maps here. | `url redis://primary:6379` (single endpoint) or `addresses ... master_name ...` (Sentinel) |
| Multiple isolated hub fleets, distinct `stream` names per fleet (e.g. one per region or deployment) | Redis Cluster — distinct stream names hash to distinct slots, sharding fleets across nodes. ElastiCache "cluster mode enabled" (CME) maps here. | `addresses node1 node2 node3` |
| Dev / single-node | Anything; `url redis://localhost:6379` | bundled `redis-test.Caddyfile` |

**AWS ElastiCache specifics:**

- **Cluster mode disabled (CMD)** — point `url` at the **primary endpoint**.
  AWS handles failover by re-pointing the primary endpoint at the promoted
  replica; failover duration is environment-dependent (engine version,
  replica lag, DNS-propagation TTL — see [AWS Auto Failover](https://docs.aws.amazon.com/AmazonElastiCache/latest/dg/AutoFailover.html)).
  Replicas exist for durability but are unused by this transport:
  XREADGROUP and XADD both require the primary, so reader endpoints are
  not configured.
- **Cluster mode enabled (CME)** — point `addresses` at the
  **configuration endpoint** (or a list of seed nodes). go-redis follows
  MOVED redirects to the slot-owning shard. Each shard has its own
  failover trigger, so an outage of one primary doesn't affect other
  shards. `tls_server_name` works only when nodes share a wildcard SAN
  — see the TLS notes below.
- **Valkey on ElastiCache** — same as Redis, supported as of Valkey 7.2.
  Connect string and topology selection are identical.

**Failover behavior of this transport:**

- The XREADGROUP listener reconnects with **capped exponential backoff**
  (1s base, doubling, 30s ceiling) so a flapping primary doesn't produce
  a tight error-log loop.
- PEL dedup prevents double-dispatch when reading entries that were
  already delivered before the disconnect.
- History replay catches subscribers up across the failover gap; the
  `lastEventID` key (also hash-tagged) survives the failover because it
  lives on the same shard as the stream.

### Auth and TLS

`username`, `password`, `db`, and `tls` directives layer over URL-derived
values — explicit directives win and emit a Warn log per conflicting
field. This lets you bake `url redis://placeholder:placeholder@host:6379/0`
into the base config and inject `password {env.REDIS_PASSWORD}` at deploy
time without URL-encoding `@`/`:`/`/`. `tls` alone enables TLS with
defaults (server name from address, TLS 1.2 minimum).

For custom certificates (private CA, mutual TLS), use the dedicated
directives — they imply `tls` automatically:

```caddyfile
transport redis {
    url redis://placeholder:placeholder@host:6379/0
    password {env.REDIS_PASSWORD}

    tls_ca_file       /etc/ssl/certs/redis-ca.pem
    tls_client_auth   /etc/ssl/certs/client.pem /etc/ssl/private/client.key
    tls_server_name   redis.internal
}
```

- **`tls_ca_file`** — PEM file with trusted CA certificate(s). Multi-cert
  bundles are accepted. **Replaces** the system root pool (matches
  `openssl -CAfile`); bundle system CAs into the file if you need both.
- **`tls_client_auth CERT KEY`** — atomic two-argument form for mutual
  TLS, matching Caddy's `reverse_proxy` convention. Both files must be
  PEM-encoded; the key must be **unencrypted** (Go's stdlib does not
  decrypt password-protected PEM keys — strip with
  `openssl pkcs8 -in key.pem -nocrypt -out key-decrypted.pem`).
- **`tls_server_name`** — overrides the SNI / certificate verification
  hostname. Useful when dialing by IP. **Cluster mode caveat:** go-redis
  applies a single shared `ServerName` across all cluster nodes, so this
  directive only works for clusters whose nodes share a SAN (e.g.,
  wildcard certs). Mismatched per-node SANs require either
  `tls_insecure_skip_verify` or a wildcard cert covering all nodes.
- **`tls_insecure_skip_verify`** — disables certificate verification
  entirely. Logically additive with the URL's `?skip_verify=true` query
  parameter (if either is set, verification is disabled). Emits a Warn
  log at startup; use only in trusted networks or for development.

Paths support Caddy's `{env.VAR}` runtime placeholders. File presence
is enforced at Provision time, so `caddy validate` will fail if a
referenced certificate file is missing — keep certificates in place
when validating configurations.

#### Redis ACL recipe

If you run Redis 6+ with ACLs (the default since 6.0), the transport
needs a specific command + key surface. A minimal-privilege ACL for
`stream mercure`:

```text
ACL SETUSER mercure on >SECRET \
    ~{mercure} ~{mercure}:* \
    +info +time \
    +xadd +xrange +xrevrange +xlen +xinfo +xreadgroup +xack +xclaim \
    +xgroup +xtrim +xdel \
    +get +set +del +mget +exists +expire +pexpire +ttl \
    +scan \
    +eval +evalsha +script
```

What each block is for:

- **`~{mercure} ~{mercure}:*`** — restricts the user to the stream key
  and every presence/lastEventID key under it. Replace `mercure` if
  you set `stream <name>`.
- **`+info +time`** — `INFO server` is read at startup for the
  Redis-vs-Valkey detection that populates the `backend_type` label;
  `TIME` is read to seed the clock-drift gauge.
- **`+xadd … +xack`** — the stream surface used by Dispatch, the
  XREADGROUP listener, and history replay. Missing any breaks publish
  or delivery (not connectivity); the hub starts but does not flow
  events.
- **`+xgroup +xtrim +xdel`** — consumer-group lifecycle (zombie GC,
  rolling deploys) and retention.
- **`+get +set +del +mget +exists +expire +pexpire +ttl`** — presence
  heartbeat keys plus the `lastEventID` mirror.
- **`+scan`** — periodic presence sweep when summary mode is off.
- **`+eval +evalsha +script`** — atomic Lua scripts used by Dispatch.
  `+evalsha` lets the cached script hash hit; `+script` is needed for
  the LOAD path that fires after a Redis restart flushes the script
  cache.

Do not grant `+@all` to bootstrap. A `+ping`-only ACL passes the
PING readiness probe but fails at startup version detection, clock
drift, the XREADGROUP listener, or the GC sweep, often with errors
that do not name ACLs.

For ElastiCache RBAC, apply the equivalent command/key list as the
user-group access string.

> The bundled `redis-test.Caddyfile` (env-overridable) and `conformance.Caddyfile`
> (rigid for reproducibility) are dev scaffolds — see file headers for env
> hooks. Do NOT ship them in production; inject the `transport redis { ... }`
> block into your operator-shaped Caddyfile via `MERCURE_EXTRA_DIRECTIVES`.

## Architecture

The hot path is a **single per-node consumer group** that reads the shared
stream via XREADGROUP, then fans each entry out to local SSE subscribers
through either a sequential walk (default) or sharded workers
(`dispatch_shards > 1`). Three background goroutines maintain operational
state: presence heartbeat, health PINGs, and (optional) periodic zombie-group
GC. All delivery — including to the publishing node's own subscribers — flows
through Redis, so every node sees events in the same server-canonical order.

### Publish → fan-out

```mermaid
sequenceDiagram
    participant Pub as Publisher
    participant Hub as Mercure Hub
    participant Tx as RedisTransport
    participant R as Redis Stream
    participant L as xreadgroupListener
    participant W as Shard Workers
    participant Sub as LocalSubscriber

    Pub->>Hub: POST /.well-known/mercure
    Hub->>Tx: Dispatch(update)
    Tx->>Tx: codec.Marshal(update)
    Tx->>R: EVAL publishScript<br/>(XADD + SET lastEventID)
    R-->>Tx: ok
    Tx-->>Hub: 200 OK

    loop per node, blocking
        L->>R: XREADGROUP node-{id} ><br/>BLOCK xread_block COUNT xread_count
        R-->>L: entries
        L->>L: codec.Unmarshal(entry.data)
        L->>W: shardedDispatch(update)
        W->>W: SubscriberList.MatchAny
        W->>Sub: Dispatch (per match)
        Sub-->>Sub: SSE write
        L->>R: XAck batch
    end
```

The Lua `publishScript` runs XADD then SET atomically: on success,
`Last-Event-ID` is updated to the just-added stream entry's ID (no
phantom IDs). If XADD fails the SET is skipped; if SET fails after XADD,
`Last-Event-ID` lags behind but the event is still in the stream and the
next publish updates it. Retention via `XTRIM MINID` can subsequently
trim the referenced entry — that is expected; history replay falls back
to a full scan when the requested ID is missing.

### Sharded dispatch

`dispatch_shards = 1` (default) walks `shards[0].subscribers.MatchAny` while
holding `t.mu`. Above 1, each shard owns its own SubscriberList, channel,
and worker goroutine — `xreadgroupListener` releases `t.mu`, fans the
update out to all shard channels, waits on a per-message WaitGroup, then
re-acquires `t.mu` to advance `lastDispatchedStreamID`. Subscribers hash
to a stable shard via xxhash on their UUIDv4 ID so per-subscriber FIFO
ordering is preserved.

```mermaid
graph LR
    L[xreadgroupListener] --> SD{numShards > 1?}
    SD -->|no| S0[shards 0 SubscriberList<br/>sequential MatchAny]
    SD -->|yes| FAN[shardedDispatch<br/>fan-out + per-msg WaitGroup]
    FAN --> C0[shardChan 0]
    FAN --> C1[shardChan 1]
    FAN --> CN[shardChan N]
    C0 --> W0[shardWorker 0]
    C1 --> W1[shardWorker 1]
    CN --> WN[shardWorker N]
    W0 --> SL0[shards 0 MatchAny]
    W1 --> SL1[shards 1 MatchAny]
    WN --> SLN[shards N MatchAny]
    SL0 --> SUB[LocalSubscriber.Dispatch]
    SL1 --> SUB
    SLN --> SUB
```

### Shutdown ordering

`Close` ignores its caller `ctx` (Caddy's `caddy validate` passes a
zero-value context that panics inside go-redis) and instead drives a
self-managed `Background` ctx with `closeTimeout = 5s`. The teardown order
is fixed because `xreadgroupListener` is the only sender into the shard
channels — closing them before it exits would race a `send-on-closed`
panic against an in-flight dispatch.

```mermaid
graph TD
    A[Close called] --> B[close closed channel]
    B --> C[cancel root ctx]
    C --> D[wait listenerDone<br/>xreadgroupListener exits]
    D --> E[closeShardChannels]
    E --> F[wg.Wait<br/>shard workers + heartbeat + GC + healthCheck exit]
    F --> G[disconnectAllSubscribers<br/>per-shard Walk + Disconnect]
    G --> H[XGroupDestroy nodeGroup]
    H --> I[DEL presence:nodeID]
    I --> J[client.Close]
    J --> K[errors.Join joined errors]

    style A fill:#fdd
    style K fill:#dfd
```

### Presence & zombie GC

Each node renews `mercure:{stream}:presence:<nodeID>` with `presence_ttl` on
every `presence_interval` tick. Above `presence_detail_threshold` subscribers
the payload switches to a count-only summary so `SET` payloads stay bounded.
A presence key that expires past its TTL signals a crashed node, and
`periodicZombieGC` (and the startup-time `gcZombieGroups`) removes the
orphaned consumer group whose presence key no longer exists.

```mermaid
graph TB
    subgraph "Node A"
        HA[presenceHeartbeat<br/>every presence_interval]
        HA -->|SET presence:nodeA EX presence_ttl| KA[(presence:nodeA<br/>JSON or summary)]
    end

    subgraph "Node B"
        HB[presenceHeartbeat]
        HB --> KB[(presence:nodeB)]
        GB[periodicZombieGC<br/>every zombie_gc_interval]
        GB -->|XINFO GROUPS| XG{For each<br/>mercure:node:* group}
        XG -->|EXISTS presence:groupNodeID| EX{Key exists?}
        EX -->|no| DEL[XGROUP DESTROY<br/>orphan group]
        EX -->|yes| SKIP[skip]
    end

    subgraph "GetSubscribers — any node"
        GS[SCAN presence:* MATCH] --> MG[MGET each batch]
        MG --> DEDUP[deduplicate by ID]
        DEDUP --> RET[return cluster-wide list]
    end

    KA -.->|TTL expires<br/>if heartbeat fails| GONE((removed))
```

`GetSubscribers` scans `presence:*` and MGETs the values; the API is
**eventually consistent** with up to one `presence_interval` of staleness
and may include subscribers from a node that crashed within the last
`presence_ttl`. Build the subscriptions endpoint and debugging on it, not
correctness-critical logic.

> **Summary-mode contract caveat.** When a peer node's subscriber count
> crosses `presence_detail_threshold` (default 1 000), that node writes
> a **summary-mode** presence payload containing only the count, NOT the
> per-subscriber details. `GetSubscribers` materializes a
> `[]*mercure.Subscriber`, which has no representation for "N subscribers
> exist on node X but their details are not available," so the
> aggregated slice silently omits the summary-mode peer's subscribers.
> The aggregate count metric (`mercure_subscribers_connected` scraped
> per-instance and summed) is the source of truth for fleet-wide
> subscriber counts; `GetSubscribers` targets the debug/subscriptions
> surface, where a single peer above threshold is uncommon. If you need
> cluster-complete enumeration above threshold, raise
> `presence_detail_threshold` (at the cost of larger SET payloads — see
> `presence_payload_bytes`).

## Configuration Reference

All Caddyfile directive values support Caddy `{env.VAR}` placeholders —
strings, paths, durations, and numeric tunables alike. Unset placeholders
fail at Provision with a directive-specific error so a typo
(`max_length {env.MAX_LEN_TYPO}`) surfaces immediately rather than
silently using the default.

**Reload caveat:** `{env.VAR}` resolves at Provision (runtime), so it
survives into Caddy's adapted JSON as the literal placeholder. Changing an
env var's *value* without editing the Caddyfile text does **not** take
effect on `caddy reload` — Caddy byte-compares the adapted JSON, sees no
change, and skips re-provisioning (Caddy framework behavior). When a reload
must pick up an env change, use `{$VAR}` (substituted at *adapt* time, so
the JSON changes) or restart the process.

Boolean directives split into two groups:

- **`tls` and `tls_insecure_skip_verify`** accept `{env.VAR}` placeholders
  (and the boolean strings `true`/`false`/`on`/`off`/`yes`/`no`/`1`/`0`).
  This is the env-injection path for "TLS in prod, plaintext in dev."
- **`gob`** is a bare flag-style shortcut for `encoding gob` and has no
  value to template. Switch codecs via `encoding {env.MERCURE_CODEC}`
  instead.

The categories below match the [Architecture](#architecture) section's
subsystems.

### Connection & topology

| Directive | Default | Description |
|---|---|---|
| `url` | (one of url/addresses) | Redis/Valkey URL (`redis://`, `rediss://`). Mutually exclusive with `addresses`. |
| `addresses` | (one of url/addresses) | Multiple seed nodes for Cluster (or Sentinel when `master_name` is set). Repeated `addresses` lines accumulate hosts. |
| `address` | — | Singular alias for `addresses`; multiple `address` lines accumulate. |
| `master_name` | "" | Sentinel master name. When set, `addresses` are sentinel seeds. |
| `username` | "" | Redis ACL username. Overrides URL userinfo. |
| `password` | "" | Redis password. Overrides URL userinfo. |
| `db` | URL path (or 0) | Database index (0–15 standalone; 0 Cluster). Explicit `db 0` overrides a non-zero URL DB. |

### Pool, timeouts, retries

go-redis client tuning. All fields are pass-through to
`redis.UniversalOptions`; leaving any directive unset preserves the
go-redis library default (which is what most deployments want — only
tune in response to observed metric pressure).

| Directive | Default (go-redis library) | Description |
|---|---|---|
| `pool_size` | 10 × runtime.NumCPU | Max open connections per pool. Raise when `mercure_redis_client_pool_timeouts_total` is non-zero under load; bound when Redis `maxclients` is close to saturation. |
| `min_idle_conns` | 0 (lazy) | Floor of idle connections kept warm. Set non-zero to amortize connect cost across the first requests after hub start / transport reload. |
| `pool_timeout` | `read_timeout + 1s` | How long a Dispatch / XREADGROUP / XRANGE caller waits for a free pool entry before returning `ErrPoolTimeout`. Tighter → faster pool-exhaustion signal; looser → swallows transient spikes. |
| `dial_timeout` | 5s | Connection-establishment ceiling. Tighten in low-latency envs; loosen on slow-DNS / VPN paths. |
| `read_timeout` | 3s | Socket-read ceiling for everything EXCEPT the XREADGROUP listener (which disables read timeout for itself because it owns the BLOCK lifetime). Applies to PING, XADD, XINFO, XRANGE, etc. |
| `write_timeout` | 3s | Socket-write ceiling. Equal to `read_timeout` in go-redis defaults. |
| `max_retries` | 3 | Per-command transient-failure retries. Pass `-1` to disable retries entirely (every transient failure surfaces in metrics rather than being absorbed). |

All duration directives accept `{env.VAR}` placeholders.

### TLS

| Directive | Default | Description |
|---|---|---|
| `tls` | false | Enable TLS with default settings (TLS 1.2 minimum). |
| `tls_ca_file` | "" | PEM file with trusted CA certificate(s); replaces system pool. |
| `tls_client_auth` | "" "" | `CERT KEY` pair for mutual TLS (atomic two-arg directive). |
| `tls_server_name` | "" | Override SNI / cert-verification hostname. |
| `tls_insecure_skip_verify` | false | Disable cert verification (additive with URL `?skip_verify`; emits Warn). |

### Stream & codec

| Directive | Go Option | Default | Description |
|---|---|---|---|
| `stream` | `WithStreamName` | `"mercure"` | Redis Stream key name. |
| `encoding` | `WithEncoding` | `"json"` | Codec: `json`, `gob`, `msgpack`. See [Codec Choices](#codec-choices). |
| `gob` | — | — | Flag-style shortcut for `encoding gob`. |

### Durability & cleanup

| Directive | Go Option | Default | Description |
|---|---|---|---|
| `max_length` | `WithMaxLength` | `0` (unlimited) | Approximate MAXLEN for stream trimming. Negative values are rejected at Caddy parse time. |
| `event_ttl` | `WithEventTTL` | `1h` (safe default) | TTL-based stream cleanup window. Set to `0` to opt out (pairs with `max_length 0` for fully unbounded). Negative values are rejected at Caddy parse time. |
| `cleanup_interval` | `WithCleanupInterval` | `5m` | How often TTL cleanup (XTRIM MINID) runs. |

> **Both zero is unsafe in production.** When both `max_length` and `event_ttl`
> are 0, the stream is never trimmed. The transport logs one Warn at startup
> and then nothing else — there is no runtime back-pressure or auto-bound.
>
> The cascade if you ignore the warning:
>
> 1. Redis memory grows linearly (≈ message size + 80 B/entry overhead).
> 2. Redis hits `maxmemory`. Behavior depends on `maxmemory-policy`:
>   - `noeviction` — publishes start returning OOM errors;
>      `mercure_redis_publish_errors_total` climbs.
>   - `allkeys-lru` / `allkeys-random` — random keys (including stream
>      entries and the `lastEventID` key) get evicted; subscribers
>      requesting history see gaps.
> 3. `XRANGE` history replay slows from milliseconds to seconds; reconnecting
>    subscribers serialize behind `history_replay_concurrency` (default 20).
> 4. In Cluster mode all stream keys are hash-tagged to one slot, so one
>    node carries the entire load until OOM hits there first.
>
> **Always set at least one bound.** Common shapes:
>
> - `max_length 100000` — cheap, applied on every publish via approximate
>   `XADD MAXLEN ~`.
> - `event_ttl 24h` + `cleanup_interval 5m` — calendar-bounded retention via
>   periodic `XTRIM MINID`.
> - Combine both for layered defense.

### Presence

| Directive | Go Option | Default | Description |
|---|---|---|---|
| `presence_ttl` | `WithPresenceTTL` | `60s` | TTL for presence keys. |
| `presence_interval` | `WithPresenceInterval` | `30s` | Heartbeat interval for presence renewal. |
| `presence_detail_threshold` | `WithPresenceDetailThreshold` | `1000` | Subscriber count above which presence switches to summary mode. |
| `presence_detail_byte_threshold` | `WithPresenceDetailByteThreshold` | `524288` (512 KiB) | Marshaled byte size above which detail-mode presence falls back to summary mode (catches large-claim subscribers blowing the payload below the count threshold). `0` disables the byte budget. |

### Dispatch

| Directive | Go Option | Default | Description |
|---|---|---|---|
| `dispatch_shards` | `WithDispatchShards` | `1` | Dispatch worker goroutines (`0` = NumCPU). Hard-capped at 256 (auto-detect on high-vCPU hosts and explicit large values both clamp with a Warn log). |
| `xread_count` | `WithXReadCount` | `100` | Messages per XREADGROUP call. |
| `xread_block` | `WithXReadBlock` | `1s` | XREADGROUP BLOCK timeout. |
| `clock_skew_margin` | `WithClockSkewMargin` | `5s` | UUIDv7 timestamp seek margin for history replay. |

### Rate limiting

| Directive | Go Option | Default | Description |
|---|---|---|---|
| `subscriber_rate_limit` | `WithSubscriberRateLimit` | `0` (disabled) | Max new subscribers/second; fractional values allowed (e.g. `0.5`). When set, new HTTP subscribers over this rate are shed at the front door with `429` rather than blocking — see [Admission control](#admission-control-load-shedding). |
| `subscriptions_max_subscribers` | `WithSubscriptionsMaxSubscribers` | `100000` | Max unique subscribers `GetSubscribers` (the `/subscriptions` API) materializes before aborting with 503, so a huge fleet can't OOM the hub by listing the whole cluster. Peak heap is roughly this many subscribers plus one MGET batch of presence values. `0` disables the cap. |
| `subscriber_rate_burst` | `WithSubscriberRateBurst` | `5000` | Burst allowance for subscriber rate limiter. |
| `publisher_rate_limit` | `WithPublisherRateLimit` | `0` (disabled) | Max publish ops/second; fractional values allowed. |
| `publisher_rate_burst` | `WithPublisherRateBurst` | `5000` | Burst allowance for publisher rate limiter. |
| `history_replay_concurrency` | `WithHistoryReplayConcurrency` | `20` | Max concurrent history replays (XRANGE). |

### Admission control (load shedding)

When `subscriber_rate_limit` or `subscriber_max_count` is set, the hub gates new
SSE subscribers at the front door — before JWT verification or any allocation —
and sheds those over the ceiling with `429 Too Many Requests` and a `Retry-After`
header. This bounds a hub's exposure to a thundering-herd reconnect storm (a
fleet reconnecting after a deploy, a network blip, or a power-restore event)
instead of letting every arrival contend for memory and CPU.

The gate runs two checks, in order:

1. **Concurrent count** (`subscriber_max_count`) — a fast reject when this hub
   already holds the maximum admitted subscribers. Recorded as `reason=capacity`.
2. **Accept rate** (`subscriber_rate_limit` + `subscriber_rate_burst`) — a token
   from the subscriber rate limiter. With `subscriber_admission_timeout = 0` (the
   default) an over-rate request is shed immediately; with a positive timeout it
   waits up to that long for a token before shedding. Recorded as `reason=rate`.

Rejections increment `mercure_redis_subscriber_admission_rejected_total{reason}`.

| Directive | Go Option | Default | Description |
|---|---|---|---|
| `subscriber_max_count` | `WithSubscriberMaxCount` | `0` (disabled) | Max concurrent admitted subscribers on this hub. Connections over the ceiling are shed with `429`. The count is maintained in-process (independent of the cluster-wide presence count), so size it to one instance's connection budget, not the fleet's. |
| `subscriber_admission_timeout` | `WithSubscriberAdmissionTimeout` | `0` (fail-fast) | How long an over-rate request may wait for a rate token before being shed. `0` sheds immediately. No effect unless `subscriber_rate_limit` is set. |
| `subscriber_registration_timeout` | `WithSubscriberRegistrationTimeout` | `0` (unbounded) | Bounds the history-replay/registration work an admitted subscriber performs before its stream is established. `0` leaves it unbounded (prior behavior). |
| `subscriber_retry_after` | `WithSubscriberRetryAfter` | `2s` | Base for the `Retry-After` header on a `429`. The emitted value is jittered (base plus up to 50%) so a shed herd reconnects spread out rather than in lockstep. Rounded up to whole seconds per RFC 9110. |

> **Behavior change — `subscriber_rate_limit` now sheds instead of blocking.**
> Previously an HTTP SSE subscriber that exceeded `subscriber_rate_limit` had its
> connection held open and queued until a token freed. With admission control it
> is shed with `429` + `Retry-After` (after an optional bounded
> `subscriber_admission_timeout` wait). Programmatic `AddSubscriber` callers are
> unaffected — they keep the prior blocking behavior. If you relied on rate-limit
> back-pressure holding connections open, set `subscriber_admission_timeout` to
> restore a bounded wait before the shed.
>
> **429 only helps if clients cooperate.** Shedding shifts a herd in time only
> when clients honor the `Retry-After` header and reconnect with jittered
> backoff. A client that ignores `Retry-After` and retries immediately merely
> moves the same load to the next instant. Before relying on admission control to
> absorb a cold-start surge, confirm your SSE client honors `Retry-After` on
> `429` and applies jitter on both its initial connect and its reconnects.

### Health

| Directive | Go Option | Default | Description |
|---|---|---|---|
| `health_interval` | `WithHealthInterval` | `10s` | Health check PING interval. |
| `health_threshold` | `WithHealthThreshold` | `3` | Consecutive PING failures before flipping to unhealthy. |

### Zombie GC

| Directive | Go Option | Default | Description |
|---|---|---|---|
| `zombie_gc_interval` | `WithZombieGCInterval` | `5m` | Periodic zombie consumer group GC (`0` = disable; startup GC always runs). |

### Validation summary

Configuration is checked in three layers. The godoc on `Option` is the
authoritative reference; this table mirrors it for operator discovery.

**Strict — refuses startup with `ErrInvalidOptions` (joined via `errors.Join` so all violations surface at once):**

| Condition | Reason |
|---|---|
| `event_ttl < 0` | Negative TTL would compute a future MINID and erase the stream every cleanup tick. |
| `max_length < 0` | Redis rejects negative `MAXLEN` at the wire — every publish would fail. |
| `presence_interval >= presence_ttl` | A single scheduling jitter would self-evict the node's presence key. |
| `encoding` not in `{json, gob, msgpack}` | Wraps both `ErrInvalidOptions` and `mercure.ErrUnsupportedCodec` so callers can match either sentinel. |
| Any other negative numeric/duration directive (`dispatch_shards`, `health_threshold`, `xread_count`, the rate limits/bursts, `history_replay_concurrency`, `presence_detail_threshold`, `presence_detail_byte_threshold`, `db`, the pool/timeout knobs, the admission knobs `subscriber_max_count` / `subscriber_admission_timeout` / `subscriber_registration_timeout` / `subscriber_retry_after`) | Rejected with `errNegativeOptionValue` at provisioning (after `{env.VAR}` resolution). A negative is an operator typo; fail fast rather than silently fall back to the default (or, for `dispatch_shards`, to NumCPU). |

**Soft — logs a startup Warn but does not fail:**

| Condition | Threshold | Concern |
|---|---|---|
| `xread_block` very short | `< 10ms` | XREADGROUP busy-polls, wasting Redis RTTs. |
| `xread_count` very high | `> 10 000` | Listener buffers up to N entries per BLOCK call (memory pressure during fan-out). |
| `subscriber_list_cache_size` very large | `> 10 000 000` | Multi-GB skipfilter allocation. |
| `clock_skew_margin` very large | `> 1h` | History replay scans a wide stream window per reconnect. |
| `health_threshold == 1` | exact match | Single failed PING flips Ready to unhealthy with no flap suppression — fine for tight latency budgets, flappy on noisy networks. |
| `history_replay_concurrency >= pool_size` | client-aware | Replay storms can exhaust the go-redis pool, starving XREADGROUP/XADD/presence ops. Predicate is `>=` so equality (zero headroom) also warns. Reported only when pool size is introspectable (`*redis.Client`, `*redis.ClusterClient`, `*redis.Ring`). |
| `max_length == 0 && event_ttl == 0` | exact match | Stream is never trimmed; grows unbounded until Redis hits `maxmemory`. See the [Durability & cleanup](#durability--cleanup) callout for the OOM cascade. |
| `subscriber_admission_timeout` set without `subscriber_rate_limit` | exact match | The admission wait has nothing to wait on — there is no rate limiter — so the timeout is a no-op. |
| `subscriber_retry_after` very large | `> 5m` | Clients are told to wait minutes before retrying; usually a unit mistake. |

**Hard clamps — silent correction with Warn audit trail:**

| Condition | Action |
|---|---|
| `dispatch_shards > 256` | Clamped to 256 with a Warn log. Auto-detect via `runtime.NumCPU()` passes through the same cap — bounds per-shard Prometheus series cardinality. |
| `subscriber_list_cache_size / dispatch_shards < 10 000` per shard | Per-shard size clamped up to a 10 000 floor; effective total exceeds the configured cap (overcommit). |
| Typed-nil `*prometheus.Registry` from Caddy `validate` | Normalized to a true-nil interface so `initMetrics` short-circuits cleanly. |

The Caddyfile adapter rejects the `gob false` / `gob {env.X}` shortcut
variants, the `tls { ... }` sub-block syntax (flat directives only), and
the mutually-exclusive `url` + `addresses` pair at parse time. Negative
numeric and duration values are rejected during provisioning
(`errNegativeOptionValue`), after any `{env.VAR}` placeholders resolve.
Either way the misconfiguration surfaces at config load rather than
silently degrading to a default.

## Sharing Redis with Caddy storage

If you also use [`pberkel/caddy-storage-redis`](https://github.com/pberkel/caddy-storage-redis)
to back Caddy's distributed TLS state, point both at the same server using
shared environment variables. One source of truth for credentials, two
independent connection pools:

```caddyfile
{
    storage redis {
        host {env.REDIS_HOST}
        port 6379
        password {env.REDIS_PASSWORD}
        # tls, db, etc. as needed
    }
}

route /.well-known/mercure {
    mercure {
        transport redis {
            url redis://default:{env.REDIS_PASSWORD}@{env.REDIS_HOST}:6379
            stream mercure
        }
    }
}
```

Each module opens its own pool — intentional for lifecycle isolation
(reload re-provisions independently), failure isolation (ACME backoff
can't exhaust the transport pool), and IO-pattern fit (transport runs
continuous XADD/XREADGROUP; storage does sparse cert loads).

## Codec Choices

`encoding` selects how `mercure.Update` values are serialized onto the
Redis Stream. The transport aliases upstream Mercure's `Codec` interface
and inherits its three implementations.

| Codec | Pick when |
|---|---|
| **`json`** (default) | Most deployments. Human-readable on the wire (`XRANGE` + `redis-cli`). |
| **`msgpack`** | Sustained high throughput or polyglot subscribers. Fastest unmarshal; schema-less. |
| **`gob`** | Almost never. Go-only, slower, and *larger* than JSON in Mercure's usage. Kept for upstream parity. |

### Benchmarks

Apple M4 Pro, a representative Mercure Update (2 topics, UUIDv7 ID,
57-byte JSON data). Regenerate with `go test -bench=Codec -benchmem ./redistransport/...`.

Typical payload (≈250 B wire):

| Codec    | Marshal ns/op | Unmarshal ns/op | Wire bytes | Unmarshal allocs |
|----------|--------------:|----------------:|-----------:|-----------------:|
| json     |           330 |            1551 |        254 |               16 |
| msgpack  |           387 |             407 |        217 |                9 |
| gob      |          1632 |            7948 |        300 |              191 |

Large payload (8 KiB data):

| Codec    | Marshal ns/op | Wire bytes |
|----------|--------------:|-----------:|
| msgpack  |          1361 |       8294 |
| gob      |          3358 |       8392 |
| json     |          5327 |       8321 |

JSON is default because operability beats microseconds at typical
Mercure rates — `XRANGE | grep` during an incident matters. Switch to
MessagePack at sustained ≥10k msg/s per node, with polyglot subscribers,
or when JSON's allocation rate shows up in profiles. Gob is larger than
JSON here (per-message type preamble); the compact form needs a
long-lived encoder which doesn't fit `Marshal`-per-publish.

### Codec migration

The `encoding` option controls what the dispatcher *writes*. Entries
already in the stream keep their original encoding; subscribers
decode with whichever codec is configured at connect time. Mismatched
codec = `mercure_redis_stream_decode_errors_total{kind="unmarshal"}`
incrementing and dropped entries. **All hubs in a fleet must run the
same encoding.**

Cutover procedure:

1. Stop publishers; let consumers ACK to the end.
2. `XTRIM mercure MAXLEN 0` (or wait `event_ttl` to expire entries).
3. Change `encoding` fleet-wide; restart.
4. Resume publishers.

Mercure does not version-tag entries, so rolling migration without a
stream clear isn't safe — schedule a maintenance window.

## Go API

```go
import (
    "time"

    "github.com/lzrf0cuz/mercure/redistransport"
    "github.com/redis/go-redis/v9"
)

client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

transport, err := redistransport.NewRedisTransport(client,
    redistransport.WithStreamName("mercure"),
    redistransport.WithMaxLength(10000),
    redistransport.WithEncoding("json"),
    redistransport.WithEventTTL(24 * time.Hour),
)
```

## Glossary

Terms that recur in the docs and operational guidance:

- **PEL** — *Pending Entries List*. Per-consumer-group queue inside a Redis
  Stream of entries that have been delivered but not yet `XACK`'d. Mercure
  uses one consumer per hub instance; PEL size is a backpressure signal.
- **`XADD` / `XREADGROUP` / `XACK` / `XRANGE` / `XTRIM`** — Redis Stream
  commands. `XADD` publishes; `XREADGROUP` consumes within a consumer
  group (the listener's read path); `XACK` confirms processing; `XRANGE`
  is the history-replay path; `XTRIM MINID` is how the GC bounds stream
  size by event-TTL boundary.
- **CMD vs CME** — *Cluster Mode Disabled* (single primary + replicas;
  AWS ElastiCache "cluster mode disabled") vs *Cluster Mode Enabled*
  (sharded across N primaries). The transport supports both, but a single
  `stream` only lives on one shard — see [Choosing a topology](#choosing-a-topology).
- **Hash-tagged keys** — Redis Cluster slot-pinning syntax: `{key}` braces
  force a key to a deterministic slot. The transport uses `{streamName}`
  on auxiliary keys (presence, lastEventID) so `MGET` and pipelined ops
  stay slot-local on a Cluster.
- **Skipfilter** — the per-shard subscriber-list cache that skips topic
  matching when a subscriber's selector cannot possibly match the
  inbound topic, cutting per-dispatch CPU.
- **UUIDv7** — time-ordered UUID variant ([RFC 9562](https://datatracker.ietf.org/doc/rfc9562/)).
  Mercure event IDs are UUIDv7; their leading 48 bits encode the publish
  millisecond timestamp, which the transport's history-replay uses for
  binary-search seek into the Redis stream's `<ms>-<seq>` IDs.

## Task Reference

`task --list` is the source of truth — what's below is a tour. From the
repo root the same tasks are namespaced `rt:*` via the root Taskfile.

```sh
# Tests
task test                       # miniredis unit + integration (race + coverage)
task test:all                   # + caddy submodule
task test:short                 # no race detector
task test:race:repeat           # -race -count=5 schedule-stress (this module)
task test:race:repeat:caddy     # caddy submodule
task test:race:repeat:all       # both modules
task bench / task bench:profile # benchmarks; profile → ./profiles
task cover                      # HTML coverage report

# Real Redis/Valkey/Cluster (build tag real_redis; auto spin-up + tear-down)
task test:real:{redis,valkey}             # TestReal_*
task test:real:stress:{redis,valkey}      # concurrent-churn stress
task test:real:multihub:{redis,valkey}    # cross-node invariants
task test:real:cluster                    # TestReal_Cluster_* against 3-master cluster
task test:real:matrix                     # redis + valkey + cluster, sequentially

# Quality
task fmt | fmt:check            # gofumpt; check is non-mutating
task lint | lint:caddy | lint:all
task vet  | vet:caddy  | vet:all
task vuln | vuln:caddy | vuln:all
task md:lint | md:fmt           # rumdl

# Build
task build | build:caddy | build:all
task build:binary               # Mercure binary with this transport registered

# Module maintenance
task mod:tidy / task mod:verify

# Cleanup
task clean                      # coverage, pprof, bench binaries, built mercure
task clean:all                  # + go clean -testcache -cache

# CI (non-mutating)
task ci                         # fmt:check + lint:all + vet:all + test:all + build:all

# Compose
task redis:{up,down,logs}
task valkey:{up,down,logs,open}
task cluster:{up,down,logs}                # 3-master Redis Cluster

# Observability profiles (add to a running stack)
task metrics:up:{redis,valkey}             # Prometheus + Grafana + redis_exporter
task metrics:down:{redis,valkey}
task tracing:up:{redis,valkey}             # Jaeger sidecar (co-networked with Grafana)
task tracing:down:{redis,valkey}

# Conformance (Playwright against a real hub)
# Runs the hub NATIVELY on :80/:443/:2019 — stop any other local stack holding
# those ports first, or the run aborts with a port-in-use message.
# Also runs inside `task verify:full` (tier 3; release-candidate / nightly).
task conformance                # full pipeline; defer cleans up Redis + hub
task conformance:{install,run}  # install deps / run Playwright against a running hub
task conformance:selftest       # unit-test the start/stop/wait helper itself
```

The multi-hub suite covers horizontal-scale invariants a single transport
can't exercise: cross-node dispatch, cluster-wide `GetSubscribers`,
presence heartbeat visibility, zombie GC across hubs, close isolation,
concurrent publish, startup races, racing `XGROUP DESTROY`, and history
replay crossing the page boundary. Each test uses two distinct
`redis.Client` pools so connection-level bugs surface. Multi-host
invariants (UUIDv7 clock skew, half-open TCP) need Toxiproxy and live
in the Playwright pipeline, not the Go suite.

Tooling tasks declare `preconditions:` checks, so missing tools print an
install hint instead of a raw shell error. `task ci` never mutates the
tree — `fmt:check` exits non-zero on diff so it's PR-block-safe.

## Compatibility

| Server | Min Version | Tested With |
|---|---|---|
| [Redis](https://redis.io) | 6.2 | 7.2, 7.4 |
| [Valkey](https://valkey.io) | 7.2 | 8.0, 8.1 |

The transport uses `INFO server` at startup to detect the server type. Valkey emits
`valkey_version:` in its INFO output (alongside `redis_version:` for wire compatibility).
The transport prioritizes `valkey_version:` when present, correctly identifying the server.

## Capacity planning

No universal numbers — characterize against your deployment using the
metrics above. Two independent ceilings to provision separately:

1. **Steady-state subscriber count per hub** — track via
   `sum by (instance) (mercure_redis_shard_subscribers)`.
2. **Subscriber accept rate** — `WithSubscriberRateLimit(0)` (disabled)
   by default; enable and tune to measured steady capacity before
   setting client ramp rates.

### Go runtime environment

Set `GOMEMLIMIT` explicitly. Without it, Go's scavenger holds onto heap
spans via `MADV_FREE` indefinitely, and the "heap in use" Prometheus
metric lags the actual working set by minutes to hours. Set it to the
value at which you want GC to start working hard against memory pressure
— typically 80-90% of the container or host's hard memory limit, leaving
headroom for runtime + sidecars. Pair with `GOGC` tuning if profiling
shows allocation-rate pressure.

### Signals to watch during ramp-up / ramp-down

| Pattern | Primary metric | Secondary |
|---|---|---|
| Accept-rate saturation (TLS handshake) | `mercure_redis_add_subscriber_duration_seconds` p99 | `mercure_subscribers_connected` growth vs client injection rate |
| `t.mu` contention under churn | `mercure_redis_add_subscriber_duration_seconds` p99 + `mercure_redis_remove_subscriber_duration_seconds` p99 | `mercure_redis_dispatch_duration_seconds` p99 |
| Heap retention after drain | `go_memstats_heap_inuse_bytes - go_memstats_heap_released_bytes` | `go_memstats_next_gc_bytes` vs `GOMEMLIMIT` |
| Zombie GC churn | `mercure_redis_zombie_gc_errors_total` | `mercure_redis_consumer_groups` (expected = replica count; sustained excess = zombies; 0 = nobody reading) |
| Misconfigured stream (publisher → nobody reads) | `mercure_redis_consumer_groups == 0` | Non-zero `rate(mercure_redis_publish_duration_seconds_count[5m])` |
| Retention tightening before clients miss | `mercure_redis_history_window_seconds` falling below expected reconnect window | `rate(mercure_redis_history_replay_fallback_total[15m])` (reactive companion) |
| Fan-out without consumers | `rate(mercure_redis_dispatch_zero_match_total[15m])` / `rate(mercure_redis_publish_duration_seconds_count[15m])` ratio | Fleet replica count multiplies the wasted I/O |
| Presence payload growth | `mercure_redis_presence_payload_bytes` and `rate(mercure_redis_presence_fallback_total[15m])` | Crossing `presence_detail_threshold` (default 1 000) or `presence_detail_byte_threshold` (default 512 KiB) flips to summary mode |
| Redis I/O saturation | `mercure_redis_client_pool_timeouts_total` | `_misses_total` rate vs `_hits_total` rate |
| Hub-side write back-pressure | `mercure_subscribe_write_flush_seconds{outcome="ok"}` p99 | `mercure_redis_xreadgroup_latency_seconds` p99 (transport-side) |

### Known ramp-down behavior

Mass disconnects (reverse-proxy failover, load-test ramp-down, client-fleet
cold-restart) serialize on the transport's `t.mu` for the `RemoveSubscriber`
path. A short CPU spike with elevated `remove_subscriber_duration_seconds`
p99 is expected until the drain completes — it is not a regression.

## License

This module is licensed under the [GNU Affero General Public License,
version 3 or any later version](LICENSE) (SPDX:
[`AGPL-3.0-or-later`](https://spdx.org/licenses/AGPL-3.0-or-later.html)).
The license matches the upstream Mercure hub.

Running this transport as part of a publicly-exposed Mercure deployment
triggers AGPL §13 (Remote Network Interaction): if you modify the
transport (or any AGPL-covered component) and operate it as a network
service, you must offer the modified source to your users — typically
via a public link served from the deployment itself.

See [`NOTICE.md`](NOTICE.md) for upstream attribution,
[`SECURITY.md`](SECURITY.md) for vulnerability reporting, and
[`CONTRIBUTING.md`](CONTRIBUTING.md) for the contribution flow.
