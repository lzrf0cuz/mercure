# Redis transport observability and metrics

How to inspect the [Redis transport](../README.md)'s metrics and traces in the dev stack, and what each `mercure_redis_*` series reports.

## Observability

The Valkey and Redis dev stacks both support an opt-in Prometheus and Grafana add-on for inspecting
the redis transport's `mercure_redis_*` metrics during development. It is
gated behind the Compose `metrics` profile so the lightweight default
`up -d` doesn't pull the prom/grafana images for ad-hoc iteration.

All commands below run from the repo root.

| Service | URL | Notes |
|---|---|---|
| Prometheus | [http://localhost:9090](http://localhost:9090) | Scrapes `host.docker.internal:9091` (the natively-running hub) and `redis_exporter:9121`. |
| Grafana | [http://localhost:3000](http://localhost:3000) | Anonymous Viewer enabled; admin password `admin`. Prometheus pre-provisioned as default datasource. |
| Redis Exporter | [http://localhost:9121/metrics](http://localhost:9121/metrics) | [`oliver006/redis_exporter`](https://github.com/oliver006/redis_exporter): server-side metrics (memory, evictions, slow log, command throughput). |

To bring up the stack with metrics:

```sh
task rt:metrics:up:redis    # against Redis stack
# or:
task rt:metrics:up:valkey   # against Valkey stack
```

For traces (Jaeger), add the matching tracing-profile task. Jaeger comes
up in the same compose project as Grafana, so the Grafana trace dashboard's
datasource resolves `http://jaeger:16686` over the shared project network:

```sh
task rt:tracing:up:redis    # against Redis stack (or `task dev:full:tracing`)
# or:
task rt:tracing:up:valkey   # against Valkey stack
```

The hub itself runs natively (`task dev:redis` or `task go:run:redis`);
Prometheus scrapes it across the host bridge via
`host.docker.internal:9091`. The hub must already be running for
Prometheus to have anything to scrape.

> **Mutual exclusion with the root compose stack.** Both stacks use the
> container names `prometheus` and `grafana`. Stop the root stack (`task up`,
> `task up:build`) before enabling the `metrics` profile here, and the reverse.

### Server-side metrics

The transport's `mercure_redis_*` metrics report what the hub sees; the
Redis Exporter sidecar reports what the Redis/Valkey server sees. The
two together separate "hub is slow" from "Redis is slow" during load
testing and incidents. The "Redis server" row on the transport dashboard
covers the load-testing-relevant signals:

| Signal | Source metric | Reads as |
|---|---|---|
| Memory pressure | `redis_memory_used_bytes` / `redis_memory_max_bytes` | When `used / max` approaches 1, the next write evicts (LRU or LFU) or returns OOM (`noeviction`), depending on `maxmemory-policy`. |
| Evictions | `rate(redis_evicted_keys_total[5m])` | Non-zero only when `maxmemory-policy` is an eviction policy. Dev `compose.redis.yaml` sets `allkeys-lru` (this counter rises, and presence and `:lastEventID` keys are at risk). Dev `compose.valkey.yaml` sets `noeviction` (the counter stays at 0; pressure surfaces instead as failed XADD or SET). |
| Rejected connections | `rate(redis_rejected_connections_total[5m])` | `maxclients` saturated. Not an OOM signal: under `noeviction` at `maxmemory` writes fail with `OOM` errors, counted in `mercure_redis_oom_rejections_total`. |
| Throughput | `rate(redis_commands_processed_total[5m])` | Aggregate command rate. Compare against hub publish/dispatch counters to spot non-hub traffic. |
| Network | `rate(redis_net_input_bytes_total[5m])`, `rate(redis_net_output_bytes_total[5m])` | Output » input is normal during fan-out; sustained input bursts track publish storms. |
| CPU | `rate(redis_cpu_user_seconds_total[5m])` + sys | `1.0` = one full core. Sustained `> 0.7` means Redis itself is the bottleneck. |
| Per-command latency | `rate(redis_commands_duration_seconds_total[5m]) / rate(redis_commands_total[5m])` per `cmd` | Watch `evalsha` for Lua publish slowdowns and `xrange`/`xreadgroup` for history-replay or listener stalls. |
| Slow log | `redis_slowlog_length` | Commands above `slowlog-log-slower-than`. Pair with hub-side dispatch p99 to triage. |
| Memory fragmentation | `redis_mem_fragmentation_ratio`, `redis_memory_used_rss_bytes - redis_memory_used_bytes` | Ratio ~1.0–1.5 is normal; sustained `> 1.5` under churn = allocator fragmentation. The RSS overshoot sets the headroom needed beyond `maxmemory` to avoid OS-level OOM. |

The dashboard panels use Grafana's `$__rate_interval` in place of `[5m]`.

Replication / persistence series (`redis_connected_slave_*`, `redis_aof_*`,
`redis_rdb_*`) are exposed at `http://localhost:9121/metrics` but not
dashboarded — dev stacks run `--save "" --appendonly no` without replicas.

Reset state between load tests with `task rt:redis:flush` (FLUSHALL, keeps
Prometheus history) or `task rt:redis:down`. The second is a clean slate: it
stops the stack and its metrics and tracing sidecars and drops the stack's
volumes, except the Prometheus and Grafana data volumes. Valkey equivalents:
`task rt:valkey:flush`, `task rt:valkey:down`.

### Tracing

The transport emits one OpenTelemetry span, `mercure.transport.history`
(`SpanKind=Internal`), from history replay. It also tags the active parent
span on every `Dispatch` and `AddSubscriber`.

Spans nest under whatever parent the caller has active. When
[Caddy's `tracing` directive](https://caddyserver.com/docs/caddyfile/directives/tracing)
is enabled, that is typically the hub's `mercure.publish` or
`mercure.subscribe` span. Otherwise the transport uses the OpenTelemetry
no-op tracer, which has no exporter cost and touches no globals.

`mercure.transport.history` carries these attributes:

| Attribute | Type | Meaning |
|-----------|------|---------|
| `mercure.transport` | string | Always `redis`, the per-backend discriminator (the BoltDB transport sets `bolt`). |
| `mercure.subscriber.id` | string | The subscriber whose reconnect triggered the replay. |
| `mercure.last_event_id.requested` | string | The client's requested `Last-Event-ID`, length-capped (80 chars) so a single oversized header can't bloat trace storage. |
| `mercure.history.events_replayed` | int | Count of matched updates **delivered** to the subscriber. A dispatch the subscriber refused (gone / buffer full) is not counted. |
| `mercure.history.future_dated` | bool | The future-ID guard tripped: the requested ID post-dates the hub's clock, so both replay passes were skipped. |
| `mercure.history.full_scan` | bool | Pass 1 (a seek from a UUIDv7's timestamp, or a scan from the stream start for any other ID) did not find the requested ID, so a Pass 2 full-stream rescan ran (it may itself be cut short; see `truncated`). |
| `mercure.history.truncated` | bool | The backlog was **not** fully replayed: a matched update went undelivered because the subscriber disconnected (for any reason, such as client close or shutdown) or its buffer was full. |

The four `mercure.history.*` attributes report the replay outcome, a detail
the BoltDB transport's single `db.View` read cannot give. `full_scan`
is set after a Pass 2 that ran without an XRANGE error, agreeing with
the `mercure_redis_history_replay_fallback_total` metric.

**`truncated` is a completeness signal, not an error**: the span stays `Status=OK`
because a gone subscriber is not a transport failure. Alert on the
attribute itself, not on span status, and expect it during normal client
churn and rolling restarts. For a true backpressure signal use the
`mercure_redis_subscribers_lost_total{reason="backpressure"}` metric.

`Dispatch` and `AddSubscriber` also annotate the **active parent
span** (`mercure.publish` or `mercure.subscribe`) with
`mercure.transport="redis"` on entry, so those traces can be filtered by
backend without an extra child span. The full list of hub-emitted parent
spans, including `mercure.publish` and `mercure.subscribe`, lives in
[the hub's tracing docs](../../docs/production/tracing.md).

The span name, the `mercure.subscriber.id` and
`mercure.last_event_id.requested` attributes, and the failure-status
semantics match the BoltDB transport. The history span is set to
`codes.Error` on any failure path, such as an XRANGE error or a context
cancelled while waiting for the replay semaphore. This matches the BoltDB
transport's `dispatchHistory`. Cross-backend dashboards that filter on
`mercure.transport` (`redis` or `bolt`) or on `span.Status` therefore see
Redis history failures the same way they see Bolt ones.

The XREADGROUP listener and presence-GC heartbeat goroutines are not
traced; their latency is in the `mercure_redis_xreadgroup_*` and other
`mercure_redis_*` histograms.

For per-Redis-command tracing, for example timing individual `EVALSHA` or
`XRANGE` calls, wrap the go-redis client with
[`redisotel`](https://pkg.go.dev/github.com/redis/go-redis/extra/redisotel/v9)
hooks before handing it to the transport. The transport accepts a
pre-built `redis.UniversalClient`, so the embedding module owns the
instrumentation.

## Metrics

Every metric carries three const labels:

- `transport_type="redis"` — the transport **family** (always `"redis"`
  for this implementation). Lets dashboards namespace cleanly when
  multiple Mercure transports coexist in the same registry.
- `backend_type` — the detected **backend server**: `"redis"`, `"valkey"`,
  or `"unknown"`. Sourced from `INFO server` at startup. The
  `valkey_version:` field wins when both are present, because Valkey also
  emits `redis_version:` for wire-protocol compatibility. Lets dashboards
  split deployment fleets by backend identity while still grouping all
  redis-streams transports under `transport_type`.
- `stream` — the transport's stream name (the `stream` directive or
  `WithStreamName`, `"mercure"` by default). It is part of collector
  identity; see the series-sharing rules below.

Names are stable and follow the `mercure_redis_<subsystem>_<unit>`
convention. The full list is grouped below by subsystem with the
actionable signal for each.

**Dashboard bookmarks:** the bundled Grafana dashboard's `backend_type`
and `stream` template variables default to `$__all`, so a bookmark
captured while one backend or stream existed shows every backend's or
stream's series once a second one joins. Re-capture bookmarks after
backend or stream topology changes.

**Caddy hot-reload caveat:** Caddy builds a new metrics registry for
each config load. A transport that survives a reload (its `redis` block
did not change) registers the same collector objects on the new registry
as well. Its series carry on there without a counter reset. The old
registry keeps reporting them while the old config still serves.

Collector identity is `{transport_type, backend_type, stream}`, so
transports on different streams never share a series. Transports on the
same stream with the same backend share every series on a registry they
are both bound to:

- `mercure_redis_ready` reads `0` when any live member is not ready.
- The `mercure_redis_client_pool_*` metrics read the client of the
  member that joined most recently and has finished construction.
- Gauges the transport sets, such as `mercure_redis_healthy` or
  `mercure_redis_stream_length`, are last writer wins.
- Counters, histograms and the gauges the transport increments and
  decrements (`mercure_redis_shard_subscribers`,
  `mercure_redis_history_replay_concurrent`) merge the transports'
  contributions.

The ready and pool collectors are read at scrape time from their live
members. On every bind, a transport joins the ready and pool collectors that
the registry already holds for its identity, whether its own or a
same-stream sibling's. It leaves all of them when it closes or its
construction fails.

The other collectors are owned by the first transport to register them on
a registry. A transport that binds the registry later is folded into them
if this is its first bind, and left out of them if it is a later bind.

After a reload in such a config, if a new same-stream sibling registered first, the
reused transport's contributions to those series are absent from the new
registry. This is an undercount, never a negative value. Series served by the
sibling's collector continue from the sibling's values, so they can look
like a counter reset.

Collector identity includes ConstLabel **values**, not just names. When
one registry outlives a backend swap (direct API: a Valkey-backed
transport built against the same `WithPrometheusRegisterer` registry as
an earlier Redis-backed one), the new transport registers a **second,
separate set** of collectors stamped `backend_type="valkey"`.

The `backend_type="redis"` set stays on the registry. Its ready and pool
metrics emit no samples once its last member has closed. Its other series
stay frozen at their last observed values until the process restarts.
Restart the process after a backend swap for clean attribution.

Do not register other `mercure_redis_*` collectors on the registry the
transport binds to: this is unsupported. A clash on the first bind panics,
as any registration failure there does. A clash on a later bind is returned
as an error or leaves that series to the other collector.

**Transparent-backend-swap caveat:** the same staleness exists when the
data store behind a stable endpoint is swapped, which is rarer. For example,
a transparent proxy such as Envoy or HAProxy might route `redis.internal:6379`
at a Valkey replica during a live migration.

The transport detects backend identity at `NewRedisTransport` by
parsing `INFO server`. The `backend_type` label is then a Prometheus
ConstLabel for the lifetime of the process; later `INFO server`
results do not update it. **Restart the hub after the endpoint flips
to the new engine** so the label reflects reality.

### Upgrading to per-stream series

Up to v0.1.0, transport metrics carried only `transport_type` and
`backend_type`. Later versions add `stream` to every `mercure_redis_*`
series, so each series starts anew when a hub is upgraded:

- Expressions whose result keeps `stream` return new series: their graphs
  break at the upgrade, and alerts on them restart their `for` windows.
  Expressions that aggregate `stream` away, such as
  `min by (job, instance, backend_type) (mercure_redis_healthy)`, keep
  their output labels, so their graphs continue and their alerts keep
  their state.
- The bundled dashboard's `stream` variable, set to All, matches series
  without a `stream` label too (`.*`), so its panels show hubs not yet
  upgraded beside upgraded ones, and history from before the upgrade stays
  visible. Selecting one stream hides them.
- `sum without (...)` keeps `stream` unless it is listed, so it returns
  one series per stream. `sum by (...)` drops it unless listed.
- An `on(...)` join between two unaggregated transport vectors matches
  several series per side when a process runs hubs on several streams.
  Add `stream` to the `on(...)` list, or aggregate both sides first.

### Publish path

| Metric | Collector | Labels | Reads as |
|---|---|---|---|
| `mercure_redis_publish_duration_seconds` | Histogram | — | End-to-end `Dispatch()` latency (Lua script: XADD + last-event-ID SET). p99 > 50ms in steady state usually means Redis under pressure. |
| `mercure_redis_publish_payload_bytes` | Histogram | — | Per-event payload size after codec encoding (XADD message body). Pair with `Network throughput (Redis)` and `publish_duration_seconds` p99 to predict bandwidth scaling and to spot publish-latency regressions caused by payload-size shifts. |
| `mercure_redis_publish_errors_total` | CounterVec | `kind` (`encode`/`publish`) | `Dispatch()` failures. An update rejected as too large (encoding over the 64 MiB codec cap) is a client error and is not counted; the hub counts it in `mercure_updates_failed_total{reason="validation"}`. Divide by `publish_duration_seconds_count` for an error rate. |
| `mercure_redis_publisher_rate_limited_total` | Counter | — | Publishes delayed by `WithPublisherRateLimit`. Sustained non-zero indicates the rate cap is the bottleneck. |
| `mercure_redis_stream_length` | Gauge | — | Sampled during health-check ticks. Watch for unbounded growth when neither `max_length` nor `event_ttl` is set. |

### Dispatch and fan-out

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
| `mercure_redis_xreadgroup_errors_total` | CounterVec | `kind` (`nogroup`/`nogroup_recreate_failed`/`other`/`xack`) | `nogroup` = a recoverable blip after consumer-group destruction. `nogroup_recreate_failed` = re-creating the group after `NOGROUP` failed (ACL, a replica, or the server out of memory under `noeviction`); investigate before ingest stops silently. `xack` = a batch XACK failed: the pending entries list grows, no data is lost. `other` = connectivity or ACL problem (alert). |
| `mercure_redis_stream_decode_errors_total` | CounterVec | `kind` (`data_type`/`unmarshal`/`forbidden_sse_chars`) | Stream entries dropped on the consumer side before dispatch. `data_type` and `unmarshal` are payload decode failures; a non-zero rate often signals a mixed-codec stream (see [Codec migration](architecture.md#codec-migration)). `forbidden_sse_chars` means the decoded update's `id` or `type` is not a valid SSE field value (invalid UTF-8, a control character such as CR, LF or NUL, or a Unicode format character) and could forge SSE frames (CWE-93). The drop is a defense-in-depth measure against an unvalidated or tampered entry (a 0.x or non-compliant hub, or direct stream tampering). It should be zero, and a sustained rate is a security signal that warrants a higher-priority alert than the codec kinds. The per-entry drop log is rate-sampled (up to 10 lines, then about one per second) so a flood cannot drown the logs. This counter, not the log, is the authoritative rate. |

### History replay

| Metric | Collector | Labels | Reads as |
|---|---|---|---|
| `mercure_redis_history_replay_duration_seconds` | Histogram | — | Per-replay wall time (paginated XRANGE). |
| `mercure_redis_history_replay_concurrent` | Gauge | — | In-flight replays. Cap at `WithHistoryReplayConcurrency` (default: half the go-redis pool, at most 20); reaching the cap means subscribers are queuing for a slot. |

### Subscriber lifecycle

| Metric | Collector | Labels | Reads as |
|---|---|---|---|
| `mercure_redis_subscriber_add_total` | Counter | — | `AddSubscriber` calls that listed the subscriber. Calls rejected on a closed transport are excluded; adds whose history replay then failed are included. |
| `mercure_redis_subscriber_remove_total` | Counter | — | `RemoveSubscriber` calls that removed a listed subscriber from an open transport, including the hub's removal of a subscriber whose history replay failed (the transport leaves it listed for the hub to remove). Redundant removals, removals of a subscriber that is not listed, and removals after `Close` are not counted. |
| `mercure_redis_add_subscriber_duration_seconds` | Histogram | — | End-to-end AddSubscriber latency (lock + shard insert + history replay). |
| `mercure_redis_remove_subscriber_duration_seconds` | Histogram | — | RemoveSubscriber latency (lock wait + shard remove), observed only for removals counted in `subscriber_remove_total`. p99 spikes during mass disconnects are expected. |
| `mercure_redis_subscribers_lost_total` | CounterVec | `reason` | Subscribers dropped with operational cause; at-most-one increment per subscriber across racing paths while it is listed (first attribution wins; rarely, a backpressure loss that races a removal is counted twice). `reason="shutdown"` (forced disconnect during `Close`, not in `subscriber_remove_total`). `reason="history_replay_failed"` (the history replay in `AddSubscriber` failed, from an XRANGE error or the registration timeout; the hub's removal that follows also counts in `subscriber_remove_total`, unless the transport closes first). `reason="backpressure"` (`LocalSubscriber.Dispatch` returned false during live dispatch: the dominant cause is slow-consumer `handleFullChan`, and it also counts the rare race where another path disconnected the subscriber in the same dispatch loop. The SSE handler ultimately observes the closed channel, exits, and triggers `RemoveSubscriber`, so backpressure events usually appear in `subscriber_remove_total` too, but not when the transport closes before that removal). |
| `mercure_redis_consumer_lag` | Gauge | — | Stream entries this node's consumer group has not yet consumed (XINFO GROUPS lag). Sustained non-zero = hub falling behind real-time. Reports `-1` when Redis cannot compute lag deterministically (entries XDEL'd from the middle, or pre-Redis-7.0 server). Sampled at the health-check cadence. |
| `mercure_redis_consumer_pending` | Gauge | — | Messages this node XREADGROUP'd but has not XACK'd yet (PEL size). Sustained growth indicates dispatch/ack pipeline backup beyond normal batch churn. |
| `mercure_redis_consumer_groups` | Gauge | — | Number of consumer groups attached to the stream (XINFO GROUPS length). One group per hub instance plus any not-yet-GC'd zombies. Gates the misconfiguration alert `publish_rate > 0 AND groups == 0` — catches "publisher writing to a stream nobody reads" (typo'd `stream` directive, wrong Redis). Reports `-1` on XINFO error (distinct from "Redis returned zero groups"). Sampled at the health-check cadence. |
| `mercure_redis_clock_drift_seconds` | Gauge | — | Absolute clock drift between hub and Redis server at connect time. Drift > `clock_skew_margin` (default 5s) breaks UUIDv7 timestamp ordering for history replay. |
| `mercure_redis_history_window_seconds` | Gauge | — | Seconds between the stream's first and last entry IDs: the history available for replay. Alert when it falls below the clients' reconnect window. 0 on an empty stream; -1 when the head or tail XRANGE fails. |
| `mercure_redis_history_replay_fallback_total` | Counter | — | Replays that fell through Pass 1 because the requested Last-Event-ID was not in the stream (trimmed past retention, or never published to it), triggering a Pass 2 full-stream replay. Sustained rate means retention is too tight for the reconnect window your clients exercise. |
| `mercure_redis_history_replay_truncated_total` | Counter | — | Replays cut short before delivering the full backlog because a matched update couldn't be handed off (subscriber disconnected, for any reason, or buffer full). Aggregate companion to the `mercure.history.truncated` span attribute; a sustained rate means reconnecting clients aren't getting their complete history (slow consumers or restart churn). |
| `mercure_redis_dispatch_zero_match_total` | Counter | — | Stream entries that matched no local subscriber. |
| `mercure_redis_ttl_cleanup_errors_total` | Counter | — | Failures of the periodic XTRIM-by-MINID call that enforces `event_ttl`. Persistent non-zero means stream growth is unbounded despite configured TTL. |

### Presence

| Metric | Collector | Labels | Reads as |
|---|---|---|---|
| `mercure_redis_presence_payload_bytes` | Histogram | — | SET payload size. Crossing `presence_detail_threshold` (default 1 000) flips to summary mode (count-only). |
| `mercure_redis_presence_read_errors_total` | CounterVec | `kind` (`get`/`unmarshal`) | Presence-key read failures in `GetSubscribers`. `kind=unmarshal`: one corrupt value skipped, the rest of the response still returned. `kind=get`: an MGET batch failed, so the whole call aborts with an error and returns no results (count reflects the failed batch's keys, not the whole fleet). |
| `mercure_redis_presence_read_capped_total` | Counter | — | `GetSubscribers` calls refused because the subscriber count exceeded `subscriptions_max_subscribers` (returns 503, no partial results). Non-zero = the `/subscriptions` endpoint is unusable at the current fleet size; raise the cap or stop polling it. |
| `mercure_redis_presence_heartbeat_errors_total` | Counter | — | Failed presence heartbeats: a failed SET (shutdown cancellation excluded) or a payload marshal failure. Timeouts, including dial timeouts while Redis is unreachable, are counted. Sustained non-zero risks `presence_ttl` expiry, so peers zombie-GC the node. |
| `mercure_redis_presence_fallback_total` | CounterVec | `reason` (`byte_budget`/`count_race`) | Presence-encoding decisions that fell back to summary mode. Counted at decision time, BEFORE the Redis SET — a downstream SET failure is tracked by `mercure_redis_presence_heartbeat_errors_total`, not by this counter. `reason=byte_budget`: the detail payload crossed `presence_detail_byte_threshold`. `reason=count_race`: membership grew past `presence_detail_threshold` between the count check and the walk (reconnect storm). |

### Health and GC

| Metric | Collector | Labels | Reads as |
|---|---|---|---|
| `mercure_redis_healthy` | Gauge | — | `1` = healthy, `0` = unhealthy. Drives the `/mercure/health/ready` 503 response after `health_threshold` consecutive PING failures. |
| `mercure_redis_ready` | Gauge | — | `1` when the transport's readiness check passes, `0` when `/mercure/health/ready` would answer `503` because PING has failed `health_threshold` times or the stream listener is stale. Read at scrape time, so it is useful where the load balancer probes `/mercure/health/live`. See [Metrics](#metrics) for transports sharing a series. |
| `mercure_redis_oom_rejections_total` | CounterVec | `operation` (`publish`/`presence`/`group_create`) | Writes the server refused because used memory is over `maxmemory` and nothing can be evicted, as always under `noeviction`. `publish`: an update was not stored (also counted in `publish_errors_total`). `presence`: this hub's presence key was not written at startup or not renewed (a renewal is also in `presence_heartbeat_errors_total`). `group_create`: creating or re-creating this hub's consumer group failed (a re-create is also in `xreadgroup_errors_total{kind="nogroup_recreate_failed"}`). An OOM condition does not lower `mercure_redis_ready` or close the `subscriber_reject_when_unhealthy` gate, as PING and XREADGROUP keep working, so this counter is the signal. |
| `mercure_redis_zombie_gc_errors_total` | Counter | — | Failures during zombie consumer-group cleanup: listing the groups (skips the pass), the presence-key check, or the destroy. Persistent non-zero may indicate zombie accumulation that needs operator attention. |
| `mercure_redis_zombie_gc_skipped_total` | Counter | — | Zombie GC passes skipped or interrupted because this node's own presence lease was unhealthy: a presence write failed, or none succeeded, within `presence_ttl`. An interrupted pass may already have destroyed some groups. Non-zero means the node could not renew its own presence key, usually because Redis refuses writes (OOM at `maxmemory`). |
| `mercure_redis_build_info` | GaugeVec | `version`, `revision`, `go_version` | Constant `1` per process. Distinguishes transport versions across nodes during a rolling deploy. `version` resolves via build-time ldflag (`RT_VERSION`) with a `runtime/debug.ReadBuildInfo` fallback; `revision` is the short VCS SHA. It is empty under `go test` or `go run` and in any `-buildvcs=false` build, including the hub image, whose Dockerfile sets that flag. |

### go-redis client pool (read-through)

The transport registers a Prometheus collector that reads go-redis's
`PoolStats()` at scrape time, so pool state is observable without
periodic polling.

With several live transports on the same stream and backend on one
registry, these metrics read one member's client: the most recently joined
member that has finished construction. A
transport drops out when it closes or its construction fails; with none
left, they have no samples. When the transport
being read drops out, the series switch to another member's client, so the
counters can jump or reset.

| Metric | Collector | Reads as |
|---|---|---|
| `mercure_redis_client_pool_hits_total` | Counter | Connection-pool hits (a pooled conn was available). |
| `mercure_redis_client_pool_misses_total` | Counter | Pool misses (a new conn was opened). High miss rate = pool size too small. |
| `mercure_redis_client_pool_timeouts_total` | Counter | Connections that waited longer than `pool_timeout` for a free slot. Non-zero = pool exhaustion; raise `pool_size` or lower `WithHistoryReplayConcurrency`. |
| `mercure_redis_client_pool_total_conns` | Gauge | Currently-open connections. |
| `mercure_redis_client_pool_idle_conns` | Gauge | Currently-idle connections. |
| `mercure_redis_client_pool_stale_conns_total` | Counter | Connections removed from the pool, for example after a failed health check (idle timeout, max lifetime, broken socket) or a connection error. |

### Suggested starter alerts

- `rate(mercure_redis_publish_errors_total[5m]) > 0` — sustained publish failures.
- `rate(mercure_redis_xreadgroup_errors_total{kind!="nogroup"}[5m]) > 0` — non-recoverable listener errors.
- `min by (job, instance, backend_type, stream) (mercure_redis_ready) == 0 for 1m` — this hub cannot serve the stream: PING is failing (a Redis outage) or the stream listener is stale. Prefer it to `mercure_redis_healthy`, which covers PING only.
- `rate(mercure_redis_client_pool_timeouts_total[5m]) > 0` — pool exhaustion. With several same-stream transports sharing the series, it can also fire once when the transport being read closes and the series switch to another member's client.
- `histogram_quantile(0.99, sum by (le) (rate(mercure_redis_dispatch_duration_seconds_bucket[5m]))) > 0.05` — dispatch p99 regression past 50ms.
- `mercure_redis_consumer_lag == -1 for 5m` — lag sampler hit non-deterministic case (warning, not page).
- `mercure_redis_consumer_pending > <threshold> for 5m` — PEL growth; threshold 10–100× steady state.
- `mercure_redis_consumer_groups == 0 AND rate(mercure_redis_publish_duration_seconds_count[5m]) > 0 for 5m` — publishes succeed but no hub reads. Almost always a `stream` directive typo or a hub deployed against the wrong Redis. Page.
- `abs(mercure_redis_clock_drift_seconds) > 4` — early warning before default 5s `clock_skew_margin`. NaN means the `TIME` probe failed.
- `mercure_redis_history_window_seconds < <expected_reconnect_seconds> for 5m` — retention is tightening; clients reconnecting at the expected gap are about to miss. Proactive companion to the `mercure_redis_history_replay_fallback_total` rate alert. Choose the threshold from your client reconnect-backoff ceiling.
- `rate(mercure_redis_history_replay_fallback_total[15m]) > 0.01` — chronic Pass-1 misses (full XRANGE scans).
- `rate(mercure_redis_history_replay_truncated_total[15m]) > 0.01` — reconnecting clients are losing backlog mid-replay (slow consumers or restart churn); correlate with `mercure_redis_subscribers_lost_total{reason="backpressure"}`.
- `rate(mercure_redis_dispatch_zero_match_total[15m]) / rate(mercure_redis_publish_duration_seconds_count[15m]) > 0.5 for 1h` — over half of published events match no local subscriber on this replica. Often means a publisher emitting to dead topics; investigate before the cost shows up in Redis CPU and hub fan-out.
- `rate(mercure_redis_ttl_cleanup_errors_total[5m]) > 0` — XTRIM MINID failing; stream grows unbounded.
- `increase(mercure_redis_zombie_gc_skipped_total[15m]) > 0` — this hub's own presence writes are failing, typically Redis or Valkey out of memory under `noeviction`; zombie groups are not reclaimed meanwhile.
- `increase(mercure_redis_oom_rejections_total[5m]) > 0` — Redis or Valkey is out of memory under `noeviction` and refuses writes: publishes fail and presence keys are not renewed. Free memory or raise `maxmemory`.
