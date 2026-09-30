# Redis transport configuration

How to configure the [Redis transport](../README.md) in a Caddyfile, and what each directive does.

## Caddyfile configuration

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
select Sentinel topology. The client tracks the current master and
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
> partition value). Each name hashes to a different slot, so
> N streams give N× the headroom on an N-shard Cluster. Mercure hubs
> are configured per-stream via the Caddyfile, so this is a deployment
> decision, not a transport-level dynamic shard.

The right choice depends on how many independent **streams** you run.
Topic-level multi-tenancy (many topics on one stream) gains nothing from
Cluster: only distinct `stream` names hash to different slots.

| Use case | Recommended topology | Caddyfile |
|---|---|---|
| Single mercure hub fleet, one transport stream, many topics (for example per-user or per-channel topics) | Single instance with replicas, or Sentinel for self-managed HA. ElastiCache "cluster mode disabled" (CMD) maps here. | `url redis://primary:6379` (single endpoint) or `addresses ... master_name ...` (Sentinel) |
| Multiple isolated hub fleets, distinct `stream` names per fleet (for example one per region or deployment) | Redis Cluster: distinct stream names hash to distinct slots, sharding fleets across nodes. ElastiCache "cluster mode enabled" (CME) maps here. | `addresses node1 node2 node3` |
| Development or single node | Anything; `url redis://localhost:6379` | bundled `redis-test.Caddyfile` |

**AWS ElastiCache specifics:**

- **Cluster mode disabled (CMD)** — point `url` at the **primary endpoint**.
  AWS handles failover by re-pointing the primary endpoint at the promoted
  replica. Failover duration depends on the environment (engine version,
  replica lag, DNS-propagation TTL); see [AWS Auto Failover](https://docs.aws.amazon.com/AmazonElastiCache/latest/dg/AutoFailover.html).
  Replicas exist for durability but are unused by this transport:
  XREADGROUP and XADD both require the primary, so reader endpoints are
  not configured.
- **Cluster mode enabled (CME)** — point `addresses` at the
  **configuration endpoint** (or a list of seed nodes). go-redis follows
  MOVED redirects to the slot-owning shard. Each shard has its own
  failover trigger, so an outage of one primary doesn't affect other
  shards. `tls_server_name` works only when nodes share a wildcard SAN;
  see the TLS notes below.
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
- **`tls_server_name`** — overrides the SNI and certificate-verification
  hostname. Useful when dialing by IP. **Cluster mode caveat:** go-redis
  applies a single shared `ServerName` across all cluster nodes, so this
  directive only works for clusters whose nodes share a SAN (for example,
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
needs specific commands and key patterns. A minimal-privilege ACL for
`stream mercure`:

```text
ACL SETUSER mercure on >SECRET \
    ~{mercure} ~{mercure}:* \
    +ping +info +time \
    +xadd +xrange +xrevrange +xlen +xinfo|groups +xreadgroup +xack \
    +xgroup|create +xgroup|destroy +xtrim \
    +get +set +del +mget +exists \
    +scan \
    +eval +evalsha
```

What each block is for:

- **`~{mercure} ~{mercure}:*`** — restricts the user to the stream key
  and every presence/lastEventID key under it. Replace `mercure` if
  you set `stream <name>`.
- **`+ping +info +time`** — `PING` is the health check: without `+ping`
  every check fails with `NOPERM`, the transport turns unhealthy after
  `health_threshold` failures, and readiness returns `503`. `INFO server`
  is read at startup for the Redis-vs-Valkey detection that populates the
  `backend_type` label; `TIME` is read to seed the clock-drift gauge.
- **`+xadd … +xack`** — the stream commands used by Dispatch (`XADD`), the
  XREADGROUP listener, history replay and the health-check samples
  (`XLEN`, `XINFO GROUPS`, `XREVRANGE`). Missing any breaks publish or
  delivery (not connectivity); the hub starts but does not flow events.
- **`+xgroup|create +xgroup|destroy +xtrim`** — consumer-group lifecycle
  (create at startup and on `NOGROUP`, destroy at shutdown and in zombie
  GC) and retention (`XTRIM MINID`).
- **`+get +set +del +mget +exists`** — presence heartbeat keys plus the
  `lastEventID` mirror. Presence TTLs are set with `SET`, so no
  `+expire`, `+pexpire` or `+ttl` is needed.
- **`+scan`** — presence-key enumeration for `GetSubscribers`, the
  subscription API.
- **`+eval +evalsha`** — the Lua scripts used by Dispatch, TTL cleanup and
  zombie GC. go-redis sends `EVALSHA` and falls back to `EVAL` when the
  server does not have the script cached, for example after a restart. The
  transport never issues `SCRIPT LOAD`.

Redis checks the user's ACL for each command a Lua script runs, so
`+xadd`, `+set`, `+xtrim`, `+exists` and `+xgroup|destroy` must be granted
even though the transport reaches them through `EVALSHA`. The
`command|subcommand` form works on Redis 6.2 and later.

Add these for specific topologies:

- **`+select`** when `db` is not 0, because go-redis sends `SELECT` on every
  new connection.
- **`+cluster|slots`** for Redis Cluster, because the cluster client reads
  the slot map with `CLUSTER SLOTS`.

Do not grant `+@all` to bootstrap. A `+ping`-only ACL passes the
PING readiness probe but fails at startup version detection, the clock-drift
check, the XREADGROUP listener, or the GC sweep. The errors often do not name
ACLs.

For ElastiCache RBAC, apply the equivalent command/key list as the
user-group access string.

> The bundled `redis-test.Caddyfile` and `conformance.Caddyfile` are
> development scaffolds; their headers list the environment hooks. Don't use
> them in production: add the `transport redis { ... }` block to your own
> Caddyfile, for example through `MERCURE_EXTRA_DIRECTIVES`.

## Configuration reference

String, secret and duration directives (connection strings, credentials,
TLS paths and names, `stream`, `encoding`, and every duration) support
Caddy `{env.VAR}` runtime placeholders. Unset placeholders fail at
Provision with a directive-specific error so a typo
(`password {env.REDIS_PASWORD}`) surfaces immediately rather than silently
using the default.

Numeric and boolean directives, such as `max_length`, `dispatch_shards`, the
rate limits and bursts, `db`, the pool knobs, `tls` and
`tls_insecure_skip_verify`, take literals: `{env.VAR}` there is a Caddyfile
parse error. To take a
value from the environment, write `"{$VAR}"` quoted
(`dispatch_shards "{$MERCURE_SHARDS}"`) or `{$VAR:default}`. Caddy
substitutes `{$VAR}` as text before splitting the line into arguments, so
an unset, unquoted variable vanishes and shifts the arguments (a bare
`tls {$VAR}` would silently mean `tls`). Quoted, an unset variable leaves
an empty argument and startup fails.

**Reload caveat:** `{env.VAR}` resolves at Provision (runtime), so it
survives into Caddy's adapted JSON as the literal placeholder. Changing an
env var's *value* without editing the Caddyfile text does **not** take
effect on `caddy reload`. Caddy byte-compares the adapted JSON, sees no
change, and skips re-provisioning (Caddy framework behavior). When a reload
must pick up an env change, use `{$VAR}` (substituted at *adapt* time, so
the JSON changes) or restart the process.

Boolean directives `tls`, `tls_insecure_skip_verify` and
`subscriber_reject_when_unhealthy` accept a bare flag or the boolean strings
`true`/`false`/`on`/`off`/`yes`/`no`/`1`/`0`. For "TLS in prod, plaintext in
dev", use `tls {$MERCURE_REDIS_TLS:false}`.

Directives are grouped by subsystem; [Architecture](architecture.md) explains how
the subsystems work.

### Connection and topology

| Directive | Default | Description |
|---|---|---|
| `url` | one of `url` or `addresses` | Redis/Valkey URL (`redis://`, `rediss://`). Mutually exclusive with `addresses`. |
| `addresses` | one of `url` or `addresses` | Multiple seed nodes for Cluster (or Sentinel when `master_name` is set). Repeated `addresses` lines accumulate hosts. |
| `address` | — | Singular alias for `addresses`; multiple `address` lines accumulate. |
| `master_name` | "" | Sentinel master name. When set, `addresses` are sentinel seeds. |
| `username` | "" | Redis ACL username. Overrides URL userinfo. |
| `password` | "" | Redis password. Overrides URL userinfo. |
| `db` | URL path (or 0) | Database index (0–15 standalone; 0 Cluster). Explicit `db 0` overrides a non-zero URL DB. |

### Pool, timeouts, retries

go-redis client tuning. All fields are pass-through to
`redis.UniversalOptions`; an unset directive keeps the go-redis default.
Tune them only in response to the pool metrics.

| Directive | Default (go-redis library) | Description |
|---|---|---|
| `pool_size` | 10 × GOMAXPROCS (5 × for Cluster) | Max open connections per pool. Raise when `mercure_redis_client_pool_timeouts_total` is non-zero under load; bound when Redis `maxclients` is close to saturation. |
| `min_idle_conns` | 0 (lazy) | Floor of idle connections kept warm. Set non-zero to amortize connect cost across the first requests after hub start or transport reload. |
| `pool_timeout` | `read_timeout + 1s` | How long a Dispatch, XREADGROUP or XRANGE caller waits for a free pool entry before returning `ErrPoolTimeout`. A tighter value signals pool exhaustion sooner; a looser one swallows transient spikes. |
| `dial_timeout` | 5s | Connection-establishment ceiling. Tighten it in low-latency environments; loosen it on slow-DNS or VPN paths. |
| `read_timeout` | 3s | Socket-read ceiling for PING, XADD, XINFO, XRANGE and the other commands. Not applied to the blocking XREADGROUP; go-redis gives it `xread_block` + 10s. A shorter caller deadline cuts the command first: the hub's `publish_timeout`, the health check's budget, `subscriber_registration_timeout`, `Close`'s fixed 5s. The Caddy module sets go-redis `ContextTimeoutEnabled` for this; a Go caller building its own client must set it too, or a stalled publish runs until `read_timeout` and then either fails with a late `504` or, if the reply arrives first, succeeds late. A cut closes its pooled connection, so a slow but live server whose replies land between the deadline and `read_timeout` causes more reconnects. |
| `write_timeout` | 3s | Socket-write ceiling. Equal to `read_timeout` in go-redis defaults. |
| `max_retries` | 3 | Per-command transient-failure retries. Pass `-1` to disable retries on a single node (every transient failure surfaces in metrics rather than being absorbed); a cluster client still retries, see the note below. |

> **Retries can duplicate a publish.** When a reply is lost (EOF, reset
> connection, read timeout), go-redis re-sends the publish script even if it
> already ran: up to `max_retries` times against a single node. Against a
> cluster, node and cluster retries multiply (16 sends with `max_retries 3`),
> and `-1` does not turn them off. Every copy carries the same id. The hub
> doesn't deduplicate, so subscribers should skip ids they have already seen.

### TLS

| Directive | Default | Description |
|---|---|---|
| `tls` | false | Enable TLS with default settings (TLS 1.2 minimum). |
| `tls_ca_file` | "" | PEM file with trusted CA certificate(s); replaces system pool. |
| `tls_client_auth` | "" "" | `CERT KEY` pair for mutual TLS (atomic two-arg directive). |
| `tls_server_name` | "" | Override SNI / cert-verification hostname. |
| `tls_insecure_skip_verify` | false | Disable cert verification (additive with URL `?skip_verify`; emits Warn). |

### Stream and codec

| Directive | Go Option | Default | Description |
|---|---|---|---|
| `stream` | `WithStreamName` | `"mercure"` | Redis Stream key name. It must not start with `}`: every key is built as `{<stream>}<suffix>` and Redis reads the hash tag up to the first `}`, so a leading `}` gives an empty tag and the stream, `lastEventID` and presence keys would hash to different Cluster slots (CROSSSLOT on publish and GC). The `stream` directive and `WithStreamName` fail startup with `ErrInvalidOptions` for such a name. A `}` anywhere later is fine. |
| `encoding` | `WithEncoding` | `"json"` | Codec: `json`, `gob`, `msgpack` (`encoding gob`; there is no bare `gob` directive). See [Codec choices](architecture.md#codec-choices). |

**Several Redis hubs in one Caddy config must use distinct `stream`
names.** Each hub gets its own transport (the hub `name` is part of the
transport-sharing key), but hubs on the same `stream` share its Redis keys.
An update published on one hub reaches the other hub's subscribers, and each
hub's subscription API lists both hubs' subscribers. Distinct streams keep
them apart, including their `mercure_redis_*` series (see
[Metrics](metrics.md#metrics)).

An update whose encoded form exceeds 64 MiB, the largest entry the codecs
decode, is rejected at publish (`mercure.ErrCodecPayloadTooLarge`) instead of
being stored and then dropped by every node. It is a client error: the hub
answers 413 Request Entity Too Large and counts it in
`mercure_updates_failed_total{reason="validation"}`, not in
`mercure_redis_publish_errors_total`.

Over HTTP that 413 is reachable only
with `max_request_body_size` set above roughly 64 MiB. With the default
(1 MiB), the hub answers 413 for the request body itself. With the limit
disabled (`0`), Go's form parser caps a URL-encoded body at 10 MiB and the
hub answers 400. `Hub.Publish` from the Go API reaches the cap directly.

### Durability and cleanup

| Directive | Go Option | Default | Description |
|---|---|---|---|
| `max_length` | `WithMaxLength` | `0` (unlimited) | Approximate MAXLEN for stream trimming. Negative values are rejected at provisioning. |
| `event_ttl` | `WithEventTTL` | `1h` | TTL-based stream cleanup window. Set to `0` to opt out (pairs with `max_length 0` for fully unbounded). Negative values are rejected at provisioning. |
| `cleanup_interval` | `WithCleanupInterval` | `5m` | How often TTL cleanup (XTRIM MINID) runs. |

> **Both zero is unsafe in production.** When both `max_length` and `event_ttl`
> are 0, the stream is never trimmed. The transport logs one Warn at startup
> and then nothing else. There is no runtime back-pressure or auto-bound.
>
> The cascade if you ignore the warning:
>
> 1. Redis memory grows linearly (≈ message size + 80 B/entry overhead).
> 2. Redis hits `maxmemory`. Behavior depends on `maxmemory-policy`:
>    - `noeviction`: publishes start returning OOM errors and
>      `mercure_redis_publish_errors_total` climbs.
>    - `allkeys-lru` / `allkeys-random`: Redis evicts whole keys, possibly
>      the stream key itself or the `lastEventID` key; subscribers
>      requesting history see gaps.
> 3. `XRANGE` history replay slows from milliseconds to seconds; reconnecting
>    subscribers serialize behind `history_replay_concurrency` (default: half
>    the go-redis pool, at most 20).
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
| `presence_detail_byte_threshold` | `WithPresenceDetailByteThreshold` | `524288` (512 KiB) | Marshaled byte size above which detail-mode presence falls back to summary mode. It catches a large payload even when the subscriber count is below `presence_detail_threshold`, for example because subscribers have long matcher lists or large `payload` claims. `0` disables the byte budget. |
| `subscriptions_max_subscribers` | `WithSubscriptionsMaxSubscribers` | `100000` | Max unique subscribers `GetSubscribers` (the `/subscriptions` API) materializes before aborting with 503, so a huge fleet can't OOM the hub by listing the whole cluster. Peak heap is roughly this many subscribers plus one MGET batch of presence values. `0` disables the cap. |

### Dispatch

| Directive | Go Option | Default | Description |
|---|---|---|---|
| `dispatch_shards` | `WithDispatchShards` | `1` | Dispatch worker goroutines (`0` = NumCPU). Hard-capped at 256 (auto-detect on high-vCPU hosts and explicit large values both clamp with a Warn log). |
| `xread_count` | `WithXReadCount` | `100` | Messages per XREADGROUP call. |
| `xread_block` | `WithXReadBlock` | `1s` | XREADGROUP BLOCK timeout. Minimum `1ms`; startup fails with `ErrInvalidOptions` below it. Redis takes BLOCK in whole milliseconds, so a shorter value would be sent as `BLOCK 0`, which the server holds until an entry arrives. go-redis's own `BLOCK` + 10s read deadline would then fail the read about every 10s on an idle stream. |
| `clock_skew_margin` | `WithClockSkewMargin` | `5s` | UUIDv7 timestamp seek margin for history replay. A requested `Last-Event-ID` that is a UUIDv7 dated more than the margin after the serving hub's clock is a future ID. History replay is skipped for it and the subscriber receives live events only. UUIDv7 event IDs therefore assume that the clock that dated them (a publisher's for publisher-chosen IDs, or another hub's) is within the margin of the serving hub's clock. |

The hub's `subscriber_list_cache_size` directive has no effect with this
transport: dispatch matches through a per-shard topic index, not the
subscriber list's match cache.

### Rate limiting

| Directive | Go Option | Default | Description |
|---|---|---|---|
| `subscriber_rate_limit` | `WithSubscriberRateLimit` | `0` (disabled) | Max new subscribers/second; fractional values allowed (for example `0.5`). Enforced by the admission gate: new HTTP subscribers over this rate are shed at the front door with `429` rather than blocking. See [Admission control](#admission-control-load-shedding). |
| `subscriber_rate_burst` | `WithSubscriberRateBurst` | `5000` | Burst allowance for subscriber rate limiter. |
| `publisher_rate_limit` | `WithPublisherRateLimit` | `0` (disabled) | Max publish ops/second; fractional values allowed. |
| `publisher_rate_burst` | `WithPublisherRateBurst` | `5000` | Burst allowance for publisher rate limiter. |
| `history_replay_concurrency` | `WithHistoryReplayConcurrency` | half the go-redis pool, at least 1, at most 20 | Max concurrent history replays (XRANGE). The default takes at most half the pool (go-redis default 10 × GOMAXPROCS, 5 × for Cluster), leaving the rest to XREADGROUP, XADD and presence, so a 1-vCPU host gets 5. A `pool_size` of 1 still gets 1, with no headroom. An explicit value at or above the pool size logs a Warn. |

### Admission control (load shedding)

When `subscriber_rate_limit` or `subscriber_max_count` is set, the hub gates new
SSE subscribers at the front door, before JWT verification or any allocation.
It sheds subscribers over the ceiling with `429 Too Many Requests` and a
`Retry-After` header. The gate bounds a hub's exposure to a thundering-herd
reconnect storm (a fleet reconnecting after a deploy, a network blip, or a
power-restore event) instead of letting every arrival contend for memory and
CPU. The `429` is transient: clients should reconnect after the `Retry-After`
delay rather than treat it as a terminal error.

The gate runs two checks, in order:

1. **Concurrent count** (`subscriber_max_count`) — a fast reject when this hub
   already holds the maximum admitted subscribers. Recorded as `reason=capacity`.
2. **Accept rate** (`subscriber_rate_limit` + `subscriber_rate_burst`) — a token
   from the subscriber rate limiter. With `subscriber_admission_timeout = 0` (the
   default) an over-rate request is shed immediately; with a positive timeout it
   waits up to that long for a token before shedding. Recorded as `reason=rate`.

With `subscriber_reject_when_unhealthy` on, the gate checks health before
both: while PING has failed `health_threshold` times in a row, a new
subscription gets `503 Service Unavailable` (no `Retry-After`). Recorded as
`reason=unhealthy`. Subscriptions already streaming stay open. A stale stream
listener alone does not trigger it.

Native browser `EventSource` treats a `503` as fatal and does not reconnect
by itself, so enable this only for clients that retry `5xx` responses with
their last event ID. A refused client that has no event ID yet misses the
events processed before it reconnects.

Rejections increment `mercure_redis_subscriber_admission_rejected_total{reason}`:
`rate` and `capacity` answer `429`, `unhealthy` answers `503`.

The hub's `subscription_limits` is a separate check that runs after
authorization. It has its own `429` with a `Retry-After` of 5 to 10 seconds
and its own metric, `mercure_subscriptions_rejected_total`. A request it
refuses has already used an admission rate token.

| Directive | Go Option | Default | Description |
|---|---|---|---|
| `subscriber_max_count` | `WithSubscriberMaxCount` | `0` (disabled) | Max concurrent admitted subscribers on this hub. Connections over the ceiling are shed with `429`. The count is maintained in-process (independent of the cluster-wide presence count), so size it to one instance's connection budget, not the fleet's. |
| `subscriber_admission_timeout` | `WithSubscriberAdmissionTimeout` | `0` (fail-fast) | How long an over-rate request may wait for a rate token before being shed. `0` sheds immediately. No effect unless `subscriber_rate_limit` is set. |
| `subscriber_registration_timeout` | `WithSubscriberRegistrationTimeout` | `0` (unbounded) | Bounds the history-replay and registration work an admitted subscriber performs before its stream is established. `0` leaves it unbounded. The bound applies to the replay's Redis reads: a page of entries already fetched when it expires is still dispatched. |
| `subscriber_retry_after` | `WithSubscriberRetryAfter` | `2s` | Base for the `Retry-After` header on a `429`. The emitted value is jittered (base plus up to 50%) so a shed herd reconnects spread out rather than in lockstep. Rounded up to whole seconds per RFC 9110. |
| `subscriber_reject_when_unhealthy` | `WithSubscriberRejectWhenUnhealthy` | off | Answer a new subscription `503` while PING has failed `health_threshold` times in a row. Existing streams stay open. Bare flag or `true`/`false`. |

> **429 only helps if clients cooperate.** Shedding shifts a herd in time only
> when clients honor the `Retry-After` header and reconnect with jittered
> backoff. A client that ignores `Retry-After` and retries immediately merely
> moves the same load to the next instant. Before relying on admission control to
> absorb a cold-start surge, confirm your SSE client honors `Retry-After` on
> `429` and applies jitter on both its initial connect and its reconnects.

### Health

| Directive | Go Option | Default | Description |
|---|---|---|---|
| `health_interval` | `WithHealthInterval` | `10s` | Health check PING interval. Each check's commands are cut at its budget (this interval clamped to 1–5s) when that is shorter than `read_timeout` and the client sets `ContextTimeoutEnabled` (see `read_timeout`). |
| `health_threshold` | `WithHealthThreshold` | `3` | Consecutive PING failures before flipping to unhealthy. |

`/mercure/health/ready` returns `503` when the transport is closed, after
`health_threshold` consecutive PING failures, or when the stream listener has
not completed a read for `max(5 × xread_block, 30s)`. `/mercure/health/live`
returns `503` only when the transport is closed or the stream listener has
exited. Point checks that replace instances at liveness; see
[Health monitoring](../../docs/production/health-monitoring.md#checks-that-replace-instances).

### Zombie GC

| Directive | Go Option | Default | Description |
|---|---|---|---|
| `zombie_gc_interval` | `WithZombieGCInterval` | `5m` | Periodic zombie consumer group GC (`0` = disable; startup GC always runs, plus one follow-up pass `presence_ttl` later). |

### Validation summary

Configuration is checked in three layers.

**Strict checks refuse startup with `ErrInvalidOptions` (joined via `errors.Join`, so all violations surface at once):**

| Condition | Reason |
|---|---|
| `event_ttl < 0` | Negative TTL would compute a future MINID and erase the stream every cleanup tick. |
| `max_length < 0` | Redis rejects negative `MAXLEN` at the wire, so every publish would fail. |
| `presence_interval >= presence_ttl` | A single scheduling jitter would self-evict the node's presence key. |
| `xread_block < 1ms` | See the [`xread_block` row](#dispatch). |
| `stream` starting with `}` | Empty Redis Cluster hash tag (CROSSSLOT). See the [`stream` row](#stream-and-codec). |
| `encoding` not in `{json, gob, msgpack}` | Wraps both `ErrInvalidOptions` and `mercure.ErrUnsupportedCodec` so callers can match either sentinel. |
| Any other negative numeric or duration directive (`dispatch_shards`, `health_threshold`, `xread_count`, the rate limits and bursts, `history_replay_concurrency`, `presence_detail_threshold`, `presence_detail_byte_threshold`, `db`, the pool and timeout knobs, the admission knobs `subscriber_max_count` / `subscriber_admission_timeout` / `subscriber_registration_timeout` / `subscriber_retry_after`) | Rejected at provisioning by the Caddy module with `option must be >= 0`, not `ErrInvalidOptions`. A negative is an operator typo; fail fast rather than silently fall back to the default (or, for `dispatch_shards`, to NumCPU). `max_retries` also accepts `-1`, which disables retries on a single node (see the `max_retries` row). |

Under Caddy, a negative `event_ttl` or `max_length` is rejected the same way, at provisioning with `option must be >= 0`, before the transport validates its options. The Go API returns `ErrInvalidOptions` for them.

**Soft checks log a startup Warn but do not fail:**

| Condition | Threshold | Concern |
|---|---|---|
| `xread_block` very short | `1ms` to `< 10ms` | XREADGROUP busy-polls, wasting Redis RTTs. |
| `xread_count` very high | `> 10 000` | Listener buffers up to N entries per BLOCK call (memory pressure during fan-out). |
| `clock_skew_margin` very large | `> 1h` | History replay scans a wide stream window per reconnect. |
| `health_threshold == 1` | exact match | Single failed PING flips Ready to unhealthy with no flap suppression. This is fine for tight latency budgets and flappy on noisy networks. |
| explicit `history_replay_concurrency >= pool_size` | client-aware | Replay storms can exhaust the go-redis pool, starving XREADGROUP/XADD/presence ops. Predicate is `>=` so equality (zero headroom) also warns. Reported only when pool size is introspectable (`*redis.Client`, `*redis.ClusterClient`, `*redis.Ring`). The default is derived from the pool size and never trips it. |
| `max_length == 0 && event_ttl == 0` | exact match | Stream is never trimmed; grows unbounded until Redis hits `maxmemory`. See the [Durability and cleanup](#durability-and-cleanup) callout for the OOM cascade. |
| `subscriber_admission_timeout` set without `subscriber_rate_limit` | exact match | The admission wait has nothing to wait on because there is no rate limiter, so the timeout is a no-op. |
| `subscriber_retry_after` very large | `> 5m` | Clients are told to wait minutes before retrying; usually a unit mistake. |

**Hard clamps correct the value without failing and log a Warn:**

| Condition | Action |
|---|---|
| `dispatch_shards > 256` | Clamped to 256 with a Warn log. Auto-detect via `runtime.NumCPU()` passes through the same cap. The cap bounds per-shard Prometheus series cardinality. |

The Caddyfile adapter rejects the `tls { ... }` sub-block syntax (flat
directives only) and a non-literal numeric or boolean value (such as
`{env.X}`) at parse time. The mutually exclusive `url` and `addresses` pair
fails at provisioning, as do negative numeric values and negative durations,
which are checked after any duration `{env.VAR}` placeholders resolve. Two
durations are the exception: a negative `clock_skew_margin` is ignored (the
`5s` default stays) and a negative `zombie_gc_interval` disables periodic GC
like `0`.
Either way the misconfiguration surfaces at config load rather than
silently degrading to a default.
