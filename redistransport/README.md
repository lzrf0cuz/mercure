# Redis transport for Mercure (Redis and Valkey)

[![Go Reference](https://pkg.go.dev/badge/github.com/lzrf0cuz/mercure/redistransport.svg)](https://pkg.go.dev/github.com/lzrf0cuz/mercure/redistransport)
[![Go Report Card](https://goreportcard.com/badge/github.com/lzrf0cuz/mercure/redistransport)](https://goreportcard.com/report/github.com/lzrf0cuz/mercure/redistransport)
[![Latest release](https://img.shields.io/github/v/release/lzrf0cuz/mercure?filter=redistransport%2Fv*&label=release)](https://github.com/lzrf0cuz/mercure/releases?q=redistransport)
[![License: AGPL-3.0](https://img.shields.io/badge/license-AGPL--3.0-blue)](LICENSE)

A [Redis Streams](https://redis.io/docs/latest/develop/data-types/streams/)–based transport
for the [Mercure](https://github.com/dunglas/mercure) hub. Targets
**Redis 6.2+** and **[Valkey](https://valkey.io) 7.2+** (see
[Compatibility](#compatibility) for version detection details).

## Documentation

| Page | Contents |
|---|---|
| [Configuration](docs/configuration.md) | Caddyfile setup, topology choice, auth and TLS, and the directive reference. |
| [Metrics and observability](docs/metrics.md) | Dev-stack Prometheus and Grafana, tracing, and the `mercure_redis_*` series. |
| [Architecture](docs/architecture.md) | Publish and dispatch design, shutdown, presence, Redis sharing and codecs. |

## Quick start

### Prerequisites

- [Task](https://taskfile.dev), the task runner used throughout the repo.
- Docker Compose v2.
- Go 1.27.3+ — only required for `task dev:redis` (native hub mode). Container
  paths build the hub in-image.

All commands below run from the repo root.

### 1. Start a Redis-compatible server

Pick one. Valkey and Redis are alternatives, not stacked. Both ship with
[RedisInsight](https://redis.io/insight/) on `http://localhost:5540` via the
shared `compose.redisinsight.yaml`, and both can opt into Prometheus +
Grafana with `--profile metrics` (see [Observability](docs/metrics.md#observability)).

#### Option A: Valkey (recommended, BSD-licensed)

```sh
task rt:valkey:up
# or from redistransport/:
# docker compose -f compose.valkey.yaml up -d
```

#### Option B: Redis

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

The HS256 JWT keys come from `.env.dev`, which `task setup` copies from
`.env.dev.example`. These are dev defaults: never use the shipped
`!ChangeThisMercureHubJWTSecretKey!` value in production. For real
secrets in dev, put them in `.env.local` (gitignored).

`redis-test.Caddyfile` is a transport-only test scaffold used by
`task go:run:redis`. It does not enable the debug UI or the demo flow; use
`task dev:redis` for those.

### 3. Open the Mercure debug UI

```text
https://localhost/.well-known/mercure/debug/
```

`task dev:redis` serves `local.Caddyfile` on `:443` (HTTPS); `:4000` is
`redis-test.Caddyfile`'s port. The debug UI also answers at
`https://localhost/`. The first load shows a self-signed-certificate warning;
after the first hub start creates the local CA, run `task tls:trust`.

### 4. Publish your first event

The fastest path is the bundled debug UI's built-in publish form: open
the UI, Subscribe to `https://example.com/books/1`, then Publish to the
same topic and watch the event arrive.

For programmatic integration, POST to `/.well-known/mercure` with an
access token allowed to publish. From the repository root, the HS256 dev
fixture (`fixtures/jwt/demo_hs256.jwt`: `iss https://localhost`,
`aud https://localhost/.well-known/mercure`, publish on every topic)
matches the dev hub's issuer and the audience it derives for
`https://localhost`:

```sh
JWT=$(cat fixtures/jwt/demo_hs256.jwt)

# Publish (-k accepts the dev self-signed cert):
curl -k -X POST https://localhost/.well-known/mercure \
  -H "Authorization: Bearer $JWT" \
  --data-urlencode "topic=https://example.com/books/1" \
  --data-urlencode 'data={"hello":"world"}'
```

In another terminal, start a subscriber, then run the publish command again:

```sh
curl -k -N "https://localhost/.well-known/mercure?match=https://example.com/books/1"
```

You should see the event stream out as `data: {"hello":"world"}`.

## RedisInsight

Both compose stacks include [RedisInsight](https://redis.io/insight/) at
[http://localhost:5540](http://localhost:5540) (`task rt:redis:open` /
`task rt:valkey:open`). A one-shot `redisinsight-bootstrap` sidecar
POSTs the `Local Redis` / `Local Valkey` connection to the RedisInsight
API once the UI healthcheck passes.

## Compose stacks

| Stack | File | Services | Task |
|---|---|---|---|
| **Valkey** | `compose.valkey.yaml` | Valkey 8, RedisInsight, (+ optional Prometheus/Grafana via `--profile metrics`) | `task rt:valkey:up` |
| **Redis** | `compose.redis.yaml` | Redis 7, RedisInsight, (+ optional Prometheus/Grafana via `--profile metrics`) | `task rt:redis:up` |
| **Redis Cluster** | `compose.cluster.yaml` | Redis 7 (3 masters; plain 7000-7002, TLS 8000-8002) | `task rt:cluster:up` |

Shared service definitions live in their own files and are pulled into
the parent stacks via Compose's `include:` directive, so each service has
one source of truth:

| Shared file | Provides | Pulled into |
|---|---|---|
| `compose.redisinsight.yaml` | RedisInsight UI on `:5540` | redis, valkey |
| `compose.observability.yaml` | Prometheus on `:9090` + Grafana on `:3000` (gated by `metrics` profile) | redis, valkey |

```sh
# Valkey stack
task rt:valkey:up          # Start Valkey + RedisInsight
task rt:valkey:down        # Stop (with sidecars) and remove volumes, except Prometheus/Grafana data
task rt:valkey:logs        # Follow logs
task rt:valkey:open        # Open RedisInsight in browser

# Redis stack
task rt:redis:up           # Start Redis + RedisInsight
task rt:redis:down         # Stop (with sidecars) and remove volumes, except Prometheus/Grafana data
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
A multi-container layout would need a custom client dialer, because
`cluster-announce-ip` drives both client routing and inter-node gossip.

Each node also listens on a TLS port (the data port plus 1000, so
8000-8002) using a self-signed cert that `task rt:cluster:up` generates. The
TLS-on-Cluster integration test connects to the TLS plane with
`InsecureSkipVerify`, mirroring the production pattern documented for
clusters whose nodes do not share a single SAN.

## Upgrading from a Mercure 0.x hub

A move to the Mercure 1.0 hub is a flag-day cutover onto fresh Redis
streams: 0.x and 1.0 hubs cannot run against the same stream, so there is no
mixed-version operation or rolling upgrade.

1. Stop every 0.x hub.
2. Update the `redis` block: a numeric or boolean directive written with
   `{env.VAR}` (`max_length {env.MAX_LEN}`, `tls {env.TLS}`) must use
   `{$VAR}` or a literal, and a bare `gob` directive becomes
   `encoding gob`. Both fail at config load otherwise.
3. Start the 1.0 hubs on a new `stream` name, or delete the old stream key,
   its `lastEventID` key and its `presence:*` keys first.

Why a shared stream does not work:

- 1.0's publish guard rejects these `id` and `type` values, which 0.x accepted:
  - a control character other than CR, LF or NUL, invalid UTF-8, or a Unicode
    format character, in either field
  - an `id` starting with `#` or equal to `earliest`
  - the `type` `mercure`
- A 1.0 node drops a 0.x entry whose `id` or `type` fails the character check,
  at replay and at live dispatch
  (`mercure_redis_stream_decode_errors_total{kind="forbidden_sse_chars"}`).
- Presence entries and subscription events changed shape: 1.0 subscribers
  carry topic matchers where 0.x ones carried topics, and 1.0 subscription
  events carry `match`/`match_type`. Each version misreads the other's.

A client that reconnects with a `Last-Event-ID` from the old stream gets
`Mercure-Last-Event-ID: earliest` (the ID is unknown on the new stream) and
the new stream's history.

## Go API

```go
import (
    "time"

    "github.com/lzrf0cuz/mercure/redistransport"
    "github.com/redis/go-redis/v9"
)

// ContextTimeoutEnabled lets publish_timeout and the other caller deadlines
// cut a command already sent instead of waiting for ReadTimeout.
client := redis.NewClient(&redis.Options{Addr: "localhost:6379", ContextTimeoutEnabled: true})

transport, err := redistransport.NewRedisTransport(client,
    redistransport.WithStreamName("mercure"),
    redistransport.WithMaxLength(10000),
    redistransport.WithEncoding("json"),
    redistransport.WithEventTTL(24 * time.Hour),
)
```

`NewRedisTransport` owns `client`: it closes it on any error, and `Close`
closes it on a transport that was built.

Pass the go-redis client itself (`*redis.Client`, `*redis.ClusterClient`,
`*redis.Ring`, or what `redis.NewUniversalClient` returns), not a type that
wraps it. With a wrapper, the presence scan behind the subscription API
loses its Cluster and Ring routing for stream names containing glob
characters (`\ * ? [ ]`). The default `history_replay_concurrency`, which is
derived from the pool size, also falls back to 20.

A log handler (`WithLogger`, or a subscriber's logger) or a codec that
panics is unsupported: the transport does not recover the panic, so a
subscriber being added when it happens can stay listed.

## Glossary

Terms that recur in the docs and operational guidance:

- **PEL** — *Pending Entries List*. Per-consumer-group queue inside a Redis
  Stream of entries that have been delivered but not yet `XACK`'d. Mercure
  uses one consumer per hub instance; PEL size is a backpressure signal.
- **`XADD` / `XREADGROUP` / `XACK` / `XRANGE` / `XTRIM`** — Redis Stream
  commands:
  - `XADD` publishes.
  - `XREADGROUP` consumes within a consumer group (the listener's read path).
  - `XACK` confirms processing.
  - `XRANGE` is the history-replay path.
  - `XTRIM MINID` is how the GC bounds stream size at the event-TTL boundary.
- **CMD vs CME** — *Cluster Mode Disabled* (single primary + replicas;
  AWS ElastiCache "cluster mode disabled") vs *Cluster Mode Enabled*
  (sharded across N primaries). The transport supports both, but a single
  `stream` only lives on one shard — see [Choosing a topology](docs/configuration.md#choosing-a-topology).
- **Hash-tagged keys** — Redis Cluster slot-pinning syntax: `{key}` braces
  force a key to a deterministic slot. The transport uses `{streamName}`
  on auxiliary keys (presence, lastEventID) so `MGET` and pipelined ops
  stay slot-local on a Cluster.
- **UUIDv7** — time-ordered UUID variant ([RFC 9562](https://datatracker.ietf.org/doc/rfc9562/)).
  Event IDs the hub assigns are UUIDv7 URNs; their leading 48 bits encode the
  publish millisecond timestamp, which history replay uses as the XRANGE
  start (minus `clock_skew_margin`) into the stream's `<ms>-<seq>` IDs. A
  publisher-chosen ID that is not a UUIDv7 (a UUIDv4, or any other string) is
  found by scanning from the start of the stream.

## Task reference

Run `task --list` for every task. From the repo root, prefix each task
below with `rt:`.

```sh
# Tests
task test                       # miniredis unit + integration (race + coverage); fails below the coverage floor (RT_COVERAGE_FLOOR in Taskfile.yml)
task test:all                   # + caddy submodule (fails below RT_CADDY_COVERAGE_FLOOR in Taskfile.yml)
task test:short                 # no race detector
task test:race:repeat           # -race -count=5 schedule-stress (this module)
task test:race:repeat:caddy     # caddy submodule
task test:race:repeat:all       # both modules
task bench / task bench:profile # benchmarks; the profile task writes to ./profiles
task cover                      # HTML coverage report

# Real Redis/Valkey/Cluster (build tag real_redis; auto spin-up + tear-down)
task test:real:{redis,valkey}             # TestReal_*
task test:real:stress:{redis,valkey}      # concurrent-churn stress
task test:real:multihub:{redis,valkey}    # cross-node invariants
task test:real:cluster                    # TestReal_Cluster_* against 3-master cluster
task test:real:matrix                     # redis + valkey + cluster, sequentially

# Quality
task fmt | fmt:check            # golangci-lint fmt; check is non-mutating
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
# Runs the hub natively on :80/:443/:2019; stop any other local stack holding
# those ports first, or the run aborts with a port-in-use message.
# Also runs inside `task release:check`.
task conformance                # full pipeline; defer cleans up Redis + hub, keeps the hub log only on failure
task conformance:{install,run}  # install deps / run Playwright against a running hub
task conformance:selftest       # unit-test the start/stop/wait helper itself
```

The multi-hub suite covers horizontal-scale invariants that a single
transport cannot exercise. These include cross-node dispatch, cluster-wide
`GetSubscribers`, presence heartbeat visibility, zombie GC across hubs, close
isolation, concurrent publish, startup races, racing `XGROUP DESTROY`, and
history replay crossing the page boundary. Each test uses two distinct
`redis.Client` pools so connection-level bugs surface. Multi-host
invariants (UUIDv7 clock skew, half-open TCP) are not covered by the
automated tests.

Tooling tasks declare `preconditions:` checks, so missing tools print an
install hint instead of a raw shell error. `task ci` never modifies the tree:
`fmt:check` exits non-zero on a formatting diff.

## Compatibility

| Server | Min Version | Tested With |
|---|---|---|
| [Redis](https://redis.io) | 6.2 | 7.2, 7.4 |
| [Valkey](https://valkey.io) | 7.2 | 8.0, 8.1 |

The transport uses `INFO server` at startup to detect the server type. Valkey emits
`valkey_version:` in its INFO output (alongside `redis_version:` for wire compatibility).
The transport prioritizes `valkey_version:` when present, correctly identifying the server.

## Capacity planning

No universal numbers apply, so characterize against your deployment using
the [metrics](docs/metrics.md#metrics). Provision two independent ceilings separately:

1. **Steady-state subscriber count per hub** — track via
   `sum by (instance) (mercure_redis_shard_subscribers)`.
2. **Subscriber accept rate** — `WithSubscriberRateLimit(0)` (disabled)
   by default; enable and tune to measured steady capacity before
   setting client ramp rates.

### Go runtime environment

Set `GOMEMLIMIT` to about 80-90% of the container or host memory limit,
leaving headroom for the runtime and sidecars, so the garbage collector
works harder before the process is OOM-killed. Pair it with `GOGC` tuning if
profiling shows allocation-rate pressure.

### Signals to watch during ramp-up and ramp-down

| Pattern | Primary metric | Secondary |
|---|---|---|
| Accept-rate saturation (TLS handshake) | `mercure_redis_add_subscriber_duration_seconds` p99 | `mercure_subscribers_connected` growth vs client injection rate |
| Transport lock contention under churn | `mercure_redis_add_subscriber_duration_seconds` p99 + `mercure_redis_remove_subscriber_duration_seconds` p99 | `mercure_redis_dispatch_duration_seconds` p99 |
| Heap retention after drain | `go_memstats_heap_inuse_bytes - go_memstats_heap_released_bytes` | `go_memstats_next_gc_bytes` vs `GOMEMLIMIT` |
| Zombie GC churn | `mercure_redis_zombie_gc_errors_total` | `mercure_redis_consumer_groups` (expected = replica count; sustained excess = zombies; 0 = nobody reading) |
| Misconfigured stream (published, but nobody reads it) | `mercure_redis_consumer_groups == 0` | Non-zero `rate(mercure_redis_publish_duration_seconds_count[5m])` |
| Retention tightening before clients miss | `mercure_redis_history_window_seconds` falling below expected reconnect window | `rate(mercure_redis_history_replay_fallback_total[15m])` (reactive companion) |
| Fan-out without consumers | `rate(mercure_redis_dispatch_zero_match_total[15m])` / `rate(mercure_redis_publish_duration_seconds_count[15m])` ratio | Fleet replica count multiplies the wasted I/O |
| Presence payload growth | `mercure_redis_presence_payload_bytes` and `rate(mercure_redis_presence_fallback_total[15m])` | Crossing `presence_detail_threshold` (default 1 000) or `presence_detail_byte_threshold` (default 512 KiB) flips to summary mode |
| Redis I/O saturation | `mercure_redis_client_pool_timeouts_total` | `_misses_total` rate vs `_hits_total` rate |
| Hub-side write back-pressure | `mercure_subscribe_write_flush_seconds{outcome="ok"}` p99 | `mercure_redis_xreadgroup_latency_seconds` p99 (transport-side) |

### Known ramp-down behavior

Mass disconnects (reverse-proxy failover, load-test ramp-down, client-fleet
cold-restart) serialize on the transport's subscriber lock in
`RemoveSubscriber`. A short CPU spike with elevated
`remove_subscriber_duration_seconds` p99 is expected until the drain
completes.

## License

This module is licensed under the [GNU Affero General Public License,
version 3 or any later version](LICENSE) (SPDX:
[`AGPL-3.0-or-later`](https://spdx.org/licenses/AGPL-3.0-or-later.html)).
The license matches the upstream Mercure hub.

Running this transport as part of a publicly exposed Mercure deployment
triggers AGPL §13 (Remote Network Interaction). If you modify the
transport (or any AGPL-covered component) and operate it as a network
service, you must offer the modified source to your users, typically
via a public link served from the deployment itself.

See [`NOTICE.md`](NOTICE.md) for upstream attribution,
[`SECURITY.md`](SECURITY.md) for vulnerability reporting, and
[`CONTRIBUTING.md`](CONTRIBUTING.md) for the contribution flow.
