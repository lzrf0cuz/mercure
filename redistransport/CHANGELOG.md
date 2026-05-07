# Changelog

All notable changes to this transport. Format:
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Pre-1.0
(`0.0.x`) tracks the upstream hub's pre-1.0 status. Wire-format breaks
ship with migration notes in the corresponding release.

## [0.0.1] — 2026-07-20

Initial release: a Redis/Valkey Streams transport for the Mercure v0.24.2 hub,
with the hub-side changes it ships with. Server compatibility: Redis 6.2+ / Valkey 7.2+
(enforced at startup via `INFO server`).

### Added

- Multi-node fan-out via per-node consumer groups on a shared Redis Stream
  (XADD / XREADGROUP), with history replay (UUIDv7 timestamp seek, clock-skew
  margin, paginated XRANGE fallback). At-least-once at the join boundary — a
  subscriber joining as an event dispatches may receive it twice; clients
  reconcile via `Last-Event-ID`.
- Per-node presence: TTL'd SET keys, heartbeat, zombie consumer-group GC, and a
  summary-mode fallback above configurable count/byte thresholds.
- Sharded live dispatch with a per-shard exact-topic index — distinct-per-topic
  workloads match at O(matches) rather than O(subscribers).
- Admission control (opt-in, default-off): `subscriber_max_count`,
  `subscriber_rate_limit` (sheds over-rate SSE subscribers with `429` + jittered
  `Retry-After`), and admission/registration timeouts.
- SSE-field injection guard (CWE-93): stream entries whose decoded `id`/`type`
  carry CR/LF/NUL are dropped, and a forbidden replay `eventID` cursor is not
  advanced. Drop/reject logs are rate-sampled while per-kind counters stay
  authoritative.
- Topologies: single-instance, Sentinel, Redis Cluster (hash-tagged keys).
- Codec aliases over `mercure.Codec` (JSON default, Gob, MsgPack) and go-redis
  pool tuning (`pool_size`, `min_idle_conns`, timeouts, `max_retries`). Every
  directive accepts `{env.VAR}` placeholders.
- `TransportHealthChecker` (Ready/Live) that gates on listener health, not just
  Redis PING.
- Observability: Prometheus metrics across dispatch, listener, replay, presence,
  health, shard, and pool; an OpenTelemetry `mercure.transport.history` span; and
  bundled Grafana dashboards (transport + optional tracing).

Hub-side, shipped in the same fork snapshot:

- `require_claim_header <claim> <header>` binds a JWT claim to an HTTP header
  (`match`, `on_missing`, `roles` modifiers), rejecting mismatches with `401`.
- CORS debug traces route through the hub's structured logger at Debug level
  instead of `rs/cors`'s default `[cors]` stdout logger.

### Defaults

Deviations from library/upstream defaults, all overridable:

- `event_ttl` 0 → 1h (bounds stream growth; opt out with `event_ttl 0`).
- `history_replay_concurrency` 50 → 20 (avoids starving small go-redis pools on
  reconnect storms).
- `presence_detail_threshold` 10000 → 1000 (bounds the presence SET payload).

See the README for operator-facing detail (scaling ceiling, ACL recipe,
validation summary, backend-swap caveat, suggested alerts).

[0.0.1]: https://github.com/lzrf0cuz/mercure/releases/tag/redistransport%2Fv0.0.1
