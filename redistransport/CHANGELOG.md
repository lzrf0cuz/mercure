# Changelog

All notable changes to this transport. Format:
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), with
[semantic versioning](https://semver.org/). `0.1.0` runs on the Mercure 1.0
hub. Wire-format breaks ship with migration notes in the corresponding
release.

## [0.1.0] - 2026-10-02

Runs on the Mercure 1.0 hub (upstream v1.0.2) instead of v0.24.2.

### Upgrading

- Moving to 1.0 is a flag-day cutover onto fresh Redis streams: stop every
  0.x hub, then start the 1.0 hubs on a new `stream` name, or delete the old
  stream, its `lastEventID` key and its `presence:*` keys first. 0.x and 1.0
  hubs cannot share a stream, so there is no rolling upgrade: 1.0 rejects
  some event `id`/`type` values that 0.x accepted and drops 0.x entries that
  fail its character check, and presence entries and subscription events
  changed shape. See the README section "Upgrading from a Mercure 0.x hub".
- **Breaking:** numeric and boolean directives (`max_length`,
  `dispatch_shards`, the rate limits and bursts, `db`, the pool knobs, `tls`,
  `tls_insecure_skip_verify`, …) no longer accept `{env.VAR}` placeholders;
  such a value now fails at config load. Write `"{$VAR}"` quoted (so an unset
  variable fails startup) or `{$VAR:default}`, or use a literal. String,
  secret and duration directives still accept `{env.VAR}`.
- **Breaking:** the bare `gob` directive is removed; write `encoding gob`.
- A JSON config adapted from a Caddyfile by a 0.x release and carrying a
  `raw_numeric` or `raw_bool` field fails to load: re-adapt it from the
  Caddyfile.
- Several Redis hubs in one Caddy config must use distinct `stream` names;
  hubs on one stream see each other's updates and subscribers.

### Changed

- `Mercure-Last-Event-ID` on history replay now follows the protocol: the
  requested ID when the stream holds it, otherwise `earliest`. It used to be
  the last stream entry scanned.
- Presence entries in Redis no longer carry subscribers' access-token claims,
  private topics or requested `Last-Event-ID`; the subscription API output is
  unchanged.
- An update whose encoded form exceeds 64 MiB is rejected at publish with
  `413` instead of being stored and then dropped by every node. It counts as
  `mercure_updates_failed_total{reason="validation"}`, not as a transport
  publish error.
- Stream entries whose `id` or `type` is not a valid SSE field value under
  1.0's rules (invalid UTF-8, a control character, or a Unicode format
  character) are dropped and counted in
  `mercure_redis_stream_decode_errors_total{kind="forbidden_sse_chars"}`.
  0.x dropped only CR, LF and NUL.
- `history_replay_concurrency` defaults to half the go-redis pool size, at
  least 1 and at most 20, instead of a fixed 20.
- `subscriber_rate_limit` is enforced only by the admission gate, which sheds
  over-rate HTTP subscribers with `429`; a programmatic `AddSubscriber` call
  is no longer rate limited.

### Removed

- Metrics: `mercure_redis_pre_binding_metric_drops_total`,
  `mercure_redis_history_cursor_rejected_total`,
  `mercure_redis_subscriber_rate_limited_total`,
  `mercure_redis_presence_read_skipped_total`, and the
  `reason="summary_marshal_failed"` series of
  `mercure_redis_presence_fallback_total`. Their panels are gone from the
  bundled transport dashboard.
- `WithSubscriberListCacheSize` (Go API). The hub's
  `subscriber_list_cache_size` directive has no effect with this transport,
  which matches through a per-shard topic index.

### Hub

Hub-side changes released with this version:

- **Breaking:** `require_claim_header` answers a missing, empty or repeated
  bound header with `400`, and a claim that does not authorize the header
  with `403` (`insufficient_scope`); an unusable claim stays `401`.
- `require_claim_value <claim> <value...>` requires a token claim to hold
  one of the listed values.
- `count_subscribers` on a `require_claim_header` binding counts connected
  subscribers per header value in `mercure_subscribers_by_binding_value`.

[0.1.0]: https://github.com/lzrf0cuz/mercure/releases/tag/redistransport%2Fv0.1.0
