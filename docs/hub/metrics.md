# Metrics

The Mercure.rocks Hub relies on [Caddy's metrics](https://caddyserver.com/docs/metrics) and extends them with Mercure-specific collectors.

Enable Caddy's Prometheus scrape endpoint as described in the [Caddy documentation](https://caddyserver.com/docs/metrics). In addition to Caddy's HTTP, filesystem, and reverse-proxy metrics, the Hub registers the following collectors:

| Name                            | Type    | Description                                               |
| ------------------------------- | ------- | --------------------------------------------------------- |
| `mercure_subscribers_total`     | Counter | Total number of subscribers handled since the hub started |
| `mercure_subscribers_connected` | Gauge   | Current number of connected subscribers                   |
| `mercure_updates_total`         | Counter | Total number of updates dispatched since the hub started  |
| `mercure_updates_failed_total`  | Counter | Updates that failed to publish, labeled `reason` (`validation` \| `transport` \| `timeout`). Counts validation + dispatch failures (not HTTP-layer auth/parse rejections); pair with `mercure_updates_total` for a success/failure ratio |
| `mercure_subscriber_disconnects_total` | Counter | Subscriber disconnections labeled `reason` (`client_closed` \| `hub_shutdown` \| `write_timeout` \| `write_failed` \| `transport_ended` \| `transport_error` \| `unknown`) for cause-tagged churn |
| `mercure_subscribe_write_flush_seconds` | Histogram | Per-event SSE write+flush latency labeled `outcome` (`ok` \| `failed`) |
| `mercure_claim_header_rejected_total` | Counter | Requests rejected by a [`require_claim_header`](config.md#binding-a-claim-to-a-header) binding, labeled `binding` (`claim:Canonical-Header`, e.g. `tenants:Tenant-Id`) and `reason` (`header_absent` \| `claim_absent` \| `mismatch` \| `malformed`). Cardinality is bounded: `binding` comes from the configuration and `reason` from a closed set, so neither is request-controlled. Authorization failures are otherwise only visible in debug logs |
