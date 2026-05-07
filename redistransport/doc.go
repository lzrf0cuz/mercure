// Package redistransport implements a [Redis Streams] / [Valkey] transport
// for the [Mercure] hub.
//
// It provides multi-node fan-out, history replay, presence tracking,
// Prometheus metrics, sharded dispatch, and rate limiting — all backed
// by Redis or Valkey Streams (XADD/XREADGROUP).
//
// # Architecture
//
// Each hub node owns a per-node consumer group on a shared Redis Stream.
// Dispatch always flows through the stream — including to the publishing
// node's own subscribers — so every node sees events in the same
// server-canonical order. Three background goroutines maintain
// operational state: presence heartbeat, health PINGs, and an optional
// periodic zombie-group GC.
//
// Above [WithDispatchShards] = 1, the per-node subscriber set is
// hash-partitioned across worker goroutines so high-fan-out updates do
// not serialize behind a single match pass. Within each shard, dispatch
// narrows candidates through a per-shard exact-topic index rather than a
// full subscriber scan, which keeps matching O(matches) for distinct
// per-topic updates instead of O(subscribers). The README contains
// rendered Mermaid diagrams for the publish→fan-out path, sharded
// dispatch, shutdown ordering, and the presence/zombie-GC subsystem.
//
// # Quick Start
//
//	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
//
//	t, err := redistransport.NewRedisTransport(
//		client,
//		redistransport.WithStreamName("mercure"),
//	)
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer func() {
//		if err := t.Close(ctx); err != nil {
//			log.Printf("redistransport close: %v", err)
//		}
//	}()
//
// The transport implements [mercure.Transport], [mercure.TransportSubscribers],
// [mercure.TransportTopicSelectorStore], [mercure.TransportCodec], and
// [mercure.TransportHealthChecker].
//
// # Caddy Integration
//
// A Caddy module is provided in the caddy/ subdirectory. Import it with:
//
//	import _ "github.com/lzrf0cuz/mercure/redistransport/caddy"
//
// See the README for the categorized Caddyfile directive reference and
// the [Architecture] section for diagrams of the dispatch and presence
// subsystems.
//
// # Spec compliance
//
// The [Mercure protocol] specifies HTTP + SSE + JWS; transports are a
// hub-internal concern. The transport-shaped constraint comes from
// §Reconciliation: subscribers may supply a Last-Event-ID (header or
// `lastEventID` query parameter) to request replay of newer events, and
// the reserved sentinel "earliest" requests replay from the start of
// available history. The transport honors mercure.EarliestLastEventID by
// issuing XRANGE from cursor "-" (Redis's "from beginning of stream"
// primitive). The Last-Event-ID response header is set by the hub layer,
// not the transport.
//
// [Redis Streams]: https://redis.io/docs/latest/develop/data-types/streams/
// [Valkey]: https://valkey.io
// [Mercure]: https://github.com/dunglas/mercure
// [Mercure protocol]: https://datatracker.ietf.org/doc/html/draft-dunglas-mercure
// [Architecture]: https://github.com/lzrf0cuz/mercure/blob/fork/main/redistransport/README.md#architecture
package redistransport
