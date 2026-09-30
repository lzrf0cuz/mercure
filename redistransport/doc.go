// Package redistransport implements a [Redis Streams] / [Valkey] transport
// for the [Mercure] hub.
//
// It provides multi-node fan-out, history replay, presence tracking,
// sharded dispatch, rate limiting and Prometheus metrics.
//
// # Architecture
//
// Each hub node owns a per-node consumer group on a shared Redis Stream.
// Dispatch always flows through the stream — including to the publishing
// node's own subscribers — so every node sees events in the same
// server-canonical order. Background goroutines send presence heartbeats and
// health PINGs and, when enabled, trim expired entries and remove consumer
// groups left by crashed nodes.
//
// With [WithDispatchShards] above 1, the per-node subscriber set is
// hash-partitioned across worker goroutines. Each shard matches updates
// through an exact-topic index, so matching costs O(matches) rather than
// O(subscribers) for per-topic updates.
//
// # Quick Start
//
//	// ContextTimeoutEnabled lets publish_timeout and the other caller
//	// deadlines cut a command already sent instead of waiting for ReadTimeout.
//	client := redis.NewClient(&redis.Options{Addr: "localhost:6379", ContextTimeoutEnabled: true})
//
//	t, err := redistransport.NewRedisTransport(
//		client,
//		redistransport.WithStreamName("mercure"),
//	)
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer func() {
//		if err := t.Close(context.Background()); err != nil {
//			log.Printf("redistransport close: %v", err)
//		}
//	}()
//
// The transport implements [mercure.Transport], [mercure.TransportSubscribers],
// [mercure.TransportTopicMatcherStore], [mercure.TransportCodec], and
// [mercure.TransportHealthChecker].
//
// A log handler or codec that panics is unsupported: the transport does not
// recover the panic. Do not register other mercure_redis_* collectors on the
// registry passed to [RedisTransport.RegisterMetricsWith] or
// [WithPrometheusRegisterer].
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
// Subscribers may send a Last-Event-ID (header or `last_event_id` query
// parameter) to replay newer events, or the reserved value "earliest" to
// replay all retained history ([Mercure protocol], §Reconciliation). The
// Mercure-Last-Event-ID response field is the requested ID when it is in the
// stream, and "earliest" when "earliest" was requested or the ID is unknown,
// trimmed or dated in the future.
//
// [Redis Streams]: https://redis.io/docs/latest/develop/data-types/streams/
// [Valkey]: https://valkey.io
// [Mercure]: https://github.com/dunglas/mercure
// [Mercure protocol]: https://datatracker.ietf.org/doc/html/draft-dunglas-mercure
// [Architecture]: https://github.com/lzrf0cuz/mercure/blob/fork/main/redistransport/README.md#architecture
package redistransport
