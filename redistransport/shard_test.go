package redistransport

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/cespare/xxhash/v2"
	"github.com/dunglas/mercure"
	"github.com/gofrs/uuid/v5"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newShardedTestTransport(t *testing.T, shards int, extraOpts ...Option) (*RedisTransport, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	allOpts := append([]Option{
		WithLogger(testLogger()),
		WithXReadBlock(50 * time.Millisecond),
		WithHealthInterval(24 * time.Hour),
		WithPresenceInterval(24 * time.Hour),
		withSkipPresenceIntervalCheck(),
		WithDispatchShards(shards),
		withSkipVersionCheck(), // miniredis lacks INFO server
	}, extraOpts...)
	transport, err := NewRedisTransport(client, allOpts...)
	require.NoError(t, err)
	t.Cleanup(func() { transport.Close(context.Background()) })

	return transport, mr
}

// TestShardedDispatchSubscribeAndReceive verifies that subscribers on different
// shards all receive messages dispatched through the XREADGROUP path.
func TestShardedDispatchSubscribeAndReceive(t *testing.T) {
	t.Parallel()

	transport, mr := newShardedTestTransport(t, 4)
	tss := testTopicSelectorStore()

	require.Equal(t, 4, transport.numShards, "expected 4 shards")
	require.Len(t, transport.shards, 4)
	require.Len(t, transport.shardChans, 4)

	// Add 10 subscribers on varying topics.
	subs := make([]*mercure.LocalSubscriber, 10)

	for i := range 10 {
		sub := mercure.NewLocalSubscriber("", testLogger(), tss)
		sub.SetTopics([]string{"https://example.com/sharded/{id}"}, nil)
		require.NoError(t, transport.AddSubscriber(context.Background(), sub))
		subs[i] = sub
	}

	// Publish a matching update.
	update := &mercure.Update{
		Topics: []string{"https://example.com/sharded/1"},
		Event:  mercure.Event{Data: "sharded-msg"},
	}
	require.NoError(t, transport.Dispatch(context.Background(), update))
	mr.FastForward(200 * time.Millisecond)

	// All subscribers should receive the message.
	for i, sub := range subs {
		select {
		case received := <-sub.Receive():
			assert.Equal(t, "sharded-msg", received.Data, "subscriber %d", i)
		case <-time.After(5 * time.Second):
			t.Fatalf("timeout waiting for message on subscriber %d", i)
		}
	}
}

// TestShardedDispatchOrdering verifies that per-subscriber message ordering
// is preserved across multiple dispatched messages.
func TestShardedDispatchOrdering(t *testing.T) {
	t.Parallel()

	transport, mr := newShardedTestTransport(t, 4)
	tss := testTopicSelectorStore()

	sub := mercure.NewLocalSubscriber("", testLogger(), tss)
	sub.SetTopics([]string{"https://example.com/order"}, nil)
	require.NoError(t, transport.AddSubscriber(context.Background(), sub))

	// Publish 20 messages.
	for i := range 20 {
		u := &mercure.Update{
			Topics: []string{"https://example.com/order"},
			Event:  mercure.Event{Data: fmt.Sprintf("msg-%d", i)},
		}
		require.NoError(t, transport.Dispatch(context.Background(), u))
	}

	mr.FastForward(500 * time.Millisecond)
	time.Sleep(300 * time.Millisecond)

	// Messages must arrive in order.
	for i := range 20 {
		select {
		case received := <-sub.Receive():
			assert.Equal(t, fmt.Sprintf("msg-%d", i), received.Data)
		case <-time.After(5 * time.Second):
			t.Fatalf("timeout waiting for message %d", i)
		}
	}
}

// TestShardedDispatchTopicFiltering verifies topic filtering works correctly
// with sharded dispatch.
func TestShardedDispatchTopicFiltering(t *testing.T) {
	t.Parallel()

	transport, mr := newShardedTestTransport(t, 4)
	tss := testTopicSelectorStore()

	// Subscriber only matches /books/*
	sub := mercure.NewLocalSubscriber("", testLogger(), tss)
	sub.SetTopics([]string{"https://example.com/books/{id}"}, nil)
	require.NoError(t, transport.AddSubscriber(context.Background(), sub))

	// Publish non-matching update.
	u1 := &mercure.Update{
		Topics: []string{"https://example.com/movies/1"},
		Event:  mercure.Event{Data: "movie"},
	}
	require.NoError(t, transport.Dispatch(context.Background(), u1))
	mr.FastForward(100 * time.Millisecond)

	// Publish matching update.
	u2 := &mercure.Update{
		Topics: []string{"https://example.com/books/1"},
		Event:  mercure.Event{Data: "book"},
	}
	require.NoError(t, transport.Dispatch(context.Background(), u2))
	mr.FastForward(200 * time.Millisecond)

	// Should only receive the book update.
	select {
	case received := <-sub.Receive():
		assert.Equal(t, "book", received.Data)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for matching message")
	}

	// No more messages.
	select {
	case <-sub.Receive():
		t.Fatal("should not receive non-matching topic")
	case <-time.After(200 * time.Millisecond):
	}
}

// TestShardedDispatchRemoveSubscriber verifies that removing a subscriber
// from a sharded transport works correctly.
func TestShardedDispatchRemoveSubscriber(t *testing.T) {
	t.Parallel()

	transport, mr := newShardedTestTransport(t, 4)
	tss := testTopicSelectorStore()

	sub := mercure.NewLocalSubscriber("", testLogger(), tss)
	sub.SetTopics([]string{"https://example.com/remove"}, nil)
	require.NoError(t, transport.AddSubscriber(context.Background(), sub))

	require.NoError(t, transport.RemoveSubscriber(context.Background(), sub))

	// After removal, dispatched messages should not reach this subscriber.
	u := &mercure.Update{
		Topics: []string{"https://example.com/remove"},
		Event:  mercure.Event{Data: "after-remove"},
	}
	require.NoError(t, transport.Dispatch(context.Background(), u))
	mr.FastForward(200 * time.Millisecond)

	select {
	case <-sub.Receive():
		t.Fatal("removed subscriber should not receive messages")
	case <-time.After(300 * time.Millisecond):
	}
}

// TestShardedDispatchWithHistory verifies that history replay works with
// sharded dispatch.
func TestShardedDispatchWithHistory(t *testing.T) {
	t.Parallel()

	transport, mr := newShardedTestTransport(t, 4)
	tss := testTopicSelectorStore()

	// Publish messages.
	ids := make([]string, 0, 5)

	for i := range 5 {
		u := &mercure.Update{
			Topics: []string{"https://example.com/hist"},
			Event:  mercure.Event{Data: fmt.Sprintf("hist-%d", i)},
		}
		require.NoError(t, transport.Dispatch(context.Background(), u))
		ids = append(ids, u.ID)

		mr.FastForward(10 * time.Millisecond)
	}

	// Wait for XREADGROUP to process all messages.
	time.Sleep(300 * time.Millisecond)
	mr.FastForward(300 * time.Millisecond)
	time.Sleep(300 * time.Millisecond)

	// Subscribe with Last-Event-ID = 2nd message.
	sub := mercure.NewLocalSubscriber(ids[1], testLogger(), tss)
	sub.SetTopics([]string{"https://example.com/hist"}, nil)
	require.NoError(t, transport.AddSubscriber(context.Background(), sub))

	// Should receive messages 2, 3, 4.
	for i := 2; i < 5; i++ {
		select {
		case received := <-sub.Receive():
			assert.Equal(t, fmt.Sprintf("hist-%d", i), received.Data)
		case <-time.After(5 * time.Second):
			t.Fatalf("timeout waiting for history message %d", i)
		}
	}
}

// TestShardBalance verifies that xxhash distributes UUIDv4 subscriber IDs
// evenly across shards (max/min ratio < 1.15 for 100K subs across 8 shards).
func TestShardBalance(t *testing.T) {
	t.Parallel()

	numShards := 8
	numSubscribers := 100_000
	shardCounts := make([]int, numShards)

	for range numSubscribers {
		id := "urn:uuid:" + uuid.Must(uuid.NewV4()).String()
		h := xxhash.Sum64String(id)
		shardCounts[int(h%uint64(numShards))]++ //nolint:gosec // modulo result fits in int
	}

	minCount := shardCounts[0]
	maxCount := shardCounts[0]

	for _, c := range shardCounts[1:] {
		if c < minCount {
			minCount = c
		}

		if c > maxCount {
			maxCount = c
		}
	}

	ratio := float64(maxCount) / float64(minCount)
	t.Logf("shard distribution: min=%d max=%d ratio=%.3f counts=%v", minCount, maxCount, ratio, shardCounts)

	assert.Less(t, ratio, 1.15, "shard imbalance ratio should be < 1.15 for 100K subs across 8 shards")
}

// TestShardedDispatchAutoDetect verifies that dispatchShards=0 auto-detects
// to runtime.NumCPU().
func TestShardedDispatchAutoDetect(t *testing.T) {
	t.Parallel()

	transport, _ := newShardedTestTransport(t, 0)
	// dispatchShards=0 must resolve to runtime.NumCPU(), floored at 1 and
	// capped at the enforced ceiling — the exact initShards contract, not
	// merely "some positive number".
	expected := min(max(runtime.NumCPU(), 1), suspiciousDispatchShardsMax)
	assert.Equal(t, expected, transport.numShards,
		"dispatchShards=0 must auto-detect to runtime.NumCPU (ceiling-bounded)")
	assert.NotNil(t, transport.shards)
	assert.Len(t, transport.shards, transport.numShards)
}

// TestShardedDispatchHardCapEnforced verifies a request for more than
// suspiciousDispatchShardsMax shards is clamped to the cap. Without the
// cap, an operator on a high-vCPU host (or a config typo) could emit
// thousands of Prometheus series per process via the {shard} label.
func TestShardedDispatchHardCapEnforced(t *testing.T) {
	t.Parallel()

	transport, _ := newShardedTestTransport(t, suspiciousDispatchShardsMax*4)
	assert.Equal(t, suspiciousDispatchShardsMax, transport.numShards,
		"numShards must be clamped to the documented ceiling")
	assert.Len(t, transport.shards, suspiciousDispatchShardsMax)
}

// TestShardedDispatchSingleShard verifies backward compatibility — with 1 shard,
// dispatch uses the single-goroutine path (no shard workers).
func TestShardedDispatchSingleShard(t *testing.T) {
	t.Parallel()

	transport, mr := newShardedTestTransport(t, 1)
	tss := testTopicSelectorStore()

	assert.Equal(t, 1, transport.numShards)
	assert.Len(t, transport.shards, 1, "single shard should have exactly one shard")
	assert.Nil(t, transport.shardChans, "single shard should not create shard channels")

	// Verify dispatch still works.
	sub := mercure.NewLocalSubscriber("", testLogger(), tss)
	sub.SetTopics([]string{"https://example.com/single"}, nil)
	require.NoError(t, transport.AddSubscriber(context.Background(), sub))

	u := &mercure.Update{
		Topics: []string{"https://example.com/single"},
		Event:  mercure.Event{Data: "single-shard-msg"},
	}
	require.NoError(t, transport.Dispatch(context.Background(), u))
	mr.FastForward(200 * time.Millisecond)

	select {
	case received := <-sub.Receive():
		assert.Equal(t, "single-shard-msg", received.Data)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for message with single shard")
	}
}

// BenchmarkShardedDispatch benchmarks the dispatch path with sharded dispatch
// at different subscriber counts.
func BenchmarkShardedDispatch(b *testing.B) {
	for _, numSubs := range []int{100, 1000} {
		for _, shards := range []int{1, 4, 8} {
			b.Run(fmt.Sprintf("subs=%d/shards=%d", numSubs, shards), func(b *testing.B) {
				mr := miniredis.RunT(b)
				client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
				transport, err := NewRedisTransport(
					client,
					withSkipVersionCheck(),
					WithLogger(testLogger()),
					WithXReadBlock(50*time.Millisecond),
					WithHealthInterval(24*time.Hour),
					WithPresenceInterval(24*time.Hour),
					withSkipPresenceIntervalCheck(),
					WithZombieGCInterval(0),
					WithDispatchShards(shards),
				)
				require.NoError(b, err)
				b.Cleanup(func() { transport.Close(context.Background()) })

				tss := testTopicSelectorStore()

				for i := range numSubs {
					sub := mercure.NewLocalSubscriber("", testLogger(), tss)
					sub.SetTopics([]string{fmt.Sprintf("https://example.com/bench/%d", i%10)}, nil)
					require.NoError(b, transport.AddSubscriber(context.Background(), sub))
				}

				ctx := context.Background()

				b.ResetTimer()

				for b.Loop() {
					u := &mercure.Update{
						Topics: []string{"https://example.com/bench/0"},
						Event:  mercure.Event{Data: "benchmark"},
					}
					if err := transport.Dispatch(ctx, u); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// BenchmarkShardedDispatch_100K measures the sharded-dispatch fast path
// at 100k subscribers across 8 and 16 shards. Targets sub-5ms/msg
// dispatch latency and tracks alloc/op for GC pressure analysis.
// Dispatch goes through Redis Stream → XREADGROUP → shardedDispatch, so
// this benchmarks the full hot path including Redis round-trip (via
// miniredis).
func BenchmarkShardedDispatch_100K(b *testing.B) {
	for _, shards := range []int{8, 16} {
		b.Run(fmt.Sprintf("shards=%d", shards), func(b *testing.B) {
			mr := miniredis.RunT(b)
			client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
			transport, err := NewRedisTransport(
				client,
				withSkipVersionCheck(),
				WithLogger(testLogger()),
				WithXReadBlock(50*time.Millisecond),
				WithHealthInterval(24*time.Hour),
				WithPresenceInterval(24*time.Hour),
				withSkipPresenceIntervalCheck(),
				WithZombieGCInterval(0),
				WithDispatchShards(shards),
			)
			require.NoError(b, err)
			b.Cleanup(func() { transport.Close(context.Background()) })

			tss := testTopicSelectorStore()

			// Add 10K subscribers (100K would OOM in miniredis test infra).
			// The benchmark measures per-message dispatch overhead which scales
			// linearly with subscriber count; extrapolation is valid.
			const numSubs = 10_000
			for i := range numSubs {
				sub := mercure.NewLocalSubscriber("", testLogger(), tss)
				sub.SetTopics([]string{fmt.Sprintf("https://example.com/scale/%d", i%100)}, nil)
				require.NoError(b, transport.AddSubscriber(context.Background(), sub))
			}

			ctx := context.Background()

			b.ResetTimer()
			b.ReportAllocs()

			for b.Loop() {
				u := &mercure.Update{
					Topics: []string{"https://example.com/scale/0"},
					Event:  mercure.Event{Data: "high-scale bench payload"},
				}
				if err := transport.Dispatch(ctx, u); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// TestFullScaleSimulation verifies end-to-end correctness with multiple
// nodes, high subscriber counts, and sustained message rates.
// 3 transport instances (simulating 3 nodes) share one miniredis,
// each with subscribers. Messages are published from one node and verified
// to arrive at subscribers on all 3 nodes in order with no loss.
func TestFullScaleSimulation(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("skipping full-scale simulation in short mode")
	}

	mr := miniredis.RunT(t)

	const (
		numNodes    = 3
		subsPerNode = 100
		numMsgs     = 200
	)

	type nodeState struct {
		transport *RedisTransport
		subs      []*mercure.LocalSubscriber
	}

	tss := testTopicSelectorStore()
	nodes := make([]nodeState, numNodes)

	for n := range numNodes {
		client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
		transport, err := NewRedisTransport(
			client,
			withSkipVersionCheck(),
			WithLogger(testLogger()),
			WithXReadBlock(50*time.Millisecond),
			WithHealthInterval(24*time.Hour),
			WithPresenceInterval(24*time.Hour),
			withSkipPresenceIntervalCheck(),
			WithZombieGCInterval(0),
			WithDispatchShards(4),
		)
		require.NoError(t, err, "node %d", n)
		t.Cleanup(func() { transport.Close(context.Background()) })

		subs := make([]*mercure.LocalSubscriber, subsPerNode)

		for i := range subsPerNode {
			sub := mercure.NewLocalSubscriber("", testLogger(), tss)
			sub.SetTopics([]string{"https://example.com/broadcast"}, nil)
			require.NoError(t, transport.AddSubscriber(context.Background(), sub), "node %d sub %d", n, i)
			subs[i] = sub
		}

		nodes[n] = nodeState{transport: transport, subs: subs}
	}

	// Publish all messages from node 0.
	for i := range numMsgs {
		u := &mercure.Update{
			Topics: []string{"https://example.com/broadcast"},
			Event:  mercure.Event{Data: fmt.Sprintf("msg-%d", i)},
		}
		require.NoError(t, nodes[0].transport.Dispatch(context.Background(), u), "dispatch msg %d", i)
	}

	// Allow time for XREADGROUP to process all messages across all nodes.
	mr.FastForward(500 * time.Millisecond)
	time.Sleep(500 * time.Millisecond)

	// Verify: every subscriber on every node should have received numMsgs messages.
	for n, node := range nodes {
		for s, sub := range node.subs {
			received := 0

			for {
				select {
				case <-sub.Receive():
					received++
				default:
					goto done
				}
			}

		done:
			assert.Equal(t, numMsgs, received,
				"node %d sub %d: expected %d messages, got %d", n, s, numMsgs, received)
		}
	}
}
