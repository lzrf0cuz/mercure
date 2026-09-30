//go:build real_redis

package redistransport

// Helpers shared by the real-server integration tests (single-node, cluster
// and multi-hub).

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dunglas/mercure"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// probeRealRedis pings the server at realRedisAddr() and returns the address,
// skipping the test when no server is reachable.
func probeRealRedis(t *testing.T) string {
	t.Helper()

	addr := realRedisAddr()

	probe := redis.NewClient(&redis.Options{
		Addr:        addr,
		DialTimeout: 2 * time.Second,
		ReadTimeout: 2 * time.Second,
	})
	defer probe.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := probe.Ping(ctx).Err(); err != nil {
		t.Skipf("real Redis not reachable at %s: %v", addr, err)
	}

	return addr
}

// cleanupStreamKeys deletes the stream, its lastEventID key, and every
// matching presence key so tests are idempotent under -count=1. Idempotent
// on an already-empty namespace. Connection errors during cleanup are
// swallowed: the shared compose stack outlives any single test's Cleanup
// window.
//
// Redis Streams semantics: DEL on the stream key also destroys every
// consumer group attached to it (no separate XGROUP DESTROY needed).
func cleanupStreamKeys(addr, streamName string) {
	cleanup := redis.NewClient(&redis.Options{
		Addr:        addr,
		DialTimeout: 2 * time.Second,
	})
	defer cleanup.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	toDelete := []string{
		"{" + streamName + "}",
		"{" + streamName + "}:lastEventID",
	}

	iter := cleanup.Scan(ctx, 0, key(escapeRedisGlob(streamName), ":presence:*"), 100).Iterator()
	for iter.Next(ctx) {
		toDelete = append(toDelete, iter.Val())
	}

	_, _ = cleanup.Del(ctx, toDelete...).Result()
}

// deleteIsolatedStreams deletes the stream key of every isolatedStreamNames
// stream under prefix. Each transport's Close already removed its presence key
// and consumer group; the keys are deleted one by one, as they sit on
// different Cluster slots.
func deleteIsolatedStreams(client redis.Cmdable, prefix string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, name := range isolatedStreamNames {
		_ = client.Del(ctx, key(prefix+name, "")).Err()
	}
}

// buildRealTransport wires a pre-built UniversalClient into a transport with
// the standard real-server test defaults (unique streamName, disabled
// background timers, real INFO path). Returns the transport and the
// streamName so callers can compose their own t.Cleanup with backend-
// specific key cleanup.
//
// The single-node (newRealTransport) and cluster (newRealClusterTransport)
// helpers share it. Only client construction and cleanup differ between them.
func buildRealTransport(t *testing.T, client redis.UniversalClient, opts ...Option) (*RedisTransport, string) {
	t.Helper()

	streamName := fmt.Sprintf("mercure-it-%s-%d", t.Name(), time.Now().UnixNano())

	defaultOpts := []Option{
		WithLogger(testLogger()),
		WithStreamName(streamName),
		WithXReadBlock(50 * time.Millisecond),
		WithHealthInterval(24 * time.Hour),
		WithPresenceInterval(noHeartbeatInterval),
		withSkipPresenceIntervalCheck(),
	}
	// Does not use withSkipVersionCheck, so the real INFO path runs.
	transport, err := NewRedisTransport(client, append(defaultOpts, opts...)...)
	require.NoError(t, err)

	return transport, streamName
}

// newProbeClient returns a short-lived redis.Client that t.Cleanup closes.
// Tests that query the server directly (such as XInfoGroups, or XGroupCreate
// for synthetic orphans) use it to avoid a deferred Close.
func newProbeClient(t *testing.T, addr string) *redis.Client {
	t.Helper()

	c := redis.NewClient(&redis.Options{
		Addr:        addr,
		DialTimeout: 2 * time.Second,
	})

	t.Cleanup(func() { _ = c.Close() })

	return c
}

// runConcurrentStressLoop drives a 500ms churn window of concurrent
// AddSubscriber, Dispatch and RemoveSubscriber operations against the given
// transport, then asserts Close returns cleanly within the budget.
//
// The single-node and cluster stress tests share it, so go-redis's different
// connection-pool and topology-refresh behavior under load runs against the
// same locking and shutdown sequence. The testBudget timeout and the
// Close-duration assertion detect a deadlock. The `-race` flag detects a data
// race.
//
// The remover snapshots the shard lists first and calls RemoveSubscriber
// outside the walk. Removing inside walkAllSubscribers would take the shard
// list's write lock while the walk holds its read lock.
func runConcurrentStressLoop(t *testing.T, transport *RedisTransport) {
	t.Helper()

	tss := testTopicMatcherStore()

	const testBudget = 30 * time.Second

	ctx, cancel := context.WithTimeout(context.Background(), testBudget)
	defer cancel()

	done := make(chan struct{})

	var (
		addsStarted    atomic.Int64
		dispatchCalled atomic.Int64
	)

	var wg sync.WaitGroup

	const adders = 4

	for range adders {
		wg.Go(func() {
			for {
				select {
				case <-done:
					return
				default:
				}

				addsStarted.Add(1)

				s := mercure.NewLocalSubscriber("", testLogger(), tss)
				s.SetMatchers(topicMatchers([]string{"https://example.com/stress"}), nil)

				_ = transport.AddSubscriber(ctx, s)
			}
		})
	}

	const dispatchers = 2

	for range dispatchers {
		wg.Go(func() {
			for i := 0; ; i++ {
				select {
				case <-done:
					return
				default:
				}

				_ = transport.Dispatch(ctx, &mercure.Update{
					Topics: []string{"https://example.com/stress"},
					Data:   fmt.Sprintf("stress-msg-%d", i),
				})

				dispatchCalled.Add(1)
			}
		})
	}

	wg.Go(func() {
		for {
			select {
			case <-done:
				return
			default:
			}

			var snapshot []*mercure.LocalSubscriber

			transport.walkAllSubscribers(func(s *mercure.LocalSubscriber) bool {
				snapshot = append(snapshot, s)

				return true
			})

			for _, s := range snapshot {
				_ = transport.RemoveSubscriber(ctx, s)
			}

			time.Sleep(time.Millisecond)
		}
	})

	time.Sleep(500 * time.Millisecond)
	close(done)
	wg.Wait()

	closeStart := time.Now()
	closeErr := transport.Close(context.Background())
	closeDur := time.Since(closeStart)

	require.NoError(t, closeErr, "Close should return no error under concurrent churn")
	assert.Less(t, closeDur, testBudget,
		"Close must complete within the stress budget — hang indicates deadlock")

	assert.Positive(t, addsStarted.Load(), "adders must have run")
	assert.Positive(t, dispatchCalled.Load(), "dispatchers must have run")

	// Post-close calls must cleanly error out.
	s := mercure.NewLocalSubscriber("", testLogger(), tss)
	s.SetMatchers(topicMatchers([]string{"https://example.com/stress"}), nil)

	err := transport.AddSubscriber(context.Background(), s)
	require.ErrorIs(t, err, ErrClosedTransport)

	err = transport.Dispatch(context.Background(), &mercure.Update{
		Topics: []string{"https://example.com/stress"},
		Data:   "post-close",
	})
	require.ErrorIs(t, err, ErrClosedTransport)
}

// waitForStreamCatchUp blocks until hub's xreadgroupListener has advanced
// lastDispatchedStreamID to the stream's current tail (as queried via the
// probe). History-replay tests that call AddSubscriber right after a tight
// publish loop need it. AddSubscriber snapshots lastDispatchedStreamID into
// toStreamID under t.mu.Lock, and if the hub's XREADGROUP is still
// mid-catchup, the snapshot is too low. Pass 1 (scanForEventID) then misses
// the requested ID and Pass 2 (replayAll) runs, which delivers events the
// test did not intend to replay.
//
// It takes a redis.UniversalClient so single-node and cluster tests share one
// helper: XRevRangeN is on the cmdable interface that both *redis.Client and
// *redis.ClusterClient implement.
func waitForStreamCatchUp(t *testing.T, hub *RedisTransport, probe redis.UniversalClient) {
	t.Helper()

	streamKey := "{" + hub.opts.streamName + "}"

	require.Eventually(t, func() bool {
		tail, err := probe.XRevRangeN(context.Background(), streamKey, "+", "-", 1).Result()
		if err != nil || len(tail) == 0 {
			return false
		}

		return *hub.lastDispatchedStreamID.Load() == tail[0].ID
	}, 10*time.Second, 20*time.Millisecond,
		"hub did not catch up to the stream tail within 10s")
}
