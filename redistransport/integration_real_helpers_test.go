//go:build real_redis

package redistransport

// Shared helpers for real-server integration tests (both single-transport
// and multi-hub). Lives in its own file so neither integration_real_test.go
// nor integration_real_multihub_test.go owns the probe/cleanup primitives,
// and extending them doesn't force a diff on either test-intent file.

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

// probeRealRedis pings the server at realRedisAddr() via a short-lived client
// and returns the address. Tests should call this first — it Skips cleanly
// when no server is running, so a suite run on a laptop without the compose
// stack up does not fail, it just reports 0 ran / 0 failed / N skipped.
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
// swallowed — the shared compose stack lifetime exceeds any single test's
// Cleanup window.
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

	iter := cleanup.Scan(ctx, 0, "{"+streamName+"}:presence:*", 100).Iterator()
	for iter.Next(ctx) {
		toDelete = append(toDelete, iter.Val())
	}

	_, _ = cleanup.Del(ctx, toDelete...).Result()
}

// buildRealTransport wires a pre-built UniversalClient into a transport with
// the standard real-server test defaults (unique streamName, disabled
// background timers, real INFO path). Returns the transport and the
// streamName so callers can compose their own t.Cleanup with backend-
// specific key cleanup.
//
// Shared between single-node (newRealTransport) and cluster
// (newRealClusterTransport) helpers — only the client construction and
// cleanup pattern differs between them.
func buildRealTransport(t *testing.T, client redis.UniversalClient, opts ...Option) (*RedisTransport, string) {
	t.Helper()

	streamName := fmt.Sprintf("mercure-it-%s-%d", t.Name(), time.Now().UnixNano())

	defaultOpts := []Option{
		WithLogger(testLogger()),
		WithStreamName(streamName),
		WithXReadBlock(50 * time.Millisecond),
		WithHealthInterval(24 * time.Hour),
		WithPresenceInterval(24 * time.Hour),
		withSkipPresenceIntervalCheck(),
	}
	// Deliberately NOT using withSkipVersionCheck — we want the real INFO path.
	transport, err := NewRedisTransport(client, append(defaultOpts, opts...)...)
	require.NoError(t, err)

	return transport, streamName
}

// newProbeClient returns a short-lived redis.Client wired to t.Cleanup so
// tests issuing direct server queries (XInfoGroups, XGroupCreate for
// synthetic orphans, etc.) don't repeat the defer Close() boilerplate.
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
// AddSubscriber/Dispatch/RemoveSubscriber operations against the given
// transport, then asserts Close returns cleanly within the budget.
//
// Shared between the single-node and cluster stress tests so go-redis's
// different connection-pool / topology-refresh behavior under load is
// exercised against the same internal-locking + shutdown sequence.
// Deadlock detection = testBudget timeout + Close-duration assertion.
// Race detection = `-race` flag.
//
// The Remover snapshots shard lists first, then RemoveSubscriber runs
// sequentially outside the walk. Calling RemoveSubscriber from inside
// walkAllSubscribers would take t.mu.Lock under a caller-held RLock —
// RWMutex upgrade deadlock. Pattern kept visible here.
func runConcurrentStressLoop(t *testing.T, transport *RedisTransport) {
	t.Helper()

	tss := testTopicSelectorStore()

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
				s.SetTopics([]string{"https://example.com/stress"}, nil)

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
					Event: mercure.Event{
						Data: fmt.Sprintf("stress-msg-%d", i),
					},
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
	s.SetTopics([]string{"https://example.com/stress"}, nil)

	err := transport.AddSubscriber(context.Background(), s)
	require.ErrorIs(t, err, ErrClosedTransport)

	err = transport.Dispatch(context.Background(), &mercure.Update{
		Topics: []string{"https://example.com/stress"},
		Event:  mercure.Event{Data: "post-close"},
	})
	require.ErrorIs(t, err, ErrClosedTransport)
}

// waitForStreamCatchUp blocks until hub's xreadgroupListener has advanced
// lastDispatchedStreamID to the stream's current tail (as queried via the
// probe). Required for history-replay tests that call AddSubscriber right
// after a tight publish loop: AddSubscriber snapshots lastDispatchedStreamID
// into toStreamID under t.mu.Lock, and if hub's XREADGROUP is still
// mid-catchup the snapshot is too low. That makes Pass 1 (scanForEventID)
// miss the requested ID and trip Pass 2 (replayAll), which delivers events
// the test didn't intend to replay.
//
// Takes redis.UniversalClient so single-node and cluster tests share one
// helper — XRevRangeN is on the cmdable interface implemented by both
// *redis.Client and *redis.ClusterClient.
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
