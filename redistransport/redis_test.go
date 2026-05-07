package redistransport

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/dunglas/mercure"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
}

// Test-only sentinel errors used by table-driven tests via errors.Is.
var (
	errXInfoGroupsMissingStream = errors.New("ERR no such key")
	errLowercaseMissing         = errors.New("no such key 'x'")
	errWrongType                = errors.New("WRONGTYPE Operation against a key")
	errCapitalizedMissing       = errors.New("ERR No such key")
)

func testTopicSelectorStore() *mercure.TopicSelectorStore {
	tss, _ := mercure.NewTopicSelectorStore(0)

	return tss
}

func newTestTransport(t *testing.T, opts ...Option) (*RedisTransport, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	allOpts := slices.Concat([]Option{
		WithLogger(testLogger()),
		WithXReadBlock(50 * time.Millisecond),
		WithHealthInterval(24 * time.Hour),
		WithPresenceInterval(24 * time.Hour),
		// Test windows are far shorter than 24h so the presence heartbeat
		// ticker never fires. skipPresenceIntervalCheck bypasses the
		// production invariant (interval < TTL); miniredis skipVersionCheck
		// bypasses the otherwise-fatal INFO server check.
		withSkipPresenceIntervalCheck(),
		withSkipVersionCheck(),
	}, opts)
	transport, err := NewRedisTransport(client, allOpts...)
	require.NoError(t, err)
	t.Cleanup(func() { transport.Close(context.Background()) })

	return transport, mr
}

func TestRedisTransportDispatch(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t)
	update := &mercure.Update{
		Topics: []string{"https://example.com/books/1"},
		Event:  mercure.Event{Data: "test data"},
	}
	err := transport.Dispatch(context.Background(), update)
	require.NoError(t, err)
	assert.NotEmpty(t, update.ID, "AssignUUID should have set the ID")
}

func TestRedisTransportClosed(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t)
	err := transport.Close(context.Background())
	require.NoError(t, err)

	update := &mercure.Update{
		Topics: []string{"https://example.com/test"},
		Event:  mercure.Event{Data: "test"},
	}
	err = transport.Dispatch(context.Background(), update)
	assert.ErrorIs(t, err, ErrClosedTransport)
}

func TestRedisTransportDoubleClose(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t)
	err := transport.Close(context.Background())
	require.NoError(t, err)
	err = transport.Close(context.Background())
	require.NoError(t, err)
}

func TestRedisTransportSubscribeAndReceive(t *testing.T) {
	t.Parallel()

	transport, mr := newTestTransport(t)
	tss := testTopicSelectorStore()
	sub := mercure.NewLocalSubscriber("", testLogger(), tss)
	sub.SetTopics([]string{"https://example.com/books/{id}"}, nil)
	err := transport.AddSubscriber(context.Background(), sub)
	require.NoError(t, err)

	update := &mercure.Update{
		Topics: []string{"https://example.com/books/1"},
		Event:  mercure.Event{Data: "test book data"},
	}
	err = transport.Dispatch(context.Background(), update)
	require.NoError(t, err)
	mr.FastForward(100 * time.Millisecond)

	select {
	case received := <-sub.Receive():
		assert.Equal(t, update.Data, received.Data)
		assert.Equal(t, update.Topics, received.Topics)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for update")
	}
}

func TestRedisTransportRemoveSubscriber(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t)
	tss := testTopicSelectorStore()
	sub := mercure.NewLocalSubscriber("", testLogger(), tss)
	sub.SetTopics([]string{"https://example.com/test"}, nil)
	err := transport.AddSubscriber(context.Background(), sub)
	require.NoError(t, err)
	err = transport.RemoveSubscriber(context.Background(), sub)
	require.NoError(t, err)
}

func TestRedisTransportConcurrentDispatch(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t)

	var wg sync.WaitGroup

	for range 50 {
		wg.Go(func() {
			update := &mercure.Update{
				Topics: []string{"https://example.com/concurrent"},
				Event:  mercure.Event{Data: "msg"},
			}
			err := transport.Dispatch(context.Background(), update)
			assert.NoError(t, err)
		})
	}

	wg.Wait()
}

func TestReconnectStormRateLimiting(t *testing.T) {
	t.Parallel()

	// Rate limit: 100/s with burst of 10. 50 concurrent subscribers
	// should see some of them delayed by the rate limiter.
	transport, _ := newTestTransport(
		t,
		WithSubscriberRateLimit(100),
		WithSubscriberRateBurst(10),
	)
	tss := testTopicSelectorStore()

	const numSubs = 50

	var wg sync.WaitGroup

	errs := make([]error, numSubs)

	start := time.Now()

	for i := range numSubs {
		wg.Go(func() {
			sub := mercure.NewLocalSubscriber("", testLogger(), tss)
			sub.SetTopics([]string{"https://example.com/reconnect"}, nil)
			errs[i] = transport.AddSubscriber(context.Background(), sub)
		})
	}

	wg.Wait()

	elapsed := time.Since(start)

	// All should succeed (rate limiter delays, doesn't reject).
	for i, err := range errs {
		require.NoError(t, err, "subscriber %d should succeed", i)
	}

	// With burst=10 and rate=100/s, adding 50 subscribers requires
	// (50-10)/100 = 0.4s. Allow some tolerance.
	assert.Greater(t, elapsed, 200*time.Millisecond,
		"rate limiter should have delayed some subscribers")
}

func TestReconnectStormContextCancel(t *testing.T) {
	t.Parallel()

	// Verify that a cancelled context returns promptly from the rate limiter
	// AND that a subscriber killed at the rate-limit gate is NOT counted by
	// subscriber_add_total. The latter binds the reposition invariant at
	// redis.go AddSubscriber (Inc must fire after the gate, not before).
	reg := prometheus.NewRegistry()
	transport, _ := newTestTransport(
		t,
		WithSubscriberRateLimit(1),
		WithSubscriberRateBurst(1),
		WithPrometheusRegisterer(reg),
	)
	tss := testTopicSelectorStore()

	// Exhaust the burst.
	sub1 := mercure.NewLocalSubscriber("", testLogger(), tss)
	sub1.SetTopics([]string{"https://example.com/test"}, nil)
	require.NoError(t, transport.AddSubscriber(context.Background(), sub1))

	// Next one should block on rate limiter; cancel context immediately.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	sub2 := mercure.NewLocalSubscriber("", testLogger(), tss)
	sub2.SetTopics([]string{"https://example.com/test"}, nil)
	err := transport.AddSubscriber(ctx, sub2)
	require.Error(t, err, "should fail with cancelled context")
	require.ErrorIs(t, err, context.Canceled)

	families, gatherErr := reg.Gather()
	require.NoError(t, gatherErr)

	counters := map[string]float64{}

	for _, f := range families {
		if len(f.GetMetric()) == 0 {
			continue
		}

		counters[f.GetName()] = f.GetMetric()[0].GetCounter().GetValue()
	}

	assert.InDelta(t, float64(1), counters[metricSubscriberAddTotal], 0,
		"only sub1 passed the rate-limit gate; sub2 was cancelled before Inc()")
	assert.InDelta(t, float64(0), counters["mercure_redis_subscriber_rate_limited_total"], 0,
		"sub2 was cancelled before Wait completed; counter records actual delays only, not failed attempts")
}

func TestHistoryReplayConcurrencyLimit(t *testing.T) {
	t.Parallel()

	// Verify the semaphore limits concurrent history replays.
	transport, mr := newTestTransport(
		t,
		WithHistoryReplayConcurrency(2),
	)
	tss := testTopicSelectorStore()

	// Dispatch a few messages so there's history to replay.
	for i := range 5 {
		update := &mercure.Update{
			Topics: []string{"https://example.com/history"},
			Event:  mercure.Event{Data: fmt.Sprintf("msg-%d", i)},
		}
		require.NoError(t, transport.Dispatch(context.Background(), update))
	}

	mr.FastForward(200 * time.Millisecond)

	// Start 10 subscribers with LastEventID — they all need history replay.
	const numSubs = 10

	var wg sync.WaitGroup

	errs := make([]error, numSubs)

	for i := range numSubs {
		wg.Go(func() {
			sub := mercure.NewLocalSubscriber(mercure.EarliestLastEventID, testLogger(), tss)
			sub.SetTopics([]string{"https://example.com/history"}, nil)
			errs[i] = transport.AddSubscriber(context.Background(), sub)
		})
	}

	wg.Wait()

	// All should succeed — semaphore gates concurrency, doesn't reject.
	for i, err := range errs {
		assert.NoError(t, err, "subscriber %d history replay should succeed", i)
	}
}

// TestCheckVersionFromInfo exercises the pure version-validation logic extracted
// from validateVersion. Lives here (not lua_test.go) because it calls a method
// on RedisTransport and benefits from the newTestTransport helper.
//
// Each subtest constructs its own transport so the shared `t.backendType`
// field (latched by checkVersionFromInfo) doesn't race across parallel
// subtests under -race. Production callers only invoke checkVersionFromInfo
// once per transport, before metrics initialization, so the production
// path has no race — this is a test-fixture concern only.
func TestCheckVersionFromInfo(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		info        string
		wantErr     error
		wantBackend serverType
	}{
		{
			name:        "redis 7 supported",
			info:        "# Server\r\nredis_version:7.2.4\r\n",
			wantErr:     nil,
			wantBackend: serverTypeRedis,
		},
		{
			name:        "redis 6.2 exactly at floor",
			info:        "redis_version:6.2.0\n",
			wantErr:     nil,
			wantBackend: serverTypeRedis,
		},
		{
			name:        "redis 6.1 below floor",
			info:        "redis_version:6.1.9\n",
			wantErr:     ErrUnsupportedServerVersion,
			wantBackend: serverTypeRedis, // latched even on error path
		},
		{
			name:        "redis 5.0 below floor",
			info:        "redis_version:5.0.14\n",
			wantErr:     ErrUnsupportedServerVersion,
			wantBackend: serverTypeRedis, // latched even on error path
		},
		{
			name:        "valkey 8 supported",
			info:        "valkey_version:8.1.6\n",
			wantErr:     nil,
			wantBackend: serverTypeValkey,
		},
		{
			name:        "valkey 7.2 exactly at floor",
			info:        "valkey_version:7.2.0\n",
			wantErr:     nil,
			wantBackend: serverTypeValkey,
		},
		{
			name:        "valkey 7.1 below floor",
			info:        "valkey_version:7.1.0\n",
			wantErr:     ErrUnsupportedServerVersion,
			wantBackend: serverTypeValkey, // latched even on error path
		},
		{
			name:        "unknown server type still permitted (warn)",
			info:        "dragonfly_version:1.0\nredis_version:6.2.0\n",
			wantErr:     nil,
			wantBackend: serverTypeRedis, // dragonfly_version isn't parsed; redis_version wins
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Per-subtest transport — see TestCheckVersionFromInfo doc comment
			// for why sharing one across parallel subtests races on backendType.
			transport, _ := newTestTransport(t)

			err := transport.checkVersionFromInfo(tc.info)
			// Latch must populate on every call, including error paths,
			// so dashboards can see what backend was tried even when
			// validateVersion rejects it.
			assert.Equal(t, tc.wantBackend, transport.backendType,
				"backendType latch must capture parseServerInfo's serverType regardless of version-gate outcome")

			if tc.wantErr == nil {
				require.NoError(t, err)

				return
			}

			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}

// TestCheckVersionFromInfoMissingFieldFatal verifies that an INFO response
// without any parseable version field is fatal in production (skipVersionCheck=false).
func TestCheckVersionFromInfoMissingFieldFatal(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	// Build transport without withSkipVersionCheck but short-circuit NewRedisTransport
	// by constructing the struct directly; we only need a valid receiver for
	// checkVersionFromInfo's option check.
	transport := &RedisTransport{
		opts:   &options{}, // skipVersionCheck = false
		logger: testLogger(),
		client: client,
	}

	err := transport.checkVersionFromInfo("# Server\r\nos:Linux\r\n")
	require.Error(t, err)
	require.ErrorIs(t, err, ErrMissingVersionField)
}

// TestCheckVersionFromInfoMissingFieldSkipped verifies skipVersionCheck bypass
// for the no-parseable-version branch (used by miniredis-backed tests).
func TestCheckVersionFromInfoMissingFieldSkipped(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t) // includes withSkipVersionCheck()

	err := transport.checkVersionFromInfo("# Server\r\nos:Linux\r\n")
	require.NoError(t, err)
}

// TestTTLCleanupTrimsExpiredEntries verifies that when WithEventTTL is enabled,
// the ttlCleanup goroutine issues XTRIM MINID with a timestamp in the past,
// removing entries older than the TTL. Uses miniredis FastForward to advance
// time deterministically.
func TestTTLCleanupTrimsExpiredEntries(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	transport, err := NewRedisTransport(
		client,
		WithLogger(testLogger()),
		WithXReadBlock(10*time.Millisecond),
		WithHealthInterval(24*time.Hour),
		WithPresenceInterval(24*time.Hour),
		WithEventTTL(100*time.Millisecond),
		WithCleanupInterval(50*time.Millisecond),
		withSkipPresenceIntervalCheck(),
		withSkipVersionCheck(),
	)
	require.NoError(t, err)
	t.Cleanup(func() { transport.Close(context.Background()) })

	// Publish several entries at t=0.
	for i := range 3 {
		update := &mercure.Update{
			Topics: []string{"https://example.com/ttl"},
			Event:  mercure.Event{Data: fmt.Sprintf("entry-%d", i)},
		}
		require.NoError(t, transport.Dispatch(context.Background(), update))
	}

	// Advance miniredis time past TTL and wait for the cleanup tick.
	mr.FastForward(200 * time.Millisecond)

	// Poll until the stream is trimmed or the deadline passes. The cleanup
	// goroutine fires every 50ms so 500ms gives ~10 attempts.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		length, err := client.XLen(context.Background(), "{mercure}").Result()
		if err == nil && length < 3 {
			return
		}

		time.Sleep(20 * time.Millisecond)
	}

	t.Fatal("ttlCleanup did not trim expired entries within deadline")
}

// TestSetCodec verifies the codec can be swapped at runtime via the
// mercure.TransportCodec interface.
func TestSetCodec(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t)

	require.IsType(t, mercure.JSONCodec{}, *transport.codec.Load())

	transport.SetCodec(mercure.GobCodec{})
	assert.IsType(t, mercure.GobCodec{}, *transport.codec.Load())
}

// TestIsStreamNotExist covers the error-string heuristic used when a stream
// has been wiped (FLUSHDB, eviction). The string match is the only signal
// go-redis surfaces for this condition.
func TestIsStreamNotExist(t *testing.T) {
	t.Parallel()

	// isStreamNotExist is called on XInfoGroups errors, which Redis emits as
	// "ERR no such key" (lowercase). Case-sensitive match is intentional —
	// NOGROUP errors from XREADGROUP are a different error surface.
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "xinfogroups on missing stream", err: errXInfoGroupsMissingStream, want: true},
		{name: "wrapped no such key", err: fmt.Errorf("info groups: %w", errLowercaseMissing), want: true},
		{name: "unrelated error", err: errWrongType, want: false},
		{name: "capitalized does not match", err: errCapitalizedMissing, want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.want, isStreamNotExist(tc.err))
		})
	}
}

// TestIsPELEmpty verifies the pending-entry-list emptiness check used to decide
// whether an XREADGROUP batch contains new messages.
func TestIsPELEmpty(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t)

	assert.True(t, transport.isPELEmpty(nil), "nil slice is empty")
	assert.True(t, transport.isPELEmpty([]redis.XStream{}), "empty slice is empty")
	assert.True(t, transport.isPELEmpty([]redis.XStream{
		{Stream: "s", Messages: nil},
		{Stream: "s", Messages: []redis.XMessage{}},
	}), "streams with no messages are empty")
	assert.False(t, transport.isPELEmpty([]redis.XStream{
		{Stream: "s", Messages: []redis.XMessage{{ID: "1-0"}}},
	}), "stream with messages is not empty")
}

// TestDispatchToSubscribersHoldsLockThroughFanOut locks in the invariant
// that dispatchToSubscribers must return with t.mu still held by the
// caller, including the sharded path's fan-out + wg.Wait. Pre-fix the
// sharded path released and re-acquired the lock around shardedDispatch,
// allowing AddSubscriber to interleave between MatchAny and the
// lastDispatchedStreamID advance — silently dropping messages for
// subscribers added during fan-out.
func TestDispatchToSubscribersHoldsLockThroughFanOut(t *testing.T) {
	t.Parallel()

	transport, _ := newShardedTestTransport(t, 4)

	update := &mercure.Update{
		Topics: []string{"https://example.com/lock-invariant"},
		Event:  mercure.Event{ID: "lock-invariant-1", Data: "x"},
	}

	transport.mu.Lock()
	transport.dispatchToSubscribers(t.Context(), update)

	// After the call returns we must still hold the lock — a TryLock
	// from another goroutine must fail. If the pre-fix Unlock/Lock
	// dance ever returns, this test fails.
	var concurrent atomic.Bool

	done := make(chan struct{})

	go func() {
		defer close(done)

		if transport.mu.TryLock() {
			concurrent.Store(true)
			transport.mu.Unlock()
		}
	}()

	<-done

	transport.mu.Unlock()

	assert.False(t, concurrent.Load(),
		"dispatchToSubscribers must return with t.mu still held by caller (post-fan-out)")
}

// TestWaitForLimiterFastPath verifies the Allow() fast-path: no counter
// increment, no Wait() call. Pre-fix used Reserve()+timer which always
// consumed a token even on the fast path; the new shape avoids the
// counter inflation and the doomed-reservation queue penalty.
func TestWaitForLimiterFastPath(t *testing.T) {
	t.Parallel()

	counter := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "waitforlimiter_fastpath_total",
		Help: "Test counter for waitForLimiter fast-path coverage.",
	})
	// rate.Inf means Allow() always returns true.
	limiter := rate.NewLimiter(rate.Inf, 1)

	require.NoError(t, waitForLimiter(t.Context(), limiter, counter, "test"))

	var m dto.Metric

	require.NoError(t, counter.Write(&m))
	assert.InDelta(t, 0.0, m.GetCounter().GetValue(), 0,
		"fast path (Allow=true) must not increment the rate-limited counter")
}

// TestWaitForLimiterSlowPathIncrementsCounter verifies the slow-path
// branch: when Allow() returns false, the rate-limited counter
// increments before Wait(ctx) blocks.
func TestWaitForLimiterSlowPathIncrementsCounter(t *testing.T) {
	t.Parallel()

	counter := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "waitforlimiter_slowpath_total",
		Help: "Test counter for waitForLimiter slow-path coverage.",
	})
	// 1 token/sec, burst 1: first call passes Allow, second forces Wait.
	limiter := rate.NewLimiter(rate.Limit(1), 1)

	require.NoError(t, waitForLimiter(t.Context(), limiter, counter, "test"))

	// Second call: Allow() returns false, Wait blocks ~1s — bound the
	// test with a deadline; the function returns once Wait wakes.
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	require.NoError(t, waitForLimiter(ctx, limiter, counter, "test"))

	var m dto.Metric

	require.NoError(t, counter.Write(&m))
	assert.InDelta(t, 1.0, m.GetCounter().GetValue(), 0,
		"slow path (Allow=false, Wait succeeds) must increment the rate-limited counter exactly once")
}

// TestWaitForLimiterCanceledCtxRejects verifies a doomed context rejects
// without blocking on a reservation. limiter.Wait(ctx) is the documented
// contract — a cancelled context returns immediately without consuming
// a token, avoiding the queue inflation Reserve+timer caused.
func TestWaitForLimiterCanceledCtxRejects(t *testing.T) {
	t.Parallel()

	// Slow limiter (1 token / 1000s, burst 1). Consume the burst, then
	// the next Allow() returns false and Wait would block ~1000s. The
	// cancelled ctx forces a fast-fail on Wait.
	limiter := rate.NewLimiter(rate.Limit(0.001), 1)
	require.True(t, limiter.Allow(), "burst should yield first token")

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err := waitForLimiter(ctx, limiter, nil, "test")
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
}

// TestWaitForLimiterNilLimiterNoOp verifies the optional-limiter shape:
// callers may pass nil to disable rate limiting entirely.
func TestWaitForLimiterNilLimiterNoOp(t *testing.T) {
	t.Parallel()

	require.NoError(t, waitForLimiter(t.Context(), nil, nil, "test"))
}

// TestDispatchAfterCloseAlwaysReturnsClosedSentinel races concurrent
// Dispatch goroutines against Close. After Close fires, every error
// returned by Dispatch must be ErrClosedTransport — never a wrapped
// `redis client is closed` or `context canceled` error. This binds the
// post-publish select that converts the race-window's redis/ctx error
// to the canonical sentinel.
func TestDispatchAfterCloseAlwaysReturnsClosedSentinel(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t)

	const dispatchers = 16

	var (
		started sync.WaitGroup
		done    sync.WaitGroup
		stop    atomic.Bool
	)

	started.Add(dispatchers)
	done.Add(dispatchers)

	errs := make(chan error, 1024)

	for i := range dispatchers {
		go func(id int) {
			defer done.Done()

			started.Done()

			for !stop.Load() {
				err := transport.Dispatch(context.Background(), &mercure.Update{
					Topics: []string{"https://example.com/race"},
					Event:  mercure.Event{Data: strconv.Itoa(id)},
				})
				if err != nil {
					select {
					case errs <- err:
					default: // shed once buffer fills — we only need a sample
					}
				}
			}
		}(i)
	}

	started.Wait()
	// Close while dispatchers are mid-flight. The race window we care
	// about is between Dispatch's entry select(t.closed) and the
	// publishScript.Run call.
	require.NoError(t, transport.Close(context.Background()))

	stop.Store(true)
	done.Wait()
	close(errs)

	for err := range errs {
		require.ErrorIs(t, err, ErrClosedTransport,
			"every Dispatch error after Close must be ErrClosedTransport, got: %v", err)
	}
}

// TestXReadErrorBackoffShape locks in the listener's reconnect-delay
// progression: 1s base, doubling per consecutive error, capped at 30s.
// Without this test a future tweak to consecutiveErrors handling could
// silently re-introduce the tight 1s loop the cap was added to prevent.
func TestXReadErrorBackoffShape(t *testing.T) {
	t.Parallel()

	cases := []struct {
		consecutiveErrors int
		want              time.Duration
	}{
		{consecutiveErrors: 0, want: time.Second},
		{consecutiveErrors: 1, want: time.Second},
		{consecutiveErrors: 2, want: 2 * time.Second},
		{consecutiveErrors: 3, want: 4 * time.Second},
		{consecutiveErrors: 4, want: 8 * time.Second},
		{consecutiveErrors: 5, want: 16 * time.Second},
		{consecutiveErrors: 6, want: 30 * time.Second}, // capped (would be 32s)
		{consecutiveErrors: 100, want: 30 * time.Second},
		{consecutiveErrors: 1_000_000, want: 30 * time.Second},
	}

	for _, tc := range cases {
		got := xreadErrorBackoff(tc.consecutiveErrors)
		assert.Equal(t, tc.want, got, "consecutiveErrors=%d", tc.consecutiveErrors)
	}
}

// TestRunHealthPingSuccess exercises the happy path of the health check body:
// PING succeeds, failures reset to 0, healthy gauge set to 1.
func TestRunHealthPingSuccess(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	transport, _ := newTestTransport(t, WithPrometheusRegisterer(reg))

	// Start from a degraded state to verify recovery path.
	failures := transport.runHealthPing(context.Background(), 5)
	assert.Equal(t, 0, failures, "successful PING resets failure count")
}

// failingHealthCtx returns a context already past its deadline, so an internal
// PING fails with context.DeadlineExceeded — a real health failure runHealthPing
// counts. It is deliberately NOT a cancelled context: a cancelled context is a
// graceful shutdown, which runHealthPing must NOT count (see isShutdownCancel and
// TestShutdownCancelSuppressesBackgroundErrors). Simulates a Redis outage fast,
// without a network wait.
func failingHealthCtx(t *testing.T) context.Context {
	t.Helper()

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Hour))
	t.Cleanup(cancel)

	return ctx
}

// TestRunHealthPingFailureIncrementsCounter exercises the failure path: an
// expired-deadline PING returns context.DeadlineExceeded immediately, which
// increments the consecutive-failure counter and flips the healthy gauge to 0
// once the threshold is crossed. The expired deadline (not a cancelled context,
// which would be a graceful shutdown the loop ignores) avoids go-redis
// connection-pool retry backoff (5 attempts x ~1s) that would make the test slow.

func TestRunHealthPingFailureIncrementsCounter(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	transport, _ := newTestTransport(
		t,
		WithPrometheusRegisterer(reg),
		WithHealthThreshold(2),
	)

	ctx := failingHealthCtx(t)

	failures := 0
	for range 3 {
		failures = transport.runHealthPing(ctx, failures)
	}

	assert.Equal(t, 3, failures, "three consecutive failures accumulate")
}

// TestHistoryReplayFallsBackToReplayAll verifies the Pass 2 fallback engages
// when a subscriber's Last-Event-ID was trimmed from the stream. The test
// publishes events, then adds a subscriber with a valid UUIDv7 Last-Event-ID
// that does not match any published event — Pass 1 scans and finds nothing,
// rs.found stays false, and run() invokes replayAll.
func TestHistoryReplayFallsBackToReplayAll(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	transport, _ := newTestTransport(t, WithPrometheusRegisterer(reg))

	// Publish three events with transport-generated IDs.
	for i := range 3 {
		require.NoError(t, transport.Dispatch(context.Background(), &mercure.Update{
			Topics: []string{"https://example.com/replay-all"},
			Event:  mercure.Event{Data: fmt.Sprintf("msg-%d", i)},
		}))
	}

	// Subscribe with a valid UUIDv7 that was never dispatched. The timestamp
	// prefix places it in the past so seekStreamID scans from stream start.
	tss := testTopicSelectorStore()
	sub := mercure.NewLocalSubscriber("urn:uuid:00000000-0000-7000-8000-000000000000", testLogger(), tss)
	sub.SetTopics([]string{"https://example.com/replay-all"}, nil)

	require.NoError(t, transport.AddSubscriber(context.Background(), sub))

	// Pass 2 should have replayed all three events.
	received := 0

	for range 3 {
		select {
		case <-sub.Receive():
			received++
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for replayAll delivery; got %d", received)
		}
	}

	assert.Equal(t, 3, received)

	// Pin the operator-facing signal: the full-scan fallback counter must
	// tick exactly once on a successful Pass 2. A regression that deletes
	// the counter call site at run() would slip past the prior assertion
	// (which only proves replay delivered).
	families, err := reg.Gather()
	require.NoError(t, err)

	var fallbackCount float64

	for _, f := range families {
		if f.GetName() == metricHistoryReplayFallbackTotal {
			fallbackCount = f.GetMetric()[0].GetCounter().GetValue()
		}
	}

	assert.InDelta(t, 1.0, fallbackCount, 0,
		"history_replay_fallback_total should record exactly one full-scan after Pass 2 success")
}

// TestHistoryReplay_FutureUUIDv7SkipsPass2 asserts end-to-end: when a
// subscriber arrives with a Last-Event-ID whose UUIDv7 wall-clock
// timestamp is past the hub's `time.Now() + clockSkewMargin`, the
// transport must skip BOTH Pass 1 and Pass 2, finalize the subscriber
// cleanly, and NOT increment the history_replay_fallback_total counter.
// The predicate has unit coverage in historyreplay_guard_test.go; this
// test exercises the full AddSubscriber → run() → HistoryDispatched flow
// against miniredis.
func TestHistoryReplay_FutureUUIDv7SkipsPass2(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	transport, _ := newTestTransport(t, WithPrometheusRegisterer(reg))

	for i := range 3 {
		require.NoError(t, transport.Dispatch(context.Background(), &mercure.Update{
			Topics: []string{"https://example.com/future-id"},
			Event:  mercure.Event{Data: fmt.Sprintf("msg-%d", i)},
		}))
	}

	// Synthesize a UUIDv7 ~24h in the future — well past clockSkewMargin
	// (5s default). UUID layout xxxxxxxx-xxxx-Mxxx-Nxxx-xxxxxxxxxxxx
	// puts timestamp high-32 bits in chars 1-8 and timestamp low-16 in
	// chars 10-13; M is the version nibble (7 for v7), N is the
	// variant (8 = RFC 4122 variant 10). Mask via uint64 so the
	// printf widths produce exactly 8 and 4 hex chars regardless of
	// arch-specific int width.
	futureMs := uint64(time.Now().Add(24 * time.Hour).UnixMilli())
	futureID := fmt.Sprintf("urn:uuid:%08x-%04x-7000-8000-000000000000",
		(futureMs>>16)&0xFFFFFFFF, futureMs&0xFFFF)

	tss := testTopicSelectorStore()
	sub := mercure.NewLocalSubscriber(futureID, testLogger(), tss)
	sub.SetTopics([]string{"https://example.com/future-id"}, nil)

	require.NoError(t, transport.AddSubscriber(context.Background(), sub),
		"AddSubscriber must succeed for a future-dated Last-Event-ID (no replay attempted)")

	// The guard must NOT have triggered Pass 2: that counter increments
	// once per full-scan, and the guard exists to avoid full-window
	// replay on future-dated IDs. Exact-zero assertion is meaningful
	// because Pass 2 fires exactly once per fallback in
	// TestHistoryReplayFallsBackToReplayAll.
	families, err := reg.Gather()
	require.NoError(t, err)

	var fallbackCount float64

	for _, f := range families {
		if f.GetName() == metricHistoryReplayFallbackTotal && len(f.GetMetric()) > 0 {
			fallbackCount = f.GetMetric()[0].GetCounter().GetValue()
		}
	}

	assert.InDelta(t, 0.0, fallbackCount, 0,
		"future-dated Last-Event-ID must NOT trigger Pass 2 full-stream replay")

	// Confirm a follow-up live publish reaches the subscriber, proving
	// HistoryDispatched + Ready ran (early-return / stuck-in-history-
	// buffer regression would block this). Use a unique token so the
	// drain loop cannot pass on a pre-add catch-up event named
	// "live-after-guard". Collect every event into a slice so a
	// regression that lets pre-add events leak through is also visible.
	const liveToken = "live-after-guard-3f8c1a2e"

	require.NoError(t, transport.Dispatch(context.Background(), &mercure.Update{
		Topics: []string{"https://example.com/future-id"},
		Event:  mercure.Event{Data: liveToken},
	}))

	var received []string

	deadline := time.After(2 * time.Second)

drainLoop:
	for {
		select {
		case u := <-sub.Receive():
			received = append(received, u.Data)
			if u.Data == liveToken {
				break drainLoop
			}
		case <-deadline:
			t.Fatalf("future-ID guard failed to finalize subscriber; live event never arrived. Received: %v", received)
		}
	}

	// Any catch-up stragglers from the pre-add publishes are acceptable
	// (listener-vs-AddSubscriber ordering), but the liveToken MUST be
	// in the received set — that proves the post-add publish landed.
	assert.Contains(t, received, liveToken,
		"the post-guard live publish must reach the subscriber")
}

// TestHistoryReplay_PublishedEventNotMisclassifiedAsFuture covers the
// listener-lag regression class: a prior predicate compared the
// requested UUIDv7 ms against the listener's lastDispatchedStreamID,
// so under listener lag (xreadBlock > clockSkewMargin) or publisher
// clock skew an already-XADDed event whose UUIDv7 timestamp was newer
// than the dispatched stream tail would be misclassified as
// "future-dated" and Pass 2 would be wrongly skipped, dropping
// legitimate history.
//
// The wall-clock predicate is independent of listener progress, so
// the test publishes events, requests EarliestLastEventID replay, and
// asserts the legacy two-pass replay STILL runs — i.e. the guard does
// NOT mis-fire on legitimate replay.
func TestHistoryReplay_PublishedEventNotMisclassifiedAsFuture(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	transport, _ := newTestTransport(t, WithPrometheusRegisterer(reg))

	for i := range 3 {
		require.NoError(t, transport.Dispatch(context.Background(), &mercure.Update{
			Topics: []string{"https://example.com/lag-regression"},
			Event:  mercure.Event{Data: fmt.Sprintf("msg-%d", i)},
		}))
	}

	// Use an EarliestLastEventID-style request so legacy two-pass replay
	// runs end-to-end. The guard must NOT fire for this sentinel.
	tss := testTopicSelectorStore()
	sub := mercure.NewLocalSubscriber(mercure.EarliestLastEventID, testLogger(), tss)
	sub.SetTopics([]string{"https://example.com/lag-regression"}, nil)

	require.NoError(t, transport.AddSubscriber(context.Background(), sub))

	// All 3 pre-add events must reach the subscriber via Pass 2 replay.
	got := make(map[string]bool)

	deadline := time.After(2 * time.Second)

	for len(got) < 3 {
		select {
		case u := <-sub.Receive():
			got[u.Data] = true
		case <-deadline:
			t.Fatalf("legacy replay dropped events; received: %v", got)
		}
	}

	for i := range 3 {
		assert.True(t, got[fmt.Sprintf("msg-%d", i)],
			"msg-%d must be replayed via Pass 2 (guard must not mis-fire on legitimate replay)", i)
	}
}

// TestHistoryReplayCursorGuardsForbiddenEventID locks the replay-cursor guard in
// BOTH directions. The "eventID" stream metadata is a denormalized copy of the
// update's id that history replay records as lastDispatchedEventID and emits in
// the Last-Event-Id response header (subscribe.go). decodeStreamEntry guards the
// decoded update.ID, but this separate metadata copy is not part of that update —
// so a tampered entry could pair a safe update.ID with a CR/LF/NUL eventID. The
// clean case (positive) kills an "always-reject" mutation that would break
// legitimate cursor advancement; the forbidden case (negative) kills a "no-guard"
// mutation. A legitimate eventID equals the published update.ID and always passes.
func TestHistoryReplayCursorGuardsForbiddenEventID(t *testing.T) {
	t.Parallel()

	topic := "https://example.com/cursor-guard"

	cases := []struct {
		name        string
		eventID     string
		wantCursor  string  // "" == cursor must be left unadvanced
		wantRejects float64 // history_cursor_rejected_total increments
	}{
		{"clean eventID becomes the cursor", "urn:uuid:evt-1", "urn:uuid:evt-1", 0},
		{"forbidden eventID is skipped and counted", "bad\r\ninjected", "", 1},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Per-subtest registry so the reject counter is isolated.
			reg := prometheus.NewRegistry()
			transport, _ := newTestTransport(t, WithPrometheusRegisterer(reg))
			codec := *transport.codec.Load()

			tss := testTopicSelectorStore()
			sub := mercure.NewLocalSubscriber("", testLogger(), tss)
			sub.SetTopics([]string{topic}, nil)

			// The decoded update.ID is always clean; only the separate eventID
			// stream metadata varies. A single buffered Dispatch is non-blocking,
			// so the delivered update can sit unread (no drain goroutine → goleak-safe).
			data, err := codec.Marshal(&mercure.Update{Topics: []string{topic}, Event: mercure.Event{ID: "urn:uuid:safe"}})
			require.NoError(t, err)

			entry := redis.XMessage{ID: "1-0", Values: map[string]any{"data": string(data), "eventID": tc.eventID}}

			rs := &historyReplayState{t: transport, s: sub}
			require.True(t, rs.dispatchAndTrack(context.Background(), entry))

			assert.Equal(t, tc.wantCursor, rs.lastDispatchedEventID)
			assert.InDelta(t, tc.wantRejects, counterValue(t, reg, "mercure_redis_history_cursor_rejected_total"), 0,
				"the reject counter is the durable signal — the Warn is rate-sampled")
		})
	}
}

// TestReceivePathDropsForbiddenEntryEndToEnd drives the REAL history-replay
// caller path (AddSubscriber → run → decodeStreamEntry) with a poisoned stream
// entry injected directly into the stream. It proves the guard's nil return
// actually prevents delivery to a live subscriber — not merely that
// decodeStreamEntry returns nil — and that the drop is counted. Complements the
// chokepoint-direct TestDecodeStreamEntryDropsForbiddenSSEChars.
func TestReceivePathDropsForbiddenEntryEndToEnd(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	transport, mr := newTestTransport(t, WithPrometheusRegisterer(reg))
	codec := *transport.codec.Load()
	streamKey := transport.key("")
	topic := "https://example.com/e2e-drop"

	clean, err := codec.Marshal(&mercure.Update{Topics: []string{topic}, Event: mercure.Event{ID: "urn:uuid:clean", Data: "good"}})
	require.NoError(t, err)

	// Forbidden CR in the decoded id — the receive-side guard must drop this.
	poisoned, err := codec.Marshal(&mercure.Update{Topics: []string{topic}, Event: mercure.Event{ID: "x\rid: forged", Data: "bad"}})
	require.NoError(t, err)

	_, err = mr.XAdd(streamKey, "*", []string{"eventID", "urn:uuid:clean", "data", string(clean)})
	require.NoError(t, err)

	_, err = mr.XAdd(streamKey, "*", []string{"eventID", "urn:uuid:bad", "data", string(poisoned)})
	require.NoError(t, err)

	tss := testTopicSelectorStore()
	sub := mercure.NewLocalSubscriber(mercure.EarliestLastEventID, testLogger(), tss)
	sub.SetTopics([]string{topic}, nil)
	require.NoError(t, transport.AddSubscriber(context.Background(), sub))

	got := map[string]bool{}

	select {
	case u := <-sub.Receive():
		got[u.Data] = true
	case <-time.After(2 * time.Second):
		t.Fatal("clean update was not replayed")
	}

	// Grace window: a wrongly-delivered poisoned entry would arrive around now.
	select {
	case u := <-sub.Receive():
		got[u.Data] = true
	case <-time.After(300 * time.Millisecond):
	}

	assert.True(t, got["good"], "the clean update must be replayed to the subscriber")
	assert.False(t, got["bad"], "the poisoned update must be dropped, never delivered")
	assert.GreaterOrEqual(t, forbiddenSSECharCount(t, reg), float64(1), "the forbidden drop must be counted")
}

// counterValue returns the value of a no-label counter metric by name (0 if absent).
func counterValue(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()

	families, err := reg.Gather()
	require.NoError(t, err)

	for _, f := range families {
		if f.GetName() == name && len(f.GetMetric()) > 0 {
			return f.GetMetric()[0].GetCounter().GetValue()
		}
	}

	return 0
}

// TestReadyAfterStartup verifies Ready returns nil once startup has set the
// healthy flag, before the first health-check PING has run.
func TestReadyAfterStartup(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t)

	require.NoError(t, transport.Ready(context.Background()))
	require.NoError(t, transport.Live(context.Background()))
}

// TestReadyAfterCloseReturnsError verifies Ready/Live both report the closed
// state with ErrClosedTransport so readiness and liveness probes flip
// immediately on shutdown.
func TestReadyAfterCloseReturnsError(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t)
	require.NoError(t, transport.Close(context.Background()))

	err := transport.Ready(context.Background())
	require.ErrorIs(t, err, ErrClosedTransport)

	err = transport.Live(context.Background())
	require.ErrorIs(t, err, ErrClosedTransport)
}

// TestReadyFlipsUnhealthyOnPingFailures verifies Ready returns
// ErrTransportUnhealthy after the health loop records healthThreshold
// consecutive PING failures. Simulates a real PING timeout (an expired-deadline
// context → DeadlineExceeded) so the test is fast and doesn't depend on network
// timeouts — NOT a cancelled context, which is a graceful shutdown the health
// loop intentionally ignores.
func TestReadyFlipsUnhealthyOnPingFailures(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t, WithHealthThreshold(2))

	require.NoError(t, transport.Ready(context.Background()), "healthy after startup")

	ctx := failingHealthCtx(t)

	failures := 0
	for range 3 {
		failures = transport.runHealthPing(ctx, failures)
	}

	err := transport.Ready(context.Background())
	require.ErrorIs(t, err, ErrTransportUnhealthy)

	// Live does NOT flip on Redis outages — only on Close.
	require.NoError(t, transport.Live(context.Background()),
		"Live stays nil during transient Redis outage")
}

// TestReadyRecoversAfterSuccessfulPing verifies Ready returns nil again once
// a subsequent PING succeeds after the transport was marked unhealthy.
func TestReadyRecoversAfterSuccessfulPing(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t, WithHealthThreshold(1))

	failures := transport.runHealthPing(failingHealthCtx(t), 0)
	require.GreaterOrEqual(t, failures, 1)

	require.ErrorIs(t, transport.Ready(context.Background()), ErrTransportUnhealthy)

	// Fresh context pings miniredis successfully → healthy flips back.
	_ = transport.runHealthPing(context.Background(), failures)
	require.NoError(t, transport.Ready(context.Background()))
}

// panickingCtx simulates Caddy's zero-value caddy.Context whose embedded
// context.Context is nil: every method panics on invocation. This is what
// go-redis triggers when the hub closes the transport during `caddy validate`.
type panickingCtx struct{}

func (panickingCtx) Deadline() (time.Time, bool) { panic("panickingCtx.Deadline called") }
func (panickingCtx) Done() <-chan struct{}       { panic("panickingCtx.Done called") }
func (panickingCtx) Err() error                  { panic("panickingCtx.Err called") }
func (panickingCtx) Value(any) any               { panic("panickingCtx.Value called") }

// TestCloseIgnoresCallerContext verifies Close tolerates a caller ctx whose
// internals would panic on any Done/Err/Deadline call. Caddy's
// TransportDestructor invokes Close(caddy.ActiveContext()) during
// `caddy validate`; that returns a zero-value caddy.Context with a nil
// embedded context, and go-redis's internal ctx.Done() call nil-derefs.
// Close uses a self-managed background context instead; this test pins the
// contract so a future refactor can't silently re-introduce the panic.
func TestCloseIgnoresCallerContext(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t)

	assert.NotPanics(t, func() {
		_ = transport.Close(panickingCtx{})
	})
}

// TestRedisPoolSizeIntrospection pins the type-switch contract for
// warnSuspiciousClientOptions. The data-plane client types
// (*redis.Client for single-instance + Sentinel-via-failover,
// *redis.ClusterClient for Cluster mode, *redis.Ring for client-side
// sharded deployments) expose PoolSize through Options(); any other
// client type returns 0 so the warning fires only when we have
// authoritative information.
func TestRedisPoolSizeIntrospection(t *testing.T) {
	t.Parallel()

	t.Run("redis.Client", func(t *testing.T) {
		t.Parallel()

		c := redis.NewClient(&redis.Options{Addr: "localhost:0", PoolSize: 42})

		t.Cleanup(func() { _ = c.Close() })

		assert.Equal(t, 42, redisPoolSize(c))
	})

	t.Run("redis.ClusterClient", func(t *testing.T) {
		t.Parallel()

		c := redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:    []string{"localhost:0"},
			PoolSize: 30,
		})

		t.Cleanup(func() { _ = c.Close() })

		assert.Equal(t, 30, redisPoolSize(c))
	})

	t.Run("redis.Ring", func(t *testing.T) {
		t.Parallel()

		c := redis.NewRing(&redis.RingOptions{
			Addrs:    map[string]string{"shard1": "localhost:0"},
			PoolSize: 25,
		})

		t.Cleanup(func() { _ = c.Close() })

		assert.Equal(t, 25, redisPoolSize(c))
	})
}

// TestWarnSuspiciousClientOptionsHistoryReplayExceedsPool pins the
// soft-warning behavior: the predicate is `>=` (not `>`) because at exact
// equality every replay slot consumes a connection and other operations
// have zero pool headroom — head-of-line blocking is immediate. Strictly
// below the pool is silent; at-or-above warns.
func TestWarnSuspiciousClientOptionsHistoryReplayExceedsPool(t *testing.T) {
	t.Parallel()

	t.Run("exceeds pool: warns", func(t *testing.T) {
		t.Parallel()

		logger, records := captureLogger()

		c := redis.NewClient(&redis.Options{Addr: "localhost:0", PoolSize: 10})

		t.Cleanup(func() { _ = c.Close() })

		o := defaultOptions()
		o.historyReplayConcurrency = 50

		warnSuspiciousClientOptions(o, c, logger)

		assert.NotNil(t, findWarnMessage(*records, "WithHistoryReplayConcurrency"))
	})

	t.Run("equal to pool: warns (zero headroom = starvation)", func(t *testing.T) {
		t.Parallel()

		logger, records := captureLogger()

		c := redis.NewClient(&redis.Options{Addr: "localhost:0", PoolSize: 50})

		t.Cleanup(func() { _ = c.Close() })

		o := defaultOptions()
		o.historyReplayConcurrency = 50

		warnSuspiciousClientOptions(o, c, logger)

		assert.NotNil(t, findWarnMessage(*records, "WithHistoryReplayConcurrency"),
			"concurrency at pool size leaves zero headroom for non-replay ops")
	})

	t.Run("strictly below pool: silent", func(t *testing.T) {
		t.Parallel()

		logger, records := captureLogger()

		c := redis.NewClient(&redis.Options{Addr: "localhost:0", PoolSize: 100})

		t.Cleanup(func() { _ = c.Close() })

		o := defaultOptions()
		o.historyReplayConcurrency = 50

		warnSuspiciousClientOptions(o, c, logger)

		assert.Empty(t, *records, "concurrency strictly below pool size should not warn")
	})
}

// TestReadyRespectsContext verifies Ready returns the caller's ctx error
// (wrapped) when the probe ctx is already cancelled, rather than misreporting
// health state.
func TestReadyRespectsContext(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := transport.Ready(ctx)
	require.ErrorIs(t, err, context.Canceled)
}
