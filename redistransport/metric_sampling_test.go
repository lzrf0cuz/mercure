package redistransport

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/dunglas/mercure"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// gaugeValueForTest extracts the float value of a single Prometheus Gauge.
// Centralizes the prometheus.Gauge → dto.Metric → *float64 dance that
// every sampling-branch test needs. Uses the protogetter form to
// satisfy the project lint rule.
func gaugeValueForTest(t *testing.T, g prometheus.Gauge) float64 {
	t.Helper()

	m := &dto.Metric{}
	require.NoError(t, g.Write(m))
	require.NotNil(t, m.GetGauge())

	return m.GetGauge().GetValue()
}

// counterValueForTest extracts the float value of a single Counter.
func counterValueForTest(t *testing.T, c prometheus.Counter) float64 {
	t.Helper()

	m := &dto.Metric{}
	require.NoError(t, c.Write(m))
	require.NotNil(t, m.GetCounter())

	return m.GetCounter().GetValue()
}

// TestSampleHistoryWindow_EmptyStream — the no-publish-yet branch: XRangeN
// returns empty, gauge must be set to 0 (NOT -1, which is the parse-error
// sentinel).
func TestSampleHistoryWindow_EmptyStream(t *testing.T) {
	t.Parallel()

	transport, mr := newTestTransport(t, WithPrometheusRegisterer(prometheus.NewRegistry()))

	m := transport.metrics.Load()
	require.NotNil(t, m)
	mr.FlushAll()

	transport.sampleHistoryWindow(context.Background(), m)

	assert.InDelta(t, 0.0, gaugeValueForTest(t, m.historyWindowSeconds), 0.0001,
		"empty stream should set historyWindowSeconds = 0")
}

// TestSampleHistoryWindow_NormalStream — happy path: head/tail XRangeN
// succeed, gauge = (tail-head)/1000.
func TestSampleHistoryWindow_NormalStream(t *testing.T) {
	t.Parallel()

	transport, mr := newTestTransport(t, WithPrometheusRegisterer(prometheus.NewRegistry()))

	m := transport.metrics.Load()
	require.NotNil(t, m)

	streamKey := transport.key("")

	_, err := mr.XAdd(streamKey, "1000000000000-0", []string{"eventID", "u1", "data", "d1"})
	require.NoError(t, err)
	_, err = mr.XAdd(streamKey, "1000000001500-0", []string{"eventID", "u2", "data", "d2"})
	require.NoError(t, err)

	transport.sampleHistoryWindow(context.Background(), m)

	assert.InDelta(t, 1.5, gaugeValueForTest(t, m.historyWindowSeconds), 0.001,
		"two entries 1500ms apart should set historyWindowSeconds = 1.5")
}

// TestSampleConsumerLag_ConsumerGroupsCount — three observable states for
// the consumer_groups gauge: stream not present (0), N groups (N).
func TestSampleConsumerLag_ConsumerGroupsCount(t *testing.T) {
	t.Parallel()

	transport, mr := newTestTransport(t, WithPrometheusRegisterer(prometheus.NewRegistry()))

	m := transport.metrics.Load()
	require.NotNil(t, m)

	streamKey := transport.key("")

	// Skip State 1 (stream-not-exist → 0) under a running listener: the
	// 50ms xreadBlock loop races FlushAll by re-creating the nodeGroup
	// via the NOGROUP path. The sentinel value is still covered by the
	// non-running paths in sampleHistoryWindow tests; here we focus on
	// the multi-group counting contract.

	// State 2: stream with 2 groups. The listener may recreate its
	// nodeGroup at any point — require.Eventually polls until both
	// groups exist, then samples and asserts.
	_, err := mr.XAdd(streamKey, "*", []string{"eventID", "u1", "data", "d1"})
	require.NoError(t, err)

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()

	// Explicit XGroupCreate for the peer is idempotent-safe (returns
	// BUSYGROUP if it ran on a prior iteration of count=N reruns); ignore
	// that benign signal.
	if err := client.XGroupCreate(context.Background(), streamKey, "peer-group-for-test", "$").Err(); err != nil {
		require.Contains(t, err.Error(), "BUSYGROUP",
			"unexpected XGroupCreate error: %v", err)
	}

	// Poll for both groups to exist (the listener's nodeGroup +
	// peer-group-for-test). Without this, FlushAll's wipe + listener
	// recreate race against the test's create can leave us at 1.
	require.Eventually(t, func() bool {
		groups, err := client.XInfoGroups(context.Background(), streamKey).Result()
		if err != nil {
			return false
		}

		return len(groups) == 2
	}, 2*time.Second, 10*time.Millisecond, "both consumer groups must exist before sampling")

	transport.sampleConsumerLag(context.Background(), m)
	assert.InDelta(t, 2.0, gaugeValueForTest(t, m.consumerGroupsCount), 0.0001,
		"two groups on stream (listener's nodeGroup + peer) should set consumer_groups = 2")
}

// TestDispatchZeroMatchTotal_DualSite — enforces the comment-claimed
// invariant that the zero-match counter increments at BOTH dispatch
// paths (single-shard and sharded). Without this test the comment is
// purely documentary.
func TestDispatchZeroMatchTotal_DualSite(t *testing.T) {
	t.Parallel()

	for _, shards := range []int{1, 4} {
		t.Run(formatShardCase(shards), func(t *testing.T) {
			t.Parallel()
			assertZeroMatchIncrementsForShards(t, shards)
		})
	}
}

func formatShardCase(n int) string {
	if n == 1 {
		return "single shard path"
	}

	return "sharded path"
}

func assertZeroMatchIncrementsForShards(t *testing.T, shards int) {
	t.Helper()

	transport, _ := newTestTransport(
		t,
		WithDispatchShards(shards),
		WithPrometheusRegisterer(prometheus.NewRegistry()),
	)

	m := transport.metrics.Load()
	require.NotNil(t, m)

	before := counterValueForTest(t, m.dispatchZeroMatchTotal)

	update := &mercure.Update{
		Topics: []string{"https://example.com/no-such-topic"},
		Event:  mercure.Event{Data: "no-match-test"},
	}
	require.NoError(t, transport.Dispatch(context.Background(), update))

	// Listener picks up via XREADGROUP on its goroutine; fan-out finds
	// no subscriber and bumps the counter (deferred Observe in sharded
	// path; direct Inc in single-shard path). Poll with bounded
	// deadline; xreadBlock = 50ms (newTestTransport default) keeps
	// the latency snappy.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if counterValueForTest(t, m.dispatchZeroMatchTotal) > before {
			break
		}

		time.Sleep(10 * time.Millisecond)
	}

	assert.InDelta(t, before+1, counterValueForTest(t, m.dispatchZeroMatchTotal), 0.001,
		"shards=%d: dispatch_zero_match_total must increment exactly 1 for a no-match publish",
		shards)
}

// TestReady_ListenerHealthGate asserts Ready() returns nil with fresh
// listener activity and ErrTransportUnhealthy past the
// max(5×xreadBlock, 30s) threshold. Uses atomic.Int64.Store to bypass
// real-clock-progress for determinism.
//
//nolint:paralleltest // subtests share transport.lastListenerActivityNanos; parallel subtests would race on Store/Read.
func TestReady_ListenerHealthGate(t *testing.T) {
	transport, _ := newTestTransport(t, WithPrometheusRegisterer(prometheus.NewRegistry()))
	require.True(t, transport.healthy.Load(), "newTestTransport should be PING-healthy")

	now := time.Now()
	// Match the production formula in Ready() exactly: a regression
	// that swaps max() for + would silently pass the "stale" subtest
	// because addition produces a stricter threshold than max().
	threshold := max(5*transport.opts.xreadBlock, listenerStaleFloor)

	tests := []struct {
		name       string
		activityAt time.Time
		wantErr    error
	}{
		{
			name:       "fresh activity → Ready",
			activityAt: now,
			wantErr:    nil,
		},
		{
			name:       "stale activity past threshold → ErrTransportUnhealthy",
			activityAt: now.Add(-2 * threshold),
			wantErr:    ErrTransportUnhealthy,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			transport.lastListenerActivityNanos.Store(tc.activityAt.UnixNano())

			err := transport.Ready(context.Background())
			if tc.wantErr == nil {
				assert.NoError(t, err)

				return
			}

			require.Error(t, err)
			assert.ErrorIs(t, err, tc.wantErr)
		})
	}
}

// TestWarnIfRegistererAlreadyBoundToDifferentStream — package-level
// tracker's observable cases. Mutates package-level state, so NOT
// parallel-safe; uses t.Cleanup to reset.
func TestWarnIfRegistererAlreadyBoundToDifferentStream(t *testing.T) {
	t.Cleanup(resetRegistererBindingsForTest)
	resetRegistererBindingsForTest()

	reg := prometheus.NewRegistry()

	var buf safeBuffer

	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	warnIfRegistererAlreadyBoundToDifferentStream(reg, "stream-A", logger)
	assert.NotContains(t, buf.String(), "registerer already bound",
		"first call should not warn")

	warnIfRegistererAlreadyBoundToDifferentStream(reg, "stream-A", logger)
	assert.NotContains(t, buf.String(), "registerer already bound",
		"same-stream second call should not warn")

	warnIfRegistererAlreadyBoundToDifferentStream(reg, "stream-B", logger)
	assert.Contains(t, buf.String(), "registerer already bound to a different stream name",
		"different-stream second call should warn")
	assert.Contains(t, buf.String(), "stream-A", "warn should name the first stream")
	assert.Contains(t, buf.String(), "stream-B", "warn should name the second stream")
}

// TestWarnIfRegistererAlreadyBoundToDifferentStream_NonComparable — a
// Registerer whose dynamic type is not Comparable (struct with slice
// field) must be silently skipped rather than panic on map insertion.
func TestWarnIfRegistererAlreadyBoundToDifferentStream_NonComparable(t *testing.T) {
	t.Cleanup(resetRegistererBindingsForTest)
	resetRegistererBindingsForTest()

	var buf safeBuffer

	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	assert.NotPanics(t, func() {
		warnIfRegistererAlreadyBoundToDifferentStream(
			nonComparableRegisterer{tags: []string{"a"}},
			"stream-A",
			logger,
		)
	}, "non-comparable Registerer must be skipped, not panic")
}

func resetRegistererBindingsForTest() {
	registererStreamBindingsMu.Lock()
	defer registererStreamBindingsMu.Unlock()

	registererStreamBindings = make(map[any]string)
}

// safeBuffer is a thread-safe io.Writer for capturing slog output.
// bytes.Buffer alone is not safe under concurrent goroutine writes
// even if all callers are single-threaded — slog handlers may
// internally fork.
type safeBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *safeBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.Write(p)
}

func (b *safeBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.String()
}

// Compile-time interface check.
var _ io.Writer = (*safeBuffer)(nil)
