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

// counterValueForTest extracts the float value of a single Counter.
func counterValueForTest(t *testing.T, c prometheus.Counter) float64 {
	t.Helper()

	m := &dto.Metric{}
	require.NoError(t, c.Write(m))
	require.NotNil(t, m.GetCounter())

	return m.GetCounter().GetValue()
}

// TestSampleHistoryWindow_EmptyStream — the no-publish-yet branch: XRangeN
// returns empty, gauge must be set to 0 (not -1, which is the parse-error
// sentinel).
func TestSampleHistoryWindow_EmptyStream(t *testing.T) {
	t.Parallel()

	transport, mr := newTestTransport(t, WithPrometheusRegisterer(prometheus.NewRegistry()))

	m := transport.metrics.Load()
	require.NotNil(t, m)
	mr.FlushAll()

	transport.sampleHistoryWindow(context.Background(), m)

	assert.InDelta(t, 0.0, gaugeValue(t, m.historyWindowSeconds), 0.0001,
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

	assert.InDelta(t, 1.5, gaugeValue(t, m.historyWindowSeconds), 0.001,
		"two entries 1500ms apart should set historyWindowSeconds = 1.5")
}

// TestSampleConsumerLag_ConsumerGroupsCount pins that consumer_groups counts
// every group on the stream (the listener's group plus a peer).
func TestSampleConsumerLag_ConsumerGroupsCount(t *testing.T) {
	t.Parallel()

	transport, mr := newTestTransport(t, WithPrometheusRegisterer(prometheus.NewRegistry()))

	m := transport.metrics.Load()
	require.NotNil(t, m)

	streamKey := transport.key("")

	// The missing-stream case (consumer_groups 0) is not tested here: the running
	// listener re-creates its group through NOGROUP as soon as the stream is gone.

	// The listener may re-create its group at any point, so poll until both
	// groups exist before sampling.
	_, err := mr.XAdd(streamKey, "*", []string{"eventID", "u1", "data", "d1"})
	require.NoError(t, err)

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()

	require.NoError(t, client.XGroupCreate(context.Background(), streamKey, "peer-group-for-test", "$").Err())

	require.Eventually(t, func() bool {
		groups, err := client.XInfoGroups(context.Background(), streamKey).Result()
		if err != nil {
			return false
		}

		return len(groups) == 2
	}, 2*time.Second, 10*time.Millisecond, "both consumer groups must exist before sampling")

	transport.sampleConsumerLag(context.Background(), m)
	assert.InDelta(t, 2.0, gaugeValue(t, m.consumerGroupsCount), 0.0001,
		"two groups on stream (listener's nodeGroup + peer) should set consumer_groups = 2")
}

// TestDispatchZeroMatchTotal_DualSite pins that dispatch_zero_match_total
// increments on both the single-shard and the sharded dispatch path.
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
		Data:   "no-match-test",
	}
	require.NoError(t, transport.Dispatch(context.Background(), update))

	// The listener dispatches on its own goroutine; poll until the counter moves.
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
// tracker's observable cases. Mutates package-level state, so not
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

// safeBuffer is an io.Writer safe for concurrent use, for capturing slog
// output written from transport goroutines.
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

var _ io.Writer = (*safeBuffer)(nil)
