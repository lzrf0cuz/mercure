package redistransport

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"log/slog"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/dunglas/mercure"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetricsRegistration(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	transport, _ := newTestTransport(t, WithPrometheusRegisterer(reg))

	require.NotNil(t, transport.metrics.Load())

	// Verify all metrics are registered by gathering.
	families, err := reg.Gather()
	require.NoError(t, err)

	metricNames := make(map[string]bool)
	for _, f := range families {
		metricNames[f.GetName()] = true
	}

	// Metrics without labels appear immediately in Gather().
	// Metrics with labels (HistogramVec, GaugeVec) only appear after first observation.
	expectedImmediate := []string{
		metricDispatchSubscribersMatched,
		metricPublishDurationSeconds,
		"mercure_redis_xreadgroup_latency_seconds",
		"mercure_redis_xreadgroup_batch_size",
		metricHistoryReplayDurationSeconds,
		"mercure_redis_history_replay_concurrent",
		metricHistoryReplayFallbackTotal,
		metricHistoryReplayTruncatedTotal,
		metricSubscriberAddTotal,
		metricSubscriberRemoveTotal,
		metricHealthy,
		"mercure_redis_stream_length",
		metricClockDriftSeconds,
		"mercure_redis_presence_payload_bytes",
		"mercure_redis_publish_payload_bytes",
		metricTTLCleanupErrorsTotal,
		metricConsumerGroups,
		metricHistoryWindowSeconds,
		metricDispatchZeroMatchTotal,
	}

	for _, name := range expectedImmediate {
		assert.True(t, metricNames[name], "metric %s should be registered", name)
	}

	// Every series carries transport_type="redis" and backend_type. miniredis
	// rejects INFO server, so backend_type stays "unknown".
	for _, f := range families {
		for _, m := range f.GetMetric() {
			gotTransport, gotBackend := "", ""

			for _, l := range m.GetLabel() {
				switch l.GetName() {
				case labelKeyTransportType:
					gotTransport = l.GetValue()
				case labelKeyBackendType:
					gotBackend = l.GetValue()
				}
			}

			assert.Equal(t, "redis", gotTransport,
				"metric %s is missing transport_type=\"redis\" ConstLabel", f.GetName())
			assert.Equal(t, "unknown", gotBackend,
				"metric %s should have backend_type=\"unknown\" (miniredis INFO-rejects → skipVersionCheck → default latch)", f.GetName())
		}
	}

	// Label-dependent metrics (HistogramVec, GaugeVec) need observation first.
	// Verify they exist by doing a dummy observation.
	transport.metrics.Load().dispatchDuration.WithLabelValues("0").Observe(0)
	transport.metrics.Load().shardSubscribers.WithLabelValues("0").Set(0)

	families, err = reg.Gather()
	require.NoError(t, err)

	metricNames = make(map[string]bool)
	for _, f := range families {
		metricNames[f.GetName()] = true
	}

	assert.True(t, metricNames["mercure_redis_dispatch_duration_seconds"],
		"dispatch_duration should appear after observation")
	assert.True(t, metricNames["mercure_redis_shard_subscribers"],
		"shard_subscribers should appear after observation")

	// build_info appears immediately because newMetrics emits a constant-1
	// observation during construction. Confirm the gauge is registered and
	// carries the version/revision/go_version label triple — a future
	// label-list change will fail this.
	require.True(t, metricNames[metricBuildInfo],
		"build_info should be registered and observable")

	for _, f := range families {
		if f.GetName() != metricBuildInfo {
			continue
		}

		require.Len(t, f.GetMetric(), 1, "build_info should emit exactly one series")
		labels := f.GetMetric()[0].GetLabel()
		labelKeys := make(map[string]string, len(labels))

		for _, l := range labels {
			labelKeys[l.GetName()] = l.GetValue()
		}

		assert.Equal(t, Version(), labelKeys["version"],
			"build_info version label must equal Version()'s resolved value")
		assert.NotEmpty(t, labelKeys["go_version"],
			"build_info go_version label must be populated")
		// revision is empty under go test; when set, it is truncated to
		// vcsRevisionShortLen.
		revision, hasRevision := labelKeys["revision"]
		assert.True(t, hasRevision, "build_info must carry a revision label key")
		assert.LessOrEqual(t, len(revision), vcsRevisionShortLen,
			"build_info revision label %q exceeds %d-char truncation contract", revision, vcsRevisionShortLen)
	}
}

func TestMetricsDisabledByDefault(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t)
	assert.Nil(t, transport.metrics.Load(), "metrics should be nil without WithPrometheusRegisterer")
}

// TestRegisterMetricsWithLateBinding covers the Caddy path: the transport is
// built without a registerer and RegisterMetricsWith binds one later. A
// dispatch before the bind records nothing; after it, metrics are emitted.
func TestRegisterMetricsWithLateBinding(t *testing.T) {
	t.Parallel()

	// Constructor path: no registerer, so metrics are nil.
	transport, _ := newTestTransport(t)
	require.Nil(t, transport.metrics.Load(), "precondition: metrics must be nil before late binding")

	// A dispatch with metrics nil must not panic — covers the
	// constructor-path window before Caddy's late binding fires.
	require.NoError(t, transport.Dispatch(context.Background(), &mercure.Update{
		Topics: []string{"https://example.com/test"},
		Data:   "x",
	}))

	// Caddy late binding.
	reg := prometheus.NewRegistry()
	require.NoError(t, transport.RegisterMetricsWith(reg))

	require.NotNil(t, transport.metrics.Load(), "metrics must be bound after RegisterMetricsWith")

	// Now exercise the bound metrics by dispatching and gathering.
	require.NoError(t, transport.Dispatch(context.Background(), &mercure.Update{
		Topics: []string{"https://example.com/test"},
		Data:   "y",
	}))

	families, err := reg.Gather()
	require.NoError(t, err)

	gathered := make(map[string]bool)
	for _, f := range families {
		gathered[f.GetName()] = true
	}

	// publish_duration is a label-less histogram — appears immediately on
	// first observation. If it's absent the late binding is broken.
	assert.True(t, gathered[metricPublishDurationSeconds],
		"publish_duration must appear after late binding + dispatch")
	assert.True(t, gathered[metricBuildInfo],
		"build_info must appear after RegisterMetricsWith")
}

// TestRegisterMetricsWithIdempotent asserts a second call with the same
// registry is a no-op rather than re-registering (which would error with
// AlreadyRegistered or duplicate the pool-stats collector). Two hub
// directives sharing one pooled transport within one Caddy config load
// produce this call pattern. The different-registry case is covered in
// metrics_rebind_test.go.
func TestRegisterMetricsWithIdempotent(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t)

	reg := prometheus.NewRegistry()
	require.NoError(t, transport.RegisterMetricsWith(reg))

	first := transport.metrics.Load()
	require.NotNil(t, first)

	// Second call with the same registry must be a no-op.
	require.NoError(t, transport.RegisterMetricsWith(reg))
	assert.Same(t, first, transport.metrics.Load(),
		"second RegisterMetricsWith on same transport must keep the original *Metrics")
}

// TestRegisterMetricsWithLateBindingSharded mirrors TestRegisterMetricsWithLateBinding
// but with multiple shards, validating that resolveShardChildren correctly sizes
// the shardDispatchObs / shardSubGauges slices when called via the late-bind
// path (not the constructor path). A regression that mis-sizes the slices on a
// sharded transport would either panic on dispatch (index out of range) or
// silently miss dispatches from the higher shard indices.
func TestRegisterMetricsWithLateBindingSharded(t *testing.T) {
	t.Parallel()

	const shards = 4

	transport, _ := newTestTransport(t, WithDispatchShards(shards))
	require.Nil(t, transport.metrics.Load(), "precondition: metrics must be nil before late binding")
	require.Equal(t, shards, transport.numShards, "precondition: sharding must be active")

	// Pre-binding dispatch must not panic across all shards.
	require.NoError(t, transport.Dispatch(context.Background(), &mercure.Update{
		Topics: []string{"https://example.com/test"},
		Data:   "pre-bind",
	}))

	reg := prometheus.NewRegistry()
	require.NoError(t, transport.RegisterMetricsWith(reg))

	m := transport.metrics.Load()
	require.NotNil(t, m)
	require.Len(t, m.shardDispatchObs, shards, "shardDispatchObs slice must be sized to numShards")
	require.Len(t, m.shardSubGauges, shards, "shardSubGauges slice must be sized to numShards")

	// Add subscribers across multiple shards (UUIDs hash deterministically).
	tss := testTopicMatcherStore()
	for range shards * 2 {
		s := mercure.NewLocalSubscriber("", testLogger(), tss)
		s.SetMatchers(topicMatchers([]string{"https://example.com/test"}), nil)
		require.NoError(t, transport.AddSubscriber(context.Background(), s))
	}

	// Drive a publish to populate dispatch_duration histogram across shards.
	require.NoError(t, transport.Dispatch(context.Background(), &mercure.Update{
		Topics: []string{"https://example.com/test"},
		Data:   "post-bind",
	}))

	families, err := reg.Gather()
	require.NoError(t, err)

	gathered := make(map[string]bool)
	for _, f := range families {
		gathered[f.GetName()] = true
	}

	assert.True(t, gathered["mercure_redis_dispatch_duration_seconds"],
		"sharded dispatch must populate per-shard histogram after late binding")
	assert.True(t, gathered["mercure_redis_shard_subscribers"],
		"per-shard subscriber gauge must populate after AddSubscriber post-binding")
}

// TestRegisterMetricsWithConcurrentLoadStore binds a registry while Dispatch
// runs on other goroutines; run it under -race. Dispatches after the bind
// must be counted.
func TestRegisterMetricsWithConcurrentLoadStore(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t)
	require.Nil(t, transport.metrics.Load(), "precondition: metrics must be nil before late binding")

	const dispatchers = 8

	stop := make(chan struct{})

	var wg sync.WaitGroup

	wg.Add(dispatchers)

	for range dispatchers {
		go func() {
			defer wg.Done()

			update := &mercure.Update{
				Topics: []string{"https://example.com/test"},
				Data:   "race",
			}

			for {
				select {
				case <-stop:
					return
				default:
					_ = transport.Dispatch(context.Background(), update)
				}
			}
		}()
	}

	// Let dispatchers warm up against nil metrics.
	time.Sleep(20 * time.Millisecond)

	reg := prometheus.NewRegistry()
	require.NoError(t, transport.RegisterMetricsWith(reg))

	// Let dispatchers run another window with metrics now bound.
	time.Sleep(20 * time.Millisecond)

	close(stop)
	wg.Wait()

	require.NotNil(t, transport.metrics.Load(), "metrics must be bound after RegisterMetricsWith")

	// Verify post-bind dispatches were counted (publish_duration is a
	// label-less histogram; its _count appears as soon as one observation
	// lands post-binding).
	families, err := reg.Gather()
	require.NoError(t, err)

	var observedPublishDuration bool

	for _, f := range families {
		if f.GetName() == metricPublishDurationSeconds {
			for _, m := range f.GetMetric() {
				if m.GetHistogram().GetSampleCount() > 0 {
					observedPublishDuration = true

					break
				}
			}
		}
	}

	assert.True(t, observedPublishDuration,
		"post-bind dispatches must reach publish_duration_seconds histogram")
}

func TestMetricsPublishDuration(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	transport, _ := newTestTransport(t, WithPrometheusRegisterer(reg))

	update := &mercure.Update{
		Topics: []string{"https://example.com/test"},
		Data:   "test",
	}
	require.NoError(t, transport.Dispatch(context.Background(), update))

	families, err := reg.Gather()
	require.NoError(t, err)

	found := false

	for _, f := range families {
		if f.GetName() == metricPublishDurationSeconds {
			found = true

			assert.Positive(t, f.GetMetric()[0].GetHistogram().GetSampleCount(),
				"publish_duration should have at least 1 observation")
		}
	}

	assert.True(t, found)
}

func TestMetricsSubscriberAddRemove(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	transport, _ := newTestTransport(t, WithPrometheusRegisterer(reg))
	tss := testTopicMatcherStore()

	sub := mercure.NewLocalSubscriber("", testLogger(), tss)
	sub.SetMatchers(topicMatchers([]string{"https://example.com/test"}), nil)
	require.NoError(t, transport.AddSubscriber(context.Background(), sub))
	require.NoError(t, transport.RemoveSubscriber(context.Background(), sub))

	families, err := reg.Gather()
	require.NoError(t, err)

	counters := make(map[string]float64)

	for _, f := range families {
		if f.GetName() == metricSubscriberAddTotal {
			counters["add"] = f.GetMetric()[0].GetCounter().GetValue()
		}

		if f.GetName() == metricSubscriberRemoveTotal {
			counters["remove"] = f.GetMetric()[0].GetCounter().GetValue()
		}
	}

	assert.InDelta(t, float64(1), counters["add"], 0)
	assert.InDelta(t, float64(1), counters["remove"], 0)
}

func TestMetricsHealthyGauge(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	_, _ = newTestTransport(t, WithPrometheusRegisterer(reg))

	families, err := reg.Gather()
	require.NoError(t, err)

	for _, f := range families {
		if f.GetName() == metricHealthy {
			assert.InDelta(t, float64(1), f.GetMetric()[0].GetGauge().GetValue(), 0,
				"healthy should default to 1")
		}
	}
}

func TestNewMetricsPanicsOnNilRegisterer(t *testing.T) {
	t.Parallel()

	assert.PanicsWithValue(
		t,
		"redis transport: newMetrics called with nil registerer",
		func() { newMetrics(nil, "") },
	)
}

// TestMetricsReRegisterAdoptsExisting verifies that when newMetrics is called
// twice against the same registry (two RedisTransport instances sharing one
// Prometheus registry — e.g. two hub directives in one Caddy config load),
// the second Metrics struct ends up pointing at the first registration's
// collectors. Without this, the second transport instance would increment
// detached counters that never get scraped, silently zeroing its metrics.
func TestMetricsReRegisterAdoptsExisting(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()

	m1 := newMetrics(reg, serverTypeRedis)
	m2 := newMetrics(reg, serverTypeRedis)

	// Counters, gauges, and vectors must be the same live collectors in both instances.
	assert.Same(t, m1.publishDuration, m2.publishDuration)
	assert.Same(t, m1.subscriberAddTotal, m2.subscriberAddTotal)
	assert.Same(t, m1.subscribersLost, m2.subscribersLost)
	assert.Same(t, m1.shardSubscribers, m2.shardSubscribers)
	assert.Same(t, m1.healthy, m2.healthy)
	assert.Same(t, m1.buildInfo, m2.buildInfo)

	// Incrementing via m2 must appear on m1's view (because they're the same object).
	m2.subscriberAddTotal.Inc()

	families, err := reg.Gather()
	require.NoError(t, err)

	var got float64

	var buildInfoSeries int

	var buildInfoValue float64

	for _, f := range families {
		switch f.GetName() {
		case metricSubscriberAddTotal:
			got = f.GetMetric()[0].GetCounter().GetValue()
		case metricBuildInfo:
			buildInfoSeries = len(f.GetMetric())

			if buildInfoSeries > 0 {
				buildInfoValue = f.GetMetric()[0].GetGauge().GetValue()
			}
		}
	}

	assert.InDelta(t, float64(1), got, 0)
	// Both newMetrics calls Set(1) on the adopted build_info gauge, so exactly
	// one series exists, with value 1.
	assert.Equal(t, 1, buildInfoSeries, "build_info should emit exactly one series after re-registration")
	assert.InDelta(t, float64(1), buildInfoValue, 0, "build_info series should hold the constant value 1")
}

// TestDispatchSubscribersMatchedRecordsOncePerMessage pins that
// dispatch_subscribers_matched records one sample per message, summed across
// shards, not one per shard.
func TestDispatchSubscribersMatchedRecordsOncePerMessage(t *testing.T) {
	t.Parallel()

	const shards = 4

	reg := prometheus.NewRegistry()
	transport, _ := newShardedTestTransport(t, shards, WithPrometheusRegisterer(reg))
	tss := testTopicMatcherStore()

	// Distribute subscribers across shards by relying on UUID hashing.
	const subs = 16
	for range subs {
		s := mercure.NewLocalSubscriber("", testLogger(), tss)
		s.SetMatchers(topicMatchers([]string{"https://example.com/match"}), nil)
		require.NoError(t, transport.AddSubscriber(context.Background(), s))
	}

	require.NoError(t, transport.Dispatch(context.Background(), &mercure.Update{
		Topics: []string{"https://example.com/match"},
		Data:   "x",
	}))

	// Wait for fan-out + listener cycle to record the histogram observation.
	require.Eventually(t, func() bool {
		families, err := reg.Gather()
		if err != nil {
			return false
		}

		for _, f := range families {
			if f.GetName() != metricDispatchSubscribersMatched {
				continue
			}

			h := f.GetMetric()[0].GetHistogram()

			return h.GetSampleCount() >= 1
		}

		return false
	}, 2*time.Second, 10*time.Millisecond, "histogram should record at least one observation")

	families, err := reg.Gather()
	require.NoError(t, err)

	for _, f := range families {
		if f.GetName() != metricDispatchSubscribersMatched {
			continue
		}

		h := f.GetMetric()[0].GetHistogram()
		// One observation per message, even with 4 shards.
		assert.Equal(t, uint64(1), h.GetSampleCount(),
			"sample count must be once-per-message, not once-per-shard")
		// Every subscriber matched, so the sum is the total across all shards.
		assert.InDelta(t, float64(subs), h.GetSampleSum(), 0,
			"sum across shards must equal total matched subscribers")
	}
}

// TestPoolStatsCollectorReRegisterSwapsClient pins that when two transports
// share a registry, the second one's RegisterMetricsWith points the registered
// pool-stats collector at its own client: with t1 closed, the scraped pool
// stats must come from t2's live pool.
func TestPoolStatsCollectorReRegisterSwapsClient(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()

	t1, _ := newTestTransport(t, WithPrometheusRegisterer(reg))
	t2, _ := newTestTransport(t, WithPrometheusRegisterer(reg))

	// PoolStats on a closed pool is zero, so non-zero stats after closing t1
	// prove the collector reads t2's client.
	require.NoError(t, t1.Close(context.Background()))

	// Drive operations through t2 to push pool counters above zero.
	for range 5 {
		require.NoError(t, t2.Dispatch(context.Background(), &mercure.Update{
			Topics: []string{"https://example.com/swap-test"},
			Data:   "v",
		}))
	}

	families, err := reg.Gather()
	require.NoError(t, err)

	var (
		totalConns float64
		seenConns  bool
	)

	for _, f := range families {
		if f.GetName() == "mercure_redis_client_pool_total_conns" {
			seenConns = true

			for _, m := range f.GetMetric() {
				totalConns += m.GetGauge().GetValue()
			}
		}
	}

	assert.True(t, seenConns, "pool_total_conns descriptor must remain registered after the second transport's init swapped its client")
	assert.Greater(t, totalConns, float64(0),
		"pool_total_conns must come from t2's live client, not t1's closed pool")
}

// nonComparableRegisterer is a Registerer whose dynamic type is a value type
// with a slice field, so comparing two of them with == panics. registered,
// when non-nil, counts Register calls.
type nonComparableRegisterer struct {
	tags       []string
	registered *atomic.Int64
}

func (r nonComparableRegisterer) Register(prometheus.Collector) error {
	if r.registered != nil {
		r.registered.Add(1)
	}

	return nil
}

func (nonComparableRegisterer) MustRegister(...prometheus.Collector) {}
func (nonComparableRegisterer) Unregister(prometheus.Collector) bool { return false }

// TestRegisterMetricsWithAdditionalRegistryLogsInfo verifies the log side of
// binding a second registry: a repeat call with the same registerer is
// silent, a call with a different registerer logs one Info line (the
// collectors are now on that registry too) and no Warn.
func TestRegisterMetricsWithAdditionalRegistryLogsInfo(t *testing.T) {
	t.Parallel()

	logBuf := &safeBuffer{}
	logger := slog.New(slog.NewJSONHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelInfo}))

	reg1 := prometheus.NewRegistry()
	transport, _ := newTestTransport(t, WithPrometheusRegisterer(reg1), WithLogger(logger))

	// Construction logs (e.g. the skipped INFO check on miniredis) are not
	// under test.
	logOffset := len(logBuf.String())

	// Same-instance second call: silent (two hub directives sharing one
	// pooled transport and registry within one Caddy config load).
	require.NoError(t, transport.RegisterMetricsWith(reg1))
	assert.NotContains(t, logBuf.String()[logOffset:], "additional registry",
		"second call with the same registerer must not log")

	// Different-instance second call: Info, not Warn.
	reg2 := prometheus.NewRegistry()
	require.NoError(t, transport.RegisterMetricsWith(reg2))

	callLog := logBuf.String()[logOffset:]
	assert.Contains(t, callLog, `"level":"INFO","msg":"redis transport: metrics registered on an additional registry"`,
		"second call with a different registerer must log the additional registration at Info")
	assert.NotContains(t, callLog, `"level":"WARN"`,
		"binding an additional registry is expected on Caddy reload and must not Warn")
}

func TestMetricsSubscribersLostOnShutdown(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	transport, _ := newTestTransport(t, WithPrometheusRegisterer(reg))
	tss := testTopicMatcherStore()

	// Add two subscribers and hold them so they're present at Close.
	for range 2 {
		sub := mercure.NewLocalSubscriber("", testLogger(), tss)
		sub.SetMatchers(topicMatchers([]string{"https://example.com/test"}), nil)
		require.NoError(t, transport.AddSubscriber(context.Background(), sub))
	}

	// Close triggers disconnectAllSubscribers, which emits subscribers_lost{reason="shutdown"}.
	require.NoError(t, transport.Close(context.Background()))

	families, err := reg.Gather()
	require.NoError(t, err)

	var shutdownCount float64

	for _, f := range families {
		if f.GetName() != metricSubscribersLostTotal {
			continue
		}

		for _, m := range f.GetMetric() {
			for _, label := range m.GetLabel() {
				if label.GetName() == labelKeyReason && label.GetValue() == "shutdown" {
					shutdownCount = m.GetCounter().GetValue()
				}
			}
		}
	}

	assert.InDelta(t, float64(2), shutdownCount, 0,
		"both subscribers should be counted against shutdown reason")
}

// TestDisconnectAllSubscribersAcrossShards exercises the cross-shard walk in
// disconnectAllSubscribers. With 50 subscribers at 4 shards, xxhash keeps
// every shard populated, and Close must count all 50 — any off-by-one in
// the shard-walk bound (`for i := range t.numShards`) would miss a shard.
func TestDisconnectAllSubscribersAcrossShards(t *testing.T) {
	t.Parallel()

	const (
		shards = 4
		nSubs  = 50
	)

	reg := prometheus.NewRegistry()
	transport, _ := newShardedTestTransport(t, shards, WithPrometheusRegisterer(reg))
	tss := testTopicMatcherStore()

	for range nSubs {
		sub := mercure.NewLocalSubscriber("", testLogger(), tss)
		sub.SetMatchers(topicMatchers([]string{"https://example.com/test"}), nil)
		require.NoError(t, transport.AddSubscriber(context.Background(), sub))
	}

	require.NoError(t, transport.Close(context.Background()))

	families, err := reg.Gather()
	require.NoError(t, err)

	var shutdownCount float64

	for _, f := range families {
		if f.GetName() != metricSubscribersLostTotal {
			continue
		}

		for _, m := range f.GetMetric() {
			for _, label := range m.GetLabel() {
				if label.GetName() == labelKeyReason && label.GetValue() == "shutdown" {
					shutdownCount = m.GetCounter().GetValue()
				}
			}
		}
	}

	assert.InDelta(t, float64(nSubs), shutdownCount, 0,
		"every subscriber across all shards should be counted")
}

// TestMetricsSubscribersLostOnBackpressure verifies that Dispatch returning
// false during live dispatch increments subscribers_lost{reason="backpressure"}.
// The dominant trigger in production is handleFullChan firing on a full
// out-channel (slow consumer), but Dispatch also returns false when the
// subscriber was already disconnected by a concurrent path — both cases
// land in this counter. This test exercises the simpler "already
// disconnected" path because pre-filling the 1000-slot out-channel buffer
// is unreliable under -race; the increment logic is identical.
func TestMetricsSubscribersLostOnBackpressure(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	transport, _ := newTestTransport(t, WithPrometheusRegisterer(reg))
	tss := testTopicMatcherStore()

	sub := mercure.NewLocalSubscriber("", testLogger(), tss)
	sub.SetMatchers(topicMatchers([]string{"https://example.com/test"}), nil)
	require.NoError(t, transport.AddSubscriber(context.Background(), sub))

	// Force Dispatch to return false on the next call: doDisconnect closes
	// s.out and flips s.disconnected, so Dispatch's first branch returns
	// false without touching handleFullChan.
	sub.Disconnect()

	update := &mercure.Update{
		Topics: []string{"https://example.com/test"},
		Data:   "x",
	}

	transport.mu.Lock()
	transport.dispatchToSubscribers(context.Background(), update)
	transport.mu.Unlock()

	require.NoError(t, transport.Close(context.Background()))

	families, err := reg.Gather()
	require.NoError(t, err)

	var backpressureCount, totalLost float64

	for _, f := range families {
		if f.GetName() != metricSubscribersLostTotal {
			continue
		}

		for _, m := range f.GetMetric() {
			totalLost += m.GetCounter().GetValue()

			for _, label := range m.GetLabel() {
				if label.GetName() == labelKeyReason && label.GetValue() == "backpressure" {
					backpressureCount = m.GetCounter().GetValue()
				}
			}
		}
	}

	assert.InDelta(t, float64(1), backpressureCount, 0,
		"Dispatch returning false during live dispatch should be counted as backpressure")
	// Close's disconnectAllSubscribers Walk also calls markLost(s, shutdown)
	// for the same subscriber. The CAS gate must reject that second attempt,
	// so the cross-reason total stays at exactly 1.
	assert.InDelta(t, float64(1), totalLost, 0,
		"backpressure → shutdown CAS gating: total subscribers_lost across all reasons should remain 1 after Close")
}

// TestMarkLostAtMostOncePerSubscriber races markLost for one subscriber with
// different reasons: subscribers_lost must total exactly 1 across all reason
// labels (the lostFlags CAS).
func TestMarkLostAtMostOncePerSubscriber(t *testing.T) {
	t.Parallel()

	const racers = 50

	reg := prometheus.NewRegistry()
	transport, _ := newTestTransport(t, WithPrometheusRegisterer(reg))
	tss := testTopicMatcherStore()

	sub := mercure.NewLocalSubscriber("", testLogger(), tss)
	sub.SetMatchers(topicMatchers([]string{"https://example.com/test"}), nil)
	require.NoError(t, transport.AddSubscriber(context.Background(), sub))

	reasons := []subscriberLossReason{
		lossReasonShutdown,
		lossReasonHistoryReplayFailed,
		lossReasonBackpressure,
	}

	var (
		wg    sync.WaitGroup
		ready atomic.Int32
		start = make(chan struct{})
	)
	wg.Add(racers)

	for i := range racers {
		go func() {
			defer wg.Done()

			ready.Add(1)
			<-start
			transport.markLost(sub, reasons[i%len(reasons)])
		}()
	}

	for ready.Load() < racers {
		time.Sleep(time.Millisecond)
	}

	close(start)
	wg.Wait()

	require.NoError(t, transport.Close(context.Background()))

	families, err := reg.Gather()
	require.NoError(t, err)

	var total float64

	for _, f := range families {
		if f.GetName() != metricSubscribersLostTotal {
			continue
		}

		for _, m := range f.GetMetric() {
			total += m.GetCounter().GetValue()
		}
	}

	assert.InDelta(t, float64(1), total, 0,
		"subscribers_lost must be exactly 1 across %d racing markLost calls (got %v)", racers, total)
}

// TestDisconnectAllSubscribersNoMetrics pins that Close with metrics disabled
// (nil registerer) does not panic in disconnectAllSubscribers.
func TestDisconnectAllSubscribersNoMetrics(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t) // no WithPrometheusRegisterer
	tss := testTopicMatcherStore()

	sub := mercure.NewLocalSubscriber("", testLogger(), tss)
	sub.SetMatchers(topicMatchers([]string{"https://example.com/test"}), nil)
	require.NoError(t, transport.AddSubscriber(context.Background(), sub))

	require.Nil(t, transport.metrics.Load(), "precondition: metrics should be nil")
	assert.NotPanics(t, func() {
		_ = transport.Close(context.Background())
	})
}

// TestUnboundMetricsErrorPathsDoNotPanic drives, with no metrics bound (a Go
// API transport built without WithPrometheusRegisterer), paths whose record*
// call relies on the method's nil-receiver guard: a failed publish, a failed
// presence heartbeat, a consumer-group re-create and a Pass 2 history replay.
func TestUnboundMetricsErrorPathsDoNotPanic(t *testing.T) {
	t.Parallel()

	const topic = "https://example.com/unbound-metrics"

	hook := &failWritesHook{}
	transport := newUnboundTransport(t, miniredis.RunT(t), hook)
	require.Nil(t, transport.metrics.Load(), "precondition: metrics must be unbound")

	ctx := t.Context()

	var err error

	hook.on.Store(true)
	require.NotPanics(t, func() {
		err = transport.Dispatch(ctx, &mercure.Update{Topics: []string{topic}, Data: "x"})
	}, "failed publish (recordPublishError)")
	require.ErrorIs(t, err, errRedisWriteDown)
	require.NotPanics(t, func() { transport.publishPresence(ctx) }, "failed presence heartbeat (recordPresenceHeartbeatError)")
	hook.on.Store(false)

	// The group exists, so the re-create fails with BUSYGROUP after recording
	// nogroup; the short ctx cuts the backoff that follows.
	recreateCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()

	require.NotPanics(t, func() { transport.recreateConsumerGroup(recreateCtx, transport.key(""), 1) },
		"consumer-group re-create (recordXReadgroupError)")

	// An ID the stream does not hold makes Pass 1 miss, so Pass 2 runs.
	s := mercure.NewLocalSubscriber("unknown-event-id", testLogger(), testTopicMatcherStore())
	s.SetMatchers(topicMatchers([]string{topic}), nil)
	require.NotPanics(t, func() { err = transport.AddSubscriber(ctx, s) }, "Pass 2 history replay (recordHistoryReplayFullScan)")
	require.NoError(t, err)
}

// TestRecordPresenceReadErrorAllKinds covers both kinds through the helper:
// kind="get" cannot be provoked on miniredis without a client wrapper.
// TestMetricsPresenceReadErrorsUnmarshalKind covers kind="unmarshal" end to end.
func TestRecordPresenceReadErrorAllKinds(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m := newMetrics(reg, serverTypeRedis)

	m.recordPresenceReadError(presenceReadErrorGet)
	m.recordPresenceReadError(presenceReadErrorGet)
	m.recordPresenceReadError(presenceReadErrorUnmarshal)

	families, err := reg.Gather()
	require.NoError(t, err)

	counts := map[string]float64{}

	for _, f := range families {
		if f.GetName() != metricPresenceReadErrorsTotal {
			continue
		}

		for _, metric := range f.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == labelKeyKind {
					counts[label.GetValue()] = metric.GetCounter().GetValue()
				}
			}
		}
	}

	assert.InDelta(t, float64(2), counts["get"], 0)
	assert.InDelta(t, float64(1), counts["unmarshal"], 0)
}

// TestMetricsPresenceReadErrorsUnmarshalKind stores malformed JSON under a
// presence key and pins that GetSubscribers still succeeds and increments
// presence_read_errors_total{kind="unmarshal"}.
func TestMetricsPresenceReadErrorsUnmarshalKind(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	transport, mr := newTestTransport(t, WithPrometheusRegisterer(reg))

	require.NoError(t, mr.Set("{mercure}:presence:badnode", "not-valid-json{"))

	_, _, err := transport.GetSubscribers(context.Background())
	require.NoError(t, err, "partial per-key errors should not fail the whole call")

	families, err := reg.Gather()
	require.NoError(t, err)

	var unmarshalCount float64

	for _, f := range families {
		if f.GetName() != metricPresenceReadErrorsTotal {
			continue
		}

		for _, m := range f.GetMetric() {
			for _, label := range m.GetLabel() {
				if label.GetName() == labelKeyKind && label.GetValue() == "unmarshal" {
					unmarshalCount = m.GetCounter().GetValue()
				}
			}
		}
	}

	assert.InDelta(t, float64(1), unmarshalCount, 0,
		"malformed presence value should increment unmarshal counter")
}

func TestMetricsHistoryReplay(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	transport, mr := newTestTransport(t, WithPrometheusRegisterer(reg))
	tss := testTopicMatcherStore()

	// Dispatch a message to create history.
	update := &mercure.Update{
		Topics: []string{"https://example.com/test"},
		Data:   "history data",
	}
	require.NoError(t, transport.Dispatch(context.Background(), update))
	mr.FastForward(100 * time.Millisecond)

	// Add subscriber with history replay.
	sub := mercure.NewLocalSubscriber(mercure.EarliestLastEventID, testLogger(), tss)
	sub.SetMatchers(topicMatchers([]string{"https://example.com/test"}), nil)
	require.NoError(t, transport.AddSubscriber(context.Background(), sub))

	families, err := reg.Gather()
	require.NoError(t, err)

	found := false

	for _, f := range families {
		if f.GetName() == metricHistoryReplayDurationSeconds {
			found = true

			assert.Positive(t, f.GetMetric()[0].GetHistogram().GetSampleCount(),
				"history_replay_duration should have at least 1 observation")
		}
	}

	assert.True(t, found)
}

// TestRecordStreamDecodeErrorAllKinds verifies every kind value increments the
// correct labeled counter. Keep the recorded/asserted set in step with the
// streamDecodeErrorKind enum when a kind is added.
func TestRecordStreamDecodeErrorAllKinds(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m := newMetrics(reg, serverTypeRedis)

	m.recordStreamDecodeError(streamDecodeErrorDataType)
	m.recordStreamDecodeError(streamDecodeErrorDataType)
	m.recordStreamDecodeError(streamDecodeErrorUnmarshal)
	m.recordStreamDecodeError(streamDecodeErrorForbiddenSSEChars)

	families, err := reg.Gather()
	require.NoError(t, err)

	counts := map[string]float64{}

	for _, f := range families {
		if f.GetName() != metricStreamDecodeErrorsTotal {
			continue
		}

		for _, metric := range f.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == labelKeyKind {
					counts[label.GetValue()] = metric.GetCounter().GetValue()
				}
			}
		}
	}

	assert.InDelta(t, 2.0, counts["data_type"], 1e-9)
	assert.InDelta(t, 1.0, counts["unmarshal"], 1e-9)
	assert.InDelta(t, 1.0, counts["forbidden_sse_chars"], 1e-9)
}

// TestStreamDecodeErrorKindsAreAllDocumented parses metrics.go for every
// streamDecodeErrorKind constant and pins that each value appears in the
// stream_decode_errors_total Help string.
func TestStreamDecodeErrorKindsAreAllDocumented(t *testing.T) {
	t.Parallel()

	values := enumStringValues(t, "metrics.go", "streamDecodeErrorKind")
	require.GreaterOrEqual(t, len(values), 3, "expected at least the 3 known decode-error kinds")

	reg := prometheus.NewRegistry()
	m := newMetrics(reg, serverTypeRedis)
	m.recordStreamDecodeError(streamDecodeErrorForbiddenSSEChars) // force the series to appear

	families, err := reg.Gather()
	require.NoError(t, err)

	var help string

	for _, f := range families {
		if f.GetName() == metricStreamDecodeErrorsTotal {
			help = f.GetHelp()
		}
	}

	require.NotEmpty(t, help, "stream_decode_errors_total Help not found")

	for _, v := range values {
		assert.Containsf(t, help, v,
			"streamDecodeErrorKind %q is not documented in the metric Help — add it (operators alert on these label values)", v)
	}
}

// enumStringValues parses a source file and returns the string literal values of
// every const declared with the given named string type.
func enumStringValues(t *testing.T, filename, typeName string) []string {
	t.Helper()

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, filename, nil, 0)
	require.NoError(t, err)

	var values []string

	ast.Inspect(file, func(n ast.Node) bool {
		vs, ok := n.(*ast.ValueSpec)
		if !ok {
			return true
		}

		id, ok := vs.Type.(*ast.Ident)
		if !ok || id.Name != typeName {
			return true
		}

		for _, v := range vs.Values {
			if lit, ok := v.(*ast.BasicLit); ok && lit.Kind == token.STRING {
				s, err := strconv.Unquote(lit.Value)
				require.NoError(t, err)

				values = append(values, s)
			}
		}

		return true
	})

	return values
}

// TestStreamDecodeErrorsMetricNameLiteral pins the wire name: the Grafana
// dashboard and README use it literally, so changing the constant's value
// would break them while every other test still passes.
func TestStreamDecodeErrorsMetricNameLiteral(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "mercure_redis_stream_decode_errors_total", metricStreamDecodeErrorsTotal)
}

// TestRecordZombieGCErrorIncrements verifies the counter increments on the
// live receiver path.
func TestRecordZombieGCErrorIncrements(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m := newMetrics(reg, serverTypeRedis)

	m.recordZombieGCError()
	m.recordZombieGCError()

	families, err := reg.Gather()
	require.NoError(t, err)

	var got float64

	for _, f := range families {
		if f.GetName() == "mercure_redis_zombie_gc_errors_total" {
			got = f.GetMetric()[0].GetCounter().GetValue()
		}
	}

	assert.InDelta(t, 2.0, got, 1e-9)
}

// TestRecordPublishErrorKinds verifies both kinds increment the vec.
func TestRecordPublishErrorKinds(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m := newMetrics(reg, serverTypeRedis)

	m.recordPublishError(publishErrorEncode)
	m.recordPublishError(publishErrorPublish)
	m.recordPublishError(publishErrorPublish)

	families, err := reg.Gather()
	require.NoError(t, err)

	counts := map[string]float64{}

	for _, f := range families {
		if f.GetName() != "mercure_redis_publish_errors_total" {
			continue
		}

		for _, metric := range f.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == labelKeyKind {
					counts[label.GetValue()] = metric.GetCounter().GetValue()
				}
			}
		}
	}

	assert.InDelta(t, 1.0, counts["encode"], 1e-9)
	assert.InDelta(t, 2.0, counts["publish"], 1e-9)
}

// TestRecordXReadgroupErrorKinds verifies all 4 kinds increment the vec.
func TestRecordXReadgroupErrorKinds(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m := newMetrics(reg, serverTypeRedis)

	m.recordXReadgroupError(xreadgroupErrorNoGroup)
	m.recordXReadgroupError(xreadgroupErrorNoGroupRecreateFailed)
	m.recordXReadgroupError(xreadgroupErrorOther)
	m.recordXReadgroupError(xreadgroupErrorOther)
	m.recordXReadgroupError(xreadgroupErrorXAck)
	m.recordXReadgroupError(xreadgroupErrorXAck)
	m.recordXReadgroupError(xreadgroupErrorXAck)

	families, err := reg.Gather()
	require.NoError(t, err)

	counts := map[string]float64{}

	for _, f := range families {
		if f.GetName() != "mercure_redis_xreadgroup_errors_total" {
			continue
		}

		for _, metric := range f.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == labelKeyKind {
					counts[label.GetValue()] = metric.GetCounter().GetValue()
				}
			}
		}
	}

	assert.InDelta(t, 1.0, counts["nogroup"], 1e-9)
	assert.InDelta(t, 1.0, counts["nogroup_recreate_failed"], 1e-9)
	assert.InDelta(t, 2.0, counts["other"], 1e-9)
	assert.InDelta(t, 3.0, counts["xack"], 1e-9)
}

// TestRecordPresenceHeartbeatError verifies the counter increments.
func TestRecordPresenceHeartbeatError(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m := newMetrics(reg, serverTypeRedis)

	m.recordPresenceHeartbeatError()
	m.recordPresenceHeartbeatError()
	m.recordPresenceHeartbeatError()

	families, err := reg.Gather()
	require.NoError(t, err)

	var got float64

	for _, f := range families {
		if f.GetName() == "mercure_redis_presence_heartbeat_errors_total" {
			got = f.GetMetric()[0].GetCounter().GetValue()
		}
	}

	assert.InDelta(t, 3.0, got, 1e-9)
}

// TestMetricsDispatchDurationSequentialPath pins that the single-shard path
// (the default) records dispatch_duration_seconds.
func TestMetricsDispatchDurationSequentialPath(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	transport, _ := newTestTransport(t, WithPrometheusRegisterer(reg))

	// Default config means numShards == 1; dispatchToSubscribers takes the
	// sequential branch.
	require.Equal(t, 1, transport.numShards)

	tss := testTopicMatcherStore()
	sub := mercure.NewLocalSubscriber("", testLogger(), tss)
	sub.SetMatchers(topicMatchers([]string{"https://example.com/test"}), nil)
	require.NoError(t, transport.AddSubscriber(context.Background(), sub))

	update := &mercure.Update{
		Topics: []string{"https://example.com/test"},
		Data:   "hi",
	}

	transport.mu.Lock()
	transport.dispatchToSubscribers(context.Background(), update)
	transport.mu.Unlock()

	families, err := reg.Gather()
	require.NoError(t, err)

	var observed bool

	for _, f := range families {
		if f.GetName() != "mercure_redis_dispatch_duration_seconds" {
			continue
		}

		for _, m := range f.GetMetric() {
			for _, label := range m.GetLabel() {
				if label.GetName() == labelKeyShard && label.GetValue() == "0" &&
					m.GetHistogram().GetSampleCount() >= 1 {
					observed = true
				}
			}
		}
	}

	assert.True(t, observed,
		"dispatch_duration_seconds{shard=\"0\"} should record at least one observation in the sequential path")
}

// TestMetricsClockDriftGaugeSet verifies that NewRedisTransport sets
// clock_drift_seconds even when drift is below the warn threshold — operators
// need the gauge to be populated unconditionally for alerting on growth.
func TestMetricsClockDriftGaugeSet(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	_, _ = newTestTransport(t, WithPrometheusRegisterer(reg))

	families, err := reg.Gather()
	require.NoError(t, err)

	var found bool

	for _, f := range families {
		if f.GetName() != metricClockDriftSeconds {
			continue
		}

		require.Len(t, f.GetMetric(), 1)
		// Any value is valid; the gauge appears in Gather() only once set, so its
		// presence proves NewRedisTransport set it.
		found = true
	}

	assert.True(t, found, "clock_drift_seconds should be populated by NewRedisTransport")
}

// TestRecordTTLCleanupError verifies the TTL-cleanup error counter increments.
func TestRecordTTLCleanupError(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	transport, _ := newTestTransport(t, WithPrometheusRegisterer(reg))

	transport.metrics.Load().recordTTLCleanupError()
	transport.metrics.Load().recordTTLCleanupError()

	families, err := reg.Gather()
	require.NoError(t, err)

	var got float64

	for _, f := range families {
		if f.GetName() == metricTTLCleanupErrorsTotal {
			got = f.GetMetric()[0].GetCounter().GetValue()
		}
	}

	assert.InDelta(t, 2.0, got, 1e-9)
}

// TestSampleConsumerLagPopulatesGauges verifies sampleConsumerLag updates
// both lag and pending gauges from XINFO GROUPS for this node's consumer
// group. With no traffic the gauges should converge to 0/0.
func TestSampleConsumerLagPopulatesGauges(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	transport, _ := newTestTransport(t, WithPrometheusRegisterer(reg))

	transport.sampleConsumerLag(context.Background(), transport.metrics.Load())

	families, err := reg.Gather()
	require.NoError(t, err)

	var (
		foundLag, foundPending bool
		lagValue, pendingValue float64
	)

	for _, f := range families {
		switch f.GetName() {
		case metricConsumerLag:
			foundLag = true
			lagValue = f.GetMetric()[0].GetGauge().GetValue()
		case metricConsumerPending:
			foundPending = true
			pendingValue = f.GetMetric()[0].GetGauge().GetValue()
		}
	}

	assert.True(t, foundLag, "consumer_lag should be populated by sampleConsumerLag")
	assert.True(t, foundPending, "consumer_pending should be populated by sampleConsumerLag")
	// At-rest miniredis: no entries published, no PEL backlog.
	assert.InDelta(t, 0.0, lagValue, 0, "consumer_lag should be 0 with no published entries")
	assert.InDelta(t, 0.0, pendingValue, 0, "consumer_pending should be 0 with no XREADGROUP backlog")
}

// TestConsumerGroupGaugeValues pins the XInfoGroup field mapping: Lag -> lag,
// Pending -> pending.
func TestConsumerGroupGaugeValues(t *testing.T) {
	t.Parallel()

	lag, pending := consumerGroupGaugeValues(redis.XInfoGroup{
		Lag:     17,
		Pending: 3,
	})

	assert.InDelta(t, 17.0, lag, 0, "lag should map from g.Lag")
	assert.InDelta(t, 3.0, pending, 0, "pending should map from g.Pending")
}

// TestSampleConsumerLagResetOnMissingStream pins that when XINFO GROUPS
// reports a missing stream, the lag and pending gauges are reset to 0 instead
// of keeping a stale value.
func TestSampleConsumerLagResetOnMissingStream(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	transport, mr := newTestTransport(t, WithPrometheusRegisterer(reg))

	// Pre-populate the gauges with non-zero so the reset is observable.
	transport.metrics.Load().consumerLag.Set(99)
	transport.metrics.Load().consumerPending.Set(99)

	// Delete the stream out from under the transport — simulates external
	// trim or DEL after the consumer group was already created at startup.
	mr.Del(transport.key(""))

	transport.sampleConsumerLag(context.Background(), transport.metrics.Load())

	families, err := reg.Gather()
	require.NoError(t, err)

	var lagValue, pendingValue float64

	for _, f := range families {
		switch f.GetName() {
		case metricConsumerLag:
			lagValue = f.GetMetric()[0].GetGauge().GetValue()
		case metricConsumerPending:
			pendingValue = f.GetMetric()[0].GetGauge().GetValue()
		}
	}

	assert.InDelta(t, 0.0, lagValue, 0,
		"consumer_lag should reset to 0 when the stream does not exist")
	assert.InDelta(t, 0.0, pendingValue, 0,
		"consumer_pending should reset to 0 when the stream does not exist")
}

// TestRecordHistoryReplayFallback verifies the fallback counter increments.
func TestRecordHistoryReplayFallback(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	transport, _ := newTestTransport(t, WithPrometheusRegisterer(reg))

	transport.metrics.Load().recordHistoryReplayFullScan()

	families, err := reg.Gather()
	require.NoError(t, err)

	var got float64

	for _, f := range families {
		if f.GetName() == metricHistoryReplayFallbackTotal {
			got = f.GetMetric()[0].GetCounter().GetValue()
		}
	}

	assert.InDelta(t, 1.0, got, 1e-9)
}

// TestRecordHistoryReplayTruncated verifies the truncated counter increments —
// the aggregate companion to the mercure.history.truncated span attribute.
func TestRecordHistoryReplayTruncated(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	transport, _ := newTestTransport(t, WithPrometheusRegisterer(reg))

	transport.metrics.Load().recordHistoryReplayTruncated()

	families, err := reg.Gather()
	require.NoError(t, err)

	var (
		got   float64
		found bool
	)

	for _, f := range families {
		if f.GetName() == metricHistoryReplayTruncatedTotal {
			got = f.GetMetric()[0].GetCounter().GetValue()
			found = true
		}
	}

	// Check registration first so a dropped collector fails with a clear message.
	require.True(t, found, "metric family %q not found in registry — registerAll likely dropped historyReplayTruncated", metricHistoryReplayTruncatedTotal)
	assert.InDelta(t, 1.0, got, 1e-9)
}

// TestConstLabelsForReturnsExpectedLabels pins the transport_type and
// backend_type const labels as literals, since dashboards filter on them.
func TestConstLabelsForReturnsExpectedLabels(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name        string
		backendType serverType
		want        prometheus.Labels
	}{
		{
			name:        "redis backend",
			backendType: serverTypeRedis,
			want:        prometheus.Labels{"transport_type": "redis", "backend_type": "redis"},
		},
		{
			name:        "valkey backend",
			backendType: serverTypeValkey,
			want:        prometheus.Labels{"transport_type": "redis", "backend_type": "valkey"},
		},
		{
			name:        "unknown backend",
			backendType: serverTypeUnknown,
			want:        prometheus.Labels{"transport_type": "redis", "backend_type": "unknown"},
		},
		{
			name:        "empty normalizes to unknown",
			backendType: "",
			want:        prometheus.Labels{"transport_type": "redis", "backend_type": "unknown"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := constLabelsFor(tc.backendType)
			assert.Equal(t, tc.want, got)
		})
	}
}

// TestMetricsTransportTypeLabelsReadsBackendField pins that
// transportTypeLabels reports the backendType passed to newMetrics.
func TestMetricsTransportTypeLabelsReadsBackendField(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name             string
		newMetricsArg    serverType
		wantBackendLabel string
	}{
		{"redis explicit", serverTypeRedis, "redis"},
		{"valkey explicit", serverTypeValkey, "valkey"},
		{"unknown explicit", serverTypeUnknown, "unknown"},
		{"empty normalizes to unknown", "", "unknown"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			reg := prometheus.NewRegistry()
			m := newMetrics(reg, tc.newMetricsArg)

			labels := m.transportTypeLabels()
			assert.Equal(t, "redis", labels["transport_type"], "transport_type is fixed for the redis-streams transport family")
			assert.Equal(t, tc.wantBackendLabel, labels["backend_type"])
		})
	}
}

// TestBackendTypeLabelOnRegisteredMetrics pins that backend_type reaches the
// scraped output for a counter, a gauge and a histogram, using literal label
// values.
func TestBackendTypeLabelOnRegisteredMetrics(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m := newMetrics(reg, serverTypeValkey)

	// Record one sample per family so each appears in Gather().
	m.publishDuration.Observe(0.001)
	m.healthy.Set(1)
	m.lostCounter(lossReasonShutdown).Inc()

	families, err := reg.Gather()
	require.NoError(t, err)

	wanted := map[string]bool{
		"mercure_redis_publish_duration_seconds": false, // histogram
		"mercure_redis_healthy":                  false, // gauge
		"mercure_redis_subscribers_lost_total":   false, // counter (vec)
	}

	for _, mf := range families {
		if _, watching := wanted[mf.GetName()]; !watching {
			continue
		}

		wanted[mf.GetName()] = true

		for _, metric := range mf.GetMetric() {
			gotTransport, gotBackend := "", ""

			for _, lp := range metric.GetLabel() {
				switch lp.GetName() {
				case labelKeyTransportType:
					gotTransport = lp.GetValue()
				case labelKeyBackendType:
					gotBackend = lp.GetValue()
				}
			}

			assert.Equal(t, "redis", gotTransport, "transport_type on %s", mf.GetName())
			assert.Equal(t, "valkey", gotBackend, "backend_type on %s", mf.GetName())
		}
	}

	for name, seen := range wanted {
		assert.True(t, seen, "metric family %s was missing from scrape — collector wiring likely broken", name)
	}
}

// TestCaddyReloadWithBackendSwapAccumulatesGhostSeries pins that two
// transports with different backends on one registry register separate
// collector sets (identity includes const-label values), so both backend_type
// series coexist. A closed transport's set keeps its last values; see
// README.md § Metrics.
func TestCaddyReloadWithBackendSwapAccumulatesGhostSeries(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()

	// First transport: redis backend.
	m1 := newMetrics(reg, serverTypeRedis)
	require.NotNil(t, m1)
	m1.publishDuration.Observe(0.001)

	// Second transport, valkey backend: it registers its own set, with no
	// AlreadyRegisteredError.
	m2 := newMetrics(reg, serverTypeValkey)
	require.NotNil(t, m2)
	m2.publishDuration.Observe(0.002)

	// Both backend_type series should coexist for at least one metric family.
	families, err := reg.Gather()
	require.NoError(t, err)

	sawRedis, sawValkey := false, false

	for _, fam := range families {
		if fam.GetName() != "mercure_redis_publish_duration_seconds" {
			continue
		}

		for _, metric := range fam.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() != labelKeyBackendType {
					continue
				}

				switch label.GetValue() {
				case "redis":
					sawRedis = true
				case "valkey":
					sawValkey = true
				}
			}
		}
	}

	assert.True(t, sawRedis, "redis backend series expected (first transport's collectors, still registered)")
	assert.True(t, sawValkey, "valkey backend series expected (second transport's collectors)")
}

// TestMetricsCollectorListMatchesStructFields counts the prometheus.Collector
// fields on Metrics by reflection and pins that Metrics.collectors lists as
// many; a new field must be added there and to registerAll.
func TestMetricsCollectorListMatchesStructFields(t *testing.T) {
	t.Parallel()

	collectorType := reflect.TypeFor[prometheus.Collector]()

	var fieldCount int

	for f := range reflect.TypeFor[Metrics]().Fields() {
		// Check both the field type and its pointer — a future field declared
		// as a concrete struct value with pointer-receiver Describe/Collect
		// methods would otherwise be missed by the type-Implements check
		// alone, escaping the audit silently.
		if f.Type.Implements(collectorType) || reflect.PointerTo(f.Type).Implements(collectorType) {
			fieldCount++
		}
	}

	// A zero count would pass the length check vacuously.
	require.NotZerof(t, fieldCount, "no prometheus.Collector fields found on Metrics — the struct may have lost its instrumentation entirely")

	built := newMetrics(prometheus.NewRegistry(), serverTypeRedis)

	assert.Lenf(t, built.collectors(), fieldCount,
		"Metrics has %d prometheus.Collector fields but Metrics.collectors lists a different count: add the new field to collectors() and registerAll", fieldCount)
}

// TestEveryBuiltCollectorIsRegistered pins that every collector in
// Metrics.collectors is non-nil and already registered: registering it again
// must return AlreadyRegisteredError.
func TestEveryBuiltCollectorIsRegistered(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	transport, _ := newTestTransport(t, WithPrometheusRegisterer(reg))
	m := transport.metrics.Load()

	for _, c := range m.collectors() {
		// Check nil with reflect so a typed-nil pointer is caught too.
		rv := reflect.ValueOf(c)
		require.NotEqualf(t, reflect.Invalid, rv.Kind(),
			"collector listed in Metrics.collectors is a nil interface")
		require.Falsef(t, rv.Kind() == reflect.Pointer && rv.IsNil(),
			"collector %T is a typed-nil pointer", c)

		var alreadyRegistered prometheus.AlreadyRegisteredError
		require.ErrorAsf(t, reg.Register(c), &alreadyRegistered,
			"collector %T must already be registered; an AlreadyRegisteredError absence means registerAll dropped it (would increment forever, never scraped)", c)
	}
}

// TestMetricsCollectorsAreUnique pins that Metrics.collectors lists no
// collector twice, which the count check alone would miss. Identity is the
// pointer, so every entry must be pointer-kind.
func TestMetricsCollectorsAreUnique(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	transport, _ := newTestTransport(t, WithPrometheusRegisterer(reg))
	m := transport.metrics.Load()

	seen := make(map[uintptr]int)

	for i, c := range m.collectors() {
		// Pointer identity needs a pointer-kind value; nil entries are caught by
		// TestEveryBuiltCollectorIsRegistered.
		rv := reflect.ValueOf(c)
		require.Equalf(t, reflect.Pointer, rv.Kind(),
			"collector at index %d (%T) is not pointer-kind; the pointer-identity dedup uses reflect.Value.Pointer which requires Ptr/UnsafePointer/Chan/Func/Map/Slice — extend this test for the new shape", i, c)

		ptr := rv.Pointer()
		if prevIdx, dup := seen[ptr]; dup {
			assert.Failf(t, "duplicate collector entry in Metrics.collectors",
				"indices %d and %d reference the same collector instance (%T) — copy-paste typo: the same field is listed twice and a different one is missing, evading TestMetricsCollectorListMatchesStructFields' count-only cross-check",
				prevIdx, i, c)
		}

		seen[ptr] = i
	}
}
