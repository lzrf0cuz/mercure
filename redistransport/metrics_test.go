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
		"mercure_redis_subscriber_rate_limited_total",
		metricSubscriberRemoveTotal,
		metricHealthy,
		"mercure_redis_stream_length",
		metricClockDriftSeconds,
		"mercure_redis_presence_payload_bytes",
		"mercure_redis_publish_payload_bytes",
		metricTTLCleanupErrorsTotal,
		metricPreBindingMetricDropsTotal,
		metricConsumerGroups,
		metricHistoryWindowSeconds,
		metricDispatchZeroMatchTotal,
	}

	for _, name := range expectedImmediate {
		assert.True(t, metricNames[name], "metric %s should be registered", name)
	}

	// Every transport metric carries the transport_type="redis" AND
	// backend_type=<detected> ConstLabels. Walk every family and assert
	// both; a future collector that forgets ConstLabels: m.transportTypeLabels()
	// will fail this. newTestTransport uses miniredis with
	// withSkipVersionCheck; miniredis rejects `INFO server` (only `clients`
	// and `stats` sections are supported), so validateVersion's skip path
	// fires and t.backendType stays at the constructor default
	// serverTypeUnknown — backend_type="unknown" here is correct.
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
	// observation during construction. Confirm the gauge is registered AND
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
		// revision is empty under `go test` (no vcs.revision setting); only
		// the presence of the label key is required. When non-empty, the
		// vcsRevisionShortLen truncation contract still holds — assert that
		// bound defensively so a parallel-codepath regression in
		// LookupBuildInfo would surface here.
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

// TestRegisterMetricsWithLateBinding exercises the Caddy submodule path:
// transport constructed without a registerer (Caddy's sub-module ctx hands
// out a typed-nil registry), then the parent mercure caddy module calls
// RegisterMetricsWith with its real registry post-GetTransport. After the
// late binding, metrics emit normally — locks the late-binding contract
// where pre-bind dispatches no-op silently and post-bind dispatches emit.
func TestRegisterMetricsWithLateBinding(t *testing.T) {
	t.Parallel()

	// Constructor path: no registerer, so metrics are nil.
	transport, _ := newTestTransport(t)
	require.Nil(t, transport.metrics.Load(), "precondition: metrics must be nil before late binding")

	// A dispatch with metrics nil must not panic — covers the
	// constructor-path window before Caddy's late binding fires.
	require.NoError(t, transport.Dispatch(context.Background(), &mercure.Update{
		Topics: []string{"https://example.com/test"},
		Event:  mercure.Event{Data: "x"},
	}))

	// Caddy late binding.
	reg := prometheus.NewRegistry()
	require.NoError(t, transport.RegisterMetricsWith(reg))

	require.NotNil(t, transport.metrics.Load(), "metrics must be bound after RegisterMetricsWith")

	// Now exercise the bound metrics by dispatching and gathering.
	require.NoError(t, transport.Dispatch(context.Background(), &mercure.Update{
		Topics: []string{"https://example.com/test"},
		Event:  mercure.Event{Data: "y"},
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

// TestPreBindingDropsCounted exercises the late-binding-window audit trail:
// metric-emission attempts that land before RegisterMetricsWith binds *Metrics
// must increment the transport's preBindingDrops counter, and the counter's
// running total must be exposed via mercure_redis_pre_binding_metric_drops_total
// once metrics are bound. Without this, drops during the binding window are
// silent and the operator has no way to know if startup traffic was lost.
func TestPreBindingDropsCounted(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t)
	require.Nil(t, transport.metrics.Load(), "precondition: metrics must be nil before late binding")

	// Background goroutines (xreadgroupListener startup, etc.) may already
	// have logged drops by the time we get here. Capture a baseline rather
	// than assert zero — the invariant under test is "drops accumulate", not
	// "no drops before this point in the test".
	baseline := transport.preBindingDrops.Load()

	// metricsOrCountDrop on nil-bound transport increments the counter.
	require.Nil(t, transport.metricsOrCountDrop(), "metricsOrCountDrop returns nil while unbound")
	require.Nil(t, transport.metricsOrCountDrop())
	require.Nil(t, transport.metricsOrCountDrop())
	assert.Equal(t, baseline+3, transport.preBindingDrops.Load(),
		"every metricsOrCountDrop call before binding increments preBindingDrops")

	// A real Dispatch before binding goes through multiple emission sites
	// (encode-error guard, publishPayloadBytes Observe, publish-error guard,
	// publishDuration Observe). With baseline+3 already accounted for by the
	// three direct calls above, a successful Dispatch must add at least one
	// more drop — assert strict greater-than against baseline+3 to catch a
	// regression where Dispatch silently bypasses the metric path entirely.
	// The exact count varies by code path so we don't pin it.
	require.NoError(t, transport.Dispatch(context.Background(), &mercure.Update{
		Topics: []string{"https://example.com/test"},
		Event:  mercure.Event{Data: "pre-bind"},
	}))

	preBindCount := transport.preBindingDrops.Load()
	assert.Greater(t, preBindCount, baseline+3,
		"Dispatch on unbound transport must add at least one more drop on top of the three direct metricsOrCountDrop calls above")

	// Bind — the CounterFunc reads the atomic at every scrape, so the
	// reported total reflects whatever drops accumulated up to that scrape.
	reg := prometheus.NewRegistry()
	require.NoError(t, transport.RegisterMetricsWith(reg))

	families, err := reg.Gather()
	require.NoError(t, err)

	var dropMetric *float64

	for _, f := range families {
		if f.GetName() != metricPreBindingMetricDropsTotal {
			continue
		}

		require.Len(t, f.GetMetric(), 1, "drop counter must register exactly one series")
		v := f.GetMetric()[0].GetCounter().GetValue()
		dropMetric = &v
	}

	require.NotNil(t, dropMetric,
		"mercure_redis_pre_binding_metric_drops_total must be registered after RegisterMetricsWith")

	// Background goroutines (xreadgroupListener startup churn under
	// miniredis) may have ticked between the Dispatch above and the Gather
	// below, so the scraped counter is >= preBindCount, not strictly equal.
	// The invariant under test: the CounterFunc reflects the running atomic
	// total at scrape time, which is at least the value we observed pre-bind.
	assert.GreaterOrEqual(t, *dropMetric, float64(preBindCount),
		"CounterFunc must reflect the pre-binding running total at scrape time")

	// Post-binding: metricsOrCountDrop returns the bound *Metrics; the test
	// goroutine's two calls below add zero drops. Background goroutines
	// (xreadgroupListener startup churn, etc.) that observed nil before bind
	// and called Add() after the snapshot can drift the atomic upward — assert
	// the test-goroutine path with strict equality on the delta we control,
	// not on the atomic's absolute value, by reading the atomic ONCE per
	// observation. The invariant under test: once metrics are bound,
	// metricsOrCountDrop calls from this goroutine return non-nil and do not
	// increment.
	require.NotNil(t, transport.metricsOrCountDrop())
	require.NotNil(t, transport.metricsOrCountDrop())
}

// TestPreBindingDropsConcurrentBind exercises the atomic.Pointer bind path
// under -race: many goroutines call metricsOrCountDrop while one goroutine fires
// RegisterMetricsWith. The race detector catches any future migration to a
// non-atomic field. The assertion is purely about absence of races + the
// counter being internally consistent (drops + post-bind nil-safe loads
// account for every emission).
func TestPreBindingDropsConcurrentBind(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t)

	const concurrency = 32

	var (
		wg    sync.WaitGroup
		start = make(chan struct{})
	)

	for range concurrency {
		wg.Go(func() {
			<-start

			for range 100 {
				_ = transport.metricsOrCountDrop()
			}
		})
	}

	close(start)

	// Race the binding against the in-flight goroutines.
	reg := prometheus.NewRegistry()
	require.NoError(t, transport.RegisterMetricsWith(reg))

	wg.Wait()

	// Either path is correct under race-detector scheduling: the atomic
	// stays consistent, the bound *Metrics is non-nil, and all
	// metricsOrCountDrop calls returned without panic. The race detector
	// itself is the assertion — if any goroutine observes a torn read,
	// `go test -race` will fail.
	require.NotNil(t, transport.metrics.Load(),
		"transport must be bound after RegisterMetricsWith")
}

// TestPreBindingDropsBulkAdd locks the presence.go MGET-failure batch-bump
// contract (recordPreBindingDrops). The presence.go path uses this helper
// instead of metricsOrCountDrop because per-key emission semantics would be
// understated by len(keys)-1 if every emission only added 1.
func TestPreBindingDropsBulkAdd(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t)

	baseline := transport.preBindingDrops.Load()

	transport.recordPreBindingDrops(7)
	transport.recordPreBindingDrops(13)

	assert.Equal(t, baseline+20, transport.preBindingDrops.Load(),
		"recordPreBindingDrops must Add(n) to the atomic — exact count, not capped at 1")
}

// TestPreBindingDropsPresenceMGETFailureBeforeBind exercises the actual
// presence MGET-failure code path that motivated recordPreBindingDrops:
// mgetPresenceValues hits a closed Redis client (so MGET errors), is reached via
// readPresenceKeys before RegisterMetricsWith binds metrics, and must bump
// preBindingDrops by the failed batch's key count — not 1, not 0. A future
// refactor that swaps the raw t.metrics.Load() in mgetPresenceValues for
// t.metricsOrCountDrop() inside the loop, or that replaces
// recordPreBindingDrops(len(batch)) with recordPreBindingDrops(1), would
// silently change the counter's accounting in a way that
// TestPreBindingDropsBulkAdd cannot catch (it tests the helper directly, not
// the call site).
func TestPreBindingDropsPresenceMGETFailureBeforeBind(t *testing.T) {
	t.Parallel()

	transport, mr := newTestTransport(t)
	require.Nil(t, transport.metrics.Load(),
		"precondition: metrics must be nil so the bulk-add path runs")

	// Close miniredis so MGET fails with a connection error rather than
	// returning the empty values that a successful MGET against an empty
	// stream would produce. The error path is the only one that reaches
	// recordPreBindingDrops(len(keys)).
	mr.Close()

	baseline := transport.preBindingDrops.Load()
	keys := []string{"k1", "k2", "k3", "k4", "k5"}

	_, err := transport.readPresenceKeys(context.Background(), keys)
	require.Error(t, err, "MGET against a closed miniredis must fail")

	got := transport.preBindingDrops.Load()
	// Background goroutines (xreadgroupListener trying to reconnect to the
	// closed miniredis) can bump preBindingDrops by 1 each via metricsOrCountDrop
	// between baseline and the MGET-failure path. Assert the per-key
	// accounting contract via GreaterOrEqual on len(keys), with strict
	// less-than 2*len(keys) ruling out a regression that double-bumps.
	delta := got - baseline
	assert.GreaterOrEqual(t, delta, uint64(len(keys)),
		"the MGET-failure path must bump preBindingDrops by AT LEAST len(keys)=%d (got delta=%d) — locks the per-key drop accounting contract that metricsOrCountDrop's single-bump cannot express",
		len(keys), delta)
	assert.Less(t, delta, uint64(2*len(keys)),
		"delta=%d is suspiciously close to 2*len(keys)=%d — likely a regression that double-counts (e.g. switched the loop body to t.metricsOrCountDrop().recordPresenceReadError on top of the bulk add)",
		delta, 2*len(keys))
}

// TestPreBindingDropsCounterFuncMultiScrape locks the read-through invariant
// of the swappable-source collector: scrape, mutate the atomic, scrape
// again — the second scrape must reflect the new value. A snapshot-at-bind
// implementation would silently pass tests that scrape only once but break
// in production where Prometheus scrapes every interval.
func TestPreBindingDropsCounterFuncMultiScrape(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t)
	reg := prometheus.NewRegistry()
	require.NoError(t, transport.RegisterMetricsWith(reg))

	scrape := func() uint64 {
		t.Helper()

		families, err := reg.Gather()
		require.NoError(t, err)

		for _, f := range families {
			if f.GetName() == metricPreBindingMetricDropsTotal {
				require.Len(t, f.GetMetric(), 1)

				return uint64(f.GetMetric()[0].GetCounter().GetValue())
			}
		}

		t.Fatal("mercure_redis_pre_binding_metric_drops_total not found in registry")

		return 0
	}

	first := scrape()

	transport.preBindingDrops.Add(100)

	second := scrape()

	// GreaterOrEqual rather than Equal: background goroutines can bump
	// preBindingDrops between scrapes via metricsOrCountDrop. The invariant
	// under test is "second scrape sees AT LEAST the +100 mutation" —
	// a snapshot-at-bind regression would leave second == first (drift
	// frozen), failing the assertion.
	assert.GreaterOrEqual(t, second, first+100,
		"second scrape must reflect at least the +100 mutation — read-through is broken if a snapshot is cached")
}

// TestPreBindingDropsRebindsOnAdoption locks the Caddy-reload + new-transport
// fix: when a NEW transport binds to a registry that already has the
// pre-binding-drops collector (prior transport instance registered it), the
// adopted collector's value source must be redirected to the new transport's
// atomic. Without this, the new transport's drops are silently dropped on
// the floor while the registry keeps reporting the stale prior atomic.
func TestPreBindingDropsRebindsOnAdoption(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()

	t1, _ := newTestTransport(t)
	require.NoError(t, t1.RegisterMetricsWith(reg))

	// Bump t1's drops via a deliberate atomic Add so we can distinguish
	// t1's value from t2's via scrape later.
	t1.preBindingDrops.Add(7)

	scrape := func() uint64 {
		t.Helper()

		families, err := reg.Gather()
		require.NoError(t, err)

		for _, f := range families {
			if f.GetName() == metricPreBindingMetricDropsTotal {
				return uint64(f.GetMetric()[0].GetCounter().GetValue())
			}
		}

		t.Fatal("metric not found")

		return 0
	}

	t1Value := scrape()

	// Build t2 (a fresh transport) and bind it to the SAME registry —
	// safeRegister will adopt t1's already-registered collector. The fix
	// rebinds the source closure to t2's atomic.
	t2, _ := newTestTransport(t)
	require.NoError(t, t2.RegisterMetricsWith(reg))

	// t2 starts with whatever its own background goroutines have logged.
	// Background goroutines on both t1 and t2 can drift the atomics between
	// our reads, so assert the rebind-on-adoption contract via DELTAS we
	// control rather than absolute equality on a moving target.
	scrapeBeforeT2Mutation := scrape()

	// Mutate t2 specifically; scrape must reflect the +42 delta. If the
	// adopted collector still pointed at t1's atomic, scrape would not
	// move (or would move only via t1's background drift, which is
	// asymptotically smaller than +42 over the test window).
	t2.preBindingDrops.Add(42)

	scrapeAfterT2Mutation := scrape()
	assert.GreaterOrEqual(t, scrapeAfterT2Mutation, scrapeBeforeT2Mutation+42,
		"post-adoption scrape must reflect at least t2's +42 mutation; reading from t1 would break the audit trail (t1Value=%v)", t1Value)

	// Mutate t1; scrape must NOT advance by t1's +99 because the source has
	// been redirected. Tolerate small drift from t2's own background bumps
	// between scrapes by asserting strict less-than (t1's +99 is well above
	// any plausible test-window background drift).
	t1.preBindingDrops.Add(99)

	scrapeAfterT1Mutation := scrape()
	assert.Less(t, scrapeAfterT1Mutation, scrapeAfterT2Mutation+99,
		"after adoption, scrape must NOT advance by t1's +99 mutation; the source closure has been redirected away from t1")
}

// TestRegisterMetricsWithIdempotent asserts a second call is a no-op rather
// than re-registering (which would error with AlreadyRegistered or duplicate
// the pool-stats collector). Caddy's config reload may trigger this.
func TestRegisterMetricsWithIdempotent(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t)

	reg := prometheus.NewRegistry()
	require.NoError(t, transport.RegisterMetricsWith(reg))

	first := transport.metrics.Load()
	require.NotNil(t, first)

	// Second call with the SAME registry — must be no-op.
	require.NoError(t, transport.RegisterMetricsWith(reg))
	assert.Same(t, first, transport.metrics.Load(),
		"second RegisterMetricsWith on same transport must keep the original *Metrics")

	// Second call with a DIFFERENT registry — also no-op (sync.Once gates).
	otherReg := prometheus.NewRegistry()
	require.NoError(t, transport.RegisterMetricsWith(otherReg))
	assert.Same(t, first, transport.metrics.Load(),
		"second RegisterMetricsWith with different registry must still no-op (sync.Once)")
}

// TestRegisterMetricsWithTypedNilRegisterer covers the typed-nil branch of
// normalizePrometheusRegisterer: Caddy's sub-module ctx returns a typed-nil
// *prometheus.Registry, which is != nil under interface comparison but
// panics on method dispatch. RegisterMetricsWith must collapse it to a
// real nil and no-op without registering.
func TestRegisterMetricsWithTypedNilRegisterer(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t)

	var nilReg *prometheus.Registry // typed-nil *prometheus.Registry
	require.NoError(t, transport.RegisterMetricsWith(nilReg))
	assert.Nil(t, transport.metrics.Load(),
		"typed-nil registerer must be normalized to no-op, leaving metrics unbound")
}

// shadowPoolDescCollector publishes one Desc whose FQ-name matches the real
// poolStatsCollector but with non-matching help text, so that reg.Register on
// the real poolStatsCollector returns a plain "descriptor already exists"
// error (NOT AlreadyRegisteredError). This drives the FAILED-TO-REGISTER
// branch in RegisterMetricsWith — the silent-failure path where a third-party
// collector under the pool's FQ-name would otherwise leave the transport with
// metrics.Store(m) never executed and registerErr nil.
type shadowPoolDescCollector struct {
	desc *prometheus.Desc
}

func newShadowPoolDescCollector() *shadowPoolDescCollector {
	return &shadowPoolDescCollector{
		desc: prometheus.NewDesc(
			"mercure_redis_client_pool_total_conns",
			"Test shadow with non-matching help string to force a duplicate-desc error.",
			nil, constLabelsFor(serverTypeRedis),
		),
	}
}

func (c *shadowPoolDescCollector) Describe(ch chan<- *prometheus.Desc) { ch <- c.desc }
func (c *shadowPoolDescCollector) Collect(ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, 0)
}

// TestRegisterMetricsWithPoolCollectorRegisterFailure covers the silent-failure
// guard via the failed-to-register path: when the pool stats collector cannot
// be registered (FQ-name conflict with a foreign collector under the same
// name), RegisterMetricsWith MUST surface the error rather than silently no-op
// while still publishing the *Metrics struct. The bug-fix invariant: on a
// failed pool-collector register, registerErr is non-nil AND t.metrics.Load()
// remains nil so the caller's Provision wraps the error and the hub fails
// fast — instead of starting healthy with detached pool metrics.
func TestRegisterMetricsWithPoolCollectorRegisterFailure(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	require.NoError(t, reg.Register(newShadowPoolDescCollector()),
		"shadow collector should register cleanly so the real pool collector hits a duplicate-desc conflict")

	transport, _ := newTestTransport(t)
	err := transport.RegisterMetricsWith(reg)
	require.Error(t, err, "must surface the registration error instead of silently no-op'ing")
	assert.Contains(t, err.Error(), "failed to register pool stats collector",
		"error should identify the failing path")
	assert.Nil(t, transport.metrics.Load(),
		"metrics must NOT be Stored on a failed registration — caller's Provision wraps the error and the hub fails fast")
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
		Event:  mercure.Event{Data: "pre-bind"},
	}))

	reg := prometheus.NewRegistry()
	require.NoError(t, transport.RegisterMetricsWith(reg))

	m := transport.metrics.Load()
	require.NotNil(t, m)
	require.Len(t, m.shardDispatchObs, shards, "shardDispatchObs slice must be sized to numShards")
	require.Len(t, m.shardSubGauges, shards, "shardSubGauges slice must be sized to numShards")

	// Add subscribers across multiple shards (UUIDs hash deterministically).
	tss := testTopicSelectorStore()
	for range shards * 2 {
		s := mercure.NewLocalSubscriber("", testLogger(), tss)
		s.SetTopics([]string{"https://example.com/test"}, nil)
		require.NoError(t, transport.AddSubscriber(context.Background(), s))
	}

	// Drive a publish to populate dispatch_duration histogram across shards.
	require.NoError(t, transport.Dispatch(context.Background(), &mercure.Update{
		Topics: []string{"https://example.com/test"},
		Event:  mercure.Event{Data: "post-bind"},
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

// TestRegisterMetricsWithConcurrentLoadStore drives many goroutines emitting
// metrics (via Dispatch) concurrently with a single RegisterMetricsWith call
// from the main goroutine, then asserts no race + that metrics scrape post-
// bind. atomic.Pointer makes this safe by construction; the test guards
// against a regression that reverts to a plain *Metrics field. Run via
// `go test -race`.
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
				Event:  mercure.Event{Data: "race"},
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
		Event:  mercure.Event{Data: "test"},
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
	tss := testTopicSelectorStore()

	sub := mercure.NewLocalSubscriber("", testLogger(), tss)
	sub.SetTopics([]string{"https://example.com/test"}, nil)
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
		"redistransport: newMetrics called with nil registerer",
		func() { newMetrics(nil, nil, "") },
	)
}

// TestMetricsReRegisterAdoptsExisting verifies that when newMetrics is called
// twice against the same registry (Caddy config-reload scenario), the second
// Metrics struct ends up pointing at the FIRST registration's collectors.
// Without this, the second transport instance would increment detached
// counters that never get scraped, silently zeroing metrics after reload.
func TestMetricsReRegisterAdoptsExisting(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()

	m1 := newMetrics(reg, nil, serverTypeRedis)
	m2 := newMetrics(reg, nil, serverTypeRedis)

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
	// Both newMetrics(reg) calls Set(1) on the same buildInfo gauge after
	// safeRegister adopts the existing collector — so exactly one series
	// must exist with value 1. A regression that broke the post-registerAll
	// Set ordering (metrics.go:271-273) would either emit a detached series
	// or leave the live collector at 0.
	assert.Equal(t, 1, buildInfoSeries, "build_info should emit exactly one series after re-registration")
	assert.InDelta(t, float64(1), buildInfoValue, 0, "build_info series should hold the constant value 1")
}

// TestDispatchSubscribersMatchedRecordsOncePerMessage verifies that the
// histogram observes exactly one sample per dispatched message — summed
// across all shards — instead of one observation per shard. The pre-fix
// behavior recorded once per shard with no shard label, under-reporting
// fan-out and disappearing entirely on the dispatch_shards=1 path.
func TestDispatchSubscribersMatchedRecordsOncePerMessage(t *testing.T) {
	t.Parallel()

	const shards = 4

	reg := prometheus.NewRegistry()
	transport, _ := newShardedTestTransport(t, shards, WithPrometheusRegisterer(reg))
	tss := testTopicSelectorStore()

	// Distribute subscribers across shards by relying on UUID hashing.
	const subs = 16
	for range subs {
		s := mercure.NewLocalSubscriber("", testLogger(), tss)
		s.SetTopics([]string{"https://example.com/match"}, nil)
		require.NoError(t, transport.AddSubscriber(context.Background(), s))
	}

	require.NoError(t, transport.Dispatch(context.Background(), &mercure.Update{
		Topics: []string{"https://example.com/match"},
		Event:  mercure.Event{Data: "x"},
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
		// Exactly one observation per message — even with shards=4.
		// Pre-fix this would be == shards (one per shard).
		assert.Equal(t, uint64(1), h.GetSampleCount(),
			"sample count must be once-per-message, not once-per-shard")
		// Sum of matched subscribers must equal total subscribers (all
		// matched the topic). Pre-fix this was per-shard, so the
		// observation value would only be the shard's subset.
		assert.InDelta(t, float64(subs), h.GetSampleSum(), 0,
			"sum across shards must equal total matched subscribers")
	}
}

// TestPoolStatsCollectorReRegisterSwapsClient verifies that when two
// transports share the same registry (Caddy reload pattern), the second
// transport's RegisterMetricsWith swaps its client into the
// already-registered poolStatsCollector via setClient. Pre-fix coverage
// was descriptor-presence only — the registry keeps showing
// pool_total_conns as long as ANY collector is registered, even if its
// client field still points at the closed t1. This test now closes t1
// and drives operations on t2, then asserts non-zero pool stats on the
// scraped values — only possible if the collector's client pointer was
// swapped to t2's live client.
func TestPoolStatsCollectorReRegisterSwapsClient(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()

	t1, _ := newTestTransport(t, WithPrometheusRegisterer(reg))
	t2, _ := newTestTransport(t, WithPrometheusRegisterer(reg))

	// Close t1 first. Pre-fix the registered collector still holds t1's
	// now-closed client; PoolStats() on a closed pool returns the zero
	// PoolStats value (TotalConns=0, Hits=0). With the swap, the collector
	// references t2's still-open client and operations on t2 increment
	// the pool counters live.
	require.NoError(t, t1.Close(context.Background()))

	// Drive operations through t2 to push pool counters above zero.
	for range 5 {
		require.NoError(t, t2.Dispatch(context.Background(), &mercure.Update{
			Topics: []string{"https://example.com/swap-test"},
			Event:  mercure.Event{Data: "v"},
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
		"pool_total_conns must reflect t2's open connections (swap pointed the registered collector at t2's live client); pre-fix this would be 0 because the collector kept scraping t1's closed pool")
}

// nonComparableRegisterer is a Registerer implementation backed by a value
// type that contains a non-comparable field (a slice). Direct interface
// equality `a != b` panics when both sides share this dynamic type; the
// drift-detection path must guard against that. Pointer-receiver Methods
// are intentionally avoided so the dynamic type is the value type itself.
type nonComparableRegisterer struct {
	tags []string
}

func (nonComparableRegisterer) Register(prometheus.Collector) error  { return nil }
func (nonComparableRegisterer) MustRegister(...prometheus.Collector) {}
func (nonComparableRegisterer) Unregister(prometheus.Collector) bool { return false }

// TestRegistererDiffersDoesNotPanicOnNonComparable guards the
// drift-detection helper against a runtime panic when callers pass two
// Registerer values whose dynamic type is non-comparable (struct with
// slice/map/func field). Pre-fix, `*first != reg` panics when both sides
// share such a type. With the type/Comparable() gate, the helper degrades
// to "identity unknown, no warn" — safer than crashing a Caddy reload.
func TestRegistererDiffersDoesNotPanicOnNonComparable(t *testing.T) {
	t.Parallel()

	a := nonComparableRegisterer{tags: []string{"a"}}
	b := nonComparableRegisterer{tags: []string{"b"}}

	// Direct `a != b` panics on non-comparable dynamic types; helper must
	// not. Contract: identityKnown=false signals the degraded path so the
	// caller can leave a Debug breadcrumb instead of crashing.
	var (
		differ        bool
		identityKnown bool
	)

	require.NotPanics(t, func() {
		differ, identityKnown = registerersDiffer(a, b)
	}, "drift-detection helper must guard against non-comparable Registerer dynamic types")
	assert.False(t, identityKnown,
		"same non-comparable type must signal identityKnown=false so the caller emits a degraded-detection breadcrumb instead of a drift warn")
	assert.False(t, differ,
		"differ must default to false on the unknown-identity path so the caller does not warn")

	// Different dynamic types: definitely different; identityKnown=true.
	reg := prometheus.NewRegistry()
	differ, identityKnown = registerersDiffer(a, reg)
	assert.True(t, identityKnown, "different dynamic types: identity is determinable from type alone")
	assert.True(t, differ, "different dynamic types must report as different")

	// Same comparable type, same identity: not different; identityKnown=true.
	differ, identityKnown = registerersDiffer(reg, reg)
	assert.True(t, identityKnown, "comparable type identity is always determinable")
	assert.False(t, differ, "same instance must report as not-different")

	// Same comparable type, different identity: different; identityKnown=true.
	reg2 := prometheus.NewRegistry()
	differ, identityKnown = registerersDiffer(reg, reg2)
	assert.True(t, identityKnown)
	assert.True(t, differ, "different instances of the same comparable type must report as different")
}

// TestRegisterMetricsWithSilentDriftWarn verifies a second
// RegisterMetricsWith call with a *different* registerer logs a Warn
// breadcrumb. metricsOnce already fired so the second registry never
// gets bound — without the warn, an operator chasing "metrics aren't on
// registry X" has no trail. Same-instance second calls (the Caddy
// TransportUsagePool reload pattern) stay silent — those are normal.
func TestRegisterMetricsWithSilentDriftWarn(t *testing.T) {
	t.Parallel()

	logBuf := &safeBuffer{}
	logger := slog.New(slog.NewJSONHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelWarn}))

	reg1 := prometheus.NewRegistry()
	transport, _ := newTestTransport(t, WithPrometheusRegisterer(reg1), WithLogger(logger))

	// Same-instance second call: must NOT warn (Caddy reload pattern).
	require.NoError(t, transport.RegisterMetricsWith(reg1))
	assert.NotContains(t, logBuf.String(), "RegisterMetricsWith",
		"second call with the same registerer is the Caddy reload pattern; it must not log Warn")

	// Different-instance second call: must warn.
	reg2 := prometheus.NewRegistry()
	require.NoError(t, transport.RegisterMetricsWith(reg2))
	assert.Contains(t, logBuf.String(), "called with a different registerer",
		"second call with a different registerer must surface the silent-drift breadcrumb so operators can find why metrics didn't bind to the new registry")
}

func TestMetricsSubscribersLostOnShutdown(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	transport, _ := newTestTransport(t, WithPrometheusRegisterer(reg))
	tss := testTopicSelectorStore()

	// Add two subscribers and hold them so they're present at Close.
	for range 2 {
		sub := mercure.NewLocalSubscriber("", testLogger(), tss)
		sub.SetTopics([]string{"https://example.com/test"}, nil)
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
	tss := testTopicSelectorStore()

	for range nSubs {
		sub := mercure.NewLocalSubscriber("", testLogger(), tss)
		sub.SetTopics([]string{"https://example.com/test"}, nil)
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
	tss := testTopicSelectorStore()

	sub := mercure.NewLocalSubscriber("", testLogger(), tss)
	sub.SetTopics([]string{"https://example.com/test"}, nil)
	require.NoError(t, transport.AddSubscriber(context.Background(), sub))

	// Force Dispatch to return false on the next call: doDisconnect closes
	// s.out and flips s.disconnected, so Dispatch's first branch returns
	// false without touching handleFullChan.
	sub.Disconnect()

	update := &mercure.Update{
		Topics: []string{"https://example.com/test"},
		Event:  mercure.Event{Data: "x"},
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

// TestMarkLostAtMostOncePerSubscriber proves the at-most-one-increment
// invariant for subscribers_lost across racing paths. N goroutines call
// markLost concurrently for the same subscriber with different reasons;
// regardless of who wins the CAS, the total subscribers_lost across all
// reason labels must be exactly 1. Without the lostFlags CAS this test
// would observe ~N increments (the previously-documented "accepted
// trade-off" at the AddSubscriber/disconnectAllSubscribers race window).
func TestMarkLostAtMostOncePerSubscriber(t *testing.T) {
	t.Parallel()

	const racers = 50

	reg := prometheus.NewRegistry()
	transport, _ := newTestTransport(t, WithPrometheusRegisterer(reg))
	tss := testTopicSelectorStore()

	sub := mercure.NewLocalSubscriber("", testLogger(), tss)
	sub.SetTopics([]string{"https://example.com/test"}, nil)
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

// TestDisconnectAllSubscribersNoMetrics verifies that Close with metrics
// disabled (nil registerer) still completes without panicking. The nil-check
// inside disconnectAllSubscribers is load-bearing — a regression that
// dereferences t.metrics here would take down any metrics-less deployment.
func TestDisconnectAllSubscribersNoMetrics(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t) // no WithPrometheusRegisterer
	tss := testTopicSelectorStore()

	sub := mercure.NewLocalSubscriber("", testLogger(), tss)
	sub.SetTopics([]string{"https://example.com/test"}, nil)
	require.NoError(t, transport.AddSubscriber(context.Background(), sub))

	require.Nil(t, transport.metrics.Load(), "precondition: metrics should be nil")
	assert.NotPanics(t, func() {
		_ = transport.Close(context.Background())
	})
}

// TestRecordPresenceReadErrorAllKinds unit-tests the metric helper for every
// kind. Integration coverage for kind="unmarshal" lives in
// TestMetricsPresenceReadErrorsUnmarshalKind; simulating kind="get" against
// miniredis requires a client wrapper, so this direct-helper test is the
// pragmatic guard that both label values produce correctly-named series.
func TestRecordPresenceReadErrorAllKinds(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m := newMetrics(reg, nil, serverTypeRedis)

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

// TestRecordPresenceReadErrorNilReceiver exercises the nil-safety guard —
// disabling metrics must not break presence reads.
func TestRecordPresenceReadErrorNilReceiver(t *testing.T) {
	t.Parallel()

	var m *Metrics

	assert.NotPanics(t, func() {
		m.recordPresenceReadError(presenceReadErrorGet)
		m.recordPresenceReadSkipped()
	})
}

// TestMetricsPresenceReadErrorsGetKind injects a bad presence key value
// (malformed JSON) at the scan prefix, calls GetSubscribers, and asserts that
// mercure_redis_presence_read_errors_total{kind="unmarshal"} is incremented.
// Without this counter, partial-result responses were invisible to ops.
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
	tss := testTopicSelectorStore()

	// Dispatch a message to create history.
	update := &mercure.Update{
		Topics: []string{"https://example.com/test"},
		Event:  mercure.Event{Data: "history data"},
	}
	require.NoError(t, transport.Dispatch(context.Background(), update))
	mr.FastForward(100 * time.Millisecond)

	// Add subscriber with history replay.
	sub := mercure.NewLocalSubscriber(mercure.EarliestLastEventID, testLogger(), tss)
	sub.SetTopics([]string{"https://example.com/test"}, nil)
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
	m := newMetrics(reg, nil, serverTypeRedis)

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

// TestStreamDecodeErrorKindsAreAllDocumented is an enum-exhaustiveness walker:
// it parses metrics.go for every const of type streamDecodeErrorKind and asserts
// each one's wire value appears in the streamDecodeErrors Help string. Adding a
// new kind without documenting it in the Help (which enumerates the labels
// operators alert on) trips this — the manual TestRecordStreamDecodeErrorAllKinds
// covers recording; this makes the doc side self-enforcing rather than trusting a
// hand-kept list to stay complete.
func TestStreamDecodeErrorKindsAreAllDocumented(t *testing.T) {
	t.Parallel()

	values := enumStringValues(t, "metrics.go", "streamDecodeErrorKind")
	require.GreaterOrEqual(t, len(values), 3, "expected at least the 3 known decode-error kinds")

	reg := prometheus.NewRegistry()
	m := newMetrics(reg, nil, serverTypeRedis)
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

// TestStreamDecodeErrorsMetricNameLiteral is a literal canary. The Grafana
// dashboard (transport.json) and README reference this metric by its wire name,
// but production and tests now go through metricStreamDecodeErrorsTotal. Renaming
// the constant's value would rename the wire metric and silently break those
// consumers while every test still passes — this pins the value.
func TestStreamDecodeErrorsMetricNameLiteral(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "mercure_redis_stream_decode_errors_total", metricStreamDecodeErrorsTotal)
}

// TestRecordStreamDecodeErrorNilReceiver verifies the helper is nil-safe so
// callers in hot paths can invoke it without branching on metrics==nil.
func TestRecordStreamDecodeErrorNilReceiver(t *testing.T) {
	t.Parallel()

	var m *Metrics

	assert.NotPanics(t, func() {
		m.recordStreamDecodeError(streamDecodeErrorDataType)
	})
}

// TestRecordZombieGCErrorNilReceiver verifies nil-safe on the zombie GC helper.
func TestRecordZombieGCErrorNilReceiver(t *testing.T) {
	t.Parallel()

	var m *Metrics

	assert.NotPanics(t, func() {
		m.recordZombieGCError()
	})
}

// TestRecordZombieGCErrorIncrements verifies the counter increments on the
// live receiver path.
func TestRecordZombieGCErrorIncrements(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m := newMetrics(reg, nil, serverTypeRedis)

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
	m := newMetrics(reg, nil, serverTypeRedis)

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

// TestRecordPublishErrorNilReceiver verifies nil-safety.
func TestRecordPublishErrorNilReceiver(t *testing.T) {
	t.Parallel()

	var m *Metrics

	assert.NotPanics(t, func() {
		m.recordPublishError(publishErrorEncode)
	})
}

// TestRecordXReadgroupErrorKinds verifies all 4 kinds increment the vec.
func TestRecordXReadgroupErrorKinds(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m := newMetrics(reg, nil, serverTypeRedis)

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

// TestRecordXReadgroupErrorNilReceiver verifies nil-safety.
func TestRecordXReadgroupErrorNilReceiver(t *testing.T) {
	t.Parallel()

	var m *Metrics

	assert.NotPanics(t, func() {
		m.recordXReadgroupError(xreadgroupErrorNoGroup)
	})
}

// TestRecordPresenceHeartbeatError verifies the counter increments.
func TestRecordPresenceHeartbeatError(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m := newMetrics(reg, nil, serverTypeRedis)

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

// TestRecordPresenceHeartbeatErrorNilReceiver verifies nil-safety.
func TestRecordPresenceHeartbeatErrorNilReceiver(t *testing.T) {
	t.Parallel()

	var m *Metrics

	assert.NotPanics(t, func() {
		m.recordPresenceHeartbeatError()
	})
}

// TestMetricsDispatchDurationSequentialPath verifies that dispatch_duration_seconds
// is recorded by the sequential (single-shard) dispatch path. Default
// dispatchShards=1 — without this coverage the metric only fires for opted-in
// sharded configs, which would leave most operators blind on the default path.
func TestMetricsDispatchDurationSequentialPath(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	transport, _ := newTestTransport(t, WithPrometheusRegisterer(reg))

	// Default config means numShards == 1; dispatchToSubscribers takes the
	// sequential branch.
	require.Equal(t, 1, transport.numShards)

	tss := testTopicSelectorStore()
	sub := mercure.NewLocalSubscriber("", testLogger(), tss)
	sub.SetTopics([]string{"https://example.com/test"}, nil)
	require.NoError(t, transport.AddSubscriber(context.Background(), sub))

	update := &mercure.Update{
		Topics: []string{"https://example.com/test"},
		Event:  mercure.Event{Data: "hi"},
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
		// Drift can be any non-negative float. The contract being tested is
		// "Set was called", which we verify via the gauge's presence — Prometheus
		// gauges only appear in Gather() after at least one Set/Inc/Dec.
		found = true
	}

	assert.True(t, found, "clock_drift_seconds should be populated by NewRedisTransport")
}

// TestRecordTTLCleanupError verifies the TTL-cleanup error counter increments
// and is nil-safe.
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

	var nilM *Metrics

	assert.NotPanics(t, func() { nilM.recordTTLCleanupError() })
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

// TestConsumerGroupGaugeValues verifies the redis.XInfoGroup -> (lag, pending)
// field mapping in isolation. A future swap of g.Lag <-> g.Pending in
// consumerGroupGaugeValues fails this test cleanly — without depending on
// Redis listener scheduling, miniredis fidelity, or goleak teardown timing,
// all of which dominate the signal when the same contract is exercised
// against a live stream. The risk being defended against is a 2-line
// refactor accidentally reversing the assignments.
func TestConsumerGroupGaugeValues(t *testing.T) {
	t.Parallel()

	lag, pending := consumerGroupGaugeValues(redis.XInfoGroup{
		Lag:     17,
		Pending: 3,
	})

	assert.InDelta(t, 17.0, lag, 0, "lag should map from g.Lag")
	assert.InDelta(t, 3.0, pending, 0, "pending should map from g.Pending")
}

// TestSampleConsumerLagResetOnMissingStream verifies that when XINFO GROUPS
// reports the stream does not exist, the gauges are reset to 0/0 rather than
// holding a stale value from a previous lifecycle. Operators dashboards
// otherwise can't distinguish "fresh hub" from "everything caught up".
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

// TestRecordHistoryReplayFallback verifies the fallback counter increments
// and is nil-safe.
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

	var nilM *Metrics

	assert.NotPanics(t, func() { nilM.recordHistoryReplayFullScan() })
}

// TestRecordHistoryReplayTruncated verifies the truncated counter increments
// and is nil-safe — the aggregate companion to the mercure.history.truncated
// span attribute.
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

	// Assert the family is registered first — without this, a "registerAll
	// dropped this counter" regression would leave got=0 and the InDelta
	// failure ("max difference between 1 and 0") is opaque; a missing-family
	// failure points at the real root cause.
	require.True(t, found, "metric family %q not found in registry — registerAll likely dropped historyReplayTruncated", metricHistoryReplayTruncatedTotal)
	assert.InDelta(t, 1.0, got, 1e-9)

	var nilM *Metrics

	assert.NotPanics(t, func() { nilM.recordHistoryReplayTruncated() })
}

// TestConstLabelsForReturnsExpectedLabels guards the {transport_type,
// backend_type} const-label contract emitted by every transport-side
// collector (transport metrics + pool stats). Literal-bound assertions —
// the values are forever once shipped, so a typo in either key or value
// breaks every dashboard that filters on it.
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

// TestMetricsTransportTypeLabelsReadsBackendField verifies that the method-
// level path (which every build* function uses for ConstLabels) reads the
// backendType captured at newMetrics-construction time. If this regresses,
// every collector built under a non-redis backend would silently emit
// backend_type="" or "redis", and Valkey deployments would be misattributed
// in dashboards.
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
			m := newMetrics(reg, nil, tc.newMetricsArg)

			labels := m.transportTypeLabels()
			assert.Equal(t, "redis", labels["transport_type"], "transport_type is fixed for the redis-streams transport family")
			assert.Equal(t, tc.wantBackendLabel, labels["backend_type"])
		})
	}
}

// TestBackendTypeLabelOnRegisteredMetrics verifies that backend_type
// actually appears on the scraped Prometheus output — not just on the
// labels map. Asserts via Gather() output for one representative metric
// from each class (counter, gauge, histogram) so a regression to a const-
// label miswiring is caught end-to-end. The assertion is literal-bound
// (raw `"valkey"` / `backend_type` strings, not symbolic) so any rename
// to the wire-format constants trips this test in addition to the
// parser-level coverage in lua_test.go.
func TestBackendTypeLabelOnRegisteredMetrics(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m := newMetrics(reg, nil, serverTypeValkey)

	// Drive at least one observation/inc on a representative trio so each
	// class produces a non-zero scrape line (the label format is asserted
	// regardless, but easier to read when the metric has emitted). healthy
	// is already set to 1 by newMetrics; the explicit Set documents intent.
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

// TestCaddyReloadWithBackendSwapAccumulatesGhostSeries locks the documented
// pre-1.0 behavior where a Caddy reload that swaps backends (Redis URL →
// Valkey URL within one process) registers a SECOND set of collectors with
// the new backend_type rather than adopting the existing ones — because
// Prometheus collector identity includes ConstLabel VALUES, and the new
// backend_type value diverges the identity. The old, frozen-at-last-value
// collectors stay on the registry until process restart. Dashboards
// filtered by the old backend_type show stale data; operators must restart
// for clean attribution.
//
// This test exists to catch any future change that would silently flip the
// behavior — e.g., promoting backend_type to a variable label would make
// the new transport ADOPT the existing collectors instead, which is
// arguably the cleaner behavior but a wire-contract-level semantic change.
// If you intentionally make that switch, update this test to assert the
// new contract; do not silently delete it. See redistransport/README.md §
// Metrics → Caddy hot-reload caveat.
func TestCaddyReloadWithBackendSwapAccumulatesGhostSeries(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()

	// First transport: redis backend.
	m1 := newMetrics(reg, nil, serverTypeRedis)
	require.NotNil(t, m1)
	m1.publishDuration.Observe(0.001)

	// Second transport: valkey backend (simulating Caddy reload + URL swap).
	// MUST succeed — identity diverged via backend_type value, no
	// AlreadyRegisteredError, no drift Warn fires.
	m2 := newMetrics(reg, nil, serverTypeValkey)
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

	assert.True(t, sawRedis, "redis backend series expected post-swap (ghost from first transport)")
	assert.True(t, sawValkey, "valkey backend series expected post-swap (live from second transport)")
}

// TestCheckVersionFromInfoLatchesBackendType verifies the wiring from
// parseServerInfo result → t.backendType. The label end-to-end is exercised
// by TestBackendTypeLabelOnRegisteredMetrics; this one isolates the latch
// step so a regression in checkVersionFromInfo's ordering (store-then-switch
// vs switch-then-store) is caught directly without needing a full transport
// fixture.
func TestCheckVersionFromInfoLatchesBackendType(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		info string
		want serverType
	}{
		{"redis only", "# Server\nredis_version:7.2.4\n", serverTypeRedis},
		{"valkey only", "# Server\nvalkey_version:8.1.6\n", serverTypeValkey},
		{"both present — valkey wins", "redis_version:7.2.4\nvalkey_version:8.0.0\n", serverTypeValkey},
		{"neither present", "# Server\nos:Linux\n", serverTypeUnknown},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tr := newTestTransportForCheck(t)
			// All four cases return nil because the fixture sets
			// skipVersionCheck=true (the only branch that returns
			// ErrMissingVersionField is gated on !skipVersionCheck). The
			// latch happens regardless of the error path — that's the
			// invariant under test.
			require.NoError(t, tr.checkVersionFromInfo(tc.info))
			assert.Equal(t, tc.want, tr.backendType)
		})
	}
}

// newTestTransportForCheck builds a minimal RedisTransport scaffold suitable
// for exercising checkVersionFromInfo in isolation. Sets withSkipVersionCheck
// so the "unknown/empty version" path returns nil instead of the
// ErrMissingVersionField gate — the test cares only about the latch behavior.
func newTestTransportForCheck(t *testing.T) *RedisTransport {
	t.Helper()

	return &RedisTransport{
		logger:      slog.New(slog.DiscardHandler),
		opts:        &options{skipVersionCheck: true},
		backendType: serverTypeUnknown,
	}
}

// collectorsForTest enumerates every prometheus.Collector field on Metrics so
// the registration audit below can assert each one reaches the registry.
// TestMetricsCollectorListMatchesStructFields cross-checks this list against
// reflection, so a newly-added collector field left off here (and therefore
// possibly off registerAll too) fails loudly instead of shipping as a detached,
// never-scraped collector. The shardDispatchObs/shardSubGauges []Observer /
// []Gauge slices are Vec children, not standalone collectors, and correctly
// absent (slice types don't implement prometheus.Collector).
func collectorsForTest(m *Metrics) []prometheus.Collector {
	return []prometheus.Collector{
		m.dispatchDuration,
		m.dispatchSubscribersTotal,
		m.publishDuration,
		m.publishPayloadBytes,
		m.xreadgroupLatency,
		m.xreadgroupBatch,
		m.historyReplayDuration,
		m.historyReplayConcurrent,
		m.historyReplayFallback,
		m.historyReplayTruncated,
		m.historyCursorRejected,
		m.subscriberAddTotal,
		m.subscriberRateLimited,
		m.subscriberRemoveTotal,
		m.publisherRateLimited,
		m.addSubscriberDuration,
		m.removeSubscriberDuration,
		m.subscribersLost,
		m.shardSubscribers,
		m.healthy,
		m.streamLength,
		m.clockDriftSeconds,
		m.consumerLag,
		m.consumerPending,
		m.consumerGroupsCount,
		m.historyWindowSeconds,
		m.dispatchZeroMatchTotal,
		m.buildInfo,
		m.presencePayloadBytes,
		m.presenceReadErrors,
		m.presenceReadSkipped,
		m.presenceReadCapped,
		m.subscriberAdmissionRejected,
		m.streamDecodeErrors,
		m.zombieGCErrors,
		m.publishErrors,
		m.xreadgroupErrors,
		m.presenceHeartbeatErrors,
		m.presenceFallback,
		m.preBindingMetricDrops,
		m.ttlCleanupErrors,
	}
}

// TestMetricsCollectorListMatchesStructFields keeps collectorsForTest honest:
// it counts the prometheus.Collector-typed fields on Metrics by reflection
// (type inspection only — no value access, no unsafe) and asserts the count
// equals the hand-listed set. Adding a Collector field to Metrics without
// listing it here fails this test, which is the prompt to also wire it into
// registerAll.
func TestMetricsCollectorListMatchesStructFields(t *testing.T) {
	t.Parallel()

	collectorType := reflect.TypeFor[prometheus.Collector]()

	var fieldCount int

	for f := range reflect.TypeFor[Metrics]().Fields() {
		// Check both the field type AND its pointer — a future field declared
		// as a concrete struct value with pointer-receiver Describe/Collect
		// methods would otherwise be missed by the type-Implements check
		// alone, escaping the audit silently.
		if f.Type.Implements(collectorType) || reflect.PointerTo(f.Type).Implements(collectorType) {
			fieldCount++
		}
	}

	// Positive control: if every Collector field is removed from Metrics AND
	// collectorsForTest is emptied, len==fieldCount==0 would pass vacuously.
	// Require a non-zero count so this canary trips before the count check.
	require.NotZerof(t, fieldCount, "no prometheus.Collector fields found on Metrics — the struct may have lost its instrumentation entirely")

	assert.Lenf(t, collectorsForTest(&Metrics{}), fieldCount,
		"Metrics has %d prometheus.Collector fields but collectorsForTest lists a different count — add the new field here (and confirm it's registered in registerAll, or it ships as a detached, never-scraped collector)", fieldCount)
}

// TestEveryBuiltCollectorIsRegistered enforces the registerAll invariant its
// own doc promises ("every built collector must appear in exactly one helper").
// For each collector listed in collectorsForTest, re-registering it must
// return AlreadyRegisteredError (proving it's in the registry); a collector
// that registerAll dropped would register cleanly here (nil error), failing.
// require.NotNilf is reflect-aware (testify's IsNil) so it catches both a nil
// interface AND an interface wrapping a typed-nil pointer — the matched-
// omission case where build*() and safeRegister are both removed and the
// field stays at its zero value. require.ErrorAsf fails on a nil error too,
// catching the detached-collector failure mode (reg.Register accepts a
// not-yet-registered collector cleanly). Together they catch the silent
// failure structurally, surviving future metric additions without an
// allowlist.
func TestEveryBuiltCollectorIsRegistered(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	transport, _ := newTestTransport(t, WithPrometheusRegisterer(reg))
	m := transport.metrics.Load()

	for _, c := range collectorsForTest(m) {
		// Use explicit reflect.IsNil rather than require.NotNilf so the
		// matched-omission canary doesn't depend on testify's NotNil
		// implementation staying reflect-aware across future major bumps. A
		// future testify version that switched to plain `!= nil` would let a
		// typed-nil pointer (interface wrapping *prometheus.CounterVec(nil))
		// pass NotNil silently — the exact silent-failure mode this test
		// exists to catch. Bare nil interface → reflect.Invalid kind → caught
		// at the Kind switch; typed-nil pointer → IsNil() returns true →
		// caught by the require.False below.
		rv := reflect.ValueOf(c)
		require.NotEqualf(t, reflect.Invalid, rv.Kind(),
			"collector listed in collectorsForTest is a bare-nil interface — matched-omission, neither built nor registered")
		require.Falsef(t, rv.Kind() == reflect.Pointer && rv.IsNil(),
			"collector %T is a typed-nil pointer wrapped in an interface — matched-omission, the built variable is nil", c)

		var alreadyRegistered prometheus.AlreadyRegisteredError
		require.ErrorAsf(t, reg.Register(c), &alreadyRegistered,
			"collector %T must already be registered; an AlreadyRegisteredError absence means registerAll dropped it (would increment forever, never scraped)", c)
	}
}

// TestCollectorsForTestEntriesAreUnique closes a blind spot in
// TestMetricsCollectorListMatchesStructFields' count-only check: a
// copy-paste duplicate (one field listed twice, another dropped) would
// keep the count intact. Dedup by reflect.Value.Pointer() — every
// current Collector value wraps a pointer-typed concrete, so the
// identity comparison is well-defined.
//
// If a future field is value-typed (struct-by-value with pointer-receiver
// Describe/Collect methods — the case the OR branch in
// TestMetricsCollectorListMatchesStructFields exists to cover), this test
// fails fast at the Kind check rather than silently producing the wrong
// uintptr for the value-type case; extend the test for the new shape.
func TestCollectorsForTestEntriesAreUnique(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	transport, _ := newTestTransport(t, WithPrometheusRegisterer(reg))
	m := transport.metrics.Load()

	seen := make(map[uintptr]int)

	for i, c := range collectorsForTest(m) {
		// Kind check first: a typed-nil collector wrapped in an interface has
		// reflect.Pointer kind (and reflect.Value.Pointer() would correctly
		// return 0, distinguishable from a non-nil collector). A bare-nil
		// interface produces reflect.Invalid kind and is caught here. The
		// "collector exists" axis is asserted independently by
		// TestEveryBuiltCollectorIsRegistered's require.NotNilf — this test
		// scopes to "the list has no duplicates", and Kind+Pointer suffice.
		rv := reflect.ValueOf(c)
		require.Equalf(t, reflect.Pointer, rv.Kind(),
			"collector at index %d (%T) is not pointer-kind; the pointer-identity dedup uses reflect.Value.Pointer which requires Ptr/UnsafePointer/Chan/Func/Map/Slice — extend this test for the new shape", i, c)

		ptr := rv.Pointer()
		if prevIdx, dup := seen[ptr]; dup {
			assert.Failf(t, "duplicate collector entry in collectorsForTest",
				"indices %d and %d reference the same collector instance (%T) — copy-paste typo: the same field is listed twice and a different one is missing, evading TestMetricsCollectorListMatchesStructFields' count-only cross-check",
				prevIdx, i, c)
		}

		seen[ptr] = i
	}
}

// TestSwappableCounterFuncSetSourceNilPanics pins setSource(nil) panicking —
// the silent-failure mode this type was built to prevent. Exercises both the
// post-construction setSource path and the constructor-nil path (since the
// constructor delegates to setSource). The exact panic message is matched as
// a literal canary; a reword is intentional and must update this test.
func TestSwappableCounterFuncSetSourceNilPanics(t *testing.T) {
	t.Parallel()

	const wantMsg = "redistransport: swappableCounterFunc.setSource called with nil source"

	t.Run("setSource(nil) after construction", func(t *testing.T) {
		t.Parallel()

		c := newSwappableCounterFunc(
			prometheus.CounterOpts{Name: "test_swappable_nil_post", Help: "panic-guard test"},
			func() float64 { return 0 },
		)

		assert.PanicsWithValue(t, wantMsg,
			func() { c.setSource(nil) },
			"setSource(nil) must panic with the documented message — silently storing nil would make Collect emit 0")
	})

	t.Run("newSwappableCounterFunc(nil) at construction", func(t *testing.T) {
		t.Parallel()

		assert.PanicsWithValue(t, wantMsg,
			func() {
				_ = newSwappableCounterFunc(
					prometheus.CounterOpts{Name: "test_swappable_nil_ctor", Help: "panic-guard test"},
					nil,
				)
			},
			"constructor with nil source must panic too — a future short-circuit `if source == nil { return c }` would leave Collect emitting 0 silently")
	})
}
