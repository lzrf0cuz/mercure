package redistransport

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"weak"

	"github.com/alicebob/miniredis/v2"
	"github.com/dunglas/mercure"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Tests for RegisterMetricsWith across more than one registry: Caddy builds a
// fresh registry per config load and reuses an unchanged transport, so the
// same transport is bound to a new registry on every reload.

const rebindTopic = "https://example.com/rebind"

// driveRebindTraffic adds a subscriber, publishes one update, waits for it to
// arrive, and removes the subscriber, so subscriber, publish and dispatch
// metrics all move by a known amount.
func driveRebindTraffic(t *testing.T, transport *RedisTransport, mr *miniredis.Miniredis) {
	t.Helper()

	ctx := context.Background()

	s := mercure.NewLocalSubscriber("", testLogger(), testTopicMatcherStore())
	s.SetMatchers(topicMatchers([]string{rebindTopic}), nil)
	require.NoError(t, transport.AddSubscriber(ctx, s))

	require.NoError(t, transport.Dispatch(ctx, &mercure.Update{
		Topics: []string{rebindTopic},
		Data:   "rebind",
	}))
	mr.FastForward(100 * time.Millisecond)

	select {
	case <-s.Receive():
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for update")
	}

	require.NoError(t, transport.RemoveSubscriber(ctx, s))
}

// assertHoldsOwnCollectors asserts reg holds each of own as that very
// object: re-registering it must report AlreadyRegistered with the identical
// collector.
func assertHoldsOwnCollectors(t *testing.T, reg prometheus.Registerer, own []prometheus.Collector, name string) {
	t.Helper()

	for _, c := range own {
		var are prometheus.AlreadyRegisteredError

		require.ErrorAsf(t, reg.Register(c), &are, "%s registry must already hold %T", name, c)
		assert.Samef(t, c, are.ExistingCollector,
			"%s registry holds a different %T object than the transport's own", name, c)
	}
}

// TestRegisterMetricsWithSecondRegistryGetsSameCollectors covers a reload:
// one transport bound to two registries must have its own
// collector objects (every one, pool collector included) on both. Traffic
// runs before and after the second bind; counters no background loop
// touches must show both rounds on both registries (no reset).
func TestRegisterMetricsWithSecondRegistryGetsSameCollectors(t *testing.T) {
	t.Parallel()

	transport, mr := newTestTransport(t)
	reg1 := prometheus.NewRegistry()
	reg2 := prometheus.NewRegistry()

	require.NoError(t, transport.RegisterMetricsWith(reg1))
	driveRebindTraffic(t, transport, mr)

	require.NoError(t, transport.RegisterMetricsWith(reg2))
	driveRebindTraffic(t, transport, mr)

	own := append(transport.metrics.Load().collectors(), transport.poolCollector)

	for name, reg := range map[string]*prometheus.Registry{"first": reg1, "second": reg2} {
		assertHoldsOwnCollectors(t, reg, own, name)

		assert.InDelta(t, 2, counterValue(t, reg, metricSubscriberAddTotal), 0,
			"%s registry must show both traffic rounds: counters continue, they do not reset", name)
		assert.InDelta(t, 2, counterValue(t, reg, metricSubscriberRemoveTotal), 0,
			"%s registry must show both traffic rounds", name)
	}
}

// TestRegisterMetricsWithNilRegisterer pins that a nil or typed-nil registerer
// is a no-op that logs at Debug, both before and after a registry is bound.
func TestRegisterMetricsWithNilRegisterer(t *testing.T) {
	t.Parallel()

	var typedNil *prometheus.Registry

	cases := map[string]prometheus.Registerer{
		"untyped nil": nil,
		"typed nil":   typedNil,
	}

	for name, nilReg := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			logBuf := &safeBuffer{}
			logger := slog.New(slog.NewJSONHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))
			transport, _ := newTestTransport(t, WithLogger(logger))

			// The constructor path already called RegisterMetricsWith(nil).
			logOffset := len(logBuf.String())

			require.NoError(t, transport.RegisterMetricsWith(nilReg))
			assert.Nil(t, transport.metrics.Load(), "first call with a nil registerer must leave metrics unbound")

			reg := prometheus.NewRegistry()
			require.NoError(t, transport.RegisterMetricsWith(reg))

			bound := transport.metrics.Load()
			require.NotNil(t, bound)

			require.NoError(t, transport.RegisterMetricsWith(nilReg))
			assert.Same(t, bound, transport.metrics.Load(), "later call with a nil registerer must change nothing")

			callLog := logBuf.String()[logOffset:]
			assert.Equal(t, 2, strings.Count(callLog, `"level":"DEBUG","msg":"redis transport: RegisterMetricsWith skipped — registerer is nil or typed-nil"`),
				"each nil call must leave the Debug breadcrumb")
			assert.NotContains(t, callLog, "additional registry")
		})
	}
}

// TestRegisterMetricsWithNonComparableRegisterer covers a later call with a
// non-comparable registerer: every own collector and the pool collector are
// offered to it, nothing panics, and the published set is unchanged.
func TestRegisterMetricsWithNonComparableRegisterer(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t)
	require.NoError(t, transport.RegisterMetricsWith(prometheus.NewRegistry()))

	bound := transport.metrics.Load()

	var calls atomic.Int64

	require.NotPanics(t, func() {
		require.NoError(t, transport.RegisterMetricsWith(nonComparableRegisterer{tags: []string{"a"}, registered: &calls}))
	})
	assert.Equal(t, int64(len(bound.collectors())+1), calls.Load(),
		"every own collector plus the pool collector must be registered on the non-comparable registerer")
	assert.Same(t, bound, transport.metrics.Load(), "a later call must not publish a new collector set")
}

// bindThrowawayRegistry records a stream binding for a registry that is
// unreachable once this function returns.
//
//go:noinline
func bindThrowawayRegistry(logger *slog.Logger) weak.Pointer[prometheus.Registry] {
	reg := prometheus.NewRegistry()
	warnIfRegistererAlreadyBoundToDifferentStream(reg, "stream-dead", logger)

	return weak.Make(reg)
}

// TestStreamBindingsPruneCollectedRegistry — the stream-binding map must not
// keep a discarded *prometheus.Registry alive, and must drop its entry on the
// next access. Mutates package-level state, so not parallel-safe.
func TestStreamBindingsPruneCollectedRegistry(t *testing.T) {
	t.Cleanup(resetRegistererBindingsForTest)
	resetRegistererBindingsForTest()

	logger := slog.New(slog.DiscardHandler)
	dead := bindThrowawayRegistry(logger)

	collected := false

	for range 20 {
		runtime.GC()

		if dead.Value() == nil {
			collected = true

			break
		}
	}

	require.True(t, collected, "registry must be collectable: the binding map must not hold it strongly")

	live := prometheus.NewRegistry()
	warnIfRegistererAlreadyBoundToDifferentStream(live, "stream-live", logger)

	registererStreamBindingsMu.Lock()
	_, deadPresent := registererStreamBindings[dead]
	entries := len(registererStreamBindings)
	registererStreamBindingsMu.Unlock()

	assert.False(t, deadPresent, "entry for the collected registry must be pruned")
	assert.Equal(t, 1, entries, "only the live registry's entry must remain")

	runtime.KeepAlive(live)
}

// addRebindSubscribers adds n subscribers to transport and returns them.
func addRebindSubscribers(t *testing.T, transport *RedisTransport, n int) []*mercure.LocalSubscriber {
	t.Helper()

	subs := make([]*mercure.LocalSubscriber, n)

	for i := range subs {
		subs[i] = mercure.NewLocalSubscriber("", testLogger(), testTopicMatcherStore())
		subs[i].SetMatchers(topicMatchers([]string{rebindTopic}), nil)
		require.NoError(t, transport.AddSubscriber(context.Background(), subs[i]))
	}

	return subs
}

// gaugeSeriesValues returns the value of every series of every gauge family
// on reg, keyed by family name.
func gaugeSeriesValues(t *testing.T, reg *prometheus.Registry) map[string][]float64 {
	t.Helper()

	mfs, err := reg.Gather()
	require.NoError(t, err)

	out := make(map[string][]float64)

	for _, mf := range mfs {
		if mf.GetType() != dto.MetricType_GAUGE {
			continue
		}

		for _, m := range mf.GetMetric() {
			out[mf.GetName()] = append(out[mf.GetName()], m.GetGauge().GetValue())
		}
	}

	return out
}

func sumValues(values []float64) float64 {
	var total float64
	for _, v := range values {
		total += v
	}

	return total
}

// TestRegisterMetricsWithSiblingFirstNeverDrivesSharedSeriesNegative covers a
// reload with a collision (two transports, same backend
// type): the reused transport has subscribers before the second bind, a new
// sibling registered on the second registry first, then the reused
// transport's subscribers leave. The second registry must show only the
// sibling's own counts — the reused transport's removals must not land on
// the sibling's series — and the shard-subscribers gauge must not go
// negative on either registry (other gauges use negative sentinels).
func TestRegisterMetricsWithSiblingFirstNeverDrivesSharedSeriesNegative(t *testing.T) {
	t.Parallel()

	const (
		reusedSubs  = 3
		siblingSubs = 2
	)

	reused, _ := newTestTransport(t)
	sibling, _ := newTestTransport(t)
	reg1 := prometheus.NewRegistry()
	reg2 := prometheus.NewRegistry()

	require.NoError(t, reused.RegisterMetricsWith(reg1))

	subs := addRebindSubscribers(t, reused, reusedSubs)

	require.NoError(t, sibling.RegisterMetricsWith(reg2))
	addRebindSubscribers(t, sibling, siblingSubs)

	require.NoError(t, reused.RegisterMetricsWith(reg2))

	for _, s := range subs {
		require.NoError(t, reused.RemoveSubscriber(context.Background(), s))
	}

	const shardSubscribers = "mercure_redis_shard_subscribers"

	for name, reg := range map[string]*prometheus.Registry{"first": reg1, "second": reg2} {
		for _, v := range gaugeSeriesValues(t, reg)[shardSubscribers] {
			assert.GreaterOrEqual(t, v, float64(0), "%s registry: %s went negative", name, shardSubscribers)
		}
	}

	assert.InDelta(t, siblingSubs, sumValues(gaugeSeriesValues(t, reg2)[shardSubscribers]), 0,
		"second registry must count exactly the sibling's subscribers")
	assert.InDelta(t, siblingSubs, counterValue(t, reg2, metricSubscriberAddTotal), 0,
		"second registry must count exactly the sibling's additions")
	assert.InDelta(t, 0, counterValue(t, reg2, metricSubscriberRemoveTotal), 0,
		"the reused transport's removals must not land on the sibling's counter")
	assert.InDelta(t, 0, sumValues(gaugeSeriesValues(t, reg1)[shardSubscribers]), 0,
		"first registry: the reused transport added and removed its subscribers on its own gauge")
	assert.InDelta(t, reusedSubs, counterValue(t, reg1, metricSubscriberRemoveTotal), 0,
		"first registry must count the reused transport's removals")
}

// TestRegisterMetricsWithRepeatAfterSiblingAdoptionIsSilentNoOp covers hubs
// A1, B, A2 on one registry: B's first bind adopted A's collectors (so the
// pool collector reads B's client). A's repeat must change none of that and
// log nothing.
func TestRegisterMetricsWithRepeatAfterSiblingAdoptionIsSilentNoOp(t *testing.T) {
	t.Parallel()

	logBuf := &safeBuffer{}
	logger := slog.New(slog.NewJSONHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	a, _ := newTestTransport(t, WithLogger(logger))
	b, _ := newTestTransport(t)
	reg := prometheus.NewRegistry()

	require.NoError(t, a.RegisterMetricsWith(reg))
	require.NoError(t, b.RegisterMetricsWith(reg))

	before := a.metrics.Load()
	pool := a.poolCollector

	require.Same(t, pool, b.poolCollector, "precondition: B adopted A's pool collector")
	require.Same(t, b.client, *pool.client.Load(), "precondition: adoption swapped in B's client")

	logOffset := len(logBuf.String())

	require.NoError(t, a.RegisterMetricsWith(reg))

	repeatLog := logBuf.String()[logOffset:]

	assert.Same(t, before, a.metrics.Load(), "repeat must not publish a new collector set")
	assert.Same(t, pool, a.poolCollector, "repeat must keep the same pool collector")
	assert.Same(t, b.client, *pool.client.Load(), "repeat must not re-point the pool collector's client")
	assert.NotContains(t, repeatLog, "redis transport", "repeat must not log")
}

// TestRegisterMetricsWithFlipFlopKeepsPublishedSet covers A binds R1, B binds
// R2, A binds R2, A binds R1: the collector set A's hot path uses is the one
// from the first bind throughout, and the final R1 repeat is silent.
func TestRegisterMetricsWithFlipFlopKeepsPublishedSet(t *testing.T) {
	t.Parallel()

	logBuf := &safeBuffer{}
	logger := slog.New(slog.NewJSONHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	a, _ := newTestTransport(t, WithLogger(logger))
	b, _ := newTestTransport(t)
	reg1 := prometheus.NewRegistry()
	reg2 := prometheus.NewRegistry()

	require.NoError(t, a.RegisterMetricsWith(reg1))

	first := a.metrics.Load()
	pool := a.poolCollector

	require.NoError(t, b.RegisterMetricsWith(reg2))
	require.NoError(t, a.RegisterMetricsWith(reg2))
	assert.Same(t, first, a.metrics.Load(), "binding a registry a sibling holds must not change the published set")

	logOffset := len(logBuf.String())

	require.NoError(t, a.RegisterMetricsWith(reg1))
	assert.Same(t, first, a.metrics.Load(), "the repeat on the first registry must not change the published set")
	assert.Same(t, pool, a.poolCollector, "the pool collector must never change after the first bind")
	assert.NotContains(t, logBuf.String()[logOffset:], "redis transport", "the repeat on the first registry must not log")
}

// TestRegisterMetricsWithRebindUnderTraffic runs Dispatch and subscriber
// churn while the transport is bound to further registries, half of them
// held by a sibling first. Run under -race: the published collector set must
// never change after the first bind. The rebinds start only after every
// traffic goroutine has completed an operation, and traffic stops only after
// every goroutine has completed an operation after the last rebind.
func TestRegisterMetricsWithRebindUnderTraffic(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t, WithDispatchShards(4))
	require.NoError(t, transport.RegisterMetricsWith(prometheus.NewRegistry()))

	first := transport.metrics.Load()

	const (
		rebinds     = 4
		dispatchers = 4
		churners    = 2
	)

	// Siblings are built up front so the rebind loop runs while traffic flows.
	siblingRegs := make([]*prometheus.Registry, rebinds)

	for i := range rebinds {
		siblingRegs[i] = prometheus.NewRegistry()
		sibling, _ := newTestTransport(t)
		require.NoError(t, sibling.RegisterMetricsWith(siblingRegs[i]))
	}

	ctx := context.Background()
	stop := make(chan struct{})

	var (
		wg, started, finishedAfter sync.WaitGroup
		rebindsDone                atomic.Bool
	)

	started.Add(dispatchers + churners)
	finishedAfter.Add(dispatchers + churners)

	// runTraffic runs op until stop, marking the barriers: once after its
	// first operation, once after its first operation that began after the
	// last rebind.
	runTraffic := func(op func()) {
		wg.Go(func() {
			markedStart, markedAfter := false, false

			for {
				select {
				case <-stop:
					return
				default:
				}

				afterRebinds := rebindsDone.Load()

				op()

				if !markedStart {
					markedStart = true

					started.Done()
				}

				if afterRebinds && !markedAfter {
					markedAfter = true

					finishedAfter.Done()
				}
			}
		})
	}

	for range dispatchers {
		runTraffic(func() {
			// Dispatch assigns the update's ID, so each call gets its own.
			_ = transport.Dispatch(ctx, &mercure.Update{Topics: []string{rebindTopic}, Data: "race"})
		})
	}

	for range churners {
		tss := testTopicMatcherStore()

		runTraffic(func() {
			s := mercure.NewLocalSubscriber("", testLogger(), tss)
			s.SetMatchers(topicMatchers([]string{rebindTopic}), nil)

			if transport.AddSubscriber(ctx, s) == nil {
				_ = transport.RemoveSubscriber(ctx, s)
			}
		})
	}

	started.Wait()

	changed := 0

	for i := range rebinds {
		require.NoError(t, transport.RegisterMetricsWith(prometheus.NewRegistry()))
		require.NoError(t, transport.RegisterMetricsWith(siblingRegs[i]))

		if transport.metrics.Load() != first {
			changed++
		}
	}

	rebindsDone.Store(true)
	finishedAfter.Wait()
	close(stop)
	wg.Wait()

	assert.Zero(t, changed, "the published collector set must never change after the first bind")
}

// TestRegisterMetricsWithRepeatSkipsStreamCheck — the stream-binding check
// runs only when a call registered or skipped something. B (a different
// stream on A's registry) warns on its first bind; B's repeat must not warn
// again.
func TestRegisterMetricsWithRepeatSkipsStreamCheck(t *testing.T) {
	t.Parallel()

	logBuf := &safeBuffer{}
	logger := slog.New(slog.NewJSONHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	a, _ := newTestTransport(t, WithStreamName("rebind-stream-a"))
	b, _ := newTestTransport(t, WithStreamName("rebind-stream-b"), WithLogger(logger))
	reg := prometheus.NewRegistry()

	require.NoError(t, a.RegisterMetricsWith(reg))
	require.NoError(t, b.RegisterMetricsWith(reg))
	require.Contains(t, logBuf.String(), "registerer already bound to a different stream name",
		"precondition: B's first bind warns about the stream collision")

	logOffset := len(logBuf.String())

	require.NoError(t, b.RegisterMetricsWith(reg))
	assert.NotContains(t, logBuf.String()[logOffset:], "redis transport", "B's repeat must not run the stream check again")
}

// TestRegisterMetricsWithLaterBindLeavesSiblingCollectorsAlone — when a
// sibling registered first on the registry of a later bind, the later bind
// must not re-point the sibling's pool collector client.
func TestRegisterMetricsWithLaterBindLeavesSiblingCollectorsAlone(t *testing.T) {
	t.Parallel()

	reused, _ := newTestTransport(t)
	sibling, _ := newTestTransport(t)
	reg1 := prometheus.NewRegistry()
	reg2 := prometheus.NewRegistry()

	require.NoError(t, reused.RegisterMetricsWith(reg1))
	require.NoError(t, sibling.RegisterMetricsWith(reg2))
	require.NoError(t, reused.RegisterMetricsWith(reg2))

	assert.Same(t, sibling.client, *sibling.poolCollector.client.Load(),
		"a later bind must not re-point the sibling's pool collector")
}

// errInjectedRegister is the failure failAfterRegisterer injects.
var errInjectedRegister = errors.New("injected register failure")

// failAfterRegisterer is a *prometheus.Registry whose Register fails with
// errInjectedRegister once allow successful calls have passed, while failing
// is set.
type failAfterRegisterer struct {
	*prometheus.Registry

	failing bool
	allow   int
}

func (r *failAfterRegisterer) Register(c prometheus.Collector) error {
	if r.failing {
		if r.allow == 0 {
			return errInjectedRegister
		}

		r.allow--
	}

	return r.Registry.Register(c)
}

// TestRegisterMetricsWithLaterBindError — a later bind whose registration
// fails part-way returns the error (wrapped); a retry then succeeds.
func TestRegisterMetricsWithLaterBindError(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t)
	require.NoError(t, transport.RegisterMetricsWith(prometheus.NewRegistry()))

	own := append(transport.metrics.Load().collectors(), transport.poolCollector)
	reg := &failAfterRegisterer{Registry: prometheus.NewRegistry(), failing: true, allow: 10}

	err := transport.RegisterMetricsWith(reg)
	require.ErrorIs(t, err, errInjectedRegister, "the registration error must be returned")
	assert.Contains(t, err.Error(), "additional registry", "the error must name the stage")

	reg.failing = false

	require.NoError(t, transport.RegisterMetricsWith(reg), "a retry must succeed")
	assertHoldsOwnCollectors(t, reg.Registry, own, "retried")
}

// TestRegisterMetricsWithConcurrentCalls runs RegisterMetricsWith from
// several goroutines on several registries at once, starting from an unbound
// transport so the first bind is among them. Every registry must end up
// holding the one published collector set. Run under -race.
func TestRegisterMetricsWithConcurrentCalls(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t)

	const callers = 6

	regs := make([]*prometheus.Registry, callers)
	for i := range regs {
		regs[i] = prometheus.NewRegistry()
	}

	start := make(chan struct{})

	var wg sync.WaitGroup

	for _, reg := range regs {
		wg.Go(func() {
			<-start

			assert.NoError(t, transport.RegisterMetricsWith(reg))
		})
	}

	close(start)
	wg.Wait()

	published := transport.metrics.Load()
	require.NotNil(t, published)

	own := append(published.collectors(), transport.poolCollector)

	for i, reg := range regs {
		assertHoldsOwnCollectors(t, reg, own, fmt.Sprintf("registry %d", i))
	}
}
