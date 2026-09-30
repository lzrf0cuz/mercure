package redistransport

import (
	"context"
	"fmt"
	"runtime"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"weak"

	"github.com/alicebob/miniredis/v2"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	metricPoolHits       = "mercure_redis_client_pool_hits_total"
	metricPoolTotalConns = "mercure_redis_client_pool_total_conns"
)

// mercure_redis_ready follows Ready at scrape time: 1 when ready, 0 while PING
// is failing and 0 while the listener is stale.
func TestReadyGaugeFollowsReady(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	stall := newListenerStallHook()
	tr, hook := newPingFaultTransportWithHooks(t, []redis.Hook{stall}, WithPrometheusRegisterer(reg))

	require.NoError(t, tr.Ready(context.Background()))
	assert.InDelta(t, 1, registeredValue(t, reg, metricReady), 0, "ready")

	failPing(t, tr, hook)
	assert.InDelta(t, 0, registeredValue(t, reg, metricReady), 0, "PING failing")

	recoverPing(t, tr, hook)
	assert.InDelta(t, 1, registeredValue(t, reg, metricReady), 0, "PING recovered")

	stallListener(t, tr, stall)
	assert.InDelta(t, 0, registeredValue(t, reg, metricReady), 0, "listener stale")
}

// A transport bound to a second registry registers the same ready collector
// there; both registries read the transport's state.
func TestReadyGaugeOnEveryBoundRegistry(t *testing.T) {
	t.Parallel()

	reg1 := prometheus.NewRegistry()
	reg2 := prometheus.NewRegistry()
	tr, hook := newPingFaultTransport(t, WithPrometheusRegisterer(reg1))

	require.NoError(t, tr.RegisterMetricsWith(reg2))
	assertHoldsOwnCollectors(t, reg2, []prometheus.Collector{tr.readyCollector}, "second")

	for name, reg := range map[string]*prometheus.Registry{"first": reg1, "second": reg2} {
		assert.InDelta(t, 1, registeredValue(t, reg, metricReady), 0, "%s registry, ready", name)
	}

	failPing(t, tr, hook)

	for name, reg := range map[string]*prometheus.Registry{"first": reg1, "second": reg2} {
		assert.InDelta(t, 0, registeredValue(t, reg, metricReady), 0, "%s registry, PING failing", name)
	}
}

// Two live same-stream transports on one registry share the ready collector,
// which reads 0 when either is not ready, whichever bound last.
func TestReadyGaugeIsWorstOfLiveTransports(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	a, _ := newTestTransport(t, WithPrometheusRegisterer(reg))
	b, _ := newTestTransport(t)

	require.NoError(t, b.RegisterMetricsWith(reg))
	require.Same(t, a.readyCollector, b.readyCollector, "precondition: b adopted a's ready collector")
	assert.InDelta(t, 1, registeredValue(t, reg, metricReady), 0, "both ready")

	b.healthy.Store(false)
	require.NoError(t, a.Ready(context.Background()))
	assert.InDelta(t, 0, registeredValue(t, reg, metricReady), 0, "b, bound last, not ready")

	b.healthy.Store(true)
	assert.InDelta(t, 1, registeredValue(t, reg, metricReady), 0, "b ready again")

	a.healthy.Store(false)
	require.NoError(t, b.Ready(context.Background()))
	assert.InDelta(t, 0, registeredValue(t, reg, metricReady), 0, "a, bound first, not ready")
}

// A sibling adopts the transport's collectors on the first registry, then
// closes (a Caddy reload that drops one of two same-stream hubs). The
// closed sibling drops out, and binding the surviving transport to the next
// registry reports it there too.
func TestReadyAndPoolGaugesFollowLatestBindAfterSiblingCloses(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	reg1 := prometheus.NewRegistry()
	reg2 := prometheus.NewRegistry()
	a, _ := newTestTransport(t)
	b, _ := newTestTransport(t)

	require.NoError(t, a.RegisterMetricsWith(reg1))
	require.NoError(t, b.RegisterMetricsWith(reg1))
	require.Same(t, a.readyCollector, b.readyCollector, "precondition: b adopted a's ready collector")
	require.Same(t, a.poolCollector, b.poolCollector, "precondition: b adopted a's pool collector")
	require.True(t, slices.Equal([]*RedisTransport{a, b}, a.poolCollector.transports.snapshot()),
		"precondition: b's client joined last")

	require.NoError(t, b.Close(ctx))
	require.NoError(t, a.Ready(ctx))
	require.InDelta(t, 1, registeredValue(t, reg1, metricReady), 0, "precondition: closed b dropped out")

	require.NoError(t, a.RegisterMetricsWith(reg2))

	assert.InDelta(t, 1, registeredValue(t, reg2, metricReady), 0, "the new registry must report a, which is ready")
	assert.True(t, slices.Equal([]*RedisTransport{a}, a.poolCollector.transports.snapshot()),
		"the pool collector must scrape a's client")
}

// Transport B binds the registry live transport A uses, through
// WithPrometheusRegisterer, then fails startup on the presence SET. B must
// leave the shared collectors, which keep reading A.
func TestFailedConstructionLeavesSharedCollectors(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	a, _ := newTestTransport(t, WithPrometheusRegisterer(reg))

	// Only SET fails, so INFO-based version detection runs as usual.
	hook := &cmdErrHook{errs: map[string]error{"set": replyError("ERR injected SET failure")}}
	hook.on.Store(true)

	client := redis.NewClient(&redis.Options{Addr: miniredis.RunT(t).Addr()})
	client.AddHook(hook)

	b, err := NewRedisTransport(client,
		WithLogger(testLogger()),
		WithPresenceInterval(noHeartbeatInterval),
		withSkipPresenceIntervalCheck(),
		withSkipVersionCheck(),
		WithPrometheusRegisterer(reg),
	)
	require.ErrorContains(t, err, "failed to set initial presence key")
	require.Nil(t, b)

	require.NoError(t, a.Ready(context.Background()))
	assert.InDelta(t, 1, registeredValue(t, reg, metricReady), 0, "the registry must report live a")
	assert.True(t, slices.Equal([]*RedisTransport{a}, a.readyCollector.transports.snapshot()),
		"the ready collector must read only a")
	assert.True(t, slices.Equal([]*RedisTransport{a}, a.poolCollector.transports.snapshot()),
		"the pool collector must read a's client")
	assert.GreaterOrEqual(t, registeredValue(t, reg, metricPoolTotalConns), float64(1),
		"the pool gauges must read a's open client, not b's closed one")
}

// A sibling that adopted the transport's collectors closes; with no re-bind,
// the registry reports the surviving transport (the failed Caddy reload
// shape: the candidate hub closes and the old config stays live).
func TestClosedSiblingLeavesSharedCollectors(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	reg := prometheus.NewRegistry()
	a, _ := newTestTransport(t, WithPrometheusRegisterer(reg))
	b, _ := newTestTransport(t)

	require.NoError(t, b.RegisterMetricsWith(reg))
	require.Same(t, a.readyCollector, b.readyCollector, "precondition: b adopted a's ready collector")

	require.NoError(t, b.Close(ctx))

	require.NoError(t, a.Ready(ctx))
	assert.InDelta(t, 1, registeredValue(t, reg, metricReady), 0, "the registry must report live a")
	assert.GreaterOrEqual(t, registeredValue(t, reg, metricPoolTotalConns), float64(1),
		"the pool gauges must read a's open client, not b's closed one")
}

// The pool gauges read the client of the member that joined last, neither the
// first one nor the sum: summing would make the counters drop when a member
// leaves.
func TestPoolGaugesReadLatestMember(t *testing.T) {
	t.Parallel()

	const aPings, bPings = 2000, 500

	ctx := context.Background()
	reg := prometheus.NewRegistry()
	a, _ := newTestTransport(t, WithPrometheusRegisterer(reg))
	b, _ := newTestTransport(t)

	require.NoError(t, b.RegisterMetricsWith(reg))

	for range aPings {
		require.NoError(t, a.client.Ping(ctx).Err())
	}

	for range bPings {
		require.NoError(t, b.client.Ping(ctx).Err())
	}

	hits := registeredValue(t, reg, metricPoolHits)

	assert.GreaterOrEqual(t, hits, float64(bPings-1), "the pool gauges must read b's client")
	assert.Less(t, hits, float64(aPings), "the pool gauges must read b's client alone, not a's or the sum")
}

// Once every member has closed, the shared collectors emit no samples.
func TestSharedCollectorsEmitNothingWithoutMembers(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	reg := prometheus.NewRegistry()
	a, _ := newTestTransport(t, WithPrometheusRegisterer(reg))
	b, _ := newTestTransport(t)

	require.NoError(t, b.RegisterMetricsWith(reg))
	require.InDelta(t, 1, registeredValue(t, reg, metricReady), 0, "precondition: the ready gauge is exposed")
	registeredValue(t, reg, metricPoolTotalConns)

	require.NoError(t, b.Close(ctx))
	require.NoError(t, a.Close(ctx))

	families, err := reg.Gather()
	require.NoError(t, err)

	for _, f := range families {
		assert.NotEqual(t, metricReady, f.GetName(), "no ready sample without members")
		assert.False(t, strings.HasPrefix(f.GetName(), "mercure_redis_client_pool_"),
			"no pool sample without members: %s", f.GetName())
	}
}

// A reused transport bound to a new registry joins the ready and pool
// collectors that registry holds for its identity, whichever same-stream
// transport registered them there first, so its unreadiness shows there.
func TestReadyGaugeShowsReusedTransportOnSharedRegistry(t *testing.T) {
	t.Parallel()

	for name, siblingFirst := range map[string]bool{
		"sibling registered first": true,
		"reused registered first":  false,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			reused, _ := newTestTransport(t)
			sibling, _ := newTestTransport(t)
			reg1 := prometheus.NewRegistry()
			reg2 := prometheus.NewRegistry()

			require.NoError(t, reused.RegisterMetricsWith(reg1))

			if siblingFirst {
				require.NoError(t, sibling.RegisterMetricsWith(reg2))
				require.NoError(t, reused.RegisterMetricsWith(reg2))
			} else {
				require.NoError(t, reused.RegisterMetricsWith(reg2))
				require.NoError(t, sibling.RegisterMetricsWith(reg2))
			}

			assert.True(t, slices.Contains(sibling.poolCollector.transports.snapshot(), reused),
				"the reused transport must be a member of the pool collector reg2 holds")
			require.InDelta(t, 1, registeredValue(t, reg2, metricReady), 0, "precondition: both ready")

			reused.healthy.Store(false)
			require.NoError(t, sibling.Ready(context.Background()))
			assert.InDelta(t, 0, registeredValue(t, reg2, metricReady), 0,
				"the reused transport's unreadiness must show on reg2")
		})
	}
}

// Close leaves every collector the transport joined: its own, and a
// same-stream sibling's it joined on a later bind.
func TestCloseLeavesEveryJoinedCollector(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	reused, _ := newTestTransport(t)
	sibling, _ := newTestTransport(t)
	reg1 := prometheus.NewRegistry()
	reg2 := prometheus.NewRegistry()

	require.NoError(t, reused.RegisterMetricsWith(reg1))
	require.NoError(t, sibling.RegisterMetricsWith(reg2))
	require.NoError(t, reused.RegisterMetricsWith(reg2))

	collectors := map[string]*collectorMembers{
		"own pool":      &reused.poolCollector.transports,
		"own ready":     &reused.readyCollector.transports,
		"sibling pool":  &sibling.poolCollector.transports,
		"sibling ready": &sibling.readyCollector.transports,
	}

	for name, members := range collectors {
		require.True(t, slices.Contains(members.snapshot(), reused), "precondition: reused joined the %s collector", name)
	}

	require.NoError(t, reused.Close(ctx))

	for name, members := range collectors {
		assert.False(t, slices.Contains(members.snapshot(), reused), "reused must have left the %s collector", name)
	}

	require.NoError(t, sibling.Ready(ctx))
	assert.InDelta(t, 1, registeredValue(t, reg2, metricReady), 0, "reg2 must report the live sibling alone")
}

// A closed transport joins nothing on a later bind: not its own collectors on
// a fresh registry, and not a same-stream sibling's.
func TestBindAfterCloseDoesNotJoin(t *testing.T) {
	t.Parallel()

	closed, _ := newTestTransport(t)
	sibling, _ := newTestTransport(t)
	siblingReg := prometheus.NewRegistry()
	freshReg := prometheus.NewRegistry()

	require.NoError(t, closed.RegisterMetricsWith(prometheus.NewRegistry()))
	require.NoError(t, closed.Close(context.Background()))
	require.NoError(t, sibling.RegisterMetricsWith(siblingReg))

	require.NoError(t, closed.RegisterMetricsWith(siblingReg))
	require.NoError(t, closed.RegisterMetricsWith(freshReg))

	assert.True(t, slices.Equal([]*RedisTransport{sibling}, sibling.readyCollector.transports.snapshot()),
		"a closed transport must not join a sibling's ready collector")
	assert.True(t, slices.Equal([]*RedisTransport{sibling}, sibling.poolCollector.transports.snapshot()),
		"a closed transport must not join a sibling's pool collector")
	assert.Empty(t, closed.readyCollector.transports.snapshot(), "a closed transport must not rejoin its own ready collector")
	assert.Empty(t, closed.poolCollector.transports.snapshot(), "a closed transport must not rejoin its own pool collector")

	families, err := freshReg.Gather()
	require.NoError(t, err)

	for _, f := range families {
		assert.NotEqual(t, metricReady, f.GetName(), "the fresh registry must have no ready sample for a closed transport")
	}
}

// blockCmdHookMaxPark bounds how long blockCmdHook parks a command. A test that
// fails before closing release would otherwise leave the command parked
// forever, and a cleanup that waits on it (Close waiting on its own parked
// Close) would hang the run until the go test timeout.
const blockCmdHookMaxPark = 10 * time.Second

// blockCmdHook parks the first command matching match, while on, until
// release is closed or blockCmdHookMaxPark elapses. entered is closed once a
// command has parked.
type blockCmdHook struct {
	match   func(redis.Cmder) bool
	on      atomic.Bool
	parked  atomic.Bool
	entered chan struct{}
	release chan struct{}
}

func newBlockCmdHook(match func(redis.Cmder) bool) *blockCmdHook {
	return &blockCmdHook{match: match, entered: make(chan struct{}), release: make(chan struct{})}
}

func (*blockCmdHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *blockCmdHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if h.on.Load() && h.match(cmd) && h.parked.CompareAndSwap(false, true) {
			close(h.entered)

			select {
			case <-h.release:
			case <-time.After(blockCmdHookMaxPark):
			}
		}

		return next(ctx, cmd)
	}
}

func (*blockCmdHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

// awaitParked waits until a command has parked in h.
func awaitParked(t *testing.T, h *blockCmdHook) {
	t.Helper()

	select {
	case <-h.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("no command reached the blocking hook")
	}
}

// hasFamily reports whether reg exposes any sample of family name.
func hasFamily(t *testing.T, reg *prometheus.Registry, name string) bool {
	t.Helper()

	families, err := reg.Gather()
	require.NoError(t, err)

	return slices.ContainsFunc(families, func(f *dto.MetricFamily) bool { return f.GetName() == name })
}

// constructParked starts building a transport bound to reg, parks it on its
// initial presence SET, and returns a function that lets it finish. While
// parked, the transport has joined reg's shared collectors but not started.
func constructParked(t *testing.T, reg *prometheus.Registry) func() *RedisTransport {
	t.Helper()

	block := newBlockCmdHook(func(cmd redis.Cmder) bool { return cmd.Name() == "set" })
	block.on.Store(true)

	client := redis.NewClient(&redis.Options{Addr: miniredis.RunT(t).Addr()})
	client.AddHook(block)

	type result struct {
		tr  *RedisTransport
		err error
	}

	done := make(chan result, 1)

	go func() {
		tr, err := NewRedisTransport(client,
			WithLogger(testLogger()),
			WithXReadBlock(50*time.Millisecond),
			WithHealthInterval(24*time.Hour),
			WithPresenceInterval(noHeartbeatInterval),
			withSkipPresenceIntervalCheck(),
			withSkipVersionCheck(),
			WithPrometheusRegisterer(reg),
		)
		done <- result{tr, err}
	}()

	awaitParked(t, block)

	return func() *RedisTransport {
		close(block.release)

		r := <-done
		require.NoError(t, r.err)
		t.Cleanup(func() { _ = r.tr.Close(context.Background()) })

		return r.tr
	}
}

// A transport whose construction binds a registry (WithPrometheusRegisterer)
// joins the ready collector before its startup has run. Until construction
// finishes it is not read: a live sibling's series stays 1, and alone it
// emits no sample.
func TestReadyGaugeSkipsTransportUnderConstruction(t *testing.T) {
	t.Parallel()

	t.Run("beside a live sibling", func(t *testing.T) {
		t.Parallel()

		reg := prometheus.NewRegistry()
		live, _ := newTestTransport(t, WithPrometheusRegisterer(reg))
		finish := constructParked(t, reg)

		require.Len(t, live.readyCollector.transports.snapshot(), 2, "precondition: the transport under construction joined")
		require.NoError(t, live.Ready(context.Background()))
		assert.InDelta(t, 1, registeredValue(t, reg, metricReady), 0,
			"a transport under construction must not drag the live sibling's series to 0")

		finish()
		assert.InDelta(t, 1, registeredValue(t, reg, metricReady), 0, "both ready once constructed")
	})

	t.Run("alone", func(t *testing.T) {
		t.Parallel()

		reg := prometheus.NewRegistry()
		finish := constructParked(t, reg)

		assert.False(t, hasFamily(t, reg, metricReady), "no ready sample while the only member is under construction")

		finish()
		assert.InDelta(t, 1, registeredValue(t, reg, metricReady), 0, "ready once constructed")
	})
}

// The pool gauges skip a member that has not started: beside a live sibling
// they keep reading the sibling's client, and alone they emit no samples.
func TestPoolGaugesSkipTransportUnderConstruction(t *testing.T) {
	t.Parallel()

	t.Run("beside a live sibling", func(t *testing.T) {
		t.Parallel()

		const livePings = 500

		ctx := context.Background()
		reg := prometheus.NewRegistry()
		live, _ := newTestTransport(t, WithPrometheusRegisterer(reg))

		for range livePings {
			require.NoError(t, live.client.Ping(ctx).Err())
		}

		finish := constructParked(t, reg)

		require.Len(t, live.poolCollector.transports.snapshot(), 2, "precondition: the transport under construction joined")
		assert.GreaterOrEqual(t, registeredValue(t, reg, metricPoolHits), float64(livePings-1),
			"the pool gauges must read the live sibling's client, not the one under construction")

		finish()
		assert.Less(t, registeredValue(t, reg, metricPoolHits), float64(livePings-1),
			"the pool gauges read the latest member once it has started")
	})

	t.Run("alone", func(t *testing.T) {
		t.Parallel()

		reg := prometheus.NewRegistry()
		finish := constructParked(t, reg)

		assert.False(t, hasFamily(t, reg, metricPoolTotalConns), "no pool samples while the only member is under construction")

		finish()
		assert.True(t, hasFamily(t, reg, metricPoolTotalConns), "pool samples once constructed")
	})
}

// A member that closes after a scrape took its member snapshot is skipped,
// not read as not ready. Putting a closed transport back in the member set
// gives Collect that snapshot deterministically.
func TestReadyGaugeSkipsMemberClosedMidScrape(t *testing.T) {
	t.Parallel()

	t.Run("beside a live sibling", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		reg := prometheus.NewRegistry()
		live, _ := newTestTransport(t, WithPrometheusRegisterer(reg))
		closed, _ := newTestTransport(t)

		require.NoError(t, closed.RegisterMetricsWith(reg))
		require.NoError(t, closed.Close(ctx))
		live.readyCollector.transports.add(closed)

		require.ErrorIs(t, closed.Ready(ctx), ErrClosedTransport, "precondition: the closed member reads as closed")
		require.NoError(t, live.Ready(ctx))
		assert.InDelta(t, 1, registeredValue(t, reg, metricReady), 0,
			"a member closed mid-scrape must not drag the live sibling's series to 0")
	})

	t.Run("alone", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		reg := prometheus.NewRegistry()
		tr, _ := newTestTransport(t, WithPrometheusRegisterer(reg))
		collector := tr.readyCollector

		require.NoError(t, tr.Close(ctx))
		collector.transports.add(tr)

		assert.False(t, hasFamily(t, reg, metricReady), "no ready sample when the only member closed mid-scrape")
	})
}

// Close leaves the shared collectors before its Redis cleanup, so a
// same-stream sibling's ready series stays 1 for the whole of Close.
func TestCloseLeavesSharedCollectorsBeforeCleanup(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	reg := prometheus.NewRegistry()
	live, _ := newTestTransport(t, WithPrometheusRegisterer(reg))

	block := newBlockCmdHook(func(cmd redis.Cmder) bool {
		args := cmd.Args()

		return cmd.Name() == "xgroup" && len(args) > 1 && strings.EqualFold(fmt.Sprint(args[1]), "destroy")
	})
	closing := newHookedTestTransport(t, []redis.Hook{block}, WithPrometheusRegisterer(reg))
	require.Same(t, live.readyCollector, closing.readyCollector, "precondition: one shared ready collector")

	block.on.Store(true)

	closeErr := make(chan error, 1)

	go func() { closeErr <- closing.Close(ctx) }()

	awaitParked(t, block)

	require.NoError(t, live.Ready(ctx))
	assert.InDelta(t, 1, registeredValue(t, reg, metricReady), 0,
		"a transport inside Close must not drag the live sibling's series to 0")

	close(block.release)
	require.NoError(t, <-closeErr)
	assert.InDelta(t, 1, registeredValue(t, reg, metricReady), 0, "after Close")
}

// newUncleanedTestTransport is newTestTransport without the cleanup that
// closes it, so nothing the test keeps references the transport.
func newUncleanedTestTransport(t *testing.T) *RedisTransport {
	t.Helper()

	client := redis.NewClient(&redis.Options{Addr: miniredis.RunT(t).Addr()})

	tr, err := NewRedisTransport(client,
		WithLogger(testLogger()),
		WithXReadBlock(50*time.Millisecond),
		WithHealthInterval(24*time.Hour),
		WithPresenceInterval(noHeartbeatInterval),
		withSkipPresenceIntervalCheck(),
		withSkipVersionCheck(),
	)
	require.NoError(t, err)

	return tr
}

// The transport holds the member sets of the collectors it joined weakly. A
// same-stream sibling's collectors, joined on a later bind, become
// collectable once the sibling has closed and its registry is gone. The next
// join prunes the cleared records, and Close skips cleared ones and still
// leaves every live joined collector.
func TestJoinedSiblingCollectorsAreCollectable(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	survivor, _ := newTestTransport(t)
	require.NoError(t, survivor.RegisterMetricsWith(prometheus.NewRegistry()))

	// goneSibling registers a sibling first on a registry the survivor then
	// binds (a reload in which a new same-stream sibling registers first),
	// closes the sibling, drops it and the registry, and waits until its
	// collectors have been collected.
	goneSibling := func() {
		t.Helper()

		gone := func() []weak.Pointer[collectorMembers] {
			sibling := newUncleanedTestTransport(t)

			reg := prometheus.NewRegistry()
			require.NoError(t, sibling.RegisterMetricsWith(reg))
			require.NoError(t, survivor.RegisterMetricsWith(reg))
			require.True(t, slices.Contains(sibling.readyCollector.transports.snapshot(), survivor),
				"precondition: the survivor joined the sibling's ready collector")
			require.NoError(t, sibling.Close(context.Background()))

			return []weak.Pointer[collectorMembers]{
				weak.Make(&sibling.poolCollector.transports),
				weak.Make(&sibling.readyCollector.transports),
			}
		}()

		require.Eventually(t, func() bool {
			runtime.GC()

			return gone[0].Value() == nil && gone[1].Value() == nil
		}, 5*time.Second, 10*time.Millisecond,
			"the closed sibling's collectors must be collectable once its registry is gone")
	}

	goneSibling()

	live, _ := newTestTransport(t)
	liveReg := prometheus.NewRegistry()
	require.NoError(t, live.RegisterMetricsWith(liveReg))
	require.NoError(t, survivor.RegisterMetricsWith(liveReg))

	survivor.metricsMu.Lock()
	joined := slices.Clone(survivor.joined)
	survivor.metricsMu.Unlock()

	assert.Len(t, joined, 4, "own pool and ready, live sibling's pool and ready")

	for i, p := range joined {
		assert.NotNil(t, p.Value(), "joined record %d must have been pruned once cleared", i)
	}

	// Cleared records Close must skip.
	goneSibling()

	require.NoError(t, survivor.Close(ctx))

	for name, members := range map[string]*collectorMembers{
		"own pool":           &survivor.poolCollector.transports,
		"own ready":          &survivor.readyCollector.transports,
		"live sibling pool":  &live.poolCollector.transports,
		"live sibling ready": &live.readyCollector.transports,
	} {
		assert.False(t, slices.Contains(members.snapshot(), survivor), "the survivor must have left the %s collector", name)
	}
}

// The pool gauges skip a member that closes after a scrape took its member
// snapshot: beside a live sibling they read the sibling's client, and alone
// they emit no samples. Putting a closed transport back in the member set
// gives Collect that snapshot deterministically.
func TestPoolGaugesSkipMemberClosedMidScrape(t *testing.T) {
	t.Parallel()

	t.Run("beside a live sibling", func(t *testing.T) {
		t.Parallel()

		const livePings = 500

		ctx := context.Background()
		reg := prometheus.NewRegistry()
		live, _ := newTestTransport(t, WithPrometheusRegisterer(reg))
		closed, _ := newTestTransport(t)

		for range livePings {
			require.NoError(t, live.client.Ping(ctx).Err())
		}

		require.NoError(t, closed.RegisterMetricsWith(reg))
		require.NoError(t, closed.Close(ctx))
		live.poolCollector.transports.add(closed)

		require.Same(t, closed, live.poolCollector.transports.snapshot()[1], "precondition: the closed member is the latest")
		assert.GreaterOrEqual(t, registeredValue(t, reg, metricPoolHits), float64(livePings-1),
			"the pool gauges must read the live sibling's client, not the closed member's")
	})

	t.Run("alone", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		reg := prometheus.NewRegistry()
		tr, _ := newTestTransport(t, WithPrometheusRegisterer(reg))
		collector := tr.poolCollector

		require.NoError(t, tr.Close(ctx))
		collector.transports.add(tr)

		assert.False(t, hasFamily(t, reg, metricPoolTotalConns), "no pool samples when the only member closed mid-scrape")
	})
}
