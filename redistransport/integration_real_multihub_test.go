//go:build real_redis

package redistransport

// Multi-hub integration tests against a real Redis/Valkey server. These cover
// the horizontal-scaling invariants that single-transport real_redis tests
// cannot validate end-to-end:
//
//   A. Cross-node dispatch delivery
//   B. Cross-node GetSubscribers via presence MGET
//   C. Presence heartbeat visibility within presenceInterval
//   D. Zombie GC across hubs (one hub cleans up another's orphan group)
//   F. Close isolation (closing hub A leaves hub B's subscribers live)
//   G. Concurrent publish from 2+ hubs with no loss / no deadlock
//   H. Concurrent startup (multiple hubs racing to create groups)
//   J. Racing XGROUP DESTROY (two GCs converge on the same zombie)
//   K. Distributed history replay across the historyPageSize boundary
//
// Out of scope here (Option 2 — container / fault-injection territory):
//
//   I. Network partition recovery — requires Toxiproxy or iptables to inject
//      half-open TCP or dropped packets without killing the Go process.
//   L. UUIDv7-vs-stream-ID clock skew — requires hosts with divergent wall
//      clocks, which a single-process test cannot simulate faithfully.
//
// Two distinct redis.Client pools per hub (not a shared pool) — a shared pool
// would mask connection-level locking and exhaustion bugs. This matches the
// Centrifugo Redis-engine test harness pattern.

import (
	"context"
	"errors"
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

// Static sentinel errors for the drain goroutine — err113 forbids dynamic
// errors.New at the call site. Wrapped with fmt.Errorf at the reporting
// site so the progress counters stay in the message.
var (
	errMultiHubDrainChannelClosed = errors.New("subscriber channel closed mid-drain")
	errMultiHubDrainTimeout       = errors.New("drain timed out")
)

// newRealMultiHub returns n RedisTransport instances bound to the same real
// server and the same streamName. Each hub gets its own redis.Client pool.
// Cleanup closes all hubs and drops the stream + lastEventID + presence:*
// keys so tests are idempotent under -count=1.
//
// n is parameterized even though every current caller passes 2 — future tests
// (e.g. 3-way presence fan-out, > 2-node zombie GC) should not have to fork
// the helper. The unparam lint waiver below documents this intent.
//
//nolint:unparam // n is kept generic for future multi-hub topologies.
func newRealMultiHub(t *testing.T, n int, opts ...Option) []*RedisTransport {
	t.Helper()

	addr := probeRealRedis(t)

	streamName := fmt.Sprintf("mercure-mh-%s-%d", t.Name(), time.Now().UnixNano())

	defaultOpts := []Option{
		WithLogger(testLogger()),
		WithStreamName(streamName),
		WithXReadBlock(50 * time.Millisecond),
		WithHealthInterval(24 * time.Hour),
		WithPresenceInterval(24 * time.Hour),
		withSkipPresenceIntervalCheck(),
	}

	hubs := make([]*RedisTransport, n)
	for i := range n {
		client := redis.NewClient(&redis.Options{
			Addr:        addr,
			DialTimeout: 2 * time.Second,
			ReadTimeout: 2 * time.Second,
		})

		tr, err := NewRedisTransport(client, append(defaultOpts, opts...)...)
		require.NoError(t, err)

		hubs[i] = tr
	}

	t.Cleanup(func() {
		for _, h := range hubs {
			_ = h.Close(context.Background())
		}

		cleanupStreamKeys(addr, streamName)
	})

	return hubs
}

// TestReal_MultiHub_CrossNodeDispatch — invariant A.
// Publish on hub A, assert a subscriber on hub B receives.
func TestReal_MultiHub_CrossNodeDispatch(t *testing.T) {
	hubs := newRealMultiHub(t, 2)
	tss := testTopicSelectorStore()

	subB := mercure.NewLocalSubscriber("", testLogger(), tss)
	subB.SetTopics([]string{"https://example.com/mh-dispatch"}, nil)
	require.NoError(t, hubs[1].AddSubscriber(context.Background(), subB))

	require.NoError(t, hubs[0].Dispatch(context.Background(), &mercure.Update{
		Topics: []string{"https://example.com/mh-dispatch"},
		Event:  mercure.Event{Data: "from-A-to-B"},
	}))

	select {
	case msg := <-subB.Receive():
		assert.Equal(t, "from-A-to-B", msg.Data)
	case <-time.After(5 * time.Second):
		t.Fatal("subscriber on hub B did not receive message published on hub A")
	}
}

// TestReal_MultiHub_GetSubscribersAcrossHubs — invariant B.
// GetSubscribers on any hub returns subscribers from every hub. Exercises
// SCAN(:presence:*) + MGET + unmarshal with TopicSelectorStore injection.
func TestReal_MultiHub_GetSubscribersAcrossHubs(t *testing.T) {
	hubs := newRealMultiHub(t, 2)
	tss := testTopicSelectorStore()

	// SetTopicSelectorStore must precede AddSubscriber so any future
	// unmarshalPresenceValue call has the store ready to inject into remote
	// subscribers — matches how the hub wires transport.SetTopicSelectorStore
	// during Provision before any request handlers run.
	hubs[0].SetTopicSelectorStore(tss)
	hubs[1].SetTopicSelectorStore(tss)

	subA := mercure.NewLocalSubscriber("", testLogger(), tss)
	subA.SetTopics([]string{"https://example.com/mh-presence"}, nil)
	require.NoError(t, hubs[0].AddSubscriber(context.Background(), subA))

	subB := mercure.NewLocalSubscriber("", testLogger(), tss)
	subB.SetTopics([]string{"https://example.com/mh-presence"}, nil)
	require.NoError(t, hubs[1].AddSubscriber(context.Background(), subB))

	// Force each hub to publish its presence payload now instead of waiting
	// for the (24h-disabled) heartbeat ticker.
	hubs[0].publishPresence(context.Background())
	hubs[1].publishPresence(context.Background())

	_, subs, err := hubs[0].GetSubscribers(context.Background())
	require.NoError(t, err)
	assert.Len(t, subs, 2,
		"GetSubscribers on hub A must return both hubs' subscribers")
}

// TestReal_MultiHub_PresenceHeartbeatVisibility — invariant C.
// With a short presenceInterval + TTL, a subscriber added on hub A becomes
// visible to hub B's GetSubscribers within one heartbeat cycle.
func TestReal_MultiHub_PresenceHeartbeatVisibility(t *testing.T) {
	hubs := newRealMultiHub(
		t, 2,
		WithPresenceInterval(100*time.Millisecond),
		WithPresenceTTL(1*time.Second),
	)
	tss := testTopicSelectorStore()

	hubs[0].SetTopicSelectorStore(tss)
	hubs[1].SetTopicSelectorStore(tss)

	sub := mercure.NewLocalSubscriber("", testLogger(), tss)
	sub.SetTopics([]string{"https://example.com/mh-heartbeat"}, nil)
	require.NoError(t, hubs[0].AddSubscriber(context.Background(), sub))

	require.Eventually(t, func() bool {
		_, subs, err := hubs[1].GetSubscribers(context.Background())

		return err == nil && len(subs) == 1
	}, 3*time.Second, 100*time.Millisecond,
		"hub B did not see hub A's subscriber via presence heartbeat within 3s")
}

// TestReal_MultiHub_ZombieGCAcrossHubs — invariant D.
// With hubs A and B both live and a synthetic orphan "C" group attached to
// the shared stream, hub A's GC must:
//  1. destroy the orphan (its presence key does not exist), AND
//  2. leave A's and B's own groups untouched (their presence keys DO exist).
//
// Using only one live hub would not catch a false-positive regression where
// GC destroys groups whose presence key is unexpectedly missing at the
// instant of the lookup.
func TestReal_MultiHub_ZombieGCAcrossHubs(t *testing.T) {
	hubs := newRealMultiHub(t, 2, WithZombieGCInterval(24*time.Hour))
	hubA, hubB := hubs[0], hubs[1]

	streamKey := "{" + hubA.opts.streamName + "}"

	// Ensure the stream has at least one entry so the synthetic group attaches.
	require.NoError(t, hubA.Dispatch(context.Background(), &mercure.Update{
		Topics: []string{"https://example.com/mh-gc"},
		Event:  mercure.Event{Data: "bootstrap"},
	}))

	zombieGroup := fmt.Sprintf("mercure:node:zombie-mh-%d", time.Now().UnixNano())

	probe := newProbeClient(t, realRedisAddr())

	require.NoError(t, probe.XGroupCreate(context.Background(), streamKey, zombieGroup, "0").Err())

	hubA.gcZombieGroups(context.Background(), streamKey)

	groups, err := probe.XInfoGroups(context.Background(), streamKey).Result()
	require.NoError(t, err)

	names := make([]string, 0, len(groups))
	for _, g := range groups {
		names = append(names, g.Name)
	}

	assert.NotContains(t, names, zombieGroup,
		"hub A's GC should destroy the orphan group")
	assert.Contains(t, names, hubA.nodeGroup,
		"hub A's GC must NOT destroy hub A's own group (presence key is live)")
	assert.Contains(t, names, hubB.nodeGroup,
		"hub A's GC must NOT destroy hub B's group (presence key is live)")
}

// TestReal_MultiHub_CloseIsolation — invariant F.
// Closing hub A does not affect hub B's subscribers; hub B continues to
// dispatch its own traffic after hub A's shutdown.
func TestReal_MultiHub_CloseIsolation(t *testing.T) {
	hubs := newRealMultiHub(t, 2)
	tss := testTopicSelectorStore()

	subA := mercure.NewLocalSubscriber("", testLogger(), tss)
	subA.SetTopics([]string{"https://example.com/mh-close"}, nil)
	require.NoError(t, hubs[0].AddSubscriber(context.Background(), subA))

	subB := mercure.NewLocalSubscriber("", testLogger(), tss)
	subB.SetTopics([]string{"https://example.com/mh-close"}, nil)
	require.NoError(t, hubs[1].AddSubscriber(context.Background(), subB))

	require.NoError(t, hubs[0].Close(context.Background()))

	// Hub A's Close runs disconnectAllSubscribers which closes every local
	// subscriber's out-channel. subA.Receive() therefore returns a closed
	// channel: the zero-value read with ok=false is the documented signal.
	select {
	case _, ok := <-subA.Receive():
		assert.False(t, ok, "subA channel should be closed after hub A's Close")
	case <-time.After(2 * time.Second):
		t.Fatal("subA channel was not closed within 2s of hub A's Close")
	}

	// After hub A close, hub B's dispatch path must still function — publish
	// from hub B and assert hub B's own subscriber receives.
	require.NoError(t, hubs[1].Dispatch(context.Background(), &mercure.Update{
		Topics: []string{"https://example.com/mh-close"},
		Event:  mercure.Event{Data: "post-A-close"},
	}))

	select {
	case msg := <-subB.Receive():
		assert.Equal(t, "post-A-close", msg.Data)
	case <-time.After(5 * time.Second):
		t.Fatal("hub B subscriber did not receive after hub A closed — close isolation broken")
	}

	// Post-Close dispatch on hub A must error cleanly.
	err := hubs[0].Dispatch(context.Background(), &mercure.Update{
		Topics: []string{"https://example.com/mh-close"},
		Event:  mercure.Event{Data: "post-close"},
	})
	require.ErrorIs(t, err, ErrClosedTransport)
}

// TestReal_MultiHub_ConcurrentPublishStress — invariant G.
// Two hubs each publish perHub messages concurrently. A subscriber on hub B
// must receive every message from both publishers.
func TestReal_MultiHub_ConcurrentPublishStress(t *testing.T) {
	hubs := newRealMultiHub(t, 2)
	hubA, hubB := hubs[0], hubs[1]
	tss := testTopicSelectorStore()

	subB := mercure.NewLocalSubscriber("", testLogger(), tss)
	subB.SetTopics([]string{"https://example.com/mh-stress"}, nil)
	require.NoError(t, hubB.AddSubscriber(context.Background(), subB))

	const perHub = 200

	totalExpected := perHub * 2

	var wg sync.WaitGroup

	wg.Go(func() {
		for i := range perHub {
			_ = hubA.Dispatch(context.Background(), &mercure.Update{
				Topics: []string{"https://example.com/mh-stress"},
				Event:  mercure.Event{Data: fmt.Sprintf("A-%d", i)},
			})
		}
	})

	wg.Go(func() {
		for i := range perHub {
			_ = hubB.Dispatch(context.Background(), &mercure.Update{
				Topics: []string{"https://example.com/mh-stress"},
				Event:  mercure.Event{Data: fmt.Sprintf("B-%d", i)},
			})
		}
	})

	wg.Wait()

	var (
		received int
		fromA    int
		fromB    int
	)

	timeout := time.After(15 * time.Second)

drain:
	for received < totalExpected {
		select {
		case msg := <-subB.Receive():
			if len(msg.Data) > 0 && msg.Data[0] == 'A' {
				fromA++
			} else {
				fromB++
			}

			received++
		case <-timeout:
			break drain
		}
	}

	assert.Equal(t, totalExpected, received,
		"expected %d total messages, got %d (A=%d, B=%d)", totalExpected, received, fromA, fromB)
	assert.Equal(t, perHub, fromA, "subscriber on hub B should receive all %d messages from hub A", perHub)
	assert.Equal(t, perHub, fromB, "subscriber on hub B should receive all %d messages from hub B", perHub)
}

// TestReal_MultiHub_ConcurrentStartup — invariant H.
// Multiple hubs starting in parallel against the same stream must all succeed
// (distinct nodeIDs → distinct consumer groups, no BUSYGROUP collision), and
// the stream must end with exactly one consumer group per hub.
func TestReal_MultiHub_ConcurrentStartup(t *testing.T) {
	addr := probeRealRedis(t)

	const hubCount = 5

	streamName := fmt.Sprintf("mercure-startup-%d", time.Now().UnixNano())

	defaultOpts := []Option{
		WithLogger(testLogger()),
		WithStreamName(streamName),
		WithXReadBlock(50 * time.Millisecond),
		WithHealthInterval(24 * time.Hour),
		WithPresenceInterval(24 * time.Hour),
		withSkipPresenceIntervalCheck(),
	}

	var (
		hubs [hubCount]*RedisTransport
		errs [hubCount]error
		wg   sync.WaitGroup
	)

	for i := range hubCount {
		wg.Go(func() {
			client := redis.NewClient(&redis.Options{Addr: addr})

			tr, err := NewRedisTransport(client, defaultOpts...)
			hubs[i] = tr
			errs[i] = err
		})
	}

	wg.Wait()

	t.Cleanup(func() {
		for _, h := range hubs {
			if h != nil {
				_ = h.Close(context.Background())
			}
		}

		cleanupStreamKeys(addr, streamName)
	})

	for i, err := range errs {
		require.NoError(t, err, "hub %d startup failed under concurrent race", i)
	}

	probe := newProbeClient(t, addr)

	groups, err := probe.XInfoGroups(context.Background(), "{"+streamName+"}").Result()
	require.NoError(t, err)
	assert.Len(t, groups, hubCount,
		"each hub should create exactly one consumer group; got %d for %d hubs", len(groups), hubCount)
}

// TestReal_MultiHub_RacingXGroupDestroy — invariant J.
// Two live hubs' GC passes fire at the same instant on the same zombie group
// and converge safely: one destroys, the other sees it gone and returns
// cleanly (Redis XGROUP DESTROY returns 0 for an already-removed group,
// which go-redis surfaces as a nil error, NOT an error).
//
// Uses a channel barrier to force both goroutines to enter gcZombieGroups
// at the same instant. Without the barrier, goroutine scheduling on a fast
// server can finish hub A's 5ms GC before hub B's network call lands,
// reducing the "race" to a serial call.
func TestReal_MultiHub_RacingXGroupDestroy(t *testing.T) {
	hubs := newRealMultiHub(t, 2, WithZombieGCInterval(24*time.Hour))
	hubA, hubB := hubs[0], hubs[1]

	streamKey := "{" + hubA.opts.streamName + "}"

	require.NoError(t, hubA.Dispatch(context.Background(), &mercure.Update{
		Topics: []string{"https://example.com/mh-race"},
		Event:  mercure.Event{Data: "bootstrap"},
	}))

	zombieGroup := fmt.Sprintf("mercure:node:zombie-race-%d", time.Now().UnixNano())

	probe := newProbeClient(t, realRedisAddr())
	require.NoError(t, probe.XGroupCreate(context.Background(), streamKey, zombieGroup, "0").Err())

	start := make(chan struct{})

	var wg sync.WaitGroup

	wg.Go(func() {
		<-start
		hubA.gcZombieGroups(context.Background(), streamKey)
	})
	wg.Go(func() {
		<-start
		hubB.gcZombieGroups(context.Background(), streamKey)
	})

	close(start) // unleash both goroutines simultaneously
	wg.Wait()

	groups, err := probe.XInfoGroups(context.Background(), streamKey).Result()
	require.NoError(t, err)

	for _, g := range groups {
		assert.NotEqual(t, zombieGroup, g.Name,
			"racing GCs must converge — zombie group must end up destroyed exactly once")
	}
}

// TestReal_MultiHub_HistoryPaginationBoundary — invariant K (table-driven).
// Hub A publishes `total` events; a subscriber on hub B with Last-Event-ID
// at `pivot` must replay events pivot+1 .. total-1 in order. The subtests
// exercise:
//
//   - crosses-page-boundary  — pivot inside the first page, replay spans pages
//   - first-event-full-replay — pivot=0, entire history replayed
//   - zero-replay            — pivot=last event, no history to replay
//   - exact-page-boundary    — pivot sits at historyPageSize-1
//
// Off-by-one errors in pagination arithmetic routinely only manifest at the
// boundaries; the table form makes those cases first-class.
//
// The drain goroutine starts BEFORE AddSubscriber because dispatchHistory
// runs inside AddSubscriber and would otherwise fill the LocalSubscriber
// buffer before a reader exists, tripping the buffer-overflow auto-disconnect.
func TestReal_MultiHub_HistoryPaginationBoundary(t *testing.T) {
	// historyPageSize is 1000 (redis.go). Keep totals modest to bound test time.
	cases := []struct {
		name  string
		total int
		pivot int
	}{
		{name: "crosses-page-boundary", total: 1500, pivot: 200},
		{name: "first-event-full-replay", total: 300, pivot: 0},
		{name: "zero-replay-at-tail", total: 300, pivot: 299},
		{name: "exact-page-boundary", total: 1200, pivot: 999},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runHistoryPaginationCase(t, tc.total, tc.pivot)
		})
	}
}

// runHistoryPaginationCase drives a single pivot/total combination. Extracted
// so each subtest owns its own streamName and cleanup via newRealMultiHub.
func runHistoryPaginationCase(t *testing.T, total, pivot int) {
	t.Helper()

	hubs := newRealMultiHub(t, 2)
	hubA, hubB := hubs[0], hubs[1]

	publishedIDs := make([]string, 0, total)

	for i := range total {
		u := &mercure.Update{
			Topics: []string{"https://example.com/mh-pagination"},
			Event:  mercure.Event{Data: fmt.Sprintf("msg-%04d", i)},
		}
		require.NoError(t, hubA.Dispatch(context.Background(), u))

		publishedIDs = append(publishedIDs, u.ID)
	}

	// Block until hub B's XREADGROUP has caught up to the stream tail so
	// toStreamID captured inside AddSubscriber is the true tail ID, not a
	// mid-catchup snapshot. Without this, Pass 1 scans a truncated range and
	// Pass 2 fires, delivering events the test didn't intend to replay.
	probe := newProbeClient(t, realRedisAddr())
	waitForStreamCatchUp(t, hubB, probe)

	tss := testTopicSelectorStore()
	sub := mercure.NewLocalSubscriber(publishedIDs[pivot], testLogger(), tss)
	sub.SetTopics([]string{"https://example.com/mh-pagination"}, nil)

	expected := total - pivot - 1 // events pivot+1 .. total-1

	var (
		received []string
		drainErr error
	)

	drainDone := make(chan struct{})

	go func() {
		defer close(drainDone)

		// Zero-replay case: drain for a short window and assert nothing
		// arrives, rather than loop forever waiting for 0 elements.
		if expected == 0 {
			select {
			case msg, ok := <-sub.Receive():
				if ok {
					received = append(received, msg.Data)
				}
			case <-time.After(1 * time.Second):
			}

			return
		}

		timeout := time.After(30 * time.Second)

		for len(received) < expected {
			select {
			case msg, ok := <-sub.Receive():
				if !ok {
					drainErr = fmt.Errorf("%w after %d/%d events (buffer overflow or early Close)",
						errMultiHubDrainChannelClosed, len(received), expected)

					return
				}

				received = append(received, msg.Data)
			case <-timeout:
				drainErr = fmt.Errorf("%w after %d/%d events",
					errMultiHubDrainTimeout, len(received), expected)

				return
			}
		}
	}()

	require.NoError(t, hubB.AddSubscriber(context.Background(), sub))

	<-drainDone
	require.NoError(t, drainErr)
	require.Len(t, received, expected,
		"pivot=%d total=%d: expected %d replay events, got %d",
		pivot, total, expected, len(received))

	for i, data := range received {
		want := fmt.Sprintf("msg-%04d", pivot+1+i)
		assert.Equal(t, want, data, "out-of-order at history index %d (pivot=%d)", i, pivot)
	}
}

// TestReal_MultiHub_DispatchBidirectional — sanity check that A↔B delivery
// works both directions under concurrent use. Complements the one-way
// CrossNodeDispatch test. Uses atomic counters instead of channels so a
// delivery regression produces an off-by-one assertion rather than a hang.
func TestReal_MultiHub_DispatchBidirectional(t *testing.T) {
	hubs := newRealMultiHub(t, 2)
	hubA, hubB := hubs[0], hubs[1]
	tss := testTopicSelectorStore()

	var seenOnA, seenOnB atomic.Int64

	subA := mercure.NewLocalSubscriber("", testLogger(), tss)
	subA.SetTopics([]string{"https://example.com/mh-bidir"}, nil)
	require.NoError(t, hubA.AddSubscriber(context.Background(), subA))

	subB := mercure.NewLocalSubscriber("", testLogger(), tss)
	subB.SetTopics([]string{"https://example.com/mh-bidir"}, nil)
	require.NoError(t, hubB.AddSubscriber(context.Background(), subB))

	done := make(chan struct{})

	go func() {
		defer close(done)

		for {
			select {
			case <-subA.Receive():
				seenOnA.Add(1)
			case <-subB.Receive():
				seenOnB.Add(1)
			case <-time.After(5 * time.Second):
				return
			}
		}
	}()

	const perHub = 20

	for i := range perHub {
		require.NoError(t, hubA.Dispatch(context.Background(), &mercure.Update{
			Topics: []string{"https://example.com/mh-bidir"},
			Event:  mercure.Event{Data: fmt.Sprintf("A-%d", i)},
		}))
		require.NoError(t, hubB.Dispatch(context.Background(), &mercure.Update{
			Topics: []string{"https://example.com/mh-bidir"},
			Event:  mercure.Event{Data: fmt.Sprintf("B-%d", i)},
		}))
	}

	require.Eventually(t, func() bool {
		return seenOnA.Load() == perHub*2 && seenOnB.Load() == perHub*2
	}, 10*time.Second, 100*time.Millisecond,
		"both subscribers must receive every message from both hubs (seenOnA=%d, seenOnB=%d, want %d each)",
		seenOnA.Load(), seenOnB.Load(), perHub*2)

	<-done
}
