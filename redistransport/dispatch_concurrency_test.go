package redistransport

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/dunglas/mercure"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setAfterMatchHookForTests installs the post-match dispatch hook. It lives
// in a _test.go so the production binary never carries the seam (the field is
// nil there). The hook runs in each dispatch path right after the topic-index
// candidate match (shard.match) and before delivery.
func (t *RedisTransport) setAfterMatchHookForTests(h func(shardID int, u *mercure.Update)) {
	t.afterMatchHookForTests.Store(&h)
}

// TestDispatchAdvanceBeforeFanoutNoGap is the deterministic gap guard for the
// lock-free dispatch. It pins the exact race the change targets: the dispatch cursor is
// advanced to event E, a shard worker takes its (empty) index candidate snapshot,
// and ONLY THEN does a subscriber join. Because the cursor already covers E, the
// joining subscriber must replay E via history — never miss it.
//
// This FAILS on the old advance-AFTER-fan-out ordering: the cursor would still
// read E-1 while the worker is blocked mid-fan-out, so the joiner snapshots
// toStreamID=E-1, replays <= E-1, and never sees E → gap (the Receive times out).
// It passes only because processStreamEntry advances the cursor before fan-out.
func TestDispatchAdvanceBeforeFanoutNoGap(t *testing.T) {
	t.Parallel()

	transport, mr := newTestTransport(t)
	tss := testTopicSelectorStore()
	ctx := context.Background()

	const (
		topic  = "https://example.com/dispatch-nogap"
		target = "event-E"
	)

	atBarrier := make(chan struct{})
	release := make(chan struct{})

	var barrierOnce, releaseOnce sync.Once

	// Always release the blocked listener, even if the test fails mid-way — else
	// the hook stays blocked and Close (t.Cleanup) hangs on <-listenerDone.
	releaseHook := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseHook()

	transport.setAfterMatchHookForTests(func(_ int, u *mercure.Update) {
		if u.Data != target {
			return
		}

		barrierOnce.Do(func() { close(atBarrier) })
		<-release
	})

	// Publish E. The listener advances the cursor to E, runs the index match (no
	// subscribers yet → empty), then blocks in the hook before delivery.
	require.NoError(t, transport.Dispatch(ctx, &mercure.Update{
		Topics: []string{topic},
		Event:  mercure.Event{Data: target},
	}))

	// Nudge miniredis's blocking XREADGROUP until the listener reaches the barrier.
	deadline := time.After(5 * time.Second)

	for reached := false; !reached; {
		select {
		case <-atBarrier:
			reached = true
		case <-deadline:
			t.Fatal("listener never reached the post-match barrier for E")
		default:
			mr.FastForward(60 * time.Millisecond)
			time.Sleep(10 * time.Millisecond)
		}
	}

	// The listener is blocked AFTER advancing the cursor to E and AFTER the empty
	// snapshot. Join now: the subscriber loads cursor=E and must replay E via
	// history (dispatchHistory reads the stream directly, independent of the
	// blocked listener).
	sub := mercure.NewLocalSubscriber(mercure.EarliestLastEventID, testLogger(), tss)
	sub.SetTopics([]string{topic}, nil)

	// Run AddSubscriber off the test goroutine with a timeout: if dispatch ever
	// regained a t.mu held across fan-out, AddSubscriber would deadlock on the
	// blocked listener — fail fast here instead of hanging until the go-test
	// timeout (the deferred releaseHook then unblocks the listener for Close).
	added := make(chan error, 1)
	go func() { added <- transport.AddSubscriber(ctx, sub) }()

	select {
	case err := <-added:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("AddSubscriber blocked — dispatch may be holding t.mu across fan-out")
	}

	select {
	case received := <-sub.Receive():
		assert.Equal(t, target, received.Data,
			"subscriber that joined after the snapshot must replay E (gap-free)")
	case <-time.After(5 * time.Second):
		t.Fatal("GAP: subscriber joined after the cursor advanced to E but never received E")
	}

	// Release the worker. The live fan-out delivers to the empty snapshot — which
	// did not contain this subscriber — so there is no duplicate live delivery.
	releaseHook()

	select {
	case dup := <-sub.Receive():
		t.Fatalf("unexpected second delivery (%q): the joiner was not in E's snapshot, so live must not re-deliver it", dup.Data)
	case <-time.After(300 * time.Millisecond):
		// Exactly-once here (replay only) — no boundary duplicate in this ordering.
	}
}

// TestCodecConcurrentSetDuringDispatchIsRaceFree reproduces the data race on the
// t.codec FIELD: both the XREADGROUP listener (decodeStreamEntry) and the publish
// path (Dispatch → Marshal) read it while SetCodec writes it. The listener starts
// in NewRedisTransport, before the hub calls SetCodec, so they run concurrently.
// Before the lock-free dispatch change the listener read t.codec under t.mu
// (synchronized with SetCodec's locked write); removing that lock reintroduced
// the race. Under -race this FAILS until t.codec is published atomically.
func TestCodecConcurrentSetDuringDispatchIsRaceFree(t *testing.T) {
	t.Parallel()

	transport, mr := newTestTransport(t)
	ctx := context.Background()

	const topic = "https://example.com/codec-race"

	// Two distinct codec instances to swap between. Both JSON: the race under
	// test is on the t.codec FIELD (SetCodec Store vs listener Load), which is
	// codec-agnostic. Using gob here would instead trip encoding/gob's own
	// global type-registry race (concurrent Encoder+Decoder first-use), which is
	// unrelated to this field and would mask the signal.
	codecA, err := newCodec("json")
	require.NoError(t, err)

	codecB, err := newCodec("json")
	require.NoError(t, err)

	// Pump miniredis so the listener keeps decoding entries (reading t.codec).
	stop := make(chan struct{})

	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				mr.FastForward(20 * time.Millisecond)
				time.Sleep(time.Millisecond)
			}
		}
	}()

	defer close(stop)

	var wg sync.WaitGroup

	wg.Add(2)

	go func() {
		defer wg.Done()

		for i := range 300 {
			_ = transport.Dispatch(ctx, &mercure.Update{
				Topics: []string{topic},
				Event:  mercure.Event{Data: strconv.Itoa(i)},
			})
		}
	}()

	go func() {
		defer wg.Done()

		for i := range 300 {
			if i%2 == 0 {
				transport.SetCodec(codecA)
			} else {
				transport.SetCodec(codecB)
			}

			time.Sleep(time.Millisecond)
		}
	}()

	wg.Wait()
}

// TestBackpressureLossCountedWhenRemovedConcurrently reproduces the metric
// under-count: a live dispatch attributes a loss (markLost backpressure) for a
// subscriber whose lostFlags entry a concurrent RemoveSubscriber already deleted.
// The hook forces that exact ordering deterministically. Before the lock-free dispatch change, dispatch
// held t.mu so the flag was always present at markLost; after the lock removal,
// markLost must still count the loss. FAILS while markLost no-ops on the no-flag
// branch (the loss is silently dropped). Unchanged by the topic-index dispatch
// matcher: the stale candidate now comes from the index snapshot instead of
// MatchAny, but the markLost no-flag branch is the same guard.
func TestBackpressureLossCountedWhenRemovedConcurrently(t *testing.T) {
	t.Parallel()

	transport, mr := newTestTransport(t, WithPrometheusRegisterer(prometheus.NewRegistry()))
	tss := testTopicSelectorStore()
	ctx := context.Background()

	const (
		topic         = "https://example.com/bp"
		sentinelTopic = "https://example.com/bp-sentinel"
		target        = "loss-event"
		sentinel      = "sentinel-event"
	)

	// The lost subscriber: in the hook it is removed (flag deleted) and
	// disconnected, so the worker's upcoming Dispatch returns false → markLost.
	lost := mercure.NewLocalSubscriber("", testLogger(), tss)
	lost.SetTopics([]string{topic}, nil)
	require.NoError(t, transport.AddSubscriber(ctx, lost))

	// A second subscriber on a different topic gives an in-order completion
	// signal: the single listener processes entries sequentially, so when this
	// one receives the sentinel, the target's markLost has already run.
	witness := mercure.NewLocalSubscriber("", testLogger(), tss)
	witness.SetTopics([]string{sentinelTopic}, nil)
	require.NoError(t, transport.AddSubscriber(ctx, witness))

	var once sync.Once

	transport.setAfterMatchHookForTests(func(_ int, u *mercure.Update) {
		if u.Data != target {
			return
		}

		once.Do(func() {
			_ = transport.RemoveSubscriber(ctx, lost) // deletes the lostFlags entry
			lost.Disconnect()                         // upcoming Dispatch → false → markLost(backpressure)
		})
	})

	m := transport.metrics.Load()
	require.NotNil(t, m)

	before := counterValueForTest(t, m.lostCounter(lossReasonBackpressure))

	require.NoError(t, transport.Dispatch(ctx, &mercure.Update{
		Topics: []string{topic},
		Event:  mercure.Event{Data: target},
	}))
	require.NoError(t, transport.Dispatch(ctx, &mercure.Update{
		Topics: []string{sentinelTopic},
		Event:  mercure.Event{Data: sentinel},
	}))

	// Pump until the witness receives the sentinel → the target (processed first,
	// in order) has completed its fan-out including markLost.
	stop := make(chan struct{})

	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				mr.FastForward(20 * time.Millisecond)
				time.Sleep(time.Millisecond)
			}
		}
	}()

	defer close(stop)

	select {
	case u := <-witness.Receive():
		require.Equal(t, sentinel, u.Data)
	case <-time.After(5 * time.Second):
		t.Fatal("witness never received the sentinel — listener did not progress")
	}

	after := counterValueForTest(t, m.lostCounter(lossReasonBackpressure))
	assert.InDelta(t, before+1, after, 0.001,
		"backpressure loss must be counted even though RemoveSubscriber deleted the flag first")
}
