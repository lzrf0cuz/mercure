//go:build real_redis

package redistransport

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dunglas/mercure"
	"github.com/stretchr/testify/require"
)

// TestReal_DispatchNoGapUnderReconnectStorm is the realistic gap guard for the
// lock-free dispatch (companion to the deterministic
// TestDispatchAdvanceBeforeFanoutNoGap). With the dispatch cursor advanced before
// fan-out and t.mu no longer held across dispatch, many subscribers join and
// leave concurrently while a monotonic sequence is published over a sharded
// transport. Each subscriber's received sequence numbers must form a CONTIGUOUS
// range (no gap, the invariant); a duplicate at the join boundary is allowed
// (at-least-once). A real server gives natural concurrency without miniredis's
// FastForward timing.
func TestReal_DispatchNoGapUnderReconnectStorm(t *testing.T) {
	transport, _ := newRealTransport(t, WithDispatchShards(4))
	tss := testTopicSelectorStore()
	ctx := context.Background()

	const (
		topic       = "https://example.com/storm"
		events      = 300
		subscribers = 24
	)

	// Publisher: emit a monotonic sequence (Data = "1".."events") spread over a
	// ~300ms window so subscribers join mid-stream. Capture publish errors so a
	// dropped publish surfaces as itself, not as phantom subscriber tail loss.
	var (
		pubWG     sync.WaitGroup
		pubErrors atomic.Int64
	)

	pubWG.Go(func() {
		for i := 1; i <= events; i++ {
			if err := transport.Dispatch(ctx, &mercure.Update{
				Topics: []string{topic},
				Event:  mercure.Event{Data: strconv.Itoa(i)},
			}); err != nil {
				pubErrors.Add(1)
			}

			time.Sleep(time.Millisecond)
		}
	})

	results := make([][]int, subscribers)

	var (
		subWG     sync.WaitGroup
		addErrors atomic.Int64
	)

	for s := range subscribers {
		subWG.Go(func() {
			results[s] = stormDrain(ctx, transport, tss, topic, s, events, &addErrors)
		})
	}

	pubWG.Wait()
	subWG.Wait()

	require.Zero(t, pubErrors.Load(), "Dispatch must not fail during the storm")
	require.Zero(t, addErrors.Load(), "AddSubscriber must not fail during the storm")

	reachedEnd := 0

	for idx, got := range results {
		if assertContiguousFromOne(t, idx, got, events) {
			reachedEnd++
		}
	}

	require.Equalf(t, subscribers, reachedEnd,
		"every subscriber must receive the full 1..%d sequence (no tail loss); only %d/%d did",
		events, reachedEnd, subscribers)
}

// stormDrain joins (staggered, so each snapshots a different cursor point), then
// reads until the full sequence has been seen or a hard cap. Reading to
// completion rather than to an idle timer is stronger and less flaky: a tail-loss
// bug (a subscriber stuck below events) trips the hard cap and fails the
// caller's reachedEnd assertion instead of idling out as a false "done".
func stormDrain(
	ctx context.Context,
	transport *RedisTransport,
	tss *mercure.TopicSelectorStore,
	topic string,
	idx, events int,
	addErrors *atomic.Int64,
) []int {
	time.Sleep(time.Duration(idx) * 5 * time.Millisecond)

	sub := mercure.NewLocalSubscriber(mercure.EarliestLastEventID, testLogger(), tss)
	sub.SetTopics([]string{topic}, nil)

	if err := transport.AddSubscriber(ctx, sub); err != nil {
		addErrors.Add(1)

		return nil
	}
	defer func() { _ = transport.RemoveSubscriber(ctx, sub) }()

	var got []int

	seen := make(map[int]bool, events)
	hard := time.After(30 * time.Second)

	for len(seen) < events {
		select {
		case u := <-sub.Receive():
			if n, err := strconv.Atoi(u.Data); err == nil {
				got = append(got, n)
				seen[n] = true
			}
		case <-hard:
			return got
		}
	}

	return got
}

// assertContiguousFromOne checks the gap-free invariant for one subscriber's
// received sequence: non-empty, replayed from 1 (EarliestLastEventID — so a
// missing prefix shows up as min > 1, which a contiguous-range check alone would
// miss), and no interior gap. Returns whether it reached the full sequence; the
// caller requires every subscriber to (catching tail loss).
func assertContiguousFromOne(t *testing.T, idx int, got []int, events int) bool {
	t.Helper()

	require.NotEmptyf(t, got, "subscriber %d received nothing — EarliestLastEventID must at least replay the history", idx)

	seen := make(map[int]bool, len(got))
	minSeq, maxSeq := got[0], got[0]

	for i, n := range got {
		seen[n] = true

		if n < minSeq {
			minSeq = n
		}

		if n > maxSeq {
			maxSeq = n
		}

		// Per-subscriber order is FIFO: replay (ascending) then live (ascending),
		// so the received stream is non-decreasing (a boundary duplicate is equal,
		// never smaller). A decrease would be an ordering regression.
		if i > 0 {
			require.GreaterOrEqualf(t, n, got[i-1],
				"subscriber %d out-of-order: got[%d]=%d < got[%d]=%d", idx, i, n, i-1, got[i-1])
		}
	}

	require.Equalf(t, 1, minSeq,
		"subscriber %d prefix loss: EarliestLastEventID must replay from 1, got min %d", idx, minSeq)

	for n := minSeq; n <= maxSeq; n++ {
		require.Truef(t, seen[n],
			"subscriber %d GAP: received range [%d..%d] but is missing %d", idx, minSeq, maxSeq, n)
	}

	return maxSeq == events
}
