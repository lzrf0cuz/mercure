package redistransport

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/dunglas/mercure"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHistoryReplayXRANGEErrorPropagates verifies that a mid-replay XRANGE
// failure surfaces through dispatchHistory → AddSubscriber, so the subscriber
// is removed (and counted against subscribers_lost{reason="history_replay_failed"})
// and the client can reconnect with Last-Event-ID to retry the gap.
// Previously the error was logged-and-swallowed; run() called Ready() as if
// replay succeeded and the subscriber silently received a truncated history.
//
// A 500ms deadline bounds the test — without it, go-redis' default MaxRetries
// + backoff stretches the failure detection to several seconds.
func TestHistoryReplayXRANGEErrorPropagates(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	transport, mr := newTestTransport(t, WithPrometheusRegisterer(reg))
	tss := testTopicSelectorStore()

	for range 5 {
		update := &mercure.Update{
			Topics: []string{"https://example.com/test"},
			Event:  mercure.Event{Data: "payload"},
		}
		require.NoError(t, transport.Dispatch(context.Background(), update))
	}

	// Simulate Redis outage between publish and a reconnecting subscriber.
	mr.Close()

	sub := mercure.NewLocalSubscriber(mercure.EarliestLastEventID, testLogger(), tss)
	sub.SetTopics([]string{"https://example.com/test"}, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	err := transport.AddSubscriber(ctx, sub)
	require.Error(t, err, "XRANGE failure during history replay must propagate")
	assert.Contains(t, err.Error(), "history replay")

	assert.EqualValues(t, 0, transport.subscriberCount.Load(),
		"history-replay rollback must decrement subscriberCount back to 0 (no leak)")

	// Counter symmetry: the add was already counted; the loss must also be counted.
	families, gatherErr := reg.Gather()
	require.NoError(t, gatherErr)

	var failedCount float64

	for _, f := range families {
		if f.GetName() != metricSubscribersLostTotal {
			continue
		}

		for _, m := range f.GetMetric() {
			for _, label := range m.GetLabel() {
				if label.GetName() == labelKeyReason && label.GetValue() == "history_replay_failed" {
					failedCount = m.GetCounter().GetValue()
				}
			}
		}
	}

	assert.InDelta(t, float64(1), failedCount, 0,
		"failed history replay must increment subscribers_lost{reason=history_replay_failed}")
}

// TestCloseJoinsShutdownErrors verifies that Close aggregates failures from
// XGroupDestroy and Del via errors.Join. Previously only client.Close() errors
// surfaced; orchestrators that key on Close()'s return value missed a Redis
// outage class that fails XGroupDestroy/Del but not the client pool close.
func TestCloseJoinsShutdownErrors(t *testing.T) {
	t.Parallel()

	transport, mr := newTestTransport(t)

	mr.Close()

	closeErr := transport.Close(context.Background())
	require.Error(t, closeErr, "Close must surface XGroupDestroy/Del failures")
	assert.Contains(t, closeErr.Error(), "destroy consumer group")
	assert.Contains(t, closeErr.Error(), "delete presence key")
}

// TestRedisTransportDoNotDispatchUntilListen verifies that updates flow
// through the Redis Stream via XREADGROUP — there is no local fast-path.
// A subscriber added before a dispatch receives the update only after the
// XREADGROUP goroutine processes it from the stream.
func TestRedisTransportDoNotDispatchUntilListen(t *testing.T) {
	t.Parallel()

	transport, mr := newTestTransport(t)
	tss := testTopicSelectorStore()

	// Add subscriber first, then publish.
	sub := mercure.NewLocalSubscriber("", testLogger(), tss)
	sub.SetTopics([]string{"https://example.com/test"}, nil)
	require.NoError(t, transport.AddSubscriber(context.Background(), sub))

	update := &mercure.Update{
		Topics: []string{"https://example.com/test"},
		Event:  mercure.Event{Data: "through-stream"},
	}
	require.NoError(t, transport.Dispatch(context.Background(), update))
	mr.FastForward(100 * time.Millisecond)

	// The message was published to Redis and picked up by XREADGROUP.
	// Verify it arrives at the subscriber with correct content.
	select {
	case received := <-sub.Receive():
		assert.Equal(t, "through-stream", received.Data)
		// Verify the update went through the codec round-trip (ID was assigned).
		assert.NotEmpty(t, received.ID, "update should have ID from AssignUUID")
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for update through XREADGROUP")
	}

	// Verify topic filtering works — non-matching topic should not be received.
	nonMatchingUpdate := &mercure.Update{
		Topics: []string{"https://example.com/other-topic"},
		Event:  mercure.Event{Data: "should-not-arrive"},
	}
	require.NoError(t, transport.Dispatch(context.Background(), nonMatchingUpdate))
	mr.FastForward(100 * time.Millisecond)

	select {
	case <-sub.Receive():
		t.Fatal("subscriber should not receive non-matching topic")
	case <-time.After(200 * time.Millisecond):
	}
}

// TestRedisTransportHistory verifies that a subscriber with a Last-Event-ID
// receives replayed history from the stream.
func TestRedisTransportHistory(t *testing.T) {
	t.Parallel()

	transport, mr := newTestTransport(t)
	tss := testTopicSelectorStore()

	// Publish several updates.
	ids := make([]string, 0, 5)

	for i := range 5 {
		u := &mercure.Update{
			Topics: []string{"https://example.com/history"},
			Event:  mercure.Event{Data: fmt.Sprintf("msg-%d", i)},
		}
		require.NoError(t, transport.Dispatch(context.Background(), u))
		ids = append(ids, u.ID)

		mr.FastForward(10 * time.Millisecond)
	}

	// Wait for XREADGROUP to process all messages so lastDispatchedStreamID advances.
	time.Sleep(200 * time.Millisecond)
	mr.FastForward(200 * time.Millisecond)
	time.Sleep(200 * time.Millisecond)

	// Subscribe with Last-Event-ID = the 2nd message's ID.
	// Expect to receive messages 3, 4, 5 (after the matched ID).
	sub := mercure.NewLocalSubscriber(ids[1], testLogger(), tss)
	sub.SetTopics([]string{"https://example.com/history"}, nil)
	require.NoError(t, transport.AddSubscriber(context.Background(), sub))

	for i := 2; i < 5; i++ {
		select {
		case received := <-sub.Receive():
			assert.Equal(t, fmt.Sprintf("msg-%d", i), received.Data)
		case <-time.After(5 * time.Second):
			t.Fatalf("timeout waiting for history message %d", i)
		}
	}
}

// TestRedisTransportHistoryAndLive verifies the history → live transition:
// after history replay completes, live updates are flushed from the liveQueue.
func TestRedisTransportHistoryAndLive(t *testing.T) {
	t.Parallel()

	transport, mr := newTestTransport(t)
	tss := testTopicSelectorStore()

	// Publish a history message.
	historyUpdate := &mercure.Update{
		Topics: []string{"https://example.com/transition"},
		Event:  mercure.Event{Data: "history-msg"},
	}
	require.NoError(t, transport.Dispatch(context.Background(), historyUpdate))
	mr.FastForward(100 * time.Millisecond)
	time.Sleep(200 * time.Millisecond)

	// Subscribe with earliest — gets all history.
	sub := mercure.NewLocalSubscriber(mercure.EarliestLastEventID, testLogger(), tss)
	sub.SetTopics([]string{"https://example.com/transition"}, nil)
	require.NoError(t, transport.AddSubscriber(context.Background(), sub))

	// History message should arrive first.
	select {
	case received := <-sub.Receive():
		assert.Equal(t, "history-msg", received.Data)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for history message")
	}

	// Now publish a live update.
	liveUpdate := &mercure.Update{
		Topics: []string{"https://example.com/transition"},
		Event:  mercure.Event{Data: "live-msg"},
	}
	require.NoError(t, transport.Dispatch(context.Background(), liveUpdate))
	mr.FastForward(100 * time.Millisecond)

	select {
	case received := <-sub.Receive():
		assert.Equal(t, "live-msg", received.Data)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for live message")
	}
}

// TestRedisTransportDeduplication verifies that messages are not delivered
// twice even when PEL drain reprocesses them.
func TestRedisTransportDeduplication(t *testing.T) {
	t.Parallel()

	transport, mr := newTestTransport(t)
	tss := testTopicSelectorStore()

	sub := mercure.NewLocalSubscriber("", testLogger(), tss)
	sub.SetTopics([]string{"https://example.com/dedup"}, nil)
	require.NoError(t, transport.AddSubscriber(context.Background(), sub))

	// Publish 3 messages.
	for i := range 3 {
		u := &mercure.Update{
			Topics: []string{"https://example.com/dedup"},
			Event:  mercure.Event{Data: fmt.Sprintf("dedup-%d", i)},
		}
		require.NoError(t, transport.Dispatch(context.Background(), u))
	}

	mr.FastForward(200 * time.Millisecond)

	// Collect all received messages.
	var received []string

	timeout := time.After(3 * time.Second)

	for {
		select {
		case msg := <-sub.Receive():
			received = append(received, msg.Data)
			if len(received) == 3 {
				goto done
			}
		case <-timeout:
			goto done
		}
	}

done:
	// Should have exactly 3 messages, no duplicates.
	require.Len(t, received, 3)

	assert.Equal(t, "dedup-0", received[0])
	assert.Equal(t, "dedup-1", received[1])
	assert.Equal(t, "dedup-2", received[2])
}

// TestRedisTransportMultiNode verifies cross-node delivery: a message published
// on one node is received by subscribers on other nodes.
func TestRedisTransportMultiNode(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)

	opts := []Option{
		withSkipVersionCheck(),
		WithLogger(testLogger()),
		WithXReadBlock(50 * time.Millisecond),
		WithHealthInterval(24 * time.Hour),
		WithPresenceInterval(24 * time.Hour),
		withSkipPresenceIntervalCheck(),
		WithZombieGCInterval(0),
	}

	// Create 3 nodes sharing the same miniredis.
	var transports [3]*RedisTransport

	var subs [3]*mercure.LocalSubscriber

	tss := testTopicSelectorStore()

	for i := range 3 {
		client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
		tr, err := NewRedisTransport(client, opts...)
		require.NoError(t, err)
		t.Cleanup(func() { tr.Close(context.Background()) })

		transports[i] = tr

		sub := mercure.NewLocalSubscriber("", testLogger(), tss)
		sub.SetTopics([]string{"https://example.com/multi/{id}"}, nil)
		require.NoError(t, tr.AddSubscriber(context.Background(), sub))
		subs[i] = sub
	}

	// Publish from node 0.
	update := &mercure.Update{
		Topics: []string{"https://example.com/multi/1"},
		Event:  mercure.Event{Data: "cross-node-msg"},
	}
	require.NoError(t, transports[0].Dispatch(context.Background(), update))
	mr.FastForward(200 * time.Millisecond)

	// All 3 nodes should receive the message.
	for i := range 3 {
		select {
		case received := <-subs[i].Receive():
			assert.Equal(t, "cross-node-msg", received.Data, "node %d should receive", i)
		case <-time.After(5 * time.Second):
			t.Fatalf("timeout waiting for message on node %d", i)
		}
	}
}

// TestRedisTransportMultiNodeHistory verifies that a subscriber on Node B
// can replay history published by Node A.
func TestRedisTransportMultiNodeHistory(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)

	opts := []Option{
		withSkipVersionCheck(),
		WithLogger(testLogger()),
		WithXReadBlock(50 * time.Millisecond),
		WithHealthInterval(24 * time.Hour),
		WithPresenceInterval(24 * time.Hour),
		withSkipPresenceIntervalCheck(),
		WithZombieGCInterval(0),
	}

	// Node A publishes messages.
	clientA := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	nodeA, err := NewRedisTransport(clientA, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { nodeA.Close(context.Background()) })

	publishedIDs := make([]string, 0, 3)

	for i := range 3 {
		u := &mercure.Update{
			Topics: []string{"https://example.com/cross-history"},
			Event:  mercure.Event{Data: fmt.Sprintf("history-%d", i)},
		}
		require.NoError(t, nodeA.Dispatch(context.Background(), u))
		publishedIDs = append(publishedIDs, u.ID)

		mr.FastForward(10 * time.Millisecond)
	}

	// Wait for Node A's XREADGROUP to process all messages.
	time.Sleep(300 * time.Millisecond)
	mr.FastForward(300 * time.Millisecond)
	time.Sleep(300 * time.Millisecond)

	// Node B starts and subscribes with Last-Event-ID from message 0.
	clientB := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	nodeB, err := NewRedisTransport(clientB, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { nodeB.Close(context.Background()) })

	tss := testTopicSelectorStore()
	sub := mercure.NewLocalSubscriber(publishedIDs[0], testLogger(), tss)
	sub.SetTopics([]string{"https://example.com/cross-history"}, nil)
	require.NoError(t, nodeB.AddSubscriber(context.Background(), sub))

	// Should receive messages 1 and 2 (after the matched ID).
	for i := 1; i < 3; i++ {
		select {
		case received := <-sub.Receive():
			assert.Equal(t, fmt.Sprintf("history-%d", i), received.Data)
		case <-time.After(5 * time.Second):
			t.Fatalf("timeout waiting for cross-node history message %d", i)
		}
	}
}

func BenchmarkRedisTransportDispatch(b *testing.B) {
	mr := miniredis.RunT(b)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	transport, err := NewRedisTransport(
		client,
		withSkipVersionCheck(),
		WithLogger(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))),
		WithXReadBlock(50*time.Millisecond),
		WithHealthInterval(24*time.Hour),
		WithPresenceInterval(24*time.Hour),
		withSkipPresenceIntervalCheck(),
		WithZombieGCInterval(0),
	)
	require.NoError(b, err)
	b.Cleanup(func() { transport.Close(context.Background()) })

	ctx := context.Background()

	b.ResetTimer()

	for b.Loop() {
		u := &mercure.Update{
			Topics: []string{"https://example.com/bench"},
			Event:  mercure.Event{Data: "benchmark payload"},
		}
		if err := transport.Dispatch(ctx, u); err != nil {
			b.Fatal(err)
		}
	}
}

// TestRedisTransportReconnect verifies that the XREADGROUP listener recovers
// from a destroyed consumer group (NOGROUP error). The listener should
// re-create the group and resume delivery without losing messages.
func TestRedisTransportReconnect(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	transport, err := NewRedisTransport(
		client,
		withSkipVersionCheck(),
		WithLogger(testLogger()),
		WithXReadBlock(50*time.Millisecond),
		WithHealthInterval(24*time.Hour),
		WithPresenceInterval(24*time.Hour),
		withSkipPresenceIntervalCheck(),
		WithZombieGCInterval(0),
	)
	require.NoError(t, err)
	t.Cleanup(func() { transport.Close(context.Background()) })

	tss := testTopicSelectorStore()
	sub := mercure.NewLocalSubscriber("", testLogger(), tss)
	sub.SetTopics([]string{"https://example.com/reconnect"}, nil)
	require.NoError(t, transport.AddSubscriber(context.Background(), sub))

	// Publish first message — should be received normally.
	u1 := &mercure.Update{
		Topics: []string{"https://example.com/reconnect"},
		Event:  mercure.Event{Data: "before-destroy"},
	}
	require.NoError(t, transport.Dispatch(context.Background(), u1))
	mr.FastForward(100 * time.Millisecond)

	select {
	case received := <-sub.Receive():
		assert.Equal(t, "before-destroy", received.Data)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for message before group destroy")
	}

	// Destroy the consumer group externally (simulates crash/admin action).
	streamKey := key(transport.opts.streamName, "")
	err = client.XGroupDestroy(context.Background(), streamKey, transport.nodeGroup).Err()
	require.NoError(t, err)

	// Give the listener time to hit the NOGROUP error and recover.
	time.Sleep(200 * time.Millisecond)

	// Publish a second message — should still be delivered after recovery.
	u2 := &mercure.Update{
		Topics: []string{"https://example.com/reconnect"},
		Event:  mercure.Event{Data: "after-recovery"},
	}
	require.NoError(t, transport.Dispatch(context.Background(), u2))
	mr.FastForward(200 * time.Millisecond)

	select {
	case received := <-sub.Receive():
		assert.Equal(t, "after-recovery", received.Data)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for message after NOGROUP recovery")
	}
}
