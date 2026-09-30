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
// failure surfaces through dispatchHistory and AddSubscriber. AddSubscriber
// counts the subscriber in subscribers_lost{reason="history_replay_failed"}
// and returns the error. The caller then removes the subscriber, and the
// client can reconnect with Last-Event-ID to retry the gap.
//
// A 500ms deadline bounds the test. Without it, go-redis's default MaxRetries
// and backoff stretch the failure detection to several seconds.
func TestHistoryReplayXRANGEErrorPropagates(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	transport, mr := newTestTransport(t, WithPrometheusRegisterer(reg))
	tss := testTopicMatcherStore()

	for range 5 {
		update := &mercure.Update{
			Topics: []string{"https://example.com/test"},
			Data:   "payload",
		}
		require.NoError(t, transport.Dispatch(context.Background(), update))
	}

	// Simulate Redis outage between publish and a reconnecting subscriber.
	mr.Close()

	sub := mercure.NewLocalSubscriber(mercure.EarliestLastEventID, testLogger(), tss)
	sub.SetMatchers(topicMatchers([]string{"https://example.com/test"}), nil)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	err := transport.AddSubscriber(ctx, sub)
	require.Error(t, err, "XRANGE failure during history replay must propagate")
	assert.Contains(t, err.Error(), "history replay")

	// The subscriber stays listed until the caller removes it, as the hub does.
	assert.EqualValues(t, 1, transport.subscriberCount.Load(), "the transport must not unlist the subscriber itself")
	require.NoError(t, transport.RemoveSubscriber(context.Background(), sub))
	assert.EqualValues(t, 0, transport.subscriberCount.Load(), "the caller's removal must unlist the subscriber")

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

// TestCloseJoinsShutdownErrors verifies that Close joins XGroupDestroy and
// Del failures into its returned error.
func TestCloseJoinsShutdownErrors(t *testing.T) {
	t.Parallel()

	transport, mr := newTestTransport(t)

	mr.Close()

	closeErr := transport.Close(context.Background())
	require.Error(t, closeErr, "Close must surface XGroupDestroy/Del failures")
	assert.Contains(t, closeErr.Error(), "destroy consumer group")
	assert.Contains(t, closeErr.Error(), "delete presence key")
}

// TestRedisTransportDoNotDispatchUntilListen verifies that a subscriber added
// before a dispatch receives the update, which carries the ID Dispatch
// assigned, and does not receive an update for a non-matching topic. Delivery
// runs through the Redis Stream and the XREADGROUP goroutine, because the
// transport has no local fast-path.
func TestRedisTransportDoNotDispatchUntilListen(t *testing.T) {
	t.Parallel()

	transport, mr := newTestTransport(t)
	tss := testTopicMatcherStore()

	// Add subscriber first, then publish.
	sub := mercure.NewLocalSubscriber("", testLogger(), tss)
	sub.SetMatchers(topicMatchers([]string{"https://example.com/test"}), nil)
	require.NoError(t, transport.AddSubscriber(context.Background(), sub))

	update := &mercure.Update{
		Topics: []string{"https://example.com/test"},
		Data:   "through-stream",
	}
	require.NoError(t, transport.Dispatch(context.Background(), update))
	mr.FastForward(100 * time.Millisecond)

	// The message was published to Redis and picked up by XREADGROUP.
	// Verify it arrives at the subscriber with correct content.
	select {
	case received := <-sub.Receive():
		assert.Equal(t, "through-stream", received.Data)
		// Dispatch assigns the ID on the update it was given; the subscriber must
		// receive that same ID.
		require.NotEmpty(t, update.ID, "Dispatch should assign an ID")
		assert.Equal(t, update.ID, received.ID, "received update must carry the dispatched ID")
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for update through XREADGROUP")
	}

	// Verify topic filtering works: a non-matching topic must not be received.
	nonMatchingUpdate := &mercure.Update{
		Topics: []string{"https://example.com/other-topic"},
		Data:   "should-not-arrive",
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
	tss := testTopicMatcherStore()

	// Publish several updates.
	ids := make([]string, 0, 5)

	for i := range 5 {
		u := &mercure.Update{
			Topics: []string{"https://example.com/history"},
			Data:   fmt.Sprintf("msg-%d", i),
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
	sub.SetMatchers(topicMatchers([]string{"https://example.com/history"}), nil)
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

// TestRedisTransportHistoryAndLive verifies the history-to-live transition: a
// subscriber receives the replayed history message first and a live update
// published afterwards.
func TestRedisTransportHistoryAndLive(t *testing.T) {
	t.Parallel()

	transport, mr := newTestTransport(t)
	tss := testTopicMatcherStore()

	// Publish a history message.
	historyUpdate := &mercure.Update{
		Topics: []string{"https://example.com/transition"},
		Data:   "history-msg",
	}
	require.NoError(t, transport.Dispatch(context.Background(), historyUpdate))
	mr.FastForward(100 * time.Millisecond)
	time.Sleep(200 * time.Millisecond)

	// Subscribe with earliest to get all history.
	sub := mercure.NewLocalSubscriber(mercure.EarliestLastEventID, testLogger(), tss)
	sub.SetMatchers(topicMatchers([]string{"https://example.com/transition"}), nil)
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
		Data:   "live-msg",
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

// TestRedisTransportDeduplication verifies that an entry the listener re-reads
// from the pending list is not dispatched twice. The first batch XACK fails, so
// the listener drains the pending list from "0" and re-reads the three entries
// it already dispatched; the PEL dedup guard must skip them. The subscriber must
// receive the three published updates once each, in order, and nothing after
// the pending list is acknowledged.
func TestRedisTransportDeduplication(t *testing.T) {
	t.Parallel()

	logs := &safeBuffer{}
	hook := &cmdErrHook{errs: map[string]error{"xack": replyError("ERR injected XACK failure")}}
	hook.on.Store(true)
	transport := newHookedTestTransport(t, []redis.Hook{hook}, WithLogger(jsonTestLogger(logs)))
	tss := testTopicMatcherStore()

	sub := mercure.NewLocalSubscriber("", testLogger(), tss)
	sub.SetMatchers(topicMatchers([]string{"https://example.com/dedup"}), nil)
	require.NoError(t, transport.AddSubscriber(context.Background(), sub))

	// Publish 3 messages.
	for i := range 3 {
		u := &mercure.Update{
			Topics: []string{"https://example.com/dedup"},
			Data:   fmt.Sprintf("dedup-%d", i),
		}
		require.NoError(t, transport.Dispatch(context.Background(), u))
	}

	// The first batch XACK fails: the entries were dispatched but stay pending.
	// Let later XACKs through, so the PEL drain that follows the backoff acks them.
	require.Eventually(t, func() bool {
		return len(logRecords(t, logs, "redis transport: batch XAck failed")) > 0
	}, 5*time.Second, 10*time.Millisecond, "the injected XACK failure must happen")
	hook.on.Store(false)

	// Collect the three published messages.
	var received []string

	timeout := time.After(3 * time.Second)

	for len(received) < 3 {
		select {
		case msg := <-sub.Receive():
			received = append(received, msg.Data)
		case <-timeout:
			require.Len(t, received, 3, "timeout waiting for the three published messages")
		}
	}

	assert.Equal(t, []string{"dedup-0", "dedup-1", "dedup-2"}, received)

	// The PEL drain re-reads the entries and dispatches before it acks, so an
	// empty pending list means a duplicate has already been delivered.
	require.Eventually(t, func() bool {
		pending, err := transport.client.XPending(context.Background(), transport.key(""), transport.nodeGroup).Result()

		return err == nil && pending.Count == 0
	}, 10*time.Second, 50*time.Millisecond, "the PEL drain must acknowledge the entries")

	select {
	case extra := <-sub.Receive():
		t.Fatalf("unexpected extra update after the pending list was acknowledged: %q", extra.Data)
	case <-time.After(200 * time.Millisecond):
	}
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
		WithPresenceInterval(noHeartbeatInterval),
		withSkipPresenceIntervalCheck(),
		WithZombieGCInterval(0),
	}

	// Create 3 nodes sharing the same miniredis.
	var transports [3]*RedisTransport

	var subs [3]*mercure.LocalSubscriber

	tss := testTopicMatcherStore()

	for i := range 3 {
		client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
		tr, err := NewRedisTransport(client, opts...)
		require.NoError(t, err)
		t.Cleanup(func() { tr.Close(context.Background()) })

		transports[i] = tr

		sub := mercure.NewLocalSubscriber("", testLogger(), tss)
		sub.SetMatchers(topicMatchers([]string{"https://example.com/multi/{id}"}), nil)
		require.NoError(t, tr.AddSubscriber(context.Background(), sub))
		subs[i] = sub
	}

	// Publish from node 0.
	update := &mercure.Update{
		Topics: []string{"https://example.com/multi/1"},
		Data:   "cross-node-msg",
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
		WithPresenceInterval(noHeartbeatInterval),
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
			Data:   fmt.Sprintf("history-%d", i),
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

	tss := testTopicMatcherStore()
	sub := mercure.NewLocalSubscriber(publishedIDs[0], testLogger(), tss)
	sub.SetMatchers(topicMatchers([]string{"https://example.com/cross-history"}), nil)
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
		WithPresenceInterval(noHeartbeatInterval),
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
			Data:   "benchmark payload",
		}
		if err := transport.Dispatch(ctx, u); err != nil {
			b.Fatal(err)
		}
	}
}

// TestRedisTransportReconnect verifies that the XREADGROUP listener recovers
// from a destroyed consumer group (NOGROUP error). The listener should
// re-create the group and resume delivery.
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
		WithPresenceInterval(noHeartbeatInterval),
		withSkipPresenceIntervalCheck(),
		WithZombieGCInterval(0),
	)
	require.NoError(t, err)
	t.Cleanup(func() { transport.Close(context.Background()) })

	tss := testTopicMatcherStore()
	sub := mercure.NewLocalSubscriber("", testLogger(), tss)
	sub.SetMatchers(topicMatchers([]string{"https://example.com/reconnect"}), nil)
	require.NoError(t, transport.AddSubscriber(context.Background(), sub))

	// Publish a first message. It should be received normally.
	u1 := &mercure.Update{
		Topics: []string{"https://example.com/reconnect"},
		Data:   "before-destroy",
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

	// Publish a second message. It should still be delivered after recovery.
	u2 := &mercure.Update{
		Topics: []string{"https://example.com/reconnect"},
		Data:   "after-recovery",
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
