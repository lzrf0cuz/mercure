package redistransport

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
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

func TestGetSubscribersEmpty(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t)

	lastID, subs, err := transport.GetSubscribers(context.Background())
	require.NoError(t, err)
	assert.Equal(t, mercure.EarliestLastEventID, lastID)
	assert.Empty(t, subs)
}

func TestGetSubscribersAfterClose(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t)
	require.NoError(t, transport.Close(context.Background()))

	_, _, err := transport.GetSubscribers(context.Background())
	assert.ErrorIs(t, err, ErrClosedTransport)
}

func TestGetSubscribersWithLocalSubscribers(t *testing.T) {
	t.Parallel()

	transport, mr := newTestTransport(t)
	tss := testTopicSelectorStore()

	// Add a local subscriber.
	sub := mercure.NewLocalSubscriber("", testLogger(), tss)
	sub.SetTopics([]string{"https://example.com/books/{id}"}, nil)
	err := transport.AddSubscriber(context.Background(), sub)
	require.NoError(t, err)

	// Trigger presence heartbeat manually.
	transport.publishPresence(context.Background())
	mr.FastForward(100 * time.Millisecond)

	lastID, subs, err := transport.GetSubscribers(context.Background())
	require.NoError(t, err)
	assert.Equal(t, mercure.EarliestLastEventID, lastID)
	require.Len(t, subs, 1)
	assert.Equal(t, sub.ID, subs[0].ID)
	assert.Equal(t, []string{"https://example.com/books/{id}"}, subs[0].SubscribedTopics)
}

func TestGetSubscribersLastEventID(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t)

	// Publish an update to set the lastEventID.
	update := &mercure.Update{
		Topics: []string{"https://example.com/test"},
		Event:  mercure.Event{Data: "test"},
	}
	err := transport.Dispatch(context.Background(), update)
	require.NoError(t, err)

	lastID, _, err := transport.GetSubscribers(context.Background())
	require.NoError(t, err)
	assert.Equal(t, update.ID, lastID)
}

func TestGetSubscribersCrossNode(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)
	client1 := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	client2 := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	tss := testTopicSelectorStore()
	opts := []Option{
		withSkipVersionCheck(),
		WithLogger(testLogger()),
		WithXReadBlock(50 * time.Millisecond),
		WithHealthInterval(24 * time.Hour),
		WithPresenceInterval(24 * time.Hour),
		withSkipPresenceIntervalCheck(),
		WithZombieGCInterval(0), // Disable periodic GC in tests
	}

	// Create two transport instances sharing the same Redis (simulating two nodes).
	transport1, err := NewRedisTransport(client1, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { transport1.Close(context.Background()) })

	transport2, err := NewRedisTransport(client2, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { transport2.Close(context.Background()) })

	// Add subscribers to each node.
	sub1 := mercure.NewLocalSubscriber("", testLogger(), tss)
	sub1.SetTopics([]string{"https://example.com/node1"}, nil)
	require.NoError(t, transport1.AddSubscriber(context.Background(), sub1))

	sub2 := mercure.NewLocalSubscriber("", testLogger(), tss)
	sub2.SetTopics([]string{"https://example.com/node2"}, nil)
	require.NoError(t, transport2.AddSubscriber(context.Background(), sub2))

	// Trigger presence on both nodes.
	transport1.publishPresence(context.Background())
	transport2.publishPresence(context.Background())

	// Query from node 1 — should see both nodes' subscribers.
	_, subs, err := transport1.GetSubscribers(context.Background())
	require.NoError(t, err)
	require.Len(t, subs, 2)

	ids := make(map[string]bool)
	for _, s := range subs {
		ids[s.ID] = true
	}

	assert.True(t, ids[sub1.ID], "should contain node 1's subscriber")
	assert.True(t, ids[sub2.ID], "should contain node 2's subscriber")
}

func TestGetSubscribersCrossNodeKeyExpiry(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)
	client1 := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	client2 := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	tss := testTopicSelectorStore()
	opts := []Option{
		withSkipVersionCheck(),
		WithLogger(testLogger()),
		WithXReadBlock(50 * time.Millisecond),
		WithHealthInterval(24 * time.Hour),
		WithPresenceInterval(24 * time.Hour),
		withSkipPresenceIntervalCheck(),
		WithPresenceTTL(2 * time.Second),
		WithZombieGCInterval(0),
	}

	transport1, err := NewRedisTransport(client1, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { transport1.Close(context.Background()) })

	transport2, err := NewRedisTransport(client2, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { transport2.Close(context.Background()) })

	// Add subscriber to node 2.
	sub := mercure.NewLocalSubscriber("", testLogger(), tss)
	sub.SetTopics([]string{"https://example.com/test"}, nil)
	require.NoError(t, transport2.AddSubscriber(context.Background(), sub))
	transport2.publishPresence(context.Background())

	// Verify node 2's subscriber is visible from node 1.
	_, subs, err := transport1.GetSubscribers(context.Background())
	require.NoError(t, err)
	require.Len(t, subs, 1)

	// Fast-forward past the presence TTL so node 2's key expires.
	mr.FastForward(3 * time.Second)

	// Node 2's subscriber should no longer appear.
	_, subs, err = transport1.GetSubscribers(context.Background())
	require.NoError(t, err)
	assert.Empty(t, subs)
}

func TestGetSubscribersDeduplication(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t)
	tss := testTopicSelectorStore()

	// Add a subscriber.
	sub := mercure.NewLocalSubscriber("", testLogger(), tss)
	sub.SetTopics([]string{"https://example.com/dedup"}, nil)
	require.NoError(t, transport.AddSubscriber(context.Background(), sub))

	// Write presence twice (simulating SCAN returning duplicates).
	transport.publishPresence(context.Background())

	// Manually write a second presence key with the same subscriber data
	// to simulate SCAN returning the same key multiple times.
	data, err := json.Marshal([]mercure.Subscriber{sub.Subscriber})
	require.NoError(t, err)

	secondKey := key(transport.opts.streamName, ":presence:duplicate-node")
	require.NoError(t, transport.client.Set(context.Background(), secondKey, string(data), time.Minute).Err())

	_, subs, err := transport.GetSubscribers(context.Background())
	require.NoError(t, err)

	// Subscriber should appear only once despite being in two presence keys.
	assert.Len(t, subs, 1)
	assert.Equal(t, sub.ID, subs[0].ID)
}

func TestGetSubscribersMultipleOnSameNode(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t)
	tss := testTopicSelectorStore()

	// Add multiple subscribers.
	sub1 := mercure.NewLocalSubscriber("", testLogger(), tss)
	sub1.SetTopics([]string{"https://example.com/topic-a"}, nil)
	require.NoError(t, transport.AddSubscriber(context.Background(), sub1))

	sub2 := mercure.NewLocalSubscriber("", testLogger(), tss)
	sub2.SetTopics([]string{"https://example.com/topic-b"}, nil)
	require.NoError(t, transport.AddSubscriber(context.Background(), sub2))

	sub3 := mercure.NewLocalSubscriber("", testLogger(), tss)
	sub3.SetTopics([]string{"https://example.com/topic-c"}, nil)
	require.NoError(t, transport.AddSubscriber(context.Background(), sub3))

	transport.publishPresence(context.Background())

	_, subs, err := transport.GetSubscribers(context.Background())
	require.NoError(t, err)
	assert.Len(t, subs, 3)

	ids := make(map[string]bool)
	for _, s := range subs {
		ids[s.ID] = true
	}

	assert.True(t, ids[sub1.ID])
	assert.True(t, ids[sub2.ID])
	assert.True(t, ids[sub3.ID])
}

func TestGetSubscribersAfterRemove(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t)
	tss := testTopicSelectorStore()

	sub := mercure.NewLocalSubscriber("", testLogger(), tss)
	sub.SetTopics([]string{"https://example.com/test"}, nil)
	require.NoError(t, transport.AddSubscriber(context.Background(), sub))

	// Publish presence with subscriber present.
	transport.publishPresence(context.Background())

	_, subs, err := transport.GetSubscribers(context.Background())
	require.NoError(t, err)
	assert.Len(t, subs, 1)

	// Remove subscriber and re-publish presence.
	require.NoError(t, transport.RemoveSubscriber(context.Background(), sub))
	transport.publishPresence(context.Background())

	_, subs, err = transport.GetSubscribers(context.Background())
	require.NoError(t, err)
	assert.Empty(t, subs)
}

func TestGetSubscribersInvalidPresenceData(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t)

	// Write invalid JSON to a presence key.
	invalidKey := key(transport.opts.streamName, ":presence:invalid-node")
	require.NoError(t, transport.client.Set(context.Background(), invalidKey, "not valid json", time.Minute).Err())

	// Should not error — invalid data is logged and skipped.
	lastID, subs, err := transport.GetSubscribers(context.Background())
	require.NoError(t, err)
	assert.Equal(t, mercure.EarliestLastEventID, lastID)
	assert.Empty(t, subs)
}

func TestGetSubscribersEmptyPresenceValue(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t)

	// Write empty string to a presence key (simulates node starting up).
	emptyKey := key(transport.opts.streamName, ":presence:empty-node")
	require.NoError(t, transport.client.Set(context.Background(), emptyKey, "", time.Minute).Err())

	// Should not error — empty values are skipped.
	_, subs, err := transport.GetSubscribers(context.Background())
	require.NoError(t, err)
	assert.Empty(t, subs)
}

func TestPublishPresenceSerializesSubscribers(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t)
	tss := testTopicSelectorStore()

	sub := mercure.NewLocalSubscriber("", testLogger(), tss)
	sub.SetTopics([]string{"https://example.com/serialize-test"}, nil)
	require.NoError(t, transport.AddSubscriber(context.Background(), sub))

	transport.publishPresence(context.Background())

	// Read the raw presence key and verify JSON structure.
	presenceKey := key(transport.opts.streamName, ":presence:"+transport.nodeID)
	val, err := transport.client.Get(context.Background(), presenceKey).Result()
	require.NoError(t, err)
	require.NotEmpty(t, val)

	var decoded []mercure.Subscriber

	require.NoError(t, json.Unmarshal([]byte(val), &decoded))
	require.Len(t, decoded, 1)
	assert.Equal(t, sub.ID, decoded[0].ID)
	assert.Equal(t, []string{"https://example.com/serialize-test"}, decoded[0].SubscribedTopics)
}

func TestPeriodicZombieGC(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	streamKey := "{mercure}"

	// Manually create a stream and a zombie consumer group (no presence key).
	_, err := client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: streamKey,
		ID:     "*",
		Values: map[string]any{"data": "dummy"},
	}).Result()
	require.NoError(t, err)

	zombieGroup := "mercure:node:zombie-dead-node-id"
	err = client.XGroupCreate(context.Background(), streamKey, zombieGroup, "0").Err()
	require.NoError(t, err)

	// Verify zombie group exists.
	groups, err := client.XInfoGroups(context.Background(), streamKey).Result()
	require.NoError(t, err)

	zombieFound := false

	for _, g := range groups {
		if g.Name == zombieGroup {
			zombieFound = true
		}
	}

	require.True(t, zombieFound, "zombie group should exist before GC")

	// Create transport — startup GC should clean the zombie.
	transport, err := NewRedisTransport(
		client,
		withSkipVersionCheck(),
		WithLogger(testLogger()),
		WithXReadBlock(50*time.Millisecond),
		WithHealthInterval(24*time.Hour),
		WithPresenceInterval(24*time.Hour),
		withSkipPresenceIntervalCheck(),
		WithZombieGCInterval(0), // Disable periodic GC for this test
	)
	require.NoError(t, err)
	t.Cleanup(func() { transport.Close(context.Background()) })

	// Zombie group should be cleaned by startup GC.
	groups, err = client.XInfoGroups(context.Background(), streamKey).Result()
	require.NoError(t, err)

	for _, g := range groups {
		assert.NotEqual(t, zombieGroup, g.Name, "zombie group should have been cleaned by startup GC")
	}
}

func TestPeriodicZombieGCPreservesLiveGroups(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)
	client1 := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	client2 := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	opts := []Option{
		withSkipVersionCheck(),
		WithLogger(testLogger()),
		WithXReadBlock(50 * time.Millisecond),
		WithHealthInterval(24 * time.Hour),
		WithPresenceInterval(24 * time.Hour),
		withSkipPresenceIntervalCheck(),
		WithZombieGCInterval(0),
	}

	// Create two transports sharing the same Redis.
	transport1, err := NewRedisTransport(client1, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { transport1.Close(context.Background()) })

	transport2, err := NewRedisTransport(client2, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { transport2.Close(context.Background()) })

	// Run GC on transport1 — it should NOT destroy transport2's group.
	streamKey := key(transport1.opts.streamName, "")
	transport1.gcZombieGroups(context.Background(), streamKey)

	// Verify transport2's group still exists.
	groups, err := transport1.client.XInfoGroups(context.Background(), streamKey).Result()
	require.NoError(t, err)

	groupFound := false

	for _, g := range groups {
		if g.Name == transport2.nodeGroup {
			groupFound = true
		}
	}

	assert.True(t, groupFound, "GC should NOT destroy live node's consumer group")
}

func TestPresenceKeyHasCorrectTTL(t *testing.T) {
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
		WithPresenceTTL(60*time.Second),
		WithZombieGCInterval(0),
	)
	require.NoError(t, err)
	t.Cleanup(func() { transport.Close(context.Background()) })

	transport.publishPresence(context.Background())

	presenceKey := key(transport.opts.streamName, ":presence:"+transport.nodeID)
	ttl := mr.TTL(presenceKey)
	assert.InDelta(t, 60.0, ttl.Seconds(), 5.0, "presence key TTL should be ~60s")
}

func TestStartupPresenceKeyIsValidJSON(t *testing.T) {
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

	// The initial presence key (set during startup) should be valid JSON: "[]".
	presenceKey := key(transport.opts.streamName, ":presence:"+transport.nodeID)
	val, err := client.Get(context.Background(), presenceKey).Result()
	require.NoError(t, err)

	var decoded []mercure.Subscriber

	require.NoError(t, json.Unmarshal([]byte(val), &decoded))
	assert.Empty(t, decoded)
}

func TestGetSubscribersCrossNodeTopicSelectorStoreInjection(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)
	client1 := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	client2 := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	tss := testTopicSelectorStore()
	opts := []Option{
		withSkipVersionCheck(),
		WithLogger(testLogger()),
		WithXReadBlock(50 * time.Millisecond),
		WithHealthInterval(24 * time.Hour),
		WithPresenceInterval(24 * time.Hour),
		withSkipPresenceIntervalCheck(),
		WithZombieGCInterval(0),
	}

	// Create node 1 and set its TopicSelectorStore (as the hub would).
	transport1, err := NewRedisTransport(client1, opts...)
	require.NoError(t, err)
	transport1.SetTopicSelectorStore(tss)
	t.Cleanup(func() { transport1.Close(context.Background()) })

	// Create node 2 with a subscriber.
	transport2, err := NewRedisTransport(client2, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { transport2.Close(context.Background()) })

	sub := mercure.NewLocalSubscriber("", testLogger(), tss)
	sub.SetTopics([]string{"https://example.com/books/{id}"}, nil)
	require.NoError(t, transport2.AddSubscriber(context.Background(), sub))
	transport2.publishPresence(context.Background())

	// Query from node 1 — cross-node subscriber should have TopicSelectorStore injected.
	_, subs, err := transport1.GetSubscribers(context.Background())
	require.NoError(t, err)
	require.Len(t, subs, 1)

	// The cross-node subscriber should be able to call MatchTopics without panicking.
	assert.True(t, subs[0].MatchTopics([]string{"https://example.com/books/42"}, false))
	assert.False(t, subs[0].MatchTopics([]string{"https://example.com/authors/1"}, false))
}

func TestPresenceSummaryMode(t *testing.T) {
	t.Parallel()

	// Set threshold to 2 so we can trigger summary mode easily.
	transport, _ := newTestTransport(t, WithPresenceDetailThreshold(2))
	tss := testTopicSelectorStore()

	// Add 3 subscribers (above threshold of 2).
	for i := range 3 {
		sub := mercure.NewLocalSubscriber("", testLogger(), tss)
		sub.SetTopics([]string{"https://example.com/test"}, nil)
		require.NoError(t, transport.AddSubscriber(context.Background(), sub), "sub %d", i)
	}

	// Trigger heartbeat — should use summary mode.
	transport.publishPresence(context.Background())

	// Verify the stored value is in summary format.
	val, err := transport.client.Get(context.Background(), transport.key(":presence:"+transport.nodeID)).Result()
	require.NoError(t, err)

	var summary presenceSummary

	require.NoError(t, json.Unmarshal([]byte(val), &summary))
	assert.True(t, summary.Summary, "should be in summary mode")
	assert.Equal(t, 3, summary.SubscriberCount)
	assert.Equal(t, transport.nodeID, summary.NodeID)
	assert.Empty(t, summary.Subscribers, "summary mode should not include subscribers")
}

func TestPresenceSummaryModeCrossNode(t *testing.T) {
	t.Parallel()

	// Node 1 is in summary mode (threshold=2 with 3 subs).
	// Node 2 queries GetSubscribers — should see 0 detailed subscribers from node 1.
	mr := miniredis.RunT(t)
	client1 := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	client2 := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	opts := []Option{
		withSkipVersionCheck(),
		WithLogger(testLogger()),
		WithXReadBlock(50 * time.Millisecond),
		WithHealthInterval(24 * time.Hour),
		WithPresenceInterval(24 * time.Hour),
		withSkipPresenceIntervalCheck(),
		WithPresenceDetailThreshold(2),
	}

	tss := testTopicSelectorStore()

	transport1, err := NewRedisTransport(client1, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { transport1.Close(context.Background()) })

	transport2, err := NewRedisTransport(client2, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { transport2.Close(context.Background()) })

	// Add 3 subscribers to node 1 (above threshold).
	for i := range 3 {
		sub := mercure.NewLocalSubscriber("", testLogger(), tss)
		sub.SetTopics([]string{"https://example.com/test"}, nil)
		require.NoError(t, transport1.AddSubscriber(context.Background(), sub), "sub %d", i)
	}

	transport1.publishPresence(context.Background())

	// Add 1 subscriber to node 2 (below threshold).
	sub := mercure.NewLocalSubscriber("", testLogger(), tss)
	sub.SetTopics([]string{"https://example.com/test"}, nil)
	require.NoError(t, transport2.AddSubscriber(context.Background(), sub))
	transport2.publishPresence(context.Background())

	// Query from node 2: should see its own 1 subscriber (detailed)
	// but no detailed subscribers from node 1 (summary mode).
	_, subs, err := transport2.GetSubscribers(context.Background())
	require.NoError(t, err)
	assert.Len(t, subs, 1, "should only see node 2's detailed subscriber, not node 1's summary")
}

func TestPresenceDetailModeUnderThreshold(t *testing.T) {
	t.Parallel()

	// Below threshold: full detail should be stored.
	transport, _ := newTestTransport(t, WithPresenceDetailThreshold(10))
	tss := testTopicSelectorStore()

	sub := mercure.NewLocalSubscriber("", testLogger(), tss)
	sub.SetTopics([]string{"https://example.com/test"}, nil)
	require.NoError(t, transport.AddSubscriber(context.Background(), sub))
	transport.publishPresence(context.Background())

	val, err := transport.client.Get(context.Background(), transport.key(":presence:"+transport.nodeID)).Result()
	require.NoError(t, err)

	// Should be a JSON array (detailed), not a summary object.
	var subs []*mercure.Subscriber

	require.NoError(t, json.Unmarshal([]byte(val), &subs))
	assert.Len(t, subs, 1)
}

// TestPresenceByteBudgetFallback exercises the byte-budget fallback:
// subscriber count stays below presenceDetailThreshold but the marshaled
// detail payload exceeds presenceDetailByteThreshold, so the writer
// re-marshals as summary mode and increments
// presence_fallback_total{reason="byte_budget"}.
func TestPresenceByteBudgetFallback(t *testing.T) {
	t.Parallel()

	// 1 KiB byte budget; topic strings sized to blow it under count gate of 10.
	transport, _ := newTestTransport(
		t,
		WithPrometheusRegisterer(prometheus.NewRegistry()),
		WithPresenceDetailThreshold(10),
		WithPresenceDetailByteThreshold(1024),
	)
	tss := testTopicSelectorStore()

	bigTopic := "https://example.com/" + strings.Repeat("a", 1500)

	for range 3 {
		sub := mercure.NewLocalSubscriber("", testLogger(), tss)
		sub.SetTopics([]string{bigTopic}, nil)
		require.NoError(t, transport.AddSubscriber(context.Background(), sub))
	}

	// Precondition: prove the test premise. If a future Subscriber-
	// shape compaction drops the marshaled detail array below 1 KiB,
	// the byte-budget branch would not fire and the test would pass
	// vacuously. Catch that here with a clear "test premise broken"
	// signal instead of a confusing "fallback didn't fire".
	var preview []mercure.Subscriber

	transport.walkAllSubscribers(func(s *mercure.LocalSubscriber) bool {
		preview = append(preview, s.Subscriber)

		return true
	})

	previewBytes, marshalErr := json.Marshal(preview)
	require.NoError(t, marshalErr)
	require.Greater(t, len(previewBytes), 1024,
		"test premise broken: marshaled detail array must exceed the 1024-byte budget for the fallback to fire")

	m := transport.metrics.Load()
	require.NotNil(t, m)
	before := counterValueForTest(t,
		m.presenceFallback.WithLabelValues(string(presenceFallbackByteBudget)))

	data, ok := transport.buildPresencePayload()
	require.True(t, ok, "byte-budget fallback must still ship a payload")

	var summary presenceSummary

	require.NoError(t, json.Unmarshal(data, &summary),
		"payload after byte-budget fallback must be summary-mode JSON")
	assert.True(t, summary.Summary)
	assert.Equal(t, 3, summary.SubscriberCount)

	after := counterValueForTest(t,
		m.presenceFallback.WithLabelValues(string(presenceFallbackByteBudget)))
	assert.InDelta(t, before+1, after, 0.001,
		"presence_fallback_total{reason=byte_budget} must increment exactly once")
}

// TestPresenceByteBudgetDisabled — byte threshold of 0 must NOT trigger
// the fallback, even with a large payload.
func TestPresenceByteBudgetDisabled(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(
		t,
		WithPrometheusRegisterer(prometheus.NewRegistry()),
		WithPresenceDetailThreshold(10),
		WithPresenceDetailByteThreshold(0),
	)
	tss := testTopicSelectorStore()

	bigTopic := "https://example.com/" + strings.Repeat("a", 1500)
	sub := mercure.NewLocalSubscriber("", testLogger(), tss)
	sub.SetTopics([]string{bigTopic}, nil)
	require.NoError(t, transport.AddSubscriber(context.Background(), sub))

	m := transport.metrics.Load()
	require.NotNil(t, m)
	before := counterValueForTest(t,
		m.presenceFallback.WithLabelValues(string(presenceFallbackByteBudget)))

	data, ok := transport.buildPresencePayload()
	require.True(t, ok)

	// Should be detail-mode (a JSON array of subscribers), not summary.
	var detail []*mercure.Subscriber

	require.NoError(t, json.Unmarshal(data, &detail))
	assert.Len(t, detail, 1)

	after := counterValueForTest(t,
		m.presenceFallback.WithLabelValues(string(presenceFallbackByteBudget)))
	assert.InDelta(t, before, after, 0.001,
		"byte-budget fallback must NOT fire when byte threshold is 0")
}

// TestSubscriberCountTracksMembership locks the invariant that the atomic
// subscriberCount — read by presence summary mode (and the future admission
// ceiling) instead of an O(n) shard walk — exactly mirrors shard membership
// across add and remove, i.e. it can neither drift nor double-count.
func TestSubscriberCountTracksMembership(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t)
	tss := testTopicSelectorStore()
	ctx := context.Background()

	require.EqualValues(t, 0, transport.subscriberCount.Load(), "starts at zero")

	subs := make([]*mercure.LocalSubscriber, 0, 5)

	for range 5 {
		s := mercure.NewLocalSubscriber("", testLogger(), tss)
		s.SetTopics([]string{"https://example.com/test"}, nil)
		require.NoError(t, transport.AddSubscriber(ctx, s))

		subs = append(subs, s)
	}

	assert.EqualValues(t, 5, transport.subscriberCount.Load(), "5 adds → 5")

	require.NoError(t, transport.RemoveSubscriber(ctx, subs[0]))
	require.NoError(t, transport.RemoveSubscriber(ctx, subs[1]))
	assert.EqualValues(t, 3, transport.subscriberCount.Load(), "5 adds − 2 removes → 3")

	// The atomic count MUST equal the actual walked shard membership — this is
	// what catches a drift or a double-count between the counter and the lists.
	var walked int

	transport.walkAllSubscribers(func(*mercure.LocalSubscriber) bool {
		walked++

		return true
	})
	assert.Equal(t, walked, int(transport.subscriberCount.Load()),
		"atomic count must equal walked membership (no drift / double-count)")
}

// TestBuildPresencePayloadSummaryUsesAtomicCount verifies that above the detail
// threshold buildPresencePayload reports the atomic count as a summary and omits
// the per-subscriber detail list (the O(n)-walk-avoiding path), AND that the
// reported count is genuinely sourced from the atomic rather than a walk.
func TestBuildPresencePayloadSummaryUsesAtomicCount(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t, WithPresenceDetailThreshold(2))
	tss := testTopicSelectorStore()
	ctx := context.Background()

	for range 3 { // 3 > threshold 2 → summary mode
		s := mercure.NewLocalSubscriber("", testLogger(), tss)
		s.SetTopics([]string{"https://example.com/test"}, nil)
		require.NoError(t, transport.AddSubscriber(ctx, s))
	}

	data, ok := transport.buildPresencePayload()
	require.True(t, ok)
	assert.Contains(t, string(data), `"subscriber_count":3`,
		"summary reports the atomic subscriber count")
	assert.NotContains(t, string(data), `"subscribers"`,
		"summary must omit the per-subscriber detail list")

	// Discriminator: desync the atomic above actual membership and confirm the
	// summary reports the atomic value (777), not the walked membership (3).
	// Without this, the test would pass even against the old walk-based gate.
	transport.subscriberCount.Store(777)

	data, ok = transport.buildPresencePayload()
	require.True(t, ok)

	var summary presenceSummary

	require.NoError(t, json.Unmarshal(data, &summary),
		"summary mode must emit object-shaped JSON (a detail array would fail this unmarshal)")
	assert.True(t, summary.Summary)
	assert.Equal(t, 777, summary.SubscriberCount,
		"summary count must read the atomic (777), not the walked membership (3)")
}

// TestBuildPresencePayloadBoundsDetailWalkOnCountRace covers the TOCTOU between
// the lock-free count gate and the detail walk: if membership outruns a
// stale-low count, the walk is bounded and the payload falls back to summary
// (reason=count_race) instead of marshaling an unbounded detail list.
func TestBuildPresencePayloadBoundsDetailWalkOnCountRace(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(
		t,
		WithPrometheusRegisterer(prometheus.NewRegistry()),
		WithPresenceDetailThreshold(2),
	)
	tss := testTopicSelectorStore()
	ctx := context.Background()

	for range 5 { // real membership (5) exceeds the detail threshold (2)
		s := mercure.NewLocalSubscriber("", testLogger(), tss)
		s.SetTopics([]string{"https://example.com/test"}, nil)
		require.NoError(t, transport.AddSubscriber(ctx, s))
	}

	// Force the gate to read stale-low so the detail path is entered despite
	// real membership exceeding the threshold (the reconnect-storm TOCTOU).
	transport.subscriberCount.Store(1)

	m := transport.metrics.Load()
	require.NotNil(t, m)
	before := counterValueForTest(t,
		m.presenceFallback.WithLabelValues(string(presenceFallbackCountRace)))

	data, ok := transport.buildPresencePayload()
	require.True(t, ok)

	var summary presenceSummary

	require.NoError(t, json.Unmarshal(data, &summary),
		"count-race overflow must fall back to summary-mode JSON, not a detail array")
	assert.True(t, summary.Summary)

	// The walk observed at least threshold+1 (=3) members before overflowing, so
	// the summary must report >= 3 even though the re-read atomic is stale-low (1).
	assert.GreaterOrEqual(t, summary.SubscriberCount, 3,
		"overflow summary must not under-report below the membership the walk observed")

	after := counterValueForTest(t,
		m.presenceFallback.WithLabelValues(string(presenceFallbackCountRace)))
	assert.InDelta(t, before+1, after, 0.001,
		"presence_fallback_total{reason=count_race} must increment exactly once")
}

// TestSubscriberCountNoDriftOnRedundantRemove locks the membership-gated
// decrement: a redundant or non-member removal is a no-op, so subscriberCount
// cannot drift negative (which would later panic make([]…, 0, count)).
func TestSubscriberCountNoDriftOnRedundantRemove(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t)
	tss := testTopicSelectorStore()
	ctx := context.Background()

	s := mercure.NewLocalSubscriber("", testLogger(), tss)
	s.SetTopics([]string{"https://example.com/test"}, nil)
	require.NoError(t, transport.AddSubscriber(ctx, s))
	require.EqualValues(t, 1, transport.subscriberCount.Load())

	require.NoError(t, transport.RemoveSubscriber(ctx, s))
	require.EqualValues(t, 0, transport.subscriberCount.Load(), "one remove → 0")

	// Redundant remove of the same subscriber + removal of a never-added one:
	// both must be no-ops, not decrements.
	require.NoError(t, transport.RemoveSubscriber(ctx, s))

	never := mercure.NewLocalSubscriber("", testLogger(), tss)
	never.SetTopics([]string{"https://example.com/test"}, nil)
	require.NoError(t, transport.RemoveSubscriber(ctx, never))

	assert.EqualValues(t, 0, transport.subscriberCount.Load(),
		"redundant / non-member removals must not drive the count negative")
	require.NotPanics(t, func() { transport.buildPresencePayload() },
		"non-negative count keeps make([]…, 0, count) panic-free")
}

// TestSubscriberCountNoDriftOnConcurrentRemove is the concurrent companion to
// the sequential test above: many goroutines removing the SAME subscriber must
// net exactly one decrement. This fails (count goes negative) if the membership
// gate and lostFlags.Delete are not atomic under t.mu. Run under -race.
func TestSubscriberCountNoDriftOnConcurrentRemove(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t)
	tss := testTopicSelectorStore()
	ctx := context.Background()

	s := mercure.NewLocalSubscriber("", testLogger(), tss)
	s.SetTopics([]string{"https://example.com/test"}, nil)
	require.NoError(t, transport.AddSubscriber(ctx, s))
	require.EqualValues(t, 1, transport.subscriberCount.Load())

	const racers = 16

	var wg sync.WaitGroup

	wg.Add(racers)

	for range racers {
		go func() {
			defer wg.Done()

			_ = transport.RemoveSubscriber(ctx, s)
		}()
	}

	wg.Wait()

	assert.EqualValues(t, 0, transport.subscriberCount.Load(),
		"concurrent redundant removes must net exactly one decrement (no negative drift)")
}

// TestBuildPresencePayloadOverflowAtThresholdPlusOne locks the walk bound at the
// exact boundary: a membership of threshold+1 (entered via a stale-low count)
// must overflow to summary, not emit a detail array one item over the limit.
// Fails against a `> threshold` (off-by-one) bound, which would marshal a detail
// array and break the summary unmarshal.
func TestBuildPresencePayloadOverflowAtThresholdPlusOne(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t, WithPresenceDetailThreshold(2))
	tss := testTopicSelectorStore()
	ctx := context.Background()

	for range 3 { // exactly threshold(2)+1
		s := mercure.NewLocalSubscriber("", testLogger(), tss)
		s.SetTopics([]string{"https://example.com/test"}, nil)
		require.NoError(t, transport.AddSubscriber(ctx, s))
	}

	// Force detail entry: count == threshold passes the top gate (count > thresh
	// is false), so the WALK boundary is what must catch the (threshold+1)th.
	transport.subscriberCount.Store(2)

	data, ok := transport.buildPresencePayload()
	require.True(t, ok)

	var summary presenceSummary

	require.NoError(t, json.Unmarshal(data, &summary),
		"membership of exactly threshold+1 must overflow to summary, not a detail array")
	assert.True(t, summary.Summary)
}

// TestJitterStartContract locks the deterministic-per-node jitter: stable for a
// given node, always within [0, d), 0 for a non-positive interval, AND spread
// across the full interval (not collapsed into a truncated sub-band).
func TestJitterStartContract(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t)

	const d = 30 * time.Second

	a := transport.jitterStart(d)
	b := transport.jitterStart(d)
	assert.Equal(t, a, b, "deterministic for a given node (no PRNG)")
	assert.GreaterOrEqual(t, a, time.Duration(0), "non-negative")
	assert.Less(t, a, d, "within [0, d)")
	assert.Zero(t, transport.jitterStart(0), "non-positive interval → 0")
	assert.Zero(t, transport.jitterStart(-time.Second), "negative interval → 0")

	// The offset must populate the FULL interval. A 32-bit-ns hash would cap
	// every offset at ~4.295s and never exceed d/2 for a 30s interval, so
	// requiring the max sampled offset to reach the upper half catches that
	// truncation (which silently defeats fleet desync).
	var maxSeen time.Duration

	for i := range 256 {
		nodeID := string([]byte{byte(i)})
		j := (&RedisTransport{nodeID: nodeID}).jitterStart(d)
		require.GreaterOrEqual(t, j, time.Duration(0), "non-negative for node %q", nodeID)
		require.Less(t, j, d, "within [0, d) for node %q", nodeID)

		if j > maxSeen {
			maxSeen = j
		}
	}

	assert.Greater(t, maxSeen, d/2,
		"jitter must populate the full [0, d) window; a sub-band defeats fleet desync")
}

// mgetCountingHook counts MGET commands issued through a client, so a test can
// assert readPresenceKeys actually splits into several MGETs.
type mgetCountingHook struct{ count atomic.Int64 }

func (h *mgetCountingHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *mgetCountingHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if cmd.Name() == "mget" {
			h.count.Add(1)
		}

		return next(ctx, cmd)
	}
}

func (h *mgetCountingHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

// TestReadPresenceKeysBatchesMGet seeds more presence keys than one MGET batch
// (presenceMGetBatchSize) and asserts (a) every node's subscriber comes back —
// a batch-loop bound bug (dropped batch / out-of-range tail slice) would lose
// subscribers or panic — and (b) the fetch actually splits into several MGETs,
// not one fleet-wide command (the whole point of the change). The MGET-counting
// hook is installed BEFORE NewRedisTransport so it can't race the listener.
func TestReadPresenceKeysBatchesMGet(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	var hook mgetCountingHook

	client.AddHook(&hook)

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

	ctx := context.Background()

	const nodes = presenceMGetBatchSize*2 + 37 // 237 → ≥ 3 batches (with the node's own key)

	want := make(map[string]bool, nodes)

	for i := range nodes {
		id := "urn:uuid:node-" + strconv.Itoa(i)
		want[id] = true

		payload, err := json.Marshal([]*mercure.Subscriber{{ID: id}})
		require.NoError(t, err)

		require.NoError(t, client.Set(ctx,
			transport.presenceKey("node-"+strconv.Itoa(i)), string(payload), 0).Err())
	}

	before := hook.count.Load()

	_, subs, err := transport.GetSubscribers(ctx)
	require.NoError(t, err)
	require.Lenf(t, subs, nodes,
		"all %d nodes' subscribers must come back across the MGET batches", nodes)

	got := make(map[string]bool, len(subs))
	for _, s := range subs {
		got[s.ID] = true
	}

	assert.Equal(t, want, got, "subscriber ID set must match across all batches")
	assert.GreaterOrEqualf(t, hook.count.Load()-before, int64(3),
		"237+ presence keys must split into ≥3 MGET batches, not one fleet-wide MGET (saw %d)",
		hook.count.Load()-before)
}

// TestGetSubscribersCapAbortsWithError verifies the materialization cap: when the
// cluster's subscriber count exceeds WithSubscriptionsMaxSubscribers,
// GetSubscribers aborts with mercure.ErrTooManySubscribers (the hub maps it to
// 503), returns no partial result, and increments presence_read_capped — rather
// than building the whole cluster's subscriber slice in one heap (OOM).
func TestGetSubscribersCapAbortsWithError(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(
		t,
		WithPrometheusRegisterer(prometheus.NewRegistry()),
		WithSubscriptionsMaxSubscribers(5),
	)
	ctx := context.Background()

	// 10 nodes, 1 subscriber each → 10 unique > cap 5.
	for i := range 10 {
		payload, err := json.Marshal([]*mercure.Subscriber{{ID: "urn:uuid:cap-" + strconv.Itoa(i)}})
		require.NoError(t, err)
		require.NoError(t, transport.client.Set(ctx,
			transport.presenceKey("node-"+strconv.Itoa(i)), string(payload), 0).Err())
	}

	m := transport.metrics.Load()
	require.NotNil(t, m)
	before := counterValueForTest(t, m.presenceReadCapped)

	_, subs, err := transport.GetSubscribers(ctx)
	require.Error(t, err)
	require.ErrorIs(t, err, mercure.ErrTooManySubscribers,
		"cap-hit must return mercure.ErrTooManySubscribers so the hub can map it to 503")
	assert.Nil(t, subs, "a cap-aborted GetSubscribers returns no partial result")

	after := counterValueForTest(t, m.presenceReadCapped)
	assert.InDelta(t, before+1, after, 0.001, "presence_read_capped must increment on cap-hit")
}

// TestGetSubscribersCapDisabledByZero verifies WithSubscriptionsMaxSubscribers(0)
// disables the cap (unbounded — the pre-cap behaviour).
func TestGetSubscribersCapDisabledByZero(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t, WithSubscriptionsMaxSubscribers(0))
	ctx := context.Background()

	const nodes = 50
	for i := range nodes {
		payload, err := json.Marshal([]*mercure.Subscriber{{ID: "urn:uuid:nocap-" + strconv.Itoa(i)}})
		require.NoError(t, err)
		require.NoError(t, transport.client.Set(ctx,
			transport.presenceKey("node-"+strconv.Itoa(i)), string(payload), 0).Err())
	}

	_, subs, err := transport.GetSubscribers(ctx)
	require.NoError(t, err)
	assert.Len(t, subs, nodes, "cap=0 is unbounded — all subscribers returned")
}

// TestGetSubscribersCapBoundary pins the inclusive edge: exactly maxSubs unique
// subscribers must succeed (and return all of them), and maxSubs+1 must refuse.
// This catches a future >= ↔ > slip that the over-cap and disabled tests miss.
func TestGetSubscribersCapBoundary(t *testing.T) {
	t.Parallel()

	const capLimit = 5
	for _, tc := range []struct {
		name    string
		nodes   int
		wantErr bool
	}{
		{name: "exactly at cap succeeds", nodes: capLimit, wantErr: false},
		{name: "one over cap refuses", nodes: capLimit + 1, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			transport, _ := newTestTransport(
				t,
				WithPrometheusRegisterer(prometheus.NewRegistry()),
				WithSubscriptionsMaxSubscribers(capLimit),
			)
			ctx := context.Background()

			for i := range tc.nodes {
				payload, err := json.Marshal([]*mercure.Subscriber{{ID: "urn:uuid:bound-" + strconv.Itoa(i)}})
				require.NoError(t, err)
				require.NoError(t, transport.client.Set(ctx,
					transport.presenceKey("node-"+strconv.Itoa(i)), string(payload), 0).Err())
			}

			_, subs, err := transport.GetSubscribers(ctx)
			if tc.wantErr {
				require.ErrorIs(t, err, mercure.ErrTooManySubscribers)
				assert.Nil(t, subs)

				return
			}

			require.NoError(t, err)
			assert.Len(t, subs, tc.nodes, "exactly maxSubs unique subscribers must all be returned")
		})
	}
}
