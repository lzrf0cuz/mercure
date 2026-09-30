//go:build real_redis

package redistransport

// Real Redis Cluster integration tests. These cover the cluster-topology
// invariants that single-node real_redis tests cannot validate:
//
//   - Hash-tag slot consistency for stream/lastEventID/presence keys
//   - Cross-primary presence enumeration via ClusterClient.Scan
//   - Multi-hub fan-out over a real cluster
//   - The production INFO version-check path through ClusterClient
//
// Gated by REDIS_CLUSTER_ADDRS (comma-separated seed list). Tests skip
// cleanly when the env var is unset, so a default `task rt:test:real:redis`
// run against the single-node compose stack does not fail — the cluster
// suite only runs when the cluster compose stack is up
// (see compose.cluster.yaml).
//
// To run:
//
//	task rt:test:real:cluster      # full lifecycle (up + test + down)
//	# or, against a cluster you brought up yourself:
//	task rt:cluster:up
//	REDIS_CLUSTER_ADDRS=127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002 \
//	  go test -tags=real_redis -run 'TestReal_Cluster_' ./redistransport/...
//	task rt:cluster:down

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/dunglas/mercure"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// realRedisClusterAddrs parses REDIS_CLUSTER_ADDRS (comma-separated). An
// empty/unset env var returns nil so callers can skip cleanly.
func realRedisClusterAddrs() []string {
	env := os.Getenv("REDIS_CLUSTER_ADDRS")
	if env == "" {
		return nil
	}

	parts := strings.Split(env, ",")
	out := make([]string, 0, len(parts))

	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}

	return out
}

// probeRealRedisCluster pings a ClusterClient at REDIS_CLUSTER_ADDRS and
// returns the addresses. Tests should call this first — it Skips cleanly
// when no cluster is configured or reachable, so suites run cleanly on a
// laptop without the cluster compose stack up.
func probeRealRedisCluster(t *testing.T) []string {
	t.Helper()

	addrs := realRedisClusterAddrs()
	if len(addrs) == 0 {
		t.Skip("REDIS_CLUSTER_ADDRS not set (skipping cluster integration test)")
	}

	probe := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:       addrs,
		DialTimeout: 2 * time.Second,
		ReadTimeout: 2 * time.Second,
	})
	defer probe.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := probe.Ping(ctx).Err(); err != nil {
		t.Skipf("real Redis Cluster not reachable at %v: %v", addrs, err)
	}

	return addrs
}

// newRealClusterTransport creates a transport over ClusterClient. The
// {streamName} hash tag places all transport keys (stream, lastEventID,
// presence:*) in one slot, so multi-key operations (presence MGET, Lua) work.
func newRealClusterTransport(t *testing.T, opts ...Option) (*RedisTransport, *redis.ClusterClient) {
	t.Helper()

	addrs := probeRealRedisCluster(t)

	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:       addrs,
		DialTimeout: 2 * time.Second,
		ReadTimeout: 2 * time.Second,
	})

	transport, streamName := buildRealTransport(t, client, opts...)

	t.Cleanup(func() {
		_ = transport.Close(context.Background())

		cleanupClusterStreamKeys(client, streamName)

		_ = client.Close()
	})

	return transport, client
}

// cleanupClusterStreamKeys deletes the stream + lastEventID + every matching
// presence key so cluster tests are idempotent under -count=1. All keys
// share the {streamName} hash tag so DEL routes to a single primary
// without cross-slot errors.
func cleanupClusterStreamKeys(client *redis.ClusterClient, streamName string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	toDelete := []string{
		"{" + streamName + "}",
		"{" + streamName + "}:lastEventID",
	}

	iter := client.Scan(ctx, 0, "{"+streamName+"}:presence:*", 100).Iterator()
	for iter.Next(ctx) {
		toDelete = append(toDelete, iter.Val())
	}

	_, _ = client.Del(ctx, toDelete...).Result()
}

// TestReal_Cluster_VersionValidation verifies the production INFO version
// check works through ClusterClient. ClusterClient routes commands to a
// chosen primary based on slot or random selection for non-keyed commands;
// INFO server is non-keyed, so the routing path differs from a basic
// Client and is worth exercising.
func TestReal_Cluster_VersionValidation(t *testing.T) {
	transport, _ := newRealClusterTransport(t)

	assert.NotNil(t, transport, "transport should initialize without withSkipVersionCheck")
}

// TestReal_Cluster_EndToEndDispatchReceive verifies a full
// publish→XREADGROUP→local-subscriber path against a real cluster.
func TestReal_Cluster_EndToEndDispatchReceive(t *testing.T) {
	transport, _ := newRealClusterTransport(t)

	tss := testTopicMatcherStore()
	sub := mercure.NewLocalSubscriber("", testLogger(), tss)
	sub.SetMatchers(topicMatchers([]string{"https://example.com/cluster"}), nil)

	require.NoError(t, transport.AddSubscriber(context.Background(), sub))

	require.NoError(t, transport.Dispatch(context.Background(), &mercure.Update{
		Topics: []string{"https://example.com/cluster"},
		Data:   "hello cluster",
	}))

	select {
	case msg := <-sub.Receive():
		assert.Equal(t, "hello cluster", msg.Data)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for message from cluster")
	}
}

// TestReal_Cluster_HashTagSlotConsistency verifies that every transport key
// hashes to the same Redis Cluster slot; a key outside the {streamName} hash
// tag would make multi-key operations (XADD+SET, presence MGET) fail with
// CROSSSLOT.
//
// CLUSTER KEYSLOT is deterministic (CRC16 mod 16384 on the substring inside
// the first {...} pair). Equal slot numbers prove cluster will route them
// to the same primary, no writes required.
func TestReal_Cluster_HashTagSlotConsistency(t *testing.T) {
	transport, client := newRealClusterTransport(t)

	streamName := transport.opts.streamName
	keys := []string{
		"{" + streamName + "}",
		"{" + streamName + "}:lastEventID",
		"{" + streamName + "}:presence:node-1",
		"{" + streamName + "}:presence:node-abcdef",
	}

	ctx := context.Background()

	firstSlot, err := client.ClusterKeySlot(ctx, keys[0]).Result()
	require.NoError(t, err)

	for _, k := range keys[1:] {
		slot, err := client.ClusterKeySlot(ctx, k).Result()
		require.NoError(t, err)
		assert.Equal(t, firstSlot, slot,
			"key %q must hash to the same slot as %q", k, keys[0])
	}
}

// TestReal_Cluster_MultiHubCrossNodeDispatch verifies cross-hub dispatch
// over a real cluster. Two transports with separate ClusterClient pools
// against the same cluster + same streamName: hub A publishes, hub B
// receives. Mirrors the multi-hub real_redis invariant on cluster topology.
func TestReal_Cluster_MultiHubCrossNodeDispatch(t *testing.T) {
	addrs := probeRealRedisCluster(t)
	streamName := fmt.Sprintf("mercure-cluster-mh-%s-%d", t.Name(), time.Now().UnixNano())

	hubs := make([]*RedisTransport, 2)
	clients := make([]*redis.ClusterClient, 2)

	for i := range hubs {
		clients[i] = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:       addrs,
			DialTimeout: 2 * time.Second,
			ReadTimeout: 2 * time.Second,
		})

		tr, err := NewRedisTransport(
			clients[i],
			WithLogger(testLogger()),
			WithStreamName(streamName),
			WithXReadBlock(50*time.Millisecond),
			WithHealthInterval(24*time.Hour),
			WithPresenceInterval(24*time.Hour),
			withSkipPresenceIntervalCheck(),
		)
		require.NoError(t, err)

		hubs[i] = tr
	}

	t.Cleanup(func() {
		for _, h := range hubs {
			_ = h.Close(context.Background())
		}

		cleanupClusterStreamKeys(clients[0], streamName)

		for _, c := range clients {
			_ = c.Close()
		}
	})

	tss := testTopicMatcherStore()
	subB := mercure.NewLocalSubscriber("", testLogger(), tss)
	subB.SetMatchers(topicMatchers([]string{"https://example.com/cluster-mh"}), nil)
	require.NoError(t, hubs[1].AddSubscriber(context.Background(), subB))

	require.NoError(t, hubs[0].Dispatch(context.Background(), &mercure.Update{
		Topics: []string{"https://example.com/cluster-mh"},
		Data:   "from-A-to-B",
	}))

	select {
	case msg := <-subB.Receive():
		assert.Equal(t, "from-A-to-B", msg.Data)
	case <-time.After(5 * time.Second):
		t.Fatal("hub B did not receive message from hub A on cluster")
	}
}

// TestReal_Cluster_PresenceAcrossPrimaries verifies that hub B's
// GetSubscribers (SCAN + MGET through ClusterClient) returns the presence
// written by both hubs; the keys share the {streamName} slot.
func TestReal_Cluster_PresenceAcrossPrimaries(t *testing.T) {
	addrs := probeRealRedisCluster(t)
	streamName := fmt.Sprintf("mercure-cluster-presence-%s-%d", t.Name(), time.Now().UnixNano())

	tss := testTopicMatcherStore()

	newHub := func() (*RedisTransport, *redis.ClusterClient) {
		client := redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:       addrs,
			DialTimeout: 2 * time.Second,
			ReadTimeout: 2 * time.Second,
		})

		tr, err := NewRedisTransport(
			client,
			WithLogger(testLogger()),
			WithStreamName(streamName),
			WithXReadBlock(50*time.Millisecond),
			WithHealthInterval(24*time.Hour),
			WithPresenceInterval(24*time.Hour),
			withSkipPresenceIntervalCheck(),
		)
		require.NoError(t, err)

		tr.SetTopicMatcherStore(tss)

		return tr, client
	}

	hubA, clientA := newHub()
	hubB, clientB := newHub()

	t.Cleanup(func() {
		_ = hubA.Close(context.Background())
		_ = hubB.Close(context.Background())

		cleanupClusterStreamKeys(clientA, streamName)

		_ = clientA.Close()
		_ = clientB.Close()
	})

	subA := mercure.NewLocalSubscriber("", testLogger(), tss)
	subA.SetMatchers(topicMatchers([]string{"https://example.com/cluster-presence"}), nil)
	require.NoError(t, hubA.AddSubscriber(context.Background(), subA))

	subB := mercure.NewLocalSubscriber("", testLogger(), tss)
	subB.SetMatchers(topicMatchers([]string{"https://example.com/cluster-presence"}), nil)
	require.NoError(t, hubB.AddSubscriber(context.Background(), subB))

	// Force an immediate presence write — the (24h-disabled) heartbeat ticker
	// would not publish in the test window.
	hubA.publishPresence(context.Background())
	hubB.publishPresence(context.Background())

	_, subs, err := hubB.GetSubscribers(context.Background())
	require.NoError(t, err)
	assert.Len(t, subs, 2,
		"hub B's GetSubscribers must enumerate both hubs' subscribers via ClusterClient.Scan")
}

// realRedisClusterTLSAddrs returns the TLS-port equivalents of the plain
// addresses (port + 1000), respecting REDIS_CLUSTER_TLS_ADDRS for override.
func realRedisClusterTLSAddrs(plainAddrs []string) []string {
	if env := os.Getenv("REDIS_CLUSTER_TLS_ADDRS"); env != "" {
		parts := strings.Split(env, ",")
		out := make([]string, 0, len(parts))

		for _, p := range parts {
			if p = strings.TrimSpace(p); p != "" {
				out = append(out, p)
			}
		}

		return out
	}

	out := make([]string, 0, len(plainAddrs))

	for _, a := range plainAddrs {
		// Convention from compose.cluster.yaml: TLS port = plain port + 1000.
		// Mechanical translation rather than re-parsing host:port pairs.
		out = append(out, strings.Replace(a, ":7", ":8", 1))
	}

	return out
}

// TestReal_Cluster_TLSEndToEnd verifies the transport over a real cluster's
// TLS ports (8000-8002) with `InsecureSkipVerify`, the pattern the README
// documents for clusters whose nodes do not share a SAN.
func TestReal_Cluster_TLSEndToEnd(t *testing.T) {
	plainAddrs := probeRealRedisCluster(t)
	tlsAddrs := realRedisClusterTLSAddrs(plainAddrs)

	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:       tlsAddrs,
		DialTimeout: 2 * time.Second,
		ReadTimeout: 2 * time.Second,
		// The compose stack's self-signed cert has SAN=IP:127.0.0.1.
		TLSConfig: &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS12}, //nolint:gosec // self-signed test fixture; pattern matches documented production guidance
	})

	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Skipf("TLS cluster plane not reachable (compose.cluster.yaml may need a refresh): %v", err)
	}

	transport, streamName := buildRealTransport(t, client)

	t.Cleanup(func() {
		_ = transport.Close(context.Background())

		cleanupClusterStreamKeys(client, streamName)

		_ = client.Close()
	})

	tss := testTopicMatcherStore()
	sub := mercure.NewLocalSubscriber("", testLogger(), tss)
	sub.SetMatchers(topicMatchers([]string{"https://example.com/cluster-tls"}), nil)

	require.NoError(t, transport.AddSubscriber(context.Background(), sub))

	require.NoError(t, transport.Dispatch(context.Background(), &mercure.Update{
		Topics: []string{"https://example.com/cluster-tls"},
		Data:   "hello tls cluster",
	}))

	select {
	case msg := <-sub.Receive():
		assert.Equal(t, "hello tls cluster", msg.Data)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for message from TLS cluster")
	}
}

// TestReal_Cluster_HistoryReplay verifies that XRANGE-driven history replay
// works through cluster slot routing. Publish N events, then add a
// subscriber with last-event-ID = first event's ID; the subscriber must
// receive events 2..N. Mirrors the single-node pagination tests but
// confirms the XRANGE path does not break on cluster topology — the
// hash-tag invariant means XRANGE on `{streamName}` lands on a single
// primary and behaves identically to single-node.
func TestReal_Cluster_HistoryReplay(t *testing.T) {
	transport, client := newRealClusterTransport(t)

	const total = 10

	publishedIDs := make([]string, 0, total)

	for i := range total {
		u := &mercure.Update{
			Topics: []string{"https://example.com/cluster-history"},
			Data:   fmt.Sprintf("msg-%02d", i),
		}
		require.NoError(t, transport.Dispatch(context.Background(), u))

		publishedIDs = append(publishedIDs, u.ID)
	}

	// Wait for the listener to have processed everything before AddSubscriber
	// snapshots toStreamID — otherwise Pass 1 (scanForEventID) sees a stale
	// tail and Pass 2 (replayAll) fires.
	waitForStreamCatchUp(t, transport, client)

	tss := testTopicMatcherStore()
	sub := mercure.NewLocalSubscriber(publishedIDs[0], testLogger(), tss)
	sub.SetMatchers(topicMatchers([]string{"https://example.com/cluster-history"}), nil)

	require.NoError(t, transport.AddSubscriber(context.Background(), sub))

	expected := total - 1 // events 1..total-1
	received := make([]string, 0, expected)

	for range expected {
		select {
		case msg := <-sub.Receive():
			received = append(received, msg.Data)
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out after %d/%d replayed events", len(received), expected)
		}
	}

	for i, data := range received {
		assert.Equal(t, fmt.Sprintf("msg-%02d", i+1), data,
			"replay sequence broken at index %d", i)
	}
}

// TestReal_Cluster_XTRIMMinID verifies that XTRIM MINID-driven TTL cleanup
// works through cluster slot routing. Same invariant as the single-node
// test — the XTRIM call lands on the {streamName} slot owner via cluster
// routing.
func TestReal_Cluster_XTRIMMinID(t *testing.T) {
	transport, client := newRealClusterTransport(t, WithEventTTL(10*time.Millisecond))

	for i := range 5 {
		require.NoError(t, transport.Dispatch(context.Background(), &mercure.Update{
			Topics: []string{"https://example.com/cluster-trim"},
			Data:   fmt.Sprintf("msg-%d", i),
		}))
	}

	time.Sleep(50 * time.Millisecond)
	transport.trimByTTL(context.Background())

	length, err := client.XLen(context.Background(), "{"+transport.opts.streamName+"}").Result()
	require.NoError(t, err)
	assert.Zero(t, length, "XTRIM MINID through cluster routing should drop all entries older than eventTTL")
}

// TestRealStress_Cluster_ConcurrentAddDispatchRemoveClose mirrors the
// single-node stress test but against a ClusterClient. go-redis's
// connection pool, cross-slot routing, and topology-refresh paths
// behave differently under load than standalone clients — this test
// catches regressions in those paths interacting with the transport's
// internal locking + shutdown sequence.
func TestRealStress_Cluster_ConcurrentAddDispatchRemoveClose(t *testing.T) {
	transport, _ := newRealClusterTransport(t, WithDispatchShards(4))
	runConcurrentStressLoop(t, transport)
}

// TestReal_Cluster_ZombieGC verifies that orphan-group cleanup works
// against cluster topology. Creates a synthetic zombie consumer group
// whose owning node's presence key does not exist; gcZombieGroups must
// destroy it via cluster-routed XGroupDestroy.
func TestReal_Cluster_ZombieGC(t *testing.T) {
	transport, client := newRealClusterTransport(t, WithZombieGCInterval(24*time.Hour))

	streamKey := "{" + transport.opts.streamName + "}"

	require.NoError(t, transport.Dispatch(context.Background(), &mercure.Update{
		Topics: []string{"https://example.com/cluster-zombie"},
		Data:   "bootstrap",
	}))

	require.NoError(t, client.XGroupCreate(context.Background(), streamKey,
		"mercure:node:zombie-cluster-test-node", "0").Err())

	transport.gcZombieGroups(context.Background(), streamKey)

	groups, err := client.XInfoGroups(context.Background(), streamKey).Result()
	require.NoError(t, err)

	for _, g := range groups {
		assert.NotEqual(t, "mercure:node:zombie-cluster-test-node", g.Name,
			"zombie group should have been destroyed via cluster-routed XGroupDestroy")
	}
}
