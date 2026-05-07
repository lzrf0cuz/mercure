//go:build real_redis

package redistransport

// This file contains tests that run against a real Redis/Valkey server. They
// exercise behaviors that miniredis cannot emulate faithfully:
//
//   - INFO server parsing (miniredis returns "section (server) is not supported")
//   - XTRIM MINID with millisecond-precision stream IDs
//   - Real consumer-group error surfaces (NOGROUP, "no such key")
//   - Production version-check flow without withSkipVersionCheck bypass
//
// To run:
//
//	task rt:redis:up      # or task rt:valkey:up
//	go test -tags=real_redis -race ./redistransport/...
//	task rt:redis:down
//
// REDIS_ADDR overrides the default localhost:6379.

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/dunglas/mercure"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// realRedisAddr returns the address from REDIS_ADDR, defaulting to localhost:6379.
// Callers should Skip if the server is unreachable so suites run cleanly when
// no Redis is available (e.g. on a laptop without the compose stack up).
func realRedisAddr() string {
	if addr := os.Getenv("REDIS_ADDR"); addr != "" {
		return addr
	}

	return "localhost:6379"
}

// newRealTransport creates a transport connected to the real single-node
// server, failing the test cleanly if the server is unreachable.
func newRealTransport(t *testing.T, opts ...Option) (*RedisTransport, *redis.Client) {
	t.Helper()

	addr := probeRealRedis(t)

	client := redis.NewClient(&redis.Options{
		Addr:        addr,
		DialTimeout: 2 * time.Second,
		ReadTimeout: 2 * time.Second,
	})

	transport, streamName := buildRealTransport(t, client, opts...)

	t.Cleanup(func() {
		_ = transport.Close(context.Background())

		cleanupStreamKeys(addr, streamName)

		_ = client.Close()
	})

	return transport, client
}

// TestReal_VersionValidationAcceptsModernServer verifies the production version
// check accepts a supported Redis/Valkey server via the real INFO command.
// If this test fails, either the server is below the minimum floor, or our
// parser broke.
func TestReal_VersionValidationAcceptsModernServer(t *testing.T) {
	transport, _ := newRealTransport(t)
	assert.NotNil(t, transport, "transport should initialize without withSkipVersionCheck")
}

// TestReal_EndToEndDispatchReceive verifies a full publish→XREADGROUP→local
// subscriber delivery path against a real server.
func TestReal_EndToEndDispatchReceive(t *testing.T) {
	transport, _ := newRealTransport(t)

	tss := testTopicSelectorStore()
	sub := mercure.NewLocalSubscriber("", testLogger(), tss)
	sub.SetTopics([]string{"https://example.com/real"}, nil)

	require.NoError(t, transport.AddSubscriber(context.Background(), sub))

	err := transport.Dispatch(context.Background(), &mercure.Update{
		Topics: []string{"https://example.com/real"},
		Event:  mercure.Event{Data: "hello real redis"},
	})
	require.NoError(t, err)

	select {
	case msg := <-sub.Receive():
		assert.Equal(t, "hello real redis", msg.Data)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for message from real server")
	}
}

// TestReal_XTRIMMinIDTrimsExpired verifies XTRIM MINID with a stream ID in the
// future causes the real server to drop entries. Miniredis implements this but
// with different precision semantics — this test confirms parity on real Redis.
func TestReal_XTRIMMinIDTrimsExpired(t *testing.T) {
	transport, client := newRealTransport(t, WithEventTTL(10*time.Millisecond))

	for i := range 5 {
		err := transport.Dispatch(context.Background(), &mercure.Update{
			Topics: []string{"https://example.com/trim"},
			Event:  mercure.Event{Data: fmt.Sprintf("msg-%d", i)},
		})
		require.NoError(t, err)
	}

	// Wait for TTL + a cleanup cycle. WithCleanupInterval defaults to 5m —
	// call the trim directly to avoid racing with the background goroutine.
	time.Sleep(50 * time.Millisecond)
	transport.trimByTTL(context.Background())

	length, err := client.XLen(context.Background(), "{"+transport.opts.streamName+"}").Result()
	require.NoError(t, err)
	assert.Zero(t, length, "XTRIM MINID should drop all entries older than eventTTL")
}

// TestReal_ZombieGCDestroysOrphanGroup verifies that a consumer group whose
// owning node's presence key is missing gets cleaned up by the GC pass.
func TestReal_ZombieGCDestroysOrphanGroup(t *testing.T) {
	transport, client := newRealTransport(t, WithZombieGCInterval(24*time.Hour))

	streamKey := "{" + transport.opts.streamName + "}"

	// Dispatch to ensure the stream exists so XGroupCreate can succeed.
	require.NoError(t, transport.Dispatch(context.Background(), &mercure.Update{
		Topics: []string{"https://example.com/zombie"},
		Event:  mercure.Event{Data: "bootstrap"},
	}))

	// Create a synthetic zombie group that no live node owns.
	require.NoError(t, client.XGroupCreate(context.Background(), streamKey,
		"mercure:node:zombie-test-node", "0").Err())

	// Run GC — the zombie's presence key doesn't exist, so the group is destroyed.
	transport.gcZombieGroups(context.Background(), streamKey)

	// The owning node's group should no longer be listed.
	groups, err := client.XInfoGroups(context.Background(), streamKey).Result()
	require.NoError(t, err)

	for _, g := range groups {
		assert.NotEqual(t, "mercure:node:zombie-test-node", g.Name,
			"zombie group should have been destroyed")
	}
}

// TestRealStress_ConcurrentAddDispatchRemoveClose hammers the transport
// with concurrent AddSubscriber/Dispatch/RemoveSubscriber while another
// goroutine issues Close.
//
// Stress tests intentionally run against a real Redis (via the real_redis
// tag + compose.redis.yaml / compose.valkey.yaml) rather than miniredis:
// miniredis's one-goroutine-per-connection model plus the race detector
// can't drain servePeer goroutines fast enough on high churn — the
// teardown hangs and goleak false-positives. Real Redis multiplexes I/O
// properly (epoll/kqueue) and handles connection churn without the test
// artifact.
func TestRealStress_ConcurrentAddDispatchRemoveClose(t *testing.T) {
	transport, _ := newRealTransport(t, WithDispatchShards(4))
	runConcurrentStressLoop(t, transport)
}
