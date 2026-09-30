//go:build real_redis

package redistransport

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/dunglas/mercure"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	realPublishTimeout = 100 * time.Millisecond
	realReadTimeout    = 2 * time.Second
	realPauseMs        = "1500"
	realSlack          = 250 * time.Millisecond
)

// runPausedPublishTimeout stalls the publish script with CLIENT PAUSE WRITE
// (EVAL/EVALSHA are paused as writes) and checks that a client with
// ContextTimeoutEnabled gives up at publish_timeout rather than at
// read_timeout, reporting the deadline, and that the hub answers 504.
// Not parallel: the pause stalls every client of the shared server.
func runPausedPublishTimeout(t *testing.T, transport *RedisTransport, pause, unpause func(context.Context) error) {
	t.Helper()

	t.Cleanup(func() { _ = unpause(context.Background()) })

	// Warm up: loads the script and opens a pooled connection.
	require.NoError(t, transport.Dispatch(t.Context(), &mercure.Update{
		Topics: []string{"https://example.com/publish-timeout"},
		Data:   "warm-up",
	}))

	hub := newStalledHub(t, transport, realPublishTimeout)

	require.NoError(t, pause(t.Context()))

	elapsed, err := stalledDispatch(t, transport, realPublishTimeout)

	require.NoError(t, unpause(t.Context()))
	require.ErrorIs(t, err, context.DeadlineExceeded, "a publish cut at publish_timeout must report the deadline: %v", err)
	assert.Less(t, elapsed, realPublishTimeout+realSlack,
		"Dispatch must return at publish_timeout, not at read_timeout (%v)", realReadTimeout)

	require.NoError(t, pause(t.Context()))

	code, elapsed := hubPublish(t, hub)

	require.NoError(t, unpause(t.Context()))
	assert.Equal(t, http.StatusGatewayTimeout, code)
	assert.Less(t, elapsed, realPublishTimeout+realSlack,
		"the 504 must arrive at publish_timeout, not at read_timeout (%v)", realReadTimeout)
}

func TestReal_PublishTimeoutCutsPausedCommand(t *testing.T) {
	for _, tc := range []struct {
		name       string
		maxRetries int
	}{
		{"default retries", 0},
		{"retries disabled", -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			addr := probeRealRedis(t)

			client := redis.NewClient(&redis.Options{
				Addr:                  addr,
				DialTimeout:           2 * time.Second,
				ReadTimeout:           realReadTimeout,
				MaxRetries:            tc.maxRetries,
				ContextTimeoutEnabled: true,
			})

			transport, streamName := buildRealTransport(t, client)

			t.Cleanup(func() {
				_ = transport.Close(context.Background())

				cleanupStreamKeys(addr, streamName)

				_ = client.Close()
			})

			probe := newProbeClient(t, addr)

			runPausedPublishTimeout(
				t, transport,
				func(ctx context.Context) error { return probe.Do(ctx, "CLIENT", "PAUSE", realPauseMs, "WRITE").Err() },
				func(ctx context.Context) error { return probe.Do(ctx, "CLIENT", "UNPAUSE").Err() },
			)
		})
	}
}

func TestReal_Cluster_PublishTimeoutCutsPausedCommand(t *testing.T) {
	for _, tc := range []struct {
		name       string
		maxRetries int
	}{
		{"default retries", 0},
		{"retries disabled", -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			addrs := probeRealRedisCluster(t)

			client := redis.NewClusterClient(&redis.ClusterOptions{
				Addrs:                 addrs,
				DialTimeout:           2 * time.Second,
				ReadTimeout:           realReadTimeout,
				MaxRetries:            tc.maxRetries,
				ContextTimeoutEnabled: true,
			})

			transport, streamName := buildRealTransport(t, client)

			t.Cleanup(func() {
				_ = transport.Close(context.Background())

				cleanupClusterStreamKeys(client, streamName)

				_ = client.Close()
			})

			probe := redis.NewClusterClient(&redis.ClusterOptions{Addrs: addrs, DialTimeout: 2 * time.Second})

			t.Cleanup(func() { _ = probe.Close() })

			eachMaster := func(args ...any) func(context.Context) error {
				return func(ctx context.Context) error {
					return probe.ForEachMaster(ctx, func(ctx context.Context, c *redis.Client) error {
						return c.Do(ctx, args...).Err()
					})
				}
			}

			runPausedPublishTimeout(
				t, transport,
				eachMaster("CLIENT", "PAUSE", realPauseMs, "WRITE"),
				eachMaster("CLIENT", "UNPAUSE"),
			)
		})
	}
}
