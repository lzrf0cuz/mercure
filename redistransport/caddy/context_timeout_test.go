package caddy

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBuildClientOptions_ContextTimeoutEnabled pins that every client shape
// the module builds (single instance, cluster, sentinel) carries
// ContextTimeoutEnabled, so publish_timeout reaches an in-flight command.
func TestBuildClientOptions_ContextTimeoutEnabled(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		r    *Redis
	}{
		{"url", &Redis{URL: "redis://localhost:6379"}},
		{"single address", &Redis{Addresses: []string{"localhost:6379"}}},
		{"cluster", &Redis{Addresses: []string{"localhost:7000", "localhost:7001"}}},
		{"sentinel", &Redis{Addresses: []string{"localhost:26379"}, MasterName: "mymaster"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			uo, _, err := tc.r.buildClientOptions()
			require.NoError(t, err)
			assert.True(t, uo.ContextTimeoutEnabled)

			client := redis.NewUniversalClient(uo)

			t.Cleanup(func() { _ = client.Close() })

			switch c := client.(type) {
			case *redis.Client:
				assert.True(t, c.Options().ContextTimeoutEnabled)
			case *redis.ClusterClient:
				assert.True(t, c.Options().ContextTimeoutEnabled)
			default:
				t.Fatalf("unexpected client type %T", client)
			}
		})
	}
}

// TestBuildClientOptions_CommandCutAtContextDeadline drives a client built
// from the module's options against a server that never replies: the
// command must fail at the caller's 50ms deadline, not at read_timeout.
func TestBuildClientOptions_CommandCutAtContextDeadline(t *testing.T) {
	t.Parallel()

	const (
		budget      = 50 * time.Millisecond
		readTimeout = 400 * time.Millisecond
		slack       = 150 * time.Millisecond
	)

	var lc net.ListenConfig

	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	var wg sync.WaitGroup

	t.Cleanup(func() {
		_ = ln.Close()

		wg.Wait()
	})

	wg.Go(func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}

			wg.Go(func() {
				defer conn.Close()

				_, _ = io.Copy(io.Discard, conn)
			})
		}
	})

	r := &Redis{URL: "redis://" + ln.Addr().String(), ReadTimeout: "400ms", MaxRetries: -1}

	uo, _, err := r.buildClientOptions()
	require.NoError(t, err)

	client := redis.NewUniversalClient(uo)

	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithTimeout(t.Context(), budget)
	defer cancel()

	start := time.Now()
	err = client.Ping(ctx).Err()
	elapsed := time.Since(start)

	require.Error(t, err)
	assert.Less(t, elapsed, budget+slack,
		"the command must be cut at the context deadline, not at read_timeout (%v): %v", readTimeout, err)
}
