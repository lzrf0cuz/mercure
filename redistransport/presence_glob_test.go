package redistransport

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/dunglas/mercure"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// isolatedStreamNames holds a stream name for each Redis glob metacharacter;
// "ab", which the unescaped patterns of "a?" and "a*" match; and "a" with
// "a}:presence:z", whose keys "a"'s presence pattern also matches.
var isolatedStreamNames = []string{"ab", "a?", "a*", "a[b]", `a\b`, "a", "a}:presence:z"}

// newStreamTransport builds a transport on client for stream with the
// test defaults and closes it at cleanup. The transport owns client.
func newStreamTransport(t *testing.T, client redis.UniversalClient, stream string) *RedisTransport {
	t.Helper()

	transport, err := NewRedisTransport(client,
		WithLogger(testLogger()),
		WithStreamName(stream),
		WithXReadBlock(50*time.Millisecond),
		WithHealthInterval(24*time.Hour),
		WithPresenceInterval(noHeartbeatInterval),
		withSkipPresenceIntervalCheck(),
		withSkipVersionCheck(),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = transport.Close(context.Background()) })

	return transport
}

// assertPresenceIsolatedPerStream starts one transport per name in
// isolatedStreamNames, prefixed with prefix, gives each one subscriber, and
// asserts that GetSubscribers on each lists only its own subscriber.
func assertPresenceIsolatedPerStream(t *testing.T, prefix string, newTransport func(stream string) *RedisTransport) {
	t.Helper()

	ctx := context.Background()
	tss := testTopicMatcherStore()

	transports := make([]*RedisTransport, len(isolatedStreamNames))
	want := make([]string, len(isolatedStreamNames))

	for i, name := range isolatedStreamNames {
		transports[i] = newTransport(prefix + name)

		sub := mercure.NewLocalSubscriber("", testLogger(), tss)
		sub.SetMatchers(topicMatchers([]string{"https://example.com/" + name}), nil)
		require.NoError(t, transports[i].AddSubscriber(ctx, sub))
		transports[i].publishPresence(ctx)

		want[i] = sub.ID
	}

	for i, tr := range transports {
		_, subs, err := tr.GetSubscribers(ctx)
		require.NoError(t, err, "stream %q", prefix+isolatedStreamNames[i])

		got := make([]string, 0, len(subs))
		for _, s := range subs {
			got = append(got, s.ID)
		}

		assert.Equal(t, []string{want[i]}, got, "stream %q must list only its own subscriber", prefix+isolatedStreamNames[i])
	}
}

// Glob characters or "}:presence:" in a stream name must not let
// GetSubscribers list another stream's subscribers from the same Redis.
func TestGetSubscribersIsolatedPerStream(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)

	assertPresenceIsolatedPerStream(t, "", func(stream string) *RedisTransport {
		return newStreamTransport(t, redis.NewClient(&redis.Options{Addr: mr.Addr()}), stream)
	})
}

// A Ring routes the presence SCAN by its MATCH pattern's hash tag, which the
// escaping changes, so the SCAN must be sent to the shard owning the stream.
func TestGetSubscribersIsolatedPerStreamOnRing(t *testing.T) {
	t.Parallel()

	addrs := map[string]string{"shard1": miniredis.RunT(t).Addr(), "shard2": miniredis.RunT(t).Addr()}
	newRing := func() *redis.Ring { return redis.NewRing(&redis.RingOptions{Addrs: addrs}) }

	// Precondition: for some name the escaped pattern's tag picks another
	// shard than the stream's keys, so a missing pin is caught.
	probe := newRing()

	t.Cleanup(func() { _ = probe.Close() })

	moved := false

	for _, name := range isolatedStreamNames {
		own, err := probe.GetShardClientForKey(key(name, ""))
		require.NoError(t, err)

		byPattern, err := probe.GetShardClientForKey(key(escapeRedisGlob(name), ":presence:*"))
		require.NoError(t, err)

		moved = moved || own != byPattern
	}

	require.True(t, moved, "no stream name's escaped pattern routes to another shard; change the shard names")

	assertPresenceIsolatedPerStream(t, "", func(stream string) *RedisTransport {
		return newStreamTransport(t, newRing(), stream)
	})
}

// scanCountingHook counts the SCAN commands sent through the client it is
// added to.
type scanCountingHook struct{ scans atomic.Int32 }

func (*scanCountingHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *scanCountingHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if cmd.Name() == "scan" {
			h.scans.Add(1)
		}

		return next(ctx, cmd)
	}
}

func (*scanCountingHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

// Only a stream name that escaping changes bypasses the Ring's own routing,
// and with it the Ring's retries; any other name's presence SCAN goes through
// the top-level client.
func TestPresenceScanPinnedOnlyForGlobStreamNames(t *testing.T) {
	t.Parallel()

	addrs := map[string]string{"shard1": miniredis.RunT(t).Addr(), "shard2": miniredis.RunT(t).Addr()}

	for _, tc := range []struct {
		stream   string
		topLevel bool
	}{
		{"mercure", true},
		{"a}:presence:z", true},
		{"a*", false},
	} {
		t.Run(tc.stream, func(t *testing.T) {
			t.Parallel()

			ring := redis.NewRing(&redis.RingOptions{Addrs: addrs})
			hook := &scanCountingHook{}
			ring.AddHook(hook)

			tr := newStreamTransport(t, ring, tc.stream)

			_, _, err := tr.GetSubscribers(context.Background())
			require.NoError(t, err)

			if tc.topLevel {
				assert.Positive(t, hook.scans.Load(), "the SCAN must go through the Ring")
			} else {
				assert.Zero(t, hook.scans.Load(), "the SCAN must go to the shard owning the stream")
			}
		})
	}
}
