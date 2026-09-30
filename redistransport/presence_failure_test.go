package redistransport

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/dunglas/mercure"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errRedisWriteDown = errors.New("injected: redis write failed")

// failWritesHook fails the presence SET and the publish script while on, as a
// Redis that stops accepting this node's writes does.
type failWritesHook struct{ on atomic.Bool }

func (*failWritesHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *failWritesHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		switch cmd.Name() {
		case "set", "eval", "evalsha":
			if h.on.Load() {
				cmd.SetErr(errRedisWriteDown)

				return errRedisWriteDown
			}
		}

		return next(ctx, cmd)
	}
}

func (*failWritesHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

// subscriptionEvents returns the data of the subscription events in the stream.
func subscriptionEvents(t *testing.T, transport *RedisTransport) []string {
	t.Helper()

	entries, err := transport.client.XRange(t.Context(), transport.key(""), "-", "+").Result()
	require.NoError(t, err)

	var events []string

	for _, e := range entries {
		u := decodeStreamEntry(e, *transport.codec.Load(), transport.logger, nil, nil)
		require.NotNil(t, u)

		if strings.HasPrefix(u.Topics[0], "/.well-known/mercure/subscriptions/") {
			events = append(events, u.Data)
		}
	}

	return events
}

// A subscriber leaves while Redis refuses its node's writes: RemoveSubscriber
// (local only) succeeds, but the hub's active:false subscription update and
// the presence heartbeat fail. The stale presence entry keeps the subscriber
// in every node's subscription API until the next successful heartbeat, and
// at most until presence_ttl after the last one. The failed active:false
// update is not retried: the stream keeps only the active:true event.
func TestFailedWithdrawalAfterAddIsBoundedByPresenceTTL(t *testing.T) {
	t.Parallel()

	const presenceTTL = 60 * time.Second

	for _, heartbeatRecovers := range []bool{true, false} {
		name := "heartbeat keeps failing"
		if heartbeatRecovers {
			name = "heartbeat recovers"
		}

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			mr := miniredis.RunT(t)
			hook := &failWritesHook{}

			newNode := func(hooks ...redis.Hook) *RedisTransport {
				client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
				for _, h := range hooks {
					client.AddHook(h)
				}

				transport, err := NewRedisTransport(client,
					withSkipVersionCheck(),
					WithLogger(testLogger()),
					WithXReadBlock(50*time.Millisecond),
					WithHealthInterval(24*time.Hour),
					WithPresenceInterval(24*time.Hour),
					WithPresenceTTL(presenceTTL),
					withSkipPresenceIntervalCheck(),
					WithZombieGCInterval(0),
				)
				require.NoError(t, err)
				t.Cleanup(func() { transport.Close(context.Background()) })

				return transport
			}

			subscriberNode, otherNode := newNode(hook), newNode()

			var logs safeBuffer

			hub, err := mercure.NewHub(
				t.Context(),
				mercure.WithTransport(subscriberNode),
				mercure.WithAnonymous(),
				mercure.WithSubscriptions(),
				mercure.WithLogger(slog.New(slog.NewJSONHandler(&logs, nil))),
			)
			require.NoError(t, err)

			srv := httptest.NewServer(hub)
			t.Cleanup(srv.Close)

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			req, err := http.NewRequestWithContext(ctx, http.MethodGet,
				srv.URL+"/.well-known/mercure?"+url.Values{"match": {"https://example.com/withdrawal"}}.Encode(), nil)
			require.NoError(t, err)

			resp, err := srv.Client().Do(req)
			require.NoError(t, err)

			defer resp.Body.Close()

			require.Equal(t, http.StatusOK, resp.StatusCode)
			require.Eventually(t, func() bool { return len(subscriptionEvents(t, subscriberNode)) == 1 },
				5*time.Second, 10*time.Millisecond, "the active:true subscription update must be published")
			require.Contains(t, subscriptionEvents(t, subscriberNode)[0], `"active":true`)

			subscriberNode.publishPresence(t.Context())

			listed := func() int {
				_, subs, err := otherNode.GetSubscribers(t.Context())
				require.NoError(t, err)

				return len(subs)
			}
			require.Equal(t, 1, listed(), "the other node must list the subscriber")

			// Redis now refuses the subscriber node's writes; the client leaves.
			hook.on.Store(true)
			cancel()

			require.Eventually(t, func() bool {
				return subscriberNode.subscriberCount.Load() == 0 && strings.Contains(logs.String(), "Failed to dispatch update")
			}, 5*time.Second, 10*time.Millisecond, "the removal must succeed and the active:false update fail")

			subscriberNode.publishPresence(t.Context())
			assert.Equal(t, 1, listed(), "a failed heartbeat leaves the departed subscriber listed")

			if heartbeatRecovers {
				hook.on.Store(false)
				subscriberNode.publishPresence(t.Context())
				assert.Zero(t, listed(), "the next successful heartbeat clears the subscriber")
			} else {
				mr.FastForward(presenceTTL)
				assert.Zero(t, listed(), "presence_ttl after the last successful heartbeat, the entry expires")
			}

			assert.Len(t, subscriptionEvents(t, subscriberNode), 1,
				"the failed active:false update is not retried: only active:true is in the stream")
		})
	}
}
