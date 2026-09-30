package redistransport

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/dunglas/mercure"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errRedisWriteDown = errors.New("injected: redis write failed")

// writesDownHook returns a hook that fails the presence SET and the publish
// script while on, as a Redis that stops accepting this node's writes does.
func writesDownHook() *cmdErrHook {
	return &cmdErrHook{errs: map[string]error{
		"set": errRedisWriteDown, "eval": errRedisWriteDown, "evalsha": errRedisWriteDown,
	}}
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
// in every node's subscription API. It lasts until the next successful
// heartbeat, and at most presence_ttl after the last successful heartbeat.
// The failed active:false update is not retried: the stream keeps only the
// active:true event.
func TestFailedWithdrawalAfterAddIsBoundedByPresenceTTL(t *testing.T) {
	t.Parallel()

	// newZombieGCTransport sets this presence TTL.
	const presenceTTL = time.Minute

	for _, heartbeatRecovers := range []bool{true, false} {
		name := "heartbeat keeps failing"
		if heartbeatRecovers {
			name = "heartbeat recovers"
		}

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			mr := miniredis.RunT(t)
			hook := writesDownHook()
			subscriberNode, otherNode := newZombieGCTransport(t, mr, hook), newZombieGCTransport(t, mr)

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

// dialTimeoutError returns the error a TCP dial returns when its deadline
// passes: a *net.OpError that matches context.DeadlineExceeded through the net
// package's timeout error. go-redis returns it when Redis is unreachable.
func dialTimeoutError(t *testing.T) error {
	t.Helper()

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", "127.0.0.1:1")
	if conn != nil {
		_ = conn.Close()
	}

	var opErr *net.OpError
	require.ErrorAs(t, err, &opErr)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.NotErrorIs(t, err, context.Canceled)

	return err
}

// presenceErrorCase is one presence command failure: a dial timeout (counted,
// logged at Error) or a cancelled context at shutdown (not counted, no Error
// log).
type presenceErrorCase struct {
	name    string
	timeout bool
}

var presenceErrorCases = []presenceErrorCase{{"dial timeout", true}, {"shutdown cancellation", false}}

// run sets up a transport whose cmd command fails as the case says and calls
// call with the context to use. It returns the transport's metrics and logs.
func (tc presenceErrorCase) run(t *testing.T, cmd string, call func(*RedisTransport, context.Context)) (*Metrics, *safeBuffer) {
	t.Helper()

	buf := &safeBuffer{}
	ctx := context.Background()
	hook := &cmdErrHook{}

	if tc.timeout {
		hook.errs = map[string]error{cmd: dialTimeoutError(t)}
	} else {
		cancelled, cancel := context.WithCancel(ctx)
		cancel()

		ctx = cancelled
	}

	tr := newHookedTestTransport(t, []redis.Hook{hook},
		WithLogger(jsonTestLogger(buf)), WithPrometheusRegisterer(prometheus.NewRegistry()))
	hook.on.Store(true)

	call(tr, ctx)

	return tr.metrics.Load(), buf
}

// The heartbeat ctx carries no deadline, so a heartbeat SET that fails with
// context.DeadlineExceeded is a Redis timeout, such as a dial timeout during an
// outage. It is counted and logged at Error. Only a cancelled context (the
// transport shutting down) is logged at Debug and not counted.
func TestPresenceHeartbeatDialTimeoutIsCounted(t *testing.T) {
	t.Parallel()

	for _, tc := range presenceErrorCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			m, buf := tc.run(t, "set", func(tr *RedisTransport, ctx context.Context) { tr.publishPresence(ctx) })

			failed := logRecords(t, buf, "redis transport: presence heartbeat failed")
			cancelled := logRecords(t, buf, "redis transport: presence heartbeat cancelled (shutdown)")

			if tc.timeout {
				assert.InDelta(t, 1, counterValueForTest(t, m.presenceHeartbeatErrors), 0)
				require.Len(t, failed, 1)
				assert.Equal(t, "ERROR", failed[0]["level"])
				assert.Empty(t, cancelled)
			} else {
				assert.Zero(t, counterValueForTest(t, m.presenceHeartbeatErrors))
				assert.Empty(t, failed)
				require.Len(t, cancelled, 1)
				assert.Equal(t, "DEBUG", cancelled[0]["level"])
			}
		})
	}
}

// GetSubscribers' ctx is the request's, and the hub sets no deadline on it, so
// an MGET that fails with context.DeadlineExceeded is usually a Redis timeout
// such as a dial timeout. It is counted once per key and logged at Error, as is
// the expiry of a deadline an operator adds to the request. A cancelled context
// is returned without either.
func TestPresenceMGetDialTimeoutIsCounted(t *testing.T) {
	t.Parallel()

	for _, tc := range presenceErrorCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			m, buf := tc.run(t, "mget", func(tr *RedisTransport, ctx context.Context) {
				_, err := tr.mgetPresenceValues(ctx, []string{"a", "b"})
				require.Error(t, err)
			})

			got := counterValueForTest(t, m.presenceReadErrors.WithLabelValues(string(presenceReadErrorGet)))
			failed := logRecords(t, buf, "redis transport: failed to MGET presence keys")

			if tc.timeout {
				assert.InDelta(t, 2, got, 0)
				require.Len(t, failed, 1)
				assert.Equal(t, "ERROR", failed[0]["level"])
			} else {
				assert.Zero(t, got)
				assert.Empty(t, failed)
			}
		})
	}
}

// A health sample holds its gauge's last value only when the health cycle's
// budget has expired. A dial timeout also matches context.DeadlineExceeded,
// but while the budget is live it is a failed sample: it logs a Warn and, for
// the gauges that have one, sets -1.
func TestHealthSampleDialTimeoutInsideBudgetIsAFailure(t *testing.T) {
	t.Parallel()

	const held = 42.0

	samples := []struct {
		cmd   string
		warn  string
		gauge func(*Metrics) prometheus.Gauge
		// failed is the gauge value after a failed sample: -1, or held for XLEN,
		// which only logs.
		failed float64
	}{
		{
			"xlen", "redis transport: failed to sample stream length during health check",
			func(m *Metrics) prometheus.Gauge { return m.streamLength }, held,
		},
		{
			"xinfo", "redis transport: failed to sample consumer-group lag",
			func(m *Metrics) prometheus.Gauge { return m.consumerGroupsCount }, -1,
		},
		{
			"xrange", "redis transport: failed to sample history-window head",
			func(m *Metrics) prometheus.Gauge { return m.historyWindowSeconds }, -1,
		},
		{
			"xrevrange", "redis transport: failed to sample history-window tail",
			func(m *Metrics) prometheus.Gauge { return m.historyWindowSeconds }, -1,
		},
	}

	for _, sample := range samples {
		for _, expired := range []bool{false, true} {
			name := sample.cmd + "/budget live"
			if expired {
				name = sample.cmd + "/budget expired"
			}

			t.Run(name, func(t *testing.T) {
				t.Parallel()

				buf := &safeBuffer{}
				hook := &cmdErrHook{errs: map[string]error{sample.cmd: dialTimeoutError(t)}, waitDone: expired}
				tr := newHookedTestTransport(t, []redis.Hook{hook},
					WithLogger(jsonTestLogger(buf)), WithPrometheusRegisterer(prometheus.NewRegistry()))

				// The tail sample runs only when the stream has an entry.
				require.NoError(t, tr.Dispatch(t.Context(), &mercure.Update{
					Topics: []string{"https://example.com/health"},
					Data:   "x",
				}))

				m := tr.metrics.Load()
				require.NotNil(t, m)
				sample.gauge(m).Set(held)

				// Live: the parent has no deadline, so the budget is the cycle's own
				// (5s, as health_interval is 24h), and the immediate failure cannot
				// outlast it. Expired: a short parent deadline becomes the budget, and
				// the hook fails the command only once it has expired.
				ctx := t.Context()

				if expired {
					var cancel context.CancelFunc

					ctx, cancel = context.WithTimeout(ctx, 200*time.Millisecond)
					defer cancel()
				}

				hook.on.Store(true)

				assert.Equal(t, 0, tr.runHealthPing(ctx, 0), "PING is not failed")

				warns := logRecords(t, buf, sample.warn)

				if expired {
					assert.InDelta(t, held, gaugeValue(t, sample.gauge(m)), 0, "an expired budget holds the last value")
					assert.Empty(t, warns)
				} else {
					assert.InDelta(t, sample.failed, gaugeValue(t, sample.gauge(m)), 0)
					require.Len(t, warns, 1)
					assert.Equal(t, "WARN", warns[0]["level"])
				}
			})
		}
	}
}
