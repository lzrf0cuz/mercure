package redistransport

import (
	"context"
	"io"
	"log/slog"
	"net"
	"os"
	"testing"
	"time"

	"github.com/dunglas/mercure"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDeadlineCutError pins which errors deadlineCutError translates: only a
// socket deadline error on a ctx whose deadline expired, and the result wraps
// both context.DeadlineExceeded and the original error.
func TestDeadlineCutError(t *testing.T) {
	t.Parallel()

	ioTimeout := &net.OpError{Op: "read", Net: "tcp", Err: os.ErrDeadlineExceeded}

	expired, cancelExpired := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	t.Cleanup(cancelExpired)

	live, cancelLive := context.WithTimeout(context.Background(), time.Hour)
	t.Cleanup(cancelLive)

	cancelled, cancel := context.WithTimeout(context.Background(), time.Hour)
	cancel()

	t.Run("socket deadline on an expired ctx wraps both", func(t *testing.T) {
		t.Parallel()

		got := deadlineCutError(expired, ioTimeout)
		require.ErrorIs(t, got, context.DeadlineExceeded)
		require.ErrorIs(t, got, os.ErrDeadlineExceeded)
		require.ErrorIs(t, got, ioTimeout)
	})

	ctxs := map[string]context.Context{
		"live":        live,
		"no deadline": context.Background(),
		"cancelled":   cancelled,
		"expired":     expired,
	}

	for _, tc := range []struct {
		name, ctx string
		err       error
	}{
		{"socket deadline on a live ctx", "live", ioTimeout},
		{"socket deadline on a ctx without deadline", "no deadline", ioTimeout},
		{"socket deadline on a cancelled ctx", "cancelled", ioTimeout},
		{"EOF on an expired ctx", "expired", io.EOF},
		{"redis nil reply on an expired ctx", "expired", redis.Nil},
		{"closed client on an expired ctx", "expired", redis.ErrClosed},
	} {
		t.Run(tc.name+" is unchanged", func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.err, deadlineCutError(ctxs[tc.ctx], tc.err))
		})
	}
}

// neverDoneCtx reports a deadline in the past but is never done: Done is nil
// and Err is nil. deadlineCutError's wait for ctx must be bounded for it.
type neverDoneCtx struct{}

func (neverDoneCtx) Deadline() (time.Time, bool) { return time.Now().Add(-time.Second), true }
func (neverDoneCtx) Done() <-chan struct{}       { return nil }
func (neverDoneCtx) Err() error                  { return nil }
func (neverDoneCtx) Value(any) any               { return nil }

// TestDispatchNeverDoneContextReturnsPromptly drives Dispatch with a ctx
// whose deadline has passed but which never reports done. The socket write
// fails at once on the past deadline; Dispatch must return promptly with the
// untranslated socket error rather than wait on Done.
func TestDispatchNeverDoneContextReturnsPromptly(t *testing.T) {
	t.Parallel()

	transport, _ := newStalledTransport(t, stallReadTimeout, -1, scriptCmds)

	errc := make(chan error, 1)
	start := time.Now()

	go func() {
		errc <- transport.Dispatch(neverDoneCtx{}, &mercure.Update{
			Topics: []string{"https://example.com/publish-timeout"},
			Data:   "payload",
		})
	}()

	select {
	case err := <-errc:
		assert.Less(t, time.Since(start), stallSlack, "Dispatch must not wait on a Done that never closes")
		require.ErrorIs(t, err, os.ErrDeadlineExceeded)
		require.NotErrorIs(t, err, context.DeadlineExceeded, "an unexpired ctx must not be reported as the deadline")
	case <-time.After(2 * time.Second):
		t.Fatal("Dispatch hung on a context whose Done never closes")
	}
}

func gaugeValue(t *testing.T, g prometheus.Gauge) float64 {
	t.Helper()

	var m dto.Metric
	require.NoError(t, g.Write(&m))

	return m.GetGauge().GetValue()
}

// TestHealthSampleCutAtBudgetHoldsLastValue stalls the XLEN, XINFO GROUPS or
// history-window XRANGE/XREVRANGE reply of a health cycle past its budget, with retries
// disabled so go-redis returns the socket deadline error rather than
// ctx.Err(). The cut must take the same hold-last-value path as with retries
// on: no WARN, gauges unchanged, the stalled sample and every later one cut
// at the budget.
// The budget comes from a parent ctx deadline (healthCtx inherits the shorter
// one), which keeps it far below the 1s floor on health_interval.
func TestHealthSampleCutAtBudgetHoldsLastValue(t *testing.T) {
	t.Parallel()

	const (
		budget      = 150 * time.Millisecond
		readTimeout = time.Second
	)

	for _, cmd := range []string{"xlen", "xinfo", "xrange", "xrevrange"} {
		t.Run(cmd, func(t *testing.T) {
			t.Parallel()

			logs := &safeBuffer{}
			logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelWarn}))

			transport, proxy := newStalledTransport(t, readTimeout, -1, []string{cmd},
				WithLogger(logger), WithPrometheusRegisterer(prometheus.NewRegistry()))

			require.NoError(t, transport.Dispatch(t.Context(), &mercure.Update{
				Topics: []string{"https://example.com/health"},
				Data:   "payload",
			}))

			m := transport.metrics.Load()
			require.NotNil(t, m)
			m.streamLength.Set(42)
			m.consumerGroupsCount.Set(7)
			m.historyWindowSeconds.Set(99)

			proxy.stall.Store(int64(stallReplyDelay))

			ctx, cancel := context.WithTimeout(t.Context(), budget)
			defer cancel()

			start := time.Now()

			assert.Equal(t, 0, transport.runHealthPing(ctx, 0), "PING is not stalled")
			assert.Less(t, time.Since(start), readTimeout, "the stalled sample must be cut at the budget")

			if cmd == "xlen" {
				assert.InDelta(t, 42.0, gaugeValue(t, m.streamLength), 0, "stream length must hold its last value")
			}

			if cmd == "xlen" || cmd == "xinfo" {
				assert.InDelta(t, 7.0, gaugeValue(t, m.consumerGroupsCount), 0, "consumer groups must hold their last value")
			}

			assert.InDelta(t, 99.0, gaugeValue(t, m.historyWindowSeconds), 0, "history window must hold its last value")

			out := logs.String()
			assert.NotContains(t, out, "failed to sample stream length")
			assert.NotContains(t, out, "failed to sample consumer-group lag")
			assert.NotContains(t, out, "failed to sample history-window")
		})
	}
}

// TestHealthHistoryWindowSampleCutAtCycleBudget stalls the history-window
// XRANGE past the health cycle's own budget (health_interval at the 1s floor),
// under a parent ctx without a deadline, as the health loop's is. The sample
// must be cut at that budget, not run on to the reply or read_timeout.
func TestHealthHistoryWindowSampleCutAtCycleBudget(t *testing.T) {
	t.Parallel()

	const readTimeout = 3 * time.Second

	logs := &safeBuffer{}
	logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelWarn}))

	transport, proxy := newStalledTransport(t, readTimeout, -1, []string{"xrange"},
		WithLogger(logger), WithPrometheusRegisterer(prometheus.NewRegistry()),
		WithHealthInterval(minHealthPingTimeout))

	require.NoError(t, transport.Dispatch(t.Context(), &mercure.Update{
		Topics: []string{"https://example.com/health"},
		Data:   "payload",
	}))

	proxy.stall.Store(int64(stallReplyDelay))

	start := time.Now()

	assert.Equal(t, 0, transport.runHealthPing(t.Context(), 0), "PING is not stalled")
	assert.Less(t, time.Since(start), minHealthPingTimeout+stallSlack,
		"the stalled history-window sample must be cut at the health cycle's budget")
	assert.NotContains(t, logs.String(), "failed to sample history-window")
}
