package redistransport

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errTimeDisabled = errors.New("ERR TIME disabled")

// failTimeHook fails the TIME command, as a server that disallows it does.
type failTimeHook struct{}

func (failTimeHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (failTimeHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if cmd.Name() == "time" {
			cmd.SetErr(errTimeDisabled)

			return errTimeDisabled
		}

		return next(ctx, cmd)
	}
}

func (failTimeHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

// newUnboundTransport builds a transport on mr without a registerer, as the
// Caddy module does: its metrics are bound only by a later RegisterMetricsWith.
func newUnboundTransport(t *testing.T, mr *miniredis.Miniredis, hooks ...redis.Hook) *RedisTransport {
	t.Helper()

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	for _, h := range hooks {
		client.AddHook(h)
	}

	transport, err := NewRedisTransport(client,
		WithLogger(testLogger()),
		WithXReadBlock(50*time.Millisecond),
		WithHealthInterval(24*time.Hour),
		WithPresenceInterval(24*time.Hour),
		withSkipPresenceIntervalCheck(),
		withSkipVersionCheck(),
	)
	require.NoError(t, err)
	t.Cleanup(func() { transport.Close(context.Background()) })

	return transport
}

// registeredValue returns the value of the single series of the named counter
// or gauge on reg, failing when reg does not carry it.
func registeredValue(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()

	families, err := reg.Gather()
	require.NoError(t, err)

	for _, f := range families {
		if f.GetName() != name {
			continue
		}

		require.Len(t, f.GetMetric(), 1)

		m := f.GetMetric()[0]
		if m.GetCounter() != nil {
			return m.GetCounter().GetValue()
		}

		return m.GetGauge().GetValue()
	}

	require.FailNow(t, "metric not registered", name)

	return 0
}

// Starting a transport and binding its metrics afterwards, as the Caddy module
// does, must not lose the startup clock-drift sample: it is recorded at the
// bind, so the gauge still reports it.
func TestStartupThenBindRecordsClockDrift(t *testing.T) {
	t.Parallel()

	const serverBehind = 10 * time.Second

	t.Run("drift measured", func(t *testing.T) {
		t.Parallel()

		mr := miniredis.RunT(t)
		mr.SetTime(time.Now().Add(-serverBehind))

		transport := newUnboundTransport(t, mr)

		reg := prometheus.NewRegistry()
		require.NoError(t, transport.RegisterMetricsWith(reg))

		assert.InDelta(t, serverBehind.Seconds(), registeredValue(t, reg, metricClockDriftSeconds), 5,
			"the bind must record the startup clock-drift sample")
	})

	t.Run("drift unknown", func(t *testing.T) {
		t.Parallel()

		transport := newUnboundTransport(t, miniredis.RunT(t), failTimeHook{})

		reg := prometheus.NewRegistry()
		require.NoError(t, transport.RegisterMetricsWith(reg))

		assert.True(t, math.IsNaN(registeredValue(t, reg, metricClockDriftSeconds)),
			"a failed TIME query must leave the gauge NaN, not a healthy-looking 0")
	})
}
