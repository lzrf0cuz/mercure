package redistransport

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Every collector the transport registers, the scrape-time pool and ready
// collectors included, carries the stream const label.
func TestEveryCollectorCarriesStreamLabel(t *testing.T) {
	t.Parallel()

	tr, _ := newTestTransport(t, WithStreamName("orders"), WithPrometheusRegisterer(prometheus.NewRegistry()))

	collectors := append(tr.metrics.Load().collectors(), tr.poolCollector, tr.readyCollector)

	descs := 0

	for _, c := range collectors {
		ch := make(chan *prometheus.Desc, 16)
		c.Describe(ch)
		close(ch)

		for d := range ch {
			descs++

			assert.Contains(t, d.String(), `stream="orders"`, "collector %T must carry the stream const label", c)
		}
	}

	assert.Greater(t, descs, len(collectors), "every collector must describe at least one metric")
}

// streamSeriesValues returns the value of each sample of family name on reg,
// keyed by its stream label.
func streamSeriesValues(t *testing.T, reg *prometheus.Registry, name string) map[string]float64 {
	t.Helper()

	families, err := reg.Gather()
	require.NoError(t, err)

	out := make(map[string]float64)

	for _, f := range families {
		if f.GetName() != name {
			continue
		}

		for _, m := range f.GetMetric() {
			var stream string

			for _, lp := range m.GetLabel() {
				if lp.GetName() == labelKeyStream {
					stream = lp.GetValue()
				}
			}

			if m.GetCounter() != nil {
				out[stream] = m.GetCounter().GetValue()
			} else {
				out[stream] = m.GetGauge().GetValue()
			}
		}
	}

	return out
}

// Two transports on different streams bound to one registry keep separate
// series: a counter, a pushed gauge, the ready gauge and the pool gauges each
// report their own transport.
func TestDifferentStreamsHaveIndependentSeries(t *testing.T) {
	t.Parallel()

	const aPings, bPings = 2000, 500

	ctx := context.Background()
	reg := prometheus.NewRegistry()
	a, _ := newTestTransport(t, WithStreamName("stream-a"), WithPrometheusRegisterer(reg))
	b, _ := newTestTransport(t, WithStreamName("stream-b"), WithPrometheusRegisterer(reg))

	addRebindSubscribers(t, a, 2)
	a.metrics.Load().healthy.Set(0)
	b.healthy.Store(false)

	for range aPings {
		require.NoError(t, a.client.Ping(ctx).Err())
	}

	for range bPings {
		require.NoError(t, b.client.Ping(ctx).Err())
	}

	assert.Equal(t, map[string]float64{"stream-a": 2, "stream-b": 0},
		streamSeriesValues(t, reg, metricSubscriberAddTotal), "counter")
	assert.Equal(t, map[string]float64{"stream-a": 0, "stream-b": 1},
		streamSeriesValues(t, reg, "mercure_redis_healthy"), "pushed gauge")
	assert.Equal(t, map[string]float64{"stream-a": 1, "stream-b": 0},
		streamSeriesValues(t, reg, metricReady), "ready gauge")

	hits := streamSeriesValues(t, reg, metricPoolHits)
	require.Len(t, hits, 2, "one pool series per stream")
	assert.GreaterOrEqual(t, hits["stream-a"], float64(aPings-1), "stream-a's pool series must read a's client")
	assert.GreaterOrEqual(t, hits["stream-b"], float64(bPings-1), "stream-b's pool series must read b's client")
	assert.Less(t, hits["stream-b"], float64(aPings), "stream-b's pool series must not read a's client")
}
