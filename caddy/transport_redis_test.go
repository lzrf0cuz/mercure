package caddy

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/dunglas/mercure"
	"github.com/lzrf0cuz/mercure/redistransport"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newRealRedisTransportForTest returns a *redistransport.RedisTransport backed by
// miniredis. The version and presence-interval checks are skipped: miniredis does
// not implement INFO server, and the test intervals are below the production floor.
func newRealRedisTransportForTest(t *testing.T) *redistransport.RedisTransport {
	t.Helper()

	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	transport, err := redistransport.NewRedisTransport(
		client,
		redistransport.WithSkipVersionCheckForTests(),
		redistransport.WithSkipPresenceIntervalCheckForTests(),
		redistransport.WithXReadBlock(50*time.Millisecond),
		redistransport.WithHealthInterval(24*time.Hour),
		redistransport.WithPresenceInterval(24*time.Hour),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, transport.Close(context.Background()))
	})

	return transport
}

// metricFamiliesContain reports whether reg exposes a metric family whose name
// starts with prefix.
func metricFamiliesContain(t *testing.T, reg *prometheus.Registry, prefix string) bool {
	t.Helper()

	mfs, err := reg.Gather()
	require.NoError(t, err)

	for _, mf := range mfs {
		if strings.HasPrefix(mf.GetName(), prefix) {
			return true
		}
	}

	return false
}

// TestBindTransportMetricsRealRedisTransport binds a real RedisTransport, so its
// RegisterMetricsWith runs against a Prometheus registry.
func TestBindTransportMetricsRealRedisTransport(t *testing.T) {
	t.Parallel()

	transport := newRealRedisTransportForTest(t)
	reg := prometheus.NewRegistry()

	require.NoError(t, bindTransportMetrics(transport, reg, nil),
		"bindTransportMetrics must drive real RedisTransport.RegisterMetricsWith without error")

	assert.True(t, metricFamiliesContain(t, reg, "mercure_redis_"),
		"mercure_redis_* metric families must appear on the registry after bindTransportMetrics")
}

// TestBindTransportMetricsRealRedisTransportReload binds the same transport to the
// same registry twice, as when two hub directives in one config load share a pooled
// transport. The second bind must be a no-op that keeps the registered collectors.
func TestBindTransportMetricsRealRedisTransportReload(t *testing.T) {
	t.Parallel()

	transport := newRealRedisTransportForTest(t)
	reg := prometheus.NewRegistry()

	require.NoError(t, bindTransportMetrics(transport, reg, nil),
		"first bindTransportMetrics call must succeed")

	// Drive a counter above zero, so a repeat bind that re-created or reset
	// the collectors shows up as a value going back to zero.
	tms, err := mercure.NewTopicMatcherStore(0)
	require.NoError(t, err)

	s := mercure.NewLocalSubscriber("", slog.New(slog.DiscardHandler), tms)
	s.SetMatchers([]mercure.TopicMatcher{{Type: mercure.MatcherTypeExact, Pattern: "https://example.com/rebind"}}, nil)
	require.NoError(t, transport.AddSubscriber(context.Background(), s))

	mfsBefore, err := reg.Gather()
	require.NoError(t, err)

	require.InDelta(t, 1, subscriberAddTotal(t, mfsBefore), 0, "the subscriber add must be counted before the repeat bind")

	require.NoError(t, bindTransportMetrics(transport, reg, nil),
		"a second bind of the same transport to the same registry must be a no-op")

	mfsAfter, err := reg.Gather()
	require.NoError(t, err)

	// The family names must not change across the second bind. Values may: the
	// pool-stats gauges move between the two Gather calls.
	namesBefore := metricFamilyNames(mfsBefore)
	namesAfter := metricFamilyNames(mfsAfter)
	assert.Equal(t, namesBefore, namesAfter,
		"no metric family may appear on a repeated bind")

	// build_info's labels are set once at construction, so a collector replaced on
	// the second bind would expose no label set.
	buildInfoBefore := buildInfoLabelSets(mfsBefore)
	buildInfoAfter := buildInfoLabelSets(mfsAfter)

	require.NotEmpty(t, buildInfoBefore, "first Gather must include mercure_redis_build_info")
	assert.Equal(t, buildInfoBefore, buildInfoAfter,
		"build_info label set must persist across the second bind")

	assert.InDelta(t, 1, subscriberAddTotal(t, mfsAfter), 0,
		"a counter must keep its value across a repeat bind to the same registry")
}

// TestBindTransportMetricsRealRedisTransportNewRegistryPerLoad covers the
// Caddy reload shape: each config load has its own registry, and an
// unchanged redis block reuses the same pooled transport. Binding it to the
// second load's registry must put the mercure_redis_* families there too,
// while the first registry keeps them, and a counter must continue across
// the reload rather than restart on the new registry.
func TestBindTransportMetricsRealRedisTransportNewRegistryPerLoad(t *testing.T) {
	t.Parallel()

	transport := newRealRedisTransportForTest(t)
	load1 := prometheus.NewRegistry()
	load2 := prometheus.NewRegistry()
	tms, err := mercure.NewTopicMatcherStore(0)
	require.NoError(t, err)

	addSubscriber := func() {
		s := mercure.NewLocalSubscriber("", slog.New(slog.DiscardHandler), tms)
		s.SetMatchers([]mercure.TopicMatcher{{Type: mercure.MatcherTypeExact, Pattern: "https://example.com/reload"}}, nil)
		require.NoError(t, transport.AddSubscriber(context.Background(), s))
	}

	require.NoError(t, bindTransportMetrics(transport, load1, nil))
	addSubscriber()
	require.NoError(t, bindTransportMetrics(transport, load2, nil))
	addSubscriber()

	mfs1, err := load1.Gather()
	require.NoError(t, err)

	mfs2, err := load2.Gather()
	require.NoError(t, err)

	assert.True(t, metricFamiliesContain(t, load2, "mercure_redis_"),
		"the second load's registry must carry the reused transport's mercure_redis_* families")
	assert.Equal(t, metricFamilyNames(mfs1), metricFamilyNames(mfs2),
		"both loads' registries must expose the same families")

	assert.InDelta(t, 2, subscriberAddTotal(t, mfs1), 0, "first load's registry must count both subscribers")
	assert.InDelta(t, 2, subscriberAddTotal(t, mfs2), 0,
		"second load's registry must count the subscriber added before the reload too: the counter continues")
}

// subscriberAddTotal returns the value of the single-series
// mercure_redis_subscriber_add_total counter in mfs, failing the test when it
// is absent.
func subscriberAddTotal(t *testing.T, mfs []*dto.MetricFamily) float64 {
	t.Helper()

	const name = "mercure_redis_subscriber_add_total"

	for _, mf := range mfs {
		if mf.GetName() == name {
			require.Len(t, mf.GetMetric(), 1, "family %s", name)

			return mf.GetMetric()[0].GetCounter().GetValue()
		}
	}

	t.Fatalf("family %s not found", name)

	return 0
}

// buildInfoLabelSets extracts the label-name→value pairs from each
// mercure_redis_build_info time series. Used to assert a repeated bind
// preserves the underlying collector instead of swapping in a fresh one
// whose label state has not been re-populated.
func buildInfoLabelSets(mfs []*dto.MetricFamily) []map[string]string {
	for _, mf := range mfs {
		if mf.GetName() != "mercure_redis_build_info" {
			continue
		}

		out := make([]map[string]string, 0, len(mf.GetMetric()))

		for _, m := range mf.GetMetric() {
			labels := make(map[string]string, len(m.GetLabel()))
			for _, lp := range m.GetLabel() {
				labels[lp.GetName()] = lp.GetValue()
			}

			out = append(out, labels)
		}

		return out
	}

	return nil
}

// TestBindTransportMetricsRealRedisTransportSeparateInstances binds two transports
// to one registry, as when two hub directives in one config load use different
// Redis configs. The second must adopt the collectors the first registered.
func TestBindTransportMetricsRealRedisTransportSeparateInstances(t *testing.T) {
	t.Parallel()

	first := newRealRedisTransportForTest(t)
	second := newRealRedisTransportForTest(t)
	reg := prometheus.NewRegistry()

	require.NoError(t, bindTransportMetrics(first, reg, nil),
		"first transport must register cleanly against fresh registry")

	require.NoError(t, bindTransportMetrics(second, reg, nil),
		"the second transport must adopt the collectors the first registered")

	// The shared registry exposes the families.
	assert.True(t, metricFamiliesContain(t, reg, "mercure_redis_"),
		"shared registry must expose mercure_redis_* families after both transports adopt")
}

func metricFamilyNames(mfs []*dto.MetricFamily) []string {
	names := make([]string, 0, len(mfs))
	for _, mf := range mfs {
		names = append(names, mf.GetName())
	}

	return names
}
