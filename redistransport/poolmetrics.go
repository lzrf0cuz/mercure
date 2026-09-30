package redistransport

import (
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

// poolStatsCollector exports go-redis connection-pool stats, read from
// PoolStats at scrape time. Rising pool_timeouts means the client pool, not
// the transport, is the bottleneck.
type poolStatsCollector struct {
	// client is held via atomic.Pointer so a second RedisTransport instance
	// adopting this already-registered collector (safeRegister's
	// AlreadyRegisteredError path in RegisterMetricsWith) can swap in its
	// own client; without the swap the collector keeps scraping the
	// previous, closed client's pool indefinitely.
	client atomic.Pointer[redis.UniversalClient]

	hits       *prometheus.Desc
	misses     *prometheus.Desc
	timeouts   *prometheus.Desc
	totalConns *prometheus.Desc
	idleConns  *prometheus.Desc
	staleConns *prometheus.Desc
}

// newPoolStatsCollector builds a collector bound to client, with the same
// const labels as every other transport metric (see constLabelsFor).
func newPoolStatsCollector(client redis.UniversalClient, backendType serverType) *poolStatsCollector {
	labels := constLabelsFor(backendType)

	c := &poolStatsCollector{
		hits: prometheus.NewDesc(
			"mercure_redis_client_pool_hits_total",
			"Connections reused from the go-redis client pool. Should dominate misses under steady state.",
			nil, labels,
		),
		misses: prometheus.NewDesc(
			"mercure_redis_client_pool_misses_total",
			"Connections opened because the pool had none idle. Sustained high misses indicate the pool is undersized for the command rate.",
			nil, labels,
		),
		timeouts: prometheus.NewDesc(
			"mercure_redis_client_pool_timeouts_total",
			"Pool Get() calls that timed out waiting for a connection. Non-zero means the pool is exhausted.",
			nil, labels,
		),
		totalConns: prometheus.NewDesc(
			"mercure_redis_client_pool_total_conns",
			"Connections currently open in the pool (idle + in-use).",
			nil, labels,
		),
		idleConns: prometheus.NewDesc(
			"mercure_redis_client_pool_idle_conns",
			"Connections currently idle in the pool. A value of 0 with non-zero misses means the pool is running at capacity.",
			nil, labels,
		),
		staleConns: prometheus.NewDesc(
			"mercure_redis_client_pool_stale_conns_total",
			"Connections discarded by the pool's idle-timeout reaper.",
			nil, labels,
		),
	}
	c.setClient(client)

	return c
}

// Describe implements prometheus.Collector.
func (c *poolStatsCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{c.hits, c.misses, c.timeouts, c.totalConns, c.idleConns, c.staleConns} {
		ch <- d
	}
}

// Collect implements prometheus.Collector.
func (c *poolStatsCollector) Collect(ch chan<- prometheus.Metric) {
	clientPtr := c.client.Load()
	if clientPtr == nil {
		return
	}

	s := (*clientPtr).PoolStats()

	for _, m := range []prometheus.Metric{
		prometheus.MustNewConstMetric(c.hits, prometheus.CounterValue, float64(s.Hits)),
		prometheus.MustNewConstMetric(c.misses, prometheus.CounterValue, float64(s.Misses)),
		prometheus.MustNewConstMetric(c.timeouts, prometheus.CounterValue, float64(s.Timeouts)),
		prometheus.MustNewConstMetric(c.totalConns, prometheus.GaugeValue, float64(s.TotalConns)),
		prometheus.MustNewConstMetric(c.idleConns, prometheus.GaugeValue, float64(s.IdleConns)),
		prometheus.MustNewConstMetric(c.staleConns, prometheus.CounterValue, float64(s.StaleConns)),
	} {
		ch <- m
	}
}

// setClient points the collector at client's pool.
func (c *poolStatsCollector) setClient(client redis.UniversalClient) {
	c.client.Store(&client)
}
