package redistransport

import (
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

// poolStatsCollector exposes go-redis's client connection-pool stats as
// Prometheus metrics. Implemented as a custom collector (values read at
// scrape time via client.PoolStats()) rather than shadowed counters/gauges
// so no polling ticker is needed and no double-bookkeeping can drift.
//
// Operationally these map to "is the Redis I/O layer saturating before the
// transport is?" — pool_timeouts climbing during a stress run means Redis
// is the bottleneck, not the transport's dispatch or presence paths.
type poolStatsCollector struct {
	// client is held via atomic.Pointer so a Caddy reload can swap the
	// reference on the already-registered collector (see initMetrics's
	// AlreadyRegisteredError path); without the swap the collector keeps
	// scraping the previous, closed client's pool indefinitely.
	client atomic.Pointer[redis.UniversalClient]

	// backendType captures the detected backend at construction time so
	// the Caddy-reload AlreadyRegisteredError path in RegisterMetricsWith
	// can compare the adopted collector's stamped label against the new
	// transport's detected backend and Warn on drift. Prometheus ConstLabels
	// inside *prometheus.Desc are immutable post-construction, so on
	// mismatch the adopted Descs keep emitting the original label until
	// the process restarts — operators need a log breadcrumb to attribute.
	backendType serverType

	hits       *prometheus.Desc
	misses     *prometheus.Desc
	timeouts   *prometheus.Desc
	totalConns *prometheus.Desc
	idleConns  *prometheus.Desc
	staleConns *prometheus.Desc
}

// newPoolStatsCollector builds a collector bound to client. ConstLabels are
// aligned with every other transport metric ({transport_type, backend_type})
// via constLabelsFor so Grafana queries pivot on the same selector. backendType
// is the detected backend family ("redis", "valkey", or "unknown") sourced
// from INFO at validateVersion time; empty values normalize to "unknown".
func newPoolStatsCollector(client redis.UniversalClient, backendType serverType) *poolStatsCollector {
	labels := constLabelsFor(backendType)

	if backendType == "" {
		backendType = serverTypeUnknown
	}

	c := &poolStatsCollector{
		backendType: backendType,
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
			"Pool Get() calls that timed out waiting for a connection. MUST be zero on a healthy transport; non-zero means pool exhaustion.",
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

// Collect implements prometheus.Collector. Snapshot-at-scrape semantics —
// no background goroutine, no caching, no drift vs go-redis internals.
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

// setClient atomically swaps the underlying client. Called on Caddy reload
// to redirect the already-registered collector at the new transport's
// client pool.
func (c *poolStatsCollector) setClient(client redis.UniversalClient) {
	c.client.Store(&client)
}
