package redistransport

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestShutdownCancelSuppressesBackgroundErrors pins the log/metric-hygiene
// guards added for graceful shutdown: a context cancellation in flight on the
// background loops (health PING, TTL cleanup, zombie-group GC) is NOT a fault —
// it must not increment an error metric, mark the transport unhealthy, or (in
// the real logs) emit an ERROR. Each op is invoked with an already-cancelled
// context, which go-redis surfaces as context.Canceled before issuing the
// command — exactly the in-flight-at-Close condition.
//
// This is the deterministic complement to the real-Redis/Valkey suite: those
// run with 24h health/presence intervals, so the health/TTL/GC loops never tick
// during a test and only the XAck path's noise is observable there. This test
// exercises the other guards directly.
func TestShutdownCancelSuppressesBackgroundErrors(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t, WithPrometheusRegisterer(prometheus.NewRegistry()))

	m := transport.metrics.Load()
	require.NotNil(t, m)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled: simulates Close cancelling ctx mid-operation

	// Health PING: a cancelled parent must not count as a failure nor flip health.
	healthyBefore := transport.healthy.Load()
	assert.Equal(t, 3, transport.runHealthPing(ctx, 3),
		"cancelled-context PING must return the failure count unchanged")
	assert.Equal(t, healthyBefore, transport.healthy.Load(),
		"cancelled-context PING must not change the health state")

	// TTL cleanup: a cancelled context must not record a cleanup error.
	ttlBefore := counterValueForTest(t, m.ttlCleanupErrors)

	transport.trimByTTL(ctx)
	assert.InDelta(t, ttlBefore, counterValueForTest(t, m.ttlCleanupErrors), 0.0001,
		"cancelled-context TTL cleanup must not record an error")

	// Zombie-group GC: a cancelled context must not record a GC error.
	gcBefore := counterValueForTest(t, m.zombieGCErrors)

	transport.gcZombieGroups(ctx, transport.key(""))
	assert.InDelta(t, gcBefore, counterValueForTest(t, m.zombieGCErrors), 0.0001,
		"cancelled-context GC must not record an error")
}
