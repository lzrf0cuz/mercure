package redistransport

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain wires goleak so every test in this package is verified against
// goroutine leaks on exit. Transport-managed goroutines (xreadgroupListener,
// presenceHeartbeat, healthCheck, ttlCleanup, periodicZombieGC,
// followUpZombieGC, shard workers) all flow through t.wg and t.cancel. Close
// must wg.Wait before returning, and tests close the transport in Cleanup.
// Real leaks here point to a missed goroutine registration or a shutdown path
// that skips wg.Done().
//
// Ignored top-functions: goroutines owned by go-redis clients and by
// miniredis that can still be running when the leak check runs.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(
		m,
		// Per-connection accept handler inside miniredis. Stays alive per
		// connection until the client closes or the miniredis stops via
		// `miniredis.RunT`'s Cleanup hook.
		goleak.IgnoreTopFunction("github.com/alicebob/miniredis/v2/server.(*Server).servePeer.func2"),
		// net.(*conn).Read blocks on the per-connection loop inside miniredis.
		goleak.IgnoreTopFunction("net.(*conn).Read"),
		// go-redis v9.18+ spawns a maintenance-notifications circuit-breaker
		// cleanup goroutine per client that exits only on client Close.
		goleak.IgnoreTopFunction("github.com/redis/go-redis/v9/maintnotifications.(*CircuitBreakerManager).cleanupLoop"),
	)
}
