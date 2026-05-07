package redistransport

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain wires goleak so every test in this package is verified against
// goroutine leaks on exit. Transport-managed goroutines (xreadgroupListener,
// presenceHeartbeat, healthCheck, ttlCleanup, periodicZombieGC, shard workers)
// all flow through t.wg + t.cancel; Close must wg.Wait before returning, and
// tests Cleanup-close the transport. Real leaks here point to a missed
// goroutine registration or a shutdown path that skips wg.Done().
//
// Ignored top-functions: the go-redis v9 pool's connection-check goroutine
// doesn't expose a clean shutdown hook and persists for the process lifetime
// of any `client` that hasn't been `Close()`d — which is legal-but-hard to
// control when tests let `t.Cleanup` close transports for them. miniredis's
// own serve loop similarly persists until `miniredis.RunT`'s cleanup fires
// (also after this TestMain's VerifyTestMain runs).
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(
		m,
		// go-redis pool connection-check goroutine. Not ours; goes away when
		// the process exits. Multiple package-internal helpers reach this
		// indirectly via redis.NewClient.
		goleak.IgnoreTopFunction("github.com/redis/go-redis/v9/internal/pool.(*ConnPool).reaper"),
		// miniredis's server accept loop. Persists beyond VerifyTestMain
		// because miniredis.RunT's cleanup runs at t.Cleanup time, which
		// fires per-test AFTER VerifyTestMain's snapshot. Harmless.
		goleak.IgnoreTopFunction("github.com/alicebob/miniredis/v2.(*Miniredis).Start.func1"),
		goleak.IgnoreTopFunction("github.com/alicebob/miniredis/v2.(*Miniredis).serve"),
		// Per-connection accept handler inside miniredis. Stays alive per
		// connection until the client closes or the miniredis stops via
		// `miniredis.RunT`'s Cleanup hook.
		goleak.IgnoreTopFunction("github.com/alicebob/miniredis/v2/server.(*Server).servePeer.func2"),
		// net.(*conn).Read blocks on the per-connection loop inside miniredis.
		goleak.IgnoreTopFunction("net.(*conn).Read"),
		// go-redis's internal monitor/watchdog for client connections.
		goleak.IgnoreTopFunction("github.com/redis/go-redis/v9/internal/pool.(*ConnPool).addIdleConn.func1"),
		// go-redis v9.18+ spawns a maintenance-notifications circuit-breaker
		// cleanup goroutine per client that exits only on client Close.
		// Similar case to the pool reaper — owned by tests' redis.Clients,
		// reaped at t.Cleanup after VerifyTestMain's snapshot.
		goleak.IgnoreTopFunction("github.com/redis/go-redis/v9/maintnotifications.(*CircuitBreakerManager).cleanupLoop"),
	)
}
