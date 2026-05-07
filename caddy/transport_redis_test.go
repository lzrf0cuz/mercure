package caddy

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/lzrf0cuz/mercure/redistransport"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newRealRedisTransportForTest constructs a real *redistransport.RedisTransport
// backed by miniredis so the cross-package e2e tests below exercise actual
// metric registration + adoption behavior, not the stand-in fakeMetricsTransport.
//
// Skips the production INFO-server version gate and presence-interval check
// via the cross-package test-only options — miniredis does not implement
// INFO server, and the test windows are vastly shorter than the production
// presence-interval floor.
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
		// Surface Close errors loudly: a stuck transport (zombie goroutine,
		// miniredis teardown race, sync.Once not gating close cleanly) would
		// otherwise vanish under `_ =`.
		assert.NoError(t, transport.Close(context.Background()))
	})

	return transport
}

// metricFamiliesContain returns true if the gathered registry contains at least
// one metric family whose name matches the given prefix. The precise metric
// set is exercised by redistransport's own tests; this helper covers the
// coarser "any registration happened" check needed at the caddy boundary.
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

// TestBindTransportMetricsRealRedisTransport is the end-to-end counterpart
// to the fakeMetricsTransport tests in transport_test.go. It drives the
// production binding helper against a real *redistransport.RedisTransport
// (not a mock) so the actual RegisterMetricsWith → newMetrics → Prometheus
// Register → swappableCounterFunc.setSource sequence is exercised. Without
// this, a regression in any of those steps — typed-nil normalization, gob
// shortcut, pool-collector adoption, sync.Once gating — would only fail
// the in-package redistransport tests, leaving the caddy-side late-binding
// integration assumed-good.
func TestBindTransportMetricsRealRedisTransport(t *testing.T) {
	t.Parallel()

	transport := newRealRedisTransportForTest(t)
	reg := prometheus.NewRegistry()

	require.NoError(t, bindTransportMetrics(transport, reg, nil),
		"bindTransportMetrics must drive real RedisTransport.RegisterMetricsWith without error")

	assert.True(t, metricFamiliesContain(t, reg, "mercure_redis_"),
		"mercure_redis_* metric families must appear on the registry after bindTransportMetrics — empty registry indicates the late-binding path silently dropped registration")
}

// TestBindTransportMetricsRealRedisTransportReload locks the Caddy-reload
// pattern at the integration boundary: when Caddy reuses a transport instance
// from TransportUsagePool across config reloads, bindTransportMetrics is
// called again with the same registry. The fake-transport version of this
// test (TestBindTransportMetricsReloadIdempotent) only locks the helper-level
// invariant; this version locks the transport-level sync.Once gate plus
// safeRegister adoption + setSource rebind, which is the actual production
// behavior under reload.
//
// Without this guard, a regression that drops sync.Once or breaks the
// swappableCounterFunc rebind surfaces only as silently-detached metrics
// post-reload — the same failure shape the late-binding rework was built
// to prevent.
func TestBindTransportMetricsRealRedisTransportReload(t *testing.T) {
	t.Parallel()

	transport := newRealRedisTransportForTest(t)
	reg := prometheus.NewRegistry()

	require.NoError(t, bindTransportMetrics(transport, reg, nil),
		"first bindTransportMetrics call must succeed")

	mfsBefore, err := reg.Gather()
	require.NoError(t, err)

	require.NoError(t, bindTransportMetrics(transport, reg, nil),
		"second bindTransportMetrics call (Caddy reload) must NOT return AlreadyRegisteredError or any other failure — sync.Once gates the second registration to a no-op")

	mfsAfter, err := reg.Gather()
	require.NoError(t, err)

	// Family count must be stable across reload — a leak would manifest as
	// growing collector counts on each reload. We don't assert exact equality
	// of family contents (poolstats Gauges shift values across the two Gather
	// calls); name set is the load-bearing invariant.
	namesBefore := metricFamilyNames(mfsBefore)
	namesAfter := metricFamilyNames(mfsAfter)
	assert.Equal(t, namesBefore, namesAfter,
		"metric family name set must be stable across reload — reload-introduced new families indicate a regression in the sync.Once gate or safeRegister adoption")

	// Value-persistence check: build_info is a static-value gauge whose
	// labels are set once at NewRedisTransport time and never change. If a
	// regression replaced the registered collector with a freshly-allocated
	// one on the second bind (sync.Once broken, AlreadyRegisteredError
	// adoption skipped), the new collector would emit zero label sets until
	// initMetrics ran again — surfacing as a missing or empty build_info
	// family after reload.
	buildInfoBefore := buildInfoLabelSets(mfsBefore)
	buildInfoAfter := buildInfoLabelSets(mfsAfter)

	require.NotEmpty(t, buildInfoBefore, "first Gather must include mercure_redis_build_info — registration drift if missing")
	assert.Equal(t, buildInfoBefore, buildInfoAfter,
		"build_info label set must persist across reload — divergence indicates collector identity was swapped, not adopted")
}

// buildInfoLabelSets extracts the label-name→value pairs from each
// mercure_redis_build_info time series. Used to assert reload preserves
// the underlying collector instead of swapping in a fresh one whose
// label state has not been re-populated.
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

// TestBindTransportMetricsRealRedisTransportSeparateInstances covers the
// other axis of reload: two distinct transport instances bound to the same
// registry — the transport-replaced-by-config-change path. The first
// instance registers; the second's RegisterMetricsWith must hit
// AlreadyRegisteredError and adopt the existing collector via safeRegister
// (then call setSource on the swappableCounterFunc so its own
// preBindingDrops counter wins subsequent scrapes). A regression that
// drops the adoption would either panic (legacy ignore-AlreadyRegistered
// pattern) or detach metrics on the new instance.
func TestBindTransportMetricsRealRedisTransportSeparateInstances(t *testing.T) {
	t.Parallel()

	first := newRealRedisTransportForTest(t)
	second := newRealRedisTransportForTest(t)
	reg := prometheus.NewRegistry()

	require.NoError(t, bindTransportMetrics(first, reg, nil),
		"first transport must register cleanly against fresh registry")

	require.NoError(t, bindTransportMetrics(second, reg, nil),
		"second (replacement) transport must adopt the existing collectors via AlreadyRegisteredError, not panic or fail — Caddy config-change reuses the registry across transport identities")

	// Confirm both instances see metric families on the shared registry.
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
