//go:build real_redis

package redistransport

import (
	"context"
	"testing"
	"time"

	"github.com/dunglas/mercure"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestReal_OOMRejectionsCounted runs the three counted writes against a real
// server at maxmemory under noeviction and checks that each refusal is
// counted. The publish reply depends on the server: Redis 6.2 wraps the OOM
// error in a script error, while Redis 7 and later and Valkey start the reply
// with it. A run covers only its server's form; the unit test
// TestOOMRejectionsCountedByOperation covers both on every run.
//
// Not parallel: it lowers the server's maxmemory, which every client sees.
// The settings are restored before the transport closes.
func TestReal_OOMRejectionsCounted(t *testing.T) {
	transport, _ := newRealTransport(t, WithPrometheusRegisterer(prometheus.NewRegistry()))
	probe := newProbeClient(t, probeRealRedis(t))
	ctx := context.Background()

	saved, err := probe.ConfigGet(ctx, "maxmemory*").Result()
	require.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, probe.ConfigSet(ctx, "maxmemory", saved["maxmemory"]).Err())
		assert.NoError(t, probe.ConfigSet(ctx, "maxmemory-policy", saved["maxmemory-policy"]).Err())
	})

	require.NoError(t, probe.ConfigSet(ctx, "maxmemory-policy", "noeviction").Err())
	require.NoError(t, probe.ConfigSet(ctx, "maxmemory", "1").Err())

	m := transport.metrics.Load()
	require.NotNil(t, m)

	err = transport.Dispatch(ctx, &mercure.Update{Topics: []string{"https://example.com/oom"}, Data: "x"})
	require.Error(t, err)
	t.Logf("publish error: %v", err)

	transport.publishPresence(ctx)

	// Delete the stream, which DEL allows at maxmemory (Redis 6.2 refuses
	// XGROUP DESTROY there). The listener then hits NOGROUP and re-creates the
	// group with MKSTREAM, which is refused.
	require.NoError(t, probe.Del(ctx, transport.key("")).Err())

	require.Eventually(t, func() bool {
		return counterValueForTest(t, m.oomRejections.WithLabelValues(string(oomOperationGroupCreate))) >= 1
	}, 15*time.Second, 50*time.Millisecond, "the listener's group re-create must be counted as an OOM rejection")

	assert.InDelta(t, 1, counterValueForTest(t, m.oomRejections.WithLabelValues(string(oomOperationPublish))), 0)
	assert.InDelta(t, 1, counterValueForTest(t, m.oomRejections.WithLabelValues(string(oomOperationPresence))), 0)
}
