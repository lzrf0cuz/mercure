package caddy

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	caddyv2 "github.com/caddyserver/caddy/v2"
	_ "github.com/caddyserver/caddy/v2/modules/metrics"
	mercurecaddy "github.com/dunglas/mercure/caddy"
	"github.com/lzrf0cuz/mercure/redistransport"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// pooledRedisTransports returns the redis transports in the transport pool.
func pooledRedisTransports() []*redistransport.RedisTransport {
	var transports []*redistransport.RedisTransport

	mercurecaddy.TransportUsagePool.Range(func(_, value any) bool {
		if d, ok := value.(mercurecaddy.TransportDestructor[*redistransport.RedisTransport]); ok {
			transports = append(transports, d.Transport)
		}

		return true
	})

	return transports
}

// scrape returns the body the config's metrics handler serves.
func scrape(t *testing.T, url string) string {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	return string(body)
}

// sampleValue returns the value of the first sample of metric in a text
// exposition body.
func sampleValue(t *testing.T, body, metric string) string {
	t.Helper()

	for line := range strings.Lines(body) {
		rest, ok := strings.CutPrefix(line, metric)
		if !ok || (rest[0] != '{' && rest[0] != ' ') {
			continue
		}

		fields := strings.Fields(rest)

		return fields[len(fields)-1]
	}

	require.FailNow(t, "metric not exposed", metric)

	return ""
}

// A reload builds a new metrics registry and keeps the pooled redis transport
// whose configuration did not change. The transport's collectors must reach
// each config's registry — the one its metrics handler serves — through the
// mercure module's binding, since this module's context carries no registry.
// Not parallel: it loads the process-wide Caddy config.
func TestRedisTransportMetricsBoundOnEachConfigLoad(t *testing.T) {
	mr := miniredis.RunT(t)
	answerInfoServer(mr)
	// The server clock runs behind, so the startup clock-drift sample is
	// clearly non-zero.
	mr.SetTime(time.Now().Add(-10 * time.Second))

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	addr := ln.Addr().String()
	require.NoError(t, ln.Close())

	config := fmt.Appendf(nil, `{"admin":{"disabled":true,"config":{"persist":false}},"apps":{"http":{"servers":{"srv0":{"listen":[%q],"automatic_https":{"disable":true},"routes":[`+
		`{"match":[{"path":["/metrics"]}],"handle":[{"handler":"metrics"}]},`+
		`{"handle":[{"handler":"mercure","name":"reload","anonymous":true,"transport":{"name":"redis","url":%q},"issuers":[{"identifier":"https://example.com","publisher":{"jwt":{"key":"test-publisher-key","alg":"HS256"}}}]}]}`+
		`]}}}}}`, addr, "redis://"+mr.Addr())

	t.Cleanup(func() { assert.NoError(t, caddyv2.Stop()) })

	metricsURL := "http://" + addr + "/metrics"

	require.NoError(t, caddyv2.Load(config, true))

	loaded := pooledRedisTransports()
	require.Len(t, loaded, 1)

	body := scrape(t, metricsURL)
	assert.Contains(t, body, "mercure_redis_build_info",
		"the first config's registry must carry the transport's collectors")
	drift, err := strconv.ParseFloat(sampleValue(t, body, "mercure_redis_clock_drift_seconds"), 64)
	require.NoError(t, err)
	assert.InDelta(t, 10, drift, 5, "the bind must record the startup clock-drift sample")

	require.NoError(t, caddyv2.Load(config, true))

	reloaded := pooledRedisTransports()
	require.Len(t, reloaded, 1)
	require.Same(t, loaded[0], reloaded[0], "the reload must reuse the pooled transport")

	body = scrape(t, metricsURL)
	assert.Contains(t, body, "mercure_redis_build_info",
		"the reloaded config's registry must carry the reused transport's collectors")
}
