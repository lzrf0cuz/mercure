package caddy

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"regexp"
	"slices"
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

// Two same-backend hubs share the ready collector on the config's registry. A
// reload adds a third same-backend hub, whose new transport adopts that
// collector on the candidate registry, and then fails on a later route. The
// candidate transport closes and the old config stays live: its registry must
// keep reporting the two surviving hubs, which are ready.
// Not parallel: it loads the process-wide Caddy config.
func TestRedisReadyGaugeSurvivesFailedReload(t *testing.T) {
	mr := miniredis.RunT(t)
	answerInfoServer(mr)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	addr := ln.Addr().String()
	require.NoError(t, ln.Close())

	hub := func(name string) string {
		return fmt.Sprintf(`{"match":[{"path":["/%s/*"]}],"handle":[{"handler":"mercure","name":%q,"anonymous":true,"transport":{"name":"redis","url":%q},"issuers":[{"identifier":"https://example.com","publisher":{"jwt":{"key":"test-publisher-key","alg":"HS256"}}}]}]}`,
			name, name, "redis://"+mr.Addr())
	}
	config := func(routes ...string) []byte {
		return fmt.Appendf(nil, `{"admin":{"disabled":true,"config":{"persist":false}},"apps":{"http":{"servers":{"srv0":{"listen":[%q],"automatic_https":{"disable":true},"routes":[%s]}}}}}`,
			addr, strings.Join(append([]string{`{"match":[{"path":["/metrics"]}],"handle":[{"handler":"metrics"}]}`}, routes...), ","))
	}

	t.Cleanup(func() { assert.NoError(t, caddyv2.Stop()) })

	metricsURL := "http://" + addr + "/metrics"

	require.NoError(t, caddyv2.Load(config(hub("one"), hub("two")), true))
	require.Len(t, pooledRedisTransports(), 2)
	require.Equal(t, "1", sampleValue(t, scrape(t, metricsURL), "mercure_redis_ready"), "precondition: both hubs ready")

	err = caddyv2.Load(config(hub("one"), hub("two"), hub("three"), `{"handle":[{"handler":"no_such_handler"}]}`), true)
	require.ErrorContains(t, err, "no_such_handler")
	require.Len(t, pooledRedisTransports(), 2, "precondition: the failed reload released the candidate hub's transport")

	assert.Equal(t, "1", sampleValue(t, scrape(t, metricsURL), "mercure_redis_ready"),
		"the live config must keep reporting its two ready hubs")
}

// streamLabelRe extracts the stream label of a text-exposition sample.
var streamLabelRe = regexp.MustCompile(`[{,]stream="([^"]*)"`)

// sampleStreams returns the sorted stream labels of every sample of metric
// in a text exposition body.
func sampleStreams(body, metric string) []string {
	var streams []string

	for line := range strings.Lines(body) {
		rest, ok := strings.CutPrefix(line, metric+"{")
		if !ok {
			continue
		}

		if m := streamLabelRe.FindStringSubmatch("{" + rest); m != nil {
			streams = append(streams, m[1])
		}
	}

	slices.Sort(streams)

	return streams
}

// Two hubs on different streams in one config get separate series on the
// config's registry: one per stream, for the scrape-time ready and pool
// collectors as for the pushed ones.
// Not parallel: it loads the process-wide Caddy config.
func TestRedisHubsOnDifferentStreamsHaveSeparateSeries(t *testing.T) {
	mr := miniredis.RunT(t)
	answerInfoServer(mr)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	addr := ln.Addr().String()
	require.NoError(t, ln.Close())

	hub := func(name, stream string) string {
		return fmt.Sprintf(`{"match":[{"path":["/%s/*"]}],"handle":[{"handler":"mercure","name":%q,"anonymous":true,"transport":{"name":"redis","url":%q,"stream":%q},"issuers":[{"identifier":"https://example.com","publisher":{"jwt":{"key":"test-publisher-key","alg":"HS256"}}}]}]}`,
			name, name, "redis://"+mr.Addr(), stream)
	}
	config := fmt.Appendf(nil, `{"admin":{"disabled":true,"config":{"persist":false}},"apps":{"http":{"servers":{"srv0":{"listen":[%q],"automatic_https":{"disable":true},"routes":[%s]}}}}}`,
		addr, strings.Join([]string{`{"match":[{"path":["/metrics"]}],"handle":[{"handler":"metrics"}]}`, hub("one", "stream-one"), hub("two", "stream-two")}, ","))

	t.Cleanup(func() { assert.NoError(t, caddyv2.Stop()) })

	require.NoError(t, caddyv2.Load(config, true))
	require.Len(t, pooledRedisTransports(), 2)

	body := scrape(t, "http://"+addr+"/metrics")

	for _, metric := range []string{
		"mercure_redis_ready",
		"mercure_redis_client_pool_total_conns",
		"mercure_redis_healthy",
		"mercure_redis_subscriber_add_total",
	} {
		assert.Equal(t, []string{"stream-one", "stream-two"}, sampleStreams(body, metric),
			"%s must have one series per stream", metric)
	}
}
