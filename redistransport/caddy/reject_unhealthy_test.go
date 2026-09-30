package caddy

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/alicebob/miniredis/v2/server"
	caddyv2 "github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/lzrf0cuz/mercure/redistransport"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// answerInfoServerFailingPing is answerInfoServer that also fails every PING
// while failPing is set.
func answerInfoServerFailingPing(mr *miniredis.Miniredis, failPing *atomic.Bool) {
	mr.Server().SetPreHook(func(c *server.Peer, cmd string, args ...string) bool {
		if strings.EqualFold(cmd, "PING") && failPing.Load() {
			c.WriteError("ERR injected PING failure")

			return true
		}

		if !strings.EqualFold(cmd, "INFO") || len(args) != 1 || !strings.EqualFold(args[0], "server") {
			return false
		}

		c.WriteBulk("# Server\r\nredis_version:7.2.4\r\n")

		return true
	})
}

func TestUnmarshalCaddyfileSubscriberRejectWhenUnhealthy(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		line string
		want bool
	}{
		{"subscriber_reject_when_unhealthy", true},
		{"subscriber_reject_when_unhealthy true", true},
		{"subscriber_reject_when_unhealthy false", false},
		{"", false},
	} {
		t.Run(tc.line, func(t *testing.T) {
			t.Parallel()

			d := caddyfile.NewTestDispenser("redis {\n\turl redis://localhost:6379\n\t" + tc.line + "\n}")
			r := &Redis{}
			require.NoError(t, r.UnmarshalCaddyfile(d))
			assert.Equal(t, tc.want, r.SubscriberRejectWhenUnhealthy)

			// caddyconfig.JSON is how the Caddyfile adapter encodes the module.
			b := caddyconfig.JSON(r, nil)
			assert.Equal(t, tc.want, strings.Contains(string(b), `"subscriber_reject_when_unhealthy":true`), string(b))

			var back Redis
			require.NoError(t, json.Unmarshal(b, &back))
			assert.Equal(t, tc.want, back.SubscriberRejectWhenUnhealthy, "JSON round-trip")
		})
	}
}

// Provision passes the directive to the transport: once PING fails, TryAdmit
// refuses new subscribers only when it is set.
func TestProvisionWiresSubscriberRejectWhenUnhealthy(t *testing.T) {
	t.Parallel()

	for _, enabled := range []bool{true, false} {
		t.Run(strconv.FormatBool(enabled), func(t *testing.T) {
			t.Parallel()

			var failPing atomic.Bool

			mr := miniredis.RunT(t)
			answerInfoServerFailingPing(mr, &failPing)

			r := &Redis{
				URL:                           "redis://" + mr.Addr(),
				HealthInterval:                "20ms",
				HealthThreshold:               1,
				SubscriberRejectWhenUnhealthy: enabled,
			}
			require.NoError(t, r.Provision(hubContext(t, "reject-unhealthy")))
			t.Cleanup(func() { assert.NoError(t, r.Cleanup()) })

			failPing.Store(true)

			require.Eventually(t, func() bool {
				return r.transport.Ready(context.Background()) != nil
			}, 5*time.Second, 10*time.Millisecond, "the transport must turn unhealthy")

			_, release, err := r.transport.TryAdmit(context.Background())
			if enabled {
				require.ErrorIs(t, err, redistransport.ErrTransportUnhealthy)

				return
			}

			require.NoError(t, err)
			release()
		})
	}
}

// End to end: with subscriber_reject_when_unhealthy, a hub whose Redis fails
// PING answers a new subscription 503, and 200 again once PING recovers.
// Not parallel: it loads the process-wide Caddy config.
func TestSubscribeRejectedWhileUnhealthy(t *testing.T) {
	var failPing atomic.Bool

	mr := miniredis.RunT(t)
	answerInfoServerFailingPing(mr, &failPing)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	addr := ln.Addr().String()
	require.NoError(t, ln.Close())

	config := fmt.Appendf(nil, `{"admin":{"disabled":true,"config":{"persist":false}},"apps":{"http":{"servers":{"srv0":{"listen":[%q],"automatic_https":{"disable":true},"routes":[`+
		`{"handle":[{"handler":"mercure","name":"reject-unhealthy","anonymous":true,"resource_identifier":%q,`+
		`"transport":{"name":"redis","url":%q,"health_interval":"20ms","health_threshold":1,"subscriber_reject_when_unhealthy":true},`+
		`"issuers":[{"identifier":%q,"publisher":{"jwt":{"key":%q,"alg":"HS256"}}}]}]}`+
		`]}}}}}`, addr, publishTimeoutResource, "redis://"+mr.Addr(), publishTimeoutIssuer, publishTimeoutKey)

	t.Cleanup(func() { assert.NoError(t, caddyv2.Stop()) })

	require.NoError(t, caddyv2.Load(config, true))

	subscribe := func() (int, http.Header) {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()

		req, err := http.NewRequestWithContext(ctx, http.MethodGet,
			"http://"+addr+"/.well-known/mercure?match=https://example.com/reject-unhealthy", nil)
		require.NoError(t, err)

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())

		return resp.StatusCode, resp.Header
	}

	code, _ := subscribe()
	require.Equal(t, http.StatusOK, code, "healthy hub")

	var rejected http.Header

	failPing.Store(true)
	require.Eventually(t, func() bool {
		code, rejected = subscribe()

		return code == http.StatusServiceUnavailable
	}, 5*time.Second, 20*time.Millisecond, "unhealthy hub must answer 503")
	assert.Empty(t, rejected.Values("Retry-After"), "the 503 must not carry Retry-After")

	failPing.Store(false)
	require.Eventually(t, func() bool {
		code, _ := subscribe()

		return code == http.StatusOK
	}, 5*time.Second, 20*time.Millisecond, "recovered hub must admit again")
}
