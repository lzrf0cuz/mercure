package caddy

import (
	"context"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/alicebob/miniredis/v2/server"
	caddyv2 "github.com/caddyserver/caddy/v2"
	mercurecaddy "github.com/dunglas/mercure/caddy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// answerInfoServer makes mr answer INFO server, which miniredis does not
// implement, with a supported Redis version, so the transport's startup
// version check passes.
func answerInfoServer(mr *miniredis.Miniredis) {
	mr.Server().SetPreHook(func(c *server.Peer, cmd string, args ...string) bool {
		if !strings.EqualFold(cmd, "INFO") || len(args) != 1 || !strings.EqualFold(args[0], "server") {
			return false
		}

		c.WriteBulk("# Server\r\nredis_version:7.2.4\r\n")

		return true
	})
}

// hubContext returns a Caddy context carrying the hub name, as the mercure
// module hands it to its transport module.
func hubContext(t *testing.T, name string) caddyv2.Context {
	t.Helper()

	ctx, cancel := caddyv2.NewContext(caddyv2.Context{Context: context.Background()})
	t.Cleanup(cancel)

	return ctx.WithValue(mercurecaddy.HubNameContextKey, name)
}

// Two hubs with identical redis configuration must not share one pooled
// transport, as Bolt and Local transports are scoped by hub name; the same
// hub reuses it.
func TestRedisTransportScopedByHub(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)
	answerInfoServer(mr)

	provision := func(name string) *Redis {
		r := &Redis{URL: "redis://" + mr.Addr()}
		require.NoError(t, r.Provision(hubContext(t, name)))
		t.Cleanup(func() { assert.NoError(t, r.Cleanup()) })

		return r
	}

	a, b, a2 := provision("a"), provision("b"), provision("a")
	assert.NotSame(t, a.transport, b.transport, "differently named hubs must get their own transport")
	assert.Same(t, a.transport, a2.transport, "the same hub must reuse its pooled transport")
}
