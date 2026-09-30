package caddy

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	caddyv2 "github.com/caddyserver/caddy/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// replyStallProxy is a TCP proxy in front of a Redis server that, while
// stalled, holds every reply it has not yet forwarded, so a command sent then
// stays in flight on its socket.
type replyStallProxy struct {
	ln      net.Listener
	target  string
	stalled atomic.Bool
	done    chan struct{}
	wg      sync.WaitGroup
}

func newReplyStallProxy(t *testing.T, target string) *replyStallProxy {
	t.Helper()

	var lc net.ListenConfig

	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	p := &replyStallProxy{ln: ln, target: target, done: make(chan struct{})}
	p.wg.Go(p.accept)

	t.Cleanup(func() {
		close(p.done)

		_ = ln.Close()

		p.wg.Wait()
	})

	return p
}

func (p *replyStallProxy) accept() {
	for {
		client, err := p.ln.Accept()
		if err != nil {
			return
		}

		var d net.Dialer

		server, err := d.DialContext(context.Background(), "tcp", p.target)
		if err != nil {
			_ = client.Close()

			continue
		}

		closeBoth := sync.OnceFunc(func() {
			_ = client.Close()
			_ = server.Close()
		})

		p.wg.Go(func() {
			defer closeBoth()

			buf := make([]byte, 32*1024)

			for {
				n, err := client.Read(buf)
				if err != nil {
					return
				}

				if _, err := server.Write(buf[:n]); err != nil {
					return
				}
			}
		})
		p.wg.Go(func() {
			defer closeBoth()

			p.forwardReplies(server, client)
		})
		p.wg.Go(func() {
			<-p.done
			closeBoth()
		})
	}
}

func (p *replyStallProxy) forwardReplies(server, client net.Conn) {
	buf := make([]byte, 32*1024)

	for {
		n, err := server.Read(buf)
		if err != nil {
			return
		}

		for p.stalled.Load() {
			select {
			case <-p.done:
				return
			case <-time.After(5 * time.Millisecond):
			}
		}

		if _, err := client.Write(buf[:n]); err != nil {
			return
		}
	}
}

const (
	publishTimeoutKey      = "publish-timeout-caddy-test-key-0123456789"
	publishTimeoutIssuer   = "https://example.com"
	publishTimeoutResource = "https://example.com/.well-known/mercure"
)

// publishTimeoutJWT signs an HS256 access token allowed to publish on every
// topic.
func publishTimeoutJWT() string {
	enc := base64.RawURLEncoding
	input := enc.EncodeToString([]byte(`{"alg":"HS256","typ":"at+jwt"}`)) + "." +
		enc.EncodeToString([]byte(`{"iss":"`+publishTimeoutIssuer+`","aud":"`+publishTimeoutResource+`","exp":`+
			strconv.FormatInt(time.Now().Add(time.Hour).Unix(), 10)+
			`,"authorization_details":[{"type":"https://mercure.rocks/authorization-detail","actions":["publish"],"topics":[{"match":"*"}]}]}`))

	mac := hmac.New(sha256.New, []byte(publishTimeoutKey))
	mac.Write([]byte(input))

	return input + "." + enc.EncodeToString(mac.Sum(nil))
}

// The mercure module's publish_timeout must reach the redis transport: a
// publish whose script reply Redis withholds is cut at the deadline and
// answered 504, well before the client's read_timeout.
// Not parallel: it loads the process-wide Caddy config.
func TestPublishTimeoutReachesRedisTransport(t *testing.T) {
	const (
		publishTimeout = 100 * time.Millisecond
		readTimeout    = 3 * time.Second
		// slack absorbs scheduling and -race overhead; the deadline plus the
		// slack stays well under readTimeout.
		slack = 900 * time.Millisecond
	)

	mr := miniredis.RunT(t)
	answerInfoServer(mr)

	proxy := newReplyStallProxy(t, mr.Addr())

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	addr := ln.Addr().String()
	require.NoError(t, ln.Close())

	config := fmt.Appendf(nil, `{"admin":{"disabled":true,"config":{"persist":false}},"apps":{"http":{"servers":{"srv0":{"listen":[%q],"automatic_https":{"disable":true},"routes":[`+
		`{"handle":[{"handler":"mercure","name":"publish-timeout","anonymous":true,"publish_timeout":%q,"resource_identifier":%q,`+
		`"transport":{"name":"redis","url":%q,"read_timeout":%q},`+
		`"issuers":[{"identifier":%q,"publisher":{"jwt":{"key":%q,"alg":"HS256"}}}]}]}`+
		`]}}}}}`, addr, publishTimeout.String(), publishTimeoutResource, "redis://"+proxy.ln.Addr().String(),
		readTimeout.String(), publishTimeoutIssuer, publishTimeoutKey)

	t.Cleanup(func() {
		proxy.stalled.Store(false)
		assert.NoError(t, caddyv2.Stop())
	})

	require.NoError(t, caddyv2.Load(config, true))

	publish := func() (int, time.Duration) {
		form := url.Values{"topic": {"https://example.com/publish-timeout"}, "data": {"payload"}}

		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "http://"+addr+"/.well-known/mercure",
			strings.NewReader(form.Encode()))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set("Authorization", "Bearer "+publishTimeoutJWT())

		start := time.Now()

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)

		elapsed := time.Since(start)

		require.NoError(t, resp.Body.Close())

		return resp.StatusCode, elapsed
	}

	// Warm up: loads the publish script and pools a connection.
	code, _ := publish()
	require.Equal(t, http.StatusOK, code)

	proxy.stalled.Store(true)

	code, elapsed := publish()
	assert.Equal(t, http.StatusGatewayTimeout, code)
	assert.GreaterOrEqual(t, elapsed, publishTimeout)
	assert.Less(t, elapsed, publishTimeout+slack,
		"the publish must be cut at publish_timeout, not at read_timeout (%v)", readTimeout)
}
