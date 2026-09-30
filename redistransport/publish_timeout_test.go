package redistransport

import (
	"bufio"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/dunglas/mercure"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stallProxy is a TCP proxy in front of a Redis server. It forwards requests
// at once and counts the EVAL/EVALSHA commands it forwards. While a stall is
// set it holds the reply to each command named in holdCmds for that long, so
// the command is in flight on the socket when the caller's deadline passes.
// Other replies (connection handshakes, the listener's XREADGROUP) pass
// through, so a command on a freshly dialled connection is still sent.
type stallProxy struct {
	ln       net.Listener
	target   string
	holdCmds map[string]bool // lower-case command names whose replies are held
	stall    atomic.Int64    // reply delay in ns; 0 passes replies through
	scripts  atomic.Int64    // EVAL / EVALSHA commands forwarded
	done     chan struct{}
	wg       sync.WaitGroup
}

// scriptCmds are the commands the publish script runs as.
var scriptCmds = []string{"eval", "evalsha"}

func newStallProxy(t *testing.T, target string, holdCmds []string) *stallProxy {
	t.Helper()

	var lc net.ListenConfig

	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	p := &stallProxy{ln: ln, target: target, holdCmds: map[string]bool{}, done: make(chan struct{})}
	for _, c := range holdCmds {
		p.holdCmds[c] = true
	}

	p.wg.Go(p.accept)

	t.Cleanup(func() {
		close(p.done)

		_ = ln.Close()

		p.wg.Wait()
	})

	return p
}

func (p *stallProxy) addr() string { return p.ln.Addr().String() }

func (p *stallProxy) accept() {
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

		// holdNext marks that the next reply on this connection answers a
		// held command sent during a stall. go-redis waits for each reply
		// before sending the next command on a connection, so the next chunk
		// read from the server is that reply.
		var holdNext atomic.Bool

		p.wg.Go(func() {
			defer closeBoth()

			p.forwardRequests(client, server, &holdNext)
		})
		p.wg.Go(func() {
			defer closeBoth()

			p.forwardReplies(server, client, &holdNext)
		})

		go func() {
			<-p.done
			closeBoth()
		}()
	}
}

// forwardRequests copies RESP command arrays from client to server, counting
// the script commands. go-redis sends every command as an array of bulk
// strings.
func (p *stallProxy) forwardRequests(client, server net.Conn, holdNext *atomic.Bool) {
	r := bufio.NewReader(client)

	for {
		header, err := r.ReadString('\n')
		if err != nil || !strings.HasPrefix(header, "*") {
			return
		}

		n, err := strconv.Atoi(strings.TrimSpace(header[1:]))
		if err != nil {
			return
		}

		raw := []byte(header)

		for i := range n {
			bulk, arg, ok := readBulk(r)
			if !ok {
				return
			}

			if i == 0 {
				p.noteCommand(strings.ToLower(arg), holdNext)
			}

			raw = append(raw, bulk...)
		}

		if _, err := server.Write(raw); err != nil {
			return
		}
	}
}

// readBulk reads one RESP bulk string, returning its raw bytes and payload.
func readBulk(r *bufio.Reader) ([]byte, string, bool) {
	lenLine, err := r.ReadString('\n')
	if err != nil || !strings.HasPrefix(lenLine, "$") {
		return nil, "", false
	}

	size, err := strconv.Atoi(strings.TrimSpace(lenLine[1:]))
	if err != nil {
		return nil, "", false
	}

	arg := make([]byte, size+2)
	if _, err := io.ReadFull(r, arg); err != nil {
		return nil, "", false
	}

	return append([]byte(lenLine), arg...), string(arg[:size]), true
}

// noteCommand counts script commands and marks a held command's reply.
func (p *stallProxy) noteCommand(name string, holdNext *atomic.Bool) {
	if name == "eval" || name == "evalsha" {
		p.scripts.Add(1)
	}

	if p.holdCmds[name] {
		holdNext.Store(p.stall.Load() > 0)
	}
}

func (p *stallProxy) forwardReplies(server, client net.Conn, holdNext *atomic.Bool) {
	buf := make([]byte, 32*1024)

	for {
		n, err := server.Read(buf)
		if err != nil {
			return
		}

		if holdNext.Swap(false) {
			p.hold()
		}

		if _, err := client.Write(buf[:n]); err != nil {
			return
		}
	}
}

// hold waits while a stall is set, for at most the stall duration.
func (p *stallProxy) hold() {
	start := time.Now()

	for {
		d := time.Duration(p.stall.Load())
		if d == 0 || time.Since(start) >= d {
			return
		}

		select {
		case <-p.done:
			return
		case <-time.After(5 * time.Millisecond):
		}
	}
}

const (
	stallPublishTimeout = 50 * time.Millisecond
	stallReadTimeout    = 400 * time.Millisecond
	stallReplyDelay     = 2 * time.Second
	// stallSlack absorbs scheduling and -race overhead; the budget plus the
	// slack stays well under stallReadTimeout, which is where the publish
	// would return if the deadline did not reach the socket.
	stallSlack = 150 * time.Millisecond
)

var stallJWTKey = []byte("publish-timeout-test-key-0123456789")

const (
	stallIssuer             = "https://example.com"
	stallResourceIdentifier = "https://example.com/.well-known/mercure"
)

// publisherJWT signs an HS256 access token allowed to publish on every topic.
func publisherJWT() string {
	enc := base64.RawURLEncoding
	input := enc.EncodeToString([]byte(`{"alg":"HS256","typ":"at+jwt"}`)) + "." +
		enc.EncodeToString([]byte(`{"iss":"`+stallIssuer+`","aud":"`+stallResourceIdentifier+`","exp":`+
			strconv.FormatInt(time.Now().Add(time.Hour).Unix(), 10)+
			`,"authorization_details":[{"type":"https://mercure.rocks/authorization-detail","actions":["publish"],"topics":[{"match":"*"}]}]}`))

	mac := hmac.New(sha256.New, stallJWTKey)
	mac.Write([]byte(input))

	return input + "." + enc.EncodeToString(mac.Sum(nil))
}

// newStalledTransport builds a transport whose client reaches miniredis
// through a stallProxy holding holdCmds' replies, with ContextTimeoutEnabled
// set as the Caddy module sets it. opts are applied after the defaults.
func newStalledTransport(
	t *testing.T, readTimeout time.Duration, maxRetries int, holdCmds []string, opts ...Option,
) (*RedisTransport, *stallProxy) {
	t.Helper()

	mr := miniredis.RunT(t)
	proxy := newStallProxy(t, mr.Addr(), holdCmds)

	client := redis.NewClient(&redis.Options{
		Addr:                  proxy.addr(),
		ReadTimeout:           readTimeout,
		MaxRetries:            maxRetries,
		ContextTimeoutEnabled: true,
	})

	transport, err := NewRedisTransport(client, append([]Option{
		WithLogger(testLogger()),
		WithXReadBlock(50 * time.Millisecond),
		WithHealthInterval(24 * time.Hour),
		WithPresenceInterval(24 * time.Hour),
		withSkipPresenceIntervalCheck(),
		withSkipVersionCheck(),
	}, opts...)...)
	require.NoError(t, err)

	t.Cleanup(func() {
		proxy.stall.Store(0)
		transport.Close(context.Background())
	})

	return transport, proxy
}

func newStalledHub(t *testing.T, transport *RedisTransport, publishTimeout time.Duration) *mercure.Hub {
	t.Helper()

	hub, err := mercure.NewHub(
		t.Context(),
		mercure.WithTransport(transport),
		mercure.WithIssuers([]mercure.Issuer{{
			Identifier: stallIssuer,
			Publisher:  mercure.Static{Key: stallJWTKey, Algorithm: "HS256"},
		}}),
		mercure.WithResourceIdentifier(stallResourceIdentifier),
		mercure.WithPublishTimeout(publishTimeout),
		mercure.WithLogger(testLogger()),
	)
	require.NoError(t, err)

	return hub
}

func hubPublish(t *testing.T, hub *mercure.Hub) (int, time.Duration) {
	t.Helper()

	form := url.Values{"topic": {"https://example.com/publish-timeout"}, "data": {"payload"}}
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/.well-known/mercure", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Authorization", "Bearer "+publisherJWT())

	w := httptest.NewRecorder()
	start := time.Now()

	hub.PublishHandler(w, req)

	return w.Code, time.Since(start)
}

func stalledDispatch(t *testing.T, transport *RedisTransport, budget time.Duration) (time.Duration, error) {
	t.Helper()

	ctx, cancel := context.WithTimeoutCause(t.Context(), budget, mercure.ErrPublishTimeout)
	defer cancel()

	start := time.Now()
	err := transport.Dispatch(ctx, &mercure.Update{
		Topics: []string{"https://example.com/publish-timeout"},
		Data:   "payload",
	})

	return time.Since(start), err
}

// TestPublishTimeoutCutsInFlightCommand pins publish_timeout against a
// command already on the socket: with ContextTimeoutEnabled the publish
// returns at the budget rather than at read_timeout, reports
// context.DeadlineExceeded (with retries on, go-redis's retry sleep returns
// it; with retries off, deadlineCutError adds it to the i/o timeout), the
// hub answers 504, and the script is sent once — no retry re-sends it after
// the deadline.
func TestPublishTimeoutCutsInFlightCommand(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name       string
		maxRetries int
	}{
		{"default retries", 0},
		{"retries disabled", -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			transport, proxy := newStalledTransport(t, stallReadTimeout, tc.maxRetries, scriptCmds)
			hub := newStalledHub(t, transport, stallPublishTimeout)

			// Warm up: loads the script and opens a pooled connection.
			code, _ := hubPublish(t, hub)
			require.Equal(t, http.StatusOK, code)

			proxy.stall.Store(int64(stallReplyDelay))
			proxy.scripts.Store(0)

			elapsed, err := stalledDispatch(t, transport, stallPublishTimeout)
			require.Error(t, err)
			require.ErrorIs(t, err, context.DeadlineExceeded,
				"a publish cut at publish_timeout must report the deadline: %v", err)
			assert.Less(t, elapsed, stallPublishTimeout+stallSlack,
				"Dispatch must return at publish_timeout, not at read_timeout (%v)", stallReadTimeout)
			assert.EqualValues(t, 1, proxy.scripts.Load(), "the script must be sent exactly once")

			proxy.scripts.Store(0)

			code, elapsed = hubPublish(t, hub)
			assert.Equal(t, http.StatusGatewayTimeout, code)
			assert.Less(t, elapsed, stallPublishTimeout+stallSlack,
				"the 504 must arrive at publish_timeout, not at read_timeout (%v)", stallReadTimeout)
			assert.EqualValues(t, 1, proxy.scripts.Load(), "the script must be sent exactly once")
		})
	}
}

// TestPublishReadTimeoutInsideBudgetStaysTransportError is the negative
// control: a read_timeout that fires while publish_timeout still has budget
// is a transport failure (500), not a publish timeout.
func TestPublishReadTimeoutInsideBudgetStaysTransportError(t *testing.T) {
	t.Parallel()

	const (
		readTimeout    = 50 * time.Millisecond
		publishTimeout = 1500 * time.Millisecond
	)

	transport, proxy := newStalledTransport(t, readTimeout, -1, scriptCmds)
	hub := newStalledHub(t, transport, publishTimeout)

	code, _ := hubPublish(t, hub)
	require.Equal(t, http.StatusOK, code)

	proxy.stall.Store(int64(stallReplyDelay))

	elapsed, err := stalledDispatch(t, transport, publishTimeout)
	require.Error(t, err)
	require.Less(t, elapsed, publishTimeout, "the read timeout must fire inside the budget")
	require.NotErrorIs(t, err, context.DeadlineExceeded,
		"a read_timeout inside the budget must not be reported as the deadline: %v", err)

	var netErr net.Error

	require.True(t, errors.As(err, &netErr) && netErr.Timeout(), "want the i/o timeout, got %v", err)

	code, elapsed = hubPublish(t, hub)
	require.Less(t, elapsed, publishTimeout, "the read timeout must fire inside the budget")
	assert.Equal(t, http.StatusInternalServerError, code)
}

// TestPublishTimeoutDeadlineRaceWithCtxTimer repeats the retries-disabled cut.
// The socket deadline and ctx's own timer expire together, and the read can
// return before ctx is marked done; deadlineCutError waits for ctx, so every
// cut reports the deadline rather than a bare i/o timeout.
func TestPublishTimeoutDeadlineRaceWithCtxTimer(t *testing.T) {
	t.Parallel()

	transport, proxy := newStalledTransport(t, stallReadTimeout, -1, scriptCmds)
	proxy.stall.Store(int64(stallReplyDelay))

	const rounds = 50

	var missed int

	for range rounds {
		ctx, cancel := context.WithTimeoutCause(t.Context(), 10*time.Millisecond, mercure.ErrPublishTimeout)
		err := transport.Dispatch(ctx, &mercure.Update{
			Topics: []string{"https://example.com/publish-timeout"},
			Data:   "payload",
		})

		if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(context.Cause(ctx), mercure.ErrPublishTimeout) {
			missed++
		}

		cancel()
	}

	assert.Zero(t, missed, "every cut publish must report the deadline, with ctx already done")
}
