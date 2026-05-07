package mercure

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testAddr        = "127.0.0.1:4242"
	testMetricsAddr = "127.0.0.1:4243"
)

func TestMain(m *testing.M) {
	flag.Parse()

	if !testing.Verbose() {
		slog.SetDefault(slog.New(slog.DiscardHandler))
	}

	os.Exit(m.Run())
}

func TestNewHub(t *testing.T) {
	t.Parallel()

	h := createDummy(t)

	assert.False(t, h.anonymous)
	assert.Equal(t, defaultCookieName, h.cookieName)
	assert.Equal(t, 40*time.Second, h.heartbeat)
	assert.Equal(t, 5*time.Second, h.dispatchTimeout)
	assert.Equal(t, 600*time.Second, h.writeTimeout)
}

func TestNewHubWithConfig(t *testing.T) {
	t.Parallel()

	h, err := NewHub(
		t.Context(),
		WithPublisherJWT([]byte("foo"), jwt.SigningMethodHS256.Name),
		WithSubscriberJWT([]byte("bar"), jwt.SigningMethodHS256.Name),
	)
	require.NotNil(t, h)
	require.NoError(t, err)
}

func TestStop(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		hub := createAnonymousDummy(t)
		ctx := t.Context()

		go func() {
			s := hub.transport.(*LocalTransport)

			var ready bool

			for !ready {
				s.RLock()
				ready = s.subscribers.Len() == 2
				s.RUnlock()
			}

			assert.NoError(t, hub.transport.Dispatch(ctx, &Update{
				Topics: []string{"https://example.com/foo"},
				Event:  Event{Data: "Hello World"},
			}))

			assert.NoError(t, hub.Stop(ctx))
		}()

		for range 2 {
			go func() {
				req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?topic=https://example.com/foo", nil)

				w := newSubscribeRecorder()
				hub.SubscribeHandler(w, req)

				r := w.Result()
				assert.NoError(t, r.Body.Close())
				assert.Equal(t, 200, r.StatusCode)
			}()
		}

		synctest.Wait()
	})
}

func TestContextCancellation(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		t.Cleanup(cancel)

		hub, err := NewHub(ctx, WithAnonymous())
		require.NoError(t, err)

		t.Cleanup(func() {
			require.NoError(t, hub.Stop(ctx))
		})

		go func() {
			s := hub.transport.(*LocalTransport)

			var ready bool

			for !ready {
				s.RLock()
				ready = s.subscribers.Len() == 2
				s.RUnlock()
			}

			cancel()
		}()

		for range 2 {
			go func() {
				req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?topic=https://example.com/foo", nil)

				w := newSubscribeRecorder()
				hub.SubscribeHandler(w, req)

				r := w.Result()
				_ = r.Body.Close()
				assert.Equal(t, 200, r.StatusCode)
			}()
		}

		synctest.Wait()
	})
}

func TestWithProtocolVersionCompatibility(t *testing.T) {
	t.Parallel()

	op := &opt{}

	assert.False(t, op.isBackwardCompatiblyEnabledWith(7))

	o := WithProtocolVersionCompatibility(7)
	require.NoError(t, o(op))
	assert.Equal(t, 7, op.protocolVersionCompatibility)
	assert.True(t, op.isBackwardCompatiblyEnabledWith(7))
	assert.True(t, op.isBackwardCompatiblyEnabledWith(8))
	assert.False(t, op.isBackwardCompatiblyEnabledWith(6))
}

func TestWithProtocolVersionCompatibilityVersions(t *testing.T) {
	t.Parallel()

	op := &opt{}

	testCases := []struct {
		version int
		ok      bool
	}{
		{5, false},
		{6, false},
		{7, true},
		{8, false},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("version %d", tc.version), func(t *testing.T) {
			t.Parallel()

			o := WithProtocolVersionCompatibility(tc.version)

			if tc.ok {
				require.NoError(t, o(op))
			} else {
				require.Error(t, o(op))
			}
		})
	}
}

func TestWithPublisherJWTKeyFunc(t *testing.T) {
	t.Parallel()

	op := &opt{}

	o := WithPublisherJWTKeyFunc(func(_ *jwt.Token) (any, error) { return []byte{}, nil })
	require.NoError(t, o(op))
	require.NotNil(t, op.publisherJWTKeyFunc)
}

func TestWithSubscriberJWTKeyFunc(t *testing.T) {
	t.Parallel()

	op := &opt{}

	o := WithSubscriberJWTKeyFunc(func(_ *jwt.Token) (any, error) { return []byte{}, nil })
	require.NoError(t, o(op))
	require.NotNil(t, op.subscriberJWTKeyFunc)
}

// nopTransport is a minimal Transport whose methods all return nil. Shared
// across hub tests as a base for transports that need extra interfaces
// (codecRecordingTransport composes it) and as a standalone no-op transport
// for opt-out branches (TestNewHubWithCodecSkipsTransportWithoutCodec).
// Using a local nopTransport avoids pulling in LocalTransport's
// SubscriberList machinery for tests that don't exercise dispatch.
type nopTransport struct{}

func (nopTransport) Dispatch(_ context.Context, _ *Update) error                  { return nil }
func (nopTransport) AddSubscriber(_ context.Context, _ *LocalSubscriber) error    { return nil }
func (nopTransport) RemoveSubscriber(_ context.Context, _ *LocalSubscriber) error { return nil }
func (nopTransport) Close(_ context.Context) error                                { return nil }

// codecRecordingTransport IS a nopTransport plus codec-recording state.
// Anonymous embedding promotes the four Transport methods automatically;
// SetCodec + Dispatch + AddSubscriber + RemoveSubscriber are shadowed
// here to record call ordering. Tests assert NewHub invokes SetCodec
// with the hub-configured codec at initialization.
//
// dispatchSeen / addSubscriberSeen / removeSubscriberSeen pin the
// SetCodec ordering contract per transport.go's TransportCodec docstring:
// SetCodec is invoked exactly once at hub initialization, before any
// Dispatch / AddSubscriber / RemoveSubscriber call. Each method-shadow
// sets its corresponding flag; SetCodec asserts they are all false at
// call time.
//
// TRIPWIRE: if the Transport interface gains a new method, anonymous
// embedding will silently delegate to nopTransport's no-op and ordering
// tracking for that method WON'T be added unless the new method is
// shadowed here. Extend the shadowed-method set if Transport grows.
type codecRecordingTransport struct {
	nopTransport

	codecSetTo            Codec
	setCalls              int
	dispatchSeen          bool
	addSubscriberSeen     bool
	removeSubscriberSeen  bool
	setCodecCalledAfterTx bool
}

func (t *codecRecordingTransport) Dispatch(_ context.Context, _ *Update) error {
	t.dispatchSeen = true

	return nil
}

func (t *codecRecordingTransport) AddSubscriber(_ context.Context, _ *LocalSubscriber) error {
	t.addSubscriberSeen = true

	return nil
}

func (t *codecRecordingTransport) RemoveSubscriber(_ context.Context, _ *LocalSubscriber) error {
	t.removeSubscriberSeen = true

	return nil
}

func (t *codecRecordingTransport) SetCodec(c Codec) {
	if t.dispatchSeen || t.addSubscriberSeen || t.removeSubscriberSeen {
		t.setCodecCalledAfterTx = true
	}

	t.codecSetTo = c
	t.setCalls++
}

var (
	_ Transport      = nopTransport{}
	_ Transport      = (*codecRecordingTransport)(nil)
	_ TransportCodec = (*codecRecordingTransport)(nil)
)

// TestNewHubWithCodecSetsCodecOnTransport locks the end-to-end contract:
// when NewHub is constructed with WithCodec AND a transport that
// implements TransportCodec, SetCodec must be called exactly once with
// the configured codec at initialization.
func TestNewHubWithCodecSetsCodecOnTransport(t *testing.T) {
	t.Parallel()

	transport := &codecRecordingTransport{}
	codec := &JSONCodec{}

	h, err := NewHub(t.Context(), WithTransport(transport), WithCodec(codec), WithTopicSelectorStore(&TopicSelectorStore{}))
	require.NoError(t, err)
	require.NotNil(t, h)

	t.Cleanup(func() { _ = h.Stop(context.Background()) })

	assert.Equal(t, 1, transport.setCalls,
		"SetCodec must be invoked exactly once at hub init when transport implements TransportCodec")
	assert.Same(t, Codec(codec), transport.codecSetTo,
		"the codec passed to WithCodec must reach the transport verbatim")
	assert.False(t, transport.setCodecCalledAfterTx,
		"SetCodec MUST be called before any Dispatch/AddSubscriber/RemoveSubscriber per transport.go's TransportCodec contract")
}

// TestNewHubNoCodecSkipsTransportCodec closes the WithCodec 2×2 matrix:
// when WithCodec is NOT supplied, even a TransportCodec-implementing
// transport must NOT receive a SetCodec call. Without this, a refactor
// that lost the `if opt.codec != nil` guard at hub.go would silently call
// SetCodec(nil) on every codec-aware transport.
func TestNewHubNoCodecSkipsTransportCodec(t *testing.T) {
	t.Parallel()

	transport := &codecRecordingTransport{}

	h, err := NewHub(t.Context(), WithTransport(transport), WithTopicSelectorStore(&TopicSelectorStore{}))
	require.NoError(t, err)
	require.NotNil(t, h)

	t.Cleanup(func() { _ = h.Stop(context.Background()) })

	assert.Equal(t, 0, transport.setCalls,
		"no WithCodec option: SetCodec must NOT be invoked even on a TransportCodec-implementing transport")
}

// TestNewHubWithCodecNil locks WithCodec(nil) semantics: it must be
// equivalent to omitting the option entirely — silently disable, no
// SetCodec call, no panic. Without this contract pin, a future refactor
// (e.g. moving the nil-guard from hub.go's NewHub into the SetCodec call
// site) could regress to passing nil to transport.SetCodec.
func TestNewHubWithCodecNil(t *testing.T) {
	t.Parallel()

	transport := &codecRecordingTransport{}

	h, err := NewHub(t.Context(), WithTransport(transport), WithCodec(nil), WithTopicSelectorStore(&TopicSelectorStore{}))
	require.NoError(t, err)
	require.NotNil(t, h)

	t.Cleanup(func() { _ = h.Stop(context.Background()) })

	assert.Equal(t, 0, transport.setCalls,
		"WithCodec(nil) must be equivalent to omitting the option — no SetCodec call, no panic")
}

// TestNewHubWithCodecSkipsTransportWithoutCodec locks the fork's
// no-panic contract for the dispatch step: when WithCodec is supplied
// but the transport does NOT implement TransportCodec (e.g. the
// upstream LocalTransport, Bolt), NewHub must succeed silently — the
// codec wiring is a transport-opt-in feature.
func TestNewHubWithCodecSkipsTransportWithoutCodec(t *testing.T) {
	t.Parallel()

	transport := &nopTransport{}
	codec := &JSONCodec{}

	h, err := NewHub(t.Context(), WithTransport(transport), WithCodec(codec), WithTopicSelectorStore(&TopicSelectorStore{}))
	require.NoError(t, err)
	require.NotNil(t, h)

	t.Cleanup(func() { _ = h.Stop(context.Background()) })
}

func TestWithDebug(t *testing.T) {
	op := &opt{}

	o := WithDebug()
	require.NoError(t, o(op))
	require.True(t, op.debug)
}

func TestWithUI(t *testing.T) {
	t.Parallel()

	op := &opt{}

	o := WithUI()
	require.NoError(t, o(op))
	require.True(t, op.ui)
}

func TestUIFixturesGating(t *testing.T) {
	t.Parallel()

	// Paths the gate must block when ui:true is set without demo:true.
	// Each one resolves to public/fixtures/* via mux's UseEncodedPath +
	// http.FileServer's internal decode/clean, so a plain PathPrefix matcher
	// against the raw URI would let them through.
	uiBlocked := []struct {
		name string
		path string
	}{
		{"canonical", defaultUIURL + "fixtures/jwks.json"},
		{"encoded f", defaultUIURL + "%66ixtures/jwks.json"},
		{"encoded slash", defaultUIURL + "fixtures%2Fjwks.json"},
		{"double slash", defaultHubURL + "/ui//fixtures/jwks.json"},
		{"dot-dot traversal", defaultUIURL + "x/../fixtures/jwks.json"},
		{"directory no-slash", defaultUIURL + "fixtures"},
		{"sensitive file", defaultUIURL + "fixtures/private-jwk.json"},
	}

	for _, tc := range uiBlocked {
		t.Run("ui blocks "+tc.name, func(t *testing.T) {
			t.Parallel()

			hub := createAnonymousDummy(t, WithUI())

			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, tc.path, nil)
			hub.ServeHTTP(w, req)

			assert.Equal(t, http.StatusNotFound, w.Code, "path=%s", tc.path)
		})
	}

	t.Run("demo serves fixtures", func(t *testing.T) {
		t.Parallel()

		hub := createAnonymousDummy(t, WithDemo())

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, defaultUIURL+"fixtures/jwks.json", nil)
		hub.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
	})

	// The UI root must remain reachable in both gating modes — the gate
	// only narrows the fixtures subtree, not the UI itself.
	for _, tc := range []struct {
		name   string
		option Option
	}{
		{"ui only", WithUI()},
		{"demo", WithDemo()},
	} {
		t.Run("ui root reachable: "+tc.name, func(t *testing.T) {
			t.Parallel()

			hub := createAnonymousDummy(t, tc.option)

			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, defaultUIURL, nil)
			hub.ServeHTTP(w, req)

			assert.Equal(t, http.StatusOK, w.Code)
		})
	}
}

func TestOriginsValidator(t *testing.T) {
	t.Parallel()

	op := &opt{}

	validOrigins := [][]string{
		{"*"},
		{"null"},
		{"https://example.com"},
		{"https://example.com:8000"},
		{"https://example.com", "https://example.org"},
		{"https://example.com", "*"},
		{"null", "https://example.com:3000"},
		{"capacitor://"},
		{"capacitor://www.example.com"},
		{"ionic://"},
		{"foobar://"},
		{"https://*.example.com"},
	}

	invalidOrigins := [][]string{
		{"f"},
		{"foo"},
		{"https://example.com", "bar"},
		{"https://example.com/"},
		{"https://user@example.com"},
		{"https://example.com:abc"},
		{"https://example.com", "https://example.org/hello"},
		{"https://example.com?query", "*"},
		{"null", "https://example.com:3000#fragment"},
	}

	for _, origins := range validOrigins {
		o := WithPublishOrigins(origins)
		require.NoError(t, o(op), "error while not expected for %#v", origins)

		o = WithCORSOrigins(origins)
		require.NoError(t, o(op), "error while not expected for %#v", origins)
	}

	for _, origins := range invalidOrigins {
		o := WithPublishOrigins(origins)
		require.Error(t, o(op), "no error while expected for %#v", origins)

		o = WithCORSOrigins(origins)
		require.Error(t, o(op), "no error while expected for %#v", origins)
	}
}

func TestSecurityHeaders(t *testing.T) {
	t.Parallel()

	hub := createAnonymousDummy(t, WithSubscriptions(), WithCORSOrigins([]string{"https://example.com"}), WithDemo())

	form := url.Values{}
	form.Add("id", "id")
	form.Add("topic", "https://example.com/books/1")
	form.Add("data", "Hello!")
	form.Add("private", "on")

	req := httptest.NewRequest(http.MethodPost, defaultHubURL, strings.NewReader(form.Encode()))
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Add("Authorization", bearerPrefix+createDummyAuthorizedJWT(rolePublisher, []string{"*"}))

	w := httptest.NewRecorder()
	hub.ServeHTTP(w, req)

	resp := w.Result()

	t.Cleanup(func() {
		assert.NoError(t, resp.Body.Close())
	})

	assert.Equal(t, "default-src 'self' mercure.rocks cdn.jsdelivr.net cdnjs.cloudflare.com fonts.googleapis.com; script-src 'self' cdn.jsdelivr.net cdnjs.cloudflare.com; style-src 'self' 'unsafe-inline' cdn.jsdelivr.net cdnjs.cloudflare.com fonts.googleapis.com; font-src 'self' fonts.gstatic.com cdnjs.cloudflare.com data:; connect-src 'self' cdn.jsdelivr.net", resp.Header.Get("Content-Security-Policy"))
	assert.Equal(t, "nosniff", resp.Header.Get("X-Content-Type-Options"))
	assert.Equal(t, "DENY", resp.Header.Get("X-Frame-Options"))
	assert.Equal(t, "1; mode=block", resp.Header.Get("X-Xss-Protection"))
	require.NoError(t, resp.Body.Close())

	// Preflight request
	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodOptions, defaultHubURL, nil)
	req.Header.Add("Origin", "https://example.com")
	req.Header.Add("Access-Control-Request-Headers", "authorization,cache-control,last-event-id")
	req.Header.Add("Access-Control-Request-Method", http.MethodGet)
	hub.ServeHTTP(w, req)

	resp2 := w.Result()
	require.NotNil(t, resp2)

	assert.Equal(t, "true", resp2.Header.Get("Access-Control-Allow-Credentials"))
	assert.Equal(t, "authorization,cache-control,last-event-id", resp2.Header.Get("Access-Control-Allow-Headers"))
	assert.Equal(t, "https://example.com", resp2.Header.Get("Access-Control-Allow-Origin"))
	require.NoError(t, resp2.Body.Close())

	// Subscriptions
	w = httptest.NewRecorder()
	req, _ = http.NewRequest(http.MethodGet, defaultHubURL+subscriptionsPath, nil)
	hub.ServeHTTP(w, req)
	resp3 := w.Result()

	require.NotNil(t, resp3)
	assert.Equal(t, http.StatusUnauthorized, resp3.StatusCode)
	require.NoError(t, resp3.Body.Close())

	// Cross-origin GET should expose Link header in demo mode
	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, defaultDemoURL+"test.jsonld", nil)
	req.Header.Add("Origin", "https://example.com")
	hub.ServeHTTP(w, req)
	resp4 := w.Result()

	assert.Equal(t, "Link", resp4.Header.Get("Access-Control-Expose-Headers"))
	require.NoError(t, resp4.Body.Close())
}

func TestWithPublishDisabled(t *testing.T) {
	t.Parallel()

	h, err := NewHub(t.Context(), WithAnonymous())
	require.NoError(t, err)

	w := httptest.NewRecorder()

	h.ServeHTTP(w, httptest.NewRequest(http.MethodPost, defaultHubURL, nil))

	assert.Equal(t, http.StatusMethodNotAllowed, w.Code)
}

func TestWithSubscribeDisabled(t *testing.T) {
	t.Parallel()

	h, err := NewHub(t.Context(), WithPublisherJWT([]byte(""), "HS256"))
	require.NoError(t, err)

	w := httptest.NewRecorder()

	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, defaultHubURL, nil))

	assert.Equal(t, http.StatusMethodNotAllowed, w.Code)
}

// waitForSubscriber waits until the LocalTransport reports at least one
// live subscriber. Used to synchronize tests that need a subscriber
// registered before a Dispatch or context cancel runs. Sleeps between
// checks to avoid busy-spinning, and fails the test via tb.Fatalf after
// 5s so a genuinely stuck expectation surfaces as a clear error instead
// of a hung suite.
//
// The signature deliberately encodes "≥1 subscriber" as the contract.
// If a future test needs to assert exactly N subscribers ready, add a
// sibling helper rather than overloading this one.
func waitForSubscriber(tb testing.TB, transport *LocalTransport) {
	tb.Helper()

	const timeout = 5 * time.Second

	deadline := time.Now().Add(timeout)

	for {
		transport.RLock()
		got := transport.subscribers.Len()
		transport.RUnlock()

		if got >= 1 {
			return
		}

		if !time.Now().Before(deadline) {
			tb.Fatalf("waited %s for subscriber, have %d", timeout, got)
		}

		time.Sleep(time.Millisecond)
	}
}

func createDummy(tb testing.TB, options ...Option) *Hub {
	tb.Helper()

	tss, err := NewTopicSelectorStore(0)
	require.NoError(tb, err)

	options = append(
		[]Option{
			WithPublisherJWT([]byte("publisher"), jwt.SigningMethodHS256.Name),
			WithSubscriberJWT([]byte("subscriber"), jwt.SigningMethodHS256.Name),
			WithTopicSelectorStore(tss),
		},
		options...,
	)

	h, err := NewHub(tb.Context(), options...)
	require.NoError(tb, err)

	setDeprecatedOptions(tb, h)

	return h
}

func createAnonymousDummy(tb testing.TB, options ...Option) *Hub {
	tb.Helper()

	options = append(
		[]Option{WithAnonymous()},
		options...,
	)

	return createDummy(tb, options...)
}

func createDummyAuthorizedJWT(r role, topics []string) string {
	return createDummyAuthorizedJWTWithPayload(r, topics, struct {
		Foo string `json:"foo"`
	}{Foo: "bar"})
}

func createDummyAuthorizedJWTWithPayload(r role, topics []string, payload any) string {
	token := jwt.New(jwt.SigningMethodHS256)

	var key []byte

	switch r {
	case rolePublisher:
		token.Claims = &claims{Mercure: mercureClaim{Publish: topics}, RegisteredClaims: jwt.RegisteredClaims{}}
		key = []byte("publisher")

	case roleSubscriber:
		token.Claims = &claims{
			Mercure: mercureClaim{
				Subscribe: topics,
				Payload:   payload,
			},
			RegisteredClaims: jwt.RegisteredClaims{},
		}

		key = []byte("subscriber")
	}

	tokenString, _ := token.SignedString(key)

	return tokenString
}

func createDummyUnauthorizedJWT() string {
	token := jwt.New(jwt.SigningMethodHS256)
	tokenString, _ := token.SignedString([]byte("unauthorized"))

	return tokenString
}

func createDummyNoneSignedJWT() string {
	token := jwt.New(jwt.SigningMethodNone)
	// The generated token must have more than 41 chars
	token.Claims = jwt.RegisteredClaims{Subject: "me"}
	tokenString, _ := token.SignedString(jwt.UnsafeAllowNoneSignatureType)

	return tokenString
}
