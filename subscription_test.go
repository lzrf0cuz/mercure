package mercure

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// errGetSubscribers is returned by getSubscribersErrorTransport.GetSubscribers
// to drive the subscription handler's error path in regression tests.
var errGetSubscribers = errors.New("forced GetSubscribers failure")

// getSubscribersErrorTransport is a Transport+TransportSubscribers that
// always errors on GetSubscribers. Used to lock in the fix for the
// initSubscription "ok=true after error" bug at subscription.go:151 —
// before the fix, the handler would write a 500 status AND continue to
// emit a 200 body, mangling the response.
type getSubscribersErrorTransport struct{}

func (*getSubscribersErrorTransport) Dispatch(_ context.Context, _ *Update) error {
	return nil
}

func (*getSubscribersErrorTransport) AddSubscriber(_ context.Context, _ *LocalSubscriber) error {
	return nil
}

func (*getSubscribersErrorTransport) RemoveSubscriber(_ context.Context, _ *LocalSubscriber) error {
	return nil
}

func (*getSubscribersErrorTransport) Close(_ context.Context) error {
	return nil
}

func (*getSubscribersErrorTransport) GetSubscribers(_ context.Context) (string, []*Subscriber, error) {
	return "", nil, errGetSubscribers
}

// Compile-time assertion that the mock satisfies both interfaces.
var (
	_ Transport            = (*getSubscribersErrorTransport)(nil)
	_ TransportSubscribers = (*getSubscribersErrorTransport)(nil)
)

func TestSubscriptionsHandlerAccessDenied(t *testing.T) {
	t.Parallel()

	hub := createDummy(t)

	req := httptest.NewRequest(http.MethodGet, subscriptionsURL, nil)
	w := httptest.NewRecorder()
	hub.SubscriptionsHandler(w, req)
	res := w.Result()
	assert.Equal(t, http.StatusUnauthorized, res.StatusCode)
	require.NoError(t, res.Body.Close())

	req = httptest.NewRequest(http.MethodGet, subscriptionsURL, nil)
	req.AddCookie(&http.Cookie{Name: "mercureAuthorization", Value: createDummyAuthorizedJWT(roleSubscriber, []string{"/.well-known/mercure/subscriptions/foo{/subscriber}"}), HttpOnly: true, Secure: true, SameSite: http.SameSiteStrictMode})

	w = httptest.NewRecorder()
	hub.SubscriptionsHandler(w, req)
	res = w.Result()
	assert.Equal(t, http.StatusUnauthorized, res.StatusCode)
	require.NoError(t, res.Body.Close())

	req = httptest.NewRequest(http.MethodGet, defaultHubURL+subscriptionsPath+"/bar", nil)
	req.AddCookie(&http.Cookie{Name: "mercureAuthorization", Value: createDummyAuthorizedJWT(roleSubscriber, []string{"/.well-known/mercure/subscriptions/foo{/subscriber}"}), HttpOnly: true, Secure: true, SameSite: http.SameSiteStrictMode})

	w = httptest.NewRecorder()
	hub.SubscriptionsHandler(w, req)
	res = w.Result()
	assert.Equal(t, http.StatusUnauthorized, res.StatusCode)
	require.NoError(t, res.Body.Close())
}

func TestSubscriptionHandlerAccessDenied(t *testing.T) {
	t.Parallel()

	hub := createDummy(t)

	req := httptest.NewRequest(http.MethodGet, defaultHubURL+subscriptionsPath+"/bar/baz", nil)
	w := httptest.NewRecorder()
	hub.SubscriptionHandler(w, req)
	res := w.Result()
	assert.Equal(t, http.StatusUnauthorized, res.StatusCode)
	require.NoError(t, res.Body.Close())

	req = httptest.NewRequest(http.MethodGet, defaultHubURL+subscriptionsPath+"/bar/baz", nil)
	req.AddCookie(&http.Cookie{Name: "mercureAuthorization", Value: createDummyAuthorizedJWT(roleSubscriber, []string{"/.well-known/mercure/subscriptions/foo{/subscriber}"}), HttpOnly: true, Secure: true, SameSite: http.SameSiteStrictMode})

	w = httptest.NewRecorder()
	hub.SubscriptionHandler(w, req)
	res = w.Result()
	assert.Equal(t, http.StatusUnauthorized, res.StatusCode)
	require.NoError(t, res.Body.Close())
}

func TestSubscriptionHandlersETag(t *testing.T) {
	t.Parallel()

	hub := createDummy(t)

	req := httptest.NewRequest(http.MethodGet, defaultHubURL+subscriptionsPath, nil)
	req.Header.Add("If-None-Match", EarliestLastEventID)
	req.AddCookie(&http.Cookie{Name: "mercureAuthorization", Value: createDummyAuthorizedJWT(roleSubscriber, []string{"/.well-known/mercure/subscriptions"}), HttpOnly: true, Secure: true, SameSite: http.SameSiteStrictMode})

	w := httptest.NewRecorder()
	hub.SubscriptionsHandler(w, req)
	res := w.Result()
	assert.Equal(t, http.StatusNotModified, res.StatusCode)
	require.NoError(t, res.Body.Close())

	req = httptest.NewRequest(http.MethodGet, defaultHubURL+subscriptionsPath+"/foo/bar", nil)
	req.Header.Add("If-None-Match", EarliestLastEventID)
	req.AddCookie(&http.Cookie{Name: "mercureAuthorization", Value: createDummyAuthorizedJWT(roleSubscriber, []string{"/.well-known/mercure/subscriptions/foo/bar"}), HttpOnly: true, Secure: true, SameSite: http.SameSiteStrictMode})

	w = httptest.NewRecorder()
	hub.SubscriptionHandler(w, req)
	res = w.Result()
	assert.Equal(t, http.StatusNotModified, res.StatusCode)
	require.NoError(t, res.Body.Close())
}

func TestSubscriptionsHandler(t *testing.T) {
	t.Parallel()

	hub := createDummy(t)
	tss := &TopicSelectorStore{}
	logger := slog.Default()
	ctx := t.Context()

	s1 := NewLocalSubscriber("", logger, tss)
	s1.SetTopics([]string{"https://example.com/foo"}, nil)
	require.NoError(t, hub.transport.AddSubscriber(ctx, s1))

	s2 := NewLocalSubscriber("", logger, tss)
	s2.SetTopics([]string{"https://example.com/bar"}, nil)
	require.NoError(t, hub.transport.AddSubscriber(ctx, s2))

	req := httptest.NewRequest(http.MethodGet, defaultHubURL+subscriptionsPath, nil)
	req.AddCookie(&http.Cookie{Name: "mercureAuthorization", Value: createDummyAuthorizedJWT(roleSubscriber, []string{"/.well-known/mercure/subscriptions"}), HttpOnly: true, Secure: true, SameSite: http.SameSiteStrictMode})

	w := httptest.NewRecorder()
	hub.SubscriptionsHandler(w, req)
	res := w.Result()
	assert.Equal(t, http.StatusOK, res.StatusCode)
	require.NoError(t, res.Body.Close())

	var subscriptions subscriptionCollection
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &subscriptions))

	assert.Equal(t, "https://mercure.rocks/", subscriptions.Context)
	assert.Equal(t, subscriptionsURL, subscriptions.ID)
	assert.Equal(t, "Subscriptions", subscriptions.Type)

	lastEventID, subscribers, _ := hub.transport.(TransportSubscribers).GetSubscribers(t.Context())

	assert.Equal(t, lastEventID, subscriptions.LastEventID)
	require.NotEmpty(t, subscribers)

	for _, s := range subscribers {
		currentSubs := s.getSubscriptions("", "", true)
		require.NotEmpty(t, currentSubs)

		for _, sub := range currentSubs {
			assert.Contains(t, subscriptions.Subscriptions, sub)
		}
	}
}

func TestSubscriptionsHandlerForTopic(t *testing.T) {
	t.Parallel()

	hub := createDummy(t)
	tss := &TopicSelectorStore{}
	ctx := t.Context()
	logger := slog.Default()

	s1 := NewLocalSubscriber("", logger, tss)
	s1.SetTopics([]string{"https://example.com/foo"}, nil)
	require.NoError(t, hub.transport.AddSubscriber(ctx, s1))

	s2 := NewLocalSubscriber("", logger, tss)
	s2.SetTopics([]string{"https://example.com/bar"}, nil)
	require.NoError(t, hub.transport.AddSubscriber(ctx, s2))

	escapedBarTopic := url.QueryEscape("https://example.com/bar")

	router := mux.NewRouter()
	router.UseEncodedPath()
	router.SkipClean(true)
	router.HandleFunc(subscriptionsForTopicURL, hub.SubscriptionsHandler)

	req := httptest.NewRequest(http.MethodGet, defaultHubURL+subscriptionsPath+"/"+s2.EscapedTopics[0], nil)
	req.AddCookie(&http.Cookie{Name: "mercureAuthorization", Value: createDummyAuthorizedJWT(roleSubscriber, []string{"/.well-known/mercure/subscriptions/" + s2.EscapedTopics[0]}), HttpOnly: true, Secure: true, SameSite: http.SameSiteStrictMode})

	w := httptest.NewRecorder()
	hub.SubscriptionsHandler(w, req)
	res := w.Result()
	assert.Equal(t, http.StatusOK, res.StatusCode)
	require.NoError(t, res.Body.Close())

	var subscriptions subscriptionCollection
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &subscriptions))

	assert.Equal(t, "https://mercure.rocks/", subscriptions.Context)
	assert.Equal(t, defaultHubURL+subscriptionsPath+"/"+escapedBarTopic, subscriptions.ID)
	assert.Equal(t, "Subscriptions", subscriptions.Type)

	lastEventID, subscribers, _ := hub.transport.(TransportSubscribers).GetSubscribers(t.Context())

	assert.Equal(t, lastEventID, subscriptions.LastEventID)
	require.NotEmpty(t, subscribers)

	for _, s := range subscribers {
		for _, sub := range s.getSubscriptions("https://example.com/bar", "", true) {
			require.NotContains(t, "foo", sub.Topic)
			assert.Contains(t, subscriptions.Subscriptions, sub)
		}
	}
}

func TestSubscriptionHandler(t *testing.T) {
	t.Parallel()

	hub := createDummy(t)
	tss := &TopicSelectorStore{}
	ctx := t.Context()
	logger := slog.Default()

	otherS := NewLocalSubscriber("", logger, tss)
	otherS.SetTopics([]string{"https://example.com/other"}, nil)
	require.NoError(t, hub.transport.AddSubscriber(ctx, otherS))

	s := NewLocalSubscriber("", logger, tss)
	s.SetTopics([]string{"https://example.com/other", "https://example.com/{foo}"}, nil)
	require.NoError(t, hub.transport.AddSubscriber(ctx, s))

	router := mux.NewRouter()
	router.UseEncodedPath()
	router.SkipClean(true)
	router.HandleFunc(subscriptionURL, hub.SubscriptionHandler)

	req := httptest.NewRequest(http.MethodGet, defaultHubURL+subscriptionsPath+"/"+s.EscapedTopics[1]+"/"+s.EscapedID, nil)
	req.AddCookie(&http.Cookie{Name: "mercureAuthorization", Value: createDummyAuthorizedJWT(roleSubscriber, []string{"/.well-known/mercure/subscriptions{/topic}{/subscriber}"}), HttpOnly: true, Secure: true, SameSite: http.SameSiteStrictMode})

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	res := w.Result()
	assert.Equal(t, http.StatusOK, res.StatusCode)
	require.NoError(t, res.Body.Close())

	var subscription subscription
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &subscription))

	expectedSub := s.getSubscriptions(s.SubscribedTopics[1], "https://mercure.rocks/", true)[0]
	expectedSub.LastEventID, _, _ = hub.transport.(TransportSubscribers).GetSubscribers(t.Context())
	assert.Equal(t, expectedSub, subscription)

	req = httptest.NewRequest(http.MethodGet, defaultHubURL+subscriptionsPath+"/notexist/"+s.EscapedID, nil)
	req.AddCookie(&http.Cookie{Name: "mercureAuthorization", Value: createDummyAuthorizedJWT(roleSubscriber, []string{"/.well-known/mercure/subscriptions{/topic}{/subscriber}"}), HttpOnly: true, Secure: true, SameSite: http.SameSiteStrictMode})

	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	res = w.Result()
	assert.Equal(t, http.StatusNotFound, res.StatusCode)
	require.NoError(t, res.Body.Close())
}

// TestSubscriptionsHandlerGetSubscribersError locks in the fix for
// subscription.go:151 — when GetSubscribers errors, initSubscription must
// return ok=false (was returning ok=true from the prior type-assertion
// shadow). Before the fix, the handler would emit a 500 status line via
// http.Error AND continue into the 200-body write path, producing a
// response with mixed/mangled content.
func TestSubscriptionsHandlerGetSubscribersError(t *testing.T) {
	t.Parallel()

	hub := createDummy(t, WithTransport(&getSubscribersErrorTransport{}))

	req := httptest.NewRequest(http.MethodGet, defaultHubURL+subscriptionsPath, nil)
	req.AddCookie(&http.Cookie{Name: "mercureAuthorization", Value: createDummyAuthorizedJWT(roleSubscriber, []string{"/.well-known/mercure/subscriptions"}), HttpOnly: true, Secure: true, SameSite: http.SameSiteStrictMode})

	w := httptest.NewRecorder()
	hub.SubscriptionsHandler(w, req)
	res := w.Result()

	t.Cleanup(func() { _ = res.Body.Close() })

	assert.Equal(t, http.StatusInternalServerError, res.StatusCode)
	// Body must NOT contain a JSON-LD subscription collection — that would
	// indicate the handler proceeded past the error.
	assert.NotContains(t, w.Body.String(), `"@context"`)
	assert.NotContains(t, w.Body.String(), `"Subscriptions"`)
}

func TestSubscriptionHandlerGetSubscribersError(t *testing.T) {
	t.Parallel()

	hub := createDummy(t, WithTransport(&getSubscribersErrorTransport{}))

	req := httptest.NewRequest(http.MethodGet, defaultHubURL+subscriptionsPath+"/topic/sub", nil)
	req.AddCookie(&http.Cookie{Name: "mercureAuthorization", Value: createDummyAuthorizedJWT(roleSubscriber, []string{"/.well-known/mercure/subscriptions{/topic}{/subscriber}"}), HttpOnly: true, Secure: true, SameSite: http.SameSiteStrictMode})

	router := mux.NewRouter()
	router.HandleFunc(subscriptionURL, hub.SubscriptionHandler)

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	res := w.Result()

	t.Cleanup(func() { _ = res.Body.Close() })

	assert.Equal(t, http.StatusInternalServerError, res.StatusCode)
	assert.NotContains(t, w.Body.String(), `"@context"`)
}

// nonSubscribersTransport implements Transport but NOT TransportSubscribers.
// Used to exercise initSubscription's type-assertion-failure branch — the
// defensive 500 path that handler.go's registerSubscriptionHandlers
// normally prevents from being reached by skipping route registration.
//
// DO NOT add a GetSubscribers method to this type. Adding it would make the
// type satisfy TransportSubscribers, the type-assertion in initSubscription
// would suddenly succeed, and TestSubscriptionsHandlerTransportWithoutSubscribers
// would silently start exercising a different code path.
type nonSubscribersTransport struct{}

func (*nonSubscribersTransport) Dispatch(_ context.Context, _ *Update) error { return nil }

func (*nonSubscribersTransport) AddSubscriber(_ context.Context, _ *LocalSubscriber) error {
	return nil
}

func (*nonSubscribersTransport) RemoveSubscriber(_ context.Context, _ *LocalSubscriber) error {
	return nil
}

func (*nonSubscribersTransport) Close(_ context.Context) error { return nil }

var _ Transport = (*nonSubscribersTransport)(nil)

// TestSubscriptionHandlersTransportWithoutSubscribers locks the contract
// that initSubscription's type-assertion-failure path emits an Error-level
// log, records the span error (with mercure.error.kind attribute), and
// returns 500 — not a silent 500.
//
// registerSubscriptionHandlers gates route registration on the
// TransportSubscribers interface, so the only ways to reach this branch
// are (1) direct handler invocation, which this test exercises, or
// (2) a regression that removes the gate at handler.go's
// registerSubscriptionHandlers.
//
// Table-driven over both SubscriptionsHandler (plural collection) and
// SubscriptionHandler (singular) — both call initSubscription, so the
// contract is identical, but covering both prevents a future refactor
// that diverges the two from silently losing coverage on one.
func TestSubscriptionHandlersTransportWithoutSubscribers(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		path     string
		jwtTopic string
		invoke   func(*Hub, http.ResponseWriter, *http.Request)
	}{
		{
			name:     "SubscriptionsHandler/plural",
			path:     defaultHubURL + subscriptionsPath,
			jwtTopic: "/.well-known/mercure/subscriptions",
			invoke:   func(h *Hub, w http.ResponseWriter, r *http.Request) { h.SubscriptionsHandler(w, r) },
		},
		{
			name:     "SubscriptionHandler/singular",
			path:     defaultHubURL + subscriptionsPath + "/topic/sub",
			jwtTopic: "/.well-known/mercure/subscriptions{/topic}{/subscriber}",
			invoke: func(h *Hub, w http.ResponseWriter, r *http.Request) {
				router := mux.NewRouter()
				router.HandleFunc(subscriptionURL, h.SubscriptionHandler)
				router.ServeHTTP(w, r)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var buf bytes.Buffer

			logger := slog.New(NewSlogHandler(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelError})))

			ctx, sr := spanRecorder(t)

			hub := createDummy(t, WithTransport(&nonSubscribersTransport{}), WithLogger(logger))

			req := httptest.NewRequest(http.MethodGet, tc.path, nil).WithContext(ctx)
			req.AddCookie(&http.Cookie{Name: "mercureAuthorization", Value: createDummyAuthorizedJWT(roleSubscriber, []string{tc.jwtTopic}), HttpOnly: true, Secure: true, SameSite: http.SameSiteStrictMode})

			w := httptest.NewRecorder()
			tc.invoke(hub, w, req)

			res := w.Result()

			t.Cleanup(func() { _ = res.Body.Close() })

			// Status half of the contract: 500 for both handlers.
			assert.Equal(t, http.StatusInternalServerError, res.StatusCode,
				"both handlers must return 500 on TransportSubscribers type-assertion failure")

			// Log half of the contract.
			assert.Contains(t, buf.String(), "transport does not implement TransportSubscribers")

			// Span half of the contract: a mercure.subscriptions span must
			// be ended with codes.Error status, a recorded exception, and
			// the queryable mercure.error.kind attribute. Otherwise a
			// regression that drops recordSpanError or the attribute would
			// pass on log-only coverage.
			var subSpan sdktrace.ReadOnlySpan

			for _, s := range sr.Ended() {
				if s.Name() == "mercure.subscriptions" {
					subSpan = s

					break
				}
			}

			require.NotNil(t, subSpan, "mercure.subscriptions span must be ended — without it the rest of the assertions are vacuous")
			assert.Equal(t, codes.Error, subSpan.Status().Code,
				"recordSpanError must set codes.Error on the type-assertion-failure branch — substring 'silent 500' regression")

			var errorType string

			for _, attr := range subSpan.Attributes() {
				if string(attr.Key) == "error.type" {
					errorType = attr.Value.AsString()
				}
			}

			assert.Equal(t, "transport_does_not_support_subscribers", errorType,
				"OTel-standard `error.type` attribute lets operators filter spans by sentinel without substring-matching log text")
		})
	}
}

// TestSubscriptionsHandlerCacheHitAttribute locks the 304-path span
// instrumentation: the If-None-Match short-circuit branch in
// initSubscription must attach mercure.subscriptions.cache_hit=true to
// the span so trace consumers can isolate ETag-served requests.
//
// Per OpenTelemetry HTTP semantic conventions, 1xx/2xx/3xx spans MUST
// keep the default Unset status — the attribute alone carries the
// distinguishing signal. This test pins both: cache_hit=true present
// AND status code remains Unset (not Ok, not Error).
func TestSubscriptionsHandlerCacheHitAttribute(t *testing.T) {
	t.Parallel()

	ctx, sr := spanRecorder(t)

	hub := createDummy(t, WithSubscriptions())

	lastEventID, _, _ := hub.transport.(TransportSubscribers).GetSubscribers(t.Context())

	req := httptest.NewRequest(http.MethodGet, subscriptionsURL, nil).WithContext(ctx)
	req.Header.Set("If-None-Match", lastEventID)
	req.AddCookie(&http.Cookie{Name: "mercureAuthorization", Value: createDummyAuthorizedJWT(roleSubscriber, []string{"/.well-known/mercure/subscriptions"}), HttpOnly: true, Secure: true, SameSite: http.SameSiteStrictMode})

	w := httptest.NewRecorder()
	hub.SubscriptionsHandler(w, req)

	res := w.Result()

	t.Cleanup(func() { _ = res.Body.Close() })

	require.Equal(t, http.StatusNotModified, res.StatusCode)

	var subSpan sdktrace.ReadOnlySpan

	for _, s := range sr.Ended() {
		if s.Name() == "mercure.subscriptions" {
			subSpan = s

			break
		}
	}

	require.NotNil(t, subSpan, "mercure.subscriptions span must be ended on the 304 path")
	assert.Equal(t, codes.Unset, subSpan.Status().Code,
		"304 is success — per OTel HTTP semconv 1xx/2xx/3xx spans MUST stay Unset; flipping to Ok or Error violates spec")

	var cacheHit bool

	for _, attr := range subSpan.Attributes() {
		if string(attr.Key) == "mercure.subscriptions.cache_hit" {
			cacheHit = attr.Value.AsBool()
		}
	}

	assert.True(t, cacheHit,
		"cache_hit=true is the only signal that distinguishes ETag-served spans from body-returning success spans; if missing, dashboard queries filtering on cache_hit silently return zero results")
}

// tooManySubscribersTransport's GetSubscribers returns a wrapped
// ErrTooManySubscribers, so the handler must map it to 503 (not the generic 500).
type tooManySubscribersTransport struct{ getSubscribersErrorTransport }

func (*tooManySubscribersTransport) GetSubscribers(_ context.Context) (string, []*Subscriber, error) {
	return "", nil, fmt.Errorf("transport: %w", ErrTooManySubscribers)
}

func TestSubscriptionsHandlerTooManySubscribersReturns503(t *testing.T) {
	t.Parallel()

	hub := createDummy(t, WithTransport(&tooManySubscribersTransport{}))

	req := httptest.NewRequest(http.MethodGet, defaultHubURL+subscriptionsPath, nil)
	req.AddCookie(&http.Cookie{Name: "mercureAuthorization", Value: createDummyAuthorizedJWT(roleSubscriber, []string{"/.well-known/mercure/subscriptions"}), HttpOnly: true, Secure: true, SameSite: http.SameSiteStrictMode})

	w := httptest.NewRecorder()
	hub.SubscriptionsHandler(w, req)
	res := w.Result()

	t.Cleanup(func() { _ = res.Body.Close() })

	assert.Equal(t, http.StatusServiceUnavailable, res.StatusCode,
		"wrapped ErrTooManySubscribers must map to 503, not the generic 500")
	assert.NotContains(t, w.Body.String(), `"@context"`)
}
