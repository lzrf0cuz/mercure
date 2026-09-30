package mercure

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type responseWriterMock struct{}

func (m *responseWriterMock) Header() http.Header {
	return http.Header{}
}

func (m *responseWriterMock) Write([]byte) (int, error) {
	return 0, nil
}

func (m *responseWriterMock) WriteHeader(_ int) {
}

type responseTester struct {
	header             http.Header
	body               string
	expectedStatusCode int
	expectedBody       string
	cancel             context.CancelFunc
	tb                 testing.TB
}

func (rt *responseTester) Header() http.Header {
	if rt.header == nil {
		return http.Header{}
	}

	return rt.header
}

func (rt *responseTester) Write(buf []byte) (int, error) {
	rt.body += string(buf)

	if rt.body == rt.expectedBody {
		rt.cancel()
	} else if !strings.HasPrefix(rt.expectedBody, rt.body) {
		defer rt.cancel()

		mess := fmt.Sprintf(`Received body "%s" doesn't match expected body "%s"`, rt.body, rt.expectedBody)
		if rt.tb == nil {
			panic(mess)
		}

		rt.tb.Error(mess)
	}

	return len(buf), nil
}

func (rt *responseTester) WriteHeader(statusCode int) {
	if rt.tb != nil {
		assert.Equal(rt.tb, rt.expectedStatusCode, statusCode)
	}
}

func (rt *responseTester) Flush() {
}

func (rt *responseTester) SetWriteDeadline(_ time.Time) error {
	return nil
}

type subscribeRecorder struct {
	*httptest.ResponseRecorder

	writeDeadline time.Time
}

func newSubscribeRecorder() *subscribeRecorder {
	return &subscribeRecorder{ResponseRecorder: httptest.NewRecorder()}
}

// sseSubscriptions decodes every subscription document carried by an SSE
// stream. Assertions then run against the document rather than against its
// serialised form, so they survive a change of JSON formatting.
func sseSubscriptions(tb testing.TB, stream string) []subscription {
	tb.Helper()

	var subs []subscription

	for frame := range strings.SplitSeq(stream, "\n\n") {
		var data []string

		for line := range strings.SplitSeq(frame, "\n") {
			if after, ok := strings.CutPrefix(line, "data:"); ok {
				data = append(data, strings.TrimPrefix(after, " "))
			}
		}

		if len(data) == 0 {
			continue
		}

		// Per the SSE grammar the data lines of a frame are joined with LF.
		var sub subscription
		require.NoError(tb, json.Unmarshal([]byte(strings.Join(data, "\n")), &sub))

		subs = append(subs, sub)
	}

	return subs
}

func (r *subscribeRecorder) SetWriteDeadline(deadline time.Time) error {
	if deadline.After(r.writeDeadline) {
		r.writeDeadline = deadline
	}

	return nil
}

func (r *subscribeRecorder) Write(buf []byte) (int, error) {
	if time.Now().After(r.writeDeadline) {
		return 0, os.ErrDeadlineExceeded
	}

	return r.ResponseRecorder.Write(buf)
}

func (r *subscribeRecorder) WriteString(str string) (int, error) {
	if time.Now().After(r.writeDeadline) {
		return 0, os.ErrDeadlineExceeded
	}

	return r.WriteString(str)
}

func (r *subscribeRecorder) FlushError() error {
	if time.Now().After(r.writeDeadline) {
		return os.ErrDeadlineExceeded
	}

	r.Flush()

	return nil
}

func TestSubscribeNotAFlusher(t *testing.T) {
	t.Parallel()

	hub := createAnonymousDummy(t)

	go func() {
		s := hub.transport.(*LocalTransport)

		var ready bool

		for !ready {
			s.RLock()
			ready = s.subscribers.Len() != 0
			s.RUnlock()
		}

		_ = hub.transport.Dispatch(t.Context(), &Update{
			Topics: []string{"https://example.com/foo"},
			Data:   "Hello World",
		})
	}()

	assert.Panics(t, func() {
		hub.SubscribeHandler(
			&responseWriterMock{},
			httptest.NewRequest(http.MethodGet, defaultHubURL+"?match=https://example.com/foo", nil),
		)
	})
}

func TestSubscribeNoCookie(t *testing.T) {
	t.Parallel()

	hub := createDummy(t)

	req := httptest.NewRequest(http.MethodGet, defaultHubURL, nil)
	w := httptest.NewRecorder()

	hub.SubscribeHandler(w, req)

	resp := w.Result()

	t.Cleanup(func() {
		require.NoError(t, resp.Body.Close())
	})

	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	assert.Equal(t, http.StatusText(http.StatusUnauthorized)+"\n", w.Body.String())
}

func TestSubscribeInvalidJWT(t *testing.T) {
	t.Parallel()

	hub := createDummy(t)

	req := httptest.NewRequest(http.MethodGet, defaultHubURL, nil)
	w := httptest.NewRecorder()

	req.AddCookie(&http.Cookie{Name: defaultCookieName, Value: "invalid"})

	hub.SubscribeHandler(w, req)

	resp := w.Result()

	t.Cleanup(func() {
		require.NoError(t, resp.Body.Close())
	})

	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	assert.Equal(t, http.StatusText(http.StatusUnauthorized)+"\n", w.Body.String())
}

func TestSubscribeUnauthorizedJWT(t *testing.T) {
	t.Parallel()

	hub := createDummy(t)

	req := httptest.NewRequest(http.MethodGet, defaultHubURL, nil)
	w := httptest.NewRecorder()

	req.AddCookie(&http.Cookie{Name: defaultCookieName, Value: createDummyUnauthorizedJWT()})
	req.Header = http.Header{"Cookie": []string{w.Header().Get("Set-Cookie")}}

	hub.SubscribeHandler(w, req)

	resp := w.Result()

	t.Cleanup(func() {
		require.NoError(t, resp.Body.Close())
	})

	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	assert.Equal(t, http.StatusText(http.StatusUnauthorized)+"\n", w.Body.String())
}

func TestSubscribeInvalidAlgJWT(t *testing.T) {
	t.Parallel()

	hub := createDummy(t)

	req := httptest.NewRequest(http.MethodGet, defaultHubURL, nil)
	w := httptest.NewRecorder()

	req.AddCookie(&http.Cookie{Name: defaultCookieName, Value: createDummyNoneSignedJWT()})

	hub.SubscribeHandler(w, req)

	resp := w.Result()

	t.Cleanup(func() {
		require.NoError(t, resp.Body.Close())
	})

	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	assert.Equal(t, http.StatusText(http.StatusUnauthorized)+"\n", w.Body.String())
}

// TestSubscribeJWTAlgorithmsPinned verifies the algorithm allowlist is enforced
// on the keyfunc (JWKS-style) path: a token whose alg is outside the allowlist
// is rejected at parse time, regardless of its signature.
func TestSubscribeJWTAlgorithmsPinned(t *testing.T) {
	t.Parallel()

	// A keyfunc returning the HMAC secret for any token, like a JWKS-backed
	// keyfunc that does not by itself pin the algorithm.
	kf := func(*jwt.Token) (any, error) { return []byte("subscriber"), nil }

	tms, err := NewTopicMatcherStore(0)
	require.NoError(t, err)

	hub, err := NewHub(t.Context(),
		WithAnonymous(),
		WithResourceIdentifier(testResourceIdentifier),
		WithIssuers([]Issuer{{
			Identifier: testIssuer,
			Subscriber: KeyFunc{Keyfunc: kf, Algorithms: []string{jwt.SigningMethodRS256.Name}},
		}}),
		WithTopicMatcherStore(tms),
	)
	require.NoError(t, err)

	// HS256 token: outside the RS256 allowlist.
	token := mintAccessToken([]byte("subscriber"), testResourceIdentifier, []authorizationDetail{{
		Type: authorizationDetailTypeMercure, Actions: []mercureAction{actionSubscribe},
		Topics: []detailTopic{{TopicMatcher{MatcherTypeExact, "https://example.com/foo"}}},
	}})

	req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?match=https://example.com/foo", nil)
	req.Header.Add("Authorization", bearerPrefix+token)

	w := httptest.NewRecorder()
	hub.SubscribeHandler(w, req)

	resp := w.Result()

	t.Cleanup(func() {
		require.NoError(t, resp.Body.Close())
	})

	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

func TestSubscribeNoTopic(t *testing.T) {
	t.Parallel()

	hub := createAnonymousDummy(t)

	req := httptest.NewRequest(http.MethodGet, defaultHubURL, nil)
	w := httptest.NewRecorder()
	hub.SubscribeHandler(w, req)

	resp := w.Result()

	t.Cleanup(func() {
		require.NoError(t, resp.Body.Close())
	})

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Equal(t, "missing \"match\" subscription parameter\n", w.Body.String())
}

func TestSubscribeTooManyTopics(t *testing.T) {
	t.Parallel()

	hub := createAnonymousDummy(t)

	q := url.Values{}
	for i := 0; i <= maxMatcherCount; i++ {
		q.Add("match", "https://example.com/foo")
	}

	req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?"+q.Encode(), nil)
	w := httptest.NewRecorder()
	hub.SubscribeHandler(w, req)

	resp := w.Result()

	t.Cleanup(func() {
		require.NoError(t, resp.Body.Close())
	})

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestSubscribeTooManyClaimMatchers(t *testing.T) {
	t.Parallel()

	hub := createDummy(t)

	scope := make([]string, maxClaimMatchers+1)
	for i := range scope {
		scope[i] = "https://example.com/foo"
	}

	req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?match=https://example.com/foo", nil)
	req.Header.Add("Authorization", bearerPrefix+createDummyAuthorizedJWT(roleSubscriber, scope))

	w := httptest.NewRecorder()
	hub.SubscribeHandler(w, req)

	resp := w.Result()

	t.Cleanup(func() {
		require.NoError(t, resp.Body.Close())
	})

	// Too many topics in a single authorization detail → invalid_token.
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

var errFailedToAddSubscriber = errors.New("failed to add a subscriber")

type addSubscriberErrorTransport struct{}

func (*addSubscriberErrorTransport) Dispatch(_ context.Context, _ *Update) error {
	return nil
}

func (*addSubscriberErrorTransport) AddSubscriber(_ context.Context, _ *LocalSubscriber) error {
	return errFailedToAddSubscriber
}

func (*addSubscriberErrorTransport) RemoveSubscriber(_ context.Context, _ *LocalSubscriber) error {
	return nil
}

func (*addSubscriberErrorTransport) GetSubscribers(_ context.Context) (string, []*LocalSubscriber, error) {
	return "", []*LocalSubscriber{}, nil
}

func (*addSubscriberErrorTransport) Close(_ context.Context) error {
	return nil
}

func TestSubscribeAddSubscriberError(t *testing.T) {
	t.Parallel()

	hub := createAnonymousDummy(t, WithTransport(&addSubscriberErrorTransport{}))

	req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?match=foo", nil)
	w := httptest.NewRecorder()

	hub.SubscribeHandler(w, req)

	resp := w.Result()

	t.Cleanup(func() {
		require.NoError(t, resp.Body.Close())
	})

	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	assert.Equal(t, http.StatusText(http.StatusServiceUnavailable)+"\n", w.Body.String())
}

func TestSubscribeQueryMethod(t *testing.T) {
	t.Parallel()

	hub := createAnonymousDummy(t)
	ctx := t.Context()

	go func() {
		s := hub.transport.(*LocalTransport)

		var ready bool

		for !ready {
			s.RLock()
			ready = s.subscribers.Len() == 1
			s.RUnlock()
		}

		_ = hub.transport.Dispatch(ctx, &Update{
			Topics: []string{"https://example.com/books/1"},
			Data:   "Hello World", ID: "b",
		})
	}()

	reqCtx, cancel := context.WithCancel(t.Context())
	// Topics travel in the QUERY request body instead of the URL.
	body := url.Values{"match": {"https://example.com/books/1"}}.Encode()
	req := httptest.NewRequest(methodQuery, defaultHubURL, strings.NewReader(body)).WithContext(reqCtx)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	w := &responseTester{
		expectedStatusCode: http.StatusOK,
		expectedBody:       ":\nid: b\ndata: Hello World\n\n",
		tb:                 t,
		cancel:             cancel,
	}
	hub.SubscribeHandler(w, req)
}

func subscribe(tb testing.TB, numberOfSubscribers int) {
	tb.Helper()

	hub := createAnonymousDummy(tb)
	ctx := tb.Context()

	go func() {
		s := hub.transport.(*LocalTransport)

		var ready bool

		for !ready {
			s.RLock()
			ready = s.subscribers.Len() == numberOfSubscribers
			s.RUnlock()
		}

		_ = hub.transport.Dispatch(ctx, &Update{
			Topics: []string{"https://example.com/not-subscribed"},
			Data:   "Hello World", ID: "a",
		})
		_ = hub.transport.Dispatch(ctx, &Update{
			Topics: []string{"https://example.com/books/1"},
			Data:   "Hello World", ID: "b",
		})
		_ = hub.transport.Dispatch(ctx, &Update{
			Topics: []string{"https://example.com/reviews/22"},
			Data:   "Great", ID: "c",
		})
		_ = hub.transport.Dispatch(ctx, &Update{
			Topics: []string{"https://example.com/hub?topic=faulty{iri"},
			Data:   "Faulty IRI", ID: "d",
		})
		_ = hub.transport.Dispatch(ctx, &Update{
			Topics: []string{"string"},
			Data:   "string", ID: "e",
		})
	}()

	var wg sync.WaitGroup

	for range numberOfSubscribers {
		wg.Go(func() {
			ctx, cancel := context.WithCancel(tb.Context())
			req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?match=https://example.com/books/1&match=string&match_urlpattern=https://example.com/reviews/:id&match=https://example.com/hub?topic=faulty{iri", nil).WithContext(ctx)

			w := &responseTester{
				expectedStatusCode: http.StatusOK,
				expectedBody:       ":\nid: b\ndata: Hello World\n\nid: c\ndata: Great\n\nid: d\ndata: Faulty IRI\n\nid: e\ndata: string\n\n",
				tb:                 tb,
				cancel:             cancel,
			}
			hub.SubscribeHandler(w, req)
		})
	}

	wg.Wait()
}

func TestSubscribe(t *testing.T) {
	t.Parallel()

	subscribe(t, 3)
}

func testSubscribeLogs(t *testing.T, hub *Hub, payload any) {
	t.Helper()

	ctx, cancel := context.WithCancel(t.Context())
	req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?match_urlpattern=https://example.com/reviews/:id", nil).WithContext(ctx)
	req.AddCookie(&http.Cookie{Name: defaultCookieName, Value: createDummySubscriberJWTWithDetails(t, payload, TopicMatcher{Type: MatcherTypeURLPattern, Pattern: "https://example.com/reviews/:id"})})

	w := &responseTester{
		expectedStatusCode: http.StatusOK,
		expectedBody:       ":\n",
		tb:                 t,
		cancel:             cancel,
	}

	hub.SubscribeHandler(w, req)
}

func TestSubscribeWithLogLevelDebug(t *testing.T) {
	t.Parallel()

	payload := map[string]any{
		"bar": "baz",
		"foo": "bar",
	}

	var buf bytes.Buffer

	opts := slog.HandlerOptions{Level: slog.LevelDebug}
	logger := slog.New(slog.NewTextHandler(&buf, &opts))

	testSubscribeLogs(t, createDummy(
		t,
		WithLogger(logger),
	), payload)

	assert.Contains(t, buf.String(), "baz")
}

func TestSubscribeLogLevelInfo(t *testing.T) {
	t.Parallel()

	payload := map[string]any{
		"bar": "baz",
		"foo": "bar",
	}

	var buf bytes.Buffer

	opts := slog.HandlerOptions{Level: slog.LevelInfo}
	logger := slog.New(slog.NewTextHandler(&buf, &opts))

	testSubscribeLogs(t, createDummy(
		t,
		WithLogger(logger),
	), payload)

	assert.NotContains(t, buf.String(), "baz")
}

func TestSubscribeLogAnonymousSubscriber(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	h := createAnonymousDummy(t, WithLogger(logger))

	ctx, cancel := context.WithCancel(t.Context())
	req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?match=https://example.com/", nil).WithContext(ctx)

	w := &responseTester{
		expectedStatusCode: http.StatusOK,
		expectedBody:       ":\n",
		tb:                 t,
		cancel:             cancel,
	}

	h.SubscribeHandler(w, req)

	assert.NotContains(t, buf.String(), "payload")
}

func TestUnsubscribe(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		hub := createAnonymousDummy(t)

		s, _ := hub.transport.(*LocalTransport)
		assert.Equal(t, 0, s.subscribers.Len())
		ctx, cancel := context.WithCancel(t.Context())

		go func() {
			req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?match=https://example.com/books/1", nil).WithContext(ctx)
			hub.SubscribeHandler(newSubscribeRecorder(), req)
			assert.Equal(t, 0, s.subscribers.Len())
			s.subscribers.Walk(0, func(s *LocalSubscriber) bool {
				_, ok := <-s.out
				assert.False(t, ok)

				return true
			})
		}()

		for {
			s.RLock()
			notEmpty := s.subscribers.Len() != 0
			s.RUnlock()

			if notEmpty {
				break
			}
		}

		cancel()
		synctest.Wait()
	})
}

func TestSubscribePrivate(t *testing.T) {
	t.Parallel()

	hub := createDummy(t)
	s, _ := hub.transport.(*LocalTransport)
	ctx := t.Context()

	go func() {
		for {
			s.RLock()
			empty := s.subscribers.Len() == 0
			s.RUnlock()

			if empty {
				continue
			}

			_ = hub.transport.Dispatch(ctx, &Update{
				Topics: []string{"https://example.com/reviews/21"},
				Data:   "Foo", ID: "a",
				Private: true,
			})
			_ = hub.transport.Dispatch(ctx, &Update{
				Topics: []string{"https://example.com/reviews/22"},
				Data:   "Hello World", ID: "b", Type: "test",
				Private: true,
			})
			_ = hub.transport.Dispatch(ctx, &Update{
				Topics: []string{"https://example.com/reviews/23"},
				Data:   "Great", ID: "c", Retry: 1,
				Private: true,
			})

			return
		}
	}()

	ctx, cancel := context.WithCancel(t.Context())
	req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?match_urlpattern=https://example.com/reviews/:id", nil).WithContext(ctx)
	req.AddCookie(&http.Cookie{Name: defaultCookieName, Value: createDummyAuthorizedJWT(roleSubscriber, []string{"https://example.com/reviews/22", "https://example.com/reviews/23"})})

	w := &responseTester{
		expectedStatusCode: http.StatusOK,
		expectedBody:       ":\nevent: test\nid: b\ndata: Hello World\n\nretry: 1\nid: c\ndata: Great\n\n",
		tb:                 t,
		cancel:             cancel,
	}

	hub.SubscribeHandler(w, req)
}

func TestSubscriptionEvents(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		hub := createDummy(t, WithSubscriptions())

		ctx1, cancel1 := context.WithCancel(t.Context())
		t.Cleanup(cancel1)

		ctx2, cancel2 := context.WithCancel(t.Context())
		t.Cleanup(cancel2)

		var wg sync.WaitGroup

		wg.Go(func() {
			// Authorized to receive connection events
			req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?match_urlpattern=/.well-known/mercure/subscriptions/*", nil).WithContext(ctx1)
			req.AddCookie(&http.Cookie{Name: defaultCookieName, Value: createDummySubscriberJWTWithDetails(t, struct {
				Foo string `json:"foo"`
			}{Foo: "bar"}, TopicMatcher{Type: MatcherTypeURLPattern, Pattern: "/.well-known/mercure/subscriptions/*"})})

			w := newSubscribeRecorder()
			hub.SubscribeHandler(w, req)

			resp := w.Result()

			t.Cleanup(func() {
				_ = resp.Body.Close()
			})

			body, _ := io.ReadAll(resp.Body)

			assert.Equal(t, http.StatusOK, resp.StatusCode)

			bodyContent := string(body)
			assert.Contains(t, bodyContent, "event: mercure\n")

			subs := sseSubscriptions(t, bodyContent)
			require.NotEmpty(t, subs)

			var announced, withdrawn []subscription

			for _, sub := range subs {
				if sub.Active {
					announced = append(announced, sub)
				} else {
					withdrawn = append(withdrawn, sub)
				}
			}

			assert.NotEmpty(t, announced, "no subscription was announced")
			assert.NotEmpty(t, withdrawn, "the disconnection was never announced")

			for _, sub := range subs {
				assert.Equal(t, "subscription", sub.Type)
				assert.Regexp(t, `^urn:uuid:`, sub.Subscriber)
			}

			i := slices.IndexFunc(subs, func(sub subscription) bool { return sub.Match == "https://example.com" })
			require.GreaterOrEqual(t, i, 0, "no event described the example.com subscription")

			assert.Equal(t, string(MatcherTypeExact), subs[i].MatchType)
			assert.Regexp(t, `^/\.well-known/mercure/subscriptions/exact/https%3A%2F%2Fexample\.com/`, subs[i].ID)
			assert.Equal(t, map[string]any{"foo": "bar"}, subs[i].Payload)
		})

		wg.Go(func() {
			// Not authorized to receive connection events
			req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?match_urlpattern=/.well-known/mercure/subscriptions/:match_type/:match/:subscriber", nil).WithContext(ctx2)
			req.AddCookie(&http.Cookie{Name: defaultCookieName, Value: createDummyAuthorizedJWT(roleSubscriber, []string{})})

			w := newSubscribeRecorder()
			hub.SubscribeHandler(w, req)

			resp := w.Result()

			t.Cleanup(func() {
				_ = resp.Body.Close()
			})

			body, _ := io.ReadAll(resp.Body)

			assert.Equal(t, http.StatusOK, resp.StatusCode)
			assert.Empty(t, string(body))
		})

		wg.Go(func() {
			// Both subscribers above are registered once they are durably
			// blocked waiting for updates.
			synctest.Wait()

			ctx, cancelRequest2 := context.WithCancel(t.Context())
			req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?match=https://example.com", nil).WithContext(ctx)
			req.AddCookie(&http.Cookie{Name: defaultCookieName, Value: createDummyAuthorizedJWT(roleSubscriber, []string{"https://example.com"})})

			w := &responseTester{
				expectedStatusCode: http.StatusOK,
				expectedBody:       ":\n",
				tb:                 t,
				cancel:             cancelRequest2,
			}
			hub.SubscribeHandler(w, req)

			// This subscriber is gone; wait for the resulting "active": false
			// update to reach the subscriber above before tearing it down.
			synctest.Wait()

			cancel2()
			cancel1()
		})

		wg.Wait()
	})
}

func TestSubscribeAll(t *testing.T) {
	t.Parallel()

	hub := createDummy(t)
	s, _ := hub.transport.(*LocalTransport)
	ctx := t.Context()

	go func() {
		for {
			s.RLock()
			empty := s.subscribers.Len() == 0
			s.RUnlock()

			if empty {
				continue
			}

			_ = hub.transport.Dispatch(ctx, &Update{
				Topics: []string{"https://example.com/reviews/21"},
				Data:   "Foo", ID: "a",
				Private: true,
			})
			_ = hub.transport.Dispatch(ctx, &Update{
				Topics: []string{"https://example.com/reviews/22"},
				Data:   "Hello World", ID: "b", Type: "test",
				Private: true,
			})

			return
		}
	}()

	ctx, cancel := context.WithCancel(t.Context())
	req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?match_urlpattern=https://example.com/reviews/:id", nil).WithContext(ctx)
	req.Header.Add("Authorization", bearerPrefix+createDummyAuthorizedJWT(roleSubscriber, []string{"random", "*"}))

	w := &responseTester{
		expectedStatusCode: http.StatusOK,
		expectedBody:       ":\nid: a\ndata: Foo\n\nevent: test\nid: b\ndata: Hello World\n\n",
		tb:                 t,
		cancel:             cancel,
	}

	hub.SubscribeHandler(w, req)
}

func TestSendMissedEvents(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		transport := createBoltTransport(t, 0, 0)
		ctx := t.Context()

		hub := createAnonymousDummy(t, WithLogger(transport.logger), WithTransport(transport), WithProtocolVersionCompatibility(7))

		require.NoError(t, transport.Dispatch(ctx, &Update{
			Topics: []string{"https://example.com/foos/a"},
			ID:     "a",
			Data:   "d1",
		}))
		require.NoError(t, transport.Dispatch(ctx, &Update{
			Topics: []string{"https://example.com/foos/b"},
			ID:     "b",
			Data:   "d2",
		}))

		// Using deprecated 'Last-Event-ID' query parameter
		go func() {
			ctx, cancel := context.WithCancel(t.Context())
			req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?match_urlpattern=https://example.com/foos/:id&Last-Event-ID=a", nil).WithContext(ctx)

			w := &responseTester{
				expectedStatusCode: http.StatusOK,
				expectedBody:       ":\nid: b\ndata: d2\n\n",
				tb:                 t,
				cancel:             cancel,
			}

			hub.SubscribeHandler(w, req)
		}()

		go func() {
			ctx, cancel := context.WithCancel(t.Context())
			req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?match_urlpattern=https://example.com/foos/:id&last_event_id=a", nil).WithContext(ctx)

			w := &responseTester{
				expectedStatusCode: http.StatusOK,
				expectedBody:       ":\nid: b\ndata: d2\n\n",
				tb:                 t,
				cancel:             cancel,
			}

			hub.SubscribeHandler(w, req)
		}()

		go func() {
			ctx, cancel := context.WithCancel(t.Context())
			req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?match_urlpattern=https://example.com/foos/:id", nil).WithContext(ctx)
			req.Header.Add("Last-Event-ID", "a")

			w := &responseTester{
				expectedStatusCode: http.StatusOK,
				expectedBody:       ":\nid: b\ndata: d2\n\n",
				tb:                 t,
				cancel:             cancel,
			}

			hub.SubscribeHandler(w, req)
		}()

		synctest.Wait()
	})
}

func TestSendAllEvents(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		transport := createBoltTransport(t, 0, 0)
		hub := createAnonymousDummy(t, WithTransport(transport))
		ctx := t.Context()

		require.NoError(t, transport.Dispatch(ctx, &Update{
			Topics: []string{"https://example.com/foos/a"},
			ID:     "a",
			Data:   "d1",
		}))
		require.NoError(t, transport.Dispatch(ctx, &Update{
			Topics: []string{"https://example.com/foos/b"},
			ID:     "b",
			Data:   "d2",
		}))

		go func() {
			ctx, cancel := context.WithCancel(t.Context())
			req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?match_urlpattern=https://example.com/foos/:id&last_event_id="+EarliestLastEventID, nil).WithContext(ctx)

			w := &responseTester{
				header:             http.Header{},
				expectedStatusCode: http.StatusOK,
				expectedBody:       ":\nid: a\ndata: d1\n\nid: b\ndata: d2\n\n",
				tb:                 t,
				cancel:             cancel,
			}

			hub.SubscribeHandler(w, req)
		}()

		go func() {
			ctx, cancel := context.WithCancel(t.Context())
			req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?match_urlpattern=https://example.com/foos/:id", nil).WithContext(ctx)
			req.Header.Add("Last-Event-ID", EarliestLastEventID)

			w := &responseTester{
				header:             http.Header{},
				expectedStatusCode: http.StatusOK,
				expectedBody:       ":\nid: a\ndata: d1\n\nid: b\ndata: d2\n\n",
				tb:                 t,
				cancel:             cancel,
			}

			hub.SubscribeHandler(w, req)
		}()

		synctest.Wait()
	})
}

func TestUnknownLastEventID(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		transport := createBoltTransport(t, 0, 0)
		hub := createAnonymousDummy(t, WithLogger(transport.logger), WithTransport(transport))

		require.NoError(t, transport.Dispatch(t.Context(), &Update{
			Topics: []string{"https://example.com/foos/a"},
			ID:     "a",
			Data:   "d1",
		}))

		ctx := t.Context()

		go func(ctx context.Context) {
			c, cancel := context.WithCancel(ctx)
			req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?match_urlpattern=https://example.com/foos/:id&last_event_id=unknown", nil).WithContext(c)

			w := &responseTester{
				header:             http.Header{},
				expectedStatusCode: http.StatusOK,
				expectedBody:       ":\nid: b\ndata: d2\n\n",
				tb:                 t,
				cancel:             cancel,
			}

			hub.SubscribeHandler(w, req)
			// "unknown" is not in the history, so nothing was replayed and the
			// cursor is the reserved "earliest", not the newest id in history.
			assert.Equal(t, EarliestLastEventID, w.Header().Get("Mercure-Last-Event-ID"))
		}(ctx)

		go func(ctx context.Context) {
			c, cancel := context.WithCancel(ctx)
			req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?match_urlpattern=https://example.com/foos/:id", nil).WithContext(c)
			req.Header.Add("Last-Event-ID", "unknown")

			w := &responseTester{
				header:             http.Header{},
				expectedStatusCode: http.StatusOK,
				expectedBody:       ":\nid: b\ndata: d2\n\n",
				tb:                 t,
				cancel:             cancel,
			}

			hub.SubscribeHandler(w, req)
			// "unknown" is not in the history, so nothing was replayed and the
			// cursor is the reserved "earliest", not the newest id in history.
			assert.Equal(t, EarliestLastEventID, w.Header().Get("Mercure-Last-Event-ID"))
		}(ctx)

		for {
			transport.RLock()
			done := transport.subscribers.Len() == 2
			transport.RUnlock()

			if done {
				break
			}
		}

		require.NoError(t, transport.Dispatch(ctx, &Update{
			Topics: []string{"https://example.com/foos/b"},
			ID:     "b",
			Data:   "d2",
		}))

		synctest.Wait()
	})
}

func TestUnknownLastEventIDDoesNotLeakPrivateEventID(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		transport := createBoltTransport(t, 0, 0)
		hub := createAnonymousDummy(t, WithLogger(transport.logger), WithTransport(transport))

		// Public event the anonymous subscriber is authorized to read.
		require.NoError(t, transport.Dispatch(t.Context(), &Update{
			Topics: []string{"https://example.com/foos/a"},
			ID:     "a", Data: "d1",
		}))
		// Private event the anonymous subscriber is NOT authorized to
		// read. Its id must not appear in the Last-Event-ID response.
		require.NoError(t, transport.Dispatch(t.Context(), &Update{
			Topics:  []string{"https://example.com/foos/b"},
			Private: true,
			ID:      "b", Data: "secret",
		}))

		ctx := t.Context()

		go func(ctx context.Context) {
			c, cancel := context.WithCancel(ctx)
			req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?match_urlpattern=https://example.com/foos/:id&last_event_id=unknown", nil).WithContext(c)

			w := &responseTester{
				header:             http.Header{},
				expectedStatusCode: http.StatusOK,
				expectedBody:       ":\nid: c\ndata: d3\n\n",
				tb:                 t,
				cancel:             cancel,
			}

			hub.SubscribeHandler(w, req)

			cursor := w.Header().Get("Mercure-Last-Event-ID")
			// The private "b" must not leak, even though it is the most
			// recent in-history event. Nothing was replayed either, since
			// "unknown" is not in the history, so the cursor is the reserved
			// "earliest" rather than the authorized "a" — which the
			// subscriber never received.
			assert.NotEqual(t, "b", cursor)
			assert.Equal(t, EarliestLastEventID, cursor)
		}(ctx)

		for {
			transport.RLock()
			done := transport.subscribers.Len() == 1
			transport.RUnlock()

			if done {
				break
			}
		}

		require.NoError(t, transport.Dispatch(ctx, &Update{
			Topics: []string{"https://example.com/foos/c"},
			ID:     "c", Data: "d3",
		}))

		synctest.Wait()
	})
}

func TestUnknownLastEventIDEmptyHistory(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		transport := createBoltTransport(t, 0, 0)
		hub := createAnonymousDummy(t, WithTransport(transport))

		ctx := t.Context()

		go func() {
			ctx, cancel := context.WithCancel(ctx)
			req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?match_urlpattern=https://example.com/foos/:id&last_event_id=unknown", nil).WithContext(ctx)

			w := &responseTester{
				header:             http.Header{},
				expectedStatusCode: http.StatusOK,
				expectedBody:       ":\nid: b\ndata: d2\n\n",
				tb:                 t,
				cancel:             cancel,
			}

			hub.SubscribeHandler(w, req)
			assert.Equal(t, EarliestLastEventID, w.Header().Get("Mercure-Last-Event-ID"))
		}()

		go func() {
			ctx, cancel := context.WithCancel(ctx)
			req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?match_urlpattern=https://example.com/foos/:id", nil).WithContext(ctx)
			req.Header.Add("Last-Event-ID", "unknown")

			w := &responseTester{
				header:             http.Header{},
				expectedStatusCode: http.StatusOK,
				expectedBody:       ":\nid: b\ndata: d2\n\n",
				tb:                 t,
				cancel:             cancel,
			}

			hub.SubscribeHandler(w, req)
			assert.Equal(t, EarliestLastEventID, w.Header().Get("Mercure-Last-Event-ID"))
		}()

		for {
			transport.RLock()
			done := transport.subscribers.Len() == 2
			transport.RUnlock()

			if done {
				break
			}
		}

		require.NoError(t, transport.Dispatch(ctx, &Update{
			Topics: []string{"https://example.com/foos/b"},
			ID:     "b",
			Data:   "d2",
		}))

		synctest.Wait()
	})
}

// A present-but-empty last_event_id still gets a Mercure-Last-Event-ID
// response field, as the protocol requires whenever the parameter is present.
func TestEmptyLastEventIDGetsResponseHeader(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		hub := createAnonymousDummy(t)
		transport, _ := hub.transport.(*LocalTransport)

		ctx := t.Context()

		go func() {
			ctx, cancel := context.WithCancel(ctx)
			req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?match=https://example.com/foo&last_event_id=", nil).WithContext(ctx)

			w := &responseTester{
				header:             http.Header{},
				expectedStatusCode: http.StatusOK,
				expectedBody:       ":\nid: e1\ndata: d\n\n",
				tb:                 t,
				cancel:             cancel,
			}

			hub.SubscribeHandler(w, req)
			assert.Equal(t, EarliestLastEventID, w.Header().Get("Mercure-Last-Event-ID"))
		}()

		for {
			transport.RLock()
			done := transport.subscribers.Len() == 1
			transport.RUnlock()

			if done {
				break
			}
		}

		require.NoError(t, transport.Dispatch(ctx, &Update{
			Topics: []string{"https://example.com/foo"},
			ID:     "e1", Data: "d",
		}))

		synctest.Wait()
	})
}

func TestSubscribeHeartbeat(t *testing.T) {
	hub := createAnonymousDummy(t, WithHeartbeat(5*time.Millisecond))
	s, _ := hub.transport.(*LocalTransport)
	ctx := t.Context()

	go func() {
		for {
			s.RLock()
			empty := s.subscribers.Len() == 0
			s.RUnlock()

			if empty {
				continue
			}

			_ = hub.transport.Dispatch(ctx, &Update{
				Topics: []string{"https://example.com/books/1"},
				Data:   "Hello World", ID: "b",
			})

			return
		}
	}()

	ctx, cancel := context.WithCancel(ctx)
	req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?match=https://example.com/books/1&match_urlpattern=https://example.com/reviews/:id", nil).WithContext(ctx)

	w := &responseTester{
		expectedStatusCode: http.StatusOK,
		expectedBody:       ":\nid: b\ndata: Hello World\n\n:\n",
		tb:                 t,
		cancel:             cancel,
	}

	hub.SubscribeHandler(w, req)
}

func TestSubscribeExpires(t *testing.T) {
	t.Parallel()

	hub := createAnonymousDummy(t, WithWriteTimeout(0), WithDispatchTimeout(0), WithHeartbeat(500*time.Millisecond))
	token := jwt.New(jwt.SigningMethodHS256)
	token.Header["typ"] = atJWTType
	token.Claims = &claims{
		Issuer:               testIssuer,
		Audience:             jwt.ClaimStrings{testResourceIdentifier},
		ExpiresAt:            jwt.NewNumericDate(time.Now().Add(time.Second)),
		AuthorizationDetails: subscribeDetailsFromMatchers(nil, TopicMatcher{Type: MatcherTypeExact, Pattern: "*"}),
	}

	signedString, err := token.SignedString([]byte("subscriber"))
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?match=foo", nil)
	req.Header.Add("Authorization", bearerPrefix+signedString)

	w := newSubscribeRecorder()
	hub.SubscribeHandler(w, req)

	resp := w.Result()

	t.Cleanup(func() {
		require.NoError(t, resp.Body.Close())
	})

	assert.Equal(t, 200, resp.StatusCode)
}

func BenchmarkSubscribe(b *testing.B) {
	for b.Loop() {
		subscribe(b, 1000)
	}
}

// hubShutdownTestHub builds a hub with a caller-controlled context so tests
// can cancel the hub independently of the subscriber's request context.
func hubShutdownTestHub(ctx context.Context, tb testing.TB, writeTimeout time.Duration) *Hub {
	tb.Helper()

	return hubShutdownTestHubWithOptions(ctx, tb, writeTimeout)
}

// hubShutdownTestHubWithOptions builds a hub like hubShutdownTestHub, with
// extra options appended (e.g. WithMetrics for the disconnect-reason tests).
func hubShutdownTestHubWithOptions(ctx context.Context, tb testing.TB, writeTimeout time.Duration, extra ...Option) *Hub {
	tb.Helper()

	tms, err := NewTopicMatcherStore(0)
	require.NoError(tb, err)

	opts := append([]Option{
		WithAnonymous(),
		WithIssuers([]Issuer{{
			Identifier: testIssuer,
			Publisher:  Static{Key: []byte("publisher"), Algorithm: jwt.SigningMethodHS256.Name},
			Subscriber: Static{Key: []byte("subscriber"), Algorithm: jwt.SigningMethodHS256.Name},
		}}),
		WithResourceIdentifier(testResourceIdentifier),
		WithTopicMatcherStore(tms),
		WithWriteTimeout(writeTimeout),
	}, extra...)

	h, err := NewHub(ctx, opts...)
	require.NoError(tb, err)

	return h
}

func hubDrainTestHub(ctx context.Context, tb testing.TB, writeTimeout, drainTimeout time.Duration, options ...Option) *Hub {
	tb.Helper()

	tms, err := NewTopicMatcherStore(0)
	require.NoError(tb, err)

	h, err := NewHub(ctx, append([]Option{
		WithAnonymous(),
		WithIssuers([]Issuer{{
			Identifier: testIssuer,
			Publisher:  Static{Key: []byte("publisher"), Algorithm: jwt.SigningMethodHS256.Name},
			Subscriber: Static{Key: []byte("subscriber"), Algorithm: jwt.SigningMethodHS256.Name},
		}}),
		WithResourceIdentifier(testResourceIdentifier),
		WithTopicMatcherStore(tms),
		WithWriteTimeout(writeTimeout),
		WithDrainTimeout(drainTimeout),
	}, options...)...)
	require.NoError(tb, err)

	return h
}

// TestShutdownKeepsSubscribersWhenWriteTimeoutEnabled verifies the graceful
// drain contract: when the hub context is cancelled (Caddy stopping, pod
// SIGTERM, ...) and writeTimeout is set, subscribers stay connected until
// their per-connection disconnection timer fires or the client disconnects.
func TestShutdownKeepsSubscribersWhenWriteTimeoutEnabled(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		hubCtx, cancelHub := context.WithCancel(t.Context())
		hub := hubShutdownTestHub(hubCtx, t, 5*time.Minute)
		transport, _ := hub.transport.(*LocalTransport)

		go func() {
			req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?match=https://example.com/books/1", nil).WithContext(t.Context())
			hub.SubscribeHandler(newSubscribeRecorder(), req)
		}()

		waitSubscribers(t, transport, 1)

		// Simulate hub shutdown.
		cancelHub()
		synctest.Wait()

		transport.RLock()
		n := transport.subscribers.Len()
		transport.RUnlock()
		assert.Equal(t, 1, n, "subscriber must stay connected when writeTimeout is set; disconnect timer is the drain mechanism")
	})
}

// TestShutdownClosesSubscribersWhenWriteTimeoutDisabled covers the escape
// hatch: with writeTimeout == 0 there is no per-connection disconnect timer,
// so the hub context cancel must still terminate subscribers — otherwise
// http.Server.Shutdown would hang forever on active handlers.
func TestShutdownClosesSubscribersWhenWriteTimeoutDisabled(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		hubCtx, cancelHub := context.WithCancel(t.Context())
		hub := hubShutdownTestHub(hubCtx, t, 0)
		transport, _ := hub.transport.(*LocalTransport)

		go func() {
			req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?match=https://example.com/books/1", nil).WithContext(t.Context())
			hub.SubscribeHandler(newSubscribeRecorder(), req)
		}()

		waitSubscribers(t, transport, 1)

		cancelHub()
		synctest.Wait()

		transport.RLock()
		n := transport.subscribers.Len()
		transport.RUnlock()
		assert.Equal(t, 0, n, "subscriber must exit on hub shutdown when writeTimeout is 0")
	})
}

func TestDrainDeadline(t *testing.T) {
	t.Parallel()

	for _, window := range []time.Duration{
		time.Nanosecond, time.Millisecond, time.Second, 5 * time.Minute, 20 * time.Minute,
	} {
		for range 1000 {
			d := drainDeadline(window)
			assert.Positive(t, d, "drain deadline must be strictly positive")
			assert.LessOrEqual(t, d, window, "drain deadline must not exceed the window")
		}
	}
}

func TestDrainDisconnectionTime(t *testing.T) {
	t.Parallel()

	now := time.Now()

	for _, tc := range []struct {
		name           string
		existing       time.Time
		offset         time.Duration
		wantReschedule bool
		want           time.Time
	}{
		{name: "no existing deadline reschedules", existing: time.Time{}, offset: 5 * time.Minute, wantReschedule: true, want: now.Add(5 * time.Minute)},
		{name: "existing later than drain reschedules sooner", existing: now.Add(20 * time.Minute), offset: 5 * time.Minute, wantReschedule: true, want: now.Add(5 * time.Minute)},
		{name: "existing sooner than drain is kept", existing: now.Add(time.Minute), offset: 5 * time.Minute, wantReschedule: false, want: now.Add(time.Minute)},
		{name: "existing equal to drain is kept", existing: now.Add(5 * time.Minute), offset: 5 * time.Minute, wantReschedule: false, want: now.Add(5 * time.Minute)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, reschedule := drainDisconnectionTime(now, tc.existing, tc.offset)
			assert.Equal(t, tc.wantReschedule, reschedule)
			assert.Equal(t, tc.want, got)
		})
	}
}

// A drain must finish within drainTimeout, not the longer writeTimeout.
func TestDrainReschedulesWithinDrainWindow(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		const (
			writeTimeout = 20 * time.Minute
			drainTimeout = 5 * time.Minute
		)

		hub := hubDrainTestHub(t.Context(), t, writeTimeout, drainTimeout)
		transport, _ := hub.transport.(*LocalTransport)

		for range 2 {
			go func() {
				req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?match=https://example.com/books/1", nil).WithContext(t.Context())
				hub.SubscribeHandler(newSubscribeRecorder(), req)
			}()
		}

		waitSubscribers(t, transport, 2)

		hub.Drain()

		time.Sleep(drainTimeout + time.Second)
		synctest.Wait()

		transport.RLock()
		n := transport.subscribers.Len()
		transport.RUnlock()
		assert.Equal(t, 0, n, "subscribers must drain within drainTimeout, not writeTimeout")
	})
}

// A reload cancels the hub context without draining, so it must stay reconnect-free.
func TestDrainDoesNotDisconnectOnReload(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		hubCtx, cancelHub := context.WithCancel(t.Context())
		hub := hubDrainTestHub(hubCtx, t, 20*time.Minute, 5*time.Minute)
		transport, _ := hub.transport.(*LocalTransport)

		go func() {
			req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?match=https://example.com/books/1", nil).WithContext(t.Context())
			hub.SubscribeHandler(newSubscribeRecorder(), req)
		}()

		waitSubscribers(t, transport, 1)

		// Reload: the hub context is cancelled, but Drain is not called.
		cancelHub()
		synctest.Wait()

		transport.RLock()
		n := transport.subscribers.Len()
		transport.RUnlock()
		assert.Equal(t, 1, n, "context cancel without Drain must not drain: drain is stop-only")
	})
}

// A connection without a deadline still drains, through a timer armed by the drain.
func TestDrainWithoutWriteTimeout(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		const drainTimeout = 3 * time.Minute

		hub := hubDrainTestHub(t.Context(), t, 0, drainTimeout)
		transport, _ := hub.transport.(*LocalTransport)

		go func() {
			req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?match=https://example.com/books/1", nil).WithContext(t.Context())
			hub.SubscribeHandler(newSubscribeRecorder(), req)
		}()

		waitSubscribers(t, transport, 1)

		hub.Drain()
		time.Sleep(drainTimeout + time.Second)
		synctest.Wait()

		transport.RLock()
		n := transport.subscribers.Len()
		transport.RUnlock()
		assert.Equal(t, 0, n, "drain must arm a disconnection timer even when writeTimeout is 0")
	})
}

// Connections ended by a drain are reported as write_timeout, never hub_shutdown:
// the hub context is cancelled only after the drain (see docs/production/metrics.md).
func TestDrainReportsWriteTimeoutDisconnectReason(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name         string
		writeTimeout time.Duration
	}{
		{name: "write timeout later than the drain", writeTimeout: 20 * time.Minute},
		{name: "no write timeout", writeTimeout: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				const drainTimeout = 5 * time.Minute

				metrics := &recordingMetrics{}
				hub := hubDrainTestHub(t.Context(), t, tc.writeTimeout, drainTimeout, WithMetrics(metrics))

				runSubscribeUntilIdle(t.Context(), t, hub, newSubscribeRecorder(), func(*LocalTransport) {
					hub.Drain()
					time.Sleep(drainTimeout + time.Second)
				})

				assert.Equal(t, []DisconnectReason{DisconnectReasonWriteTimeout}, metrics.reasons())
			})
		})
	}
}

// Without a deadline, a reload must still close the connection, or Shutdown would hang.
func TestDrainReloadWithoutWriteTimeoutStillExits(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		hubCtx, cancelHub := context.WithCancel(t.Context())
		hub := hubDrainTestHub(hubCtx, t, 0, 5*time.Minute)
		transport, _ := hub.transport.(*LocalTransport)

		go func() {
			req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?match=https://example.com/books/1", nil).WithContext(t.Context())
			hub.SubscribeHandler(newSubscribeRecorder(), req)
		}()

		waitSubscribers(t, transport, 1)

		// Reload: context cancelled, Drain not called.
		cancelHub()
		synctest.Wait()

		transport.RLock()
		n := transport.subscribers.Len()
		transport.RUnlock()
		assert.Equal(t, 0, n, "writeTimeout==0 connection must exit on reload via the escape hatch")
	})
}

// syncDeadlineRecorder records socket write deadlines set by the handler goroutine.
type syncDeadlineRecorder struct {
	*httptest.ResponseRecorder

	mu        sync.Mutex
	deadlines []time.Time
}

func (r *syncDeadlineRecorder) SetWriteDeadline(deadline time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.deadlines = append(r.deadlines, deadline)

	return nil
}

func (r *syncDeadlineRecorder) last() time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.deadlines[len(r.deadlines)-1]
}

// A write blocked on a stalled client must not outlive the drain, even with dispatch_timeout 0.
func TestDrainShortensSocketWriteDeadline(t *testing.T) {
	t.Parallel()

	for _, writeTimeout := range []time.Duration{0, 20 * time.Minute} {
		t.Run(writeTimeout.String(), func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				const drainTimeout = 5 * time.Minute

				hub := hubDrainTestHub(t.Context(), t, writeTimeout, drainTimeout, WithDispatchTimeout(0))
				transport, _ := hub.transport.(*LocalTransport)
				w := &syncDeadlineRecorder{ResponseRecorder: httptest.NewRecorder()}

				go func() {
					req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?match=https://example.com/books/1", nil).WithContext(t.Context())
					hub.SubscribeHandler(w, req)
				}()

				waitSubscribers(t, transport, 1)

				hub.Drain()
				synctest.Wait()

				deadline := w.last()
				assert.False(t, deadline.IsZero(), "drain must set a socket write deadline")
				assert.False(t, deadline.After(time.Now().Add(drainTimeout)), "socket write deadline must fall within the drain window")

				time.Sleep(drainTimeout)
				synctest.Wait()
			})
		})
	}
}

// The drain only shortens deadlines: the dispatch margin it adds must not push the socket deadline past the token exp.
func TestDrainDoesNotExtendPastTokenExpiry(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		hub := hubDrainTestHub(t.Context(), t, 0, 2*time.Second, WithDispatchTimeout(5*time.Second))
		transport, _ := hub.transport.(*LocalTransport)
		w := &syncDeadlineRecorder{ResponseRecorder: httptest.NewRecorder()}

		exp := time.Now().Add(3 * time.Second)
		token := jwt.New(jwt.SigningMethodHS256)
		token.Header["typ"] = atJWTType
		token.Claims = &claims{
			Issuer:               testIssuer,
			Audience:             jwt.ClaimStrings{testResourceIdentifier},
			ExpiresAt:            jwt.NewNumericDate(exp),
			AuthorizationDetails: subscribeDetailsFromMatchers(nil, TopicMatcher{Type: MatcherTypeExact, Pattern: "*"}),
		}

		signedString, err := token.SignedString([]byte("subscriber"))
		require.NoError(t, err)

		go func() {
			req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?match=foo", nil).WithContext(t.Context())
			req.Header.Add("Authorization", bearerPrefix+signedString)
			hub.SubscribeHandler(w, req)
		}()

		waitSubscribers(t, transport, 1)

		hub.Drain()
		synctest.Wait()

		w.mu.Lock()
		for _, d := range w.deadlines {
			assert.False(t, d.After(exp), "socket write deadline %v must not exceed the token exp %v", d, exp)
		}
		w.mu.Unlock()

		time.Sleep(3 * time.Second)
		synctest.Wait()

		transport.RLock()
		n := transport.subscribers.Len()
		transport.RUnlock()
		assert.Equal(t, 0, n, "subscriber must disconnect by the token exp")
	})
}

// The disconnection timer is armed with time.Until(disconnectionTime), so a
// disconnectionTime in the past closes the connection as soon as it opens.
// Reachable whenever the write deadline is nearer than dispatchTimeout.
func TestNewResponseControllerDisconnectionTimeStaysInTheFuture(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name            string
		writeTimeout    time.Duration
		dispatchTimeout time.Duration
		tokenExpiresIn  time.Duration
	}{
		{name: "token expiring sooner than dispatchTimeout", writeTimeout: 600 * time.Second, dispatchTimeout: 5 * time.Second, tokenExpiresIn: 2 * time.Second},
		{name: "token expiring at exactly dispatchTimeout", writeTimeout: 600 * time.Second, dispatchTimeout: 5 * time.Second, tokenExpiresIn: 5 * time.Second},
		{name: "dispatchTimeout larger than writeTimeout", writeTimeout: 5 * time.Second, dispatchTimeout: 10 * time.Second},
		{name: "healthy defaults", writeTimeout: DefaultWriteTimeout, dispatchTimeout: DefaultDispatchTimeout},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := &Hub{opt: &opt{writeTimeout: tc.writeTimeout, dispatchTimeout: tc.dispatchTimeout}}

			s := &LocalSubscriber{}
			if tc.tokenExpiresIn != 0 {
				s.Claims = &claims{
					ExpiresAt: jwt.NewNumericDate(time.Now().Add(tc.tokenExpiresIn)),
				}
			}

			rc := h.newResponseController(httptest.NewRecorder(), s)

			assert.True(t, rc.disconnectionTime.After(time.Now()),
				"disconnectionTime is %v in the past, the connection would close immediately", time.Until(rc.disconnectionTime))
			assert.False(t, rc.disconnectionTime.After(rc.writeDeadline),
				"disconnectionTime must not outlive the write deadline")
		})
	}
}

// With neither a write timeout nor a token expiry there is no deadline, so no
// disconnection timer is armed and the zero time must be preserved.
func TestNewResponseControllerNoDeadline(t *testing.T) {
	t.Parallel()

	h := &Hub{opt: &opt{writeTimeout: 0, dispatchTimeout: DefaultDispatchTimeout}}
	rc := h.newResponseController(httptest.NewRecorder(), &LocalSubscriber{})

	assert.True(t, rc.writeDeadline.IsZero())
	assert.True(t, rc.disconnectionTime.IsZero())
}

type deadlineRecorder struct {
	*httptest.ResponseRecorder

	deadlines []time.Time
}

func (r *deadlineRecorder) SetWriteDeadline(deadline time.Time) error {
	r.deadlines = append(r.deadlines, deadline)

	return nil
}

// Without a write deadline, a subscriber that stops reading must still be cut off by the dispatch timeout.
func TestDispatchWriteDeadlineWithoutWriteTimeout(t *testing.T) {
	t.Parallel()

	h := &Hub{opt: &opt{writeTimeout: 0, dispatchTimeout: time.Second}}
	w := &deadlineRecorder{ResponseRecorder: httptest.NewRecorder()}
	rc := h.newResponseController(w, &LocalSubscriber{})

	require.True(t, rc.setDispatchWriteDeadline(t.Context()))
	require.Len(t, w.deadlines, 1)
	assert.WithinDuration(t, time.Now().Add(time.Second), w.deadlines[0], 100*time.Millisecond)
}

// refusingTransport records dispatched updates and removed subscribers and
// refuses to register subscribers, to exercise the registration-failure path.
type refusingTransport struct {
	dispatched []*Update
	removed    []*LocalSubscriber
}

func (t *refusingTransport) Dispatch(_ context.Context, u *Update) error {
	t.dispatched = append(t.dispatched, u)

	return nil
}

func (t *refusingTransport) AddSubscriber(context.Context, *LocalSubscriber) error {
	return ErrClosedTransport
}

func (t *refusingTransport) RemoveSubscriber(_ context.Context, s *LocalSubscriber) error {
	t.removed = append(t.removed, s)

	return nil
}

func (t *refusingTransport) Close(context.Context) error { return nil }

// A registration that fails must not announce a subscription that never
// existed, nor take it back with a compensating active:false.
func TestNoSubscriptionEventWhenRegistrationFails(t *testing.T) {
	t.Parallel()

	transport := &refusingTransport{}
	hub := createAnonymousDummy(t, WithSubscriptions(), WithTransport(transport))

	req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?match=https://example.com/foo", nil)
	w := httptest.NewRecorder()

	hub.SubscribeHandler(w, req)

	resp := w.Result()

	t.Cleanup(func() {
		require.NoError(t, resp.Body.Close())
	})

	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	assert.Empty(t, transport.dispatched)
}

// A transport may have listed the subscriber before failing, so it must be
// removed and disconnected rather than left behind.
func TestSubscriberRemovedWhenRegistrationFails(t *testing.T) {
	t.Parallel()

	transport := &refusingTransport{}
	hub := createAnonymousDummy(t, WithTransport(transport))

	req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?match=https://example.com/foo", nil)
	w := httptest.NewRecorder()

	hub.SubscribeHandler(w, req)

	resp := w.Result()

	t.Cleanup(func() {
		require.NoError(t, resp.Body.Close())
	})

	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	require.Len(t, transport.removed, 1)
	assert.True(t, transport.removed[0].disconnected.Load())
}

// The subscription is announced once it exists, so a subscriber authorized for
// the subscriptions namespace sees its own arrival and needs no reconciliation
// against the snapshot it fetched from the subscription API.
func TestSubscriptionEventReachesTheSubscriberItDescribes(t *testing.T) {
	t.Parallel()

	hub := createDummy(t, WithSubscriptions())

	req := httptest.NewRequest(http.MethodGet,
		defaultHubURL+"?match_urlpattern=/.well-known/mercure/subscriptions/:mt/:m/:s", nil)
	req.AddCookie(&http.Cookie{
		Name:  defaultCookieName,
		Value: createDummyAuthorizedJWT(roleSubscriber, []string{"*"}),
	})

	w := newSubscribeRecorder()

	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()

	hub.SubscribeHandler(w, req.WithContext(ctx))

	body := w.Body.String()
	assert.Contains(t, body, "event: mercure")

	subs := sseSubscriptions(t, body)
	require.NotEmpty(t, subs)
	assert.True(t, slices.ContainsFunc(subs, func(sub subscription) bool {
		return sub.Active && sub.Match == "/.well-known/mercure/subscriptions/:mt/:m/:s"
	}), "the subscriber was not told about its own subscription")
}

// A QUERY naming no media type at all is incorrect by definition, so it is a
// bad request rather than an unsupported one (RFC 10008, Section 2.3).
func TestQuerySubscribeWithoutMediaTypeRejectedWith400(t *testing.T) {
	t.Parallel()

	hub := createAnonymousDummy(t)

	req := httptest.NewRequest(methodQuery, defaultHubURL,
		strings.NewReader("match=https://example.com/books/1"))

	w := httptest.NewRecorder()
	hub.SubscribeHandler(w, req)

	resp := w.Result()

	t.Cleanup(func() { assert.NoError(t, resp.Body.Close()) })

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

// A QUERY naming a media type the hub cannot read as a subscription is
// unsupported: its content is not read as a form (RFC 10008, Section 2.3).
func TestQuerySubscribeUnsupportedMediaTypeRejectedWith415(t *testing.T) {
	t.Parallel()

	hub := createAnonymousDummy(t)

	req := httptest.NewRequest(methodQuery, defaultHubURL,
		strings.NewReader(`{"match": "https://example.com/books/1"}`))
	req.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()
	hub.SubscribeHandler(w, req)

	resp := w.Result()

	t.Cleanup(func() { assert.NoError(t, resp.Body.Close()) })

	assert.Equal(t, http.StatusUnsupportedMediaType, resp.StatusCode)
}

// Media type parameters do not change what the body is.
func TestQuerySubscribeMediaTypeParametersAccepted(t *testing.T) {
	t.Parallel()

	hub := createAnonymousDummy(t)

	ctx, cancel := context.WithCancel(t.Context())
	req := httptest.NewRequest(methodQuery, defaultHubURL,
		strings.NewReader("match=https://example.com/books/1")).WithContext(ctx)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")

	w := &responseTester{
		expectedStatusCode: http.StatusOK,
		expectedBody:       ":\n",
		tb:                 t,
		cancel:             cancel,
	}
	hub.SubscribeHandler(w, req)
}

// A subscription that named no media type it will read is not refusing any,
// and the most specific matching range decides. Only one refusing
// text/event-stream is answered 406, there being nothing left to send it.
func TestAcceptsEventStream(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		accept []string
		want   bool
	}{
		"absent":                 {nil, true},
		"empty":                  {[]string{""}, true},
		"exact":                  {[]string{"text/event-stream"}, true},
		"wildcard":               {[]string{"*/*"}, true},
		"type wildcard":          {[]string{"text/*"}, true},
		"weighted":               {[]string{"application/json;q=0.8, text/event-stream;q=0.2"}, true},
		"refused exact":          {[]string{"text/event-stream;q=0"}, false},
		"refused wildcard":       {[]string{"*/*;q=0"}, false},
		"specific grant wins":    {[]string{"*/*;q=0, text/event-stream"}, true},
		"specific refusal wins":  {[]string{"*/*, text/event-stream;q=0"}, false},
		"other types only":       {[]string{"application/json"}, false},
		"split over field lines": {[]string{"application/json;q=0.8", "text/event-stream;q=0.2"}, true},
		"split refusal":          {[]string{"application/json", "text/event-stream;q=0"}, false},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodGet, defaultHubURL, nil)
			for _, a := range tc.accept {
				req.Header.Add("Accept", a)
			}

			assert.Equal(t, tc.want, acceptsEventStream(req))
		})
	}
}

// A subscription refusing the only media type the hub streams leaves nothing
// to send it, whether it asked with GET or QUERY.
func TestSubscribeRefusedResponseMediaTypeRejectedWith406(t *testing.T) {
	t.Parallel()

	hub := createAnonymousDummy(t)

	req := httptest.NewRequest(http.MethodGet,
		defaultHubURL+"?match=https://example.com/books/1", nil)
	req.Header.Set("Accept", "text/event-stream;q=0")

	w := httptest.NewRecorder()
	hub.SubscribeHandler(w, req)

	resp := w.Result()

	t.Cleanup(func() { assert.NoError(t, resp.Body.Close()) })

	assert.Equal(t, http.StatusNotAcceptable, resp.StatusCode)
}

func TestQuerySubscribeRefusedResponseMediaTypeRejectedWith406(t *testing.T) {
	t.Parallel()

	hub := createAnonymousDummy(t)

	req := httptest.NewRequest(methodQuery, defaultHubURL,
		strings.NewReader("match=https://example.com/books/1"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	w := httptest.NewRecorder()
	hub.SubscribeHandler(w, req)

	resp := w.Result()

	t.Cleanup(func() { assert.NoError(t, resp.Body.Close()) })

	assert.Equal(t, http.StatusNotAcceptable, resp.StatusCode)
}

// A subscription is answered with the media types a QUERY body can express it
// in, so a client learns what the hub reads (RFC 10008, Section 3) — on
// refusals included: a client told 415 needs to know what to send instead.
func TestSubscribeAcceptQuery(t *testing.T) {
	t.Parallel()

	hub := createAnonymousDummy(t)

	ctx, cancel := context.WithCancel(t.Context())
	req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?match=https://example.com/foo", nil).WithContext(ctx)

	w := &responseTester{
		header:             http.Header{},
		expectedStatusCode: http.StatusOK,
		expectedBody:       ":\n",
		tb:                 t,
		cancel:             cancel,
	}
	hub.SubscribeHandler(w, req)

	assert.Equal(t, "application/x-www-form-urlencoded", w.Header().Get("Accept-Query"))
}

func TestSubscribeAcceptQueryOnUnsupportedMediaType(t *testing.T) {
	t.Parallel()

	hub := createAnonymousDummy(t)

	req := httptest.NewRequest(methodQuery, defaultHubURL, strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()
	hub.SubscribeHandler(w, req)

	resp := w.Result()

	t.Cleanup(func() { assert.NoError(t, resp.Body.Close()) })

	assert.Equal(t, http.StatusUnsupportedMediaType, resp.StatusCode)
	assert.Equal(t, "application/x-www-form-urlencoded", resp.Header.Get("Accept-Query"))
}

// A subscription asks intermediaries to forward each chunk as it is produced
// rather than buffered (RFC 10036), which SSE needs as much as any
// incremental response.
func TestSubscribeIncremental(t *testing.T) {
	t.Parallel()

	hub := createAnonymousDummy(t)

	ctx, cancel := context.WithCancel(t.Context())
	req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?match=https://example.com/foo", nil).WithContext(ctx)

	w := &responseTester{
		header:             http.Header{},
		expectedStatusCode: http.StatusOK,
		expectedBody:       ":\n",
		tb:                 t,
		cancel:             cancel,
	}
	hub.SubscribeHandler(w, req)

	assert.Equal(t, "?1", w.Header().Get("Incremental"))
}

// outBufferCapturingTransport records the out-channel capacity of the subscriber
// passed to AddSubscriber, so a test can assert the Hub option propagated.
type outBufferCapturingTransport struct {
	nopTransport

	capacity int
}

func (tr *outBufferCapturingTransport) AddSubscriber(_ context.Context, s *LocalSubscriber) error {
	tr.capacity = cap(s.out)

	return nil
}

// TestSubscribeHandlerThreadsOutBuffer proves the Hub option value reaches the
// created subscriber's channel end-to-end (WithSubscriberOutBuffer →
// h.subscriberOutBuffer → withOutBuffer → NewLocalSubscriber), not just the
// lower-level withOutBuffer option in isolation.
func TestSubscribeHandlerThreadsOutBuffer(t *testing.T) {
	t.Parallel()

	tr := &outBufferCapturingTransport{}
	hub := createAnonymousDummy(t, WithTransport(tr), WithSubscriberOutBuffer(32))

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?match=https://example.com/foo", nil).WithContext(ctx)

	// The client goes away as soon as the stream opens.
	hub.SubscribeHandler(&responseTester{expectedStatusCode: http.StatusOK, expectedBody: ":\n", cancel: cancel, tb: t}, req)

	assert.Equal(t, 32, tr.capacity, "WithSubscriberOutBuffer(32) must size the created subscriber's out channel")
}

// TestWithSubscriberOutBufferRejectsSubMinimum: a positive value below the floor
// is rejected at construction (not silently coerced to the default); 0 means
// "use the default" and at/above the floor is accepted verbatim.
func TestWithSubscriberOutBufferRejectsSubMinimum(t *testing.T) {
	t.Parallel()

	for _, size := range []int{-1, 1, MinSubscriberOutBuffer - 1} {
		_, err := NewHub(t.Context(), WithSubscriberOutBuffer(size))
		require.ErrorIs(t, err, ErrInvalidSubscriberOutBuffer, "size %d must be rejected, not silently coerced", size)
	}

	for _, size := range []int{0, MinSubscriberOutBuffer, 4096} {
		h, err := NewHub(t.Context(), WithSubscriberOutBuffer(size))
		require.NoError(t, err)
		assert.Equal(t, size, h.subscriberOutBuffer)
	}
}

type writeFlushSample struct {
	seconds float64
	ok      bool
}

// recordingMetrics records connects, the disconnect reasons reported through
// DisconnectReasonReporter, the plain SubscriberDisconnected calls, and the
// WriteFlushObserver observations.
type recordingMetrics struct {
	mu                  sync.Mutex
	connected           int
	disconnected        int
	disconnectedReasons []DisconnectReason
	writeFlushes        []writeFlushSample
}

func (m *recordingMetrics) SubscriberConnected(*LocalSubscriber) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.connected++
}

func (m *recordingMetrics) SubscriberDisconnected(*LocalSubscriber) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.disconnected++
}

func (*recordingMetrics) UpdatePublished(*Update) {}

func (m *recordingMetrics) SubscriberDisconnectedWithReason(_ *LocalSubscriber, reason DisconnectReason) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.disconnectedReasons = append(m.disconnectedReasons, reason)
}

func (m *recordingMetrics) ObserveWriteFlush(seconds float64, ok bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.writeFlushes = append(m.writeFlushes, writeFlushSample{seconds: seconds, ok: ok})
}

func (m *recordingMetrics) connects() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.connected
}

func (m *recordingMetrics) reasons() []DisconnectReason {
	m.mu.Lock()
	defer m.mu.Unlock()

	return slices.Clone(m.disconnectedReasons)
}

func (m *recordingMetrics) plainDisconnects() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.disconnected
}

func (m *recordingMetrics) writeFlushSamples() []writeFlushSample {
	m.mu.Lock()
	defer m.mu.Unlock()

	return slices.Clone(m.writeFlushes)
}

// baseOnlyMetrics implements Metrics but not DisconnectReasonReporter, and
// counts the SubscriberDisconnected calls.
type baseOnlyMetrics struct {
	mu           sync.Mutex
	disconnected int
}

func (*baseOnlyMetrics) SubscriberConnected(*LocalSubscriber) {}

func (m *baseOnlyMetrics) SubscriberDisconnected(*LocalSubscriber) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.disconnected++
}

func (*baseOnlyMetrics) UpdatePublished(*Update) {}

func (m *baseOnlyMetrics) plainDisconnects() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.disconnected
}

// noWriteFlushObserverMetrics implements Metrics but not WriteFlushObserver.
type noWriteFlushObserverMetrics struct{}

func (noWriteFlushObserverMetrics) SubscriberConnected(*LocalSubscriber)    {}
func (noWriteFlushObserverMetrics) SubscriberDisconnected(*LocalSubscriber) {}
func (noWriteFlushObserverMetrics) UpdatePublished(*Update)                 {}

// stubStreamWriter is a flushable ResponseWriter with deadline support whose
// Write, FlushError and SetWriteDeadline return the configured errors. By
// default it models HTTP/1: once a Write failed the next FlushError reports
// that error. With flushForgetsWriteErr it reproduces the sequence HTTP/2
// produces after a failed write that bypassed the buffer: every Write fails,
// FlushError returns nil, SetWriteDeadline returns nil.
type stubStreamWriter struct {
	header      http.Header
	writeErr    error
	flushErr    error
	deadlineErr error

	flushForgetsWriteErr bool

	writeFailed error
}

func (w *stubStreamWriter) Header() http.Header {
	if w.header == nil {
		w.header = http.Header{}
	}

	return w.header
}

func (w *stubStreamWriter) Write(p []byte) (int, error) {
	if w.writeErr != nil {
		if !w.flushForgetsWriteErr {
			w.writeFailed = w.writeErr
		}

		return 0, w.writeErr
	}

	return len(p), nil
}

func (*stubStreamWriter) WriteHeader(int) {}

func (w *stubStreamWriter) FlushError() error {
	if w.writeFailed != nil {
		return w.writeFailed
	}

	return w.flushErr
}

func (w *stubStreamWriter) SetWriteDeadline(time.Time) error { return w.deadlineErr }

// errTransportOffline is what errorRemovingTransport's RemoveSubscriber returns.
var errTransportOffline = errors.New("transport offline")

// errorRemovingTransport is a LocalTransport whose RemoveSubscriber fails with removeErr.
type errorRemovingTransport struct {
	*LocalTransport

	removeErr error
}

func (t *errorRemovingTransport) RemoveSubscriber(context.Context, *LocalSubscriber) error {
	return t.removeErr
}

// runSubscribeUntilIdle starts a subscription on w in the synctest bubble,
// waits for it to register and block, then runs act (which may be nil) and
// waits for the bubble to go idle.
func runSubscribeUntilIdle(ctx context.Context, t *testing.T, hub *Hub, w http.ResponseWriter, act func(transport *LocalTransport)) {
	t.Helper()

	transport, _ := hub.transport.(*LocalTransport)

	go func() {
		req := httptest.NewRequestWithContext(ctx, http.MethodGet, defaultHubURL+"?match=https://example.com/books/1", nil)
		hub.SubscribeHandler(w, req)
	}()

	synctest.Wait()

	transport.RLock()
	registered := transport.subscribers.Len()
	transport.RUnlock()

	require.Equal(t, 1, registered, "the subscriber must be registered")

	if act != nil {
		act(transport)
	}

	synctest.Wait()
}

// dispatchBook dispatches an update the subscription of runSubscribeUntilIdle receives.
func dispatchBook(t *testing.T) func(transport *LocalTransport) {
	t.Helper()

	return func(transport *LocalTransport) {
		require.NoError(t, transport.Dispatch(t.Context(), &Update{
			Topics: []string{"https://example.com/books/1"},
			Data:   "book",
		}))
	}
}

// Each way out of SubscribeHandler's loop is reported with its reason, through
// DisconnectReasonReporter only: SubscriberDisconnected must not
// also count it. Each exit follows exactly one SubscriberConnected.
func TestSubscribeHandlerReportsDisconnectReason(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name         string
		writeTimeout time.Duration
		options      []Option
		writer       func() http.ResponseWriter
		// act runs once the subscriber is registered; cancelHub and
		// cancelReq cancel the hub's and the request's contexts.
		act  func(t *testing.T, transport *LocalTransport, cancelHub, cancelReq context.CancelFunc)
		want DisconnectReason
	}{
		{
			name: "hub shutdown",
			writer: func() http.ResponseWriter {
				return newSubscribeRecorder()
			},
			act: func(_ *testing.T, _ *LocalTransport, cancelHub, _ context.CancelFunc) {
				cancelHub()
			},
			want: DisconnectReasonHubShutdown,
		},
		{
			name:         "client closed",
			writeTimeout: 5 * time.Minute,
			writer: func() http.ResponseWriter {
				return newSubscribeRecorder()
			},
			act: func(_ *testing.T, _ *LocalTransport, _, cancelReq context.CancelFunc) {
				cancelReq()
			},
			want: DisconnectReasonClientClosed,
		},
		{
			// Subscriptions enabled: the connect is reported as when they are not.
			name:         "client closed with subscriptions",
			writeTimeout: 5 * time.Minute,
			options:      []Option{WithSubscriptions()},
			writer: func() http.ResponseWriter {
				return newSubscribeRecorder()
			},
			act: func(_ *testing.T, _ *LocalTransport, _, cancelReq context.CancelFunc) {
				cancelReq()
			},
			want: DisconnectReasonClientClosed,
		},
		{
			// writeTimeout is shorter than the default dispatchTimeout, so the
			// timer is armed at the write deadline itself.
			name:         "write timeout",
			writeTimeout: 100 * time.Millisecond,
			writer: func() http.ResponseWriter {
				return newSubscribeRecorder()
			},
			act: func(*testing.T, *LocalTransport, context.CancelFunc, context.CancelFunc) {
				time.Sleep(101 * time.Millisecond)
			},
			want: DisconnectReasonWriteTimeout,
		},
		{
			// HTTP/1 model: the Flush after the failed Write reports it too.
			name:         "update write failed",
			writeTimeout: 5 * time.Minute,
			writer: func() http.ResponseWriter {
				return &stubStreamWriter{writeErr: io.ErrClosedPipe}
			},
			act: func(t *testing.T, transport *LocalTransport, _, _ context.CancelFunc) {
				t.Helper()

				dispatchBook(t)(transport)
			},
			want: DisconnectReasonWriteFailed,
		},
		{
			// HTTP/2 model: the Flush after the failed Write returns nil, so
			// only the Write's own error can classify the exit.
			name:         "update write failed, flush reports nothing",
			writeTimeout: 5 * time.Minute,
			writer: func() http.ResponseWriter {
				return &stubStreamWriter{writeErr: io.ErrClosedPipe, flushForgetsWriteErr: true}
			},
			act: func(t *testing.T, transport *LocalTransport, _, _ context.CancelFunc) {
				t.Helper()

				dispatchBook(t)(transport)
			},
			want: DisconnectReasonWriteFailed,
		},
		{
			name:         "heartbeat write failed",
			writeTimeout: 5 * time.Minute,
			options:      []Option{WithHeartbeat(50 * time.Millisecond)},
			writer: func() http.ResponseWriter {
				return &stubStreamWriter{writeErr: io.ErrClosedPipe}
			},
			act: func(*testing.T, *LocalTransport, context.CancelFunc, context.CancelFunc) {
				time.Sleep(100 * time.Millisecond)
			},
			want: DisconnectReasonWriteFailed,
		},
		{
			name:         "transport ended",
			writeTimeout: 5 * time.Minute,
			writer: func() http.ResponseWriter {
				return newSubscribeRecorder()
			},
			act: func(t *testing.T, transport *LocalTransport, _, _ context.CancelFunc) {
				t.Helper()

				require.NoError(t, transport.Close(t.Context()))
			},
			want: DisconnectReasonTransportEnded,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				metrics := &recordingMetrics{}
				hubCtx, cancelHub := context.WithCancel(t.Context())
				hub := hubShutdownTestHubWithOptions(hubCtx, t, tc.writeTimeout, append([]Option{WithMetrics(metrics)}, tc.options...)...)
				reqCtx, cancelReq := context.WithCancel(t.Context())

				runSubscribeUntilIdle(reqCtx, t, hub, tc.writer(), func(transport *LocalTransport) {
					tc.act(t, transport, cancelHub, cancelReq)
				})

				assert.Equal(t, []DisconnectReason{tc.want}, metrics.reasons())
				assert.Equal(t, 1, metrics.connects(), "one SubscriberConnected per handler exit, subscriptions enabled or not")
				assert.Zero(t, metrics.plainDisconnects(), "SubscriberDisconnected must not also count the disconnect")
			})
		})
	}
}

// A Metrics without DisconnectReasonReporter still gets SubscriberDisconnected.
func TestSubscribeHandlerBaseMetricsFallback(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		metrics := &baseOnlyMetrics{}
		hubCtx, cancelHub := context.WithCancel(t.Context())
		hub := hubShutdownTestHubWithOptions(hubCtx, t, 0, WithMetrics(metrics))

		runSubscribeUntilIdle(t.Context(), t, hub, newSubscribeRecorder(), func(*LocalTransport) { cancelHub() })

		assert.Equal(t, 1, metrics.plainDisconnects())
	})
}

// Every write is observed with its outcome, failures included, whichever
// step failed.
func TestWriteObserverOutcome(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name       string
		writer     http.ResponseWriter
		wantFailed bool
	}{
		{name: "healthy", writer: newSubscribeRecorder()},
		{name: "write fails", writer: &stubStreamWriter{writeErr: io.ErrClosedPipe}, wantFailed: true},
		{name: "write fails, flush reports nothing", writer: &stubStreamWriter{writeErr: io.ErrClosedPipe, flushForgetsWriteErr: true}, wantFailed: true},
		{name: "flush fails", writer: &stubStreamWriter{flushErr: io.ErrShortWrite}, wantFailed: true},
		// Needs a dispatch timeout to set the dispatch deadline at all.
		{name: "deadline fails", writer: &stubStreamWriter{deadlineErr: io.ErrClosedPipe}, wantFailed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				metrics := &recordingMetrics{}
				hub := hubShutdownTestHubWithOptions(t.Context(), t, 5*time.Minute, WithMetrics(metrics), WithDispatchTimeout(100*time.Millisecond))

				runSubscribeUntilIdle(t.Context(), t, hub, tc.writer, dispatchBook(t))

				samples := metrics.writeFlushSamples()
				require.Len(t, samples, 1)
				assert.Equal(t, !tc.wantFailed, samples[0].ok)
				assert.GreaterOrEqual(t, samples[0].seconds, 0.0)
			})
		})
	}
}

// Without WriteFlushObserver, writes still go through.
func TestWriteObserverNonImplementerNoOps(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		hub := hubShutdownTestHubWithOptions(t.Context(), t, 5*time.Minute, WithMetrics(noWriteFlushObserverMetrics{}))
		recorder := newSubscribeRecorder()

		runSubscribeUntilIdle(t.Context(), t, hub, recorder, dispatchBook(t))

		assert.Contains(t, recorder.Body.String(), "data: book")
	})
}

// A RemoveSubscriber failure turns an unclassified exit into transport_error,
// keeps a reason the loop already classified, and is logged.
func TestShutdownReasonOnTransportError(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		in, want DisconnectReason
	}{
		{in: DisconnectReasonUnknown, want: DisconnectReasonTransportError},
		{in: DisconnectReasonWriteFailed, want: DisconnectReasonWriteFailed},
	} {
		t.Run(string(tc.in), func(t *testing.T) {
			t.Parallel()

			metrics := &recordingMetrics{}
			logs := &recordingLogHandler{}
			hub := hubShutdownTestHubWithOptions(t.Context(), t, 0, WithMetrics(metrics), WithLogger(slog.New(logs)))
			hub.transport = &errorRemovingTransport{LocalTransport: hub.transport.(*LocalTransport), removeErr: errTransportOffline}

			hub.shutdown(t.Context(), NewLocalSubscriber("", slog.Default(), hub.topicMatcherStore), tc.in)

			assert.Equal(t, []DisconnectReason{tc.want}, metrics.reasons())
			assert.Zero(t, metrics.plainDisconnects())
			assert.Len(t, logs.withError(slog.LevelError, errTransportOffline), 1, "records: %v", logs.messages())
		})
	}
}
