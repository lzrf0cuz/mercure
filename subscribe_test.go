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
			Event:  Event{Data: "Hello World"},
		})
	}()

	assert.Panics(t, func() {
		hub.SubscribeHandler(
			&responseWriterMock{},
			httptest.NewRequest(http.MethodGet, defaultHubURL+"?topic=https://example.com/foo", nil),
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

	req.AddCookie(&http.Cookie{Name: "mercureAuthorization", Value: "invalid", HttpOnly: true, Secure: true, SameSite: http.SameSiteStrictMode})

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

	req.AddCookie(&http.Cookie{Name: "mercureAuthorization", Value: createDummyUnauthorizedJWT(), HttpOnly: true, Secure: true, SameSite: http.SameSiteStrictMode})
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

	req.AddCookie(&http.Cookie{Name: "mercureAuthorization", Value: createDummyNoneSignedJWT(), HttpOnly: true, Secure: true, SameSite: http.SameSiteStrictMode})

	hub.SubscribeHandler(w, req)

	resp := w.Result()

	t.Cleanup(func() {
		require.NoError(t, resp.Body.Close())
	})

	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	assert.Equal(t, http.StatusText(http.StatusUnauthorized)+"\n", w.Body.String())
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
	assert.Equal(t, "Missing \"topic\" parameter.\n", w.Body.String())
}

func TestSubscribeTooManyTopics(t *testing.T) {
	t.Parallel()

	hub := createAnonymousDummy(t)

	q := url.Values{}
	for i := 0; i <= maxQueryTopics; i++ {
		q.Add("topic", "https://example.com/foo")
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

	req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?topic=https://example.com/foo", nil)
	req.Header.Add("Authorization", bearerPrefix+createDummyAuthorizedJWT(roleSubscriber, scope))

	w := httptest.NewRecorder()
	hub.SubscribeHandler(w, req)

	resp := w.Result()

	t.Cleanup(func() {
		require.NoError(t, resp.Body.Close())
	})

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

	req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?topic=foo", nil)
	w := httptest.NewRecorder()

	hub.SubscribeHandler(w, req)

	resp := w.Result()

	t.Cleanup(func() {
		require.NoError(t, resp.Body.Close())
	})

	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	assert.Equal(t, http.StatusText(http.StatusServiceUnavailable)+"\n", w.Body.String())
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
			Event:  Event{Data: "Hello World", ID: "a"},
		})
		_ = hub.transport.Dispatch(ctx, &Update{
			Topics: []string{"https://example.com/books/1"},
			Event:  Event{Data: "Hello World", ID: "b"},
		})
		_ = hub.transport.Dispatch(ctx, &Update{
			Topics: []string{"https://example.com/reviews/22"},
			Event:  Event{Data: "Great", ID: "c"},
		})
		_ = hub.transport.Dispatch(ctx, &Update{
			Topics: []string{"https://example.com/hub?topic=faulty{iri"},
			Event:  Event{Data: "Faulty IRI", ID: "d"},
		})
		_ = hub.transport.Dispatch(ctx, &Update{
			Topics: []string{"string"},
			Event:  Event{Data: "string", ID: "e"},
		})
	}()

	var wg sync.WaitGroup

	for range numberOfSubscribers {
		wg.Go(func() {
			ctx, cancel := context.WithCancel(tb.Context())
			req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?topic=https://example.com/books/1&topic=string&topic=https://example.com/reviews/{id}&topic=https://example.com/hub?topic=faulty{iri", nil).WithContext(ctx)

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
	req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?topic=https://example.com/reviews/{id}", nil).WithContext(ctx)
	req.AddCookie(&http.Cookie{Name: "mercureAuthorization", Value: createDummyAuthorizedJWTWithPayload(roleSubscriber, []string{"https://example.com/reviews/22"}, payload), HttpOnly: true, Secure: true, SameSite: http.SameSiteStrictMode})

	w := &responseTester{
		expectedStatusCode: http.StatusOK,
		expectedBody:       ":\n",
		tb:                 t,
		cancel:             cancel,
	}

	hub.SubscribeHandler(w, req)
}

// TestSubscribePerUpdateDebugLogBoundedFields is the literal canary for
// the "no slog.Any(*Update)" invariant on the subscribe-side per-update
// Debug log at subscribe.go. A regression to slog.Any("update", update)
// would silently reopen the publisher-controlled-Type log-flood surface
// (amplified by subscriber fanout). The canary publishes an update with a
// sentinel Type, drives one subscriber, and asserts the sentinel does NOT
// appear in the captured Debug output, while the bounded fields DO.
func TestSubscribePerUpdateDebugLogBoundedFields(t *testing.T) {
	t.Parallel()

	const sentinelType = "SENTINEL_SUBSCRIBER_TYPE_SHOULD_NOT_LEAK"

	var buf bytes.Buffer

	// Wrap with NewSlogHandler so the canary exercises the full production
	// log path — the mercureHandler middleware extracts UpdateContextKey
	// from ctx and emits slog.Any("update", u). Without the wrapper, this
	// test bypasses mercureHandler entirely and would miss a regression
	// that re-leaks Type through that path. JSON handler so we can
	// distinguish the bounded update-attrs from subscriber-injected attrs
	// (mercureHandler also auto-injects SubscriberContextKey, whose
	// subscriber.topics field coincidentally shares the topic URL we
	// publish to).
	logger := slog.New(NewSlogHandler(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	hub := createAnonymousDummy(t, WithLogger(logger))

	ctx, cancel := context.WithCancel(t.Context())

	localTransport, _ := hub.transport.(*LocalTransport)

	go func() {
		// Wait for the subscriber to register before dispatching, mirroring the
		// subscribe() helper's readiness gate.
		for {
			localTransport.RLock()
			ready := localTransport.subscribers.Len() == 1
			localTransport.RUnlock()

			if ready {
				break
			}
		}

		_ = hub.transport.Dispatch(ctx, &Update{
			Topics: []string{"https://example.com/canary"},
			Event:  Event{Data: "canary-data", ID: "urn:uuid:canary-sub", Type: sentinelType},
		})
	}()

	req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?topic=https://example.com/canary", nil).WithContext(ctx)

	w := &responseTester{
		expectedStatusCode: http.StatusOK,
		expectedBody:       ":\nevent: " + sentinelType + "\nid: urn:uuid:canary-sub\ndata: canary-data\n\n",
		tb:                 t,
		cancel:             cancel,
	}

	hub.SubscribeHandler(w, req)

	// Find the "Update sent" record and parse it structurally — the line
	// will also contain mercureHandler-injected `subscriber.topics` which
	// shares the publish URL, so a substring match for the URL is too
	// loose. JSON parsing lets the assertion target the bounded update
	// attributes specifically.
	var updateSentRecord map[string]any

	for line := range strings.SplitSeq(buf.String(), "\n") {
		if line == "" {
			continue
		}

		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			continue
		}

		if rec["msg"] == "Update sent" {
			updateSentRecord = rec

			break
		}
	}

	require.NotNil(t, updateSentRecord,
		"per-update Debug log record must fire — without it the rest of this canary is vacuous")

	// Bounded-fields positive control: update_id present and equal to the
	// dispatched ID.
	assert.Equal(t, "urn:uuid:canary-sub", updateSentRecord["update_id"],
		"bounded update_id field must reach the log record verbatim")

	// Negative: no top-level "update" group attribute. A regression to
	// `slog.Any("update", update)` would produce a nested "update" object
	// here (because Update implements LogValuer / GroupValue). This
	// invariant catches the regression independent of what LogValue does
	// today — even if a future LogValue removes Type, the per-subscriber
	// fanout cost of group-logging the full update remains.
	_, hasUpdateGroup := updateSentRecord["update"]
	assert.False(t, hasUpdateGroup,
		"no slog.Any(*Update) on the per-update Debug record; regression form: slog.Any(\"update\", update)")

	// Negative: no top-level "type" attr — explicit guard against a
	// regression that re-adds Type as a separate bounded attribute on the
	// subscribe path (operationally it's the publish-side log that owns
	// Type forensics, not per-subscriber delivery records).
	_, hasType := updateSentRecord["type"]
	assert.False(t, hasType,
		"publisher-controlled Type must NOT appear as a top-level attr on the per-update Debug record")

	// Negative: no top-level "topics" attr — subscribe-side omits them
	// (identical across every subscriber; fanout-multiplied cost).
	_, hasTopics := updateSentRecord["topics"]
	assert.False(t, hasTopics, "topics must NOT appear; regression form: slog.Any(\"topics\", update.Topics)")

	// Negative: sentinelType doesn't appear anywhere in the marshaled
	// record (catches Type leaking under any nested key shape).
	asJSON, err := json.Marshal(updateSentRecord)
	require.NoError(t, err, "log record must round-trip through json.Marshal — otherwise the sentinel scan below cannot run")
	assert.NotContains(t, string(asJSON), sentinelType,
		"publisher-controlled Type sentinel must not appear anywhere in the per-update Debug record")
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
	req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?topic=https://example.com/", nil).WithContext(ctx)

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
			req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?topic=https://example.com/books/1", nil).WithContext(ctx)
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
				Topics:  []string{"https://example.com/reviews/21"},
				Event:   Event{Data: "Foo", ID: "a"},
				Private: true,
			})
			_ = hub.transport.Dispatch(ctx, &Update{
				Topics:  []string{"https://example.com/reviews/22"},
				Event:   Event{Data: "Hello World", ID: "b", Type: "test"},
				Private: true,
			})
			_ = hub.transport.Dispatch(ctx, &Update{
				Topics:  []string{"https://example.com/reviews/23"},
				Event:   Event{Data: "Great", ID: "c", Retry: 1},
				Private: true,
			})

			return
		}
	}()

	ctx, cancel := context.WithCancel(t.Context())
	req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?topic=https://example.com/reviews/{id}", nil).WithContext(ctx)
	req.AddCookie(&http.Cookie{Name: "mercureAuthorization", Value: createDummyAuthorizedJWT(roleSubscriber, []string{"https://example.com/reviews/22", "https://example.com/reviews/23"}), HttpOnly: true, Secure: true, SameSite: http.SameSiteStrictMode})

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

	hub := createDummy(t, WithSubscriptions())

	ctx1, cancel1 := context.WithCancel(t.Context())
	t.Cleanup(cancel1)

	ctx2, cancel2 := context.WithCancel(t.Context())
	t.Cleanup(cancel2)

	var wg sync.WaitGroup

	wg.Go(func() {
		// Authorized to receive connection events
		req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?topic=/.well-known/mercure/subscriptions/{topic}/{subscriber}", nil).WithContext(ctx1)
		req.AddCookie(&http.Cookie{Name: "mercureAuthorization", Value: createDummyAuthorizedJWT(roleSubscriber, []string{"/.well-known/mercure/subscriptions/{topic}/{subscriber}"}), HttpOnly: true, Secure: true, SameSite: http.SameSiteStrictMode})

		w := newSubscribeRecorder()
		hub.SubscribeHandler(w, req)

		resp := w.Result()

		t.Cleanup(func() {
			_ = resp.Body.Close()
		})

		body, _ := io.ReadAll(resp.Body)

		assert.Equal(t, http.StatusOK, resp.StatusCode)

		bodyContent := string(body)
		assert.Contains(t, bodyContent, `data:   "@context": "https://mercure.rocks/",`)
		assert.Regexp(t, `(?m)^data:   "id": "/\.well-known/mercure/subscriptions/https%3A%2F%2Fexample\.com/.*,$`, bodyContent)
		assert.Contains(t, bodyContent, `data:   "type": "Subscription",`)
		assert.Contains(t, bodyContent, `data:   "subscriber": "urn:uuid:`)
		assert.Contains(t, bodyContent, `data:   "topic": "https://example.com",`)
		assert.Contains(t, bodyContent, `data:   "active": true,`)
		assert.Contains(t, bodyContent, `data:   "active": false,`)
		assert.Contains(t, bodyContent, `data:   "payload": {`)
		assert.Contains(t, bodyContent, `data:     "foo": "bar"`)
	})

	wg.Go(func() {
		// Not authorized to receive connection events
		req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?topic=/.well-known/mercure/subscriptions/{topicSelector}/{subscriber}", nil).WithContext(ctx2)
		req.AddCookie(&http.Cookie{Name: "mercureAuthorization", Value: createDummyAuthorizedJWT(roleSubscriber, []string{}), HttpOnly: true, Secure: true, SameSite: http.SameSiteStrictMode})

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
		ctx := t.Context()

		for {
			_, s, _ := hub.transport.(TransportSubscribers).GetSubscribers(ctx)
			if len(s) == 2 {
				break
			}
		}

		ctx, cancelRequest2 := context.WithCancel(ctx)
		req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?topic=https://example.com", nil).WithContext(ctx)
		req.AddCookie(&http.Cookie{Name: "mercureAuthorization", Value: createDummyAuthorizedJWT(roleSubscriber, []string{}), HttpOnly: true, Secure: true, SameSite: http.SameSiteStrictMode})

		w := &responseTester{
			expectedStatusCode: http.StatusOK,
			expectedBody:       ":\n",
			tb:                 t,
			cancel:             cancelRequest2,
		}
		hub.SubscribeHandler(w, req)
		time.Sleep(1 * time.Second) // TODO: find a better way to wait for the disconnection update to be dispatched
		cancel2()
		cancel1()
	})

	wg.Wait()
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
				Topics:  []string{"https://example.com/reviews/21"},
				Event:   Event{Data: "Foo", ID: "a"},
				Private: true,
			})
			_ = hub.transport.Dispatch(ctx, &Update{
				Topics:  []string{"https://example.com/reviews/22"},
				Event:   Event{Data: "Hello World", ID: "b", Type: "test"},
				Private: true,
			})

			return
		}
	}()

	ctx, cancel := context.WithCancel(t.Context())
	req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?topic=https://example.com/reviews/{id}", nil).WithContext(ctx)
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
			Event: Event{
				ID:   "a",
				Data: "d1",
			},
		}))
		require.NoError(t, transport.Dispatch(ctx, &Update{
			Topics: []string{"https://example.com/foos/b"},
			Event: Event{
				ID:   "b",
				Data: "d2",
			},
		}))

		// Using deprecated 'Last-Event-ID' query parameter
		go func() {
			ctx, cancel := context.WithCancel(t.Context())
			req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?topic=https://example.com/foos/{id}&Last-Event-ID=a", nil).WithContext(ctx)

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
			req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?topic=https://example.com/foos/{id}&lastEventID=a", nil).WithContext(ctx)

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
			req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?topic=https://example.com/foos/{id}", nil).WithContext(ctx)
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
			Event: Event{
				ID:   "a",
				Data: "d1",
			},
		}))
		require.NoError(t, transport.Dispatch(ctx, &Update{
			Topics: []string{"https://example.com/foos/b"},
			Event: Event{
				ID:   "b",
				Data: "d2",
			},
		}))

		go func() {
			ctx, cancel := context.WithCancel(t.Context())
			req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?topic=https://example.com/foos/{id}&lastEventID="+EarliestLastEventID, nil).WithContext(ctx)

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
			req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?topic=https://example.com/foos/{id}", nil).WithContext(ctx)
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
			Event: Event{
				ID:   "a",
				Data: "d1",
			},
		}))

		ctx := t.Context()

		go func(ctx context.Context) {
			c, cancel := context.WithCancel(ctx)
			req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?topic=https://example.com/foos/{id}&lastEventID=unknown", nil).WithContext(c)

			w := &responseTester{
				header:             http.Header{},
				expectedStatusCode: http.StatusOK,
				expectedBody:       ":\nid: b\ndata: d2\n\n",
				tb:                 t,
				cancel:             cancel,
			}

			hub.SubscribeHandler(w, req)
			assert.Equal(t, "a", w.Header().Get("Last-Event-ID"))
		}(ctx)

		go func(ctx context.Context) {
			c, cancel := context.WithCancel(ctx)
			req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?topic=https://example.com/foos/{id}", nil).WithContext(c)
			req.Header.Add("Last-Event-ID", "unknown")

			w := &responseTester{
				header:             http.Header{},
				expectedStatusCode: http.StatusOK,
				expectedBody:       ":\nid: b\ndata: d2\n\n",
				tb:                 t,
				cancel:             cancel,
			}

			hub.SubscribeHandler(w, req)
			assert.Equal(t, "a", w.Header().Get("Last-Event-ID"))
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
			Event: Event{
				ID:   "b",
				Data: "d2",
			},
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
			Event:  Event{ID: "a", Data: "d1"},
		}))
		// Private event the anonymous subscriber is NOT authorized to
		// read. Its id must not appear in the Last-Event-ID response.
		require.NoError(t, transport.Dispatch(t.Context(), &Update{
			Topics:  []string{"https://example.com/foos/b"},
			Private: true,
			Event:   Event{ID: "b", Data: "secret"},
		}))

		ctx := t.Context()

		go func(ctx context.Context) {
			c, cancel := context.WithCancel(ctx)
			req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?topic=https://example.com/foos/{id}&lastEventID=unknown", nil).WithContext(c)

			w := &responseTester{
				header:             http.Header{},
				expectedStatusCode: http.StatusOK,
				expectedBody:       ":\nid: c\ndata: d3\n\n",
				tb:                 t,
				cancel:             cancel,
			}

			hub.SubscribeHandler(w, req)
			// Authorized "a" leaks back as the recovery anchor; the
			// private "b" does not, even though it is the most recent
			// in-history event.
			assert.Equal(t, "a", w.Header().Get("Last-Event-ID"))
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
			Event:  Event{ID: "c", Data: "d3"},
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
			req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?topic=https://example.com/foos/{id}&lastEventID=unknown", nil).WithContext(ctx)

			w := &responseTester{
				header:             http.Header{},
				expectedStatusCode: http.StatusOK,
				expectedBody:       ":\nid: b\ndata: d2\n\n",
				tb:                 t,
				cancel:             cancel,
			}

			hub.SubscribeHandler(w, req)
			assert.Equal(t, EarliestLastEventID, w.Header().Get("Last-Event-ID"))
		}()

		go func() {
			ctx, cancel := context.WithCancel(ctx)
			req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?topic=https://example.com/foos/{id}", nil).WithContext(ctx)
			req.Header.Add("Last-Event-ID", "unknown")

			w := &responseTester{
				header:             http.Header{},
				expectedStatusCode: http.StatusOK,
				expectedBody:       ":\nid: b\ndata: d2\n\n",
				tb:                 t,
				cancel:             cancel,
			}

			hub.SubscribeHandler(w, req)
			assert.Equal(t, EarliestLastEventID, w.Header().Get("Last-Event-ID"))
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
			Event: Event{
				ID:   "b",
				Data: "d2",
			},
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
				Event:  Event{Data: "Hello World", ID: "b"},
			})

			return
		}
	}()

	ctx, cancel := context.WithCancel(ctx)
	req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?topic=https://example.com/books/1&topic=https://example.com/reviews/{id}", nil).WithContext(ctx)

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

	token.Claims = &claims{
		Mercure: mercureClaim{
			Subscribe: []string{"*"},
		},
		RegisteredClaims: jwt.RegisteredClaims{ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Second))},
	}

	signedString, err := token.SignedString([]byte("subscriber"))
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?topic=foo", nil)
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

// hubShutdownTestHubWithOptions builds a hub like hubShutdownTestHub but lets
// the caller append extra options (e.g. WithMetrics for reason-tracking tests).
func hubShutdownTestHubWithOptions(ctx context.Context, tb testing.TB, writeTimeout time.Duration, extra ...Option) *Hub {
	tb.Helper()

	tss, err := NewTopicSelectorStore(0)
	require.NoError(tb, err)

	opts := make([]Option, 0, 5+len(extra))
	opts = append(
		opts,
		WithAnonymous(),
		WithPublisherJWT([]byte("publisher"), jwt.SigningMethodHS256.Name),
		WithSubscriberJWT([]byte("subscriber"), jwt.SigningMethodHS256.Name),
		WithTopicSelectorStore(tss),
		WithWriteTimeout(writeTimeout),
	)
	opts = append(opts, extra...)

	h, err := NewHub(ctx, opts...)
	require.NoError(tb, err)
	setDeprecatedOptions(tb, h)

	return h
}

type writeFlushSample struct {
	seconds float64
	ok      bool
}

// recordingMetrics records the disconnect reason emitted via the optional
// DisconnectReasonReporter interface, the count of legacy
// SubscriberDisconnected fallback calls, and the per-call write+flush
// latency observations from the optional WriteFlushObserver interface.
// Tests use it to verify SubscribeHandler's exit-path classification and
// the h.write hot-path observation contract.
type recordingMetrics struct {
	mu                  sync.Mutex
	connected           int
	disconnected        int
	disconnectedReasons []DisconnectReason
	writeFlushes        []writeFlushSample
}

func (m *recordingMetrics) SubscriberConnected(_ *LocalSubscriber) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.connected++
}

func (m *recordingMetrics) SubscriberDisconnected(_ *LocalSubscriber) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.disconnected++
}

func (m *recordingMetrics) UpdatePublished(_ *Update) {}

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

func (m *recordingMetrics) reasons() []DisconnectReason {
	m.mu.Lock()
	defer m.mu.Unlock()

	out := make([]DisconnectReason, len(m.disconnectedReasons))
	copy(out, m.disconnectedReasons)

	return out
}

func (m *recordingMetrics) legacyDisconnects() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.disconnected
}

func (m *recordingMetrics) writeFlushSamples() []writeFlushSample {
	m.mu.Lock()
	defer m.mu.Unlock()

	out := make([]writeFlushSample, len(m.writeFlushes))
	copy(out, m.writeFlushes)

	return out
}

// legacyOnlyMetrics implements Metrics but deliberately does NOT implement
// DisconnectReasonReporter, so SubscribeHandler must fall through to the
// legacy SubscriberDisconnected path. Counts the legacy calls so a regression
// that bypasses the path entirely (e.g. an early-return between the type
// assertion and the legacy call) fails this fixture's assertion — NopMetrics
// alone cannot detect that, since its method is a no-op.
type legacyOnlyMetrics struct {
	mu           sync.Mutex
	disconnected int
}

func (m *legacyOnlyMetrics) SubscriberConnected(_ *LocalSubscriber) {}

func (m *legacyOnlyMetrics) SubscriberDisconnected(_ *LocalSubscriber) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.disconnected++
}

func (m *legacyOnlyMetrics) UpdatePublished(_ *Update) {}

func (m *legacyOnlyMetrics) legacyDisconnects() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.disconnected
}

// failingWriteRecorder is a minimal http.ResponseWriter whose Write always
// returns io.ErrClosedPipe — the failure shape matches an SSE client that
// dropped its TCP connection mid-response. Used to exercise the
// write_failed disconnect-reason classification in (h *Hub).write.
type failingWriteRecorder struct {
	header http.Header
}

func (r *failingWriteRecorder) Header() http.Header {
	if r.header == nil {
		r.header = http.Header{}
	}

	return r.header
}

func (r *failingWriteRecorder) Write([]byte) (int, error) {
	return 0, io.ErrClosedPipe
}

func (r *failingWriteRecorder) WriteHeader(_ int) {}

func (r *failingWriteRecorder) Flush() {}

func (r *failingWriteRecorder) FlushError() error { return nil }

func (r *failingWriteRecorder) SetWriteDeadline(_ time.Time) error { return nil }

// flushFailingWriter accepts Write but errors on FlushError, modeling the
// "TCP buffered the bytes but the kernel rejected the flush" failure
// shape. Used to exercise the rc.flush(...)==false branch in (h *Hub).write
// where ok is set false AFTER Write succeeded — distinct from
// failingWriteRecorder which short-circuits at Write.
type flushFailingWriter struct {
	header http.Header
}

func (r *flushFailingWriter) Header() http.Header {
	if r.header == nil {
		r.header = http.Header{}
	}

	return r.header
}

func (r *flushFailingWriter) Write(p []byte) (int, error)        { return len(p), nil }
func (r *flushFailingWriter) WriteHeader(_ int)                  {}
func (r *flushFailingWriter) Flush()                             {}
func (r *flushFailingWriter) FlushError() error                  { return io.ErrShortWrite }
func (r *flushFailingWriter) SetWriteDeadline(_ time.Time) error { return nil }

// deadlineFailingWriter accepts Write and Flush but errors on
// SetWriteDeadline — modeling a kernel-level "cannot set per-connection
// write deadline" failure. Exercises the rc.setDispatchWriteDeadline → false
// early-return path in (h *Hub).write, the most operationally-load-bearing
// failure mode the histogram is meant to catch (kernel/conn-pool issues
// silently masquerading as healthy writes if the observation were skipped).
type deadlineFailingWriter struct {
	header http.Header
}

func (r *deadlineFailingWriter) Header() http.Header {
	if r.header == nil {
		r.header = http.Header{}
	}

	return r.header
}

func (r *deadlineFailingWriter) Write(p []byte) (int, error)        { return len(p), nil }
func (r *deadlineFailingWriter) WriteHeader(_ int)                  {}
func (r *deadlineFailingWriter) Flush()                             {}
func (r *deadlineFailingWriter) FlushError() error                  { return nil }
func (r *deadlineFailingWriter) SetWriteDeadline(_ time.Time) error { return io.ErrClosedPipe }

// syncBuffer is a goroutine-safe bytes.Buffer for capturing slog handler
// output in concurrent tests. Required because subscribe-handler tests run
// the handler in a goroutine that may emit logs in parallel with the
// test goroutine's read of the captured output.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.String()
}

// errTransportOffline is the canned sentinel errorRemovingTransport returns
// from RemoveSubscriber so the shutdown-override tests can errors.Is-check
// the cause without coupling to the message.
var errTransportOffline = errors.New("transport offline")

// errorRemovingTransport wraps a LocalTransport but always returns a sentinel
// error from RemoveSubscriber, so tests can exercise the shutdown override
// branch (Unknown -> TransportError).
type errorRemovingTransport struct {
	*LocalTransport

	removeErr error
}

func (t *errorRemovingTransport) RemoveSubscriber(_ context.Context, _ *LocalSubscriber) error {
	return t.removeErr
}

// TestSubscribeHandlerReportsHubShutdownReason verifies the proximate cause
// for an exit driven by hub-context cancellation is classified as
// hub_shutdown (not unknown), AND that the subscriber actually drains from
// the transport's subscriber set. The drain assertion is the escape-hatch
// invariant — with writeTimeout=0 there's no per-connection disconnection
// timer, so http.Server.Shutdown would hang on the active handler unless
// hub-ctx-Done unblocks the select.
func TestSubscribeHandlerReportsHubShutdownReason(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		metrics := &recordingMetrics{}
		hubCtx, cancelHub := context.WithCancel(t.Context())
		hub := hubShutdownTestHubWithOptions(hubCtx, t, 0, WithMetrics(metrics))
		transport, _ := hub.transport.(*LocalTransport)

		go func() {
			req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?topic=https://example.com/books/1", nil).WithContext(t.Context())
			hub.SubscribeHandler(newSubscribeRecorder(), req)
		}()

		waitForSubscriber(t, transport)

		cancelHub()
		synctest.Wait()

		assert.Equal(t, []DisconnectReason{DisconnectReasonHubShutdown}, metrics.reasons())
		assert.Equal(t, 0, metrics.legacyDisconnects(),
			"reporter-path must early-return; legacy SubscriberDisconnected must NOT also fire (double-count guard)")

		transport.RLock()
		n := transport.subscribers.Len()
		transport.RUnlock()
		assert.Equal(t, 0, n, "subscriber must exit on hub shutdown when writeTimeout is 0")
	})
}

// TestSubscribeHandlerReportsClientClosedReason verifies that a request-context
// cancellation (the steady-state TCP-close path) is classified as client_closed.
func TestSubscribeHandlerReportsClientClosedReason(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		metrics := &recordingMetrics{}
		hub := hubShutdownTestHubWithOptions(t.Context(), t, 5*time.Minute, WithMetrics(metrics))
		transport, _ := hub.transport.(*LocalTransport)

		reqCtx, cancelReq := context.WithCancel(t.Context())
		go func() {
			req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?topic=https://example.com/books/1", nil).WithContext(reqCtx)
			hub.SubscribeHandler(newSubscribeRecorder(), req)
		}()

		waitForSubscriber(t, transport)

		cancelReq()
		synctest.Wait()

		assert.Equal(t, []DisconnectReason{DisconnectReasonClientClosed}, metrics.reasons())
		assert.Equal(t, 0, metrics.legacyDisconnects(),
			"reporter-path must early-return; legacy SubscriberDisconnected must NOT also fire (double-count guard)")
	})
}

// TestSubscribeHandlerReportsWriteTimeoutReason verifies the disconnection
// timer firing path: with writeTimeout set and dispatchTimeout=0,
// disconnectionTime equals time.Now()+writeTimeout. With no other timers
// armed (heartbeat unset) and no traffic, the for-loop's only firing case
// is disconnectionTimerC — which classifies as write_timeout. synctest.Wait
// is the right sync primitive: waitForSubscriber can miss the brief
// live-subscriber window because the AddSubscriber → disconnectionTimer →
// RemoveSubscriber sequence completes in a single time advance.
func TestSubscribeHandlerReportsWriteTimeoutReason(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		metrics := &recordingMetrics{}
		hub := hubShutdownTestHubWithOptions(t.Context(), t, 100*time.Millisecond, WithMetrics(metrics))

		go func() {
			req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?topic=https://example.com/books/1", nil).WithContext(t.Context())
			hub.SubscribeHandler(newSubscribeRecorder(), req)
		}()

		synctest.Wait()

		assert.Equal(t, []DisconnectReason{DisconnectReasonWriteTimeout}, metrics.reasons())
		assert.Equal(t, 0, metrics.legacyDisconnects(),
			"reporter-path must early-return; legacy SubscriberDisconnected must NOT also fire (double-count guard)")
	})
}

// TestSubscribeHandlerReportsWriteFailedReason verifies the update-write
// failure path: a Dispatch lands on the subscriber's channel, the for-loop's
// s.Receive case fires, the underlying ResponseWriter's Write returns an
// error, so (h *Hub).write returns false and the loop sets
// disconnectReason=write_failed before returning. The invariant under guard:
// folding the slog level-gate into the err != nil expression silently
// swallows write errors at INFO+, so write_failed never fires and the
// eventual shutdown gets mislabeled (typically client_closed when the
// request ctx finally cancels). The level-gate must stay split from the
// err check so the return-false path always observes write errors.
func TestSubscribeHandlerReportsWriteFailedReason(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		metrics := &recordingMetrics{}
		hub := hubShutdownTestHubWithOptions(t.Context(), t, 5*time.Minute, WithMetrics(metrics))
		transport, _ := hub.transport.(*LocalTransport)

		go func() {
			req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?topic=https://example.com/books/1", nil).WithContext(t.Context())
			hub.SubscribeHandler(&failingWriteRecorder{}, req)
		}()

		waitForSubscriber(t, transport)

		// Drive a Dispatch into the subscriber's channel; the handler wakes,
		// h.write fails on failingWriteRecorder, reason=WriteFailed, return.
		require.NoError(t, transport.Dispatch(t.Context(), &Update{
			Topics: []string{"https://example.com/books/1"},
			Event:  Event{Data: "test"},
		}))

		synctest.Wait()

		assert.Equal(t, []DisconnectReason{DisconnectReasonWriteFailed}, metrics.reasons())
		assert.Equal(t, 0, metrics.legacyDisconnects(),
			"reporter-path must early-return; legacy SubscriberDisconnected must NOT also fire (double-count guard)")
	})
}

// TestWriteObserverFiresOnHealthyDispatch locks the WriteFlushObserver
// hot-path wiring: a single Dispatch into the subscriber's channel must
// produce at least one ObserveWriteFlush call with ok=true. The header
// preamble (sendHeaders) writes to a different ResponseWriter helper; only
// the per-event h.write delivery path is timed by the observer.
// A refactor that drops the type-assertion or moves the defer outside
// h.write would silently disable the metric — the histogram would still
// register but never observe.
func TestWriteObserverFiresOnHealthyDispatch(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		metrics := &recordingMetrics{}
		hub := hubShutdownTestHubWithOptions(t.Context(), t, 5*time.Minute, WithMetrics(metrics))
		transport, _ := hub.transport.(*LocalTransport)

		ctx, cancel := context.WithCancel(t.Context())
		t.Cleanup(cancel)

		go func() {
			req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?topic=https://example.com/books/1", nil).WithContext(ctx)
			hub.SubscribeHandler(newSubscribeRecorder(), req)
		}()

		waitForSubscriber(t, transport)

		require.NoError(t, transport.Dispatch(t.Context(), &Update{
			Topics: []string{"https://example.com/books/1"},
			Event:  Event{Data: "ok-path"},
		}))

		synctest.Wait()

		samples := metrics.writeFlushSamples()
		require.NotEmpty(t, samples, "h.write must drive WriteFlushObserver on healthy dispatch")
		assert.True(t, samples[0].ok,
			"healthy in-process write must record outcome=ok; got ok=%v at %fs", samples[0].ok, samples[0].seconds)
		assert.GreaterOrEqual(t, samples[0].seconds, 0.0,
			"latency must be non-negative")
	})
}

// TestWriteObserverFiresOnFailedDispatch is the failure-path mirror: a
// dispatch against a writer that returns an error must still produce an
// observation, with ok=false. The "still observe failures" contract is
// load-bearing — operators rely on this histogram to differentiate fast
// errors (broken pipe in 200µs) from slow ones (deadline exceeded near
// dispatchTimeout). A refactor that skips the observation on the failure
// path would silently strip that signal and bias the latency distribution
// toward the healthy tail.
//
// Structurally parallel to TestWriteObserverFiresOnFlushFailure (different
// failure-mode writer); kept as separate funcs so each failure class has
// its own named test to grep for in regressions.
//
//nolint:dupl // see comment above; intentional parallel structure.
func TestWriteObserverFiresOnFailedDispatch(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		metrics := &recordingMetrics{}
		hub := hubShutdownTestHubWithOptions(t.Context(), t, 5*time.Minute, WithMetrics(metrics))
		transport, _ := hub.transport.(*LocalTransport)

		go func() {
			req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?topic=https://example.com/books/1", nil).WithContext(t.Context())
			hub.SubscribeHandler(&failingWriteRecorder{}, req)
		}()

		waitForSubscriber(t, transport)

		require.NoError(t, transport.Dispatch(t.Context(), &Update{
			Topics: []string{"https://example.com/books/1"},
			Event:  Event{Data: "failed-path"},
		}))

		synctest.Wait()

		samples := metrics.writeFlushSamples()
		require.NotEmpty(t, samples, "failed-write path must STILL drive WriteFlushObserver")

		var sawFailed bool

		for _, s := range samples {
			if !s.ok {
				sawFailed = true

				break
			}
		}

		assert.True(t, sawFailed,
			"failed-write path must record at least one outcome=failed sample; got %+v", samples)
	})
}

// TestWriteObserverFiresOnFlushFailure exercises the path where rc.rw.Write
// succeeds but the subsequent rc.flush returns false. ok flips false on the
// flush-failure assignment after the write succeeds — a refactor that
// short-circuits the && evaluation before flush, or reorders write/flush,
// would silently let the histogram record outcome=ok for events that did
// not reach the wire. Uses flushFailingWriter (Write returns nil error,
// FlushError returns io.ErrShortWrite) so only the flush arm errors.
//
// Structurally parallel to TestWriteObserverFiresOnFailedDispatch (different
// failure-mode writer); kept as separate funcs so each failure class has
// its own named test to grep for in regressions.
//
//nolint:dupl // see comment above; intentional parallel structure.
func TestWriteObserverFiresOnFlushFailure(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		metrics := &recordingMetrics{}
		hub := hubShutdownTestHubWithOptions(t.Context(), t, 5*time.Minute, WithMetrics(metrics))
		transport, _ := hub.transport.(*LocalTransport)

		go func() {
			req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?topic=https://example.com/books/1", nil).WithContext(t.Context())
			hub.SubscribeHandler(&flushFailingWriter{}, req)
		}()

		waitForSubscriber(t, transport)

		require.NoError(t, transport.Dispatch(t.Context(), &Update{
			Topics: []string{"https://example.com/books/1"},
			Event:  Event{Data: "flush-failure"},
		}))

		synctest.Wait()

		samples := metrics.writeFlushSamples()
		require.NotEmpty(t, samples, "flush-failure path must STILL drive WriteFlushObserver")

		var sawFailed bool

		for _, s := range samples {
			if !s.ok {
				sawFailed = true

				break
			}
		}

		assert.True(t, sawFailed,
			"flush-failure path must record at least one outcome=failed sample; got %+v", samples)
	})
}

// TestWriteObserverFiresOnDeadlineFailure exercises the earliest-exit path
// in (h *Hub).write: rc.setDispatchWriteDeadline returns false because the
// underlying SetWriteDeadline call errors. ok stays at its initial false
// across the deferred observation. Without this test, a refactor that
// moves the defer below the deadline-check, renames the local, or returns
// before the defer registers would silently lose the observation slice
// most useful for diagnosing kernel/conn-pool issues — the very
// failure-class operators rely on the histogram to surface.
//
// Requires WithDispatchTimeout > 0 to take the SetWriteDeadline branch
// (the default 0 short-circuits true without calling the writer).
func TestWriteObserverFiresOnDeadlineFailure(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		metrics := &recordingMetrics{}
		hub := hubShutdownTestHubWithOptions(
			t.Context(), t, 5*time.Minute,
			WithMetrics(metrics),
			WithDispatchTimeout(100*time.Millisecond),
		)
		transport, _ := hub.transport.(*LocalTransport)

		go func() {
			req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?topic=https://example.com/books/1", nil).WithContext(t.Context())
			hub.SubscribeHandler(&deadlineFailingWriter{}, req)
		}()

		waitForSubscriber(t, transport)

		require.NoError(t, transport.Dispatch(t.Context(), &Update{
			Topics: []string{"https://example.com/books/1"},
			Event:  Event{Data: "deadline-failure"},
		}))

		synctest.Wait()

		samples := metrics.writeFlushSamples()
		require.NotEmpty(t, samples, "deadline-failure path must STILL drive WriteFlushObserver — the most load-bearing failure mode the histogram surfaces")

		var sawFailed bool

		for _, s := range samples {
			if !s.ok {
				sawFailed = true

				break
			}
		}

		assert.True(t, sawFailed,
			"deadline-failure path must record at least one outcome=failed sample; got %+v", samples)
	})
}

// noWriteFlushObserverMetrics implements Metrics but deliberately does NOT
// implement WriteFlushObserver. SubscribeHandler's per-call type-assertion
// must short-circuit cleanly: no panic, no observation. Without this guard
// a regression that inverts the type-assertion sense (`if observer == nil`)
// or removes the nil check would still pass every test that uses
// recordingMetrics (which always satisfies WriteFlushObserver).
type noWriteFlushObserverMetrics struct{}

func (noWriteFlushObserverMetrics) SubscriberConnected(_ *LocalSubscriber)    {}
func (noWriteFlushObserverMetrics) SubscriberDisconnected(_ *LocalSubscriber) {}
func (noWriteFlushObserverMetrics) UpdatePublished(_ *Update)                 {}

// TestWriteObserverNonImplementerNoOps locks the negative side of the
// type-assertion: a Metrics impl without WriteFlushObserver must still
// allow h.write to function normally — Dispatch + h.write must not panic
// or short-circuit early just because the observer isn't there. Asserting
// the recorder actually saw the dispatched event distinguishes "h.write
// ran cleanly without an observer" from "Dispatch silently dropped before
// reaching h.write" — the latter would also "not panic" but defeats the
// purpose of the test.
func TestWriteObserverNonImplementerNoOps(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		hub := hubShutdownTestHubWithOptions(t.Context(), t, 5*time.Minute, WithMetrics(noWriteFlushObserverMetrics{}))
		transport, _ := hub.transport.(*LocalTransport)

		recorder := newSubscribeRecorder()

		go func() {
			req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?topic=https://example.com/books/1", nil).WithContext(t.Context())
			hub.SubscribeHandler(recorder, req)
		}()

		waitForSubscriber(t, transport)

		require.NoError(t, transport.Dispatch(t.Context(), &Update{
			Topics: []string{"https://example.com/books/1"},
			Event:  Event{Data: "no-observer"},
		}))

		synctest.Wait()

		assert.Contains(t, recorder.Body.String(), "no-observer",
			"h.write must execute and write event data even when h.metrics lacks WriteFlushObserver")
	})
}

// TestSubscribeHandlerReportsWriteFailedReasonOnHeartbeat covers the
// heartbeat-write failure path — a separate switch arm from the
// s.Receive case (see TestSubscribeHandlerReportsWriteFailedReason)
// but the same h.write call. A future refactor that classifies
// heartbeat-write differently (e.g. a dedicated reason) would silently
// regress without this guard. time.Sleep advances the bubble clock past
// the heartbeat firing; relying on synctest.Wait to auto-advance proved
// flaky for this specific event-shape.
func TestSubscribeHandlerReportsWriteFailedReasonOnHeartbeat(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		metrics := &recordingMetrics{}
		hub := hubShutdownTestHubWithOptions(
			t.Context(), t, 5*time.Minute,
			WithMetrics(metrics),
			WithHeartbeat(50*time.Millisecond),
		)

		go func() {
			req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?topic=https://example.com/books/1", nil).WithContext(t.Context())
			hub.SubscribeHandler(&failingWriteRecorder{}, req)
		}()

		// Advance the bubble clock past the heartbeat firing so the heartbeat
		// case arm runs h.write against the failingWriteRecorder; with the
		// h.write fix in place, the for-loop must classify the exit as
		// write_failed.
		time.Sleep(100 * time.Millisecond)
		synctest.Wait()

		assert.Equal(t, []DisconnectReason{DisconnectReasonWriteFailed}, metrics.reasons())
		assert.Equal(t, 0, metrics.legacyDisconnects(),
			"reporter-path must early-return; legacy SubscriberDisconnected must NOT also fire (double-count guard)")

		// Heartbeat-write also drives WriteFlushObserver — a refactor that
		// classifies heartbeat-write differently (e.g. dedicated reason or
		// dedicated writer) must not silently lose the observation slice.
		samples := metrics.writeFlushSamples()
		require.NotEmpty(t, samples, "heartbeat-write path must drive WriteFlushObserver")

		var sawFailed bool

		for _, s := range samples {
			if !s.ok {
				sawFailed = true

				break
			}
		}

		assert.True(t, sawFailed,
			"heartbeat-write failure must record at least one outcome=failed sample; got %+v", samples)
	})
}

// TestSubscribeHandlerReportsTransportEndedReason covers the channel-close
// exit path: transport.Close() invokes Disconnect on every subscriber, which
// closes its OutChan; SubscribeHandler observes !ok on the channel-receive
// case arm and labels the disconnect transport_ended (not hub_shutdown:
// the hub context stays alive; not write_timeout: writeTimeout is set
// non-zero so the disconnect timer is parked far in the future).
func TestSubscribeHandlerReportsTransportEndedReason(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		metrics := &recordingMetrics{}
		hub := hubShutdownTestHubWithOptions(t.Context(), t, 5*time.Minute, WithMetrics(metrics))
		transport, _ := hub.transport.(*LocalTransport)

		go func() {
			req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?topic=https://example.com/books/1", nil).WithContext(t.Context())
			hub.SubscribeHandler(newSubscribeRecorder(), req)
		}()

		waitForSubscriber(t, transport)

		require.NoError(t, transport.Close(t.Context()))
		synctest.Wait()

		assert.Equal(t, []DisconnectReason{DisconnectReasonTransportEnded}, metrics.reasons())
		assert.Equal(t, 0, metrics.legacyDisconnects(),
			"reporter-path must early-return; legacy SubscriberDisconnected must NOT also fire (double-count guard)")
	})
}

// TestSubscribeHandlerLegacyMetricsFallback verifies that a Metrics impl that
// does NOT implement DisconnectReasonReporter still gets SubscriberDisconnected
// called via the legacy path — so existing custom Metrics impls keep working
// post-taxonomy. legacyOnlyMetrics counts the legacy calls so a regression
// that bypasses the legacy path (e.g. an early-return after the type
// assertion fails) fails the assertion below — testing with NopMetrics
// alone is insufficient because its SubscriberDisconnected is a no-op.
func TestSubscribeHandlerLegacyMetricsFallback(t *testing.T) {
	t.Parallel()

	// Runtime invariant: legacyOnlyMetrics must NOT satisfy
	// DisconnectReasonReporter. If a future field/method addition makes it
	// satisfy the interface, this test's premise is invalid.
	if _, ok := any(&legacyOnlyMetrics{}).(DisconnectReasonReporter); ok {
		t.Fatal("test setup invariant: legacyOnlyMetrics must not implement DisconnectReasonReporter")
	}

	synctest.Test(t, func(t *testing.T) {
		metrics := &legacyOnlyMetrics{}
		hubCtx, cancelHub := context.WithCancel(t.Context())
		hub := hubShutdownTestHubWithOptions(hubCtx, t, 0, WithMetrics(metrics))
		transport, _ := hub.transport.(*LocalTransport)

		go func() {
			req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?topic=https://example.com/books/1", nil).WithContext(t.Context())
			hub.SubscribeHandler(newSubscribeRecorder(), req)
		}()

		waitForSubscriber(t, transport)

		cancelHub()
		synctest.Wait()

		transport.RLock()
		n := transport.subscribers.Len()
		transport.RUnlock()
		assert.Equal(t, 0, n, "non-Reporter Metrics impl must still drain subscribers cleanly via the legacy path")
		assert.Equal(t, 1, metrics.legacyDisconnects(),
			"legacy SubscriberDisconnected must be called exactly once when the Metrics impl is not a DisconnectReasonReporter")
	})
}

// runShutdownTransportErrorTest exercises (*Hub).shutdown when
// transport.RemoveSubscriber fails. Two callers feed it different inReason
// values: Unknown (override branch — must upgrade to transport_error) and
// WriteFailed (preserve branch — must keep the more-specific reason). All
// shared invariants (single SubscriberDisconnectedWithReason call, no
// legacy double-count, wrapped transport error reaches the operator log)
// live here so a future invariant add lands in one place.
//
// Setup caveat: callers pass a fresh LocalSubscriber that was never
// registered via AddSubscriber. The errorRemovingTransport stub's
// RemoveSubscriber is the only transport method exercised;
// dispatchSubscriptionUpdate's Dispatch falls into the default branch
// because subscriptions are disabled. Do NOT enable WithSubscriptions(true)
// — the embedded LocalTransport's subscribers set has no record of sub,
// so any code path that walks it would behave unexpectedly.
func runShutdownTransportErrorTest(t *testing.T, inReason, wantReason DisconnectReason) {
	t.Helper()

	metrics := &recordingMetrics{}
	logBuf := &syncBuffer{}
	logger := slog.New(slog.NewJSONHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelError}))
	hub := hubShutdownTestHubWithOptions(t.Context(), t, 0, WithMetrics(metrics), WithLogger(logger))
	hub.transport = &errorRemovingTransport{
		LocalTransport: hub.transport.(*LocalTransport),
		removeErr:      errTransportOffline,
	}

	tss, err := NewTopicSelectorStore(0)
	require.NoError(t, err)

	sub := NewLocalSubscriber("", slog.Default(), tss)

	hub.shutdown(t.Context(), sub, inReason)

	assert.Equal(t, []DisconnectReason{wantReason}, metrics.reasons())
	assert.Equal(t, 0, metrics.legacyDisconnects(),
		"reporter-path must early-return; legacy SubscriberDisconnected must NOT also fire (double-count guard)")
	assert.Contains(t, logBuf.String(), "transport offline",
		"the wrapped transport error must reach the operator log — operator visibility on the failed cleanup is independent of metric attribution")
}

// TestShutdownOverridesUnknownReasonOnTransportError covers the override
// branch in (*Hub).shutdown: when the SubscribeHandler loop didn't classify
// the exit (reason=unknown) AND transport.RemoveSubscriber returns an error,
// the reason is upgraded to transport_error so the metric reflects the
// actual failure mode.
func TestShutdownOverridesUnknownReasonOnTransportError(t *testing.T) {
	t.Parallel()
	runShutdownTransportErrorTest(t, DisconnectReasonUnknown, DisconnectReasonTransportError)
}

// TestShutdownPreservesSpecificReasonOnTransportError verifies the inverse
// of the override branch: when the loop already classified the exit (e.g.
// write_failed mid-write), a follow-on RemoveSubscriber failure does NOT
// downgrade the reason to the generic transport_error — the more specific
// proximate cause survives.
func TestShutdownPreservesSpecificReasonOnTransportError(t *testing.T) {
	t.Parallel()
	runShutdownTransportErrorTest(t, DisconnectReasonWriteFailed, DisconnectReasonWriteFailed)
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
			req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?topic=https://example.com/books/1", nil).WithContext(t.Context())
			hub.SubscribeHandler(newSubscribeRecorder(), req)
		}()

		waitForSubscriber(t, transport)

		// Simulate hub shutdown.
		cancelHub()
		synctest.Wait()

		transport.RLock()
		n := transport.subscribers.Len()
		transport.RUnlock()
		assert.Equal(t, 1, n, "subscriber must stay connected when writeTimeout is set; disconnect timer is the drain mechanism")
	})
}

// outBufferCapturingTransport records the out-channel capacity of the subscriber
// passed to AddSubscriber, so a test can assert the Hub option propagated.
type outBufferCapturingTransport struct {
	nopTransport

	capCh chan int
}

func (tr *outBufferCapturingTransport) AddSubscriber(_ context.Context, s *LocalSubscriber) error {
	tr.capCh <- cap(s.out)

	return nil
}

// TestSubscribeHandlerThreadsOutBuffer proves the Hub option value reaches the
// created subscriber's channel end-to-end (WithSubscriberOutBuffer →
// h.subscriberOutBuffer → withOutBuffer → NewLocalSubscriber), not just the
// lower-level withOutBuffer option in isolation.
func TestSubscribeHandlerThreadsOutBuffer(t *testing.T) {
	t.Parallel()

	tr := &outBufferCapturingTransport{capCh: make(chan int, 1)}
	hub := createAnonymousDummy(t, WithTransport(tr), WithSubscriberOutBuffer(32))

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?topic=https://example.com/foo", nil).WithContext(ctx)
	w := newSubscribeRecorder()

	go hub.SubscribeHandler(w, req)

	select {
	case got := <-tr.capCh:
		assert.Equal(t, 32, got, "WithSubscriberOutBuffer(32) must size the created subscriber's out channel")
	case <-time.After(2 * time.Second):
		t.Fatal("AddSubscriber was not reached")
	}

	cancel() // unblock the handler's stream loop so the goroutine exits
}

// TestWithSubscriberOutBufferRejectsSubMinimum: a positive value below the floor
// is rejected at construction (not silently coerced to the default); 0 means
// "use the default" and at/above the floor is accepted verbatim.
func TestWithSubscriberOutBufferRejectsSubMinimum(t *testing.T) {
	t.Parallel()

	_, err := NewHub(t.Context(), WithSubscriberOutBuffer(MinSubscriberOutBuffer-1))
	require.Error(t, err, "a positive sub-minimum out buffer must be rejected, not silently coerced")

	for _, size := range []int{0, MinSubscriberOutBuffer, 4096} {
		h, err := NewHub(t.Context(), WithSubscriberOutBuffer(size))
		require.NoError(t, err)
		assert.Equal(t, size, h.subscriberOutBuffer)
	}
}
