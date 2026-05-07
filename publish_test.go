package mercure

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/gofrs/uuid/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPublish(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		hub := createDummy(t)

		topics := []string{"https://example.com/books/1"}
		s := NewLocalSubscriber("", slog.Default(), &TopicSelectorStore{})
		s.SetTopics(topics, topics)
		s.Claims = &claims{Mercure: mercureClaim{Subscribe: topics}}

		require.NoError(t, hub.transport.AddSubscriber(t.Context(), s))

		go func() {
			u, ok := <-s.Receive()

			assert.True(t, ok)
			assert.NotNil(t, u)
			assert.Equal(t, "id", u.ID)
			assert.Equal(t, s.SubscribedTopics, u.Topics)
			assert.Equal(t, "Hello!", u.Data)
			assert.True(t, u.Private)
		}()

		require.NoError(t, hub.Publish(t.Context(), &Update{
			Event: Event{
				ID:   "id",
				Data: "Hello!",
			},
			Topics:  s.SubscribedTopics,
			Private: true,
		}))

		synctest.Wait()
	})
}

func TestPublishHandlerNoAuthorizationHeader(t *testing.T) {
	t.Parallel()

	hub := createDummy(t)

	req := httptest.NewRequest(http.MethodPost, defaultHubURL, nil)
	w := httptest.NewRecorder()
	hub.PublishHandler(w, req)

	resp := w.Result()

	t.Cleanup(func() {
		_ = resp.Body.Close()
	})

	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	assert.Equal(t, http.StatusText(http.StatusUnauthorized)+"\n", w.Body.String())
}

func TestPublishHandlerUnauthorizedJWT(t *testing.T) {
	t.Parallel()

	hub := createDummy(t)

	req := httptest.NewRequest(http.MethodPost, defaultHubURL, nil)
	req.Header.Add("Authorization", bearerPrefix+createDummyUnauthorizedJWT())

	w := httptest.NewRecorder()
	hub.PublishHandler(w, req)

	resp := w.Result()

	t.Cleanup(func() {
		assert.NoError(t, resp.Body.Close())
	})

	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	assert.Equal(t, http.StatusText(http.StatusUnauthorized)+"\n", w.Body.String())
}

func TestPublishHandlerInvalidAlgJWT(t *testing.T) {
	t.Parallel()

	hub := createDummy(t)

	req := httptest.NewRequest(http.MethodPost, defaultHubURL, nil)
	req.Header.Add("Authorization", bearerPrefix+createDummyNoneSignedJWT())

	w := httptest.NewRecorder()
	hub.PublishHandler(w, req)

	resp := w.Result()

	t.Cleanup(func() {
		assert.NoError(t, resp.Body.Close())
	})

	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	assert.Equal(t, http.StatusText(http.StatusUnauthorized)+"\n", w.Body.String())
}

func TestPublishHandlerBadContentType(t *testing.T) {
	t.Parallel()

	hub := createDummy(t)

	req := httptest.NewRequest(http.MethodPost, defaultHubURL, nil)
	req.Header.Add("Authorization", bearerPrefix+createDummyAuthorizedJWT(rolePublisher, []string{"*"}))
	req.Header.Add("Content-Type", "text/plain; boundary=")

	w := httptest.NewRecorder()
	hub.PublishHandler(w, req)

	resp := w.Result()

	t.Cleanup(func() {
		assert.NoError(t, resp.Body.Close())
	})

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestPublishHandlerNoTopic(t *testing.T) {
	t.Parallel()

	hub := createDummy(t)

	req := httptest.NewRequest(http.MethodPost, defaultHubURL, nil)
	req.Header.Add("Authorization", bearerPrefix+createDummyAuthorizedJWT(rolePublisher, []string{"*"}))

	w := httptest.NewRecorder()
	hub.PublishHandler(w, req)

	resp := w.Result()

	t.Cleanup(func() {
		assert.NoError(t, resp.Body.Close())
	})

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Equal(t, `Missing "topic" parameter
`, w.Body.String())
}

func TestPublishHandlerInvalidRetry(t *testing.T) {
	t.Parallel()

	hub := createDummy(t)

	form := url.Values{}
	form.Add("topic", "https://example.com/books/1")
	form.Add("data", "foo")
	form.Add("retry", "invalid")

	req := httptest.NewRequest(http.MethodPost, defaultHubURL, strings.NewReader(form.Encode()))
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Add("Authorization", bearerPrefix+createDummyAuthorizedJWT(rolePublisher, []string{"*"}))

	w := httptest.NewRecorder()
	hub.PublishHandler(w, req)

	resp := w.Result()

	t.Cleanup(func() {
		assert.NoError(t, resp.Body.Close())
	})

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Equal(t, `Invalid "retry" parameter
`, w.Body.String())
}

func TestPublishHandlerNotAuthorizedTopicSelector(t *testing.T) {
	t.Parallel()

	hub := createDummy(t)

	form := url.Values{}
	form.Add("topic", "https://example.com/books/1")
	form.Add("data", "foo")
	form.Add("private", "on")

	req := httptest.NewRequest(http.MethodPost, defaultHubURL, strings.NewReader(form.Encode()))
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Add("Authorization", bearerPrefix+createDummyAuthorizedJWT(rolePublisher, []string{"foo"}))

	w := httptest.NewRecorder()
	hub.PublishHandler(w, req)

	resp := w.Result()

	t.Cleanup(func() {
		assert.NoError(t, resp.Body.Close())
	})

	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

func TestPublishHandlerEmptyTopicSelector(t *testing.T) {
	t.Parallel()

	hub := createDummy(t)

	form := url.Values{}
	form.Add("topic", "https://example.com/books/1")

	req := httptest.NewRequest(http.MethodPost, defaultHubURL, strings.NewReader(form.Encode()))
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Add("Authorization", bearerPrefix+createDummyAuthorizedJWT(rolePublisher, []string{}))

	w := httptest.NewRecorder()
	hub.PublishHandler(w, req)

	resp := w.Result()

	t.Cleanup(func() {
		assert.NoError(t, resp.Body.Close())
	})

	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

func TestPublishHandlerLegacyAuthorization(t *testing.T) {
	t.Parallel()

	hub := createDummy(t, WithProtocolVersionCompatibility(7))

	form := url.Values{}
	form.Add("topic", "https://example.com/books/1")

	req := httptest.NewRequest(http.MethodPost, defaultHubURL, strings.NewReader(form.Encode()))
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Add("Authorization", bearerPrefix+createDummyAuthorizedJWT(rolePublisher, []string{}))

	w := httptest.NewRecorder()
	hub.PublishHandler(w, req)

	resp := w.Result()

	t.Cleanup(func() {
		assert.NoError(t, resp.Body.Close())
	})

	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestPublishHandlerOK(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		hub := createDummy(t)

		topics := []string{"https://example.com/books/1"}
		s := NewLocalSubscriber("", slog.Default(), &TopicSelectorStore{})
		s.SetTopics(topics, topics)
		s.Claims = &claims{Mercure: mercureClaim{Subscribe: topics}}

		require.NoError(t, hub.transport.AddSubscriber(t.Context(), s))

		go func() {
			u, ok := <-s.Receive()
			assert.True(t, ok)
			assert.NotNil(t, u)
			assert.Equal(t, "id", u.ID)
			assert.Equal(t, s.SubscribedTopics, u.Topics)
			assert.Equal(t, "Hello!", u.Data)
			assert.True(t, u.Private)
		}()

		form := url.Values{}
		form.Add("id", "id")
		form.Add("topic", "https://example.com/books/1")
		form.Add("data", "Hello!")
		form.Add("private", "on")

		req := httptest.NewRequest(http.MethodPost, defaultHubURL, strings.NewReader(form.Encode()))
		req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Add("Authorization", bearerPrefix+createDummyAuthorizedJWT(rolePublisher, s.SubscribedTopics))

		w := httptest.NewRecorder()
		hub.PublishHandler(w, req)

		resp := w.Result()

		t.Cleanup(func() {
			assert.NoError(t, resp.Body.Close())
		})

		body, _ := io.ReadAll(resp.Body)

		assert.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Equal(t, "id", string(body))

		synctest.Wait()
	})
}

// blockingTransport stalls Dispatch until the context is done, then returns its
// error — modelling an unresponsive transport so a configured publish_timeout
// can be exercised deterministically.
type blockingTransport struct {
	nopTransport
}

func (blockingTransport) Dispatch(ctx context.Context, _ *Update) error {
	<-ctx.Done()

	return ctx.Err()
}

// TestPublishHandlerPublishTimeout: when a dispatch outlives the configured
// publish_timeout, PublishHandler returns 504 (the write may have committed, so
// the publisher must treat it as indeterminate).
func TestPublishHandlerPublishTimeout(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		hub := createDummy(t, WithTransport(blockingTransport{}), WithPublishTimeout(5*time.Second))

		topics := []string{"https://example.com/books/1"}

		form := url.Values{}
		form.Add("topic", topics[0])
		form.Add("data", "Hello!")

		req := httptest.NewRequest(http.MethodPost, defaultHubURL, strings.NewReader(form.Encode()))
		req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Add("Authorization", bearerPrefix+createDummyAuthorizedJWT(rolePublisher, topics))

		w := httptest.NewRecorder()
		hub.PublishHandler(w, req)

		resp := w.Result()

		t.Cleanup(func() { assert.NoError(t, resp.Body.Close()) })

		assert.Equal(t, http.StatusGatewayTimeout, resp.StatusCode, "a dispatch exceeding publish_timeout must return 504")
	})
}

// recordingFailureMetrics is a Metrics impl that also implements the optional
// PublishFailureReporter, capturing the reasons Hub.Publish reports so the
// wiring (right reason at each failure site) can be asserted.
type recordingFailureMetrics struct {
	mu      sync.Mutex
	reasons []PublishFailureReason
}

func (*recordingFailureMetrics) SubscriberConnected(*LocalSubscriber)    {}
func (*recordingFailureMetrics) SubscriberDisconnected(*LocalSubscriber) {}
func (*recordingFailureMetrics) UpdatePublished(*Update)                 {}

// UpdatePublishFailed is concurrency-safe: a real Metrics impl is called from
// many publish goroutines, so the test double honors the same contract (mirrors
// the recordingMetrics fixture).
func (m *recordingFailureMetrics) UpdatePublishFailed(_ *Update, reason PublishFailureReason) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.reasons = append(m.reasons, reason)
}

func (m *recordingFailureMetrics) last() (PublishFailureReason, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.reasons) == 0 {
		return "", false
	}

	return m.reasons[len(m.reasons)-1], true
}

func (m *recordingFailureMetrics) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return len(m.reasons)
}

// errTestDispatch is a static (err113-friendly) stand-in for a transport
// Dispatch failure in the classification test.
var errTestDispatch = errors.New("test dispatch failure")

// TestPublishFailureReasonForDispatch covers the timeout-vs-transport
// classification directly, including the TOCTOU guard that a coincidental
// DeadlineExceeded without the ErrPublishTimeout cause is NOT a publish_timeout.
func TestPublishFailureReasonForDispatch(t *testing.T) {
	t.Parallel()

	// Expired publish_timeout context: cause is ErrPublishTimeout, error is a
	// deadline error → timeout (either the deadline error or the sentinel).
	timedOut, cancel := context.WithTimeoutCause(context.Background(), 0, ErrPublishTimeout)
	defer cancel()

	<-timedOut.Done()

	assert.Equal(t, PublishFailureReasonTimeout, publishFailureReasonForDispatch(timedOut, timedOut.Err()))
	assert.Equal(t, PublishFailureReasonTimeout, publishFailureReasonForDispatch(timedOut, ErrPublishTimeout))

	// Cause IS ErrPublishTimeout but the error is NOT a deadline error (a hard
	// transport error raced the deadline) → transport, not timeout. The
	// symmetric half of the guard: the deadline-error clause must also hold.
	assert.Equal(t, PublishFailureReasonTransport,
		publishFailureReasonForDispatch(timedOut, errTestDispatch))

	// Plain transport error (no publish_timeout cause) → transport.
	assert.Equal(t, PublishFailureReasonTransport,
		publishFailureReasonForDispatch(context.Background(), errTestDispatch))

	// TOCTOU guard: a DeadlineExceeded WITHOUT the ErrPublishTimeout cause must
	// stay transport, not be reclassified as a publish_timeout.
	plainDeadline, cancel2 := context.WithTimeout(context.Background(), 0)
	defer cancel2()

	<-plainDeadline.Done()

	assert.Equal(t, PublishFailureReasonTransport,
		publishFailureReasonForDispatch(plainDeadline, plainDeadline.Err()))
}

// TestPublishRecordsFailureReason verifies Hub.Publish reports the correct
// reason to the optional PublishFailureReporter at the validation and transport
// failure sites.
func TestPublishRecordsFailureReason(t *testing.T) {
	t.Parallel()

	t.Run("validation", func(t *testing.T) {
		t.Parallel()

		rec := &recordingFailureMetrics{}
		hub := createDummy(t, WithMetrics(rec))

		// A topic with an SSE-forbidden char is rejected by update.Validate().
		err := hub.Publish(t.Context(), &Update{Topics: []string{"https://example.com/\n"}})
		require.Error(t, err)

		reason, ok := rec.last()
		require.True(t, ok, "a failed publish must report a reason")
		assert.Equal(t, PublishFailureReasonValidation, reason)
		assert.Equal(t, 1, rec.count(), "exactly one failure record per failed publish (no double-count)")
	})

	t.Run("transport", func(t *testing.T) {
		t.Parallel()

		rec := &recordingFailureMetrics{}
		hub := createDummy(t, WithMetrics(rec))
		// A closed transport makes Dispatch return an error (non-timeout).
		require.NoError(t, hub.transport.Close(t.Context()))

		err := hub.Publish(t.Context(), &Update{Topics: []string{"https://example.com/books/1"}})
		require.Error(t, err)

		reason, ok := rec.last()
		require.True(t, ok, "a failed publish must report a reason")
		assert.Equal(t, PublishFailureReasonTransport, reason)
		assert.Equal(t, 1, rec.count(), "exactly one failure record per failed publish (no double-count)")
	})
}

// TestPublishRecordsTimeoutFailureReason proves the publish_timeout abort path
// (the 504) reports reason=timeout end-to-end through PublishHandler.
func TestPublishRecordsTimeoutFailureReason(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		rec := &recordingFailureMetrics{}
		hub := createDummy(t, WithTransport(blockingTransport{}), WithPublishTimeout(5*time.Second), WithMetrics(rec))

		topics := []string{"https://example.com/books/1"}

		form := url.Values{}
		form.Add("topic", topics[0])
		form.Add("data", "Hello!")

		req := httptest.NewRequest(http.MethodPost, defaultHubURL, strings.NewReader(form.Encode()))
		req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Add("Authorization", bearerPrefix+createDummyAuthorizedJWT(rolePublisher, topics))

		w := httptest.NewRecorder()
		hub.PublishHandler(w, req)

		t.Cleanup(func() { assert.NoError(t, w.Result().Body.Close()) })

		assert.Equal(t, http.StatusGatewayTimeout, w.Result().StatusCode)

		reason, ok := rec.last()
		require.True(t, ok, "a publish_timeout abort must report a reason")
		assert.Equal(t, PublishFailureReasonTimeout, reason)
	})
}

// TestPublishHandlerRecordsValidationFailureReason covers the HTTP early-reject
// path: PublishHandler runs validateTopics BEFORE Hub.Publish, so a forbidden
// topic must still be metered as reason=validation.
func TestPublishHandlerRecordsValidationFailureReason(t *testing.T) {
	t.Parallel()

	rec := &recordingFailureMetrics{}
	hub := createDummy(t, WithMetrics(rec))

	form := url.Values{}
	// A control char in the topic fails validateTopics in PublishHandler,
	// before Hub.Publish is reached.
	form.Add("topic", "https://example.com/\n")
	form.Add("data", "Hello!")

	req := httptest.NewRequest(http.MethodPost, defaultHubURL, strings.NewReader(form.Encode()))
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Add("Authorization", bearerPrefix+createDummyAuthorizedJWT(rolePublisher, []string{"*"}))

	w := httptest.NewRecorder()
	hub.PublishHandler(w, req)

	t.Cleanup(func() { assert.NoError(t, w.Result().Body.Close()) })

	// Fail fast on the status (require, not assert): if a forbidden topic is NOT
	// rejected at the handler's early validateTopics, that's the real failure —
	// don't let it cascade into a misleading "must report a reason" below.
	require.Equal(t, http.StatusBadRequest, w.Result().StatusCode, "a forbidden topic must be rejected before Hub.Publish")

	reason, ok := rec.last()
	require.True(t, ok, "an HTTP topic-validation rejection must report a reason")
	assert.Equal(t, PublishFailureReasonValidation, reason)
	assert.Equal(t, 1, rec.count(), "early-reject must meter exactly once (handler returns before Hub.Publish)")
}

// TestPublishNoOpsWithoutPublishFailureReporter locks the opt-in contract: a
// Metrics impl that does NOT implement PublishFailureReporter must not panic
// when a publish fails (the metering is purely additive).
func TestPublishNoOpsWithoutPublishFailureReporter(t *testing.T) {
	t.Parallel()

	if _, ok := any(NopMetrics{}).(PublishFailureReporter); ok {
		t.Fatal("NopMetrics must NOT implement PublishFailureReporter (the opt-in contract)")
	}

	hub := createDummy(t, WithMetrics(NopMetrics{}))
	require.NoError(t, hub.transport.Close(t.Context()))

	assert.Error(t, hub.Publish(t.Context(), &Update{Topics: []string{"https://example.com/books/1"}}))
}

func TestPublishHandlerNoData(t *testing.T) {
	t.Parallel()

	hub := createDummy(t)

	form := url.Values{}
	form.Add("topic", "https://example.com/books/1")

	req := httptest.NewRequest(http.MethodPost, defaultHubURL, strings.NewReader(form.Encode()))
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Add("Authorization", bearerPrefix+createDummyAuthorizedJWT(rolePublisher, []string{"*"}))

	w := httptest.NewRecorder()
	hub.PublishHandler(w, req)

	resp := w.Result()

	t.Cleanup(func() {
		assert.NoError(t, resp.Body.Close())
	})

	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestPublishHandlerGenerateUUID(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		h := createDummy(t)

		s := NewLocalSubscriber("", slog.Default(), &TopicSelectorStore{})
		s.SetTopics([]string{"https://example.com/books/1"}, s.SubscribedTopics)

		require.NoError(t, h.transport.AddSubscriber(t.Context(), s))

		go func() {
			u := <-s.Receive()
			assert.NotNil(t, u)

			_, err := uuid.FromString(strings.TrimPrefix(u.ID, "urn:uuid:"))
			assert.NoError(t, err)
		}()

		form := url.Values{}
		form.Add("topic", "https://example.com/books/1")
		form.Add("data", "Hello!")

		req := httptest.NewRequest(http.MethodPost, defaultHubURL, strings.NewReader(form.Encode()))
		req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Add("Authorization", bearerPrefix+createDummyAuthorizedJWT(rolePublisher, []string{"*"}))

		w := httptest.NewRecorder()
		h.PublishHandler(w, req)

		resp := w.Result()

		t.Cleanup(func() {
			assert.NoError(t, resp.Body.Close())
		})

		assert.Equal(t, http.StatusOK, resp.StatusCode)

		bodyBytes, _ := io.ReadAll(resp.Body)
		body := string(bodyBytes)

		_, err := uuid.FromString(strings.TrimPrefix(body, "urn:uuid:"))
		require.NoError(t, err)

		synctest.Wait()
	})
}

func TestPublishHandlerWithErrorInTransport(t *testing.T) {
	t.Parallel()

	hub := createDummy(t)
	require.NoError(t, hub.transport.Close(t.Context()))

	form := url.Values{}
	form.Add("id", "id")
	form.Add("topic", "https://example.com/books/1")
	form.Add("data", "Hello!")
	form.Add("private", "on")

	req := httptest.NewRequest(http.MethodPost, defaultHubURL, strings.NewReader(form.Encode()))
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Add("Authorization", bearerPrefix+createDummyAuthorizedJWT(rolePublisher, []string{"foo", "https://example.com/books/1"}))

	w := httptest.NewRecorder()
	hub.PublishHandler(w, req)

	resp := w.Result()

	t.Cleanup(func() {
		assert.NoError(t, resp.Body.Close())
	})

	body, _ := io.ReadAll(resp.Body)

	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	assert.Equal(t, "Internal Server Error\n", string(body))
}

func FuzzPublish(f *testing.F) {
	hub := createDummy(f)
	authorizationHeader := bearerPrefix + createDummyAuthorizedJWT(rolePublisher, []string{"*"})

	testCases := []struct {
		topic1, topic2, id, data, private, retry, typ string
	}{
		{"https://localhost/foo/bar", "baz", "", "", "", "", ""},
		{"https://localhost/foo/baz", "bat", "id", "data", "on", "22", "mytype"},
	}

	for _, tc := range testCases {
		f.Add(tc.topic1, tc.topic2, tc.id, tc.data, tc.private, tc.retry, tc.typ)
	}

	f.Fuzz(func(t *testing.T, topic1, topic2, id, data, private, retry, typ string) {
		form := url.Values{}
		form.Add("topic", topic1)
		form.Add("topic", topic2)
		form.Add("id", id)
		form.Add("data", data)
		form.Add("private", private)
		form.Add("retry", retry)
		form.Add("type", typ)

		req := httptest.NewRequest(http.MethodPost, defaultHubURL, strings.NewReader(form.Encode()))
		req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Add("Authorization", authorizationHeader)

		w := httptest.NewRecorder()
		hub.PublishHandler(w, req)

		resp := w.Result()

		t.Cleanup(func() {
			assert.NoError(t, resp.Body.Close())
		})

		body, _ := io.ReadAll(resp.Body)

		// 400 (invalid id/type/topic or too many topics) and 403 (reserved
		// topic namespace) are valid security rejections, not fuzz failures.
		if resp.StatusCode == http.StatusBadRequest || resp.StatusCode == http.StatusForbidden {
			return
		}

		assert.Equal(t, http.StatusOK, resp.StatusCode)

		if id == "" {
			assert.NotEmpty(t, string(body))

			return
		}

		assert.Equal(t, id, string(body))
	})
}

func TestUpdateValidate(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		update Update
		want   error
	}{
		{"valid", Update{Event: Event{ID: "id", Type: "type"}, Topics: []string{"https://example.com/books/1"}}, nil},
		{"empty", Update{}, nil},
		{"reserved topic", Update{Topics: []string{"https://example.com/.well-known/mercure/subscriptions/foo"}}, ErrReservedTopic},
		{"id LF", Update{Event: Event{ID: "foo\nevent: injected"}}, ErrInvalidEventID},
		{"id CR", Update{Event: Event{ID: "foo\rinjected"}}, ErrInvalidEventID},
		{"id NUL", Update{Event: Event{ID: "foo\x00bar"}}, ErrInvalidEventID},
		{"type LF", Update{Event: Event{Type: "foo\nid: injected"}}, ErrInvalidEventType},
		{"type CR", Update{Event: Event{Type: "foo\rinjected"}}, ErrInvalidEventType},
		{"type NUL", Update{Event: Event{Type: "foo\x00bar"}}, ErrInvalidEventType},
		// Fork superset checks (upstream Validate lacks these) — asserted directly
		// so this stays the self-contained Validate contract test, not reliant on
		// TestHubPublishValidatesProgrammaticPath for the length/topic-char branches.
		{"topic too long", Update{Topics: []string{strings.Repeat("a", maxUpdateTopicBytes+1)}}, ErrTopicTooLong},
		{"topic forbidden char", Update{Topics: []string{"https://example.com/foo\nbar"}}, ErrInvalidTopic},
		{"id too long", Update{Event: Event{ID: strings.Repeat("a", maxUpdateIDBytes+1)}}, ErrEventIDTooLong},
		{"type too long", Update{Event: Event{Type: strings.Repeat("a", maxUpdateTypeBytes+1)}}, ErrEventTypeTooLong},
		// Precedence: the length cap is checked before the SSE char check, so an id
		// that is both oversize and CR/LF-bearing reports the length error.
		{"id length beats char check", Update{Event: Event{ID: strings.Repeat("a", maxUpdateIDBytes+1) + "\n"}}, ErrEventIDTooLong},
	}

	for i := range cases {
		tc := &cases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := tc.update.Validate()
			if tc.want == nil {
				assert.NoError(t, err)

				return
			}

			assert.ErrorIs(t, err, tc.want)
		})
	}
}

func TestUpdateValidateTooManyTopics(t *testing.T) {
	t.Parallel()

	topics := make([]string, maxPublishTopics+1)
	for i := range topics {
		topics[i] = "https://example.com/books/1"
	}

	err := (&Update{Topics: topics}).Validate()
	assert.ErrorIs(t, err, ErrTooManyTopics)
}

func TestPublishHandlerReservedTopicNamespace(t *testing.T) {
	t.Parallel()

	for _, topic := range []string{
		"/.well-known/mercure/subscriptions/foo",
		"https://example.com/.well-known/mercure/subscriptions/foo",
		"foo/.well-known/mercure/bar",
	} {
		t.Run(topic, func(t *testing.T) {
			t.Parallel()

			hub := createDummy(t)

			form := url.Values{}
			form.Add("topic", topic)
			form.Add("data", "Hello!")

			req := httptest.NewRequest(http.MethodPost, defaultHubURL, strings.NewReader(form.Encode()))
			req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
			req.Header.Add("Authorization", bearerPrefix+createDummyAuthorizedJWT(rolePublisher, []string{"*"}))

			w := httptest.NewRecorder()
			hub.PublishHandler(w, req)

			resp := w.Result()

			t.Cleanup(func() {
				assert.NoError(t, resp.Body.Close())
			})

			assert.Equal(t, http.StatusForbidden, resp.StatusCode)
			assert.Equal(t, fmt.Sprintf("%q: %s\n", topic, ErrReservedTopic), w.Body.String(),
				"reserved-topic 403 body echoes the escaped, length-bounded topic")
		})
	}
}

func TestPublishHandlerRejectsSSEControlChars(t *testing.T) {
	t.Parallel()

	cases := []struct {
		field string
		value string
	}{
		{"id", "foo\nevent: injected"},
		{"id", "foo\revent: injected"},
		{"id", "foo\x00bar"},
		{"type", "foo\nid: injected"},
		{"type", "foo\rdata: injected"},
		{"type", "foo\x00bar"},
	}

	for _, tc := range cases {
		t.Run(fmt.Sprintf("%s/%q", tc.field, tc.value), func(t *testing.T) {
			t.Parallel()

			hub := createDummy(t)

			form := url.Values{}
			form.Add("topic", "https://example.com/books/1")
			form.Add(tc.field, tc.value)
			form.Add("data", "Hello!")

			req := httptest.NewRequest(http.MethodPost, defaultHubURL, strings.NewReader(form.Encode()))
			req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
			req.Header.Add("Authorization", bearerPrefix+createDummyAuthorizedJWT(rolePublisher, []string{"*"}))

			w := httptest.NewRecorder()
			hub.PublishHandler(w, req)

			resp := w.Result()

			t.Cleanup(func() {
				assert.NoError(t, resp.Body.Close())
			})

			assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
		})
	}
}

func TestPublishHandlerTooManyTopics(t *testing.T) {
	t.Parallel()

	hub := createDummy(t)

	form := url.Values{}
	for i := 0; i <= maxPublishTopics; i++ {
		form.Add("topic", "https://example.com/books/1")
	}

	form.Add("data", "Hello!")

	req := httptest.NewRequest(http.MethodPost, defaultHubURL, strings.NewReader(form.Encode()))
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Add("Authorization", bearerPrefix+createDummyAuthorizedJWT(rolePublisher, []string{"*"}))

	w := httptest.NewRecorder()
	hub.PublishHandler(w, req)

	resp := w.Result()

	t.Cleanup(func() {
		assert.NoError(t, resp.Body.Close())
	})

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Equal(t, ErrTooManyTopics.Error()+"\n", w.Body.String())
}

func TestPublishHandlerTooManyClaimMatchers(t *testing.T) {
	t.Parallel()

	hub := createDummy(t)

	scope := make([]string, maxClaimMatchers+1)
	for i := range scope {
		scope[i] = "https://example.com/books/1"
	}

	form := url.Values{}
	form.Add("topic", "https://example.com/books/1")
	form.Add("data", "Hello!")

	req := httptest.NewRequest(http.MethodPost, defaultHubURL, strings.NewReader(form.Encode()))
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Add("Authorization", bearerPrefix+createDummyAuthorizedJWT(rolePublisher, scope))

	w := httptest.NewRecorder()
	hub.PublishHandler(w, req)

	resp := w.Result()

	t.Cleanup(func() {
		assert.NoError(t, resp.Body.Close())
	})

	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

// TestPublishUpdateIngressBoundsAreFixed is the literal canary for the
// ingress caps: a typo that bumped a constant would otherwise be invisible
// because TestPublishHandlerInvalidUpdateField uses `+1` for its oversize
// fixture. Bumping any of these is an SDK/CHANGELOG-worthy contract change.
func TestPublishUpdateIngressBoundsAreFixed(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 1024, maxUpdateIDBytes)
	assert.Equal(t, 1024, maxUpdateTypeBytes)
	assert.Equal(t, 1024, maxUpdateTopicBytes)
	assert.Equal(t, 64, maxPublishTopics)
	assert.Equal(t, 1000, maxClaimMatchers)
	assert.Equal(t, 1000, maxQueryTopics)
}

// TestPublishHandlerAcceptsMaxLengthTopic is the positive control for the topic
// byte-length cap: a topic at exactly maxUpdateTopicBytes must pass. Without it
// a `>` → `>=` regression on the topic length check ships green, since
// TestPublishHandlerInvalidTopicField only submits maxUpdateTopicBytes+1.
func TestPublishHandlerAcceptsMaxLengthTopic(t *testing.T) {
	t.Parallel()

	const prefix = "https://example.com/"

	topic := prefix + strings.Repeat("a", maxUpdateTopicBytes-len(prefix))
	require.Len(t, topic, maxUpdateTopicBytes, "fixture must be exactly the byte cap")

	hub := createDummy(t)

	form := url.Values{}
	form.Add("topic", topic)
	form.Add("data", "foo")

	req := httptest.NewRequest(http.MethodPost, defaultHubURL, strings.NewReader(form.Encode()))
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Add("Authorization", bearerPrefix+createDummyAuthorizedJWT(rolePublisher, []string{"*"}))

	w := httptest.NewRecorder()
	hub.PublishHandler(w, req)
	resp := w.Result()

	t.Cleanup(func() { _ = resp.Body.Close() })

	assert.Equal(t, http.StatusOK, resp.StatusCode,
		"a topic at exactly maxUpdateTopicBytes must be accepted (validator counts bytes)")
}

// TestPublishHandlerAcceptsMaxTopicCount is the positive control for the topic
// COUNT cap: exactly maxPublishTopics topics must be accepted. Without it a
// `>` → `>=` off-by-one in either count guard (the handler pre-check or
// Validate) silently rejects legitimate maxPublishTopics-topic publishes.
func TestPublishHandlerAcceptsMaxTopicCount(t *testing.T) {
	t.Parallel()

	hub := createDummy(t)

	form := url.Values{}
	for range maxPublishTopics {
		form.Add("topic", "https://example.com/books/1")
	}

	form.Add("data", "foo")

	req := httptest.NewRequest(http.MethodPost, defaultHubURL, strings.NewReader(form.Encode()))
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Add("Authorization", bearerPrefix+createDummyAuthorizedJWT(rolePublisher, []string{"*"}))

	w := httptest.NewRecorder()
	hub.PublishHandler(w, req)
	resp := w.Result()

	t.Cleanup(func() { _ = resp.Body.Close() })

	assert.Equal(t, http.StatusOK, resp.StatusCode,
		"exactly maxPublishTopics topics must be accepted")
}

// TestHubPublishValidatesProgrammaticPath proves the architectural claim that
// validation lives in Hub.Publish, not only PublishHandler: a programmatic
// caller (library use, or a transport republishing) is rejected with the
// sentinel error and the update never reaches transport.Dispatch. A regression
// moving validation back into the handler would keep the handler tests green
// but fail this one.
func TestHubPublishValidatesProgrammaticPath(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		update *Update
		want   error
	}{
		{"too many topics", &Update{Topics: make([]string, maxPublishTopics+1)}, ErrTooManyTopics},
		{"oversize topic", &Update{Topics: []string{strings.Repeat("a", maxUpdateTopicBytes+1)}}, ErrTopicTooLong},
		{"forbidden char topic", &Update{Topics: []string{"https://example.com/a\nb"}}, ErrInvalidTopic},
		{"reserved topic", &Update{Topics: []string{"https://example.com/.well-known/mercure/x"}}, ErrReservedTopic},
		{"oversize id", &Update{Topics: []string{"https://example.com/x"}, Event: Event{ID: strings.Repeat("a", maxUpdateIDBytes+1)}}, ErrEventIDTooLong},
		{"forbidden char id", &Update{Topics: []string{"https://example.com/x"}, Event: Event{ID: "a\nb"}}, ErrInvalidEventID},
		{"oversize type", &Update{Topics: []string{"https://example.com/x"}, Event: Event{Type: strings.Repeat("a", maxUpdateTypeBytes+1)}}, ErrEventTypeTooLong},
		{"forbidden char type", &Update{Topics: []string{"https://example.com/x"}, Event: Event{Type: "a\nb"}}, ErrInvalidEventType},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			transport := &dispatchRecordingTransport{}
			hub := createDummy(t, WithTransport(transport))

			err := hub.Publish(t.Context(), tc.update)

			require.ErrorIs(t, err, tc.want)
			assert.Nil(t, transport.got, "rejected update must not reach transport.Dispatch")
		})
	}
}

// TestUpdateValidateSSEFields locks the NARROW contract of the exported
// ValidateSSEFields: it checks ONLY the fields event.go writes verbatim into
// the SSE wire frame (id, type) for the frame-forging chars, and nothing else.
// It deliberately does NOT check topics (never on the wire) or length caps —
// so a receive-side caller (redistransport, a separate module) can guard the
// SSE-injection vector without rejecting reserved-topic subscription events or
// re-imposing publish-only length limits. The topic/oversize cases below are
// the tripwire against a refactor that makes ValidateSSEFields delegate to the
// full Validate() and reintroduce that reserved-topic landmine.
func TestUpdateValidateSSEFields(t *testing.T) {
	t.Parallel()

	reserved := "https://example.com/.well-known/mercure/x"
	cases := []struct {
		name   string
		update *Update
		want   error // nil == accepted
	}{
		{"clean id and type", &Update{Event: Event{ID: "abc", Type: "message"}}, nil},
		{"empty id and type", &Update{}, nil},
		{"CR in id", &Update{Event: Event{ID: "a\rb"}}, ErrInvalidEventID},
		{"LF in id", &Update{Event: Event{ID: "a\nb"}}, ErrInvalidEventID},
		{"NUL in id", &Update{Event: Event{ID: "a\x00b"}}, ErrInvalidEventID},
		{"CR in type", &Update{Event: Event{Type: "a\rb"}}, ErrInvalidEventType},
		{"LF in type", &Update{Event: Event{Type: "a\nb"}}, ErrInvalidEventType},
		{"NUL in type", &Update{Event: Event{Type: "a\x00b"}}, ErrInvalidEventType},
		{"clean id, forbidden type", &Update{Event: Event{ID: "ok", Type: "a\nb"}}, ErrInvalidEventType},
		{"both forbidden, id checked first", &Update{Event: Event{ID: "a\nb", Type: "c\nd"}}, ErrInvalidEventID},
		// Contract tripwires: ValidateSSEFields must ignore everything but id/type chars.
		{"reserved topic, clean id/type accepted", &Update{Topics: []string{reserved}, Event: Event{ID: "ok"}}, nil},
		{"forbidden-char topic ignored", &Update{Topics: []string{"https://example.com/a\nb"}, Event: Event{ID: "ok"}}, nil},
		{"oversize id ignored", &Update{Event: Event{ID: strings.Repeat("a", maxUpdateIDBytes+1)}}, nil},
		{"oversize type ignored", &Update{Event: Event{Type: strings.Repeat("a", maxUpdateTypeBytes+1)}}, nil},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := tc.update.ValidateSSEFields()

			if tc.want == nil {
				assert.NoError(t, err)

				return
			}

			require.ErrorIs(t, err, tc.want)
		})
	}
}

// TestValidSSEFieldValue covers the shared predicate that ValidateSSEFields and
// the transport's replay-cursor guard both rely on: any value written verbatim
// into an SSE frame must be free of CR/LF/NUL.
func TestValidSSEFieldValue(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		s    string
		want bool
	}{
		{"empty", "", true},
		{"plain", "urn:uuid:abc", true},
		{"CR", "a\rb", false},
		{"LF", "a\nb", false},
		{"NUL", "a\x00b", false},
		{"CRLF", "a\r\nb", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.want, ValidSSEFieldValue(tc.s))
		})
	}
}

// TestValidateSSEFieldsCoversEveryVerbatimField structurally enforces the I2
// invariant — ValidateSSEFields checks exactly the Event fields that Event.String
// writes verbatim into the SSE wire frame — rather than trusting prose + hand-kept
// case lists. For EVERY string field of Event (via reflection, so a newly added
// field is auto-covered), it injects a CR-delimited fake SSE field: if Event.String
// emits it as a raw new frame line (verbatim, like id/type), ValidateSSEFields MUST
// reject it; if Event.String escapes it (Data, via dataReplacer), it must NOT need
// to. Adding a verbatim string field to Event.String without covering it in
// ValidateSSEFields trips this test.
func TestValidateSSEFieldsCoversEveryVerbatimField(t *testing.T) {
	t.Parallel()

	et := reflect.TypeFor[Event]()
	for i := range et.NumField() {
		f := et.Field(i)
		if f.Type.Kind() != reflect.String {
			continue
		}

		t.Run(f.Name, func(t *testing.T) {
			t.Parallel()

			ev := Event{}
			reflect.ValueOf(&ev).Elem().Field(i).SetString("ok\revent: INJECTED")

			// A raw CR that survives into the rendered frame forges a new SSE line;
			// dataReplacer neutralizes it for Data. The guard must reject exactly the
			// fields whose bad value forges a frame.
			forged := strings.Contains(ev.String(), "\revent: INJECTED")
			rejected := (&Update{Event: ev}).ValidateSSEFields() != nil

			assert.Equalf(t, forged, rejected,
				"bad %s: Event.String forges a frame=%v but ValidateSSEFields rejects=%v — they must agree (cover the field in ValidateSSEFields, or confirm Event.String escapes it)",
				f.Name, forged, rejected)
		})
	}
}

// TestPublishHandlerInvalidTopicField mirrors the id/type validation
// matrix on the "topic" form field. Topics flow into log attrs, OTel span
// attrs, and the Redis stream payload — without ingress validation a
// publisher with `mercure.publish: ["*"]` can submit oversize/CR/LF/NUL
// topics with no spec ceiling.
func TestPublishHandlerInvalidTopicField(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		topic    string
		wantBody error
	}{
		{"oversize", strings.Repeat("a", maxUpdateTopicBytes+1), ErrTopicTooLong},
		{"contains LF", "https://example.com/foo\nbar", ErrInvalidTopic},
		{"contains CR", "https://example.com/foo\rbar", ErrInvalidTopic},
		{"contains NUL", "https://example.com/foo\x00bar", ErrInvalidTopic},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			hub := createDummy(t)

			form := url.Values{}
			form.Add("topic", tc.topic)
			form.Add("data", "foo")

			req := httptest.NewRequest(http.MethodPost, defaultHubURL, strings.NewReader(form.Encode()))
			req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
			req.Header.Add("Authorization", bearerPrefix+createDummyAuthorizedJWT(rolePublisher, []string{"*"}))

			w := httptest.NewRecorder()
			hub.PublishHandler(w, req)
			resp := w.Result()

			t.Cleanup(func() { assert.NoError(t, resp.Body.Close()) })

			assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
			assert.Equal(t, tc.wantBody.Error()+"\n", w.Body.String())
		})
	}
}

// TestPublishHandlerAcceptsEmptyID is the positive control for the
// validator's empty-string handling: an empty supplied id passes through
// (and AssignUUID assigns a UUIDv7 URN downstream). Without this baseline,
// a typo flipping the length check to `len(rawID) >= 0` would silently
// reject every publish.
func TestPublishHandlerAcceptsEmptyID(t *testing.T) {
	t.Parallel()

	hub := createDummy(t)

	form := url.Values{}
	form.Add("topic", "https://example.com/books/1")
	form.Add("data", "foo")
	// id deliberately omitted

	req := httptest.NewRequest(http.MethodPost, defaultHubURL, strings.NewReader(form.Encode()))
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Add("Authorization", bearerPrefix+createDummyAuthorizedJWT(rolePublisher, []string{"*"}))

	w := httptest.NewRecorder()
	hub.PublishHandler(w, req)
	resp := w.Result()

	t.Cleanup(func() { assert.NoError(t, resp.Body.Close()) })

	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, _ := io.ReadAll(resp.Body)
	assert.True(t, strings.HasPrefix(string(body), "urn:uuid:"),
		"empty id must trigger downstream UUIDv7 URN assignment; got %q", body)
}

// assertPublishHandlerRejectsField runs the PublishHandler against an
// otherwise-valid publish form with the named field set to value, and
// asserts a 400 Bad Request whose body is the expected sentinel error
// (wantErr). Used to keep id-vs-type validation cases consistent.
func assertPublishHandlerRejectsField(t *testing.T, field, value string, wantErr error) {
	t.Helper()

	hub := createDummy(t)

	form := url.Values{}
	form.Add("topic", "https://example.com/books/1")
	form.Add("data", "foo")
	form.Add(field, value)

	req := httptest.NewRequest(http.MethodPost, defaultHubURL, strings.NewReader(form.Encode()))
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Add("Authorization", bearerPrefix+createDummyAuthorizedJWT(rolePublisher, []string{"*"}))

	w := httptest.NewRecorder()
	hub.PublishHandler(w, req)

	resp := w.Result()

	t.Cleanup(func() {
		assert.NoError(t, resp.Body.Close())
	})

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Equal(t, wantErr.Error()+"\n", w.Body.String())
}

// TestPublishHandlerInvalidUpdateField locks the ingress validation
// contract for publisher-supplied "id" and "type": oversize and
// CR/LF/NUL must be rejected with 400. Event.String writes both fields
// directly into "id: …\n" / "event: …\n" without newline escaping (only
// "data" is escaped), so a CR/LF in either field would let a publisher
// inject arbitrary SSE frames onto the wire.
func TestPublishHandlerInvalidUpdateField(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		field string
		value string
		want  error
	}{
		{"id/oversize", "id", strings.Repeat("a", maxUpdateIDBytes+1), ErrEventIDTooLong},
		{"id/contains LF", "id", "urn:uuid:foo\nbar", ErrInvalidEventID},
		{"id/contains CR", "id", "urn:uuid:foo\rbar", ErrInvalidEventID},
		{"id/contains NUL", "id", "urn:uuid:foo\x00bar", ErrInvalidEventID},
		{"type/oversize", "type", strings.Repeat("a", maxUpdateTypeBytes+1), ErrEventTypeTooLong},
		{"type/contains LF", "type", "create\nevent", ErrInvalidEventType},
		{"type/contains CR", "type", "create\revent", ErrInvalidEventType},
		{"type/contains NUL", "type", "create\x00event", ErrInvalidEventType},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assertPublishHandlerRejectsField(t, tc.field, tc.value, tc.want)
		})
	}
}

// TestPublishHandlerDispatchErrorLogsBoundedFields is the literal canary
// for the "no slog.Any(*Update)" invariant on the publish-side error path.
// A regression to slog.Any("update", u) would silently reopen the
// publisher-controlled-Type log-flood surface that the bounded-fields
// rewrite closed; the canary uses a sentinel Type string and asserts it
// is NOT present in the captured log line, while the bounded fields
// (update_id, error) ARE present.
func TestPublishHandlerDispatchErrorLogsBoundedFields(t *testing.T) {
	t.Parallel()

	const sentinelType = "SENTINEL_PUBLISHER_TYPE_SHOULD_NOT_LEAK"

	var buf bytes.Buffer

	// Wrap with NewSlogHandler so the canary exercises the full production
	// log path — the mercureHandler middleware extracts UpdateContextKey
	// from ctx and emits slog.Any("update", u). If a future refactor makes
	// that middleware leak Type ahead of LogValue's bounded shape, this
	// canary catches it; without the wrapper the test would silently
	// bypass mercureHandler and miss the regression.
	logger := slog.New(NewSlogHandler(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelError})))
	hub := createDummy(t, WithLogger(logger))

	require.NoError(t, hub.transport.Close(t.Context()))

	form := url.Values{}
	form.Add("topic", "https://example.com/books/1")
	form.Add("data", "Hello!")
	form.Add("id", "urn:uuid:canary-id")
	form.Add("type", sentinelType)

	req := httptest.NewRequest(http.MethodPost, defaultHubURL, strings.NewReader(form.Encode()))
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Add("Authorization", bearerPrefix+createDummyAuthorizedJWT(rolePublisher, []string{"*"}))

	w := httptest.NewRecorder()
	hub.PublishHandler(w, req)
	resp := w.Result()

	t.Cleanup(func() {
		assert.NoError(t, resp.Body.Close())
	})

	require.Equal(t, http.StatusInternalServerError, resp.StatusCode,
		"dispatch error must surface as 500; without it the dispatch-error log path didn't fire and this canary is vacuous")

	logOutput := buf.String()
	assert.NotContains(t, logOutput, sentinelType,
		"publisher-controlled Type must NOT appear in slog attrs on dispatch error — slog.Any(*Update) regression")
	assert.Contains(t, logOutput, "Failed to dispatch update",
		"dispatch-error log line must fire — positive control proving the canary is exercising the right code path")
	assert.Contains(t, logOutput, "urn:uuid:canary-id",
		"bounded update_id field must appear — positive control proving the bounded-fields path is structurally correct")
	assert.Contains(t, logOutput, "https://example.com/books/1",
		"bounded topics field must appear on publish-side — publish-side intentionally keeps topics (bounded by JWT publish claim); removing would lose forensic information")
}

// dispatchRecordingTransport captures the Update passed to Dispatch so
// tests can assert publisher-supplied fields reach the transport without
// silent truncation by the ingress validator.
type dispatchRecordingTransport struct {
	nopTransport

	got *Update
}

func (t *dispatchRecordingTransport) Dispatch(_ context.Context, u *Update) error {
	t.got = u

	return nil
}

var _ Transport = (*dispatchRecordingTransport)(nil)

// TestPublishHandlerMaxLengthIDReachesTransportUntruncated locks the
// no-silent-truncation contract: a max-length id must reach the transport
// with its full byte content. Without this, a regression like
// rawID = rawID[:maxUpdateIDBytes-1] (silent truncation) would still pass
// the existing positive-control test, since StatusOK only proves the
// validator didn't reject.
func TestPublishHandlerMaxLengthIDReachesTransportUntruncated(t *testing.T) {
	t.Parallel()

	transport := &dispatchRecordingTransport{}
	hub := createDummy(t, WithTransport(transport))

	maxID := strings.Repeat("a", maxUpdateIDBytes)
	maxType := strings.Repeat("b", maxUpdateTypeBytes)

	form := url.Values{}
	form.Add("topic", "https://example.com/books/1")
	form.Add("data", "foo")
	form.Add("id", maxID)
	form.Add("type", maxType)

	req := httptest.NewRequest(http.MethodPost, defaultHubURL, strings.NewReader(form.Encode()))
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Add("Authorization", bearerPrefix+createDummyAuthorizedJWT(rolePublisher, []string{"*"}))

	w := httptest.NewRecorder()
	hub.PublishHandler(w, req)
	resp := w.Result()

	t.Cleanup(func() { _ = resp.Body.Close() })

	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NotNil(t, transport.got, "transport.Dispatch must be invoked for the positive-control to be meaningful")
	assert.Equal(t, maxID, transport.got.ID, "id at the byte cap must reach the transport verbatim (no silent truncation)")
	assert.Equal(t, maxType, transport.got.Type, "type at the byte cap must reach the transport verbatim (no silent truncation)")
}

// TestPublishHandlerMultiByteUnicodeIDAccepted pins the byte-vs-codepoint
// semantics of the id/type validators. ContainsAny is rune-aware, so a
// multi-byte UTF-8 codepoint that doesn't decode to any forbidden rune is
// safe. The length check is byte-counted, so a 1024-byte payload of
// 256 4-byte runes hits exactly the cap.
func TestPublishHandlerMultiByteUnicodeIDAccepted(t *testing.T) {
	t.Parallel()

	const fourByteRune = "𝕏" // U+1D54F MATHEMATICAL DOUBLE-STRUCK CAPITAL X — 4 UTF-8 bytes

	multiByteID := strings.Repeat(fourByteRune, maxUpdateIDBytes/4)

	require.Len(t, multiByteID, maxUpdateIDBytes,
		"test fixture must produce exactly maxUpdateIDBytes bytes; without this the validator-boundary claim is unfounded")

	hub := createDummy(t)

	form := url.Values{}
	form.Add("topic", "https://example.com/books/1")
	form.Add("data", "foo")
	form.Add("id", multiByteID)

	req := httptest.NewRequest(http.MethodPost, defaultHubURL, strings.NewReader(form.Encode()))
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Add("Authorization", bearerPrefix+createDummyAuthorizedJWT(rolePublisher, []string{"*"}))

	w := httptest.NewRecorder()
	hub.PublishHandler(w, req)
	resp := w.Result()

	t.Cleanup(func() { _ = resp.Body.Close() })

	assert.Equal(t, http.StatusOK, resp.StatusCode,
		"multi-byte UTF-8 id at exactly the byte cap (no forbidden runes) must pass — the validator counts bytes, not codepoints")
}

// TestPublishHandlerAcceptsMaxLengthUpdateID is the positive control: an
// id exactly at the limit (and a type exactly at the limit) must pass
// validation. Without this baseline, a typo like ">=" instead of ">"
// in the length check would silently regress to "always reject".
func TestPublishHandlerAcceptsMaxLengthUpdateID(t *testing.T) {
	t.Parallel()

	hub := createDummy(t)

	form := url.Values{}
	form.Add("topic", "https://example.com/books/1")
	form.Add("data", "foo")
	form.Add("id", strings.Repeat("a", maxUpdateIDBytes))
	form.Add("type", strings.Repeat("b", maxUpdateTypeBytes))

	req := httptest.NewRequest(http.MethodPost, defaultHubURL, strings.NewReader(form.Encode()))
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Add("Authorization", bearerPrefix+createDummyAuthorizedJWT(rolePublisher, []string{"*"}))

	w := httptest.NewRecorder()
	hub.PublishHandler(w, req)

	resp := w.Result()

	t.Cleanup(func() {
		assert.NoError(t, resp.Body.Close())
	})

	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

// slowTransport completes Dispatch after a delay but respects ctx cancellation —
// used to prove the publish_timeout guard does NOT arm a deadline when disabled.
type slowTransport struct {
	nopTransport

	delay time.Duration
}

func (tr slowTransport) Dispatch(ctx context.Context, _ *Update) error {
	select {
	case <-time.After(tr.delay):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// TestPublishHandlerNoPublishTimeoutCompletes: with publish_timeout disabled
// (the default), a slow-but-completing dispatch is NOT aborted — the `> 0` guard
// must not arm a zero-duration deadline. If it did, the ctx-respecting transport
// would observe an immediately-cancelled context and the handler would 504.
func TestPublishHandlerNoPublishTimeoutCompletes(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		hub := createDummy(t, WithTransport(slowTransport{delay: time.Hour}))

		topics := []string{"https://example.com/books/1"}

		form := url.Values{}
		form.Add("topic", topics[0])
		form.Add("data", "Hello!")

		req := httptest.NewRequest(http.MethodPost, defaultHubURL, strings.NewReader(form.Encode()))
		req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Add("Authorization", bearerPrefix+createDummyAuthorizedJWT(rolePublisher, topics))

		w := httptest.NewRecorder()
		hub.PublishHandler(w, req)

		resp := w.Result()

		t.Cleanup(func() { assert.NoError(t, resp.Body.Close()) })

		assert.Equal(t, http.StatusOK, resp.StatusCode, "a completing dispatch must not be aborted when publish_timeout is disabled")
	})
}

// TestPublishHandlerTimeoutSetNonTimeoutErrorKeepsStatus: with publish_timeout
// configured (but not firing), a non-timeout failure from Hub.Publish keeps its
// real status — it is NOT reclassified as 504. Here an over-long "type" trips
// ErrEventTypeTooLong inside Hub.Publish, which must surface as 400.
func TestPublishHandlerTimeoutSetNonTimeoutErrorKeepsStatus(t *testing.T) {
	t.Parallel()

	hub := createDummy(t, WithPublishTimeout(time.Hour))

	topics := []string{"https://example.com/books/1"}

	form := url.Values{}
	form.Add("topic", topics[0])
	form.Add("data", "Hello!")
	form.Add("type", strings.Repeat("a", maxUpdateTypeBytes+1))

	req := httptest.NewRequest(http.MethodPost, defaultHubURL, strings.NewReader(form.Encode()))
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Add("Authorization", bearerPrefix+createDummyAuthorizedJWT(rolePublisher, topics))

	w := httptest.NewRecorder()
	hub.PublishHandler(w, req)

	resp := w.Result()

	t.Cleanup(func() { assert.NoError(t, resp.Body.Close()) })

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode, "a non-timeout error must keep its status even when publish_timeout is configured")
}
