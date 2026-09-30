package mercure

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"testing/iotest"
	"testing/synctest"
	"time"
	"uuid"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPublish(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		hub := createDummy(t)

		topics := []string{"https://example.com/books/1"}
		s := NewLocalSubscriber("", slog.Default(), &TopicMatcherStore{})
		s.setMatchers(stringsToExactMatchers(topics), stringsToExactMatchers(topics))

		require.NoError(t, hub.transport.AddSubscriber(t.Context(), s))

		go func() {
			u, ok := <-s.Receive()

			assert.True(t, ok)
			assert.NotNil(t, u)
			assert.Equal(t, "id", u.ID)
			assert.Equal(t, s.SubscribedMatchers[0].Pattern, u.Topics[0])
			assert.Equal(t, "Hello!", u.Data)
			assert.True(t, u.Private)
		}()

		require.NoError(t, hub.Publish(t.Context(), &Update{
			ID:      "id",
			Data:    "Hello!",
			Topics:  []string{s.SubscribedMatchers[0].Pattern},
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

func TestPublishHandlerNotAuthorizedTopicMatcher(t *testing.T) {
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

	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
}

func TestPublishHandlerEmptyTopicMatcher(t *testing.T) {
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

	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
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
		s := NewLocalSubscriber("", slog.Default(), &TopicMatcherStore{})
		s.setMatchers(stringsToExactMatchers(topics), stringsToExactMatchers(topics))

		require.NoError(t, hub.transport.AddSubscriber(t.Context(), s))

		go func() {
			u, ok := <-s.Receive()
			assert.True(t, ok)
			assert.NotNil(t, u)
			assert.Equal(t, "id", u.ID)
			assert.Equal(t, s.SubscribedMatchers[0].Pattern, u.Topics[0])
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
		req.Header.Add("Authorization", bearerPrefix+createDummyAuthorizedJWT(rolePublisher, topics))

		w := httptest.NewRecorder()
		hub.PublishHandler(w, req)

		resp := w.Result()

		t.Cleanup(func() {
			assert.NoError(t, resp.Body.Close())
		})

		body, _ := io.ReadAll(resp.Body)

		assert.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Equal(t, "text/plain; charset=utf-8", resp.Header.Get("Content-Type"))
		assert.Equal(t, "id", string(body))

		synctest.Wait()
	})
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

		s := NewLocalSubscriber("", slog.Default(), &TopicMatcherStore{})
		s.setMatchers(stringsToExactMatchers([]string{"https://example.com/books/1"}), nil)

		require.NoError(t, h.transport.AddSubscriber(t.Context(), s))

		go func() {
			u := <-s.Receive()
			assert.NotNil(t, u)

			_, err := uuid.Parse(strings.TrimPrefix(u.ID, "urn:uuid:"))
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

		_, err := uuid.Parse(strings.TrimPrefix(body, "urn:uuid:"))
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

// A dispatch refused by a closed transport is logged at Debug (the transport
// may wrap ErrClosedTransport) and answers 500; an update the transport
// refuses as too large is a client error, logged at Info and answered 413; any
// other dispatch failure is logged at ERROR and answers 500.
func TestPublishDispatchErrorLogLevel(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name       string
		err        error
		wantLevel  slog.Level
		wantStatus int
	}{
		{name: "closed transport", err: ErrClosedTransport, wantLevel: slog.LevelDebug, wantStatus: http.StatusInternalServerError},
		{name: "transport's own closed error", err: ownClosedTransportError{}, wantLevel: slog.LevelDebug, wantStatus: http.StatusInternalServerError},
		{name: "update too large", err: fmt.Errorf("encode: %w", ErrCodecPayloadTooLarge), wantLevel: slog.LevelInfo, wantStatus: http.StatusRequestEntityTooLarge},
		{name: "other error", err: errDispatchFailed, wantLevel: slog.LevelError, wantStatus: http.StatusInternalServerError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			logs := &recordingLogHandler{}
			transport := &dispatchErrorTransport{LocalTransport: NewLocalTransport(NewSubscriberList(0)), err: tc.err}
			hub := createDummy(t, WithTransport(transport), WithLogger(slog.New(logs)))

			form := url.Values{}
			form.Add("topic", "https://example.com/books/1")
			form.Add("data", "Hello!")

			req := httptest.NewRequest(http.MethodPost, defaultHubURL, strings.NewReader(form.Encode()))
			req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
			req.Header.Add("Authorization", bearerPrefix+createDummyAuthorizedJWT(rolePublisher, []string{"https://example.com/books/1"}))

			w := httptest.NewRecorder()
			hub.PublishHandler(w, req)

			assert.Equal(t, tc.wantStatus, w.Code)
			assertLoggedOnceAt(t, logs, "Failed to dispatch update", tc.wantLevel)
		})
	}
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

		if resp.StatusCode == http.StatusBadRequest {
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
		{"valid", Update{ID: "id", Type: "type", Topics: []string{"https://example.com/books/1"}}, nil},
		{"no topics", Update{}, ErrMissingTopic},
		// An empty topic value resolves to the hub URL itself, which is reserved.
		{"empty topic value", Update{Topics: []string{""}}, ErrReservedTopic},
		{"reserved topic", Update{Topics: []string{"https://example.com/.well-known/mercure/subscriptions/foo"}}, ErrReservedTopic},
		{"reserved topic relative", Update{Topics: []string{"mercure/subscriptions/foo"}}, ErrReservedTopic},
		{"reserved topic absolute path", Update{Topics: []string{"/.well-known/mercure/subscriptions/foo"}}, ErrReservedTopic},
		{"reserved topic exact", Update{Topics: []string{"https://example.com/.well-known/mercure"}}, ErrReservedTopic},
		{"reserved topic percent-encoded", Update{Topics: []string{"https://example.com/.well-known/%6Dercure/subscriptions/foo"}}, ErrReservedTopic},
		{"reserved topic backslashes", Update{Topics: []string{`https://example.com\.well-known\mercure\subscriptions\foo`}}, ErrReservedTopic},
		{"reserved wildcard", Update{Topics: []string{"*"}}, ErrReservedWildcard},
		{"non-reserved mid-path namespace", Update{Topics: []string{"https://example.com/foo/.well-known/mercure/bar"}}, nil},
		{"non-reserved sibling path", Update{Topics: []string{"https://example.com/.well-known/mercure-dashboard"}}, nil},
		{"non-reserved opaque topic", Update{Topics: []string{"urn:example:mercure"}}, nil},
		{"id starts with #", Update{Topics: []string{"https://example.com/books/1"}, ID: "#42"}, ErrInvalidEventID},
		{"id too long", Update{Topics: []string{"https://example.com/books/1"}, ID: strings.Repeat("a", maxEventIDLength+1)}, ErrInvalidEventID},
		{"topic too long", Update{Topics: []string{"https://example.com/" + strings.Repeat("a", maxTopicLength)}}, ErrInvalidTopic},
		{"id earliest", Update{Topics: []string{"https://example.com/books/1"}, ID: EarliestLastEventID}, ErrInvalidEventID},
		{"topic NUL", Update{Topics: []string{"https://example.com/foo\x00bar"}}, ErrInvalidTopic},
		{"topic C0", Update{Topics: []string{"https://example.com/foo\nbar"}}, ErrInvalidTopic},
		{"topic invalid UTF-8", Update{Topics: []string{"https://example.com/\xff"}}, ErrInvalidTopic},
		{"id LF", Update{Topics: []string{"https://example.com/books/1"}, ID: "foo\nevent: injected"}, ErrInvalidEventID},
		{"id CR", Update{Topics: []string{"https://example.com/books/1"}, ID: "foo\rinjected"}, ErrInvalidEventID},
		{"id NUL", Update{Topics: []string{"https://example.com/books/1"}, ID: "foo\x00bar"}, ErrInvalidEventID},
		{"type LF", Update{Topics: []string{"https://example.com/books/1"}, Type: "foo\nid: injected"}, ErrInvalidEventType},
		{"type CR", Update{Topics: []string{"https://example.com/books/1"}, Type: "foo\rinjected"}, ErrInvalidEventType},
		{"type NUL", Update{Topics: []string{"https://example.com/books/1"}, Type: "foo\x00bar"}, ErrInvalidEventType},
		{"type reserved mercure", Update{Topics: []string{"https://example.com/books/1"}, Type: reservedEventType}, ErrReservedEventType},
	}

	for i := range cases {
		tc := &cases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := tc.update.Validate(urlPatternFallbackBase)
			if tc.want == nil {
				assert.NoError(t, err)

				return
			}

			assert.ErrorIs(t, err, tc.want)
		})
	}
}

func TestValidSSEFieldValue(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		value string
		want  bool
	}{
		{"plain", "ordinary-value", true},
		{"empty", "", true},
		{"CR", "foo\rbar", false},
		{"LF", "foo\nbar", false},
		{"CRLF", "foo\r\nbar", false},
		{"NUL", "foo\x00bar", false},
		{"other control", "foo\x1fbar", false},
		{"invalid UTF-8", "foo\xffbar", false},
	}

	for i := range cases {
		tc := &cases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, ValidSSEFieldValue(tc.value))
		})
	}
}

func TestUpdateValidateSSEFields(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		update Update
		want   error
	}{
		{"plain", Update{ID: "id", Type: "type"}, nil},
		{"empty", Update{}, nil},
		{"reserved topic", Update{Topics: []string{"/.well-known/mercure/subscriptions/1"}, ID: "id", Type: "mercure"}, nil},
		{"long ID", Update{ID: strings.Repeat("a", maxEventIDLength+1)}, nil},
		{"ID CR", Update{ID: "foo\rbar"}, ErrInvalidEventID},
		{"ID LF", Update{ID: "foo\nbar"}, ErrInvalidEventID},
		{"ID CRLF", Update{ID: "foo\r\nbar"}, ErrInvalidEventID},
		{"ID NUL", Update{ID: "foo\x00bar"}, ErrInvalidEventID},
		{"ID other control", Update{ID: "foo\x1fbar"}, ErrInvalidEventID},
		{"ID invalid UTF-8", Update{ID: "foo\xffbar"}, ErrInvalidEventID},
		{"Type CR", Update{Type: "foo\rbar"}, ErrInvalidEventType},
		{"Type LF", Update{Type: "foo\nbar"}, ErrInvalidEventType},
		{"Type CRLF", Update{Type: "foo\r\nbar"}, ErrInvalidEventType},
		{"Type NUL", Update{Type: "foo\x00bar"}, ErrInvalidEventType},
		{"Type other control", Update{Type: "foo\x1fbar"}, ErrInvalidEventType},
		{"Type invalid UTF-8", Update{Type: "foo\xffbar"}, ErrInvalidEventType},
	}

	for i := range cases {
		tc := &cases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := tc.update.ValidateSSEFields()
			if tc.want == nil {
				assert.NoError(t, err)

				return
			}

			assert.ErrorIs(t, err, tc.want)
		})
	}
}

// Hub-generated updates never go through Validate, so a receive-path guard must
// accept what the hub itself builds.
func TestValidateSSEFieldsAcceptsHubGeneratedUpdates(t *testing.T) {
	t.Parallel()

	// Built like the subscription events in subscribe.go.
	u := &Update{
		Topics:  []string{"/.well-known/mercure/subscriptions/foo/bar"},
		Private: true,
		Data:    "{}",
		Type:    reservedEventType,
	}
	u.AssignUUID()

	require.True(t, strings.HasPrefix(u.ID, "urn:uuid:"))
	require.NoError(t, u.ValidateSSEFields())

	// The reserved values are rejected by Validate for publishers only; the
	// receive path must not reject them.
	for _, id := range []string{"#fragment", EarliestLastEventID} {
		require.NoError(t, (&Update{ID: id}).ValidateSSEFields(), id)
		assert.True(t, ValidSSEFieldValue(id), id)
	}

	assert.True(t, ValidSSEFieldValue("urn:uuid:0195e2a0-7c00-7000-8000-000000000000"))
	assert.True(t, ValidSSEFieldValue(reservedEventType))
}

func TestValidateAcceptedUpdatesPassValidateSSEFields(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		id   string
		typ  string
	}{
		{"empty", "", ""},
		{"plain", "id", "type"},
		{"urn uuid", "urn:uuid:0195e2a0-7c00-7000-8000-000000000000", "message"},
		{"colon type", "a:b", "ns:event"},
		{"non-ASCII", "caf\u00e9-\u4e16\u754c", "\u00e9v\u00e9nement"},
	}

	for i := range cases {
		tc := &cases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			u := &Update{Topics: []string{"https://example.com/books/1"}, ID: tc.id, Type: tc.typ}
			require.NoError(t, u.Validate(urlPatternFallbackBase))
			assert.NoError(t, u.ValidateSSEFields())
		})
	}
}

func TestUpdateValidateTooManyTopics(t *testing.T) {
	t.Parallel()

	topics := make([]string, maxPublishTopics+1)
	for i := range topics {
		topics[i] = "https://example.com/books/1"
	}

	err := testUpdate(&Update{}, topics...).Validate(urlPatternFallbackBase)
	assert.ErrorIs(t, err, ErrTooManyTopics)
}

func TestPublishHandlerReservedTopicNamespace(t *testing.T) {
	t.Parallel()

	for _, topic := range []string{
		"/.well-known/mercure/subscriptions/foo",
		"https://example.com/.well-known/mercure/subscriptions/foo",
		"mercure/subscriptions/foo", // relative, resolves into the namespace
		"https://example.com/.well-known/%6Dercure/subscriptions/foo",
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

			// The reserved namespace is off-limits to every publisher
			// regardless of grants, so it is a request-validation failure
			// (400 invalid_request), not an authorization failure.
			assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
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
		// "mercure" is reserved for hub-generated events; a publisher using it
		// must be rejected (exercises the PublishHandler ErrReservedEventType arm).
		{"type", "mercure"},
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

	// Too many topics in a single authorization detail → invalid_token.
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

// blockingTransport stalls Dispatch until the context is done, then returns its
// error, modelling an unresponsive transport so a configured publish_timeout
// can be exercised deterministically.
type blockingTransport struct {
	nopTransport
}

func (blockingTransport) Dispatch(ctx context.Context, _ *Update) error {
	<-ctx.Done()

	return ctx.Err()
}

// newPublishRequest builds an authorized publish of form to topics.
func newPublishRequest(topics []string, form url.Values) *http.Request {
	req := httptest.NewRequest(http.MethodPost, defaultHubURL, strings.NewReader(form.Encode()))
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Add("Authorization", bearerPrefix+createDummyAuthorizedJWT(rolePublisher, topics))

	return req
}

// TestPublishHandlerPublishTimeout: when a dispatch outlives the configured
// publish_timeout, PublishHandler returns 504 (the write may have committed, so
// the publisher must treat it as indeterminate).
func TestPublishHandlerPublishTimeout(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		hub := createDummy(t, WithTransport(blockingTransport{}), WithPublishTimeout(5*time.Second))

		topics := []string{"https://example.com/books/1"}

		w := httptest.NewRecorder()
		hub.PublishHandler(w, newPublishRequest(topics, url.Values{"topic": topics, "data": {"Hello!"}}))

		resp := w.Result()

		t.Cleanup(func() { assert.NoError(t, resp.Body.Close()) })

		assert.Equal(t, http.StatusGatewayTimeout, resp.StatusCode, "a dispatch exceeding publish_timeout must return 504")
	})
}

// slowTransport completes Dispatch after a delay but respects ctx cancellation;
// it shows the publish_timeout guard arms no deadline when disabled.
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
// (the default), a slow-but-completing dispatch is not aborted: the `> 0` guard
// must not arm a zero-duration deadline. If it did, the ctx-respecting transport
// would observe an immediately-cancelled context and the handler would 504.
func TestPublishHandlerNoPublishTimeoutCompletes(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		hub := createDummy(t, WithTransport(slowTransport{delay: time.Hour}))

		topics := []string{"https://example.com/books/1"}

		w := httptest.NewRecorder()
		hub.PublishHandler(w, newPublishRequest(topics, url.Values{"topic": topics, "data": {"Hello!"}}))

		resp := w.Result()

		t.Cleanup(func() { assert.NoError(t, resp.Body.Close()) })

		assert.Equal(t, http.StatusOK, resp.StatusCode, "a completing dispatch must not be aborted when publish_timeout is disabled")
	})
}

// TestPublishHandlerTimeoutSetNonTimeoutErrorKeepsStatus: with publish_timeout
// configured (but not firing), a non-timeout failure from Hub.Publish keeps its
// real status and is not reclassified as 504. Here a control character in
// "type" trips ErrInvalidEventType inside Hub.Publish, which must surface as 400.
func TestPublishHandlerTimeoutSetNonTimeoutErrorKeepsStatus(t *testing.T) {
	t.Parallel()

	hub := createDummy(t, WithPublishTimeout(time.Hour))

	topics := []string{"https://example.com/books/1"}

	w := httptest.NewRecorder()
	hub.PublishHandler(w, newPublishRequest(topics, url.Values{"topic": topics, "data": {"Hello!"}, "type": {"a\x01b"}}))

	resp := w.Result()

	t.Cleanup(func() { assert.NoError(t, resp.Body.Close()) })

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode, "a non-timeout error must keep its status even when publish_timeout is configured")
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
// many publish goroutines, so the test double honors the same contract.
func (m *recordingFailureMetrics) UpdatePublishFailed(_ *Update, reason PublishFailureReason) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.reasons = append(m.reasons, reason)
}

func (m *recordingFailureMetrics) recorded() []PublishFailureReason {
	m.mu.Lock()
	defer m.mu.Unlock()

	return append([]PublishFailureReason(nil), m.reasons...)
}

// errTestDispatch is a static stand-in for a transport Dispatch failure in the
// classification test.
var errTestDispatch = errors.New("test dispatch failure")

// TestPublishFailureReasonForDispatch covers the timeout-vs-transport
// classification directly, including the guard that a coincidental
// DeadlineExceeded without the ErrPublishTimeout cause is not a publish_timeout.
func TestPublishFailureReasonForDispatch(t *testing.T) {
	t.Parallel()

	// Expired publish_timeout context: cause is ErrPublishTimeout, error is a
	// deadline error → timeout (either the deadline error or the sentinel).
	timedOut, cancel := context.WithTimeoutCause(context.Background(), 0, ErrPublishTimeout)
	defer cancel()

	<-timedOut.Done()

	assert.Equal(t, PublishFailureReasonTimeout, publishFailureReasonForDispatch(timedOut, timedOut.Err()))
	assert.Equal(t, PublishFailureReasonTimeout, publishFailureReasonForDispatch(timedOut, ErrPublishTimeout))

	// Cause is ErrPublishTimeout but the error is not a deadline error (a hard
	// transport error raced the deadline) → transport, not timeout.
	assert.Equal(t, PublishFailureReasonTransport, publishFailureReasonForDispatch(timedOut, errTestDispatch))

	// Plain transport error (no publish_timeout cause) → transport.
	assert.Equal(t, PublishFailureReasonTransport, publishFailureReasonForDispatch(context.Background(), errTestDispatch))

	// A DeadlineExceeded without the ErrPublishTimeout cause must stay
	// transport, not be reclassified as a publish_timeout.
	plainDeadline, cancel2 := context.WithTimeout(context.Background(), 0)
	defer cancel2()

	<-plainDeadline.Done()

	assert.Equal(t, PublishFailureReasonTransport, publishFailureReasonForDispatch(plainDeadline, plainDeadline.Err()))
}

// TestPublishRecordsFailureReason verifies Hub.Publish reports the correct
// reason, once, to the optional PublishFailureReporter at the validation and
// transport failure sites.
func TestPublishRecordsFailureReason(t *testing.T) {
	t.Parallel()

	t.Run("validation", func(t *testing.T) {
		t.Parallel()

		rec := &recordingFailureMetrics{}
		hub := createDummy(t, WithMetrics(rec))

		// A topic with a control character is rejected by update.Validate().
		require.Error(t, hub.Publish(t.Context(), &Update{Topics: []string{"https://example.com/\n"}}))

		assert.Equal(t, []PublishFailureReason{PublishFailureReasonValidation}, rec.recorded())
	})

	t.Run("transport", func(t *testing.T) {
		t.Parallel()

		rec := &recordingFailureMetrics{}
		hub := createDummy(t, WithMetrics(rec))
		// A closed transport makes Dispatch return an error (non-timeout).
		require.NoError(t, hub.transport.Close(t.Context()))

		require.Error(t, hub.Publish(t.Context(), &Update{Topics: []string{"https://example.com/books/1"}}))

		assert.Equal(t, []PublishFailureReason{PublishFailureReasonTransport}, rec.recorded())
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

		w := httptest.NewRecorder()
		hub.PublishHandler(w, newPublishRequest(topics, url.Values{"topic": topics, "data": {"Hello!"}}))

		resp := w.Result()

		t.Cleanup(func() { assert.NoError(t, resp.Body.Close()) })

		require.Equal(t, http.StatusGatewayTimeout, resp.StatusCode)
		assert.Equal(t, []PublishFailureReason{PublishFailureReasonTimeout}, rec.recorded())
	})
}

// TestPublishHandlerRecordsValidationFailureReason covers the HTTP early-reject
// paths: PublishHandler checks the topic count and each topic before Hub.Publish,
// so those rejections must still be metered as reason=validation, exactly once.
func TestPublishHandlerRecordsValidationFailureReason(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		topics []string
	}{
		{name: "forbidden character", topics: []string{"https://example.com/\n"}},
		{name: "too many topics", topics: make([]string, maxPublishTopics+1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rec := &recordingFailureMetrics{}
			hub := createDummy(t, WithMetrics(rec))

			w := httptest.NewRecorder()
			hub.PublishHandler(w, newPublishRequest([]string{"*"}, url.Values{"topic": tc.topics, "data": {"Hello!"}}))

			resp := w.Result()

			t.Cleanup(func() { assert.NoError(t, resp.Body.Close()) })

			// Fail fast on the status, so a missed early reject does not surface
			// as a reason mismatch.
			require.Equal(t, http.StatusBadRequest, resp.StatusCode, "the topics must be rejected before Hub.Publish")
			assert.Equal(t, []PublishFailureReason{PublishFailureReasonValidation}, rec.recorded())
		})
	}
}

// TestPublishHandlerRecordsContentRejectionsAsValidation: the handler's early
// 400s that reject the update's content (before Hub.Publish is reached) each
// meter reason=validation exactly once.
func TestPublishHandlerRecordsContentRejectionsAsValidation(t *testing.T) {
	t.Parallel()

	topics := []string{"https://example.com/books/1"}

	for _, tc := range []struct {
		name string
		body string
	}{
		{name: "missing topic", body: url.Values{"data": {"Hello!"}}.Encode()},
		{name: "invalid retry", body: url.Values{"topic": topics, "retry": {"soon"}}.Encode()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rec := &recordingFailureMetrics{}
			hub := createDummy(t, WithMetrics(rec))

			req := newPublishRequest([]string{"*"}, nil)
			req.Body = io.NopCloser(strings.NewReader(tc.body))

			w := httptest.NewRecorder()
			hub.PublishHandler(w, req)

			resp := w.Result()

			t.Cleanup(func() { assert.NoError(t, resp.Body.Close()) })

			require.Equal(t, http.StatusBadRequest, resp.StatusCode, "the request must be rejected before Hub.Publish")
			assert.Equal(t, []PublishFailureReason{PublishFailureReasonValidation}, rec.recorded())
		})
	}
}

// TestPublishHandlerDoesNotMeterNonContentRejections: auth rejections and body
// read or size problems are not rejections of the update's content, so they
// must record no publish failure.
func TestPublishHandlerDoesNotMeterNonContentRejections(t *testing.T) {
	t.Parallel()

	granted := []string{"https://example.com/books/1"}
	other := []string{"https://example.com/books/2"}

	for _, tc := range []struct {
		name       string
		options    []Option
		request    func() *http.Request
		wantStatus int
	}{
		{
			name: "401 without a token",
			request: func() *http.Request {
				req := newPublishRequest(granted, url.Values{"topic": granted})
				req.Header.Del("Authorization")

				return req
			},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "403 private update beyond the token scope",
			request: func() *http.Request {
				return newPublishRequest(granted, url.Values{"topic": other, "private": {"on"}})
			},
			wantStatus: http.StatusForbidden,
		},
		{
			name: "403 public update beyond the token scope",
			request: func() *http.Request {
				return newPublishRequest(granted, url.Values{"topic": other})
			},
			wantStatus: http.StatusForbidden,
		},
		{
			name:    "413 body over max_request_body_size",
			options: []Option{WithMaxRequestBodySize(16)},
			request: func() *http.Request {
				return newPublishRequest([]string{"*"}, url.Values{"topic": granted, "data": {strings.Repeat("x", 64)}})
			},
			wantStatus: http.StatusRequestEntityTooLarge,
		},
		{
			name: "400 body read error",
			request: func() *http.Request {
				req := newPublishRequest([]string{"*"}, nil)
				req.Body = io.NopCloser(iotest.ErrReader(io.ErrUnexpectedEOF))

				return req
			},
			wantStatus: http.StatusBadRequest,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rec := &recordingFailureMetrics{}
			hub := createDummy(t, append([]Option{WithMetrics(rec)}, tc.options...)...)

			w := httptest.NewRecorder()
			hub.PublishHandler(w, tc.request())

			resp := w.Result()

			t.Cleanup(func() { assert.NoError(t, resp.Body.Close()) })

			require.Equal(t, tc.wantStatus, resp.StatusCode)
			assert.Empty(t, rec.recorded())
		})
	}
}

// deadlineTransport fails Dispatch immediately with a deadline error of its
// own (e.g. a transport-internal dial timeout), not ctx's.
type deadlineTransport struct {
	nopTransport
}

func (deadlineTransport) Dispatch(context.Context, *Update) error {
	return fmt.Errorf("transport dial: %w", context.DeadlineExceeded)
}

// TestPublishHandlerTransportOwnDeadlineIsNotATimeout: with publish_timeout
// configured, a transport returning its own context.DeadlineExceeded before the
// publish timeout fires is a plain transport failure (500), not a 504.
func TestPublishHandlerTransportOwnDeadlineIsNotATimeout(t *testing.T) {
	t.Parallel()

	rec := &recordingFailureMetrics{}
	hub := createDummy(t, WithTransport(deadlineTransport{}), WithPublishTimeout(time.Hour), WithMetrics(rec))

	topics := []string{"https://example.com/books/1"}

	w := httptest.NewRecorder()
	hub.PublishHandler(w, newPublishRequest(topics, url.Values{"topic": topics, "data": {"Hello!"}}))

	resp := w.Result()

	t.Cleanup(func() { assert.NoError(t, resp.Body.Close()) })

	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	assert.Equal(t, []PublishFailureReason{PublishFailureReasonTransport}, rec.recorded())
}

type oversizeTransport struct {
	nopTransport
}

func (oversizeTransport) Dispatch(context.Context, *Update) error {
	return fmt.Errorf("transport: encoded update too large: %w", ErrCodecPayloadTooLarge)
}

// TestPublishHandlerOversizeUpdateIs413: an update the transport refuses as
// too large to store can never be published, so the hub answers 413, not a
// retryable 500, and meters it as a validation failure.
func TestPublishHandlerOversizeUpdateIs413(t *testing.T) {
	t.Parallel()

	rec := &recordingFailureMetrics{}
	hub := createDummy(t, WithTransport(oversizeTransport{}), WithMetrics(rec))

	topics := []string{"https://example.com/books/1"}

	w := httptest.NewRecorder()
	hub.PublishHandler(w, newPublishRequest(topics, url.Values{"topic": topics, "data": {"Hello!"}}))

	resp := w.Result()

	t.Cleanup(func() { assert.NoError(t, resp.Body.Close()) })

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	assert.Equal(t, http.StatusRequestEntityTooLarge, resp.StatusCode)
	assert.Equal(t, ErrCodecPayloadTooLarge.Error()+"\n", string(body))
	assert.Equal(t, []PublishFailureReason{PublishFailureReasonValidation}, rec.recorded())
}

// gatedTransport blocks Dispatch until proceed is closed, then records the
// context error it sees and the update it was handed.
type gatedTransport struct {
	nopTransport

	started chan struct{}
	proceed chan struct{}

	mu         sync.Mutex
	ctxErr     error
	dispatched *Update
}

func (g *gatedTransport) Dispatch(ctx context.Context, u *Update) error {
	close(g.started)
	<-g.proceed

	g.mu.Lock()
	defer g.mu.Unlock()

	g.ctxErr = ctx.Err()
	g.dispatched = u

	return nil
}

// TestPublishHandlerDispatchSurvivesPublisherDisconnect: without publish_timeout,
// a publisher whose request context is cancelled mid-dispatch does not abort the
// dispatch: the transport sees an uncancelled context, the update goes through
// and no failure is recorded.
func TestPublishHandlerDispatchSurvivesPublisherDisconnect(t *testing.T) {
	t.Parallel()

	rec := &recordingFailureMetrics{}
	tr := &gatedTransport{started: make(chan struct{}), proceed: make(chan struct{})}
	hub := createDummy(t, WithTransport(tr), WithMetrics(rec))

	topics := []string{"https://example.com/books/1"}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	req := newPublishRequest(topics, url.Values{"topic": topics, "data": {"Hello!"}}).WithContext(ctx)
	w := httptest.NewRecorder()
	done := make(chan struct{})

	go func() {
		defer close(done)

		hub.PublishHandler(w, req)
	}()

	<-tr.started
	cancel() // the publisher disconnects mid-dispatch
	close(tr.proceed)
	<-done

	tr.mu.Lock()
	defer tr.mu.Unlock()

	require.NoError(t, tr.ctxErr, "the dispatch context must not be cancelled by the publisher's disconnect")
	require.NotNil(t, tr.dispatched, "the update must be dispatched")
	assert.Equal(t, topics, tr.dispatched.Topics)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Empty(t, rec.recorded())
}

// TestPublishNoOpsWithoutPublishFailureReporter: a Metrics implementation without
// PublishFailureReporter does not panic when a publish fails.
func TestPublishNoOpsWithoutPublishFailureReporter(t *testing.T) {
	t.Parallel()

	if _, ok := any(NopMetrics{}).(PublishFailureReporter); ok {
		t.Fatal("NopMetrics must not implement PublishFailureReporter")
	}

	hub := createDummy(t, WithMetrics(NopMetrics{}))
	require.NoError(t, hub.transport.Close(t.Context()))

	assert.Error(t, hub.Publish(t.Context(), &Update{Topics: []string{"https://example.com/books/1"}}))
}
