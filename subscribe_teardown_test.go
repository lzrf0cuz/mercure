package mercure

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recordingLogHandler keeps every record at every level. WithAttrs and
// WithGroup return the handler unchanged, dropping their attributes.
type recordingLogHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (*recordingLogHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *recordingLogHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.records = append(h.records, r.Clone())

	return nil
}

func (h *recordingLogHandler) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h *recordingLogHandler) WithGroup(string) slog.Handler { return h }

// find returns the records logged at level with message msg.
func (h *recordingLogHandler) find(level slog.Level, msg string) []slog.Record {
	h.mu.Lock()
	defer h.mu.Unlock()

	var found []slog.Record

	for _, r := range h.records {
		if r.Level == level && r.Message == msg {
			found = append(found, r)
		}
	}

	return found
}

func (h *recordingLogHandler) messages() []string {
	h.mu.Lock()
	defer h.mu.Unlock()

	messages := make([]string, 0, len(h.records))
	for _, r := range h.records {
		messages = append(messages, r.Level.String()+" "+r.Message)
	}

	return messages
}

// withError returns the records logged at level whose "error" attribute
// matches target (errors.Is).
func (h *recordingLogHandler) withError(level slog.Level, target error) []slog.Record {
	h.mu.Lock()
	defer h.mu.Unlock()

	var found []slog.Record

	for _, r := range h.records {
		if err, ok := attrValue(r, "error").(error); ok && r.Level == level && errors.Is(err, target) {
			found = append(found, r)
		}
	}

	return found
}

// attrValue returns the value of r's first top-level attribute named key, or nil.
func attrValue(r slog.Record, key string) any {
	var v any

	r.Attrs(func(a slog.Attr) bool {
		if a.Key != key {
			return true
		}

		v = a.Value.Any()

		return false
	})

	return v
}

// attrValues returns the values of r's top-level attributes named key.
func attrValues(r slog.Record, key string) []any {
	var values []any

	r.Attrs(func(a slog.Attr) bool {
		if a.Key == key {
			values = append(values, a.Value.Any())
		}

		return true
	})

	return values
}

// ownClosedTransportError is a transport's own closed error that unwraps to
// ErrClosedTransport, as redistransport's does.
type ownClosedTransportError struct{}

func (ownClosedTransportError) Error() string { return "own transport: closed" }

func (ownClosedTransportError) Unwrap() error { return ErrClosedTransport }

// admittingRecordingTransport is a LocalTransport that records dispatched
// updates and admits every subscriber, recording how many subscribers it still
// held when the admission slot was released. A non-nil addErr makes
// AddSubscriber fail; closeAfterRemove closes the transport once
// RemoveSubscriber succeeded, so the withdrawal that follows fails.
type admittingRecordingTransport struct {
	*LocalTransport

	mu               sync.Mutex
	addErr           error
	closeAfterRemove bool
	adds             int
	dispatched       []*Update
	releases         int
	heldAtRelease    int
}

func (t *admittingRecordingTransport) RemoveSubscriber(ctx context.Context, s *LocalSubscriber) error {
	if err := t.LocalTransport.RemoveSubscriber(ctx, s); err != nil {
		return err
	}

	if t.closeAfterRemove {
		return t.Close(ctx)
	}

	return nil
}

func (t *admittingRecordingTransport) AddSubscriber(ctx context.Context, s *LocalSubscriber) error {
	t.mu.Lock()
	t.adds++
	err := t.addErr
	t.mu.Unlock()

	if err != nil {
		return err
	}

	return t.LocalTransport.AddSubscriber(ctx, s)
}

// Dispatch records u only once the inner Dispatch succeeded, so a withdrawal
// that failed cannot count as sent.
func (t *admittingRecordingTransport) Dispatch(ctx context.Context, u *Update) error {
	if err := t.LocalTransport.Dispatch(ctx, u); err != nil {
		return err
	}

	t.mu.Lock()
	t.dispatched = append(t.dispatched, u)
	t.mu.Unlock()

	return nil
}

func (t *admittingRecordingTransport) TryAdmit(ctx context.Context) (context.Context, func(), error) {
	return ctx, func() {
		t.RLock()
		held := t.subscribers.Len()
		t.RUnlock()

		t.mu.Lock()
		t.releases++
		t.heldAtRelease = held
		t.mu.Unlock()
	}, nil
}

// Once AddSubscriber succeeded, SubscribeHandler leaves through shutdown: the
// subscriber removed, its active:true withdrawn by an active:false, a connect
// and a disconnect reported once each, and the admission slot released only
// after that transport teardown. A client leaving during the handshake (the
// first write) is such an exit. A failed AddSubscriber announces nothing and
// reports neither a connect nor a disconnect.
func TestSubscribeTearsDownSubscriber(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name             string
		addErr           error
		closeAfterRemove bool
		wantActive       []bool
		wantConnected    int
		wantReasons      []DisconnectReason
	}{
		{
			name:          "client leaves during the handshake",
			wantActive:    []bool{true, false},
			wantConnected: 1,
			wantReasons:   []DisconnectReason{DisconnectReasonClientClosed},
		},
		{
			// The transport closes after removing the subscriber, so the
			// active:false Dispatch fails and must not count as sent.
			name:             "withdrawal fails during teardown",
			closeAfterRemove: true,
			wantActive:       []bool{true},
			wantConnected:    1,
			wantReasons:      []DisconnectReason{DisconnectReasonClientClosed},
		},
		{
			name:          "AddSubscriber error",
			addErr:        errFailedToAddSubscriber,
			wantActive:    []bool{},
			wantConnected: 0,
			wantReasons:   nil,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			transport := &admittingRecordingTransport{
				LocalTransport:   NewLocalTransport(NewSubscriberList(0)),
				addErr:           tc.addErr,
				closeAfterRemove: tc.closeAfterRemove,
			}
			metrics := &recordingMetrics{}
			hub := createAnonymousDummy(t, WithSubscriptions(), WithTransport(transport), WithMetrics(metrics))

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			req := httptest.NewRequestWithContext(ctx, http.MethodGet, defaultHubURL+"?match=https://example.com/foo", nil)

			var w http.ResponseWriter = httptest.NewRecorder()
			if tc.addErr == nil {
				// The client goes away on the first bytes of the stream.
				w = &responseTester{expectedStatusCode: http.StatusOK, expectedBody: ":\n", cancel: cancel, tb: t}
			}

			hub.SubscribeHandler(w, req)

			transport.RLock()
			held := transport.subscribers.Len()
			transport.RUnlock()

			assert.Zero(t, held, "the subscriber must not stay in the transport")

			transport.mu.Lock()
			defer transport.mu.Unlock()

			assert.Equal(t, 1, transport.adds, "the request must reach AddSubscriber")

			active := make([]bool, 0, len(transport.dispatched))

			for _, u := range transport.dispatched {
				var subscription struct {
					Active bool `json:"active"`
				}

				require.NoError(t, json.Unmarshal([]byte(u.Data), &subscription))

				active = append(active, subscription.Active)
			}

			assert.Equal(t, tc.wantActive, active, "subscription updates")
			assert.Equal(t, tc.wantConnected, metrics.connects(), "SubscriberConnected calls")
			assert.Equal(t, tc.wantReasons, metrics.reasons(), "disconnects reported by shutdown")
			assert.Zero(t, metrics.plainDisconnects())

			assert.Equal(t, 1, transport.releases, "the admission slot must be released once")
			assert.Zero(t, transport.heldAtRelease, "the admission slot must be released after the transport teardown")
		})
	}
}

// The hub's log lines about a subscriber, "New subscriber" included, carry
// it once, through the log handler.
func TestSubscriberLogAttributeAppearsOnce(t *testing.T) {
	t.Parallel()

	logs := &recordingLogHandler{}
	hub := createAnonymousDummy(t, WithLogger(slog.New(NewSlogHandler(logs))))

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	req := httptest.NewRequestWithContext(ctx, http.MethodGet, defaultHubURL+"?match=https://example.com/foo", nil)

	hub.SubscribeHandler(&responseTester{expectedStatusCode: http.StatusOK, expectedBody: ":\n", cancel: cancel, tb: t}, req)

	for _, msg := range []string{"New subscriber", "Subscriber disconnected"} {
		records := logs.find(slog.LevelInfo, msg)
		require.Len(t, records, 1, "%q; records: %v", msg, logs.messages())
		assert.Len(t, attrValues(records[0], "subscriber"), 1, "%q must carry the subscriber attribute once", msg)
	}
}

// The debug-level "New subscriber" line, which adds the subscription
// payloads, names the subscriber once too.
func TestNewSubscriberLogWithClaimsNamesSubscriberOnce(t *testing.T) {
	t.Parallel()

	logs := &recordingLogHandler{}
	hub := createDummy(t, WithLogger(slog.New(NewSlogHandler(logs))))

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	req := httptest.NewRequestWithContext(ctx, http.MethodGet, defaultHubURL+"?match=https://example.com/foo", nil)
	req.Header.Add("Authorization", bearerPrefix+createDummyAuthorizedJWT(roleSubscriber, []string{"https://example.com/foo"}))

	hub.SubscribeHandler(&responseTester{expectedStatusCode: http.StatusOK, expectedBody: ":\n", cancel: cancel, tb: t}, req)

	records := logs.find(slog.LevelInfo, "New subscriber")
	require.Len(t, records, 1, "records: %v", logs.messages())
	require.Len(t, attrValues(records[0], "payload"), 1, "the claims branch must have been taken")
	assert.Len(t, attrValues(records[0], "subscriber"), 1, "the subscriber attribute must appear once")
}

// loggingAddTransport is a LocalTransport whose AddSubscriber logs a line
// naming the subscriber itself, as a transport does.
type loggingAddTransport struct {
	*LocalTransport

	logger *slog.Logger
}

func (t *loggingAddTransport) AddSubscriber(ctx context.Context, s *LocalSubscriber) error {
	t.logger.LogAttrs(ctx, slog.LevelInfo, "Transport adding subscriber", slog.Any("subscriber", &s.Subscriber))

	return t.LocalTransport.AddSubscriber(ctx, s)
}

// The context AddSubscriber gets does not carry the subscriber, so a line
// the transport logs about it names it once.
func TestTransportAddSubscriberLogNamesSubscriberOnce(t *testing.T) {
	t.Parallel()

	logs := &recordingLogHandler{}
	logger := slog.New(NewSlogHandler(logs))
	transport := &loggingAddTransport{LocalTransport: NewLocalTransport(NewSubscriberList(0)), logger: logger}
	hub := createAnonymousDummy(t, WithTransport(transport), WithLogger(logger))

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	req := httptest.NewRequestWithContext(ctx, http.MethodGet, defaultHubURL+"?match=https://example.com/foo", nil)

	hub.SubscribeHandler(&responseTester{expectedStatusCode: http.StatusOK, expectedBody: ":\n", cancel: cancel, tb: t}, req)

	records := logs.find(slog.LevelInfo, "Transport adding subscriber")
	require.Len(t, records, 1, "records: %v", logs.messages())
	assert.Len(t, attrValues(records[0], "subscriber"), 1, "the subscriber attribute must appear once")
}

// A closed transport refusing the removal is not a failed removal: no ERROR,
// and the exit is not reclassified as transport_error.
func TestShutdownIgnoresClosedTransportOnRemove(t *testing.T) {
	t.Parallel()

	metrics := &recordingMetrics{}
	logs := &recordingLogHandler{}
	hub := hubShutdownTestHubWithOptions(t.Context(), t, 0, WithMetrics(metrics), WithLogger(slog.New(logs)))
	hub.transport = &errorRemovingTransport{
		LocalTransport: hub.transport.(*LocalTransport),
		removeErr:      fmt.Errorf("wrapped: %w", ErrClosedTransport),
	}

	hub.shutdown(t.Context(), NewLocalSubscriber("", slog.Default(), hub.topicMatcherStore), DisconnectReasonUnknown)

	assert.Equal(t, []DisconnectReason{DisconnectReasonUnknown}, metrics.reasons())
	assert.Empty(t, logs.withError(slog.LevelError, ErrClosedTransport), "records: %v", logs.messages())
}

var errDispatchFailed = errors.New("dispatch failed")

// dispatchErrorTransport is a LocalTransport whose Dispatch always fails with err.
type dispatchErrorTransport struct {
	*LocalTransport

	err error
}

func (t *dispatchErrorTransport) Dispatch(context.Context, *Update) error {
	return t.err
}

// assertLoggedOnceAt asserts msg was logged once at want, and not at the other
// levels of Debug, Info and Error.
func assertLoggedOnceAt(t *testing.T, logs *recordingLogHandler, msg string, want slog.Level) {
	t.Helper()

	for _, level := range []slog.Level{slog.LevelDebug, slog.LevelInfo, slog.LevelError} {
		n := 0
		if level == want {
			n = 1
		}

		assert.Len(t, logs.find(level, msg), n, "%q at %s; records: %v", msg, level, logs.messages())
	}
}

// A subscription update a closed transport refuses is logged at Debug; any
// other dispatch failure stays at ERROR.
func TestSubscriptionUpdateDispatchFailureLogLevel(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		err       error
		wantLevel slog.Level
	}{
		{name: "closed transport", err: ownClosedTransportError{}, wantLevel: slog.LevelDebug},
		{name: "other error", err: errDispatchFailed, wantLevel: slog.LevelError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			logs := &recordingLogHandler{}
			transport := &dispatchErrorTransport{LocalTransport: NewLocalTransport(NewSubscriberList(0)), err: tc.err}
			hub := createAnonymousDummy(t, WithSubscriptions(), WithTransport(transport), WithLogger(slog.New(logs)))

			s := NewLocalSubscriber("", slog.Default(), hub.topicMatcherStore)
			s.setMatchers(stringsToExactMatchers([]string{"https://example.com/foo"}), nil)

			hub.dispatchSubscriptionUpdate(t.Context(), s, false)

			assertLoggedOnceAt(t, logs, "Failed to dispatch update", tc.wantLevel)
		})
	}
}

// An AddSubscriber refused by a closed transport is logged at Debug, and the
// RemoveSubscriber the closed transport then refuses is not logged at all;
// any other add failure stays at ERROR.
func TestAddSubscriberErrorLogLevel(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		addErr    error
		closed    bool
		wantLevel slog.Level
	}{
		{name: "closed transport", closed: true, wantLevel: slog.LevelDebug},
		{name: "transport's own closed error", addErr: ownClosedTransportError{}, wantLevel: slog.LevelDebug},
		{name: "other error", addErr: errFailedToAddSubscriber, wantLevel: slog.LevelError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			logs := &recordingLogHandler{}
			transport := &admittingRecordingTransport{LocalTransport: NewLocalTransport(NewSubscriberList(0)), addErr: tc.addErr}
			hub := createAnonymousDummy(t, WithTransport(transport), WithLogger(slog.New(logs)))

			if tc.closed {
				require.NoError(t, transport.Close(t.Context()))
			}

			req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?match=https://example.com/foo", nil)
			w := httptest.NewRecorder()

			hub.SubscribeHandler(w, req)

			assert.Equal(t, http.StatusServiceUnavailable, w.Code)
			assertLoggedOnceAt(t, logs, "Unable to add subscriber", tc.wantLevel)
			assert.Empty(t, logs.find(slog.LevelError, "Failed to remove subscriber after a failed registration"), "records: %v", logs.messages())
		})
	}
}

// A subscription update goes to other subscribers. A line the transport logs
// about one of them (a backpressure disconnect) must not name the subscriber
// whose subscription the update announces.
func TestSubscriptionUpdateBackpressureLogDoesNotNameTheAnnouncedSubscriber(t *testing.T) {
	t.Parallel()

	hub := createAnonymousDummy(t, WithSubscriptions())

	logs := &recordingLogHandler{}
	watcher := NewLocalSubscriber("", slog.New(NewSlogHandler(logs)), hub.topicMatcherStore, withOutBuffer(MinSubscriberOutBuffer))
	watcher.setMatchers(stringsToExactMatchers([]string{"*"}), stringsToExactMatchers([]string{"*"}))
	require.NoError(t, hub.transport.AddSubscriber(t.Context(), watcher))

	// Leave room for the active:true only, so the active:false overflows.
	for range MinSubscriberOutBuffer - 1 {
		require.True(t, watcher.Dispatch(t.Context(), &Update{}, true))
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	req := httptest.NewRequestWithContext(ctx, http.MethodGet, defaultHubURL+"?match=https://example.com/foo", nil)

	hub.SubscribeHandler(&responseTester{expectedStatusCode: http.StatusOK, expectedBody: ":\n", cancel: cancel, tb: t}, req)

	records := logs.find(slog.LevelInfo, "Subscriber unable to receive updates fast enough")
	require.Len(t, records, 1, "records: %v", logs.messages())

	for _, v := range attrValues(records[0], "subscriber") {
		assert.Same(t, &watcher.Subscriber, v, "the line may name only the subscriber it disconnected")
	}
}

// removeFailingRecordingTransport is a LocalTransport whose RemoveSubscriber
// fails and whose Dispatch records the update.
type removeFailingRecordingTransport struct {
	*LocalTransport

	mu         sync.Mutex
	dispatched []*Update
}

func (*removeFailingRecordingTransport) RemoveSubscriber(context.Context, *LocalSubscriber) error {
	return errTransportOffline
}

func (t *removeFailingRecordingTransport) Dispatch(_ context.Context, u *Update) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.dispatched = append(t.dispatched, u)

	return nil
}

// A removal that fails with a real error still dispatches active:false: it
// reports the client connection ending, which it did.
func TestShutdownDispatchesActiveFalseWhenRemovalFails(t *testing.T) {
	t.Parallel()

	transport := &removeFailingRecordingTransport{LocalTransport: NewLocalTransport(NewSubscriberList(0))}
	metrics := &recordingMetrics{}
	hub := createAnonymousDummy(t, WithSubscriptions(), WithTransport(transport), WithMetrics(metrics), WithLogger(slog.New(&recordingLogHandler{})))

	s := NewLocalSubscriber("", slog.Default(), hub.topicMatcherStore)
	s.setMatchers(stringsToExactMatchers([]string{"https://example.com/foo"}), nil)

	hub.shutdown(t.Context(), s, DisconnectReasonUnknown)

	transport.mu.Lock()
	defer transport.mu.Unlock()

	require.Len(t, transport.dispatched, 1, "active:false must be dispatched")

	var subscription struct {
		Active *bool `json:"active"`
	}

	require.NoError(t, json.Unmarshal([]byte(transport.dispatched[0].Data), &subscription))
	require.NotNil(t, subscription.Active)
	assert.False(t, *subscription.Active)
	assert.Equal(t, []DisconnectReason{DisconnectReasonTransportError}, metrics.reasons())
}

// listingFailingTransport lists the subscriber in the inner LocalTransport,
// then fails AddSubscriber: a transport that failed after registering it.
type listingFailingTransport struct {
	*LocalTransport
}

func (t *listingFailingTransport) AddSubscriber(ctx context.Context, s *LocalSubscriber) error {
	if err := t.LocalTransport.AddSubscriber(ctx, s); err != nil {
		return err
	}

	return errFailedToAddSubscriber
}

// A subscriber the transport listed before AddSubscriber failed must not stay
// listed: the hub removes it when it abandons the request.
func TestFailedAddSubscriberRemovesListedSubscriber(t *testing.T) {
	t.Parallel()

	transport := &listingFailingTransport{LocalTransport: NewLocalTransport(NewSubscriberList(0))}
	hub := createAnonymousDummy(t, WithTransport(transport))

	w := httptest.NewRecorder()
	hub.SubscribeHandler(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, defaultHubURL+"?match=https://example.com/foo", nil))

	assert.Equal(t, http.StatusServiceUnavailable, w.Code)

	transport.RLock()
	held := transport.subscribers.Len()
	transport.RUnlock()

	assert.Zero(t, held, "the subscriber the failed AddSubscriber listed must be removed")
}
