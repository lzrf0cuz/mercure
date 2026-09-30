package mercure

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// rejectingAdmitter is a nopTransport whose TryAdmit always fails with err.
type rejectingAdmitter struct {
	nopTransport

	err error
}

func (a rejectingAdmitter) TryAdmit(ctx context.Context) (context.Context, func(), error) {
	return ctx, nil, a.err
}

// readTrackingBody records whether the request body was read.
type readTrackingBody struct {
	io.Reader

	read atomic.Bool
}

func (b *readTrackingBody) Read(p []byte) (int, error) {
	b.read.Store(true)

	return b.Reader.Read(p)
}

// TestSubscribeHandlerAdmissionReject: a transport that sheds via TryAdmit makes
// SubscribeHandler return 429 with an integer Retry-After header, before the
// QUERY body is read and before authorization/allocation; any other TryAdmit
// error (a closed transport) is a 503 without Retry-After, like a failed
// AddSubscriber.
func TestSubscribeHandlerAdmissionReject(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name           string
		err            error
		wantStatus     int
		wantRetryAfter string
	}{
		{
			name:           "shed",
			err:            &AdmissionError{Reason: AdmissionCapacity, RetryAfter: 3 * time.Second},
			wantStatus:     http.StatusTooManyRequests,
			wantRetryAfter: "3",
		},
		{
			name:           "shed with a fractional delay rounds up",
			err:            &AdmissionError{Reason: AdmissionRate, RetryAfter: 1500 * time.Millisecond},
			wantStatus:     http.StatusTooManyRequests,
			wantRetryAfter: "2",
		},
		{
			name:       "closed transport",
			err:        ErrClosedTransport,
			wantStatus: http.StatusServiceUnavailable,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// createDummy requires authentication: the unauthenticated request
			// proves admission runs before authorization.
			hub := createDummy(t, WithTransport(rejectingAdmitter{err: tc.err}))

			// A QUERY subscription: the gate must shed before its body is read.
			body := &readTrackingBody{Reader: strings.NewReader("match=https://example.com/foo")}
			req := httptest.NewRequest(methodQuery, defaultHubURL, body)
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

			w := httptest.NewRecorder()

			hub.SubscribeHandler(w, req)

			assert.False(t, body.read.Load(), "the QUERY request body must not be read before admission")

			resp := w.Result()

			t.Cleanup(func() { require.NoError(t, resp.Body.Close()) })

			assert.Equal(t, tc.wantStatus, resp.StatusCode)
			assert.Equal(t, http.StatusText(tc.wantStatus)+"\n", w.Body.String(), "a rejected request must stop at the gate")
			assert.Equal(t, tc.wantRetryAfter, resp.Header.Get("Retry-After"), "Retry-After must be integer delay-seconds, rounded up")
			assert.Equal(t, headerAcceptQuery, resp.Header.Values("Accept-Query"), "an admission refusal must advertise Accept-Query like every other answer")
		})
	}
}

type admittedMarkerKey struct{}

// recordingAdmitter is a LocalTransport whose TryAdmit admits with a marked
// context and records the subscribers the transport still held when the slot
// was released, and whether AddSubscriber saw the marker.
type recordingAdmitter struct {
	*LocalTransport

	mu            sync.Mutex
	markerSeen    bool
	releases      int
	heldAtRelease int
}

func (a *recordingAdmitter) TryAdmit(ctx context.Context) (context.Context, func(), error) {
	return context.WithValue(ctx, admittedMarkerKey{}, true), func() {
		a.RLock()
		held := a.subscribers.Len()
		a.RUnlock()

		a.mu.Lock()
		a.releases++
		a.heldAtRelease = held
		a.mu.Unlock()
	}, nil
}

func (a *recordingAdmitter) AddSubscriber(ctx context.Context, s *LocalSubscriber) error {
	a.mu.Lock()
	a.markerSeen, _ = ctx.Value(admittedMarkerKey{}).(bool)
	a.mu.Unlock()

	return a.LocalTransport.AddSubscriber(ctx, s)
}

// TestSubscribeHandlerAdmissionReleaseOnExit: the admitted context reaches
// AddSubscriber (so a transport can recognise a subscriber it already charged),
// and the admission slot is released exactly once when the handler returns,
// after the subscriber's transport teardown, so the slot is never free while
// the subscriber is still held.
func TestSubscribeHandlerAdmissionReleaseOnExit(t *testing.T) {
	t.Parallel()

	adm := &recordingAdmitter{LocalTransport: NewLocalTransport(NewSubscriberList(0))}
	hub := createAnonymousDummy(t, WithTransport(adm))

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?match=https://example.com/foo", nil).WithContext(ctx)

	// The client goes away as soon as the stream opens.
	hub.SubscribeHandler(&responseTester{expectedStatusCode: http.StatusOK, expectedBody: ":\n", cancel: cancel, tb: t}, req)

	adm.mu.Lock()
	defer adm.mu.Unlock()

	assert.True(t, adm.markerSeen, "AddSubscriber must receive the admitted context")
	assert.Equal(t, 1, adm.releases, "the admission slot must be released once")
	assert.Zero(t, adm.heldAtRelease, "the admission slot must be released after the transport teardown")
}

// TestSubscribeHandlerAdmissionReleaseOnEarlyReturn: a request refused after
// admission (here a 401 from registerSubscriber) must still release the
// admission slot exactly once: `defer release()` is deferred before
// registerSubscriber's early return.
func TestSubscribeHandlerAdmissionReleaseOnEarlyReturn(t *testing.T) {
	t.Parallel()

	adm := &recordingAdmitter{LocalTransport: NewLocalTransport(NewSubscriberList(0))}
	hub := createDummy(t, WithTransport(adm)) // authentication required

	w := httptest.NewRecorder()
	hub.SubscribeHandler(w, httptest.NewRequest(http.MethodGet, defaultHubURL+"?match=https://example.com/foo", nil))

	resp := w.Result()

	t.Cleanup(func() { require.NoError(t, resp.Body.Close()) })

	require.Equal(t, http.StatusUnauthorized, resp.StatusCode, "the request must be refused after admission")

	adm.mu.Lock()
	defer adm.mu.Unlock()

	assert.Equal(t, 1, adm.releases, "the admission slot must be released exactly once on an early return")
}

// A shed subscriber is told when to retry. A cross-origin fetch client can read that hint
// only if CORS exposes Retry-After.
func TestCORSExposesRetryAfterOnShed(t *testing.T) {
	t.Parallel()

	hub := createDummy(t,
		WithTransport(rejectingAdmitter{err: &AdmissionError{Reason: AdmissionCapacity, RetryAfter: 3 * time.Second}}),
		WithCORSOrigins([]string{"https://example.com"}),
	)

	req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?topic=https://example.com/x", nil)
	req.Header.Set("Origin", "https://example.com")

	w := httptest.NewRecorder()
	hub.ServeHTTP(w, req)

	resp := w.Result()
	require.NoError(t, resp.Body.Close())

	require.Equal(t, http.StatusTooManyRequests, resp.StatusCode)
	assert.Equal(t, "3", resp.Header.Get("Retry-After"))
	assert.Equal(t, "https://example.com", resp.Header.Get("Access-Control-Allow-Origin"))
	assert.Contains(t, strings.Split(resp.Header.Get("Access-Control-Expose-Headers"), ", "), "Retry-After")
}
