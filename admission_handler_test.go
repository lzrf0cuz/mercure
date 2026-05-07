package mercure

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// rejectingAdmitter is a nopTransport that always sheds via the admission gate.
type rejectingAdmitter struct {
	nopTransport
}

func (rejectingAdmitter) TryAdmit(ctx context.Context) (context.Context, func(), error) {
	return ctx, nil, &AdmissionError{Reason: AdmissionCapacity, RetryAfter: 3 * time.Second}
}

// TestSubscribeHandlerAdmissionReject429: a transport that sheds via TryAdmit
// makes SubscribeHandler return 429 with an integer Retry-After header, before
// any authorization/allocation.
func TestSubscribeHandlerAdmissionReject429(t *testing.T) {
	t.Parallel()

	hub := createDummy(t, WithTransport(rejectingAdmitter{}))

	req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?topic=https://example.com/foo", nil)
	w := httptest.NewRecorder()

	hub.SubscribeHandler(w, req)

	resp := w.Result()

	t.Cleanup(func() { require.NoError(t, resp.Body.Close()) })

	assert.Equal(t, http.StatusTooManyRequests, resp.StatusCode)
	assert.Equal(t, "3", resp.Header.Get("Retry-After"), "Retry-After must be integer delay-seconds (ceil of 3s)")
}

// recordingAdmitter is a nopTransport whose TryAdmit admits and records whether
// its release closure was called.
type recordingAdmitter struct {
	nopTransport

	released atomic.Bool
}

func (a *recordingAdmitter) TryAdmit(ctx context.Context) (context.Context, func(), error) {
	return ctx, func() { a.released.Store(true) }, nil
}

// TestSubscribeHandlerAdmissionReleaseOnExit: a client that disconnected before
// registration is abandoned at registerSubscriber's `ctx.Err()` recheck (which
// requires auth to pass — hence the anonymous hub — so the recheck is actually
// reached, not the 401 path), and the admission slot is released via the
// deferred closure when the handler returns.
func TestSubscribeHandlerAdmissionReleaseOnExit(t *testing.T) {
	t.Parallel()

	adm := &recordingAdmitter{}
	hub := createAnonymousDummy(t, WithTransport(adm))

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?topic=https://example.com/foo", nil).WithContext(ctx)
	w := httptest.NewRecorder()

	hub.SubscribeHandler(w, req)

	resp := w.Result()

	t.Cleanup(func() { require.NoError(t, resp.Body.Close()) })

	// Anonymous auth passes; the cancelled ctx then trips the recheck (no 401,
	// no 429), and the slot is released on exit.
	assert.NotEqual(t, http.StatusUnauthorized, resp.StatusCode, "auth must pass so the ctx.Err recheck is reached")
	assert.NotEqual(t, http.StatusTooManyRequests, resp.StatusCode)
	assert.True(t, adm.released.Load(), "the admission slot must be released when the handler returns")
}
