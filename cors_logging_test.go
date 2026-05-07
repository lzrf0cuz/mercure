package mercure

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCORSDebugTracesRouteThroughSlog verifies that rs/cors's debug traces
// (preflight decisions, etc. — emitted only when the hub is in debug mode) go
// through the hub's structured slog at Debug level, NOT rs/cors's default
// "[cors] " stdout logger. Without this, debug-mode CORS output bypasses the
// hub's logging config and lands as unleveled "[cors] " output on stdout.
func TestCORSDebugTracesRouteThroughSlog(t *testing.T) {
	t.Parallel()

	buf := &bytes.Buffer{}
	logger := slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	h, err := NewHub(
		t.Context(),
		WithLogger(logger),
		WithCORSOrigins([]string{"https://example.com"}),
		WithSubscriberJWT([]byte("subscriber"), "HS256"),
		WithPublisherJWT([]byte("publisher"), "HS256"),
		WithDebug(),
	)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodOptions, defaultHubURL, nil)
	req.Header.Add("Origin", "https://example.com")
	req.Header.Add("Access-Control-Request-Method", http.MethodGet)
	h.ServeHTTP(httptest.NewRecorder(), req)

	out := buf.String()
	assert.Contains(t, out, "msg=cors", "rs/cors debug traces must route through the hub's slog")
	assert.Contains(t, out, "level=DEBUG", "cors traces must be logged at Debug level")
	assert.Contains(t, out, "detail=", "the rs/cors trace text must be carried in the detail attribute")
	assert.Contains(t, out, "Preflight", "the real rs/cors preflight trace content must flow through, not an empty detail")
}

// TestCORSNonDebugEmitsNothingToHubLogger locks the `if h.debug` guard: without
// debug mode, rs/cors installs no logger and emits nothing (Debug false), so no
// cors traces reach the hub's logger. Guards against a regression that wired the
// adapter unconditionally (rs/cors's logf is gated on Log != nil, not Debug).
func TestCORSNonDebugEmitsNothingToHubLogger(t *testing.T) {
	t.Parallel()

	buf := &bytes.Buffer{}
	logger := slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	h, err := NewHub(
		t.Context(),
		WithLogger(logger),
		WithCORSOrigins([]string{"https://example.com"}),
		WithSubscriberJWT([]byte("subscriber"), "HS256"),
		WithPublisherJWT([]byte("publisher"), "HS256"),
		// deliberately no WithDebug()
	)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodOptions, defaultHubURL, nil)
	req.Header.Add("Origin", "https://example.com")
	req.Header.Add("Access-Control-Request-Method", http.MethodGet)
	h.ServeHTTP(httptest.NewRecorder(), req)

	assert.NotContains(t, buf.String(), "msg=cors", "non-debug mode must not route cors traces to the hub logger")
}
