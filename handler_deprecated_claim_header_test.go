//go:build deprecated_server

package mercure

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The deprecated server registers the same SubscribeHandler, so a mismatched bound header
// must still be rejected under that build. The reject path returns 401 immediately (unlike
// a matching subscribe, which would block on the SSE stream), so only that direction is
// exercised here — enough to prove enforcement is wired through chainHandlers.
func TestDeprecatedServerEnforcesClaimHeaderBindings(t *testing.T) {
	t.Parallel()

	h := createDummy(t, WithClaimHeaderBindings(mustBinding(t, "tenants", "Tenant-ID")))

	r := httptest.NewRequest(http.MethodGet, defaultHubURL+"?topic=https://example.com/x", nil)
	r.Header.Set("Authorization", bearerPrefix+createDummyJWTWithExtraClaims(t, roleSubscriber, []string{"*"}, map[string]any{"tenants": []string{"acme"}}))
	r.Header.Set("Tenant-ID", "evil")

	w := httptest.NewRecorder()
	h.chainHandlers().ServeHTTP(w, r)

	require.Equal(t, http.StatusUnauthorized, w.Code, "a mismatched bound header must 401 under the deprecated server")
}

// The deprecated server builds its CORS middleware in chainHandlers, a separate call site
// from initHandler. Its corsAllowedHeaders() change is invisible to the default build, so
// without this test a binding header would be silently missing from the preflight allowlist
// for anyone still running the deprecated server.
func TestDeprecatedServerCORSAllowsBoundHeaders(t *testing.T) {
	t.Parallel()

	preflight := func(tb testing.TB, h *Hub, requested string) string {
		tb.Helper()

		r := httptest.NewRequest(http.MethodOptions, defaultHubURL, nil)
		r.Header.Set("Origin", "https://example.com")
		r.Header.Set("Access-Control-Request-Method", http.MethodGet)
		r.Header.Set("Access-Control-Request-Headers", requested)

		w := httptest.NewRecorder()
		h.chainHandlers().ServeHTTP(w, r)

		resp := w.Result()
		defer resp.Body.Close()

		return resp.Header.Get("Access-Control-Allow-Headers")
	}

	// createDummy installs the deprecated server's viper config, which chainHandlers reads.
	newHub := func(tb testing.TB, bindings ...ClaimHeaderBinding) *Hub {
		tb.Helper()

		return createDummy(
			tb,
			WithClaimHeaderBindings(bindings...),
			WithCORSOrigins([]string{"https://example.com"}),
		)
	}

	t.Run("a bound header is allowed", func(t *testing.T) {
		t.Parallel()

		h := newHub(t, mustBinding(t, "tenants", "Tenant-ID"))

		assert.Contains(t, preflight(t, h, "authorization,tenant-id"), "tenant-id")
	})

	t.Run("without a binding the header stays disallowed", func(t *testing.T) {
		t.Parallel()

		assert.NotContains(t, preflight(t, newHub(t), "tenant-id"), "tenant-id")
	})

	t.Run("no bindings leaves the base list untouched", func(t *testing.T) {
		t.Parallel()

		base := "authorization,cache-control,last-event-id"

		assert.Equal(t, base, preflight(t, newHub(t), base))
	})
}
