package mercure

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUIFixturesGating(t *testing.T) {
	t.Parallel()

	// Paths the gate must block when debugger:true is set without playground:true.
	// Each one resolves to public/fixtures/* via mux's UseEncodedPath +
	// http.FileServer's internal decode/clean, so a plain PathPrefix matcher
	// against the raw URI would let them through.
	uiBlocked := []struct {
		name string
		path string
	}{
		{"canonical", defaultDebugURL + "fixtures/jwks.json"},
		{"encoded f", defaultDebugURL + "%66ixtures/jwks.json"},
		{"encoded slash", defaultDebugURL + "fixtures%2Fjwks.json"},
		{"double slash", defaultDebugURL + "/fixtures/jwks.json"},
		{"dot-dot traversal", defaultDebugURL + "x/../fixtures/jwks.json"},
		{"directory no-slash", defaultDebugURL + "fixtures"},
		{"sensitive file", defaultDebugURL + "fixtures/private-jwk.json"},
		// The embedded FS is case-sensitive, so these reach a file only under
		// dev_ui (os.DirFS) on a case-insensitive filesystem such as macOS.
		{"upper case", defaultDebugURL + "FIXTURES/jwks.json"},
		{"mixed case", defaultDebugURL + "Fixtures/private-jwk.json"},
	}

	for _, tc := range uiBlocked {
		t.Run("ui blocks "+tc.name, func(t *testing.T) {
			t.Parallel()

			hub := createAnonymousDummy(t, WithDebugger())

			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, tc.path, nil)
			hub.ServeHTTP(w, req)

			assert.Equal(t, http.StatusNotFound, w.Code, "path=%s", tc.path)
		})
	}

	t.Run("playground serves fixtures", func(t *testing.T) {
		t.Parallel()

		hub := createAnonymousDummy(t, WithPlayground())

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, defaultDebugURL+"fixtures/jwks.json", nil)
		hub.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
	})

	// The UI root stays reachable in both gating modes: the gate only narrows
	// the fixtures subtree.
	for _, tc := range []struct {
		name   string
		option Option
	}{
		{"ui only", WithDebugger()},
		{"playground", WithPlayground()},
	} {
		t.Run("ui root reachable: "+tc.name, func(t *testing.T) {
			t.Parallel()

			hub := createAnonymousDummy(t, tc.option)

			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, defaultDebugURL, nil)
			hub.ServeHTTP(w, req)

			assert.Equal(t, http.StatusOK, w.Code)
		})
	}
}
