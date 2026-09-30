//go:build dev_ui

package mercure

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDevUIDiskChanges(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("MERCURE_PUBLIC_DIR", dir)
	index := filepath.Join(dir, "index.html")
	require.NoError(t, os.WriteFile(index, []byte("first"), 0o600))

	hub := createAnonymousDummy(t, WithDebugger())
	for _, body := range []string{"first", "edited without rebuilding"} {
		require.NoError(t, os.WriteFile(index, []byte(body), 0o600))

		w := httptest.NewRecorder()
		hub.ServeHTTP(w, httptest.NewRequest(http.MethodGet, defaultDebugURL, nil))
		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, body, w.Body.String())
	}
}

func TestDevUIMissingDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "missing")
	t.Setenv("MERCURE_PUBLIC_DIR", dir)

	var log bytes.Buffer

	logger := slog.New(slog.NewTextHandler(&log, nil))
	handler := http.FileServer(http.FS(publicFS(logger)))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))
	assert.Equal(t, http.StatusNotFound, w.Code)
	assert.Contains(t, log.String(), "MERCURE_PUBLIC_DIR not accessible")
}
