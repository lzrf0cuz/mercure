//go:build dev_ui

package mercure

import (
	"bytes"
	"io/fs"
	"log/slog"
	"strings"
	"testing"
)

// newCaptureLogger returns a slog.Logger that writes WARN-and-above
// records to the returned buffer. Used to assert publicFS() emits the
// "MERCURE_PUBLIC_DIR not accessible" Warn through the caller-supplied
// logger (matching the production path where Caddy injects its own
// structured logger).
func newCaptureLogger() (*slog.Logger, *bytes.Buffer) {
	var buf bytes.Buffer

	return slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})), &buf
}

func TestPublicFS_DevUI_WarnsOnMissingDir(t *testing.T) {
	t.Setenv("MERCURE_PUBLIC_DIR", "/this/path/does/not/exist/xyz")

	logger, buf := newCaptureLogger()

	got := publicFS(logger)
	if got == nil {
		t.Fatal("publicFS() returned nil; expected an os.DirFS handle even when the dir is missing")
	}

	out := buf.String()
	if !strings.Contains(out, "MERCURE_PUBLIC_DIR not accessible") {
		t.Errorf("expected Warn about missing MERCURE_PUBLIC_DIR; got: %q", out)
	}

	if !strings.Contains(out, "/this/path/does/not/exist/xyz") {
		t.Errorf("expected Warn to include the offending dir; got: %q", out)
	}
}

func TestPublicFS_DevUI_DefaultsToPublicWhenEnvUnset(t *testing.T) {
	t.Setenv("MERCURE_PUBLIC_DIR", "")

	logger, buf := newCaptureLogger()

	got := publicFS(logger)
	if got == nil {
		t.Fatal("publicFS() returned nil")
	}

	// The test runs from the repo root (where ./public exists). No Warn
	// should appear since the default path resolves cleanly.
	if strings.Contains(buf.String(), "MERCURE_PUBLIC_DIR not accessible") {
		t.Errorf("did not expect Warn when MERCURE_PUBLIC_DIR is unset and ./public is present; got: %q", buf.String())
	}

	// Sanity: the returned FS should resolve files under public/.
	// `public/index.html` is the canonical demo entry point.
	if _, err := fs.Stat(got, "index.html"); err != nil {
		t.Errorf("expected to stat public/index.html via dev_ui FS; got error: %v", err)
	}
}

func TestPublicFS_DevUI_NilLoggerFallsBackToDefault(t *testing.T) {
	t.Setenv("MERCURE_PUBLIC_DIR", "/this/path/also/does/not/exist")

	// Pinning the slog.Default() so the nil-fallback path doesn't emit
	// to stderr during the test run; we don't assert on the output here,
	// only that the function tolerates nil and doesn't panic.
	old := slog.Default()
	defer slog.SetDefault(old)

	slog.SetDefault(slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)))

	got := publicFS(nil)
	if got == nil {
		t.Fatal("publicFS(nil) returned nil; expected an os.DirFS handle")
	}
}
