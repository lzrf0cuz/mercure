//go:build dev_ui

package mercure

import (
	"io/fs"
	"log/slog"
	"os"
)

// publicFS returns the on-disk `public/` tree so JS/CSS edits are live
// without a hub rebuild. Path comes from MERCURE_PUBLIC_DIR (default
// `public`, resolved relative to the process cwd). A dev-runner is
// expected to set the env var relative to its own cwd if cwd doesn't
// already contain `public/`.
//
// If the directory doesn't exist or isn't accessible, we log a Warn
// at first call. UI requests then 404 against an empty FS, and the
// operator sees a clear "MERCURE_PUBLIC_DIR=… not accessible" hint in
// the log instead of silent UI breakage. The Warn routes through the
// caller-supplied logger so it lands in Caddy's structured log sink
// (configured at the hub module's Provision step) alongside every
// other hub log line. Defaulting to slog.Default() emits raw text on
// stderr and bypasses any JSON / sink configuration the operator set up.
//
// Caveat: `os.DirFS` returns real `ModTime` for served files, while
// the production `embed.FS` path returns zero `ModTime`. Browsers
// therefore see `Last-Modified` / `If-Modified-Since` 304 negotiation
// in the dev path but always-200 in production. Cache-related debugging
// done in dev does NOT exercise the production cache shape — re-test
// against the production build before claiming a cache fix lands.
func publicFS(logger *slog.Logger) fs.FS {
	if logger == nil {
		logger = slog.Default()
	}

	dir := os.Getenv("MERCURE_PUBLIC_DIR")
	if dir == "" {
		dir = "public"
	}

	// gosec G703: dev_ui is opt-in via build tag and operator-supplied
	// env var. Validating the path *is* the goal — turning a typo into
	// a Warn instead of silent 404s. Trust contract: the developer
	// running `task dev:redis` chose this path.
	if _, err := os.Stat(dir); err != nil { //nolint:gosec
		logger.Warn(
			"dev_ui: MERCURE_PUBLIC_DIR not accessible — UI requests will 404",
			slog.String("dir", dir),
			slog.Any("error", err),
		)
	}

	return os.DirFS(dir) //nolint:gosec // see Stat comment above; same trust contract.
}
