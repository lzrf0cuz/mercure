//go:build dev_ui

package mercure

import (
	"io/fs"
	"log/slog"
	"os"
)

// publicFS serves live debugger assets from the developer's public directory.
// MERCURE_PUBLIC_DIR is relative to the process cwd; it defaults to public.
// Disk files have modification times, unlike the production embedded assets.
func publicFS(logger *slog.Logger) fs.FS {
	if logger == nil {
		logger = slog.Default()
	}

	dir := os.Getenv("MERCURE_PUBLIC_DIR")
	if dir == "" {
		dir = "public"
	}

	public := os.DirFS(dir)
	if _, err := fs.Stat(public, "."); err != nil {
		logger.Warn("dev_ui: MERCURE_PUBLIC_DIR not accessible; UI requests will 404",
			slog.String("dir", dir), slog.Any("error", err))
	}

	return public
}
