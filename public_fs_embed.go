//go:build !dev_ui

package mercure

import (
	"embed"
	"io/fs"
	"log/slog"
)

//go:embed public
var debuggerContent embed.FS

// publicFS serves the embedded debugger assets in production builds.
func publicFS(_ *slog.Logger) fs.FS {
	public, err := fs.Sub(debuggerContent, "public")
	if err != nil {
		panic(err)
	}

	return public
}
