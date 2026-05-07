//go:build !dev_ui

package mercure

import (
	"embed"
	"io/fs"
	"log/slog"
)

//go:embed public
var embedFS embed.FS

// publicFS returns the embedded `public/` tree (the demo UI's root).
// Production builds go through this path.
//
// The logger parameter is unused here; the dev_ui sibling uses it to
// emit a Warn when MERCURE_PUBLIC_DIR is misconfigured. Symmetric
// signature across build tags simplifies callers.
//
// fs.Sub on a //go:embed FS with a hardcoded valid pattern cannot fail
// at runtime — the panic is a programmer-bug assertion, not an error
// path.
func publicFS(_ *slog.Logger) fs.FS {
	sub, err := fs.Sub(embedFS, "public")
	if err != nil {
		panic(err)
	}

	return sub
}
