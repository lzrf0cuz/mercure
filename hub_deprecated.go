//go:build deprecated_server

package mercure

import (
	"net/http"
	"sync/atomic"

	"github.com/spf13/viper"
)

// Deprecated: use the Caddy server module or the standalone library instead.
//
// server / metricsServer are atomic.Pointer because Serve writes them in one
// goroutine and tests (plus the Serve goroutine's own signal-listener child)
// read them in others. The HTTP-protocol "server is ready when client.Get
// returns" handshake does not establish a Go memory-order happens-before, so
// the race detector flagged plain *http.Server reads. The atomic Load/Store
// is a small surface change in the test/internal-shutdown paths.
type deprecatedHub struct {
	config        *viper.Viper
	server        atomic.Pointer[http.Server]
	metricsServer atomic.Pointer[http.Server]
}
