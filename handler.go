package mercure

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"path"
	"strings"

	"github.com/gorilla/mux"
	"github.com/rs/cors"
	"github.com/unrolled/secure"
)

const (
	defaultHubURL  = "/.well-known/mercure"
	defaultUIURL   = defaultHubURL + "/ui/"
	defaultDemoURL = defaultUIURL + "demo/"
)

// initHandler wires the mux, CSP, secure middleware, CORS, and (when
// enabled) the demo + UI/fixtures gating. Length and nesting are inherent
// to a single setup function that has to span all those concerns; splitting
// would scatter related routing decisions across multiple helpers without
// changing the underlying complexity.
//
//nolint:funlen,nestif // see comment above.
func (h *Hub) initHandler() {
	router := mux.NewRouter()
	router.UseEncodedPath()
	router.SkipClean(true)

	csp := "default-src 'self'"

	if h.demo {
		router.PathPrefix(defaultDemoURL).HandlerFunc(h.Demo).Methods(http.MethodGet, http.MethodHead)
	}

	if h.ui {
		fileServer := http.StripPrefix(defaultUIURL, http.FileServer(http.FS(publicFS(h.logger))))

		if h.demo {
			router.PathPrefix(defaultUIURL).Handler(fileServer)
		} else {
			// public/fixtures/ ships demo-only artefacts (private JWK,
			// demo HS256 secret, pre-minted demo tokens). The router runs
			// with UseEncodedPath()+SkipClean(true) for the protocol routes,
			// which lets percent-encoded segments and `..` traversal sneak
			// past a plain PathPrefix matcher (FileServer decodes+cleans
			// before the file lookup). Gate against path.Clean(r.URL.Path)
			// here so encoded variants resolve to the same comparison.
			const fixturesPath = defaultUIURL + "fixtures"
			router.PathPrefix(defaultUIURL).Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				cleaned := path.Clean(r.URL.Path)
				if cleaned == fixturesPath || strings.HasPrefix(cleaned, fixturesPath+"/") {
					http.NotFound(w, r)

					return
				}

				fileServer.ServeHTTP(w, r)
			}))
		}

		csp = strings.Join([]string{
			"default-src 'self' mercure.rocks cdn.jsdelivr.net cdnjs.cloudflare.com fonts.googleapis.com",
			"script-src 'self' cdn.jsdelivr.net cdnjs.cloudflare.com",
			"style-src 'self' 'unsafe-inline' cdn.jsdelivr.net cdnjs.cloudflare.com fonts.googleapis.com",
			"font-src 'self' fonts.gstatic.com cdnjs.cloudflare.com data:",
			"connect-src 'self' cdn.jsdelivr.net",
		}, "; ")
	}

	h.registerSubscriptionHandlers(router)

	if h.subscriberJWTKeyFunc != nil || h.anonymous {
		router.HandleFunc(defaultHubURL, h.SubscribeHandler).Methods(http.MethodGet, http.MethodHead)
	}

	if h.publisherJWTKeyFunc != nil {
		router.HandleFunc(defaultHubURL, h.PublishHandler).Methods(http.MethodPost)
	}

	secureMiddleware := secure.New(secure.Options{
		IsDevelopment:         h.debug,
		AllowedHosts:          h.allowedHosts,
		FrameDeny:             true,
		ContentTypeNosniff:    true,
		BrowserXssFilter:      true,
		ContentSecurityPolicy: csp,
	})

	if len(h.corsOrigins) == 0 {
		h.handler = secureMiddleware.Handler(router)

		return
	}

	corsOptions := cors.Options{
		AllowedOrigins:   h.corsOrigins,
		AllowCredentials: true,
		AllowedHeaders:   h.corsAllowedHeaders(),
		Debug:            h.debug,
	}

	// rs/cors emits debug traces only when Debug is set. Route them through the
	// hub's structured logger at Debug level instead of rs/cors's default
	// "[cors] " stdout logger, so they're leveled and filterable like every other
	// hub log line. This unifies the output; it does not silence it.
	if h.debug {
		corsOptions.Logger = corsLogger{h.logger}
	}

	if h.demo {
		// Expose Link header for cross-origin hub discovery.
		corsOptions.ExposedHeaders = []string{"link"}
	}

	h.handler = secureMiddleware.Handler(
		cors.New(corsOptions).Handler(router),
	)
}

// corsLogger adapts rs/cors's Logger interface onto the hub's slog, logging each
// trace at Debug level with the trace text under a "detail" attribute.
type corsLogger struct{ logger *slog.Logger }

func (l corsLogger) Printf(format string, v ...any) {
	l.logger.LogAttrs(context.Background(), slog.LevelDebug, "cors",
		slog.String("detail", strings.TrimSpace(fmt.Sprintf(format, v...))))
}

func (h *Hub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.handler.ServeHTTP(w, r)
}

func (h *Hub) registerSubscriptionHandlers(r *mux.Router) {
	if !h.subscriptions {
		return
	}

	if _, ok := h.transport.(TransportSubscribers); !ok {
		if h.logger.Enabled(h.ctx, slog.LevelError) {
			h.logger.LogAttrs(h.ctx, slog.LevelError, "The current transport doesn't support subscriptions. Subscription API disabled.")
		}

		return
	}

	r.UseEncodedPath()
	r.SkipClean(true)

	r.HandleFunc(subscriptionURL, h.SubscriptionHandler).Methods(http.MethodGet)
	r.HandleFunc(subscriptionsForTopicURL, h.SubscriptionsHandler).Methods(http.MethodGet)
	r.HandleFunc(subscriptionsURL, h.SubscriptionsHandler).Methods(http.MethodGet)
}
