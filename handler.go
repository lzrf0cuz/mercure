package mercure

import (
	"log/slog"
	"net/http"
	"net/url"
	"path"
	"slices"
	"strings"

	"github.com/gorilla/mux"
	"github.com/rs/cors"
	"github.com/unrolled/secure"
	"golang.org/x/net/publicsuffix"
)

const (
	defaultHubURL   = "/.well-known/mercure"
	defaultDebugURL = defaultHubURL + "/debug/"
)

// PlaygroundURLPrefix is the root path the insecure playground echo endpoints are
// served under (only when the playground is enabled). It sits outside the reserved
// /.well-known/mercure namespace, so the resources it serves are valid, subscribable
// Mercure topics; nesting them under the hub path would make them reserved.
//
// INSECURE + EXPERIMENTAL. Not covered by the backward compatibility promise.
const PlaygroundURLPrefix = "/playground/"

func (h *Hub) initHandler() {
	router := mux.NewRouter()
	router.UseEncodedPath()
	router.SkipClean(true)

	csp := "default-src 'self'"

	if h.playground {
		router.PathPrefix(PlaygroundURLPrefix).HandlerFunc(h.Playground).Methods(http.MethodGet, http.MethodHead)
	}

	if h.debugger {
		// Register the UI's dynamic endpoints before the file server: it claims
		// the whole defaultDebugURL prefix, so the more specific routes must come
		// first (gorilla/mux matches in registration order).
		router.HandleFunc(defaultDebugURL+"config.json", h.DebuggerConfigHandler).Methods(http.MethodGet, http.MethodHead)

		if h.playground && h.playgroundTokenFunc != nil {
			router.HandleFunc(defaultDebugURL+"playground-token", h.PlaygroundTokenHandler).Methods(http.MethodGet, http.MethodHead)
		}

		router.PathPrefix(defaultDebugURL).Handler(h.debuggerFileHandler())
	}

	h.registerSubscriptionHandlers(router)

	if h.subscriberConfigured || h.anonymous {
		router.HandleFunc(defaultHubURL, h.SubscribeHandler).Methods(http.MethodGet, http.MethodHead, methodQuery)
	}

	if h.publisherConfigured {
		router.HandleFunc(defaultHubURL, h.PublishHandler).Methods(http.MethodPost)
	}

	// Advertise OAuth 2.0 protected resource metadata (RFC 9728) only when the
	// hub validates access tokens; a pure-anonymous hub is not a protected
	// resource.
	if h.publisherConfigured || h.subscriberConfigured {
		router.HandleFunc(protectedResourceMetadataPath, h.ProtectedResourceMetadataHandler).Methods(http.MethodGet, http.MethodHead)
	}

	secureMiddleware := secure.New(secure.Options{
		IsDevelopment:         h.debug,
		FrameDeny:             true,
		ContentTypeNosniff:    true,
		BrowserXssFilter:      true,
		ContentSecurityPolicy: csp,
	})

	h.handler = secureMiddleware.Handler(h.corsHandler(router))
}

func spansArbitraryOrigins(origin string) bool {
	// Any site can send Origin: null from a sandboxed iframe or a data: URL.
	if origin == "null" {
		return true
	}

	prefix, suffix, found := strings.Cut(origin, "*")
	if !found {
		return false
	}

	if !strings.HasSuffix(prefix, "://") || !strings.HasPrefix(suffix, ".") || strings.Contains(suffix, "*") {
		return true
	}

	u, err := url.Parse(strings.ToLower(prefix + strings.TrimPrefix(suffix, ".")))
	if err != nil {
		return true
	}

	_, err = publicsuffix.EffectiveTLDPlusOne(u.Hostname())

	return err != nil
}

// corsHandler wraps the router with CORS when origins are configured,
// otherwise returns it unchanged.
func (h *Hub) corsHandler(router http.Handler) http.Handler {
	if len(h.corsOrigins) == 0 {
		return router
	}

	// Reflected wildcard origins must not expose credentials across registrable domains.
	allowCredentials := !slices.ContainsFunc(h.corsOrigins, spansArbitraryOrigins)

	return cors.New(cors.Options{
		AllowedOrigins:   h.corsOrigins,
		AllowCredentials: allowCredentials,
		AllowedMethods:   []string{http.MethodGet, http.MethodHead, http.MethodPost, methodQuery},
		AllowedHeaders:   h.corsAllowedHeaders(),
		// Exposed so cross-origin subscribers can read the subscription API's
		// rel="mercure" Link header, which carries the last-event-id cursor,
		// and the Mercure-Last-Event-Id field a subscription answers with:
		// without it, a fetch-based cross-origin subscriber cannot detect
		// data loss when resuming. Retry-After carries the back-off of a 429
		// that sheds a subscriber.
		ExposedHeaders: []string{"Link", "Mercure-Last-Event-Id", "Accept-Query", "Retry-After"},
		Debug:          h.debug,
	}).Handler(router)
}

func (h *Hub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Reject a request whose origin is not in the public-URL allowlist before
	// deriving any identity from it (see requestIdentity). The origin is the one
	// an embedding server resolved (the Caddy module, from Caddy's request
	// placeholders), else the request's own scheme and Host.
	scheme, host := h.requestOrigin(r)

	// No Host (HTTP/1.0, or HTTP/2 without :authority) leaves a hub with no
	// configured identifier nothing to derive an identity from: no audience to
	// check tokens against, no valid RFC 9728 metadata to serve.
	if host == "" {
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)

		return
	}

	if len(h.allowedOrigins) > 0 {
		if !slices.Contains(h.allowedOrigins, strings.ToLower(scheme+"://"+host)) {
			http.Error(w, http.StatusText(http.StatusMisdirectedRequest), http.StatusMisdirectedRequest)

			return
		}
	}

	h.handler.ServeHTTP(w, r)
}

func (h *Hub) registerSubscriptionHandlers(r *mux.Router) {
	if !h.subscriptions {
		return
	}

	// The spec requires API clients to be authorized, which needs a subscriber verifier; 0.x served it openly.
	if !h.subscriberConfigured && !h.compatClaimsEnabled() {
		if h.logger.Enabled(h.ctx, slog.LevelError) {
			h.logger.LogAttrs(h.ctx, slog.LevelError, "No subscriber verifier is configured. Subscription API disabled.")
		}

		return
	}

	if _, ok := h.transport.(TransportSubscribers); !ok {
		if h.logger.Enabled(h.ctx, slog.LevelError) {
			h.logger.LogAttrs(h.ctx, slog.LevelError, "The current transport doesn't support subscriptions. Subscription API disabled.")
		}

		return
	}

	// 3-segment route (more specific, registered first).
	r.HandleFunc(subscriptionMatchURL, h.SubscriptionHandler).Methods(http.MethodGet)

	// The collection route /subscriptions/{match_type}/{match} and the
	// deprecated /subscriptions/{topic}/{subscriber} route have the same
	// shape. In modern-only mode only the modern route is registered, so
	// there is no ambiguity and no per-request check is added — this is the
	// hot path. Under the deprecated_topic tag and compatibility mode, the
	// deprecated registration guards the modern route with a MatcherFunc and
	// adds the v8 routes.
	if !h.registerDeprecatedSubscriptionHandlers(r) {
		r.HandleFunc(subscriptionsForMatchURL, h.SubscriptionsHandler).Methods(http.MethodGet)
	}

	r.HandleFunc(subscriptionsURL, h.SubscriptionsHandler).Methods(http.MethodGet)
}

// debuggerFileHandler gates playground fixtures using the decoded, cleaned path,
// matching FileServer's lookup even when mux uses encoded paths and skips cleaning.
func (h *Hub) debuggerFileHandler() http.Handler {
	fileServer := http.StripPrefix(defaultDebugURL, http.FileServer(http.FS(publicFS(h.logger))))
	if h.playground {
		return fileServer
	}

	const fixturesPath = "/fixtures"

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Strip and anchor exactly as FileServer does, including traversal above its root.
		// Lower-cased because dev_ui's os.DirFS resolves FIXTURES/ on a case-insensitive filesystem.
		cleaned := strings.ToLower(path.Clean("/" + strings.TrimPrefix(r.URL.Path, defaultDebugURL)))
		if cleaned == fixturesPath || strings.HasPrefix(cleaned, fixturesPath+"/") {
			http.NotFound(w, r)

			return
		}

		fileServer.ServeHTTP(w, r)
	})
}
