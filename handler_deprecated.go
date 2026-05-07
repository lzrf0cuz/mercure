//go:build deprecated_server

package mercure

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/rs/cors"
	"github.com/unrolled/secure"
	"golang.org/x/crypto/acme/autocert"
)

// Serve starts the HTTP server.
//
// Deprecated: use the Caddy server module or the standalone library instead.
func (h *Hub) Serve(ctx context.Context) {
	addr := h.config.GetString("addr")

	server := &http.Server{
		Addr:              addr,
		Handler:           h.baseHandler(),
		ReadTimeout:       h.config.GetDuration("read_timeout"),
		ReadHeaderTimeout: h.config.GetDuration("read_header_timeout"),
		WriteTimeout:      h.config.GetDuration("write_timeout"),
	}
	h.server.Store(server)

	if _, ok := h.metrics.(*PrometheusMetrics); ok {
		addr := h.config.GetString("metrics_addr")

		metricsServer := &http.Server{
			Addr:              addr,
			Handler:           h.metricsHandler(),
			ReadTimeout:       h.config.GetDuration("read_timeout"),
			ReadHeaderTimeout: h.config.GetDuration("read_header_timeout"),
			WriteTimeout:      h.config.GetDuration("write_timeout"),
		}
		h.metricsServer.Store(metricsServer)

		if h.logger.Enabled(ctx, slog.LevelInfo) {
			h.logger.LogAttrs(ctx, slog.LevelInfo, "Mercure metrics started", slog.String("addr", addr))
		}

		go func() {
			if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) && h.logger.Enabled(ctx, slog.LevelError) {
				h.logger.LogAttrs(ctx, slog.LevelError, "Mercure metrics server error", slog.Any("error", err))
			}
		}()
	}

	acme := len(h.allowedHosts) > 0

	certFile := h.config.GetString("cert_file")
	keyFile := h.config.GetString("key_file")

	done := h.listenShutdown(ctx)

	var err error

	if !acme && certFile == "" && keyFile == "" {
		if h.logger.Enabled(ctx, slog.LevelInfo) {
			h.logger.LogAttrs(ctx, slog.LevelInfo, "Mercure started", slog.String("protocol", "http"), slog.String("addr", addr))
		}

		err = server.ListenAndServe()
	} else {
		// TLS
		if acme {
			certManager := &autocert.Manager{
				Prompt:     autocert.AcceptTOS,
				HostPolicy: autocert.HostWhitelist(h.allowedHosts...),
			}

			acmeCertDir := h.config.GetString("acme_cert_dir")
			if acmeCertDir != "" {
				certManager.Cache = autocert.DirCache(acmeCertDir)
			}

			server.TLSConfig = certManager.TLSConfig()

			// Mandatory for Let's Encrypt http-01 challenge
			go func() {
				if err := http.ListenAndServe(h.config.GetString("acme_http01_addr"), certManager.HTTPHandler(nil)); err != nil && !errors.Is(err, http.ErrServerClosed) && h.logger.Enabled(ctx, slog.LevelError) {
					h.logger.LogAttrs(ctx, slog.LevelError, "Error running HTTP endpoint", slog.Any("error", err))
				}
			}()
		}

		if h.logger.Enabled(ctx, slog.LevelInfo) {
			h.logger.LogAttrs(ctx, slog.LevelInfo, "Mercure started", slog.String("protocol", "https"), slog.String("addr", addr))
		}

		err = server.ListenAndServeTLS(certFile, keyFile)
	}

	if !errors.Is(err, http.ErrServerClosed) && h.logger.Enabled(ctx, slog.LevelError) {
		h.logger.LogAttrs(ctx, slog.LevelError, "Unexpected error", slog.Any("error", err))
	}

	<-done
}

// Deprecated: use the Caddy server module or the standalone library instead.
func (h *Hub) listenShutdown(ctx context.Context) <-chan struct{} {
	idleConnsClosed := make(chan struct{})

	server := h.server.Load()
	server.RegisterOnShutdown(func() {
		select {
		case <-idleConnsClosed:
		default:
			close(idleConnsClosed)
		}
	})

	go func() {
		sigint := make(chan os.Signal, 1)
		signal.Notify(sigint, os.Interrupt)
		<-sigint

		if err := server.Shutdown(ctx); err != nil && h.logger.Enabled(ctx, slog.LevelError) {
			h.logger.LogAttrs(ctx, slog.LevelError, "Unexpected error during server shutdown", slog.Any("error", err))
		}

		if metricsServer := h.metricsServer.Load(); metricsServer != nil {
			if err := metricsServer.Shutdown(ctx); err != nil && h.logger.Enabled(ctx, slog.LevelError) {
				h.logger.LogAttrs(ctx, slog.LevelError, "Unexpected error during metrics server shutdown", slog.Any("error", err))
			}
		}

		if h.logger.Enabled(ctx, slog.LevelInfo) {
			h.logger.LogAttrs(ctx, slog.LevelInfo, "My Baby Shot Me Down")
		}

		select {
		case <-idleConnsClosed:
		default:
			close(idleConnsClosed)
		}
	}()

	return idleConnsClosed
}

// chainHandlers configures and chains handlers.
//
// Deprecated: use the Caddy server module or the standalone library instead.
func (h *Hub) chainHandlers() http.Handler {
	r := mux.NewRouter()
	h.registerSubscriptionHandlers(r)

	r.HandleFunc(defaultHubURL, h.SubscribeHandler).Methods(http.MethodGet, http.MethodHead)
	r.HandleFunc(defaultHubURL, h.PublishHandler).Methods(http.MethodPost)

	csp := "default-src 'self'"

	if h.demo {
		r.PathPrefix("/demo").HandlerFunc(h.Demo).Methods(http.MethodGet, http.MethodHead)
	}

	if h.ui {
		r.PathPrefix("/").Handler(http.FileServer(http.FS(publicFS(h.logger))))

		csp += " mercure.rocks cdn.jsdelivr.net"
	} else {
		r.HandleFunc("/", welcomeHandler).Methods(http.MethodGet, http.MethodHead)
	}

	secureMiddleware := secure.New(secure.Options{
		IsDevelopment:         h.debug,
		AllowedHosts:          h.allowedHosts,
		FrameDeny:             true,
		ContentTypeNosniff:    true,
		BrowserXssFilter:      true,
		ContentSecurityPolicy: csp,
	})

	var corsHandler http.Handler

	if len(h.corsOrigins) > 0 {
		corsHandler = cors.New(cors.Options{
			AllowedOrigins:   h.corsOrigins,
			AllowCredentials: true,
			AllowedHeaders:   h.corsAllowedHeaders(),
		}).Handler(r)
	} else {
		corsHandler = r
	}

	var compressHandler http.Handler
	if h.config.GetBool("compress") {
		compressHandler = handlers.CompressHandler(corsHandler)
	} else {
		compressHandler = corsHandler
	}

	var useForwardedHeadersHandlers http.Handler
	if h.config.GetBool("use_forwarded_headers") {
		useForwardedHeadersHandlers = handlers.ProxyHeaders(compressHandler)
	} else {
		useForwardedHeadersHandlers = compressHandler
	}

	secureHandler := secureMiddleware.Handler(useForwardedHeadersHandlers)

	var loggingHandler http.Handler

	if h.logger.Enabled(h.ctx, slog.LevelError) {
		loggingHandler = handlers.CombinedLoggingHandler(os.Stderr, secureHandler)
	} else {
		loggingHandler = secureHandler
	}

	recoveryHandler := handlers.RecoveryHandler(
		handlers.RecoveryLogger(slogRecoveryHandlerLogger{h.logger}),
		handlers.PrintRecoveryStack(h.debug),
	)(loggingHandler)

	return recoveryHandler
}

// Deprecated: use the Caddy server module or the standalone library instead.
func (h *Hub) baseHandler() http.Handler {
	mainRouter := mux.NewRouter()
	mainRouter.UseEncodedPath()
	mainRouter.SkipClean(true)

	// Register /healthz (if enabled, in a way that doesn't pollute the HTTP logs).
	registerHealthz(mainRouter)

	handler := h.chainHandlers()
	mainRouter.PathPrefix("/").Handler(handler)

	return mainRouter
}

// Deprecated: use the Caddy server module or the standalone library instead.
func (h *Hub) metricsHandler() http.Handler {
	router := mux.NewRouter()

	registerHealthz(router)
	h.metrics.(*PrometheusMetrics).Register(router.PathPrefix("/").Subrouter())

	return router
}

// Deprecated: use the Caddy server module or the standalone library instead.
func registerHealthz(router *mux.Router) {
	router.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, "ok")
	}).Methods(http.MethodGet, http.MethodHead)
}

// Deprecated: use the Caddy server module or the standalone library instead.
func welcomeHandler(w http.ResponseWriter, _ *http.Request) {
	fmt.Fprint(w, `<!DOCTYPE html>
<title>Mercure Hub</title>
<h1>Welcome to <a href="https://mercure.rocks">Mercure</a>!</h1>`)
}

// Deprecated: use the Caddy server module or the standalone library instead.
type slogRecoveryHandlerLogger struct {
	logger *slog.Logger
}

func (l slogRecoveryHandlerLogger) Println(args ...any) {
	l.logger.Error(fmt.Sprint(args...))
}
