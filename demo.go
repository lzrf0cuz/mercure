package mercure

import (
	"io"
	"log/slog"
	"mime"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"time"
)

const hubLink = "<" + defaultHubURL + `>; rel="mercure"`

// isLoopbackAddr reports whether the request RemoteAddr is a loopback host
// (127.0.0.0/8 or ::1). Used by the demo handler to suppress the non-Secure
// cookie warning on legitimate dev traffic. Falls back to false on parse
// error so the warning fires conservatively.
func isLoopbackAddr(remoteAddr string) bool {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		host = remoteAddr
	}

	ip := net.ParseIP(host)

	return ip != nil && ip.IsLoopback()
}

// Demo exposes INSECURE Demo endpoints to test discovery and authorization mechanisms.
// Add a query parameter named "body" to define the content to return in the response's body.
// Add a query parameter named "jwt" set a "mercureAuthorization" cookie containing this token.
// The Content-Type header will automatically be set according to the URL's extension.
func (h *Hub) Demo(w http.ResponseWriter, r *http.Request) {
	// JSON-LD is the preferred format
	_ = mime.AddExtensionType(".jsonld", "application/ld+json")
	url := r.URL.String()
	mimeType := mime.TypeByExtension(filepath.Ext(r.URL.Path))

	query := r.URL.Query()
	body := query.Get("body")
	jwt := query.Get("jwt")

	// Several Link headers are set on purpose to allow testing advanced discovery mechanism
	header := w.Header()

	if h.cookieName == defaultCookieName {
		header["Link"] = append(header["Link"], hubLink, "<"+url+`>; rel="self"`)
	} else {
		header["Link"] = append(header["Link"], hubLink+`; cookie-name="`+h.cookieName+`"`, "<"+url+`>; rel="self"`)
	}

	// When the caller supplies an arbitrary body via the `?body=` query param,
	// reflecting it under a Content-Type derived from the URL extension turns
	// the demo into a Reflected-XSS gadget (e.g. /demo.html?body=<script>...).
	// Force text/plain so the body is never interpreted as HTML/JS by browsers.
	if body != "" {
		header["Content-Type"] = []string{"text/plain; charset=utf-8"}
	} else if mimeType != "" {
		header["Content-Type"] = []string{mimeType}
	}

	// X-Forwarded-Proto is read from the request as-is. The Caddy site is
	// expected to terminate or front-end TLS (direct, or via a
	// TLS-terminating load balancer) and to set
	// `trusted_proxies` so spoofed values from untrusted clients are
	// stripped. Caddy's reverse_proxy strips the inbound header by default
	// when trusted_proxies is configured.
	//
	// Multi-hop deployments (e.g. load balancer → Caddy → hub, with another L7 ahead)
	// can produce a comma-separated chain; per RFC-7239 the leftmost value
	// is the original client's protocol, so check that hop only. TrimSpace
	// covers single-value-with-whitespace cases produced by misconfigured
	// proxies as well.
	proto := strings.TrimSpace(r.Header.Get("X-Forwarded-Proto"))
	if i := strings.IndexByte(proto, ','); i >= 0 {
		proto = strings.TrimSpace(proto[:i])
	}

	isSecure := r.TLS != nil || proto == "https"

	// Operator-warning when the cookie ends up non-Secure on a non-loopback
	// request — the legitimate dev case is loopback (HTTP on localhost),
	// where Secure=false is fine. A non-loopback HTTP request with no
	// XFP=https almost always means a misconfigured trusted_proxies in
	// front of the demo endpoint; the resulting non-Secure cookie can be
	// intercepted by an attacker with the right network position.
	// Hub.Publish callers can ignore; the demo endpoint banner already
	// labels this surface INSECURE.
	// Once per process: the Demo endpoint is unauthenticated, so an attacker
	// flooding it over plain HTTP could otherwise amplify this Warn into a
	// log-volume DoS. The condition reflects a static misconfiguration
	// (trusted_proxies / X-Forwarded-Proto), so a single warning is enough
	// for an operator to act on. CAS so the log stays in handler scope with
	// the request context in view.
	if !isSecure && !isLoopbackAddr(r.RemoteAddr) &&
		h.logger.Enabled(r.Context(), slog.LevelWarn) &&
		h.demoInsecureWarned.CompareAndSwap(false, true) {
		h.logger.LogAttrs(r.Context(), slog.LevelWarn,
			"Demo endpoint setting non-Secure cookie on non-loopback request (logged once per hub instance)",
			slog.String("remote_addr", r.RemoteAddr),
			slog.String("hint", "set trusted_proxies and X-Forwarded-Proto, or front the demo with TLS"))
	}

	// Secure is set from the runtime scheme (X-Forwarded-Proto / r.TLS) rather
	// than hardcoded true: the demo runs on plain HTTP in local dev, where
	// Secure=true would prevent the browser from sending the cookie back.
	// HttpOnly + SameSite=Strict are unconditional.
	cookie := &http.Cookie{ //nolint:gosec // see comment above; Secure mirrors scheme.
		Name:     h.cookieName,
		Path:     defaultHubURL,
		Value:    jwt,
		HttpOnly: true,
		Secure:   isSecure,
		SameSite: http.SameSiteStrictMode,
	}
	if jwt == "" {
		// Remove cookie if not provided, to be sure a previous one doesn't exist
		cookie.Expires = time.Unix(0, 0)
	}

	http.SetCookie(w, cookie)

	if _, err := io.WriteString(w, body); err != nil { //nolint:gosec
		ctx := r.Context()

		if h.logger.Enabled(ctx, slog.LevelInfo) {
			h.logger.LogAttrs(ctx, slog.LevelInfo, "Failed to write demo response", slog.Any("error", err))
		}
	}
}
