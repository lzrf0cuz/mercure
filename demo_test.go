package mercure

import (
	"crypto/tls"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestEmptyBodyAndJWT(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequest(http.MethodGet, "https://example.com/demo/foo.jsonld", nil)
	w := httptest.NewRecorder()

	h, _ := NewHub(t.Context())
	h.Demo(w, req)

	resp := w.Result()
	assert.Equal(t, "application/ld+json", resp.Header.Get("Content-Type"))
	assert.Equal(t, []string{hubLink, `<https://example.com/demo/foo.jsonld>; rel="self"`}, resp.Header["Link"])

	cookie := resp.Cookies()[0]
	assert.Equal(t, "mercureAuthorization", cookie.Name)
	assert.Empty(t, cookie.Value)
	assert.True(t, cookie.Expires.Before(time.Now()))
	assert.True(t, cookie.HttpOnly)
	assert.Equal(t, http.SameSiteStrictMode, cookie.SameSite)

	t.Cleanup(func() {
		_ = resp.Body.Close()
	})

	body, _ := io.ReadAll(resp.Body)
	assert.Empty(t, string(body))
}

func TestBodyAndJWT(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequest(http.MethodGet, "https://example.com/demo/foo/bar.xml?body=<hello/>&jwt=token", nil)
	w := httptest.NewRecorder()

	h, _ := NewHub(t.Context())
	h.Demo(w, req)

	resp := w.Result()
	// XSS mitigation: when ?body= is supplied, Content-Type is forced to
	// text/plain regardless of the URL extension so the reflected payload
	// cannot be interpreted as HTML / XML / JS by the browser.
	assert.Equal(t, "text/plain; charset=utf-8", resp.Header.Get("Content-Type"))
	assert.Equal(t, []string{hubLink, `<https://example.com/demo/foo/bar.xml?body=<hello/>&jwt=token>; rel="self"`}, resp.Header["Link"])

	cookie := resp.Cookies()[0]
	assert.Equal(t, "mercureAuthorization", cookie.Name)
	assert.Equal(t, "token", cookie.Value)
	assert.Empty(t, cookie.Expires)
	assert.True(t, cookie.HttpOnly)
	assert.Equal(t, http.SameSiteStrictMode, cookie.SameSite)

	t.Cleanup(func() {
		_ = resp.Body.Close()
	})

	body, _ := io.ReadAll(resp.Body)
	assert.Equal(t, "<hello/>", string(body))
}

// TestBodyXSSMitigation locks in the demo Reflected-XSS mitigation: when the
// caller supplies an arbitrary `body` query param, the response Content-Type
// must be text/plain regardless of the URL extension, so the reflected payload
// cannot be interpreted as HTML/JS by browsers.
func TestBodyXSSMitigation(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		path string
	}{
		{"HTMLExtension", "/demo/x.html"},
		{"SVGExtension", "/demo/x.svg"},
		{"JSExtension", "/demo/x.js"},
		{"XMLExtension", "/demo/x.xml"},
		{"NoExtension", "/demo/x"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			payload := `<script>alert(1)</script>`
			req := httptest.NewRequest(http.MethodGet, "https://example.com"+tc.path+"?body="+payload, nil)
			w := httptest.NewRecorder()

			h, _ := NewHub(t.Context())
			h.Demo(w, req)

			resp := w.Result()

			t.Cleanup(func() { _ = resp.Body.Close() })

			assert.Equal(t, "text/plain; charset=utf-8", resp.Header.Get("Content-Type"))

			body, _ := io.ReadAll(resp.Body)
			assert.Equal(t, payload, string(body))
		})
	}
}

func TestDemoCookieSecure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		setup  func(*http.Request)
		secure bool
	}{
		{"TLS", func(r *http.Request) { r.TLS = &tls.ConnectionState{} }, true},
		{"XForwardedProto", func(r *http.Request) { r.Header.Set("X-Forwarded-Proto", "https") }, true},
		// Single-value with surrounding whitespace from misconfigured proxies.
		{"XForwardedProtoSingleWithSpaces", func(r *http.Request) { r.Header.Set("X-Forwarded-Proto", "  https  ") }, true},
		// Multi-hop chain: leftmost value is the original client's protocol per RFC-7239.
		{"XForwardedProtoChainHTTPS", func(r *http.Request) { r.Header.Set("X-Forwarded-Proto", "https, http") }, true},
		{"XForwardedProtoChainPlain", func(r *http.Request) { r.Header.Set("X-Forwarded-Proto", "http, https") }, false},
		{"XForwardedProtoChainSpaces", func(r *http.Request) { r.Header.Set("X-Forwarded-Proto", "  https  , http") }, true},
		// Comma-first / empty-first / all-commas header — defensive cases for
		// proxies that misformat the chain. The leftmost-hop slot is empty, so
		// the resolver MUST default to non-secure: an empty leftmost-proto is
		// treated as insecure, never silently elevated past a trust boundary.
		{"XForwardedProtoChainEmptyFirst", func(r *http.Request) { r.Header.Set("X-Forwarded-Proto", ", https") }, false},
		{"XForwardedProtoChainEmptyFirstSpaced", func(r *http.Request) { r.Header.Set("X-Forwarded-Proto", " , https") }, false},
		{"XForwardedProtoChainAllCommas", func(r *http.Request) { r.Header.Set("X-Forwarded-Proto", ",,,") }, false},
		{"XForwardedProtoChainLeadingComma", func(r *http.Request) { r.Header.Set("X-Forwarded-Proto", ",https,http") }, false},
		{"PlainHTTP", func(_ *http.Request) {}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodGet, defaultDemoURL+"?jwt=token", nil)
			tt.setup(req)

			w := httptest.NewRecorder()

			h, _ := NewHub(t.Context())
			h.Demo(w, req)

			cookie := w.Result().Cookies()[0]
			assert.Equal(t, tt.secure, cookie.Secure)
			assert.True(t, cookie.HttpOnly)
		})
	}
}

// TestIsLoopbackAddr locks the parse behavior of the helper that gates the
// Demo non-Secure cookie Warn. Without this, a refactor that drops the
// SplitHostPort fallback or inverts the conservative-on-parse-error branch
// would silently flip the warning fire/suppress decision without any test
// signalling the change.
func TestIsLoopbackAddr(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want bool
	}{
		// Loopback MUST be recognised — otherwise legitimate dev traffic
		// (HTTP on localhost) trips the non-Secure cookie Warn.
		{"IPv4LocalhostWithPort", "127.0.0.1:8080", true},
		{"IPv4LocalhostBare", "127.0.0.1", true},
		{"IPv4LocalhostAlt", "127.0.0.42:8080", true}, // any 127.0.0.0/8 is loopback
		{"IPv6LoopbackWithPort", "[::1]:8080", true},
		{"IPv6LoopbackBare", "::1", true},

		// Non-loopback MUST be non-loopback — that's where the Warn is the
		// operator-actionable signal.
		{"PublicIPv4", "203.0.113.5:443", false},
		{"RFC1918Private", "192.168.1.10:443", false},
		{"PublicIPv6", "[2001:db8::1]:443", false},

		// IPv4-mapped IPv6 loopback parses via net.ParseIP and normalises
		// to 127.0.0.1 — Go's IsLoopback returns true. Locks the contract
		// for clients connecting over a dual-stack socket on macOS/Linux.
		{"IPv4MappedIPv6Loopback", "[::ffff:127.0.0.1]:8080", true},

		// DNS-name hosts (e.g. "localhost:8080") are NOT resolved by this
		// helper — net.ParseIP returns nil for "localhost". The Warn fires
		// for HTTP-on-localhost-by-name traffic; documented gap. A future
		// "fix" that resolved DNS here would change behavior silently.
		{"DNSNameLocalhost", "localhost:8080", false},

		// Parse-error / unrecognised shapes MUST fall back to false so the
		// warning fires conservatively. A regression that swallowed
		// SplitHostPort and defaulted to true would silently suppress warns
		// for every misformatted RemoteAddr.
		{"Empty", "", false},
		{"GarbageNoColon", "garbage", false},
		{"NotAnIP", "not-an-ip:1234", false},
		{"UnixSocketAbstract", "@", false}, // documented gap; warning fires
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, isLoopbackAddr(tt.in),
				"isLoopbackAddr(%q) returned %v; the Demo handler uses this to gate the non-Secure cookie Warn for non-loopback peers", tt.in, !tt.want)
		})
	}
}
