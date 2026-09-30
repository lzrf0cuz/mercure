package mercure

import (
	"context"
	"net/http"
	"strings"
	"unicode/utf8"

	"go.opentelemetry.io/otel/attribute"
)

// requestOriginKey keys the per-request origin resolved by an embedding server.
type requestOriginKey struct{}

// RequestOrigin is the scheme and host an embedding server resolved for the
// current request. The hub derives its per-request OAuth resource identifier
// and RFC 9728 protected resource metadata URL from it (see requestIdentity).
type RequestOrigin struct {
	Scheme string
	Host   string
}

// NewRequestOriginContext returns a copy of ctx carrying the resolved request
// origin. An embedding server (for example the Caddy module) calls it once per
// request before invoking the hub, passing scheme and host taken from a trusted
// source; the hub validates them against the public-URL allowlist. The Caddy
// module reads them from Caddy's request placeholders, which report the
// request's own Host and TLS state, overridden by the X-Forwarded-Proto and
// X-Forwarded-Host headers of requests from trusted_proxies only.
func NewRequestOriginContext(ctx context.Context, scheme, host string) context.Context {
	return context.WithValue(ctx, requestOriginKey{}, RequestOrigin{Scheme: scheme, Host: host})
}

// requestOrigin returns the origin an embedding server resolved for r, falling
// back to the request's own scheme and Host for standalone net/http use.
func (h *Hub) requestOrigin(r *http.Request) (scheme, host string) {
	if o, ok := r.Context().Value(requestOriginKey{}).(RequestOrigin); ok {
		return o.Scheme, o.Host
	}

	scheme = "http"
	if r.TLS != nil {
		scheme = schemeHTTPS
	}

	return scheme, r.Host
}

// verifiedIdentityKey keys the per-request callback that receives the verified caller identity.
type verifiedIdentityKey struct{}

const (
	// maxIdentityValueLen caps each VerifiedIdentity value taken from a claim or a bound
	// header, in bytes.
	maxIdentityValueLen = 256

	// bindingSpanKeyPrefix prefixes the span attribute that reports a binding's value.
	bindingSpanKeyPrefix = "mercure.binding."

	// The VerifiedIdentity.Role values.
	identityRolePublisher  = "publisher"
	identityRoleSubscriber = "subscriber"
)

// VerifiedIdentity is the caller identity of a request whose access token was verified and
// that passed every claim binding applying to it. A later check can still refuse the request,
// for example a topic-grant 403. Only the handler's span carries the span attributes, for a
// publish the mercure.publish handler span, not the inner span of Hub.Publish. Values taken
// from the token or a bound header lose their C0 control characters (U+0000 to U+001F) and
// DEL (U+007F), so a signed claim cannot put a NUL, an ESC, a CR or an LF into a header or log
// line the embedder writes, then are truncated to 256 bytes at a UTF-8 boundary. NEL (U+0085),
// U+2028 and U+2029 are kept. The token itself is never part of it.
type VerifiedIdentity struct {
	// Subject, ClientID and JWTID are the sub, client_id and jti claims; each is empty when
	// the token carries none.
	Subject  string
	ClientID string
	JWTID    string
	// Role is the role the token was verified for: "publisher" or "subscriber".
	Role string
	// Bindings holds, in configuration order, every binding that matched a value. A binding
	// that on_missing allow skipped matched nothing and is not listed.
	Bindings []BindingMatch
}

// BindingMatch is the value a claim binding matched: the bound header's value, or the
// configured value of a claim-value binding.
type BindingMatch struct {
	Claim string
	Value string
}

// NewVerifiedIdentityContext returns a copy of ctx carrying fn. An embedding server (for
// example the Caddy module) calls it once per request before invoking the hub. The hub then
// calls fn synchronously with the request's context, at most once per request, only after
// the token is verified and every applicable binding passed, never for an anonymous or
// rejected request. fn must not modify the identity it receives: its Bindings slice is shared
// with the span attributes the hub sets right after.
//
// Without fn, no VerifiedIdentity is built.
func NewVerifiedIdentityContext(ctx context.Context, fn func(context.Context, VerifiedIdentity)) context.Context {
	return context.WithValue(ctx, verifiedIdentityKey{}, fn)
}

// verifiedIdentityFunc returns the callback an embedding server put in ctx, or nil.
func verifiedIdentityFunc(ctx context.Context) func(context.Context, VerifiedIdentity) {
	fn, _ := ctx.Value(verifiedIdentityKey{}).(func(context.Context, VerifiedIdentity))

	return fn
}

// newVerifiedIdentity builds the identity of a verified request from its claims and the
// values its bindings matched, which the caller has already capped.
func newVerifiedIdentity(c *claims, publish bool, bindings []BindingMatch) VerifiedIdentity {
	role := identityRoleSubscriber
	if publish {
		role = identityRolePublisher
	}

	return VerifiedIdentity{
		Subject:  capIdentityValue(c.Subject),
		ClientID: capIdentityValue(string(c.ClientID)),
		JWTID:    capIdentityValue(c.ID),
		Role:     role,
		Bindings: bindings,
	}
}

// identitySpanAttributes returns the attributes the hub sets on a verified request's span.
// The jti is left out: it identifies one token, not a caller.
func (h *Hub) identitySpanAttributes(id VerifiedIdentity) []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, 3+len(id.Bindings))

	if id.Subject != "" {
		attrs = append(attrs, attribute.String("user.id", id.Subject))
	}

	if id.ClientID != "" {
		attrs = append(attrs, attribute.String("mercure.client_id", id.ClientID))
	}

	attrs = append(attrs, attribute.String("mercure.role", id.Role))

	for _, m := range id.Bindings {
		attrs = append(attrs, attribute.KeyValue{Key: h.bindingSpanKey(m.Claim), Value: attribute.StringValue(m.Value)})
	}

	return attrs
}

// bindingSpanKey returns the span attribute key of the binding on claim. Every match comes
// from a configured binding, and bindings on the same claim share one key.
func (h *Hub) bindingSpanKey(claim string) attribute.Key {
	for _, b := range h.claimHeaderBindings {
		if b.claim == claim {
			return b.spanKey
		}
	}

	return ""
}

// capIdentityValue removes the C0 control characters and DEL from s, then truncates it to
// maxIdentityValueLen bytes, backing off to a rune boundary so a valid UTF-8 value stays
// valid. A value with no control character, the usual case, is returned without allocating.
func capIdentityValue(s string) string {
	if i := firstIdentityControl(s); i >= 0 {
		s = stripIdentityControls(s, i)
	}

	if len(s) <= maxIdentityValueLen {
		return s
	}

	i := maxIdentityValueLen
	for i > 0 && !utf8.RuneStart(s[i]) {
		i--
	}

	return s[:i]
}

// isIdentityControl reports whether b is a C0 control character or DEL. They are single
// bytes in UTF-8 and never part of a multi-byte sequence, so scanning bytes is exact and
// leaves any other byte, valid UTF-8 or not, untouched.
func isIdentityControl(b byte) bool {
	return b < ' ' || b == 0x7f
}

// firstIdentityControl returns the index of the first control character of s, or -1.
func firstIdentityControl(s string) int {
	for i := range len(s) {
		if isIdentityControl(s[i]) {
			return i
		}
	}

	return -1
}

// stripIdentityControls returns s without its control characters, cut after the first
// maxIdentityValueLen+1 bytes; first is the index of the first control character.
func stripIdentityControls(s string, first int) string {
	var b strings.Builder

	// Build no more than capIdentityValue can use: it keeps maxIdentityValueLen bytes and
	// reads the byte after them to find a rune boundary, so one more byte is collected and
	// the rest of a longer value is never read.
	const limit = maxIdentityValueLen + 1

	b.Grow(min(len(s), limit))
	b.WriteString(s[:min(first, limit)])

	for i := first + 1; i < len(s) && b.Len() < limit; i++ {
		if !isIdentityControl(s[i]) {
			b.WriteByte(s[i])
		}
	}

	return b.String()
}
