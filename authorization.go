package mercure

import (
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"

	"github.com/golang-jwt/jwt/v5"
)

// claims contains the validated claims of a Mercure access token.
type claims struct {
	jwt.RegisteredClaims

	deprecatedMercureClaims //nolint:unused // populated only in deprecated_claim builds

	// AuthorizationDetails carries the RFC 9396 authorization_details claim.
	AuthorizationDetails []authorizationDetail `json:"authorization_details,omitempty"`

	// authz holds the validated mercure authorization details (and, under the
	// deprecated_claim tag in compatibility mode, the legacy mercure claim
	// resolved into the same shape). Unexported, so it is never (un)marshaled.
	authz *mercureAuthz

	// captureRaw asks UnmarshalJSON to populate rawClaims. Set by validateJWT only when a
	// binding applies to this request, so a deployment that configures none, or whose
	// bindings are scoped to the other role, pays neither the decode nor the map. The zero
	// value captures nothing, so selectVerifier's unverified pre-parse never does.
	captureRaw bool

	// rawClaims holds every top-level claim as raw JSON so a configured binding
	// (claim_header_validation.go) can read an arbitrary claim by name without a
	// second JWT verify. Unexported, so it is never (un)marshaled and can never
	// round-trip into a marshaled token.
	//
	// nil in three distinct situations, which evalClaim must keep apart: before the token is
	// parsed, when no binding applies to the request, and after authorizeAndBind has
	// consumed the map and released it. Only an anonymous request (nil *claims) means "no
	// claim"; a nil map on a live *claims is malformed, never absent.
	rawClaims map[string]jsontext.Value
}

// UnmarshalJSON decodes the claim set with encoding/json/v2, like the authorization details.
//
// When captureRaw is set it first records every top-level claim as raw JSON. Neither decode
// verifies anything: both run inside jwt.ParseWithClaims, which checks the signature
// afterwards, and validateJWT discards the claims unless that check passed, so rawClaims
// never escapes for an unverified token. The typed decode merges into c (json/v2 merge
// semantics), so it leaves rawClaims in place, and json/v2 copies each jsontext.Value
// rather than aliasing data, so rawClaims is safe to retain.
//
// captureRaw is read off the receiver, so this relies on jwt.ParseWithClaims decoding into
// the instance validateJWT handed it.
func (c *claims) UnmarshalJSON(data []byte) error {
	type plainClaims claims

	if c.captureRaw {
		var raw map[string]jsontext.Value
		if err := jsonv2.Unmarshal(data, &raw); err != nil {
			return fmt.Errorf("unable to capture raw JWT claims: %w", err)
		}

		c.rawClaims = raw
	}

	return jsonv2.Unmarshal(data, (*plainClaims)(c)) //nolint:wrapcheck
}

type role int

const (
	// defaultCookieName is the name of the authorization cookie carrying the
	// access token: the spec-recommended "__Secure-" prefixed name, which user
	// agents refuse over insecure transport. Plain-HTTP deployments (local
	// development) must configure a prefix-less name with WithCookieName. The
	// pre-1.0 name "mercureAuthorization" is accepted as a fallback only in
	// deprecated_claim builds running in compatibility mode.
	defaultCookieName = "__Secure-mercure_access_token"
	bearerPrefix      = "Bearer "
	// minCompactJWSLen is the shortest plausible length of a JWS in compact
	// serialization (two dots plus base64url-encoded header, claims and
	// signature). Anything shorter is garbage and is rejected before signature
	// verification.
	minCompactJWSLen = 41
	// maxCompactJWSLen bounds the unverified input decoded before the signature check.
	maxCompactJWSLen = 64 << 10
	// authorizationHeader is the lowercase name of the "Authorization" HTTP
	// header, used in the CORS allowed-headers list.
	authorizationHeader = "authorization"
	// atJWTType is the required JWT access token "typ" header value (RFC 9068).
	atJWTType = "at+jwt"
)

const (
	roleSubscriber role = iota
	rolePublisher
)

var (
	// ErrInvalidAuthorizationHeader is returned when the Authorization header is invalid.
	ErrInvalidAuthorizationHeader = errors.New(`invalid "Authorization" HTTP header`)
	// ErrNoOrigin is returned when the cookie authorization mechanism is used and no Origin nor Referer headers are presents.
	ErrNoOrigin = errors.New(`an "Origin" or a "Referer" HTTP header must be present to use the cookie-based authorization mechanism`)
	// ErrOriginNotAllowed is returned when the Origin is not allowed to post updates.
	ErrOriginNotAllowed = errors.New("origin not allowed to post updates")
	// ErrInvalidJWT is returned when the access token is invalid.
	ErrInvalidJWT = errors.New("invalid JWT")
)

// wildcard has been copied from https://github.com/rs/cors/blob/1084d89a16921942356d1c831fbe523426cf836e/utils.go
// Copyright (c) 2014 Olivier Poitrey <rs@dailymotion.com>
// MIT licensed.
type wildcard struct {
	prefix string
	suffix string
}

func (w wildcard) match(s string) bool {
	return len(s) >= len(w.prefix)+len(w.suffix) &&
		strings.HasPrefix(s, w.prefix) &&
		strings.HasSuffix(s, w.suffix)
}

// authorize validates the JWT that may be provided through an "Authorization" HTTP header or an authorization cookie.
// It returns the claims contained in the token if it exists and is valid, nil if no token is provided (anonymous mode), and an error if the token is not valid.
func (h *Hub) authorize(r *http.Request, publish bool) (*claims, error) { //nolint:funlen
	// The expected audience is the hub's per-request resource identifier, so a
	// token minted for the public URL the client contacted is accepted while one
	// minted for a different host is rejected (RFC 9068).
	expectedAudience, _ := h.requestIdentity(r)

	authorizationHeaders, authorizationHeaderExists := r.Header["Authorization"]
	if authorizationHeaderExists {
		// The token must be at least minCompactJWSLen bytes after the prefix.
		// The auth scheme is matched case-insensitively per RFC 9110 §11.1.
		if len(authorizationHeaders) != 1 || len(authorizationHeaders[0]) < len(bearerPrefix)+minCompactJWSLen ||
			!strings.EqualFold(authorizationHeaders[0][:len(bearerPrefix)], bearerPrefix) {
			return nil, ErrInvalidAuthorizationHeader
		}

		return h.validateJWT(authorizationHeaders[0][len(bearerPrefix):], publish, expectedAudience)
	}

	// The deprecated "authorization" query parameter is honored only in
	// deprecated_claim builds running in compatibility mode. The RFC 6750
	// "access_token" query parameter is not accepted: RFC 9700 §4.3.2 forbids
	// passing access tokens in the URI query string.
	if token, ok := h.legacyAuthQueryParam(r); ok {
		return h.validateJWT(token, publish, expectedAudience)
	}

	cookie, err := h.readCookie(r)
	if err != nil {
		// Anonymous
		return nil, nil //nolint:nilerr,nilnil
	}

	// CSRF attacks cannot occur when using safe methods
	if r.Method != http.MethodPost {
		return h.validateJWT(cookie.Value, publish, expectedAudience)
	}

	origin := r.Header.Get("Origin")
	if origin == "" {
		// Try to extract the origin from the Referer, or return an error
		referer := r.Header.Get("Referer")
		if referer == "" {
			return nil, ErrNoOrigin
		}

		u, err := url.Parse(referer)
		if err != nil {
			return nil, fmt.Errorf("unable to parse referer: %w", err)
		}

		origin = fmt.Sprintf("%s://%s", u.Scheme, u.Host)
	}

	if h.publishOriginsAll {
		return h.validateJWT(cookie.Value, publish, expectedAudience)
	}

	if slices.Contains(h.publishOrigins, origin) {
		return h.validateJWT(cookie.Value, publish, expectedAudience)
	}

	for _, allowedOrigin := range h.publishWOrigins {
		if allowedOrigin.match(origin) {
			return h.validateJWT(cookie.Value, publish, expectedAudience)
		}
	}

	return nil, fmt.Errorf("%q: %w", origin, ErrOriginNotAllowed)
}

// authorizeAndBind is authorizeAndBindCounted without the counted binding value.
func (h *Hub) authorizeAndBind(r *http.Request, publish bool) (*claims, error) {
	c, _, err := h.authorizeAndBindCounted(r, publish)

	return c, err
}

// authorizeAndBindCounted authorizes the request and then enforces every applicable binding.
// Wrapping authorize() covers every credential carrier: header, cookie and query parameter.
//
// A nil claims means the request is anonymous: it carries no token, so no claim to bind,
// and bindings are skipped. A rejection is returned as a *claimBindingError, which
// writeAuthError answers with the status the rejection maps to.
//
// The returned bindingValue is the header value matched by the binding that counts subscribers
// (WithBindingCountSubscribers), with that binding's identity. Its value is empty when no such
// binding applies, when on_missing=allow skipped it, and when the request is anonymous.
func (h *Hub) authorizeAndBindCounted(r *http.Request, publish bool) (*claims, bindingValue, error) {
	c, err := h.authorize(r, publish)
	if err != nil || c == nil {
		return c, bindingValue{}, err
	}

	counted, err := h.enforceBindings(r, c, publish)
	if err != nil {
		return nil, bindingValue{}, err
	}

	// The bindings have consumed the raw claims and nothing downstream reads them, yet a
	// subscriber pins its *claims for the lifetime of the connection. Release the map here
	// rather than retain a decoded copy of every top-level claim per connected subscriber.
	// evalClaim treats a stripped claims object as malformed, so any later evaluation fails
	// closed instead of skipping the binding.
	c.rawClaims = nil

	return c, counted, nil
}

// enforceBindings runs every binding that applies to the role, one tier at a time, so the
// answer never depends on the order the bindings are configured in:
//  1. the request side of every binding: a missing or malformed bound header makes the
//     request malformed whatever the token says (400);
//  2. the claim shape of every binding: a bound claim of unusable type makes the token
//     invalid (401);
//  3. absence and comparison: a valid token that does not authorize the request (403).
//
// In every tier, the first configured binding with a fault decides: its rejection is the one
// returned and the one metered (reportRejection). The status depends only on the tier, so
// the order never changes the answer, only which binding is metered. The counted value is the
// value matched by the binding that counts subscribers.
func (h *Hub) enforceBindings(r *http.Request, c *claims, publish bool) (counted bindingValue, err error) {
	evals := make([]bindingEval, len(h.claimHeaderBindings))

	for i, b := range h.claimHeaderBindings {
		if !b.appliesTo(publish) {
			continue
		}

		var reason AuthzRejectReason
		if evals[i].requested, reason, err = b.checkRequest(r); err != nil {
			b.reportRejection(h.metrics, reason)

			return bindingValue{}, err
		}
	}

	for i, b := range h.claimHeaderBindings {
		if !b.appliesTo(publish) {
			continue
		}

		var reason AuthzRejectReason
		if evals[i].claim, evals[i].allowed, reason, err = b.checkClaimShape(c); err != nil {
			b.reportRejection(h.metrics, reason)

			return bindingValue{}, err
		}
	}

	for i, b := range h.claimHeaderBindings {
		if !b.appliesTo(publish) {
			continue
		}

		matched, reason, err := b.checkScope(evals[i])
		if err != nil {
			b.reportRejection(h.metrics, reason)

			return bindingValue{}, err
		}

		if b.countSubscribers {
			counted = bindingValue{binding: b.id(), value: matched}
		}
	}

	return counted, nil
}

// capturesRawClaims reports whether a token for this role must have its raw top-level
// claims captured. They are read only by the bindings that apply to the request, so a
// publisher-scoped binding must not make every subscribe request pay the capture. This
// predicate is exactly the one enforceBindings uses to pick the bindings it runs, which is
// what guarantees rawClaims is populated whenever a binding will actually read it.
func (h *Hub) capturesRawClaims(publish bool) bool {
	return slices.ContainsFunc(h.claimHeaderBindings, func(b ClaimHeaderBinding) bool {
		return b.appliesTo(publish)
	})
}

// jwtParserOptions returns the RFC 9068 parser checks enforced in modern mode:
// a required audience matching the hub's per-request resource identifier
// (expectedAudience, never empty: validateJWT refuses that) and a required exp. In compatibility mode (deprecated_claim builds with
// WithProtocolVersionCompatibility) these checks are relaxed. The accepted
// algorithms are pinned here (RFC 8725) so the algorithm can never be taken
// from the token header: they come from the selected issuer's Verifier (a
// Static pins its one algorithm, a KeyFunc its allowlist, defaulting to the
// asymmetric algorithms), so algs is only empty in compatibility mode.
func (h *Hub) jwtParserOptions(algs []string, expectedAudience string) []jwt.ParserOption {
	var opts []jwt.ParserOption

	if len(algs) > 0 {
		opts = append(opts, jwt.WithValidMethods(algs))
	}

	if h.compatClaimsEnabled() {
		return opts
	}

	return append(opts, jwt.WithExpirationRequired(), jwt.WithAudience(expectedAudience))
}

// selectVerifier picks the issuer-specific verifier for a token, using the
// token's unverified iss claim as a selection hint only. An unverified iss can
// only select among the issuer bindings established by trusted configuration;
// it never introduces a key source. Compatibility mode does not check the iss
// claim, so it falls back to the sole configured issuer.
func (h *Hub) selectVerifier(encodedToken string, publish bool) (roleVerifier, error) {
	var pre claims
	if _, _, err := jwt.NewParser().ParseUnverified(encodedToken, &pre); err != nil {
		return roleVerifier{}, fmt.Errorf("%w: %w", ErrInvalidJWT, err)
	}

	iv, ok := h.issuers[pre.Issuer]
	if !ok {
		if !h.compatClaimsEnabled() || len(h.issuers) != 1 {
			return roleVerifier{}, fmt.Errorf("%w: untrusted issuer %q", ErrInvalidJWT, pre.Issuer)
		}

		for _, v := range h.issuers {
			iv = v
		}
	}

	rv := iv.subscriber
	if publish {
		rv = iv.publisher
	}

	if rv.keyfunc == nil {
		return roleVerifier{}, fmt.Errorf("%w: no verifier configured for this role", ErrInvalidJWT)
	}

	return rv, nil
}

// validateJWT parses and validates an access token, returning its claims with
// the mercure authorization details resolved into c.authz.
func (h *Hub) validateJWT(encodedToken string, publish bool, expectedAudience string) (*claims, error) {
	if len(encodedToken) > maxCompactJWSLen {
		return nil, fmt.Errorf("%w: the token exceeds %d bytes", ErrInvalidJWT, maxCompactJWSLen)
	}

	// Fail closed: with no identity to bind the token to, parsing without
	// jwt.WithAudience accepts one audienced anywhere, or carrying no aud at all.
	if expectedAudience == "" && !h.compatClaimsEnabled() {
		return nil, fmt.Errorf("%w: the hub has no resource identifier to check the audience against", ErrInvalidJWT)
	}

	rv, err := h.selectVerifier(encodedToken, publish)
	if err != nil {
		return nil, err
	}

	token, err := jwt.ParseWithClaims(encodedToken, &claims{captureRaw: h.capturesRawClaims(publish)}, rv.keyfunc, h.jwtParserOptions(rv.algorithms, expectedAudience)...)
	if err != nil {
		// Signature, audience, expiration and algorithm failures are all
		// invalid-token conditions; classify them as such for RFC 6750.
		return nil, fmt.Errorf("%w: %w", ErrInvalidJWT, err)
	}

	c, ok := token.Claims.(*claims)
	if !ok || !token.Valid {
		return nil, ErrInvalidJWT
	}

	// RFC 9068: reject tokens not issued as JWT access tokens, so a token
	// minted for another purpose (e.g. an OpenID Connect ID Token) is not
	// accepted. The media type is matched case-insensitively, including the
	// optional "application/" prefix. Relaxed in compatibility mode.
	if h.requireATJWT() {
		typ, _ := token.Header["typ"].(string)

		const mediaTypePrefix = "application/"
		if len(typ) >= len(mediaTypePrefix) && strings.EqualFold(typ[:len(mediaTypePrefix)], mediaTypePrefix) {
			typ = typ[len(mediaTypePrefix):]
		}

		if !strings.EqualFold(typ, atJWTType) {
			return nil, fmt.Errorf(`%w: the "typ" header must be %q`, ErrInvalidJWT, atJWTType)
		}
	}

	// RFC 9068 §4: the verified issuer must be one of the configured issuers.
	// What actually prevents a token signed for one trusted issuer from being
	// accepted under another is selectVerifier, which looks the issuer up before
	// choosing a keyfunc and rejects an unknown one; both parses decode the same
	// payload, so this lookup cannot fail today. It is kept as a guard on that
	// invariant: should selectVerifier ever widen which issuer it falls back to,
	// this is what still refuses the token. Relaxed in compatibility mode, whose
	// fallback to the sole configured issuer is deliberate.
	if !h.compatClaimsEnabled() {
		if _, ok := h.issuers[c.Issuer]; !ok {
			return nil, fmt.Errorf("%w: untrusted issuer %q", ErrInvalidJWT, c.Issuer)
		}
	}

	authz, err := validateAuthorizationDetails(h.topicMatcherStore, c.AuthorizationDetails)
	if err != nil {
		return nil, err
	}

	c.authz = authz

	h.dropLegacyClaims(c)

	// The legacy mercure claim is honored only when the token carries no
	// authorization_details, and only in deprecated_claim builds running in
	// compatibility mode (the stub is a no-op otherwise).
	if len(c.AuthorizationDetails) == 0 {
		if err := h.resolveLegacyClaims(c); err != nil {
			return nil, err
		}
	}

	return c, nil
}
