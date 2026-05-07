package mercure

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"strings"

	"github.com/golang-jwt/jwt/v5"
)

// claims contains Mercure's JWT claims.
type claims struct {
	jwt.RegisteredClaims

	Mercure mercureClaim `json:"mercure"`
	// Optional fallback
	MercureNamespaced *mercureClaim `json:"https://mercure.rocks/"`

	// captureRaw asks UnmarshalJSON to populate rawClaims. Set by validateJWT only when a
	// claim-header binding applies to this request, so a deployment that configures none —
	// or whose bindings are scoped to the other role — pays neither the decode nor the map.
	captureRaw bool

	// rawClaims holds every top-level claim as raw JSON so a configured claim-header
	// binding (claim_header_validation.go) can read an arbitrary claim by name without a
	// second JWT verify. Unexported, so encoding/json neither reads nor writes it and it
	// can never round-trip into a marshalled token.
	//
	// nil in three distinct situations, which evalClaim must keep apart: before the token is
	// parsed, when no binding applies to the request, and after authorizeAndBind has
	// consumed the map and released it. Only an anonymous request (nil *claims) means "no
	// claim"; a nil map on a live *claims is malformed, never absent.
	rawClaims map[string]json.RawMessage
}

// UnmarshalJSON decodes the typed Mercure claims via an alias type, to avoid recursing into
// this method, and — when captureRaw is set — takes a second pass to record every top-level
// claim as raw JSON. Neither pass verifies anything: both run inside jwt.ParseWithClaims,
// which checks the signature afterwards, and validateJWT discards the claims unless that
// check passed. So rawClaims never escapes for an unverified token, and the token is still
// verified exactly once. json.RawMessage.UnmarshalJSON appends into a fresh slice rather
// than aliasing the decoder's buffer, so rawClaims is safe to retain.
//
// captureRaw is read off the receiver, which relies on jwt.ParseWithClaims decoding into the
// instance validateJWT handed it. A decoder that allocated a fresh claims value would read
// captureRaw as false and never build the map — TestRawClaimsAreCapturedOnlyWhenBoundAnd
// ReleasedAfterUse fails loudly if that ever changes.
//
// The MercureNamespaced fallback is applied later in validateJWT and is unaffected.
func (c *claims) UnmarshalJSON(data []byte) error {
	type claimsAlias claims // no UnmarshalJSON method → default struct decoding

	var typed claimsAlias
	if err := json.Unmarshal(data, &typed); err != nil {
		return fmt.Errorf("unable to decode JWT claims: %w", err)
	}

	captureRaw := c.captureRaw // survives the overwrite below
	*c = claims(typed)
	c.captureRaw = captureRaw

	if !captureRaw {
		return nil
	}

	raw := make(map[string]json.RawMessage)
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("unable to capture raw JWT claims: %w", err)
	}

	c.rawClaims = raw

	return nil
}

type mercureClaim struct {
	Publish   []string `json:"publish"`
	Subscribe []string `json:"subscribe"`
	Payload   any      `json:"payload"`
}

type role int

const (
	defaultCookieName = "mercureAuthorization"
	bearerPrefix      = "Bearer "
)

const (
	roleSubscriber role = iota
	rolePublisher
)

var (
	// ErrInvalidAuthorizationHeader is returned when the Authorization header is invalid.
	ErrInvalidAuthorizationHeader = errors.New(`invalid "Authorization" HTTP header`)
	// ErrInvalidAuthorizationQuery is returned when the authorization query parameter is invalid.
	ErrInvalidAuthorizationQuery = errors.New(`invalid "authorization" Query parameter`)
	// ErrNoOrigin is returned when the cookie authorization mechanism is used and no Origin nor Referer headers are presents.
	ErrNoOrigin = errors.New(`an "Origin" or a "Referer" HTTP header must be present to use the cookie-based authorization mechanism`)
	// ErrOriginNotAllowed is returned when the Origin is not allowed to post updates.
	ErrOriginNotAllowed = errors.New("origin not allowed to post updates")
	// ErrInvalidJWT is returned when the JWT is invalid.
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
	var jwtKeyfunc jwt.Keyfunc
	if publish {
		jwtKeyfunc = h.publisherJWTKeyFunc
	} else {
		jwtKeyfunc = h.subscriberJWTKeyFunc
	}

	// Raw top-level claims are read only by the bindings that apply to this request, so a
	// publisher-scoped binding must not make every subscribe request pay the capture. This
	// predicate is exactly the one authorizeAndBind's loop uses, which is what guarantees
	// rawClaims is populated whenever a binding will actually read it.
	captureRaw := slices.ContainsFunc(h.claimHeaderBindings, func(b ClaimHeaderBinding) bool {
		return b.appliesTo(publish)
	})

	authorizationHeaders, authorizationHeaderExists := r.Header["Authorization"]
	if authorizationHeaderExists {
		if len(authorizationHeaders) != 1 || len(authorizationHeaders[0]) < 48 || authorizationHeaders[0][:7] != bearerPrefix {
			return nil, ErrInvalidAuthorizationHeader
		}

		return validateJWT(authorizationHeaders[0][7:], jwtKeyfunc, captureRaw)
	}

	if authorizationQuery, queryExists := r.URL.Query()["authorization"]; queryExists {
		if len(authorizationQuery) != 1 || len(authorizationQuery[0]) < 41 {
			return nil, ErrInvalidAuthorizationQuery
		}

		return validateJWT(authorizationQuery[0], jwtKeyfunc, captureRaw)
	}

	cookie, err := r.Cookie(h.cookieName)
	if err != nil {
		// Anonymous
		return nil, nil //nolint:nilerr,nilnil
	}

	// CSRF attacks cannot occur when using safe methods
	if r.Method != http.MethodPost {
		return validateJWT(cookie.Value, jwtKeyfunc, captureRaw)
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
		return validateJWT(cookie.Value, jwtKeyfunc, captureRaw)
	}

	if slices.Contains(h.publishOrigins, origin) {
		return validateJWT(cookie.Value, jwtKeyfunc, captureRaw)
	}

	for _, allowedOrigin := range h.publishWOrigins {
		if allowedOrigin.match(origin) {
			return validateJWT(cookie.Value, jwtKeyfunc, captureRaw)
		}
	}

	return nil, fmt.Errorf("%q: %w", origin, ErrOriginNotAllowed)
}

// authorizeAndBind authorizes the request and then enforces every applicable claim-header
// binding. It is the single enforcement site: authorize() has several `return
// validateJWT(...)` points, one per credential carrier, so a hook inside it would silently
// miss whichever carriers it did not cover. Wrapping authorize() cannot miss any.
//
// A nil claims means the request is anonymous — it carries no token, therefore no claim to
// bind — so bindings are skipped. Rejections are returned as errors and rendered by each
// caller's own 401 path.
func (h *Hub) authorizeAndBind(r *http.Request, publish bool) (*claims, error) {
	c, err := h.authorize(r, publish)
	if err != nil || c == nil {
		return c, err
	}

	for _, b := range h.claimHeaderBindings {
		if !b.appliesTo(publish) {
			continue
		}

		reason, err := b.validate(r, c)
		if err != nil {
			// Opt-in: a Metrics implementation without the reporter isn't metered.
			if reporter, ok := h.metrics.(AuthorizationRejectionReporter); ok {
				reporter.AuthorizationRejected(b.id(), reason)
			}

			return nil, err
		}
	}

	// The bindings have consumed the raw claims and nothing downstream reads them, yet a
	// subscriber pins its *claims for the lifetime of the connection. Release the map here
	// rather than retain a decoded copy of every top-level claim per connected subscriber.
	// evalClaim treats a stripped claims object as malformed, so a hypothetical second
	// evaluation fails closed instead of skipping the binding.
	c.rawClaims = nil

	return c, nil
}

// ErrTooManyClaimMatchers is returned when mercure.subscribe or
// mercure.publish exceeds maxClaimMatchers.
var ErrTooManyClaimMatchers = errors.New("too many matchers in mercure claim")

// validateJWT validates that the provided JWT token is a valid Mercure token. captureRaw
// asks the claims decoder to also record every top-level claim as raw JSON, which only a
// configured claim-header binding needs.
func validateJWT(encodedToken string, jwtKeyfunc jwt.Keyfunc, captureRaw bool) (*claims, error) {
	token, err := jwt.ParseWithClaims(encodedToken, &claims{captureRaw: captureRaw}, jwtKeyfunc)
	if err != nil {
		return nil, fmt.Errorf("unable to parse JWT: %w", err)
	}

	c, ok := token.Claims.(*claims)
	if !ok || !token.Valid {
		return nil, ErrInvalidJWT
	}

	if c.MercureNamespaced != nil {
		c.Mercure = *c.MercureNamespaced
	}

	if len(c.Mercure.Publish) > maxClaimMatchers || len(c.Mercure.Subscribe) > maxClaimMatchers {
		return nil, ErrTooManyClaimMatchers
	}

	return c, nil
}

func canReceive(s *TopicSelectorStore, topics, topicSelectors []string) bool {
	for _, topic := range topics {
		for _, topicSelector := range topicSelectors {
			if s.match(topic, topicSelector) {
				return true
			}
		}
	}

	return false
}

func canDispatch(s *TopicSelectorStore, topics, topicSelectors []string) bool {
	for _, topic := range topics {
		var matched bool

		for _, topicSelector := range topicSelectors {
			if topicSelector == "*" {
				return true
			}

			if s.match(topic, topicSelector) {
				matched = true

				break
			}
		}

		if !matched {
			return false
		}
	}

	return true
}

func (h *Hub) httpAuthorizationError(w http.ResponseWriter, r *http.Request, err error) {
	http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)

	ctx := r.Context()
	if h.logger.Enabled(ctx, slog.LevelDebug) {
		h.logger.LogAttrs(ctx, slog.LevelDebug, "Topic selectors not matched, not provided or authorization error", slog.Any("error", err))
	}
}
