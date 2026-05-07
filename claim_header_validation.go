package mercure

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"unicode"
)

// AuthzRejectReason classifies why a claim-header binding rejected a request. The
// taxonomy is closed (bounded Prometheus label cardinality) and deliberately separates
// *genuine absence* from *invalid presence*, so `on_missing allow` can never fail open:
// only the two "absent" reasons are skippable.
type AuthzRejectReason string

const (
	// ReasonHeaderAbsent: the bound header was not present at all.
	ReasonHeaderAbsent AuthzRejectReason = "header_absent"
	// ReasonClaimAbsent: the bound claim key was missing, null, or empty ("" under a mode
	// that accepts scalars, [] under a mode that accepts arrays).
	ReasonClaimAbsent AuthzRejectReason = "claim_absent"
	// ReasonMismatch: header and claim were both well-formed, but the header value is
	// not authorized by the claim.
	ReasonMismatch AuthzRejectReason = "mismatch"
	// ReasonMalformed: invalid presence on either side — an empty or repeated header
	// value; a claim that is an object/number/bool, an array holding a non-string or
	// empty-string element, an array over maxClaimHeaderValues; a claim whose shape
	// contradicts the match mode (an array under exact, a scalar under member); or a
	// claims object whose raw claims were never captured. Never skippable by on_missing.
	ReasonMalformed AuthzRejectReason = "malformed"
)

const (
	// maxClaimHeaderValues caps a bound claim's array length. Deliberately separate from
	// maxClaimMatchers, which carries mercure.publish/subscribe semantics. It bounds the
	// authorized-SET size, not the decode: an over-cap array is materialized (like upstream's
	// maxClaimMatchers) then rejected. The bytes are signature-verified, so an unbounded claim
	// is a signing-key holder's DoS, out of scope. A raw-byte cap to bound the decode is
	// deliberately not used: at any value consistent with this element cap it would
	// false-reject a legitimate under-cap array of long values.
	maxClaimHeaderValues = 1000
	// maxBindingNameLen bounds claim/header names so metric labels stay sane.
	maxBindingNameLen = 256
)

var (
	// ErrClaimHeaderRejected wraps every binding rejection. The message carries the
	// binding identity and the reason — never the header or claim VALUES, which would
	// leak into Debug logs and OpenTelemetry spans.
	ErrClaimHeaderRejected = errors.New("claim-header binding rejected")
	// ErrInvalidClaimHeaderBinding is returned when a binding cannot be constructed.
	ErrInvalidClaimHeaderBinding = errors.New("invalid claim-header binding")
)

// reservedMercureClaim and reservedMercureNamespacedClaim are the claims Mercure decodes for
// its OWN protocol use (claims.Mercure and its namespaced fallback), both objects. These MUST
// match the json tags on the claims struct; a canary test pins that.
const (
	reservedMercureClaim           = "mercure"
	reservedMercureNamespacedClaim = "https://mercure.rocks/"
)

// unbindableClaim reports whether a claim can never hold a string or string array, so a
// binding on it could only ever evaluate malformed. Such a binding is rejected at
// construction rather than allowed to 401 every request. The set is exactly the claims whose
// type is known at config time: Mercure's two object claims, and the RFC 7519 numeric-date
// claims. String claims (sub/iss/jti) and the string-or-array aud stay bindable; a custom
// claim's type is not knowable here, so it is only ever caught at runtime.
func unbindableClaim(claim string) bool {
	switch claim {
	case reservedMercureClaim, reservedMercureNamespacedClaim, "exp", "nbf", "iat":
		return true
	default:
		return false
	}
}

// matchMode selects which claim shapes a binding accepts. The three modes are pairwise
// distinct: a mode that merely aliased another would let an operator believe they had
// constrained the policy when they had not.
type matchMode uint8

const (
	matchAuto   matchMode = iota // either shape: scalar compares equal, array tests membership
	matchMember                  // array only; a scalar claim is malformed
	matchExact                   // scalar only; an array claim is malformed
)

type missingMode uint8

const (
	onMissingReject missingMode = iota
	onMissingAllow
)

type roleSet uint8

const (
	rolesAll roleSet = iota
	rolesSubscriber
	rolesPublisher
)

// operand is the evaluated state of one side (header or claim) of a binding.
type operand uint8

const (
	operandOK operand = iota
	operandAbsent
	operandMalformed
)

// ClaimHeaderBinding requires that a named request header match a named JWT claim.
// It is *configured*, not implemented, by consumers: the fields are unexported and the
// only constructor validates them. This is data/config API, not a validator hook.
type ClaimHeaderBinding struct {
	claim     string
	header    string // canonical HTTP field name
	match     matchMode
	onMissing missingMode
	roles     roleSet
}

// BindingOption configures a ClaimHeaderBinding.
type BindingOption func(*ClaimHeaderBinding) error

// WithBindingMatch sets which claim shapes the binding accepts: "auto" (default, either
// shape), "member" (array claims only) or "exact" (scalar claims only). A claim of the
// shape the mode forbids is malformed, so it rejects even under on_missing=allow.
// An unknown value is an error — never a silent fallback, which would weaken the policy.
func WithBindingMatch(mode string) BindingOption {
	return func(b *ClaimHeaderBinding) error {
		switch mode {
		case "auto":
			b.match = matchAuto
		case "member":
			b.match = matchMember
		case "exact":
			b.match = matchExact
		default:
			return fmt.Errorf("%w: unknown match %q (want auto|member|exact)", ErrInvalidClaimHeaderBinding, mode)
		}

		return nil
	}
}

// WithBindingOnMissing sets behavior when the header or claim is genuinely absent:
// "reject" (default, fail-closed) or "allow" (skips the binding, weakening it).
func WithBindingOnMissing(mode string) BindingOption {
	return func(b *ClaimHeaderBinding) error {
		switch mode {
		case "reject":
			b.onMissing = onMissingReject
		case "allow":
			b.onMissing = onMissingAllow
		default:
			return fmt.Errorf("%w: unknown on_missing %q (want reject|allow)", ErrInvalidClaimHeaderBinding, mode)
		}

		return nil
	}
}

// WithBindingRoles restricts the binding to "all" (default), "subscriber" or "publisher".
func WithBindingRoles(roles string) BindingOption {
	return func(b *ClaimHeaderBinding) error {
		switch roles {
		case "all":
			b.roles = rolesAll
		case "subscriber":
			b.roles = rolesSubscriber
		case "publisher":
			b.roles = rolesPublisher
		default:
			return fmt.Errorf("%w: unknown roles %q (want all|subscriber|publisher)", ErrInvalidClaimHeaderBinding, roles)
		}

		return nil
	}
}

// NewClaimHeaderBinding validates and constructs a binding. Set-level checks
// (duplicates/overlap) belong to the Hub, which sees every binding.
func NewClaimHeaderBinding(claim, header string, opts ...BindingOption) (ClaimHeaderBinding, error) {
	if err := validateClaimName(claim); err != nil {
		return ClaimHeaderBinding{}, err
	}

	if err := validateHeaderName(header); err != nil {
		return ClaimHeaderBinding{}, err
	}

	b := ClaimHeaderBinding{
		claim:     claim,
		header:    http.CanonicalHeaderKey(header),
		match:     matchAuto,
		onMissing: onMissingReject,
		roles:     rolesAll,
	}

	for _, o := range opts {
		if err := o(&b); err != nil {
			return ClaimHeaderBinding{}, err
		}
	}

	return b, nil
}

func validateClaimName(claim string) error {
	if claim == "" {
		return fmt.Errorf("%w: claim name must not be empty", ErrInvalidClaimHeaderBinding)
	}

	if unbindableClaim(claim) {
		return fmt.Errorf("%w: claim %q cannot be bound; it never holds a string or string array", ErrInvalidClaimHeaderBinding, claim)
	}

	if len(claim) > maxBindingNameLen {
		return fmt.Errorf("%w: claim name longer than %d bytes", ErrInvalidClaimHeaderBinding, maxBindingNameLen)
	}

	// Caddy JSON can carry strings a Caddyfile token cannot; the name reaches error
	// text and the Prometheus binding label.
	for _, r := range claim {
		if unicode.IsControl(r) {
			return fmt.Errorf("%w: claim name contains a control character", ErrInvalidClaimHeaderBinding)
		}
	}

	return nil
}

// validateHeaderName enforces an RFC 7230 field-name token. http.CanonicalHeaderKey
// normalizes but does not validate, so an invalid name would silently never match.
func validateHeaderName(header string) error {
	if header == "" {
		return fmt.Errorf("%w: header name must not be empty", ErrInvalidClaimHeaderBinding)
	}

	if len(header) > maxBindingNameLen {
		return fmt.Errorf("%w: header name longer than %d bytes", ErrInvalidClaimHeaderBinding, maxBindingNameLen)
	}

	for i := range len(header) {
		if !isTokenChar(header[i]) {
			return fmt.Errorf("%w: header name is not a valid HTTP field name", ErrInvalidClaimHeaderBinding)
		}
	}

	return nil
}

func isTokenChar(c byte) bool {
	switch {
	case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		return true
	}

	switch c {
	case '!', '#', '$', '%', '&', '\'', '*', '+', '-', '.', '^', '_', '`', '|', '~':
		return true
	}

	return false
}

// WithClaimHeaderBindings enables one or more claim-header bindings on the Hub. Bindings
// must come from NewClaimHeaderBinding. The slice is copied defensively so a caller
// mutating its own backing array after NewHub cannot change the Hub's policy.
//
// Passing none (the default) leaves authorization behavior unchanged.
func WithClaimHeaderBindings(bindings ...ClaimHeaderBinding) Option {
	return func(o *opt) error {
		o.claimHeaderBindings = slices.Clone(bindings)

		return nil
	}
}

// validateClaimHeaderBindings performs the set-level checks a single-binding constructor
// cannot: zero-value detection, overlap, and the JWT preconditions without which a binding
// would be silently bypassed. Called by NewHub, so Go embedders and the Caddy adapter get
// identical protection.
//
// The number of bindings is deliberately uncapped. Each contributes at most four series to
// mercure_claim_header_rejected_total (one per reason), and every binding comes from the
// configuration, never from a request — so cardinality is bounded by whatever the operator
// wrote, exactly as it is for cors_origins or publish_origins.
func (o *opt) validateClaimHeaderBindings() error {
	if len(o.claimHeaderBindings) == 0 {
		return nil
	}

	var coversSubscriber, coversPublisher bool

	seen := make(map[string]struct{}, len(o.claimHeaderBindings))

	for _, b := range o.claimHeaderBindings {
		// A ClaimHeaderBinding{} is constructible from another package (unexported fields
		// stay zero), so refuse anything that didn't come from the constructor.
		if b.claim == "" || b.header == "" {
			return fmt.Errorf("%w: zero-value binding; construct it with NewClaimHeaderBinding", ErrInvalidClaimHeaderBinding)
		}

		// Two bindings on the same claim+header are ambiguous and would collide on the
		// metric label, whatever their other options.
		if _, duplicate := seen[b.id()]; duplicate {
			return fmt.Errorf("%w: overlapping bindings for %s", ErrInvalidClaimHeaderBinding, b.id())
		}

		seen[b.id()] = struct{}{}

		coversSubscriber = coversSubscriber || b.appliesTo(false)
		coversPublisher = coversPublisher || b.appliesTo(true)
	}

	// Without the corresponding JWT there is no token, so the binding never runs: a
	// silent bypass. Fail loudly at startup instead.
	if coversSubscriber && o.subscriberJWTKeyFunc == nil {
		return fmt.Errorf("%w: a subscriber-scoped binding requires a subscriber JWT key", ErrInvalidClaimHeaderBinding)
	}

	if coversPublisher && o.publisherJWTKeyFunc == nil {
		return fmt.Errorf("%w: a publisher-scoped binding requires a publisher JWT key", ErrInvalidClaimHeaderBinding)
	}

	if coversSubscriber && o.anonymous {
		o.logger.Warn("A subscriber-scoped claim-header binding is configured while anonymous access is enabled; anonymous requests carry no token and therefore skip the binding, so the bound header is not trustworthy for metrics, rate limiting or routing")
	}

	// Rejections are Debug-logged, so the counter is their only production signal. A Metrics
	// implementation that cannot report them still enforces the 401 — it just does so
	// invisibly. Say that once at startup rather than let an operator discover it during an
	// incident.
	if _, ok := o.metrics.(AuthorizationRejectionReporter); !ok {
		o.logger.Warn("A claim-header binding is configured but the metrics implementation does not implement AuthorizationRejectionReporter; rejections will be enforced but not counted")
	}

	return nil
}

// baseCORSAllowedHeaders is the request-header allowlist the hub exposes regardless of
// configuration. Returned fresh on each call because corsAllowedHeaders appends to it.
func baseCORSAllowedHeaders() []string {
	return []string{"authorization", "cache-control", "last-event-id"}
}

// corsAllowedHeaders extends the CORS allowed-request-headers list with every configured
// binding header. A browser fetch carrying a bound header (a fetch-based SSE client can
// set one; native EventSource cannot) triggers a preflight, which the hub's otherwise
// fixed list would reject.
//
// Callers invoke this only where CORS is actually installed, so an unset cors_origins
// still means no CORS middleware at all. De-duplicated case-insensitively: a binding on
// Authorization must not duplicate the existing entry, because the preflight
// Access-Control-Allow-Headers string is asserted verbatim.
func (o *opt) corsAllowedHeaders() []string {
	allowed := baseCORSAllowedHeaders()

	if len(o.claimHeaderBindings) == 0 {
		return allowed
	}

	seen := make(map[string]struct{}, len(allowed)+len(o.claimHeaderBindings))
	for _, h := range allowed {
		seen[strings.ToLower(h)] = struct{}{}
	}

	for _, b := range o.claimHeaderBindings {
		name := strings.ToLower(b.header)
		if _, duplicate := seen[name]; duplicate {
			continue
		}

		seen[name] = struct{}{}
		allowed = append(allowed, name) // the base list is lowercase; stay consistent
	}

	return allowed
}

// id is the stable binding identity used as the Prometheus label. The Hub rejects
// overlapping (same claim+header) bindings, so it is unique.
func (b ClaimHeaderBinding) id() string { return b.claim + ":" + b.header }

// appliesTo reports whether the binding is enforced for this role.
func (b ClaimHeaderBinding) appliesTo(publish bool) bool {
	switch b.roles {
	case rolesAll:
		return true
	case rolesPublisher:
		return publish
	case rolesSubscriber:
		return !publish
	}

	return false
}

func (b ClaimHeaderBinding) reject(reason AuthzRejectReason) (AuthzRejectReason, error) {
	return reason, fmt.Errorf("%w: %s: %s", ErrClaimHeaderRejected, b.id(), reason)
}

// validate enforces the binding against a request and its verified claims. It returns
// ("", nil) when the request is authorized or the binding is skipped, and (reason, err)
// on rejection. Precedence is fixed: malformed (either side) → absent → compare.
func (b ClaimHeaderBinding) validate(r *http.Request, c *claims) (AuthzRejectReason, error) {
	headerState, headerValue := b.evalHeader(r)
	claimState, allowed := b.evalClaim(c)

	// 1. Invalid presence always rejects, even under on_missing=allow.
	if headerState == operandMalformed || claimState == operandMalformed {
		return b.reject(ReasonMalformed)
	}

	// 2. Genuine absence is the only thing on_missing=allow may skip.
	if headerState == operandAbsent {
		if b.onMissing == onMissingAllow {
			return "", nil
		}

		return b.reject(ReasonHeaderAbsent)
	}

	if claimState == operandAbsent {
		if b.onMissing == onMissingAllow {
			return "", nil
		}

		return b.reject(ReasonClaimAbsent)
	}

	// 3. Compare. Every allowed shape reduces to membership: a scalar claim (auto/exact)
	// is a one-element set, an array claim (auto/member) is the set itself.
	if slices.Contains(allowed, headerValue) {
		return "", nil
	}

	return b.reject(ReasonMismatch)
}

// evalHeader requires exactly one non-empty value. Header.Values (not Get) is used so a
// repeated header is detected rather than silently taking the first.
func (b ClaimHeaderBinding) evalHeader(r *http.Request) (operand, string) {
	values := r.Header.Values(b.header)

	switch {
	case len(values) == 0:
		return operandAbsent, ""
	case len(values) > 1:
		return operandMalformed, ""
	case values[0] == "":
		return operandMalformed, ""
	}

	return operandOK, values[0]
}

// evalClaim decodes the bound claim into the set of authorized values.
func (b ClaimHeaderBinding) evalClaim(c *claims) (operand, []string) {
	// An anonymous request carries no token, so it carries no claim to bind.
	if c == nil {
		return operandAbsent, nil
	}

	// A token was parsed, yet its raw claims are gone: either the capture was never
	// requested or authorizeAndBind already consumed and cleared them. Neither is a claim
	// that is genuinely absent, so this must never be skippable by on_missing=allow.
	if c.rawClaims == nil {
		return operandMalformed, nil
	}

	raw, ok := c.rawClaims[b.claim]
	if !ok {
		return operandAbsent, nil
	}

	var decoded any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return operandMalformed, nil
	}

	switch value := decoded.(type) {
	case nil: // JSON null
		return operandAbsent, nil
	case string:
		// member authorizes membership of a set, so a scalar is the wrong operand shape —
		// checked before emptiness, because the shape disagreement is the stronger signal.
		if b.match == matchMember {
			return operandMalformed, nil
		}

		if value == "" {
			return operandAbsent, nil
		}

		return operandOK, []string{value}
	case []any:
		return b.evalClaimArray(value)
	default: // object, number, bool
		return operandMalformed, nil
	}
}

func (b ClaimHeaderBinding) evalClaimArray(items []any) (operand, []string) {
	// exact authorizes one scalar value, so an array is the wrong operand shape — checked
	// before emptiness, or `[]` under exact would read as a genuinely absent claim and
	// become skippable by on_missing=allow.
	if b.match == matchExact {
		return operandMalformed, nil
	}

	if len(items) == 0 {
		return operandAbsent, nil
	}

	if len(items) > maxClaimHeaderValues {
		return operandMalformed, nil
	}

	allowed := make([]string, 0, len(items))

	for _, item := range items {
		s, ok := item.(string)
		if !ok || s == "" {
			return operandMalformed, nil
		}

		allowed = append(allowed, s)
	}

	return operandOK, allowed
}
