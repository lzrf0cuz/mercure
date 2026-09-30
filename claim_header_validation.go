package mercure

import (
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"unicode"
)

// The AuthzRejectReason taxonomy separates genuine absence from invalid presence, so
// `on_missing allow` can never fail open: only the two "absent" reasons are skippable.
const (
	// ReasonHeaderAbsent: the bound header was not present at all.
	ReasonHeaderAbsent AuthzRejectReason = "header_absent"
	// ReasonClaimAbsent: the bound claim key was missing, null, or empty ("" under a mode
	// that accepts scalars, [] under a mode that accepts arrays).
	ReasonClaimAbsent AuthzRejectReason = "claim_absent"
	// ReasonMismatch: both sides were well-formed, but the claim authorizes neither the
	// header value nor, for a value binding, any of the configured values.
	ReasonMismatch AuthzRejectReason = "mismatch"
	// ReasonMalformed: invalid presence on either side, namely an empty or repeated header
	// value; a claim that is an object/number/bool, an array holding a non-string or
	// empty-string element, an array over maxClaimHeaderValues; a claim whose shape
	// contradicts the match mode (an array under exact, a scalar under member); or a
	// claims object whose raw claims were never captured. Never skippable by on_missing.
	ReasonMalformed AuthzRejectReason = "malformed"
)

const (
	// maxClaimHeaderValues caps a bound claim's array length, separately from
	// maxClaimMatchers. It bounds the authorized-set size, not the decode: an over-cap
	// array is decoded, then rejected. The claim is signature-verified, so only a
	// signing-key holder can send an oversized one.
	maxClaimHeaderValues = 1000
	// maxBindingNameLen bounds claim and header names, which become metric labels.
	maxBindingNameLen = 256
)

var (
	// ErrClaimHeaderRejected wraps every binding rejection. The message carries the
	// binding identity and the reason, never the header or claim values, which would
	// leak into the logs and OpenTelemetry spans.
	ErrClaimHeaderRejected = errors.New("claim-header binding rejected")
	// ErrInvalidClaimHeaderBinding is returned when a binding cannot be constructed.
	ErrInvalidClaimHeaderBinding = errors.New("invalid claim-header binding")
)

// Claims that never hold a string or string array. reservedMercureClaim and
// reservedMercureNamespacedClaim are the legacy mercure object claims (decoded, in
// deprecated_claim builds, by deprecatedMercureClaims), authorizationDetailsClaim is the
// RFC 9396 array of objects. They must match the json tags on the claims struct;
// TestReservedClaimsMatchStructTags checks that.
const (
	reservedMercureClaim           = "mercure"
	reservedMercureNamespacedClaim = "https://mercure.rocks/"
	authorizationDetailsClaim      = "authorization_details"
)

// unbindableClaim reports whether a claim can never hold a string or string array, so a
// binding on it could only ever evaluate malformed. Such a binding is rejected at
// construction rather than allowed to reject every request. The set is exactly the claims
// whose type is known at config time: Mercure's object claims, the authorization_details
// array of objects, and the RFC 7519 numeric-date claims. String claims (sub/iss/jti) and
// the string-or-array aud stay bindable; a custom claim's type is not knowable here, so it
// is only ever caught at runtime.
func unbindableClaim(claim string) bool {
	switch claim {
	case reservedMercureClaim, reservedMercureNamespacedClaim, authorizationDetailsClaim, "exp", "nbf", "iat":
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

// bindingKind names what a binding compares the claim against.
type bindingKind uint8

const (
	kindHeader  bindingKind = iota // a request header (NewClaimHeaderBinding)
	kindLiteral                    // a fixed set of configured values (NewClaimValueBinding)
)

// ClaimHeaderBinding requires that a named request header match a named JWT claim or, when
// built by NewClaimValueBinding, that the claim hold one of a fixed set of values.
// The fields are unexported, so a binding can only be built by a validating constructor.
type ClaimHeaderBinding struct {
	kind   bindingKind
	claim  string
	header string // canonical HTTP field name; kindHeader only
	// values are the configured values of a kindLiteral binding. They are never modified,
	// and the pointer keeps ClaimHeaderBinding comparable.
	values    *[]string
	match     matchMode
	onMissing missingMode
	roles     roleSet
	// countSubscribers labels each subscriber this binding authorizes with the header value it
	// matched, for mercure_subscribers_by_binding_value.
	countSubscribers bool
}

// BindingOption configures a ClaimHeaderBinding.
type BindingOption func(*ClaimHeaderBinding) error

// WithBindingMatch sets which claim shapes the binding accepts: "auto" (default, either
// shape), "member" (array claims only) or "exact" (scalar claims only). A claim of the
// shape the mode forbids is malformed, so it rejects even under on_missing=allow.
// An unknown value is an error, never a silent fallback that would weaken the policy.
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

// WithBindingCountSubscribers counts connected subscribers per value of the binding's header
// in mercure_subscribers_by_binding_value. A subscriber is counted only when the binding
// matched: the header and the claim were present and the claim authorized the header value.
// At most one binding per hub may set it, and it must be a header binding that applies to
// subscribers (roles all or subscriber). NewHub enforces both rules.
func WithBindingCountSubscribers() BindingOption {
	return func(b *ClaimHeaderBinding) error {
		b.countSubscribers = true

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

// NewClaimValueBinding validates and constructs a binding that accepts a request only when
// the claim holds at least one of values. No header is read. The options are the same as
// for NewClaimHeaderBinding. values is copied, so the caller may reuse its slice.
func NewClaimValueBinding(claim string, values []string, opts ...BindingOption) (ClaimHeaderBinding, error) {
	if err := validateClaimName(claim); err != nil {
		return ClaimHeaderBinding{}, err
	}

	if len(values) == 0 {
		return ClaimHeaderBinding{}, fmt.Errorf("%w: at least one value is required", ErrInvalidClaimHeaderBinding)
	}

	for i, v := range values {
		if v == "" {
			return ClaimHeaderBinding{}, fmt.Errorf("%w: values must not be empty", ErrInvalidClaimHeaderBinding)
		}

		if slices.Contains(values[:i], v) {
			return ClaimHeaderBinding{}, fmt.Errorf("%w: duplicate value %q", ErrInvalidClaimHeaderBinding, v)
		}
	}

	b := ClaimHeaderBinding{
		kind:      kindLiteral,
		claim:     claim,
		values:    new(slices.Clone(values)),
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

	// "*" is a valid field name, but in the CORS allowed-headers list it means every
	// request header, so a binding on it would open the preflight to all of them.
	if header == "*" {
		return fmt.Errorf(`%w: header name "*" cannot be bound; it would make the CORS preflight allow every request header`, ErrInvalidClaimHeaderBinding)
	}

	// Over HTTP/1.1 Go moves Host out of the request headers into Request.Host, so a binding
	// would never see it; over HTTP/2 and HTTP/3 a client may send its own host field, which
	// the binding would evaluate. Binding it is never meaningful.
	if strings.EqualFold(header, "Host") {
		return fmt.Errorf(`%w: header name "Host" cannot be bound; Go moves it out of the request headers over HTTP/1.1, and over HTTP/2 and HTTP/3 a client may send its own value`, ErrInvalidClaimHeaderBinding)
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

// WithClaimHeaderBindings enables one or more bindings on the Hub. Bindings must come from
// NewClaimHeaderBinding or NewClaimValueBinding. The slice is copied defensively so a caller
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
// would be silently bypassed. NewHub runs it after the logger and metrics defaults are set,
// which its startup warnings use.
//
// The number of bindings is uncapped. Each contributes one series per reason: at most four to
// mercure_claim_header_rejected_total for a header binding, three to
// mercure_claim_value_rejected_total for a value binding. Bindings come from the
// configuration, never from a request, so cardinality is bounded by the configuration, as it
// is for cors_origins or publish_origins.
func (o *opt) validateClaimHeaderBindings() error {
	if len(o.claimHeaderBindings) == 0 {
		return nil
	}

	var coversSubscriber, coversPublisher bool

	var hasHeader, hasLiteral bool

	// Keyed by kind as well as id: the two kinds report to different counters, and a
	// claim name may itself contain the ":" that separates claim and header in a header id.
	type bindingKey struct {
		kind bindingKind
		id   string
	}

	seen := make(map[bindingKey]struct{}, len(o.claimHeaderBindings))

	for _, b := range o.claimHeaderBindings {
		// A ClaimHeaderBinding{} is constructible from another package (unexported fields
		// stay zero), so refuse anything that didn't come from the constructor.
		if b.claim == "" || (b.kind == kindHeader && b.header == "") {
			return fmt.Errorf("%w: zero-value binding; construct it with NewClaimHeaderBinding or NewClaimValueBinding", ErrInvalidClaimHeaderBinding)
		}

		// Two bindings on the same claim+header, or two value bindings on the same claim,
		// are ambiguous and would collide on the metric label, whatever their other options.
		key := bindingKey{b.kind, b.id()}
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf("%w: overlapping bindings for %s", ErrInvalidClaimHeaderBinding, b.id())
		}

		seen[key] = struct{}{}

		hasHeader = hasHeader || b.kind == kindHeader
		hasLiteral = hasLiteral || b.kind == kindLiteral
		coversSubscriber = coversSubscriber || b.appliesTo(false)
		coversPublisher = coversPublisher || b.appliesTo(true)
	}

	if err := checkCountingBindings(o.claimHeaderBindings); err != nil {
		return err
	}

	// Without a verifier for the role there is no token, so the binding never runs: a
	// silent bypass. Fail loudly at startup instead.
	if coversSubscriber && !o.subscriberConfigured {
		return fmt.Errorf("%w: a subscriber-scoped binding requires a subscriber JWT key", ErrInvalidClaimHeaderBinding)
	}

	if coversPublisher && !o.publisherConfigured {
		return fmt.Errorf("%w: a publisher-scoped binding requires a publisher JWT key", ErrInvalidClaimHeaderBinding)
	}

	if coversSubscriber && o.anonymous {
		o.logger.Warn("A subscriber-scoped claim-header or claim-value binding is configured while anonymous access is enabled; anonymous subscribers carry no token and skip every binding check")
	}

	// The counters are the only aggregate signal of rejections; the per-request log lines
	// cannot be alerted on. A Metrics implementation that cannot report them still enforces
	// the rejection, uncounted, so warn once at startup.
	if _, ok := o.metrics.(AuthorizationRejectionReporter); hasHeader && !ok {
		o.logger.Warn("A claim-header binding is configured but the metrics implementation does not implement AuthorizationRejectionReporter; rejections will be enforced but not counted")
	}

	if _, ok := o.metrics.(ClaimValueRejectionReporter); hasLiteral && !ok {
		o.logger.Warn("A claim-value binding is configured but the metrics implementation does not implement ClaimValueRejectionReporter; rejections will be enforced but not counted")
	}

	return nil
}

// checkCountingBindings enforces the count_subscribers rules: at most one binding counts
// subscribers, and it is a header binding that applies to subscribers.
func checkCountingBindings(bindings []ClaimHeaderBinding) error {
	counting := false

	for _, b := range bindings {
		if !b.countSubscribers {
			continue
		}

		switch {
		case counting:
			return fmt.Errorf("%w: only one binding may count subscribers", ErrInvalidClaimHeaderBinding)
		case b.kind != kindHeader:
			return fmt.Errorf("%w: %s: only a header binding may count subscribers", ErrInvalidClaimHeaderBinding, b.id())
		case !b.appliesTo(false):
			return fmt.Errorf("%w: %s: a binding that counts subscribers must apply to subscribers (roles all or subscriber)", ErrInvalidClaimHeaderBinding, b.id())
		}

		counting = true
	}

	return nil
}

// corsAllowedHeaders returns the CORS allowed request headers: the hub's fixed list plus
// every configured binding header. A browser fetch carrying a bound header triggers a
// preflight, which the fixed list alone would reject. Called only where CORS is installed,
// so an unset cors_origins still means no CORS middleware at all. rs/cors lower-cases and
// de-duplicates the list, so a binding on Authorization is harmless.
func (o *opt) corsAllowedHeaders() []string {
	allowed := []string{authorizationHeader, "cache-control", "last-event-id"}

	for _, b := range o.claimHeaderBindings {
		// A value binding reads no header, so it needs no preflight entry.
		if b.kind == kindHeader {
			allowed = append(allowed, b.header)
		}
	}

	return allowed
}

// id is the stable binding identity used in rejection errors and as the Prometheus label:
// claim:Header for a header binding, the claim alone for a value binding. The Hub rejects
// overlapping bindings of the same kind, so it is unique within a kind.
func (b ClaimHeaderBinding) id() string {
	if b.kind == kindLiteral {
		return b.claim
	}

	return b.claim + ":" + b.header
}

// reportRejection meters a rejection on the counter for the binding's kind. Both reporters
// are opt-in, so a Metrics implementation without the matching one is not metered.
func (b ClaimHeaderBinding) reportRejection(m Metrics, reason AuthzRejectReason) {
	if b.kind == kindLiteral {
		if reporter, ok := m.(ClaimValueRejectionReporter); ok {
			reporter.ClaimValueRejected(b.claim, reason)
		}

		return
	}

	if reporter, ok := m.(AuthorizationRejectionReporter); ok {
		reporter.AuthorizationRejected(b.id(), reason)
	}
}

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

// claimBindingError is the error of every binding rejection. It wraps
// ErrClaimHeaderRejected and records whether the request side (the bound header) was at
// fault, which, with the reason, decides the HTTP answer (see response).
type claimBindingError struct {
	id     string
	reason AuthzRejectReason
	// requestFault is set when the bound header was absent or malformed: the request is
	// malformed whatever the token says.
	requestFault bool
}

func (e *claimBindingError) Error() string {
	return fmt.Sprintf("%s: %s: %s", ErrClaimHeaderRejected, e.id, e.reason)
}

func (*claimBindingError) Unwrap() error {
	return ErrClaimHeaderRejected
}

// response maps a binding rejection to its HTTP status and RFC 6750 error code, per the
// protocol's Error Responses section and RFC 6750 §3.1. It is the only place this mapping
// lives:
//   - the bound header absent or malformed → 400, without a Bearer error code: the request
//     is malformed independently of the token;
//   - a bound claim present but of an unusable type or shape (including a shape the match
//     mode forbids) → 401 invalid_token: a defect of the token itself, as a malformed
//     authorization_details claim is;
//   - a claim that does not authorize the header value or any configured value, or a bound
//     claim absent under on_missing reject → 403 insufficient_scope: the token is valid but
//     does not authorize the request.
func (e *claimBindingError) response() (status int, code string) {
	switch {
	case e.requestFault:
		return http.StatusBadRequest, ""
	case e.reason == ReasonMalformed:
		return http.StatusUnauthorized, bearerErrInvalidToken
	default:
		return http.StatusForbidden, bearerErrInsufficientScope
	}
}

func (b ClaimHeaderBinding) reject(reason AuthzRejectReason, requestFault bool) (AuthzRejectReason, error) {
	return reason, &claimBindingError{id: b.id(), reason: reason, requestFault: requestFault}
}

// bindingEval carries one binding's evaluation from tier to tier (see
// enforceBindings): the requested values checkRequest returned (nil when
// on_missing=allow let the header be absent), then the claim's state and values.
type bindingEval struct {
	requested []string
	claim     operand
	allowed   []string
}

// checkRequest is tier 1, the request side of the binding, which never depends on the token:
// the bound header's single value, or the configured set of a value binding. A malformed
// header rejects even under on_missing=allow; an absent one rejects under on_missing=reject.
// Both are request faults (400). requested is nil when on_missing=allow let the header be
// absent.
func (b ClaimHeaderBinding) checkRequest(r *http.Request) (requested []string, reason AuthzRejectReason, err error) {
	state, requested := b.evalRequested(r)

	switch {
	case state == operandMalformed:
		reason, err = b.reject(ReasonMalformed, true)
	case state == operandAbsent && b.onMissing == onMissingReject:
		reason, err = b.reject(ReasonHeaderAbsent, true)
	default:
		return requested, "", nil
	}

	return nil, reason, err
}

// checkClaimShape is tier 2: a bound claim of unusable type or shape makes the token invalid
// (401). It rejects even under on_missing=allow, and even when on_missing=allow let the
// header be absent, so allow can never skip past a malformed claim.
func (b ClaimHeaderBinding) checkClaimShape(c *claims) (claim operand, allowed []string, reason AuthzRejectReason, err error) {
	claim, allowed = b.evalClaim(c)
	if claim == operandMalformed {
		reason, err = b.reject(ReasonMalformed, false)

		return claim, nil, reason, err
	}

	return claim, allowed, "", nil
}

// checkScope is tier 3, reached with a well-formed request side and claim: genuine absence of
// either side, the only thing on_missing=allow may skip, then the comparison. Its rejections
// are scope failures of a valid token (403).
func (b ClaimHeaderBinding) checkScope(ev bindingEval) (matched string, reason AuthzRejectReason, err error) {
	switch {
	case ev.requested == nil:
		// The header is absent and on_missing=allow skips the binding.
		return "", "", nil
	case ev.claim == operandAbsent && b.onMissing == onMissingAllow:
		return "", "", nil
	case ev.claim == operandAbsent:
		reason, err = b.reject(ReasonClaimAbsent, false)

		return "", reason, err
	}

	// Every allowed shape reduces to membership: a scalar claim (auto/exact) is a
	// one-element set, an array claim (auto/member) is the set itself. The request is
	// authorized when the two sets intersect; a header is a one-element set.
	for _, v := range ev.requested {
		if slices.Contains(ev.allowed, v) {
			return v, "", nil
		}
	}

	reason, err = b.reject(ReasonMismatch, false)

	return "", reason, err
}

// evalRequested returns the values the claim must authorize: the bound header's single
// value, or the configured set of a value binding.
func (b ClaimHeaderBinding) evalRequested(r *http.Request) (operand, []string) {
	if b.kind == kindLiteral {
		return operandOK, *b.values
	}

	return b.evalHeader(r)
}

// evalHeader requires exactly one non-empty value. Header.Values (not Get) is used so a
// repeated header is detected rather than silently taking the first.
func (b ClaimHeaderBinding) evalHeader(r *http.Request) (operand, []string) {
	values := r.Header.Values(b.header)

	switch {
	case len(values) == 0:
		return operandAbsent, nil
	case len(values) > 1:
		return operandMalformed, nil
	case values[0] == "":
		return operandMalformed, nil
	}

	return operandOK, values
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
	if err := jsonv2.Unmarshal(raw, &decoded); err != nil {
		return operandMalformed, nil
	}

	switch value := decoded.(type) {
	case nil: // JSON null
		return operandAbsent, nil
	case string:
		// member authorizes membership of a set, so a scalar is the wrong operand shape. It is
		// checked before emptiness because the shape disagreement is the stronger signal.
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
	// exact authorizes one scalar value, so an array is the wrong operand shape. It is checked
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
