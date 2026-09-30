package caddy

import (
	"errors"
	"fmt"
	"strings"

	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/dunglas/mercure"
)

// ClaimBindingOptions holds the block options shared by require_claim_header and
// require_claim_value. An empty field selects the core default (auto / reject / all).
type ClaimBindingOptions struct {
	// Which claim shapes are accepted: "auto" (default), "member" or "exact".
	Match string `json:"match,omitempty"`

	// What to do when the header or the claim is absent: "reject" (default) or "allow".
	OnMissing string `json:"on_missing,omitempty"`

	// Which requests the binding applies to: "all" (default), "subscriber" or "publisher".
	Roles string `json:"roles,omitempty"`
}

// ClaimHeaderBindingConfig is the Caddy-facing form of a mercure.ClaimHeaderBinding,
// whose fields stay unexported so a binding can only be built through its validating
// constructor.
type ClaimHeaderBindingConfig struct {
	ClaimBindingOptions

	// The top-level JWT claim to bind, e.g. "groups".
	Claim string `json:"claim"`

	// The HTTP header that must agree with the claim, e.g. "Group-ID".
	Header string `json:"header"`

	// Count connected subscribers per header value in mercure_subscribers_by_binding_value.
	// At most one binding may set it, and it must apply to subscribers.
	CountSubscribers bool `json:"count_subscribers,omitempty"`
}

// ClaimValueBindingConfig is the Caddy-facing form of a binding built by
// mercure.NewClaimValueBinding.
type ClaimValueBindingConfig struct {
	ClaimBindingOptions

	// The top-level JWT claim to check, e.g. "groups".
	Claim string `json:"claim"`

	// The accepted values. The claim must hold at least one of them.
	Values []string `json:"values"`
}

const (
	requireClaimHeader = "require_claim_header"
	requireClaimValue  = "require_claim_value"

	// countSubscribers is a flag with no argument, accepted on require_claim_header only.
	countSubscribers = "count_subscribers"

	// rolesPublisher is the roles value that excludes subscribers.
	rolesPublisher = "publisher"
)

// Claim names, values and header names are never replaced at runtime, so
// `require_claim_value {env.PLANCLAIM} pro` would bind a claim literally named
// "{env.PLANCLAIM}" that no token carries. Any {…} sequence is refused; the advice
// depends on the configuration format.
var (
	// A Caddyfile substitutes {$VAR} as text before tokenizing, so an unset, unquoted
	// variable vanishes and shifts the arguments; quoting it leaves an empty argument,
	// which fails startup instead.
	errCaddyfilePlaceholder = errors.New(`claim names, values and header names are literals and cannot contain {…}, which is never replaced here; to use an environment variable, write "{$VAR}" (quoted, so that an unset variable leaves an empty argument and startup fails) or {$VAR:default}`)
	errJSONPlaceholder      = errors.New("claim names, values and header names are literals and cannot contain {…}, which is never replaced here; JSON takes the literal value")
)

// rejectPlaceholders fails with reason when any of literals holds a "{" followed later by a
// "}", the shape of a Caddy placeholder. The error names the offending literal; callers
// name the directive.
func rejectPlaceholders(reason error, literals ...string) error {
	for _, l := range literals {
		if open := strings.IndexByte(l, '{'); open >= 0 && strings.IndexByte(l[open:], '}') > 0 {
			return fmt.Errorf("%s: %w", l, reason)
		}
	}

	return nil
}

// claimHeaderOption ties a Caddyfile block key to the core option that validates its
// value, the config field it populates, and how that field is read back. Keeping the three
// together means a newly added option cannot be parsed yet never reach the core binding,
// which would silently drop the restriction the operator asked for.
type claimHeaderOption struct {
	key      string
	validate func(string) mercure.BindingOption
	assign   func(*ClaimBindingOptions, string)
	read     func(ClaimBindingOptions) string
}

// claimHeaderOptions returns the supported block keys. A slice, not a map, so the resulting
// option order cannot vary between config loads.
func claimHeaderOptions() []claimHeaderOption {
	return []claimHeaderOption{
		{
			"match", mercure.WithBindingMatch,
			func(c *ClaimBindingOptions, v string) { c.Match = v },
			func(c ClaimBindingOptions) string { return c.Match },
		},
		{
			"on_missing", mercure.WithBindingOnMissing,
			func(c *ClaimBindingOptions, v string) { c.OnMissing = v },
			func(c ClaimBindingOptions) string { return c.OnMissing },
		},
		{
			"roles", mercure.WithBindingRoles,
			func(c *ClaimBindingOptions, v string) { c.Roles = v },
			func(c ClaimBindingOptions) string { return c.Roles },
		},
	}
}

func lookupClaimHeaderOption(key string) (claimHeaderOption, bool) {
	for _, o := range claimHeaderOptions() {
		if o.key == key {
			return o, true
		}
	}

	return claimHeaderOption{}, false
}

// options returns a core option for every field the operator set, leaving the rest at
// their defaults.
func (c ClaimBindingOptions) options() []mercure.BindingOption {
	var opts []mercure.BindingOption

	for _, o := range claimHeaderOptions() {
		if v := o.read(c); v != "" {
			opts = append(opts, o.validate(v))
		}
	}

	return opts
}

// options adds count_subscribers to the shared block options.
func (c ClaimHeaderBindingConfig) options() []mercure.BindingOption {
	opts := c.ClaimBindingOptions.options()

	if c.CountSubscribers {
		opts = append(opts, mercure.WithBindingCountSubscribers())
	}

	return opts
}

// claimHeaderBindings converts the configs into core bindings. This is the only validation
// point for the Caddy JSON path, which never runs the Caddyfile parser. A Caddyfile has
// already rejected the same inputs at parse time, where the error carries a file:line.
func (m *Mercure) claimHeaderBindings() ([]mercure.ClaimHeaderBinding, error) {
	if len(m.ClaimHeaderBindings) == 0 {
		return nil, nil
	}

	bindings := make([]mercure.ClaimHeaderBinding, 0, len(m.ClaimHeaderBindings))

	for _, c := range m.ClaimHeaderBindings {
		if err := rejectPlaceholders(errJSONPlaceholder, c.Claim, c.Header); err != nil {
			return nil, directiveErr(requireClaimHeader, err)
		}

		b, err := mercure.NewClaimHeaderBinding(c.Claim, c.Header, c.options()...)
		if err != nil {
			return nil, fmt.Errorf("require_claim_header %q %q: %w", c.Claim, c.Header, err)
		}

		bindings = append(bindings, b)
	}

	return bindings, nil
}

// claimValueBindings converts the configs into core bindings. Like claimHeaderBindings, it is
// the only validation point for the Caddy JSON path.
func (m *Mercure) claimValueBindings() ([]mercure.ClaimHeaderBinding, error) {
	if len(m.ClaimValueBindings) == 0 {
		return nil, nil
	}

	bindings := make([]mercure.ClaimHeaderBinding, 0, len(m.ClaimValueBindings))

	for _, c := range m.ClaimValueBindings {
		if err := rejectPlaceholders(errJSONPlaceholder, append([]string{c.Claim}, c.Values...)...); err != nil {
			return nil, directiveErr(requireClaimValue, err)
		}

		b, err := mercure.NewClaimValueBinding(c.Claim, c.Values, c.options()...)
		if err != nil {
			return nil, fmt.Errorf("%s %q: %w", requireClaimValue, c.Claim, err)
		}

		bindings = append(bindings, b)
	}

	return bindings, nil
}

// parseRequireClaimHeader parses:
//
//	require_claim_header <claim> <header> [{
//	    match       auto|member|exact
//	    on_missing  reject|allow
//	    roles       all|subscriber|publisher
//	    count_subscribers
//	}]
//
// Parsing is strict: a surplus argument, an unknown or repeated block key and an
// unknown enum value are all errors, since a silently ignored token in an
// authorization check would fail open.
func parseRequireClaimHeader(d *caddyfile.Dispenser) (ClaimHeaderBindingConfig, error) {
	var cfg ClaimHeaderBindingConfig

	if !d.NextArg() {
		return cfg, directiveErr(requireClaimHeader, d.ArgErr())
	}

	cfg.Claim = d.Val()

	if !d.NextArg() {
		return cfg, directiveErr(requireClaimHeader, d.ArgErr())
	}

	cfg.Header = d.Val()

	// NextArg rolls back on "{", so an opening brace is not mistaken for an argument.
	if d.NextArg() {
		return cfg, directiveErr(requireClaimHeader, d.Err("expects exactly two arguments: <claim> <header>"))
	}

	if err := rejectPlaceholders(errCaddyfilePlaceholder, cfg.Claim, cfg.Header); err != nil {
		return cfg, directiveErr(requireClaimHeader, d.WrapErr(err))
	}

	// Build the binding now so an invalid claim or header name, and each block option
	// below, is reported against this directive's file:line. Provision rebuilds it from
	// cfg, the path Caddy JSON also takes.
	binding, err := mercure.NewClaimHeaderBinding(cfg.Claim, cfg.Header)
	if err != nil {
		return cfg, directiveErr(requireClaimHeader, d.WrapErr(err))
	}

	if err := parseClaimBindingBlock(d, requireClaimHeader, &binding, &cfg.ClaimBindingOptions, &cfg.CountSubscribers); err != nil {
		return cfg, err
	}

	// NewHub rejects this too, but without a file:line.
	if cfg.CountSubscribers && cfg.Roles == rolesPublisher {
		backToCountSubscribers(d)

		return cfg, directiveErr(requireClaimHeader, d.Errf("option %q requires roles all or subscriber", countSubscribers))
	}

	return cfg, nil
}

// parseRequireClaimValue parses:
//
//	require_claim_value <claim> <value>... [{
//	    match       auto|member|exact
//	    on_missing  reject|allow
//	    roles       all|subscriber|publisher
//	}]
//
// The block goes through the same parser and option table as require_claim_header.
func parseRequireClaimValue(d *caddyfile.Dispenser) (ClaimValueBindingConfig, error) {
	var cfg ClaimValueBindingConfig

	if !d.NextArg() {
		return cfg, directiveErr(requireClaimValue, d.ArgErr())
	}

	cfg.Claim = d.Val()

	// RemainingArgs stops before "{", so an opening brace is not taken for a value.
	cfg.Values = d.RemainingArgs()
	if len(cfg.Values) == 0 {
		return cfg, directiveErr(requireClaimValue, d.ArgErr())
	}

	if err := rejectPlaceholders(errCaddyfilePlaceholder, append([]string{cfg.Claim}, cfg.Values...)...); err != nil {
		return cfg, directiveErr(requireClaimValue, d.WrapErr(err))
	}

	// Built now so an invalid claim or value list is reported with this directive's
	// file:line. Provision rebuilds it from cfg, the path Caddy JSON also takes.
	binding, err := mercure.NewClaimValueBinding(cfg.Claim, cfg.Values)
	if err != nil {
		return cfg, directiveErr(requireClaimValue, d.WrapErr(err))
	}

	if err := parseClaimBindingBlock(d, requireClaimValue, &binding, &cfg.ClaimBindingOptions, nil); err != nil {
		return cfg, err
	}

	return cfg, nil
}

// parseClaimBindingBlock parses the optional block shared by require_claim_header and
// require_claim_value into opts. Each value is validated against binding, which the caller
// has already constructed. count receives the count_subscribers flag; when it is nil, the
// flag is an unrecognized option. Errors name directive.
func parseClaimBindingBlock(d *caddyfile.Dispenser, directive string, binding *mercure.ClaimHeaderBinding, opts *ClaimBindingOptions, count *bool) error {
	seen := make(map[string]struct{}, 4)

	for nesting := d.Nesting(); d.NextBlock(nesting); {
		key := d.Val()
		if _, dup := seen[key]; dup {
			return directiveErr(directive, d.Errf("duplicate %q option", key))
		}

		seen[key] = struct{}{}

		// A flag, so it is not in the option table.
		if key == countSubscribers && count != nil {
			if d.NextArg() {
				return directiveErr(directive, d.Errf("option %q takes no arguments", key))
			}

			*count = true

			continue
		}

		option, known := lookupClaimHeaderOption(key)
		if !known {
			return directiveErr(directive, d.Errf("unrecognized option %q", key))
		}

		if !d.NextArg() {
			return directiveErr(directive, d.ArgErr())
		}

		value := d.Val()

		if d.NextArg() {
			return directiveErr(directive, d.Errf("option %q takes exactly one argument", key))
		}

		// Validated against the core option so the accepted values live in exactly one
		// place, and a typo fails at config load rather than at request time.
		if err := option.validate(value)(binding); err != nil {
			return directiveErr(directive, d.WrapErr(err))
		}

		option.assign(opts, value)
	}

	return nil
}

// backToCountSubscribers moves d's cursor from the closing brace of the
// binding block just parsed back to that block's count_subscribers flag, so
// that an error about the flag points at its line.
func backToCountSubscribers(d *caddyfile.Dispenser) {
	for d.Val() != countSubscribers {
		if !d.Prev() {
			return
		}
	}
}

// directiveErr names the offending directive. The wrapped Dispenser error already carries
// the file:line, so the two compose into "<directive>: <what>, at <file>:<line>".
func directiveErr(directive string, err error) error {
	return fmt.Errorf("%s: %w", directive, err)
}
