package caddy

import (
	"fmt"

	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/dunglas/mercure"
)

// ClaimHeaderBindingConfig is the Caddy-facing form of a mercure.ClaimHeaderBinding,
// whose fields stay unexported so a binding can only be built through its validating
// constructor. An empty Match, OnMissing or Roles selects the core default
// (auto / reject / all).
type ClaimHeaderBindingConfig struct {
	// The top-level JWT claim to bind, e.g. "tenants".
	Claim string `json:"claim"`

	// The HTTP header that must agree with the claim, e.g. "Tenant-ID".
	Header string `json:"header"`

	// How the header value is compared to the claim: "auto" (default), "member" or "exact".
	Match string `json:"match,omitempty"`

	// What to do when the header or the claim is absent: "reject" (default) or "allow".
	OnMissing string `json:"on_missing,omitempty"`

	// Which requests the binding applies to: "all" (default), "subscriber" or "publisher".
	Roles string `json:"roles,omitempty"`
}

// claimHeaderOption ties a Caddyfile block key to the core option that validates its
// value, the config field it populates, and how that field is read back. Keeping the three
// together means a newly added option cannot be parsed yet never reach the core binding,
// which would silently drop the restriction the operator asked for.
type claimHeaderOption struct {
	key      string
	validate func(string) mercure.BindingOption
	assign   func(*ClaimHeaderBindingConfig, string)
	read     func(ClaimHeaderBindingConfig) string
}

// claimHeaderOptions returns the supported block keys. A slice, not a map, so the resulting
// option order cannot vary between config loads.
func claimHeaderOptions() []claimHeaderOption {
	return []claimHeaderOption{
		{
			"match", mercure.WithBindingMatch,
			func(c *ClaimHeaderBindingConfig, v string) { c.Match = v },
			func(c ClaimHeaderBindingConfig) string { return c.Match },
		},
		{
			"on_missing", mercure.WithBindingOnMissing,
			func(c *ClaimHeaderBindingConfig, v string) { c.OnMissing = v },
			func(c ClaimHeaderBindingConfig) string { return c.OnMissing },
		},
		{
			"roles", mercure.WithBindingRoles,
			func(c *ClaimHeaderBindingConfig, v string) { c.Roles = v },
			func(c ClaimHeaderBindingConfig) string { return c.Roles },
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
func (c ClaimHeaderBindingConfig) options() []mercure.BindingOption {
	var opts []mercure.BindingOption

	for _, o := range claimHeaderOptions() {
		if v := o.read(c); v != "" {
			opts = append(opts, o.validate(v))
		}
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
		b, err := mercure.NewClaimHeaderBinding(c.Claim, c.Header, c.options()...)
		if err != nil {
			return nil, fmt.Errorf("require_claim_header %q %q: %w", c.Claim, c.Header, err)
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
//	}]
//
// Parsing is deliberately strict — a surplus argument, an unknown or repeated block key,
// and an unknown enum value are all errors. This directive is an authorization check, so
// a silently ignored token would fail open.
func parseRequireClaimHeader(d *caddyfile.Dispenser) (ClaimHeaderBindingConfig, error) {
	var cfg ClaimHeaderBindingConfig

	if !d.NextArg() {
		return cfg, directiveErr(d.ArgErr())
	}

	cfg.Claim = d.Val()

	if !d.NextArg() {
		return cfg, directiveErr(d.ArgErr())
	}

	cfg.Header = d.Val()

	// NextArg rolls back on "{", so an opening brace is not mistaken for an argument.
	if d.NextArg() {
		return cfg, directiveErr(d.Err("expects exactly two arguments: <claim> <header>"))
	}

	// Build the real binding now, so an invalid claim or header name is reported against
	// this directive's file:line. It also gives the option validators below a properly
	// constructed target instead of a fabricated zero value. The binding itself is
	// discarded: Provision rebuilds it from cfg, which is the path Caddy JSON also takes.
	binding, err := mercure.NewClaimHeaderBinding(cfg.Claim, cfg.Header)
	if err != nil {
		return cfg, directiveErr(d.WrapErr(err))
	}

	seen := make(map[string]struct{}, 3)

	for nesting := d.Nesting(); d.NextBlock(nesting); {
		key := d.Val()
		if _, dup := seen[key]; dup {
			return cfg, directiveErr(d.Errf("duplicate %q option", key))
		}

		seen[key] = struct{}{}

		option, known := lookupClaimHeaderOption(key)
		if !known {
			return cfg, directiveErr(d.Errf("unrecognized option %q", key))
		}

		if !d.NextArg() {
			return cfg, directiveErr(d.ArgErr())
		}

		value := d.Val()

		if d.NextArg() {
			return cfg, directiveErr(d.Errf("option %q takes exactly one argument", key))
		}

		// Validated against the core option so the accepted values live in exactly one
		// place, and a typo fails at config load rather than at request time.
		if err := option.validate(value)(&binding); err != nil {
			return cfg, directiveErr(d.WrapErr(err))
		}

		option.assign(&cfg, value)
	}

	return cfg, nil
}

// directiveErr names the offending directive. The wrapped Dispenser error already carries
// the file:line, so the two compose into "require_claim_header: <what>, at <file>:<line>".
func directiveErr(err error) error {
	return fmt.Errorf("require_claim_header: %w", err)
}
