package caddy

import (
	"encoding/json"
	"testing"

	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUnmarshalCaddyfileRequireClaimHeader(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		input string
		// wantErr asserts the specific rejection: a bare Error() check would pass even
		// if the parse failed for an unrelated reason (a stray token reaching the
		// switch's default case, say).
		wantErr string
		want    []ClaimHeaderBindingConfig
	}{
		{
			name:  "bare form uses defaults",
			input: "mercure {\n\trequire_claim_header tenants Tenant-ID\n}",
			want:  []ClaimHeaderBindingConfig{{Claim: "tenants", Header: "Tenant-ID"}},
		},
		{
			name:  "explicit block",
			input: "mercure {\n\trequire_claim_header tenants Tenant-ID {\n\t\tmatch member\n\t\ton_missing allow\n\t\troles subscriber\n\t}\n}",
			want:  []ClaimHeaderBindingConfig{{Claim: "tenants", Header: "Tenant-ID", Match: "member", OnMissing: "allow", Roles: "subscriber"}},
		},
		{
			name:  "repeatable",
			input: "mercure {\n\trequire_claim_header tenants Tenant-ID\n\trequire_claim_header regions X-Region\n}",
			want: []ClaimHeaderBindingConfig{
				{Claim: "tenants", Header: "Tenant-ID"},
				{Claim: "regions", Header: "X-Region"},
			},
		},

		// Strict parsing: this is a security directive, so nothing may be silently ignored.
		{name: "missing both args", input: "mercure {\n\trequire_claim_header\n}", wantErr: "require_claim_header: wrong argument count"},
		{name: "missing header arg", input: "mercure {\n\trequire_claim_header tenants\n}", wantErr: "require_claim_header: wrong argument count"},
		{name: "extra arg", input: "mercure {\n\trequire_claim_header tenants Tenant-ID oops\n}", wantErr: "require_claim_header: expects exactly two arguments"},
		{name: "bad match enum", input: "mercure {\n\trequire_claim_header tenants Tenant-ID {\n\t\tmatch exat\n\t}\n}", wantErr: `unknown match "exat"`},
		{name: "bad on_missing enum", input: "mercure {\n\trequire_claim_header tenants Tenant-ID {\n\t\ton_missing mabye\n\t}\n}", wantErr: `unknown on_missing "mabye"`},
		{name: "bad roles enum", input: "mercure {\n\trequire_claim_header tenants Tenant-ID {\n\t\troles subscribr\n\t}\n}", wantErr: `unknown roles "subscribr"`},
		{name: "unknown block key", input: "mercure {\n\trequire_claim_header tenants Tenant-ID {\n\t\tnope x\n\t}\n}", wantErr: `require_claim_header: unrecognized option "nope"`},
		{name: "duplicate block key", input: "mercure {\n\trequire_claim_header tenants Tenant-ID {\n\t\tmatch member\n\t\tmatch exact\n\t}\n}", wantErr: `require_claim_header: duplicate "match" option`},
		{name: "block key without value", input: "mercure {\n\trequire_claim_header tenants Tenant-ID {\n\t\tmatch\n\t}\n}", wantErr: "require_claim_header: wrong argument count"},
		{name: "block key with extra value", input: "mercure {\n\trequire_claim_header tenants Tenant-ID {\n\t\tmatch member exact\n\t}\n}", wantErr: `require_claim_header: option "match" takes exactly one argument`},

		// Invalid claim/header NAMES must also be caught at parse time, so the operator gets
		// a file:line rather than a bare Provision error.
		{name: "header name is not an RFC 7230 token", input: "mercure {\n\trequire_claim_header tenants \"Tenant ID\"\n}", wantErr: "invalid claim-header binding"},
		{name: "empty claim name", input: "mercure {\n\trequire_claim_header \"\" Tenant-ID\n}", wantErr: "claim name must not be empty"},
		{name: "empty header name", input: "mercure {\n\trequire_claim_header tenants \"\"\n}", wantErr: "invalid claim-header binding"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			m := &Mercure{}
			err := m.UnmarshalCaddyfile(caddyfile.NewTestDispenser(tc.input))

			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.want, m.ClaimHeaderBindings)
		})
	}
}

// A misspelled directive must NOT be silently ignored: `require_claim_header` is a
// security control, and a typo that parses clean would fail open (binding absent,
// requests allowed).
func TestUnmarshalCaddyfileRejectsUnknownDirective(t *testing.T) {
	t.Parallel()

	for input, want := range map[string]string{
		"mercure {\n\trequre_claim_header tenants Tenant-ID\n}": `unrecognized mercure subdirective "requre_claim_header"`,
		"mercure {\n\ttotally_unknown_directive 1\n}":           `unrecognized mercure subdirective "totally_unknown_directive"`,
	} {
		m := &Mercure{}
		err := m.UnmarshalCaddyfile(caddyfile.NewTestDispenser(input))
		require.Error(t, err, "unknown mercure subdirective must be a parse error, not silently ignored")
		assert.Contains(t, err.Error(), want)
	}
}

// Every option's assign and read must address the same field, and options() must emit one
// core option per field the operator set. A mismatched or forgotten arm would let a parsed
// directive be silently dropped on its way to the hub.
func TestClaimHeaderOptionsRoundTrip(t *testing.T) {
	t.Parallel()

	for _, o := range claimHeaderOptions() {
		t.Run(o.key, func(t *testing.T) {
			t.Parallel()

			var cfg ClaimHeaderBindingConfig
			assert.Empty(t, cfg.options(), "a zero config must select every core default")

			o.assign(&cfg, "sentinel")
			assert.Equal(t, "sentinel", o.read(cfg), "assign and read must address the same field")
			assert.Len(t, cfg.options(), 1, "a set field must produce exactly one core option")
		})
	}
}

// The core ClaimHeaderBinding has unexported fields, so the adapter carries its own
// exported, JSON-tagged config struct — the feature must be configurable via Caddy JSON
// too, not only the Caddyfile.
func TestClaimHeaderBindingsJSONConfig(t *testing.T) {
	t.Parallel()

	var m Mercure
	require.NoError(t, json.Unmarshal([]byte(`{
		"require_claim_headers": [
			{"claim": "tenants", "header": "Tenant-ID"},
			{"claim": "regions", "header": "X-Region", "match": "member", "on_missing": "allow", "roles": "publisher"}
		]
	}`), &m))

	assert.Equal(t, []ClaimHeaderBindingConfig{
		{Claim: "tenants", Header: "Tenant-ID"},
		{Claim: "regions", Header: "X-Region", Match: "member", OnMissing: "allow", Roles: "publisher"},
	}, m.ClaimHeaderBindings)
}

// The Caddy JSON API never runs the Caddyfile parser, so claimHeaderBindings() is that
// path's only validation point. An invalid name or enum must surface there as a Provision
// error, naming the offending directive.
func TestClaimHeaderBindingsConversionValidates(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		cfg     ClaimHeaderBindingConfig
		wantErr string
	}{
		"valid":         {ClaimHeaderBindingConfig{Claim: "tenants", Header: "Tenant-ID", Match: "member"}, ""},
		"empty claim":   {ClaimHeaderBindingConfig{Claim: "", Header: "Tenant-ID"}, "claim name must not be empty"},
		"bad header":    {ClaimHeaderBindingConfig{Claim: "tenants", Header: "Tenant ID"}, "invalid claim-header binding"},
		"bad match":     {ClaimHeaderBindingConfig{Claim: "tenants", Header: "Tenant-ID", Match: "exat"}, `unknown match "exat"`},
		"bad onMissing": {ClaimHeaderBindingConfig{Claim: "tenants", Header: "Tenant-ID", OnMissing: "mabye"}, `unknown on_missing "mabye"`},
		"bad roles":     {ClaimHeaderBindingConfig{Claim: "tenants", Header: "Tenant-ID", Roles: "subscribr"}, `unknown roles "subscribr"`},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			m := &Mercure{ClaimHeaderBindings: []ClaimHeaderBindingConfig{tc.cfg}}

			bindings, err := m.claimHeaderBindings()

			if tc.wantErr == "" {
				require.NoError(t, err)
				assert.Len(t, bindings, 1)

				return
			}

			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
			assert.Contains(t, err.Error(), "require_claim_header", "the error must name the offending directive")
		})
	}
}

// No bindings configured must convert to no bindings, not to a one-element slice holding a
// zero-value binding (which NewHub would reject).
func TestClaimHeaderBindingsConversionEmpty(t *testing.T) {
	t.Parallel()

	bindings, err := (&Mercure{}).claimHeaderBindings()
	require.NoError(t, err)
	assert.Empty(t, bindings)
}

// A JSON null in the require_claim_headers array decodes to a zero-value config, which the
// conversion must reject rather than pass through as an empty binding.
func TestClaimHeaderBindingsJSONNullElementRejected(t *testing.T) {
	t.Parallel()

	var m Mercure
	require.NoError(t, json.Unmarshal([]byte(`{"require_claim_headers":[null]}`), &m))
	require.Len(t, m.ClaimHeaderBindings, 1)
	assert.Equal(t, ClaimHeaderBindingConfig{}, m.ClaimHeaderBindings[0])

	_, err := m.claimHeaderBindings()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "claim name must not be empty")
}
