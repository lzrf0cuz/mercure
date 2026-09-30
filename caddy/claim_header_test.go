package caddy

import (
	"context"
	"encoding/json"
	"maps"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/caddytest"
	"github.com/dunglas/mercure"
	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUnmarshalCaddyfileRequireClaimHeader(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		input string
		// wantErr is matched as a substring, so each case pins its own rejection.
		wantErr string
		want    []ClaimHeaderBindingConfig
	}{
		{
			name:  "bare form uses defaults",
			input: "mercure {\n\trequire_claim_header groups Group-ID\n}",
			want:  []ClaimHeaderBindingConfig{{Claim: "groups", Header: "Group-ID"}},
		},
		{
			name:  "explicit block",
			input: "mercure {\n\trequire_claim_header groups Group-ID {\n\t\tmatch member\n\t\ton_missing allow\n\t\troles subscriber\n\t}\n}",
			want:  []ClaimHeaderBindingConfig{{Claim: "groups", Header: "Group-ID", Match: "member", OnMissing: "allow", Roles: "subscriber"}},
		},
		{
			name:  "count_subscribers flag",
			input: "mercure {\n\trequire_claim_header groups Group-ID {\n\t\troles subscriber\n\t\tcount_subscribers\n\t}\n}",
			want:  []ClaimHeaderBindingConfig{{Claim: "groups", Header: "Group-ID", Roles: "subscriber", CountSubscribers: true}},
		},
		{
			name:  "repeatable",
			input: "mercure {\n\trequire_claim_header groups Group-ID\n\trequire_claim_header regions X-Region\n}",
			want: []ClaimHeaderBindingConfig{
				{Claim: "groups", Header: "Group-ID"},
				{Claim: "regions", Header: "X-Region"},
			},
		},

		// Rejections.
		{name: "missing both args", input: "mercure {\n\trequire_claim_header\n}", wantErr: "require_claim_header: wrong argument count"},
		{name: "missing header arg", input: "mercure {\n\trequire_claim_header groups\n}", wantErr: "require_claim_header: wrong argument count"},
		{name: "extra arg", input: "mercure {\n\trequire_claim_header groups Group-ID oops\n}", wantErr: "require_claim_header: expects exactly two arguments"},
		{name: "bad match enum", input: "mercure {\n\trequire_claim_header groups Group-ID {\n\t\tmatch exat\n\t}\n}", wantErr: `unknown match "exat"`},
		{name: "bad on_missing enum", input: "mercure {\n\trequire_claim_header groups Group-ID {\n\t\ton_missing mabye\n\t}\n}", wantErr: `unknown on_missing "mabye"`},
		{name: "bad roles enum", input: "mercure {\n\trequire_claim_header groups Group-ID {\n\t\troles subscribr\n\t}\n}", wantErr: `unknown roles "subscribr"`},
		{name: "unknown block key", input: "mercure {\n\trequire_claim_header groups Group-ID {\n\t\tnope x\n\t}\n}", wantErr: `require_claim_header: unrecognized option "nope"`},
		{name: "duplicate block key", input: "mercure {\n\trequire_claim_header groups Group-ID {\n\t\tmatch member\n\t\tmatch exact\n\t}\n}", wantErr: `require_claim_header: duplicate "match" option`},
		{name: "block key without value", input: "mercure {\n\trequire_claim_header groups Group-ID {\n\t\tmatch\n\t}\n}", wantErr: "require_claim_header: wrong argument count"},
		{name: "block key with extra value", input: "mercure {\n\trequire_claim_header groups Group-ID {\n\t\tmatch member exact\n\t}\n}", wantErr: `require_claim_header: option "match" takes exactly one argument`},
		{name: "duplicate count_subscribers", input: "mercure {\n\trequire_claim_header groups Group-ID {\n\t\tcount_subscribers\n\t\troles all\n\t\tcount_subscribers\n\t}\n}", wantErr: `require_claim_header: duplicate "count_subscribers" option`},
		{name: "count_subscribers with an argument", input: "mercure {\n\trequire_claim_header groups Group-ID {\n\t\tcount_subscribers foo\n\t}\n}", wantErr: `require_claim_header: option "count_subscribers" takes no arguments`},
		{name: "count_subscribers with roles publisher", input: "mercure {\n\trequire_claim_header groups Group-ID {\n\t\troles publisher\n\t\tcount_subscribers\n\t}\n}", wantErr: `require_claim_header: option "count_subscribers" requires roles all or subscriber, at Testfile:4`},
		{name: "second count_subscribers binding", input: "mercure {\n\trequire_claim_header groups Group-ID {\n\t\tcount_subscribers\n\t}\n\trequire_claim_header regions X-Region {\n\t\tcount_subscribers\n\t}\n}", wantErr: `require_claim_header: option "count_subscribers" may be set on only one binding, at Testfile:6`},
		{name: "roles publisher after count_subscribers", input: "mercure {\n\trequire_claim_header groups Group-ID {\n\t\tcount_subscribers\n\t\troles publisher\n\t}\n}", wantErr: `require_claim_header: option "count_subscribers" requires roles all or subscriber, at Testfile:3`},

		// Invalid claim and header names are also caught at parse time, so the operator gets
		// a file:line rather than a bare Provision error.
		{name: "header name is not an RFC 7230 token", input: "mercure {\n\trequire_claim_header groups \"Group ID\"\n}", wantErr: "invalid claim-header binding"},
		{name: "empty claim name", input: "mercure {\n\trequire_claim_header \"\" Group-ID\n}", wantErr: "claim name must not be empty"},
		{name: "empty header name", input: "mercure {\n\trequire_claim_header groups \"\"\n}", wantErr: "invalid claim-header binding"},
		{name: "wildcard header name", input: "mercure {\n\trequire_claim_header groups *\n}", wantErr: `require_claim_header: invalid claim-header binding: header name "*" cannot be bound`},

		// A claim name is never replaced at runtime: a placeholder would bind a claim no token
		// carries. {$VAR} is substituted before parsing, so it never reaches the directive.
		{name: "runtime placeholder in the claim", input: "mercure {\n\trequire_claim_header {env.GROUP_CLAIM} Group-ID\n}", wantErr: "require_claim_header: {env.GROUP_CLAIM}: " + errCaddyfilePlaceholder.Error() + ", at Testfile:2"},
		{name: "runtime placeholder in the header", input: "mercure {\n\trequire_claim_header groups {env.GROUP_HEADER}\n}", wantErr: "require_claim_header: {env.GROUP_HEADER}: " + errCaddyfilePlaceholder.Error() + ", at Testfile:2"},
		{name: "Host header", input: "mercure {\n\trequire_claim_header groups Host\n}", wantErr: `require_claim_header: invalid claim-header binding: header name "Host" cannot be bound`},
		{name: "Host header in lower case", input: "mercure {\n\trequire_claim_header groups host\n}", wantErr: `header name "Host" cannot be bound`},
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

// Every option's assign and read must address the same field, and options() must emit one
// core option per field the operator set. A mismatched or forgotten arm would let a parsed
// directive be silently dropped on its way to the hub.
func TestClaimHeaderOptionsRoundTrip(t *testing.T) {
	t.Parallel()

	for _, o := range claimHeaderOptions() {
		t.Run(o.key, func(t *testing.T) {
			t.Parallel()

			var cfg ClaimBindingOptions
			assert.Empty(t, cfg.options(), "a zero config must select every core default")

			o.assign(&cfg, "sentinel")
			assert.Equal(t, "sentinel", o.read(cfg), "assign and read must address the same field")
			assert.Len(t, cfg.options(), 1, "a set field must produce exactly one core option")
		})
	}
}

// The core ClaimHeaderBinding has unexported fields, so the adapter carries its own
// exported, JSON-tagged config struct: the feature must be configurable through Caddy
// JSON too, not only the Caddyfile.
func TestClaimHeaderBindingsJSONConfig(t *testing.T) {
	t.Parallel()

	var m Mercure
	require.NoError(t, json.Unmarshal([]byte(`{
		"require_claim_headers": [
			{"claim": "groups", "header": "Group-ID"},
			{"claim": "regions", "header": "X-Region", "match": "member", "on_missing": "allow", "roles": "publisher"}
		]
	}`), &m))

	assert.Equal(t, []ClaimHeaderBindingConfig{
		{Claim: "groups", Header: "Group-ID"},
		{Claim: "regions", Header: "X-Region", Match: "member", OnMissing: "allow", Roles: "publisher"},
	}, m.ClaimHeaderBindings)
}

// count_subscribers is a JSON bool, and it must reach the core binding: NewHub rejects two
// counting bindings, so it can only fail if the flag was carried through.
func TestClaimHeaderBindingsJSONCountSubscribers(t *testing.T) {
	t.Parallel()

	var m Mercure
	require.NoError(t, json.Unmarshal([]byte(`{
		"require_claim_headers": [
			{"claim": "groups", "header": "Group-ID", "count_subscribers": true},
			{"claim": "regions", "header": "X-Region", "count_subscribers": true}
		]
	}`), &m))

	assert.True(t, m.ClaimHeaderBindings[0].CountSubscribers)
	assert.True(t, m.ClaimHeaderBindings[1].CountSubscribers)

	newHub := func(cfgs ...ClaimHeaderBindingConfig) error {
		bindings, err := (&Mercure{ClaimHeaderBindings: cfgs}).claimHeaderBindings()
		require.NoError(t, err)

		_, err = mercure.NewHub(
			t.Context(),
			mercure.WithClaimHeaderBindings(bindings...),
			mercure.WithIssuers([]mercure.Issuer{{
				Identifier: caddyTrustedIssuer,
				Publisher:  mercure.Static{Key: []byte("publisher"), Algorithm: "HS256"},
				Subscriber: mercure.Static{Key: []byte("subscriber"), Algorithm: "HS256"},
			}}),
		)

		return err
	}

	require.ErrorIs(t, newHub(m.ClaimHeaderBindings...), mercure.ErrInvalidClaimHeaderBinding)
	require.NoError(t, newHub(m.ClaimHeaderBindings[0]))

	pubOnly := m.ClaimHeaderBindings[0]
	pubOnly.Roles = "publisher"
	require.ErrorIs(t, newHub(pubOnly), mercure.ErrInvalidClaimHeaderBinding)
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
		"valid":                 {ClaimHeaderBindingConfig{Claim: "groups", Header: "Group-ID", Match: "member"}, ""},
		"empty claim":           {ClaimHeaderBindingConfig{Claim: "", Header: "Group-ID"}, "claim name must not be empty"},
		"bad header":            {ClaimHeaderBindingConfig{Claim: "groups", Header: "Group ID"}, "invalid claim-header binding"},
		"bad match":             {ClaimHeaderBindingConfig{Claim: "groups", Header: "Group-ID", Match: "exat"}, `unknown match "exat"`},
		"bad onMissing":         {ClaimHeaderBindingConfig{Claim: "groups", Header: "Group-ID", OnMissing: "mabye"}, `unknown on_missing "mabye"`},
		"bad roles":             {ClaimHeaderBindingConfig{Claim: "groups", Header: "Group-ID", Roles: "subscribr"}, `unknown roles "subscribr"`},
		"wildcard header":       {ClaimHeaderBindingConfig{Claim: "groups", Header: "*"}, `header name "*" cannot be bound`},
		"placeholder in claim":  {ClaimHeaderBindingConfig{Claim: "{env.GROUP_CLAIM}", Header: "Group-ID"}, "require_claim_header: {env.GROUP_CLAIM}: " + errJSONPlaceholder.Error()},
		"placeholder in header": {ClaimHeaderBindingConfig{Claim: "groups", Header: "{env.GROUP_HEADER}"}, "require_claim_header: {env.GROUP_HEADER}: " + errJSONPlaceholder.Error()},
		"Host header":           {ClaimHeaderBindingConfig{Claim: "groups", Header: "Host"}, `header name "Host" cannot be bound`},
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

func TestUnmarshalCaddyfileRequireClaimValue(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		input   string
		wantErr string
		want    []ClaimValueBindingConfig
	}{
		{
			name:  "one value",
			input: "mercure {\n\trequire_claim_value groups red\n}",
			want:  []ClaimValueBindingConfig{{Claim: "groups", Values: []string{"red"}}},
		},
		{
			name:  "several values",
			input: "mercure {\n\trequire_claim_value groups red blue green\n}",
			want:  []ClaimValueBindingConfig{{Claim: "groups", Values: []string{"red", "blue", "green"}}},
		},
		{
			name:  "a lone brace is not a placeholder",
			input: "mercure {\n\trequire_claim_value plan pro{ }enterprise\n}",
			want:  []ClaimValueBindingConfig{{Claim: "plan", Values: []string{"pro{", "}enterprise"}}},
		},
		{
			name:  "explicit block",
			input: "mercure {\n\trequire_claim_value groups red blue {\n\t\tmatch member\n\t\ton_missing allow\n\t\troles subscriber\n\t}\n}",
			want:  []ClaimValueBindingConfig{{Claim: "groups", Values: []string{"red", "blue"}, Match: "member", OnMissing: "allow", Roles: "subscriber"}},
		},
		{
			name:  "repeatable",
			input: "mercure {\n\trequire_claim_value groups red\n\trequire_claim_value regions region1\n}",
			want: []ClaimValueBindingConfig{
				{Claim: "groups", Values: []string{"red"}},
				{Claim: "regions", Values: []string{"region1"}},
			},
		},

		{name: "missing both args", input: "mercure {\n\trequire_claim_value\n}", wantErr: "require_claim_value: wrong argument count"},
		{name: "missing values", input: "mercure {\n\trequire_claim_value groups\n}", wantErr: "require_claim_value: wrong argument count"},
		{name: "missing values before block", input: "mercure {\n\trequire_claim_value groups {\n\t\tmatch member\n\t}\n}", wantErr: "require_claim_value: wrong argument count"},
		{name: "empty claim", input: "mercure {\n\trequire_claim_value \"\" red\n}", wantErr: "require_claim_value: invalid claim-header binding: claim name must not be empty"},
		{name: "unbindable claim", input: "mercure {\n\trequire_claim_value exp 1\n}", wantErr: `require_claim_value: invalid claim-header binding: claim "exp" cannot be bound`},
		{name: "empty value", input: "mercure {\n\trequire_claim_value groups red \"\"\n}", wantErr: "require_claim_value: invalid claim-header binding: values must not be empty"},
		{name: "duplicate value", input: "mercure {\n\trequire_claim_value groups red blue red\n}", wantErr: `require_claim_value: invalid claim-header binding: duplicate value "red"`},
		{name: "bad match enum", input: "mercure {\n\trequire_claim_value groups red {\n\t\tmatch exat\n\t}\n}", wantErr: `require_claim_value: invalid claim-header binding: unknown match "exat"`},
		{name: "bad on_missing enum", input: "mercure {\n\trequire_claim_value groups red {\n\t\ton_missing mabye\n\t}\n}", wantErr: `require_claim_value: invalid claim-header binding: unknown on_missing "mabye"`},
		{name: "bad roles enum", input: "mercure {\n\trequire_claim_value groups red {\n\t\troles subscribr\n\t}\n}", wantErr: `require_claim_value: invalid claim-header binding: unknown roles "subscribr"`},
		{name: "unknown block key", input: "mercure {\n\trequire_claim_value groups red {\n\t\tnope x\n\t}\n}", wantErr: `require_claim_value: unrecognized option "nope"`},
		{name: "duplicate block key", input: "mercure {\n\trequire_claim_value groups red {\n\t\tmatch member\n\t\tmatch exact\n\t}\n}", wantErr: `require_claim_value: duplicate "match" option`},
		{name: "block key without value", input: "mercure {\n\trequire_claim_value groups red {\n\t\troles\n\t}\n}", wantErr: "require_claim_value: wrong argument count"},
		{name: "block key with extra value", input: "mercure {\n\trequire_claim_value groups red {\n\t\tmatch member exact\n\t}\n}", wantErr: `require_claim_value: option "match" takes exactly one argument`},
		{name: "count_subscribers is a header-binding option", input: "mercure {\n\trequire_claim_value groups red {\n\t\tcount_subscribers\n\t}\n}", wantErr: `require_claim_value: unrecognized option "count_subscribers"`},
		{name: "runtime placeholder in the claim", input: "mercure {\n\trequire_claim_value {env.PLANCLAIM} pro {\n\t\ton_missing allow\n\t}\n}", wantErr: "require_claim_value: {env.PLANCLAIM}: " + errCaddyfilePlaceholder.Error() + ", at Testfile:2"},
		{name: "runtime placeholder in a value", input: "mercure {\n\trequire_claim_value plan pro {env.PLAN}\n}", wantErr: "require_claim_value: {env.PLAN}: " + errCaddyfilePlaceholder.Error() + ", at Testfile:2"},
		// Any {…} sequence is refused, not only a runtime placeholder.
		{name: "braced GUID value", input: "mercure {\n\trequire_claim_value group {0f8fad5b-d9cb-469f-a165-70867728950e}\n}", wantErr: "require_claim_value: {0f8fad5b-d9cb-469f-a165-70867728950e}: " + errCaddyfilePlaceholder.Error() + ", at Testfile:2"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			m := &Mercure{}
			err := m.UnmarshalCaddyfile(caddyfile.NewTestDispenser(tc.input))

			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
				assert.Regexp(t, `Testfile:\d+`, err.Error(), "the error must carry a file:line")

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.want, m.ClaimValueBindings)
			assert.Empty(t, m.ClaimHeaderBindings)
		})
	}
}

// Both configs embed ClaimBindingOptions, and their JSON keeps the flat shape.
func TestClaimBindingConfigsJSONShape(t *testing.T) {
	t.Parallel()

	opts := ClaimBindingOptions{Match: "member", OnMissing: "allow", Roles: "subscriber"}

	assertJSONRoundTrip(t,
		ClaimHeaderBindingConfig{Claim: "groups", Header: "Group-ID", ClaimBindingOptions: opts, CountSubscribers: true},
		`{"claim":"groups","header":"Group-ID","match":"member","on_missing":"allow","roles":"subscriber","count_subscribers":true}`)
	assertJSONRoundTrip(t,
		ClaimValueBindingConfig{Claim: "groups", Values: []string{"red", "blue"}, ClaimBindingOptions: opts},
		`{"claim":"groups","values":["red","blue"],"match":"member","on_missing":"allow","roles":"subscriber"}`)
}

func assertJSONRoundTrip[T any](t *testing.T, cfg T, want string) {
	t.Helper()

	got, err := json.Marshal(cfg)
	require.NoError(t, err)
	assert.JSONEq(t, want, string(got))

	var decoded T
	require.NoError(t, json.Unmarshal([]byte(want), &decoded))
	assert.Equal(t, cfg, decoded)
}

func TestClaimValueBindingsJSONConfig(t *testing.T) {
	t.Parallel()

	var m Mercure
	require.NoError(t, json.Unmarshal([]byte(`{
		"require_claim_values": [
			{"claim": "groups", "values": ["red"]},
			{"claim": "regions", "values": ["region1", "region2"], "match": "member", "on_missing": "allow", "roles": "publisher"}
		]
	}`), &m))

	assert.Equal(t, []ClaimValueBindingConfig{
		{Claim: "groups", Values: []string{"red"}},
		{Claim: "regions", Values: []string{"region1", "region2"}, Match: "member", OnMissing: "allow", Roles: "publisher"},
	}, m.ClaimValueBindings)
	assert.Empty(t, m.ClaimHeaderBindings)

	bindings, err := m.claimValueBindings()
	require.NoError(t, err)
	assert.Len(t, bindings, 2)
}

// The Caddy JSON API never runs the Caddyfile parser, so claimValueBindings() is that path's
// only validation point.
func TestClaimValueBindingsConversionValidates(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		cfg     ClaimValueBindingConfig
		wantErr string
	}{
		"one value":            {ClaimValueBindingConfig{Claim: "groups", Values: []string{"red"}}, ""},
		"several values":       {ClaimValueBindingConfig{Claim: "groups", Values: []string{"red", "blue"}, Match: "member"}, ""},
		"empty claim":          {ClaimValueBindingConfig{Claim: "", Values: []string{"red"}}, "claim name must not be empty"},
		"no values":            {ClaimValueBindingConfig{Claim: "groups"}, "at least one value is required"},
		"empty value":          {ClaimValueBindingConfig{Claim: "groups", Values: []string{""}}, "values must not be empty"},
		"duplicate value":      {ClaimValueBindingConfig{Claim: "groups", Values: []string{"red", "red"}}, `duplicate value "red"`},
		"bad match":            {ClaimValueBindingConfig{Claim: "groups", Values: []string{"red"}, Match: "exat"}, `unknown match "exat"`},
		"bad onMissing":        {ClaimValueBindingConfig{Claim: "groups", Values: []string{"red"}, OnMissing: "mabye"}, `unknown on_missing "mabye"`},
		"bad roles":            {ClaimValueBindingConfig{Claim: "groups", Values: []string{"red"}, Roles: "subscribr"}, `unknown roles "subscribr"`},
		"placeholder in claim": {ClaimValueBindingConfig{Claim: "{env.PLANCLAIM}", Values: []string{"pro"}, OnMissing: "allow"}, "require_claim_value: {env.PLANCLAIM}: " + errJSONPlaceholder.Error()},
		"placeholder in value": {ClaimValueBindingConfig{Claim: "plan", Values: []string{"pro", "{env.PLAN}"}}, "require_claim_value: {env.PLAN}: " + errJSONPlaceholder.Error()},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			m := &Mercure{ClaimValueBindings: []ClaimValueBindingConfig{tc.cfg}}

			bindings, err := m.claimValueBindings()

			if tc.wantErr == "" {
				require.NoError(t, err)
				assert.Len(t, bindings, 1)

				return
			}

			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
			assert.Contains(t, err.Error(), "require_claim_value", "the error must name the offending directive")
		})
	}
}

func TestClaimValueBindingsConversionEmpty(t *testing.T) {
	t.Parallel()

	bindings, err := (&Mercure{}).claimValueBindings()
	require.NoError(t, err)
	assert.Empty(t, bindings)
}

func TestClaimValueBindingsJSONNullElementRejected(t *testing.T) {
	t.Parallel()

	var m Mercure
	require.NoError(t, json.Unmarshal([]byte(`{"require_claim_values":[null]}`), &m))
	require.Len(t, m.ClaimValueBindings, 1)

	_, err := m.claimValueBindings()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "claim name must not be empty")
}

// mintHMACTokenWithClaims mints an access token like mustMintHMACToken, plus extra top-level
// claims for a binding to read.
func mintHMACTokenWithClaims(tb testing.TB, extra jwt.MapClaims, details ...map[string]any) string {
	tb.Helper()

	claims := newAccessTokenClaims(details...)
	maps.Copy(claims, extra)

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	token.Header["typ"] = "at+jwt"

	s, err := token.SignedString([]byte("!ChangeMe!"))
	require.NoError(tb, err)

	return s
}

// A Caddyfile configuration must reach the hub: Provision hands both kinds of binding to
// it, and cors_origins lets a browser send the bound header. Every assertion here fails if
// a binding is parsed but never wired.
func TestClaimBindingsAreEnforced(t *testing.T) {
	tester := caddytest.NewTester(t)
	tester.InitServer(`{
	skip_install_trust
	admin localhost:2999
	http_port     9080
	https_port    9443
}

localhost:9080 {
	route {
		mercure {
			transport local
			issuer https://example.com {
				publisher {
					jwt !ChangeMe!
				}
				subscriber {
					jwt !ChangeMe!
				}
			}
			resource_identifier https://example.com/.well-known/mercure
			cors_origins https://app.example.com
			require_claim_header groups Group-ID {
				roles subscriber
				count_subscribers
			}
			require_claim_value plan pro {
				roles publisher
			}
		}

		respond 404
	}
}`, "caddyfile")

	const hubURL = "http://localhost:9080/.well-known/mercure"

	t.Run("preflight allows the bound header", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodOptions, hubURL, nil)
		require.NoError(t, err)
		req.Header.Set("Origin", "https://app.example.com")
		req.Header.Set("Access-Control-Request-Method", http.MethodGet)
		req.Header.Set("Access-Control-Request-Headers", "authorization,group-id")

		resp := tester.AssertResponseCode(req, http.StatusNoContent)
		require.NoError(t, resp.Body.Close())

		assert.Equal(t, "authorization,group-id", resp.Header.Get("Access-Control-Allow-Headers"))
		assert.Equal(t, "https://app.example.com", resp.Header.Get("Access-Control-Allow-Origin"))
	})

	subscriberToken := mintHMACTokenWithClaims(t, jwt.MapClaims{"groups": []string{"red"}}, actionDetail("subscribe", topicMatch()))

	subscribe := func(ctx context.Context, groupIDs ...string) *http.Request {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, hubURL+"?match=https%3A%2F%2Fexample.com%2Ffoo", nil)
		require.NoError(t, err)
		req.Header.Set("Authorization", bearerPrefix+subscriberToken)

		for _, v := range groupIDs {
			req.Header.Add("Group-ID", v)
		}

		return req
	}

	t.Run("a subscriber whose header matches its claim connects", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()

		// The response headers arrive once the subscription is established.
		resp := tester.AssertResponseCode(subscribe(ctx, "red"), http.StatusOK)

		cancel()
		require.NoError(t, resp.Body.Close())
	})

	t.Run("a mismatched header is forbidden", func(t *testing.T) {
		resp := tester.AssertResponseCode(subscribe(t.Context(), "blue"), http.StatusForbidden)
		require.NoError(t, resp.Body.Close())
	})

	t.Run("a missing header is a bad request", func(t *testing.T) {
		resp := tester.AssertResponseCode(subscribe(t.Context()), http.StatusBadRequest)
		require.NoError(t, resp.Body.Close())
	})

	publish := func(token string) *http.Request {
		body := url.Values{"topic": {"https://example.com/foo"}, "data": {"bar"}}
		req, err := http.NewRequest(http.MethodPost, hubURL, strings.NewReader(body.Encode()))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set("Authorization", bearerPrefix+token)

		return req
	}

	t.Run("a publisher without the required claim value is forbidden", func(t *testing.T) {
		resp := tester.AssertResponseCode(publish(publisherJWT), http.StatusForbidden)
		require.NoError(t, resp.Body.Close())
	})

	t.Run("a publisher holding the required claim value publishes", func(t *testing.T) {
		token := mintHMACTokenWithClaims(t, jwt.MapClaims{"plan": "pro"}, actionDetail("publish", topicMatch()))

		resp := tester.AssertResponseCode(publish(token), http.StatusOK)
		require.NoError(t, resp.Body.Close())
	})
}

// The advice for an environment variable depends on the configuration format. A Caddyfile
// substitutes {$VAR} as text before tokenizing, so an unset, unquoted variable vanishes and
// shifts the arguments ("require_claim_value {$P} pro enterprise" binds claim "pro"): the
// advice is the quoted form, or a default. Caddy JSON has no {$VAR}, so it must not be
// suggested there.
func TestClaimBindingPlaceholderAdvice(t *testing.T) {
	t.Parallel()

	caddyfileErr := new(Mercure).UnmarshalCaddyfile(caddyfile.NewTestDispenser("mercure {\n\trequire_claim_value {env.P} pro\n}"))
	require.ErrorIs(t, caddyfileErr, errCaddyfilePlaceholder)
	assert.Contains(t, caddyfileErr.Error(), `write "{$VAR}" (quoted, so that an unset variable leaves an empty argument and startup fails) or {$VAR:default}`)

	_, jsonErr := (&Mercure{ClaimValueBindings: []ClaimValueBindingConfig{{Claim: "{env.P}", Values: []string{"pro"}}}}).claimValueBindings()
	require.ErrorIs(t, jsonErr, errJSONPlaceholder)
	assert.Contains(t, jsonErr.Error(), "JSON takes the literal value")
	assert.NotContains(t, jsonErr.Error(), "$VAR", "JSON has no {$VAR} substitution")
}
