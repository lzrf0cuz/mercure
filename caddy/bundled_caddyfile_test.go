package caddy

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig"
	"github.com/stretchr/testify/require"
)

// TestBundledCaddyfilesIssuerModes adapts and validates the repository's real
// Caddyfile and local.Caddyfile, since deployments run them unmodified and
// parameterise them only through environment variables. It covers both ways
// MERCURE_ISSUER_MODE verifies tokens, and the failure of the jwks mode when
// its URLs are missing.
//
// No t.Parallel: caddy.Validate writes certmagic.Default.Storage and the test
// sets process environment variables.
func TestBundledCaddyfilesIssuerModes(t *testing.T) {
	publisherSet, err := filepath.Abs("../public/fixtures/jwks.json")
	require.NoError(t, err)

	subscriberSet, err := filepath.Abs("testdata/RS256.jwks.json")
	require.NoError(t, err)

	publisherURI, subscriberURI := "file://"+publisherSet, "file://"+subscriberSet

	cases := []struct {
		name       string
		env        map[string]string
		issuer     string
		publisher  map[string]any
		subscriber map[string]any
		wantErr    string
	}{
		{
			name: "keys is the default mode",
			env: map[string]string{
				"MERCURE_PUBLISHER_JWT_KEY":  "publisher-key",
				"MERCURE_SUBSCRIBER_JWT_KEY": "subscriber-key",
			},
			issuer: "https://localhost",
			publisher: map[string]any{"jwt": map[string]any{
				"key": "{env.MERCURE_PUBLISHER_JWT_KEY}", "alg": "{env.MERCURE_PUBLISHER_JWT_ALG}",
			}},
			subscriber: map[string]any{"jwt": map[string]any{
				"key": "{env.MERCURE_SUBSCRIBER_JWT_KEY}", "alg": "{env.MERCURE_SUBSCRIBER_JWT_ALG}",
			}},
		},
		{
			name: "jwks",
			env: map[string]string{
				"MERCURE_ISSUER_MODE":         "jwks",
				"MERCURE_TRUSTED_ISSUERS":     "https://idp.test",
				"MERCURE_PUBLISHER_JWKS_URI":  publisherURI,
				"MERCURE_SUBSCRIBER_JWKS_URI": subscriberURI,
			},
			issuer:     "https://idp.test",
			publisher:  map[string]any{"jwks_uri": publisherURI},
			subscriber: map[string]any{"jwks_uri": subscriberURI},
		},
		{
			name:    "jwks without URLs",
			env:     map[string]string{"MERCURE_ISSUER_MODE": "jwks"},
			wantErr: "jwks_uri",
		},
	}

	adapter := caddyconfig.GetAdapter("caddyfile")
	require.NotNil(t, adapter)

	for _, file := range []string{"Caddyfile", "local.Caddyfile"} {
		path, err := filepath.Abs(filepath.Join("..", file))
		require.NoError(t, err)

		body, err := os.ReadFile(path)
		require.NoError(t, err)

		for _, tc := range cases {
			t.Run(file+"/"+tc.name, func(t *testing.T) {
				for _, name := range []string{
					"MERCURE_ISSUER_MODE", "MERCURE_TRUSTED_ISSUERS",
					"MERCURE_PUBLISHER_JWT_KEY", "MERCURE_PUBLISHER_JWT_ALG",
					"MERCURE_SUBSCRIBER_JWT_KEY", "MERCURE_SUBSCRIBER_JWT_ALG",
					"MERCURE_PUBLISHER_JWKS_URI", "MERCURE_SUBSCRIBER_JWKS_URI",
				} {
					// Restored when the subtest ends.
					t.Setenv(name, "")
					require.NoError(t, os.Unsetenv(name))
				}

				t.Setenv("SERVER_NAME", ":80")

				for name, value := range tc.env {
					t.Setenv(name, value)
				}

				adapted, _, err := adapter.Adapt(body, map[string]any{"filename": path})
				if tc.wantErr != "" {
					require.ErrorContains(t, err, tc.wantErr)

					return
				}

				require.NoError(t, err)

				var adaptedCfg any
				require.NoError(t, json.Unmarshal(adapted, &adaptedCfg))

				hub := findMercureHandler(adaptedCfg)
				require.NotNil(t, hub, "no mercure handler in the adapted config")

				issuers, _ := hub["issuers"].([]any)
				require.Len(t, issuers, 1)

				issuer, _ := issuers[0].(map[string]any)
				require.Equal(t, tc.issuer, issuer["identifier"])
				require.Equal(t, tc.publisher, issuer["publisher"])
				require.Equal(t, tc.subscriber, issuer["subscriber"])

				var cfg caddy.Config
				require.NoError(t, caddy.StrictUnmarshalJSON(adapted, &cfg))
				require.NoError(t, caddy.Validate(&cfg))
			})
		}
	}
}

// findMercureHandler returns the first `mercure` handler in an adapted Caddy
// config, however deeply the Caddyfile's handle and route blocks nest it.
func findMercureHandler(node any) map[string]any {
	switch v := node.(type) {
	case map[string]any:
		if v["handler"] == "mercure" {
			return v
		}

		for _, child := range v {
			if found := findMercureHandler(child); found != nil {
				return found
			}
		}
	case []any:
		for _, child := range v {
			if found := findMercureHandler(child); found != nil {
				return found
			}
		}
	}

	return nil
}

// TestBundledCaddyfileAccessLogFormat adapts the repository's Caddyfile with
// and without CADDY_ACCESS_LOG_FORMAT: the default keeps the query-token
// redaction, a file path replaces the format, and an empty value or a missing
// file fails instead of silently logging unredacted URLs.
//
// No t.Parallel: the test sets process environment variables.
func TestBundledCaddyfileAccessLogFormat(t *testing.T) {
	path, err := filepath.Abs("../Caddyfile")
	require.NoError(t, err)

	body, err := os.ReadFile(path)
	require.NoError(t, err)

	formatFile := filepath.Join(t.TempDir(), "access-log-format.caddy")
	require.NoError(t, os.WriteFile(formatFile, []byte("format json {\n\ttime_key timestamp\n}\n"), 0o600))

	defaultEncoder := map[string]any{
		"format": "filter",
		"fields": map[string]any{
			"request>uri": map[string]any{
				"filter": "query",
				"actions": []any{
					map[string]any{"type": "replace", "parameter": "authorization", "value": "REDACTED"},
					map[string]any{"type": "replace", "parameter": "jwt", "value": "REDACTED"},
				},
			},
		},
	}

	cases := []struct {
		name    string
		value   *string
		encoder map[string]any
		wantErr string
	}{
		{name: "unset redacts query tokens", encoder: defaultEncoder},
		{
			name:    "file replaces the format",
			value:   &formatFile,
			encoder: map[string]any{"format": "json", "time_key": "timestamp"},
		},
		{name: "empty fails", value: new(""), wantErr: "import"},
		{name: "missing file fails", value: new(formatFile + ".missing"), wantErr: "File to import not found"},
	}

	adapter := caddyconfig.GetAdapter("caddyfile")
	require.NotNil(t, adapter)

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("SERVER_NAME", ":80")
			t.Setenv("MERCURE_PUBLISHER_JWT_KEY", "publisher-key")
			t.Setenv("MERCURE_SUBSCRIBER_JWT_KEY", "subscriber-key")

			// Restored when the subtest ends.
			t.Setenv("CADDY_ACCESS_LOG_FORMAT", "")

			if tc.value == nil {
				require.NoError(t, os.Unsetenv("CADDY_ACCESS_LOG_FORMAT"))
			} else {
				t.Setenv("CADDY_ACCESS_LOG_FORMAT", *tc.value)
			}

			adapted, _, err := adapter.Adapt(body, map[string]any{"filename": path})
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)

				return
			}

			require.NoError(t, err)

			var cfg struct {
				Logging struct {
					Logs map[string]struct {
						Encoder map[string]any `json:"encoder"`
					} `json:"logs"`
				} `json:"logging"`
			}
			require.NoError(t, json.Unmarshal(adapted, &cfg))

			var encoders []map[string]any

			for name, logger := range cfg.Logging.Logs {
				if name != "default" {
					encoders = append(encoders, logger.Encoder)
				}
			}

			require.Equal(t, []map[string]any{tc.encoder}, encoders)
		})
	}
}
