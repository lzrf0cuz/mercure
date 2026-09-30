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
