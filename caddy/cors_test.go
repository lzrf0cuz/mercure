package caddy

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/caddytest"
	"github.com/dunglas/mercure"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCORSAllowedHeadersPreflight(t *testing.T) {
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
			anonymous
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
			cors_allowed_headers X-Request-ID
		}

		respond 404
	}
}`, "caddyfile")

	req, err := http.NewRequestWithContext(t.Context(), http.MethodOptions, "http://localhost:9080/.well-known/mercure", nil)
	require.NoError(t, err)
	req.Header.Set("Origin", "https://app.example.com")
	req.Header.Set("Access-Control-Request-Method", http.MethodGet)
	req.Header.Set("Access-Control-Request-Headers", "authorization,x-request-id")

	resp := tester.AssertResponseCode(req, http.StatusNoContent)
	require.NoError(t, resp.Body.Close())

	assert.Equal(t, "authorization,x-request-id", resp.Header.Get("Access-Control-Allow-Headers"))
}

func TestUnmarshalCaddyfileCORSAllowedHeaders(t *testing.T) {
	t.Parallel()

	m := new(Mercure)
	require.NoError(t, m.UnmarshalCaddyfile(caddyfile.NewTestDispenser("mercure {\n\tcors_allowed_headers X-Request-ID traceparent\n}")))
	assert.Equal(t, []string{"X-Request-ID", "traceparent"}, m.CORSAllowedHeaders)

	require.ErrorContains(t, new(Mercure).UnmarshalCaddyfile(caddyfile.NewTestDispenser("mercure {\n\tcors_allowed_headers\n}")), "wrong argument count")
}

// An invalid header name stops the configuration from loading, through either format.
func TestCORSAllowedHeadersRejectedAtProvision(t *testing.T) {
	t.Parallel()

	validate := func(headers string) error {
		raw := `{"admin":{"disabled":true,"config":{"persist":false}},"apps":{"http":{"servers":{"srv0":{"listen":["127.0.0.1:0"],"automatic_https":{"disable":true},"routes":[{"handle":[{"handler":"mercure","name":"cors_allowed_headers","anonymous":true,"transport":{"name":"local"},"cors_origins":["https://app.example.com"],"cors_allowed_headers":` + headers + `,"issuers":[{"identifier":"https://example.com","publisher":{"jwt":{"key":"test-publisher-key","alg":"HS256"}}}]}]}]}}}}}`

		var cfg caddy.Config
		require.NoError(t, json.Unmarshal([]byte(raw), &cfg))

		return caddy.Validate(&cfg)
	}

	require.NoError(t, validate(`["X-Request-ID"]`))

	for _, headers := range []string{`[""]`, `["X Request"]`, `["*"]`} {
		err := validate(headers)
		require.ErrorContains(t, err, mercure.ErrInvalidCORSAllowedHeader.Error(), headers)
	}
}
