package caddy

import (
	"context"
	"maps"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2/caddytest"
	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// identityPlaceholders maps each response header the test Caddyfile sets to the identity
// placeholder it echoes. A placeholder the hub left unset stays literal in the header.
var identityPlaceholders = map[string]string{
	"X-Test-User":    "{http.auth.user.id}",
	"X-Test-Client":  "{http.auth.user.client_id}",
	"X-Test-Jti":     "{http.auth.user.jti}",
	"X-Test-Role":    "{http.mercure.role}",
	"X-Test-Groups":  "{http.mercure.binding.groups}",
	"X-Test-Plan":    "{http.mercure.binding.plan}",
	"X-Test-Missing": "{http.mercure.binding.region}",
}

// The hub publishes a verified request's identity as Caddy placeholders, which a deferred
// header directive reads back, and leaves them unset for an anonymous or rejected request.
func TestIdentityPlaceholders(t *testing.T) {
	tester := caddytest.NewTester(t)
	tester.InitServer(`{
	skip_install_trust
	admin localhost:2999
	http_port     9080
	https_port    9443
}

localhost:9080 {
	route {
		header >X-Test-User    "{http.auth.user.id}"
		header >X-Test-Client  "{http.auth.user.client_id}"
		header >X-Test-Jti     "{http.auth.user.jti}"
		header >X-Test-Role    "{http.mercure.role}"
		header >X-Test-Groups  "{http.mercure.binding.groups}"
		header >X-Test-Plan    "{http.mercure.binding.plan}"
		header >X-Test-Missing "{http.mercure.binding.region}"

		mercure {
			transport local
			anonymous
			subscriptions
			issuer https://example.com {
				publisher {
					jwt !ChangeMe!
				}
				subscriber {
					jwt !ChangeMe!
				}
			}
			resource_identifier https://example.com/.well-known/mercure
			require_claim_header groups Group-ID {
				roles subscriber
			}
			require_claim_header region X-Region {
				roles subscriber
				on_missing allow
			}
			require_claim_value plan pro {
				roles publisher
			}
		}

		respond 404
	}
}`, "caddyfile")

	const hubURL = "http://localhost:9080/.well-known/mercure"

	identity := jwt.MapClaims{"sub": "user-1", "client_id": "client-1", "jti": "token-1"}
	subscriberToken := mintHMACTokenWithClaims(t, withClaims(identity, jwt.MapClaims{"groups": []string{"red"}}), actionDetail("subscribe", topicMatch()))
	publisherToken := mintHMACTokenWithClaims(t, withClaims(identity, jwt.MapClaims{"plan": "pro"}), actionDetail("publish", topicMatch()))
	freePublisherToken := mintHMACTokenWithClaims(t, withClaims(identity, jwt.MapClaims{"plan": "free"}), actionDetail("publish", topicMatch()))
	// No client_id and no jti: their placeholders must stay unset rather than be set empty.
	bareToken := mintHMACTokenWithClaims(t, jwt.MapClaims{"sub": "user-2", "groups": []string{"red"}}, actionDetail("subscribe", topicMatch()))
	// A sub carrying a CR, a LF, a NUL, an ESC and a DEL, which must not reach the response.
	dirtyToken := mintHMACTokenWithClaims(t, withClaims(identity, jwt.MapClaims{"sub": "us\r\ner\x00\x1b-\x7f3", "groups": []string{"red"}}), actionDetail("subscribe", topicMatch()))
	badToken := mintHMACTokenWithClaims(t, withClaims(identity, jwt.MapClaims{"groups": []string{"red"}, "aud": "https://other.example.com"}), actionDetail("subscribe", topicMatch()))

	subscribe := func(token, groupID string, extra ...string) func(context.Context) *http.Request {
		return func(ctx context.Context) *http.Request {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, hubURL+"?match=https%3A%2F%2Fexample.com%2Ffoo", nil)
			require.NoError(t, err)

			if token != "" {
				req.Header.Set("Authorization", bearerPrefix+token)
			}

			if groupID != "" {
				req.Header.Set("Group-ID", groupID)
			}

			for i := 0; i+1 < len(extra); i += 2 {
				req.Header.Set(extra[i], extra[i+1])
			}

			return req
		}
	}

	subscriptions := func(token, groupID string) func(context.Context) *http.Request {
		return func(ctx context.Context) *http.Request {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, hubURL+"/subscriptions", nil)
			require.NoError(t, err)
			req.Header.Set("Authorization", bearerPrefix+token)
			req.Header.Set("Group-ID", groupID)

			return req
		}
	}

	publish := func(token string) func(context.Context) *http.Request {
		return func(ctx context.Context) *http.Request {
			body := url.Values{"topic": {"https://example.com/foo"}, "data": {"bar"}}
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, hubURL, strings.NewReader(body.Encode()))
			require.NoError(t, err)
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			req.Header.Set("Authorization", bearerPrefix+token)

			return req
		}
	}

	unset := func(names ...string) map[string]string {
		want := map[string]string{}
		for _, name := range names {
			want[name] = identityPlaceholders[name]
		}

		return want
	}

	allUnset := unset("X-Test-User", "X-Test-Client", "X-Test-Jti", "X-Test-Role", "X-Test-Groups", "X-Test-Plan", "X-Test-Missing")

	for _, tc := range []struct {
		name    string
		request func(context.Context) *http.Request
		status  int
		want    map[string]string
	}{
		{
			name:    "verified subscribe",
			request: subscribe(subscriberToken, "red"),
			status:  http.StatusOK,
			want: withHeaders(unset("X-Test-Plan", "X-Test-Missing"), map[string]string{
				"X-Test-User": "user-1", "X-Test-Client": "client-1", "X-Test-Jti": "token-1",
				"X-Test-Role": "subscriber", "X-Test-Groups": "red",
			}),
		},
		{
			// X-Region is sent but the token has no region claim, so the on_missing allow
			// binding matches nothing and its placeholder stays unset.
			name:    "verified subscribe, bound header sent, claim absent (on_missing allow)",
			request: subscribe(subscriberToken, "red", "X-Region", "eu"),
			status:  http.StatusOK,
			want: withHeaders(unset("X-Test-Plan", "X-Test-Missing"), map[string]string{
				"X-Test-User": "user-1", "X-Test-Client": "client-1", "X-Test-Jti": "token-1",
				"X-Test-Role": "subscriber", "X-Test-Groups": "red",
			}),
		},
		{
			name:    "verified subscribe, token without client_id or jti",
			request: subscribe(bareToken, "red"),
			status:  http.StatusOK,
			want: withHeaders(unset("X-Test-Client", "X-Test-Jti", "X-Test-Plan", "X-Test-Missing"), map[string]string{
				"X-Test-User": "user-2", "X-Test-Role": "subscriber", "X-Test-Groups": "red",
			}),
		},
		{
			name:    "verified subscribe, sub with control characters",
			request: subscribe(dirtyToken, "red"),
			status:  http.StatusOK,
			want: withHeaders(unset("X-Test-Plan", "X-Test-Missing"), map[string]string{
				"X-Test-User": "user-3", "X-Test-Client": "client-1", "X-Test-Jti": "token-1",
				"X-Test-Role": "subscriber", "X-Test-Groups": "red",
			}),
		},
		{
			name:    "verified publish",
			request: publish(publisherToken),
			status:  http.StatusOK,
			want: withHeaders(unset("X-Test-Groups", "X-Test-Missing"), map[string]string{
				"X-Test-User": "user-1", "X-Test-Client": "client-1", "X-Test-Jti": "token-1",
				"X-Test-Role": "publisher", "X-Test-Plan": "pro",
			}),
		},
		{
			name:    "verified subscription API request",
			request: subscriptions(subscriberToken, "red"),
			status:  http.StatusOK,
			want: withHeaders(unset("X-Test-Plan", "X-Test-Missing"), map[string]string{
				"X-Test-User": "user-1", "X-Test-Client": "client-1", "X-Test-Jti": "token-1",
				"X-Test-Role": "subscriber", "X-Test-Groups": "red",
			}),
		},
		{name: "subscription API request with a mismatched bound header (403)", request: subscriptions(subscriberToken, "blue"), status: http.StatusForbidden, want: allUnset},
		{name: "anonymous subscribe", request: subscribe("", ""), status: http.StatusOK, want: allUnset},
		{name: "invalid token (401)", request: subscribe(badToken, "red"), status: http.StatusUnauthorized, want: allUnset},
		{name: "bound header mismatch (403)", request: subscribe(subscriberToken, "blue"), status: http.StatusForbidden, want: allUnset},
		{name: "bound header missing (400)", request: subscribe(subscriberToken, ""), status: http.StatusBadRequest, want: allUnset},
		{name: "claim value absent (403)", request: publish(freePublisherToken), status: http.StatusForbidden, want: allUnset},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()

			// A subscription's response headers arrive once it is established.
			resp := tester.AssertResponseCode(tc.request(ctx), tc.status)

			cancel()
			require.NoError(t, resp.Body.Close())

			got := map[string]string{}
			for name := range identityPlaceholders {
				got[name] = resp.Header.Get(name)
			}

			assert.Equal(t, tc.want, got)
		})
	}
}

func withClaims(base, extra jwt.MapClaims) jwt.MapClaims {
	out := maps.Clone(base)
	maps.Copy(out, extra)

	return out
}

func withHeaders(base, extra map[string]string) map[string]string {
	maps.Copy(base, extra)

	return base
}
