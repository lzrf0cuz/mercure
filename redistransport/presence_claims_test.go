package redistransport

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/dunglas/mercure"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// signTestJWT signs an HS256 access token over claims with stallJWTKey.
func signTestJWT(claims string) string {
	enc := base64.RawURLEncoding
	input := enc.EncodeToString([]byte(`{"alg":"HS256","typ":"at+jwt"}`)) + "." + enc.EncodeToString([]byte(claims))

	mac := hmac.New(sha256.New, stallJWTKey)
	mac.Write([]byte(input))

	return input + "." + enc.EncodeToString(mac.Sum(nil))
}

// subscribeTestJWT returns a token allowed to subscribe to topic, carrying
// the registered claims a real issuer sets and a subscription payload.
func subscribeTestJWT(topic, jti string) string {
	return signTestJWT(`{"iss":"` + stallIssuer + `","aud":"` + stallResourceIdentifier + `","sub":"user-42","jti":"` + jti +
		`","exp":` + strconv.FormatInt(time.Now().Add(time.Hour).Unix(), 10) +
		`,"authorization_details":[{"type":"https://mercure.rocks/authorization-detail","actions":["subscribe"],` +
		`"topics":[{"match":"` + topic + `"}],"payload":{"role":"reader"}}]}`)
}

// newClaimsTestHub builds a hub on transport that verifies stallJWTKey tokens
// and serves the subscription API.
func newClaimsTestHub(t *testing.T, transport *RedisTransport) *mercure.Hub {
	t.Helper()

	hub, err := mercure.NewHub(
		t.Context(),
		mercure.WithTransport(transport),
		mercure.WithIssuers([]mercure.Issuer{{
			Identifier: stallIssuer,
			Subscriber: mercure.Static{Key: stallJWTKey, Algorithm: "HS256"},
		}}),
		mercure.WithResourceIdentifier(stallResourceIdentifier),
		mercure.WithSubscriptions(),
		mercure.WithLogger(testLogger()),
	)
	require.NoError(t, err)

	return hub
}

// A detailed presence entry is written to the shared Redis for every node's
// subscription API. It must not carry the subscriber's claims, the private
// topics the token grants or the requested Last-Event-ID, and the API must
// still render the subscriber, payload included, on another node.
func TestPresenceEntryOmitsClaims(t *testing.T) {
	t.Parallel()

	const (
		topic = "https://example.com/presence-claims"
		jti   = "jti-presence-7f3a"
	)

	mr := miniredis.RunT(t)
	newNode := func() *RedisTransport {
		transport, err := NewRedisTransport(redis.NewClient(&redis.Options{Addr: mr.Addr()}),
			withSkipVersionCheck(),
			WithLogger(testLogger()),
			WithXReadBlock(50*time.Millisecond),
			WithHealthInterval(24*time.Hour),
			WithPresenceInterval(24*time.Hour),
			withSkipPresenceIntervalCheck(),
			WithZombieGCInterval(0),
		)
		require.NoError(t, err)
		t.Cleanup(func() { transport.Close(context.Background()) })

		return transport
	}

	subscriberNode, otherNode := newNode(), newNode()

	// Subscribe on the first node with a token carrying claims.
	srv := httptest.NewServer(newClaimsTestHub(t, subscriberNode))
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/.well-known/mercure?"+url.Values{"match": {topic}, "last_event_id": {mercure.EarliestLastEventID}}.Encode(), nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+subscribeTestJWT(topic, jti))

	resp, err := srv.Client().Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Eventually(t, func() bool { return subscriberNode.subscriberCount.Load() == 1 },
		5*time.Second, 10*time.Millisecond, "the subscriber must be added")

	subscriberNode.publishPresence(t.Context())

	raw, err := mr.Get(key(subscriberNode.opts.streamName, ":presence:"+subscriberNode.nodeID))
	require.NoError(t, err)

	var entries []map[string]any

	require.NoError(t, json.Unmarshal([]byte(raw), &entries))
	require.Len(t, entries, 1)
	assert.Nil(t, entries[0]["Claims"], "the presence entry must carry no claims")
	assert.Nil(t, entries[0]["AllowedPrivateMatchers"], "the presence entry must carry no private-topic grants")
	assert.Empty(t, entries[0]["RequestLastEventID"], "the presence entry must carry no requested Last-Event-ID")
	assert.Equal(t, false, entries[0]["RequestLastEventIDSet"])
	assert.NotContains(t, raw, jti)
	assert.NotContains(t, raw, "authorization_details")

	// The other node's subscription API renders the subscriber from presence.
	listing := httptest.NewRecorder()
	listReq := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/.well-known/mercure/subscriptions", nil)
	listReq.Header.Set("Authorization", "Bearer "+subscribeTestJWT("/.well-known/mercure/subscriptions", "jti-reader"))
	newClaimsTestHub(t, otherNode).ServeHTTP(listing, listReq)
	require.Equal(t, http.StatusOK, listing.Code, listing.Body.String())

	var collection struct {
		Subscriptions []map[string]any `json:"subscriptions"`
	}

	require.NoError(t, json.Unmarshal(listing.Body.Bytes(), &collection))
	require.Len(t, collection.Subscriptions, 1)

	escapedMatchers, ok := entries[0]["EscapedMatchers"].([]any)
	require.True(t, ok)
	require.Len(t, escapedMatchers, 1)

	assert.Equal(t, map[string]any{
		"id":         "/.well-known/mercure/subscriptions/" + escapedMatchers[0].(string) + "/" + entries[0]["EscapedID"].(string),
		"type":       "subscription",
		"subscriber": entries[0]["ID"],
		"active":     true,
		"match":      topic,
		"match_type": "exact",
		"payload":    map[string]any{"role": "reader"},
	}, collection.Subscriptions[0])
}
