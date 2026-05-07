package mercure

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// demoTestOrigin is the concrete origin the JS template vars are resolved to so
// Go tests can assert on the values the demo UI actually ships.
const demoTestOrigin = "https://example.test"

// demoSource returns the bytes of the demo UI script (public/app.js), the source
// of truth for the topics and form wiring the UI ships.
func demoSource(t *testing.T) []byte {
	t.Helper()

	src, err := os.ReadFile("public/app.js")
	require.NoError(t, err)

	return src
}

// demoTopicConstant returns the named demo topic constant (e.g.
// UI_PLACEHOLDER_TOPIC, UI_DISCOVER_TOPIC) from public/app.js with its JS
// template vars resolved to a concrete origin.
func demoTopicConstant(t *testing.T, name string) string {
	t.Helper()

	re := regexp.MustCompile(name + ":\\s*`([^`]+)`")
	m := re.FindSubmatch(demoSource(t))
	require.NotNilf(t, m, "%s not found in public/app.js — update this regression test", name)

	// Order matters: defaultHubUrl is itself `${window.location.origin}/.well-known/mercure`
	// (app.js), so the compound var must be expanded before the bare origin var.
	// UI_DISCOVER_TOPIC uses ${defaultHubUrl}; UI_PLACEHOLDER_TOPIC uses only the bare origin.
	resolved := string(m[1])
	resolved = strings.ReplaceAll(resolved, "${defaultHubUrl}", demoTestOrigin+defaultHubURL)
	resolved = strings.ReplaceAll(resolved, "${window.location.origin}", demoTestOrigin)

	require.NotContainsf(t, resolved, "${",
		"unresolved JS template var in demo topic %q — extend the substitutions in demoTopicConstant", resolved)

	return resolved
}

// demoPlaceholderTopic returns the demo UI's default publish/subscribe topic
// (UI_PLACEHOLDER_TOPIC in public/app.js), resolved to a concrete origin.
func demoPlaceholderTopic(t *testing.T) string {
	t.Helper()

	return demoTopicConstant(t, "UI_PLACEHOLDER_TOPIC")
}

// demoSubscribeClaims decodes the named bundled demo token
// (public/fixtures/tokens.json, e.g. "hs256" / "rs256") and returns its
// mercure.subscribe selectors — the set of topics the demo UI's shipped token is
// authorized to receive. Used to guard against the default topic and the token
// drifting apart.
func demoSubscribeClaims(t *testing.T, key string) []string {
	t.Helper()

	raw, err := os.ReadFile("public/fixtures/tokens.json")
	require.NoError(t, err)

	var tokens map[string]string
	require.NoError(t, json.Unmarshal(raw, &tokens))

	tok := tokens[key]
	require.NotEmptyf(t, tok, "%s demo token missing from public/fixtures/tokens.json", key)

	parts := strings.Split(tok, ".")
	require.Lenf(t, parts, 3, "malformed %s demo JWT in fixtures: %q", key, tok)

	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	require.NoError(t, err)

	var claims struct {
		Mercure struct {
			Subscribe []string `json:"subscribe"`
		} `json:"mercure"`
	}
	require.NoError(t, json.Unmarshal(payload, &claims))
	require.NotEmpty(t, claims.Mercure.Subscribe, "demo token carries no subscribe claim")

	return claims.Mercure.Subscribe
}

// TestDemoDefaultTopicIsPublishable guards the demo UI's default publish topic
// against the reserved "/.well-known/mercure" namespace.
//
// The demo UI populates its publish (and subscribe) forms from
// UI_PLACEHOLDER_TOPIC. validateTopics rejects any topic containing the reserved
// substring with 403 (the reserved-topic forgery guard), and the demo flag does
// NOT relax publish validation — so a default topic under the hub path makes the
// demo's publish button 403. Regression test for the bug where UI_PLACEHOLDER_TOPIC
// was built as `${defaultHubUrl}/ui/demo/books/1.jsonld` (defaultHubUrl already
// includes /.well-known/mercure).
func TestDemoDefaultTopicIsPublishable(t *testing.T) {
	t.Parallel()

	topic := demoPlaceholderTopic(t)

	// The invariant — not merely the one historical bug spelling: the demo's
	// default publish topic must not sit in the reserved namespace, in any shape.
	require.NotContainsf(t, topic, reservedTopicSubstring,
		"demo default topic %q is in the reserved namespace", topic)
	require.NoErrorf(t, validateTopics([]string{topic}),
		"the demo UI default topic %q must be publishable", topic)

	// Positive control: a hub-path topic MUST be rejected for the reserved reason —
	// proves this test catches a regression rather than passing vacuously.
	require.ErrorIs(t,
		validateTopics([]string{demoTestOrigin + defaultHubURL + "/ui/demo/books/1.jsonld"}), ErrReservedTopic,
		"positive control: a topic under the hub path must be rejected as reserved")
}

// TestDemoDiscoverTopicStaysUnderHubPath is the inverse invariant of
// TestDemoDefaultTopicIsPublishable: the discover/cookie URL (UI_DISCOVER_TOPIC)
// is the reflector RESOURCE served by the Demo handler under the hub path, and it
// MUST stay there — it is fetched with GET (no reserved-topic guard), unlike the
// pub/sub topic which is published with POST (guarded). Together the two tests
// pin the decouple: the two constants must not collapse back into one.
func TestDemoDiscoverTopicStaysUnderHubPath(t *testing.T) {
	t.Parallel()

	discover := demoTopicConstant(t, "UI_DISCOVER_TOPIC")

	require.Containsf(t, discover, reservedTopicSubstring,
		"the demo discover topic %q must stay under the hub path (it is the reflector resource)", discover)
}

// TestDemoFormsWiredToCorrectTopicConstants guards the form WIRING, which the
// topic-value tests above do not cover: each demo form's default must be
// populated from the constant with the correct reserved-namespace property.
// Regression test for wiring swaps — e.g. discover ← UI_PLACEHOLDER_TOPIC breaks
// cookie-auth/discovery, and subscribe/publish ← UI_DISCOVER_TOPIC reintroduces
// the 403 — either of which would leave the value-level tests above still green.
func TestDemoFormsWiredToCorrectTopicConstants(t *testing.T) {
	t.Parallel()

	src := demoSource(t)

	for _, tc := range []struct {
		field    string
		expected string
	}{
		{"DOM.subscribeForm.topics.value", "UI_PLACEHOLDER_TOPIC"},
		{"DOM.publishForm.topics.value", "UI_PLACEHOLDER_TOPIC"},
		{"DOM.discoverForm.topic.value", "UI_DISCOVER_TOPIC"},
	} {
		re := regexp.MustCompile(regexp.QuoteMeta(tc.field) + `\s*=\s*CONFIG\.(\w+)`)
		m := re.FindSubmatch(src)
		require.NotNilf(t, m, "wiring for %s not found in public/app.js — update this regression test", tc.field)
		assert.Equalf(t, tc.expected, string(m[1]),
			"%s must be populated from CONFIG.%s", tc.field, tc.expected)
	}
}

// TestDemoDefaultTopicMatchesSubscribeClaim guards the subscribe side: the demo
// UI feeds the same UI_PLACEHOLDER_TOPIC into its subscribe form, so the default
// topic must be authorized by the bundled demo token's subscribe claim. Without
// this, a drift in public/fixtures/tokens.json (or in the topic) could make the
// demo's Subscribe button fail while the publish-side tests stayed green.
func TestDemoDefaultTopicMatchesSubscribeClaim(t *testing.T) {
	t.Parallel()

	topic := demoPlaceholderTopic(t)

	tss, err := NewTopicSelectorStore(DefaultTopicSelectorStoreCacheSize)
	require.NoError(t, err)

	// The demo UI ships both an HS256 and an RS256 token (selectable via the JWT
	// Algorithm toggle); the default topic must be authorized by BOTH so neither
	// can silently drift out of sync with the topic.
	for _, key := range []string{"hs256", "rs256"} {
		subscribe := demoSubscribeClaims(t, key)
		require.Truef(t, canReceive(tss, []string{topic}, subscribe),
			"the demo default topic %q must be authorized by the %s token subscribe claim %v", topic, key, subscribe)

		// Positive control: an unrelated topic must NOT match the claim, proving
		// the assertion is not vacuously true for any input.
		require.Falsef(t, canReceive(tss, []string{demoTestOrigin + "/unrelated/42"}, subscribe),
			"positive control: an unrelated topic must not match the %s subscribe claim %v", key, subscribe)
	}
}

// TestDemoDefaultTopicPublishHandlerOK is the end-to-end companion: it POSTs the
// demo UI's default topic through the real PublishHandler with a synthetic
// publisher JWT carrying mercure.publish ["*"] (the same publish claim the demo
// token holds) and asserts 200 — exercising the full validateTopics ->
// authorization -> dispatch path, not just the unit rule. A reserved-namespace
// regression surfaces here as a 403.
func TestDemoDefaultTopicPublishHandlerOK(t *testing.T) {
	t.Parallel()

	topic := demoPlaceholderTopic(t)
	hub := createDummy(t)

	form := url.Values{}
	form.Add("topic", topic)
	form.Add("data", "Hello!")

	req := httptest.NewRequest(http.MethodPost, defaultHubURL, strings.NewReader(form.Encode()))
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Add("Authorization", bearerPrefix+createDummyAuthorizedJWT(rolePublisher, []string{"*"}))

	w := httptest.NewRecorder()
	hub.PublishHandler(w, req)

	resp := w.Result()

	t.Cleanup(func() {
		assert.NoError(t, resp.Body.Close())
	})

	assert.Equalf(t, http.StatusOK, resp.StatusCode,
		"publishing the demo UI default topic %q must succeed (not 403 reserved-namespace)", topic)
}
