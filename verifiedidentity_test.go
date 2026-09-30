package mercure

import (
	"context"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

const identityTopic = "https://example.com/books/1"

// identityToken mints a token for r carrying sub, client_id and jti, plus extra claims.
func identityToken(tb testing.TB, r role, topics []string, extra map[string]any) string {
	tb.Helper()

	claims := map[string]any{"sub": "user-1", "client_id": "client-1", "jti": "token-1"}
	maps.Copy(claims, extra)

	return createDummyJWTWithExtraClaims(tb, r, topics, claims)
}

// identitySink returns ctx carrying a callback that records every identity the hub reports.
func identitySink(ctx context.Context) (context.Context, *[]VerifiedIdentity) {
	var got []VerifiedIdentity

	return NewVerifiedIdentityContext(ctx, func(_ context.Context, id VerifiedIdentity) {
		got = append(got, id)
	}), &got
}

// recordingSpan returns a recording span, named "request", and the recorder that receives it
// once ended. The span is ended at cleanup if the test has not ended it.
func recordingSpan(t *testing.T) (trace.Span, *tracetest.SpanRecorder) {
	t.Helper()

	sr := tracetest.NewSpanRecorder()
	_, span := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr)).Tracer("test").Start(t.Context(), "request")
	t.Cleanup(func() { span.End() })

	return span, sr
}

// endedAttributes ends the span recordingSpan returned and returns its attributes.
func endedAttributes(t *testing.T, span trace.Span, sr *tracetest.SpanRecorder) map[attribute.Key]string {
	t.Helper()

	span.End()

	return spanAttributesByName(t, sr, "request")
}

// identityKeys are the span attributes that carry a verified identity.
func identityKeys(attrs map[attribute.Key]string) map[attribute.Key]string {
	out := map[attribute.Key]string{}

	for k, v := range attrs {
		if k == "user.id" || k == "mercure.client_id" ||
			k == "mercure.role" || strings.HasPrefix(string(k), "mercure.binding.") {
			out[k] = v
		}
	}

	return out
}

// identityHub is a hub with a header, a value and an on_missing allow binding.
func identityHub(tb testing.TB, opts ...Option) *Hub {
	tb.Helper()

	return createDummy(tb, append([]Option{WithClaimHeaderBindings(
		mustBinding(tb, "groups", "Group-ID"),
		mustValueBinding(tb, "plan", []string{"pro"}),
		mustBinding(tb, "region", "X-Region", WithBindingOnMissing("allow")),
	)}, opts...)...)
}

// A verified request whose bindings all passed reports its identity to the context callback
// and onto its span, for both roles. A binding that on_missing allow skipped matched nothing,
// so it is not reported.
func TestVerifiedIdentityReported(t *testing.T) {
	t.Parallel()

	for _, publish := range []bool{false, true} {
		role, r := "subscriber", roleSubscriber
		if publish {
			role, r = "publisher", rolePublisher
		}

		t.Run(role, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodGet, defaultHubURL, nil)
			req.Header.Set("Authorization", bearerPrefix+identityToken(t, r, []string{identityTopic}, map[string]any{"groups": []string{"red"}, "plan": "pro"}))
			req.Header.Set("Group-ID", "red")

			ctx, got := identitySink(req.Context())
			span, sr := recordingSpan(t)

			c, _, err := identityHub(t).authorizeAndBindCounted(req.WithContext(ctx), span, publish)
			require.NoError(t, err)
			require.NotNil(t, c)

			assert.Equal(t, []VerifiedIdentity{{
				Subject:  "user-1",
				ClientID: "client-1",
				JWTID:    "token-1",
				Role:     role,
				Bindings: []BindingMatch{{Claim: "groups", Value: "red"}, {Claim: "plan", Value: "pro"}},
			}}, *got)

			assert.Equal(t, map[attribute.Key]string{
				"user.id":                "user-1",
				"mercure.client_id":      "client-1",
				"mercure.role":           role,
				"mercure.binding.groups": "red",
				"mercure.binding.plan":   "pro",
			}, endedAttributes(t, span, sr))
		})
	}
}

// Claims the token does not carry are left empty, and absent from the span.
func TestVerifiedIdentityOmitsAbsentClaims(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequest(http.MethodGet, defaultHubURL, nil)
	req.Header.Set("Authorization", bearerPrefix+createDummyJWTWithExtraClaims(t, roleSubscriber, []string{identityTopic}, nil))

	ctx, got := identitySink(req.Context())
	span, sr := recordingSpan(t)

	_, _, err := createDummy(t).authorizeAndBindCounted(req.WithContext(ctx), span, false)
	require.NoError(t, err)

	assert.Equal(t, []VerifiedIdentity{{Role: "subscriber"}}, *got)
	assert.Equal(t, map[attribute.Key]string{"mercure.role": "subscriber"}, endedAttributes(t, span, sr))
}

// client_id is only reported, so a token whose client_id is not a string still verifies and
// reports no client_id.
func TestNonStringClientIDStillVerifies(t *testing.T) {
	t.Parallel()

	for name, clientID := range map[string]any{
		"number": 123,
		"object": map[string]any{"id": "client-1"},
		"array":  []string{"client-1"},
		"bool":   true,
		"null":   nil,
	} {
		for _, publish := range []bool{false, true} {
			role, r := identityRoleSubscriber, roleSubscriber
			if publish {
				role, r = identityRolePublisher, rolePublisher
			}

			t.Run(name+" "+role, func(t *testing.T) {
				t.Parallel()

				req := httptest.NewRequest(http.MethodGet, defaultHubURL, nil)
				req.Header.Set("Authorization", bearerPrefix+identityToken(t, r, []string{identityTopic}, map[string]any{"client_id": clientID}))

				ctx, got := identitySink(req.Context())

				c, _, err := createDummy(t).authorizeAndBindCounted(req.WithContext(ctx), noSpan, publish)
				require.NoError(t, err)
				require.NotNil(t, c)

				assert.Equal(t, []VerifiedIdentity{{Subject: "user-1", JWTID: "token-1", Role: role}}, *got)
			})
		}
	}
}

// No identity is reported for a request without a token, with an invalid one, or rejected
// by a binding, whichever check rejects it.
func TestVerifiedIdentityNotReportedOnRejection(t *testing.T) {
	t.Parallel()

	validWith := func(extra map[string]any) func(testing.TB) string {
		return func(tb testing.TB) string {
			tb.Helper()

			claims := map[string]any{"groups": []string{"red"}, "plan": "pro"}
			maps.Copy(claims, extra)

			return identityToken(tb, roleSubscriber, []string{identityTopic}, claims)
		}
	}

	for name, tc := range map[string]struct {
		token   func(testing.TB) string
		groupID string
		wantErr bool
	}{
		"anonymous": {token: func(testing.TB) string { return "" }, groupID: "red"},
		"invalid token": {token: func(tb testing.TB) string {
			tb.Helper()

			return mintAccessToken([]byte("wrong key"), testResourceIdentifier, nil)
		}, groupID: "red", wantErr: true},
		"binding mismatch (403)":      {token: validWith(nil), groupID: "blue", wantErr: true},
		"missing bound header (400)":  {token: validWith(nil), wantErr: true},
		"malformed bound claim (401)": {token: validWith(map[string]any{"groups": 1}), groupID: "red", wantErr: true},
		"claim value absent (403)":    {token: validWith(map[string]any{"plan": "free"}), groupID: "red", wantErr: true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodGet, defaultHubURL, nil)
			if token := tc.token(t); token != "" {
				req.Header.Set("Authorization", bearerPrefix+token)
			}

			if tc.groupID != "" {
				req.Header.Set("Group-ID", tc.groupID)
			}

			ctx, got := identitySink(req.Context())
			span, sr := recordingSpan(t)

			_, _, err := identityHub(t, WithAnonymous()).authorizeAndBindCounted(req.WithContext(ctx), span, false)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			assert.Empty(t, *got)
			assert.Empty(t, endedAttributes(t, span, sr))
		})
	}
}

// Every value taken from the token or a bound header is capped at 256 bytes.
func TestVerifiedIdentityCapsValues(t *testing.T) {
	t.Parallel()

	long := strings.Repeat("a", 300)

	req := httptest.NewRequest(http.MethodGet, defaultHubURL, nil)
	req.Header.Set("Authorization", bearerPrefix+createDummyJWTWithExtraClaims(t, roleSubscriber, []string{identityTopic}, map[string]any{
		"sub": long, "client_id": long, "jti": long, "groups": []string{long},
	}))
	req.Header.Set("Group-ID", long)

	ctx, got := identitySink(req.Context())
	span, sr := recordingSpan(t)

	h := createDummy(t, WithClaimHeaderBindings(mustBinding(t, "groups", "Group-ID")))
	_, _, err := h.authorizeAndBindCounted(req.WithContext(ctx), span, false)
	require.NoError(t, err)

	// The literal, not maxIdentityValueLen, so a changed cap fails here.
	capped := long[:256]
	assert.Equal(t, []VerifiedIdentity{{
		Subject:  capped,
		ClientID: capped,
		JWTID:    capped,
		Role:     "subscriber",
		Bindings: []BindingMatch{{Claim: "groups", Value: capped}},
	}}, *got)

	attrs := endedAttributes(t, span, sr)
	assert.Equal(t, capped, attrs["user.id"])
	assert.Equal(t, capped, attrs["mercure.client_id"])
	assert.Equal(t, capped, attrs["mercure.binding.groups"])
}

func TestCapIdentityValue(t *testing.T) {
	t.Parallel()

	exact := strings.Repeat("a", 256)
	assert.Equal(t, exact, capIdentityValue(exact))
	assert.Equal(t, exact, capIdentityValue(exact+"b"))

	// A two-byte rune straddling the limit is dropped whole, not split.
	straddling := strings.Repeat("a", 255) + "é"
	assert.Equal(t, strings.Repeat("a", 255), capIdentityValue(straddling))

	var controls strings.Builder
	for c := range 0x20 {
		controls.WriteByte(byte(c))
	}

	controls.WriteByte(0x7f)
	assert.Empty(t, capIdentityValue(controls.String()))
	assert.Equal(t, "user-1", capIdentityValue("us\r\ner\x00\x1b-\x7f1"))
	assert.Equal(t, " ~é€", capIdentityValue(" ~\x7fé\n€"), "the characters next to the controls are kept")

	// Bytes that are not valid UTF-8 are not controls and are left alone.
	assert.Equal(t, "a\xffb\xc3", capIdentityValue("a\xff\x00b\xc3"))

	padded := strings.Repeat("a\x00", 300)
	assert.Equal(t, strings.Repeat("a", 256), capIdentityValue(padded))

	// A rune that straddles the cap is dropped whole, however much input follows.
	ctrlStraddling := strings.Repeat("a", 255) + "\x00é" + strings.Repeat("b\x00", 1000)
	assert.Equal(t, strings.Repeat("a", 255), capIdentityValue(ctrlStraddling))
}

// A value without a control character, the usual case, is returned without allocating, whether
// or not it is cut. Not parallel: AllocsPerRun refuses to run in a parallel test.
func TestCapIdentityValueDoesNotAllocate(t *testing.T) {
	long := strings.Repeat("a", 300)

	assert.InDelta(t, 0, testing.AllocsPerRun(100, func() { _ = capIdentityValue("user-1") }), 0)
	assert.InDelta(t, 0, testing.AllocsPerRun(100, func() { _ = capIdentityValue(long) }), 0)
}

// A signed claim carrying a NUL, an ESC or a line break reports without them, from the token
// and from a bound header alike.
func TestVerifiedIdentityDropsControlCharacters(t *testing.T) {
	t.Parallel()

	const dirty = "\r\n\x00\x1bab\x7f\tcd"

	req := httptest.NewRequest(http.MethodGet, defaultHubURL, nil)
	req.Header.Set("Authorization", bearerPrefix+createDummyJWTWithExtraClaims(t, roleSubscriber, []string{identityTopic}, map[string]any{
		"sub": dirty, "client_id": dirty, "jti": dirty, "groups": []string{dirty},
	}))
	req.Header["Group-Id"] = []string{dirty}

	ctx, got := identitySink(req.Context())
	span, sr := recordingSpan(t)

	h := createDummy(t, WithClaimHeaderBindings(mustBinding(t, "groups", "Group-ID")))
	_, _, err := h.authorizeAndBindCounted(req.WithContext(ctx), span, false)
	require.NoError(t, err)

	assert.Equal(t, []VerifiedIdentity{{
		Subject:  "abcd",
		ClientID: "abcd",
		JWTID:    "abcd",
		Role:     "subscriber",
		Bindings: []BindingMatch{{Claim: "groups", Value: "abcd"}},
	}}, *got)

	attrs := endedAttributes(t, span, sr)
	assert.Equal(t, "abcd", attrs["user.id"])
	assert.Equal(t, "abcd", attrs["mercure.client_id"])
	assert.Equal(t, "abcd", attrs["mercure.binding.groups"])
}

// Once the identity is reported, the claims a subscriber pins for its whole connection no
// longer hold the client_id.
func TestAuthorizeAndBindClearsClientID(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequest(http.MethodGet, defaultHubURL, nil)
	req.Header.Set("Authorization", bearerPrefix+identityToken(t, roleSubscriber, []string{identityTopic}, nil))

	ctx, got := identitySink(req.Context())

	c, _, err := createDummy(t).authorizeAndBindCounted(req.WithContext(ctx), noSpan, false)
	require.NoError(t, err)
	require.NotNil(t, c)

	require.Len(t, *got, 1)
	assert.Equal(t, "client-1", (*got)[0].ClientID)
	assert.Empty(t, c.ClientID)
	assert.Equal(t, "user-1", c.Subject, "the claims the hub keeps for authorization are untouched")
}

// A binding on_missing allow skipped matched nothing: when the request sends the bound header
// but the token lacks the claim, no value is reported, whatever the header said.
func TestVerifiedIdentitySkippedBindingReportsNothing(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequest(http.MethodGet, defaultHubURL, nil)
	req.Header.Set("Authorization", bearerPrefix+identityToken(t, roleSubscriber, []string{identityTopic}, map[string]any{"groups": []string{"red"}, "plan": "pro"}))
	req.Header.Set("Group-ID", "red")
	req.Header.Set("X-Region", "eu")

	ctx, got := identitySink(req.Context())
	span, sr := recordingSpan(t)

	_, _, err := identityHub(t).authorizeAndBindCounted(req.WithContext(ctx), span, false)
	require.NoError(t, err)

	require.Len(t, *got, 1)
	assert.Equal(t, []BindingMatch{{Claim: "groups", Value: "red"}, {Claim: "plan", Value: "pro"}}, (*got)[0].Bindings)

	attrs := endedAttributes(t, span, sr)
	assert.NotContains(t, attrs, attribute.Key("mercure.binding.region"))
	assert.Equal(t, "red", attrs["mercure.binding.groups"])
}

// A recording span gets the identity, binding values included, when the embedder registered no
// callback.
func TestVerifiedIdentityRecordingSpanWithoutCallback(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequest(http.MethodGet, defaultHubURL, nil)
	req.Header.Set("Authorization", bearerPrefix+identityToken(t, roleSubscriber, []string{identityTopic}, map[string]any{"groups": []string{"red"}, "plan": "pro"}))
	req.Header.Set("Group-ID", "red")

	span, sr := recordingSpan(t)

	_, _, err := identityHub(t).authorizeAndBindCounted(req, span, false)
	require.NoError(t, err)

	assert.Equal(t, map[attribute.Key]string{
		"user.id":                "user-1",
		"mercure.client_id":      "client-1",
		"mercure.role":           "subscriber",
		"mercure.binding.groups": "red",
		"mercure.binding.plan":   "pro",
	}, endedAttributes(t, span, sr))
}

// Without a callback and with a span that does not record, enforceBindings collects nothing.
// Unlike the allocation count below, this holds under the race detector too.
func TestEnforceBindingsCollectsOnlyWhenAsked(t *testing.T) {
	t.Parallel()

	h := identityHub(t)

	req := httptest.NewRequest(http.MethodGet, defaultHubURL, nil)
	req.Header.Set("Authorization", bearerPrefix+identityToken(t, roleSubscriber, []string{identityTopic}, map[string]any{"groups": []string{"red"}, "plan": "pro"}))
	req.Header.Set("Group-ID", "red")

	c, err := h.authorize(req, false)
	require.NoError(t, err)

	_, matches, err := h.enforceBindings(req, c, false, false)
	require.NoError(t, err)
	assert.Nil(t, matches)

	_, matches, err = h.enforceBindings(req, c, false, true)
	require.NoError(t, err)
	assert.Equal(t, []BindingMatch{{Claim: "groups", Value: "red"}, {Claim: "plan", Value: "pro"}}, matches)
}

// The publish, subscribe and subscription API handlers report the identity on their own span
// and to the context callback, and neither on a rejected request.
func TestVerifiedIdentityOnHandlerSpans(t *testing.T) {
	t.Parallel()

	const badToken = "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6ImF0K2p3dCJ9.e30.c2lnbmF0dXJlLXRoYXQtZG9lcy1ub3QtdmVyaWZ5"

	cases := map[string]struct {
		span  string
		role  string
		serve func(t *testing.T, h *Hub, ctx context.Context, auth string, ok bool)
	}{
		"publish": {"mercure.publish", "publisher", func(t *testing.T, h *Hub, ctx context.Context, auth string, ok bool) {
			t.Helper()

			body := url.Values{"topic": {identityTopic}, "data": {"x"}}
			req := httptest.NewRequestWithContext(ctx, http.MethodPost, defaultHubURL, strings.NewReader(body.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			req.Header.Set("Authorization", auth)

			w := httptest.NewRecorder()
			h.PublishHandler(w, req)

			assert.Equal(t, ok, w.Code == http.StatusOK, w.Code)
		}},
		"subscribe": {"mercure.subscribe", "subscriber", func(t *testing.T, h *Hub, ctx context.Context, auth string, ok bool) {
			t.Helper()

			reqCtx, cancel := context.WithCancel(ctx)
			defer cancel()

			req := httptest.NewRequestWithContext(reqCtx, http.MethodGet, defaultHubURL+"?match="+url.QueryEscape(identityTopic), nil)
			req.Header.Set("Authorization", auth)

			if !ok {
				w := httptest.NewRecorder()
				h.SubscribeHandler(w, req)
				assert.Equal(t, http.StatusUnauthorized, w.Code)

				return
			}

			h.SubscribeHandler(&responseTester{expectedStatusCode: http.StatusOK, expectedBody: ":\n", tb: t, cancel: cancel}, req)
		}},
		"subscription API": {"mercure.subscriptions", "subscriber", func(t *testing.T, h *Hub, ctx context.Context, auth string, ok bool) {
			t.Helper()

			req := httptest.NewRequestWithContext(ctx, http.MethodGet, subscriptionsURL, nil)
			req.Header.Set("Authorization", auth)

			w := httptest.NewRecorder()
			h.SubscriptionsHandler(w, req)

			assert.Equal(t, ok, w.Code == http.StatusOK, w.Code)
		}},
	}

	for name, tc := range cases {
		r, topics := roleSubscriber, []string{identityTopic, subscriptionsURL}
		if tc.role == "publisher" {
			r, topics = rolePublisher, []string{identityTopic}
		}

		t.Run(name+" verified", func(t *testing.T) {
			t.Parallel()

			ctx, sr := spanRecorder(t)
			ctx, got := identitySink(ctx)

			tc.serve(t, createDummy(t, WithSubscriptions()), ctx, bearerPrefix+identityToken(t, r, topics, nil), true)

			require.Len(t, *got, 1)
			assert.Equal(t, "user-1", (*got)[0].Subject)
			assert.Equal(t, map[attribute.Key]string{
				"user.id":           "user-1",
				"mercure.client_id": "client-1",
				"mercure.role":      tc.role,
			}, identityKeys(spanAttributesByName(t, sr, tc.span)))
		})

		t.Run(name+" rejected", func(t *testing.T) {
			t.Parallel()

			ctx, sr := spanRecorder(t)
			ctx, got := identitySink(ctx)

			tc.serve(t, createDummy(t, WithSubscriptions()), ctx, badToken, false)

			assert.Empty(t, *got)
			assert.Empty(t, identityKeys(spanAttributesByName(t, sr, tc.span)))
		})
	}
}

// spanAttributesByName returns the attributes of the last ended span called name: a publish
// request ends Hub.Publish's inner mercure.publish span before the handler's own.
func spanAttributesByName(t *testing.T, sr *tracetest.SpanRecorder, name string) map[attribute.Key]string {
	t.Helper()

	for _, s := range slices.Backward(sr.Ended()) {
		if s.Name() != name {
			continue
		}

		attrs := map[attribute.Key]string{}
		for _, kv := range s.Attributes() {
			attrs[kv.Key] = kv.Value.String()
		}

		return attrs
	}

	require.FailNow(t, "span not ended", name)

	return nil
}

const allocRuns = 2000

// meanAllocs is testing.AllocsPerRun without the truncation to a whole number. A garbage
// collection empties the sync.Pool caches the JSON decoding draws from, so a run sometimes
// allocates a fraction more; truncated, that fraction can flip the count by one.
func meanAllocs(f func()) float64 {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))

	f()

	var before, after runtime.MemStats

	runtime.ReadMemStats(&before)

	for range allocRuns {
		f()
	}

	runtime.ReadMemStats(&after)

	return float64(after.Mallocs-before.Mallocs) / float64(allocRuns)
}

// With no callback and a non-recording span, the identity costs no allocation.
// enforceBindings(collect=false) must allocate less than collect=true, and a verified request
// must allocate what authorize plus enforceBindings(collect=false) allocate. A recording span
// must allocate more, which proves the measurement sees the work. The 0.5 tolerance absorbs
// sync.Pool noise; one extra allocation adds 1.
//
// Skipped under -race (sync.Pool drops items at random); task go:test runs it without -race.
// Not parallel: allocation counters are process-wide.
func TestVerifiedIdentityAllocatesNothingWhenUnused(t *testing.T) {
	if raceEnabled {
		t.Skip("allocation counts are not deterministic under the race detector")
	}

	for name, opts := range map[string][]Option{
		"no binding":  nil,
		"one binding": {WithClaimHeaderBindings(mustBinding(t, "groups", "Group-ID"))},
	} {
		t.Run(name, func(t *testing.T) {
			h := createDummy(t, opts...)

			req := httptest.NewRequest(http.MethodGet, defaultHubURL, nil)
			req.Header.Set("Authorization", bearerPrefix+identityToken(t, roleSubscriber, []string{identityTopic}, map[string]any{"groups": []string{"red"}}))
			req.Header.Set("Group-ID", "red")

			c, err := h.authorize(req, false)
			require.NoError(t, err)

			enforce := func(collect bool) float64 {
				return meanAllocs(func() {
					if _, _, err := h.enforceBindings(req, c, false, collect); err != nil {
						panic(err)
					}
				})
			}

			enforceUnused, enforceCollecting := enforce(false), enforce(true)

			base := meanAllocs(func() {
				c, err := h.authorize(req, false)
				if err != nil {
					panic(err)
				}

				if _, _, err := h.enforceBindings(req, c, false, false); err != nil {
					panic(err)
				}
			})

			unused := meanAllocs(func() {
				if _, _, err := h.authorizeAndBindCounted(req, noSpan, false); err != nil {
					panic(err)
				}
			})

			span, _ := recordingSpan(t)

			used := meanAllocs(func() {
				if _, _, err := h.authorizeAndBindCounted(req, span, false); err != nil {
					panic(err)
				}
			})

			t.Logf("allocs/run: enforce_unused=%.2f enforce_collecting=%.2f base=%.2f unused=%.2f used=%.2f",
				enforceUnused, enforceCollecting, base, unused, used)

			if len(opts) > 0 {
				assert.Less(t, enforceUnused, enforceCollecting-0.5, "enforceBindings must build no match unless asked to")
			}

			assert.InDelta(t, base, unused, 0.5, "no callback and no recording span must add no allocation")
			assert.Greater(t, used, unused+0.5, "a recording span must make the identity work visible to the measurement")
		})
	}
}
