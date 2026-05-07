package mercure

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/golang-jwt/jwt/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// claimsWithRaw builds a *claims carrying only the raw top-level claims a binding reads.
// rawJSON of "" means the claim key is absent entirely.
func claimsWithRaw(tb testing.TB, name, rawJSON string) *claims {
	tb.Helper()

	c := &claims{rawClaims: map[string]json.RawMessage{}}
	if rawJSON != "" {
		c.rawClaims[name] = json.RawMessage(rawJSON)
	}

	return c
}

// reqWithHeader builds a request. vals nil means the header is absent.
func reqWithHeader(tb testing.TB, name string, vals []string) *http.Request {
	tb.Helper()

	r := httptest.NewRequest(http.MethodGet, "/", nil)
	for _, v := range vals {
		r.Header.Add(name, v)
	}

	return r
}

func mustBinding(tb testing.TB, claim, header string, opts ...BindingOption) ClaimHeaderBinding {
	tb.Helper()

	b, err := NewClaimHeaderBinding(claim, header, opts...)
	require.NoError(tb, err)

	return b
}

func TestClaimHeaderBindingValidate(t *testing.T) {
	t.Parallel()

	const claim, header = "tenants", "Tenant-ID"

	testCases := []struct {
		name       string
		headerVals []string // nil ⇒ absent
		claimJSON  string   // "" ⇒ claim key absent
		opts       []BindingOption
		wantReason AuthzRejectReason // "" ⇒ allowed/skipped (no error)
	}{
		// --- happy paths -------------------------------------------------
		{name: "auto/array member", headerVals: []string{"acme"}, claimJSON: `["acme","beta"]`},
		{name: "auto/scalar exact", headerVals: []string{"acme"}, claimJSON: `"acme"`},
		{name: "member/array", headerVals: []string{"beta"}, claimJSON: `["acme","beta"]`, opts: []BindingOption{WithBindingMatch("member")}},
		{name: "exact/scalar", headerVals: []string{"acme"}, claimJSON: `"acme"`, opts: []BindingOption{WithBindingMatch("exact")}},

		// --- mismatch ----------------------------------------------------
		{name: "auto/array not member", headerVals: []string{"zzz"}, claimJSON: `["acme"]`, wantReason: ReasonMismatch},
		{name: "auto/scalar differs", headerVals: []string{"zzz"}, claimJSON: `"acme"`, wantReason: ReasonMismatch},
		{name: "value comparison is case-sensitive", headerVals: []string{"ACME"}, claimJSON: `["acme"]`, wantReason: ReasonMismatch},

		// --- header_absent -----------------------------------------------
		{name: "header absent", headerVals: nil, claimJSON: `["acme"]`, wantReason: ReasonHeaderAbsent},

		// --- claim_absent ------------------------------------------------
		{name: "claim key missing", headerVals: []string{"acme"}, claimJSON: "", wantReason: ReasonClaimAbsent},
		{name: "claim null", headerVals: []string{"acme"}, claimJSON: `null`, wantReason: ReasonClaimAbsent},
		{name: "claim empty scalar", headerVals: []string{"acme"}, claimJSON: `""`, wantReason: ReasonClaimAbsent},
		{name: "claim empty array", headerVals: []string{"acme"}, claimJSON: `[]`, wantReason: ReasonClaimAbsent},
		{name: "member + empty array", headerVals: []string{"acme"}, claimJSON: `[]`, opts: []BindingOption{WithBindingMatch("member")}, wantReason: ReasonClaimAbsent},

		// --- malformed (header side) -------------------------------------
		{name: "header empty value", headerVals: []string{""}, claimJSON: `["acme"]`, wantReason: ReasonMalformed},
		{name: "header multiple values", headerVals: []string{"acme", "beta"}, claimJSON: `["acme"]`, wantReason: ReasonMalformed},

		// --- malformed (claim side) --------------------------------------
		{name: "claim object", headerVals: []string{"acme"}, claimJSON: `{"a":1}`, wantReason: ReasonMalformed},
		{name: "claim number", headerVals: []string{"acme"}, claimJSON: `42`, wantReason: ReasonMalformed},
		{name: "claim non-string element", headerVals: []string{"acme"}, claimJSON: `["acme",1]`, wantReason: ReasonMalformed},
		{name: "claim null element", headerVals: []string{"acme"}, claimJSON: `["acme",null]`, wantReason: ReasonMalformed},
		{name: "claim bool element", headerVals: []string{"acme"}, claimJSON: `["acme",true]`, wantReason: ReasonMalformed},
		{name: "claim nested-array element", headerVals: []string{"acme"}, claimJSON: `["acme",["x"]]`, wantReason: ReasonMalformed},
		{name: "claim object element", headerVals: []string{"acme"}, claimJSON: `["acme",{}]`, wantReason: ReasonMalformed},
		{name: "claim empty-string element alone", headerVals: []string{"acme"}, claimJSON: `[""]`, wantReason: ReasonMalformed},
		{name: "claim empty-string element mixed", headerVals: []string{"acme"}, claimJSON: `["acme",""]`, wantReason: ReasonMalformed},
		{name: "claim bool", headerVals: []string{"acme"}, claimJSON: `true`, wantReason: ReasonMalformed},

		// --- malformed (operand shape disagrees with the configured match mode)
		// exact demands a scalar, member demands an array. A claim of the other shape is
		// a config/token disagreement, not a value the operator meant to authorize.
		{name: "exact on array claim", headerVals: []string{"acme"}, claimJSON: `["acme"]`, opts: []BindingOption{WithBindingMatch("exact")}, wantReason: ReasonMalformed},
		{name: "exact on empty array claim", headerVals: []string{"acme"}, claimJSON: `[]`, opts: []BindingOption{WithBindingMatch("exact")}, wantReason: ReasonMalformed},
		{name: "member on scalar claim", headerVals: []string{"acme"}, claimJSON: `"acme"`, opts: []BindingOption{WithBindingMatch("member")}, wantReason: ReasonMalformed},
		{name: "member on empty scalar claim", headerVals: []string{"acme"}, claimJSON: `""`, opts: []BindingOption{WithBindingMatch("member")}, wantReason: ReasonMalformed},

		// --- precedence: malformed beats on_missing=allow (fail-open guard)
		{name: "allow + multiple headers still rejects", headerVals: []string{"acme", "beta"}, claimJSON: `["acme"]`, opts: []BindingOption{WithBindingOnMissing("allow")}, wantReason: ReasonMalformed},
		{name: "allow + empty header value still rejects", headerVals: []string{""}, claimJSON: `["acme"]`, opts: []BindingOption{WithBindingOnMissing("allow")}, wantReason: ReasonMalformed},
		{name: "allow + malformed claim still rejects", headerVals: []string{"acme"}, claimJSON: `{"a":1}`, opts: []BindingOption{WithBindingOnMissing("allow")}, wantReason: ReasonMalformed},

		// The cross-cases: a malformed operand on the OPPOSITE side of an absent one. These
		// are what break if validate() ever checks absence before malformedness — the
		// single-sided cases above would all still pass.
		{name: "allow + header absent + malformed claim rejects", headerVals: nil, claimJSON: `{"a":1}`, opts: []BindingOption{WithBindingOnMissing("allow")}, wantReason: ReasonMalformed},
		{name: "allow + claim absent + multiple headers rejects", headerVals: []string{"acme", "beta"}, claimJSON: "", opts: []BindingOption{WithBindingOnMissing("allow")}, wantReason: ReasonMalformed},
		{name: "allow + header absent + exact on array rejects", headerVals: nil, claimJSON: `["acme"]`, opts: []BindingOption{WithBindingOnMissing("allow"), WithBindingMatch("exact")}, wantReason: ReasonMalformed},

		// --- precedence: allow skips genuine absence ----------------------
		{name: "allow + header absent skips", headerVals: nil, claimJSON: `["acme"]`, opts: []BindingOption{WithBindingOnMissing("allow")}},
		{name: "allow + claim absent skips", headerVals: []string{"acme"}, claimJSON: "", opts: []BindingOption{WithBindingOnMissing("allow")}},
		{name: "allow + both absent skips", headerVals: nil, claimJSON: "", opts: []BindingOption{WithBindingOnMissing("allow")}},

		// --- precedence: both absent, reject ⇒ header_absent reported first
		{name: "reject + both absent ⇒ header_absent", headerVals: nil, claimJSON: "", wantReason: ReasonHeaderAbsent},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := mustBinding(t, claim, header, tc.opts...)
			reason, err := b.validate(reqWithHeader(t, header, tc.headerVals), claimsWithRaw(t, claim, tc.claimJSON))

			if tc.wantReason == "" {
				require.NoError(t, err)
				assert.Empty(t, string(reason))

				return
			}

			require.Error(t, err)
			assert.Equal(t, tc.wantReason, reason)
		})
	}
}

// The element cap rejects an over-1000-entry array as malformed rather than turning it into
// an oversized authorized set. It bounds the authorized-set SIZE, not the decode cost — the
// array is materialized before the length is known, exactly as upstream does for the
// mercure.publish/subscribe claims against maxClaimMatchers.
func TestClaimHeaderBindingRejectsOversizedClaimArray(t *testing.T) {
	t.Parallel()

	vals := make([]string, 0, maxClaimHeaderValues+1)
	for range maxClaimHeaderValues + 1 {
		vals = append(vals, "x")
	}

	raw, err := json.Marshal(vals)
	require.NoError(t, err)

	b := mustBinding(t, "tenants", "Tenant-ID")
	reason, err := b.validate(reqWithHeader(t, "Tenant-ID", []string{"x"}), claimsWithRaw(t, "tenants", string(raw)))

	require.Error(t, err)
	assert.Equal(t, ReasonMalformed, reason)
}

// The two protocol-reserved claims decode into an object and can therefore never be a valid
// binding target — every request would evaluate malformed. Reject them at construction
// rather than let an operator ship a hub that 401s everything. A string-valued registered
// claim like `sub` stays bindable.
func TestNewClaimHeaderBindingRejectsUnbindableClaims(t *testing.T) {
	t.Parallel()

	// Provably always-malformed, knowable at config time: Mercure's object claims and the
	// RFC 7519 numeric-date claims. Rejecting these at construction (rather than at runtime,
	// where they 401 every request) also freezes the config contract strictly before release
	// — loosening later is non-breaking, tightening later would not be.
	for _, claim := range []string{"mercure", "https://mercure.rocks/", "exp", "nbf", "iat"} {
		_, err := NewClaimHeaderBinding(claim, "X-Bound")
		require.ErrorIs(t, err, ErrInvalidClaimHeaderBinding, "binding on unbindable claim %q must be rejected", claim)
		assert.Contains(t, err.Error(), "cannot be bound")
	}

	// String and string-or-array registered claims stay bindable.
	for _, claim := range []string{"sub", "iss", "jti", "aud"} {
		_, err := NewClaimHeaderBinding(claim, "X-Bound")
		require.NoError(t, err, "claim %q must stay bindable", claim)
	}
}

// The reserved-claim constants must stay in lockstep with the json tags on the claims
// struct: if a tag is renamed, the reject would silently stop matching and an object-valued
// claim would become bindable. Read the tags via reflection so a rename fails here.
func TestReservedClaimsMatchStructTags(t *testing.T) {
	t.Parallel()

	typ := reflect.TypeFor[claims]()
	tag := func(field string) string {
		f, ok := typ.FieldByName(field)
		require.True(t, ok, "claims.%s must exist", field)

		return strings.Split(f.Tag.Get("json"), ",")[0]
	}

	assert.Equal(t, reservedMercureClaim, tag("Mercure"))
	assert.Equal(t, reservedMercureNamespacedClaim, tag("MercureNamespaced"))
}

// Header name lookup is case-insensitive (HTTP), while the VALUE compare is not.
func TestClaimHeaderBindingHeaderNameIsCaseInsensitive(t *testing.T) {
	t.Parallel()

	b := mustBinding(t, "tenants", "Tenant-ID")
	// sent with a differently-cased field name
	reason, err := b.validate(reqWithHeader(t, "tenant-id", []string{"acme"}), claimsWithRaw(t, "tenants", `["acme"]`))

	require.NoError(t, err)
	assert.Empty(t, string(reason))
}

// The three match modes must be pairwise distinguishable, or an operator who selects one
// is not choosing anything. auto accepts either shape; exact demands a scalar; member
// demands an array. This test fails if member ever silently degrades into an alias of auto.
func TestClaimHeaderBindingMatchModesAreDistinct(t *testing.T) {
	t.Parallel()

	const scalar, array = `"acme"`, `["acme"]`

	cases := []struct {
		mode              string
		scalarOK, arrayOK bool
	}{
		{"auto", true, true},
		{"exact", true, false},
		{"member", false, true},
	}

	for _, tc := range cases {
		t.Run(tc.mode, func(t *testing.T) {
			t.Parallel()

			b := mustBinding(t, "tenants", "Tenant-ID", WithBindingMatch(tc.mode))

			for shape, claimJSON := range map[string]string{"scalar": scalar, "array": array} {
				reason, err := b.validate(reqWithHeader(t, "Tenant-ID", []string{"acme"}), claimsWithRaw(t, "tenants", claimJSON))

				if (shape == "scalar" && tc.scalarOK) || (shape == "array" && tc.arrayOK) {
					require.NoError(t, err, "%s must accept a %s claim", tc.mode, shape)

					continue
				}

				require.ErrorIs(t, err, ErrClaimHeaderRejected, "%s must reject a %s claim", tc.mode, shape)
				assert.Equal(t, ReasonMalformed, reason, "a shape the mode forbids is malformed, not a mismatch")
			}
		})
	}
}

// Bindings must be enforced on the publish path too, not only on subscribe. publish.go
// calls authorizeAndBind(r, true), a code path no other test in this file exercises.
func TestAuthorizeAndBindEnforcesOnPublish(t *testing.T) {
	t.Parallel()

	binding := mustBinding(t, "tenants", "Tenant-ID")
	pubToken := func(tb testing.TB, tenants any) string {
		tb.Helper()

		return createDummyJWTWithExtraClaims(tb, rolePublisher, []string{"*"}, map[string]any{"tenants": tenants})
	}

	publishReq := func(tb testing.TB, tenant string) *http.Request {
		tb.Helper()

		r := httptest.NewRequest(http.MethodPost, defaultHubURL, nil)
		r.Header.Set("Authorization", bearerPrefix+pubToken(tb, []string{"acme"}))

		if tenant != "" {
			r.Header.Set("Tenant-ID", tenant)
		}

		return r
	}

	t.Run("matching header is authorized", func(t *testing.T) {
		t.Parallel()

		c, err := boundHub(t, binding).authorizeAndBind(publishReq(t, "acme"), true)
		require.NoError(t, err)
		assert.NotNil(t, c)
	})

	t.Run("mismatched header is rejected", func(t *testing.T) {
		t.Parallel()

		_, err := boundHub(t, binding).authorizeAndBind(publishReq(t, "evil"), true)
		require.ErrorIs(t, err, ErrClaimHeaderRejected)
	})

	t.Run("absent header is rejected", func(t *testing.T) {
		t.Parallel()

		_, err := boundHub(t, binding).authorizeAndBind(publishReq(t, ""), true)
		require.ErrorIs(t, err, ErrClaimHeaderRejected)
	})

	t.Run("a subscriber-scoped binding does not apply to publish", func(t *testing.T) {
		t.Parallel()

		subOnly := mustBinding(t, "tenants", "Tenant-ID", WithBindingRoles("subscriber"))

		c, err := boundHub(t, subOnly).authorizeAndBind(publishReq(t, "evil"), true)
		require.NoError(t, err)
		assert.NotNil(t, c)
	})
}

// The subtests above call authorizeAndBind directly, so they would all still pass if
// publish.go reverted to plain authorize(). This one drives PublishHandler and therefore
// pins the wiring itself.
func TestPublishHandlerEnforcesClaimHeaderBindings(t *testing.T) {
	t.Parallel()

	publish := func(tb testing.TB, h *Hub, tenant string) int {
		tb.Helper()

		form := url.Values{"topic": {"https://example.com/books/1"}, "data": {"hi"}}

		r := httptest.NewRequest(http.MethodPost, defaultHubURL, strings.NewReader(form.Encode()))
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		r.Header.Set("Authorization", bearerPrefix+createDummyJWTWithExtraClaims(tb, rolePublisher, []string{"*"}, map[string]any{"tenants": []string{"acme"}}))

		if tenant != "" {
			r.Header.Set("Tenant-ID", tenant)
		}

		w := httptest.NewRecorder()
		h.PublishHandler(w, r)

		return w.Code
	}

	h := createDummy(t, WithClaimHeaderBindings(mustBinding(t, "tenants", "Tenant-ID")))

	assert.Equal(t, http.StatusOK, publish(t, h, "acme"), "a matching header must publish")
	assert.Equal(t, http.StatusUnauthorized, publish(t, h, "evil"), "a mismatched header must 401")
	assert.Equal(t, http.StatusUnauthorized, publish(t, h, ""), "an absent header must 401")
}

// A token-bearing request whose header mismatches must be rejected outright, NOT silently
// downgraded to an anonymous connection. subscribe.go tests `err != nil` before it tests
// `claims == nil`, and this pins that ordering.
func TestAuthorizeAndBindRejectsMismatchEvenWhenAnonymousAllowed(t *testing.T) {
	t.Parallel()

	h, err := NewHub(
		t.Context(),
		WithClaimHeaderBindings(mustBinding(t, "tenants", "Tenant-ID")),
		WithSubscriberJWT([]byte("subscriber"), "HS256"),
		WithPublisherJWT([]byte("publisher"), "HS256"),
		WithAnonymous(),
	)
	require.NoError(t, err)

	r := httptest.NewRequest(http.MethodGet, defaultHubURL, nil)
	r.Header.Set("Authorization", bearerPrefix+createDummyJWTWithExtraClaims(t, roleSubscriber, []string{"*"}, map[string]any{"tenants": []string{"acme"}}))
	r.Header.Set("Tenant-ID", "evil")

	c, err := h.authorizeAndBind(r, false)

	require.ErrorIs(t, err, ErrClaimHeaderRejected, "a mismatched token must 401, not fall back to anonymous")
	assert.Nil(t, c)
}

// Capturing every top-level claim costs a second decode per authenticated request, and the
// map is then pinned to each subscriber for the connection's lifetime. Deployments with no
// binding configured must not pay either cost, and a hub that does bind must release the
// map once the bindings have consumed it.
func TestRawClaimsAreCapturedOnlyWhenBoundAndReleasedAfterUse(t *testing.T) {
	t.Parallel()

	token := createDummyJWTWithExtraClaims(t, roleSubscriber, []string{"*"}, map[string]any{"tenants": []string{"acme"}})

	req := func() *http.Request {
		r := httptest.NewRequest(http.MethodGet, defaultHubURL, nil)
		r.Header.Set("Authorization", bearerPrefix+token)
		r.Header.Set("Tenant-ID", "acme")

		return r
	}

	// Observed through authorize(), NOT authorizeAndBind(): the wrapper releases the map on
	// every path, so asserting on its output cannot tell "never captured" from "captured
	// then cleared".
	t.Run("no bindings configured captures nothing", func(t *testing.T) {
		t.Parallel()

		c, err := createDummy(t).authorize(req(), false)
		require.NoError(t, err)
		require.NotNil(t, c)
		assert.Nil(t, c.rawClaims, "a hub with no bindings must not pay the raw-claim capture")
		assert.NotNil(t, c.Mercure.Subscribe, "the typed claims must still decode")
	})

	t.Run("a binding configured captures the map", func(t *testing.T) {
		t.Parallel()

		c, err := boundHub(t, mustBinding(t, "tenants", "Tenant-ID")).authorize(req(), false)
		require.NoError(t, err)
		require.NotNil(t, c)
		assert.NotEmpty(t, c.rawClaims, "a hub with a binding must capture the raw claims")
		assert.Contains(t, c.rawClaims, "tenants")
	})

	// The capture is gated on the bindings that apply to *this* request, so a
	// publisher-scoped binding must not tax every subscribe request.
	t.Run("an inapplicable binding captures nothing", func(t *testing.T) {
		t.Parallel()

		pubOnly := mustBinding(t, "tenants", "Tenant-ID", WithBindingRoles("publisher"))

		c, err := boundHub(t, pubOnly).authorize(req(), false)
		require.NoError(t, err)
		require.NotNil(t, c)
		assert.Nil(t, c.rawClaims, "a publisher-scoped binding must not capture on a subscribe request")
	})

	// ...but the request must still be authorized, not rejected by the nil-rawClaims guard.
	t.Run("an inapplicable binding still authorizes the request", func(t *testing.T) {
		t.Parallel()

		pubOnly := mustBinding(t, "tenants", "Tenant-ID", WithBindingRoles("publisher"))

		c, err := boundHub(t, pubOnly).authorizeAndBind(req(), false)
		require.NoError(t, err, "a skipped binding must not reject via the nil-rawClaims guard")
		assert.NotNil(t, c)
	})

	t.Run("bindings configured release the map after enforcement", func(t *testing.T) {
		t.Parallel()

		c, err := boundHub(t, mustBinding(t, "tenants", "Tenant-ID")).authorizeAndBind(req(), false)
		require.NoError(t, err)
		require.NotNil(t, c)
		assert.Nil(t, c.rawClaims, "rawClaims must be released once the bindings have run")
	})
}

// The load-bearing invariant behind the capture gate: rawClaims must be populated whenever
// SOME binding will call evalClaim on this request, because evalClaim reads a nil map on a
// live *claims as malformed. If authorize's gate and authorizeAndBind's loop ever disagree
// about which bindings apply, a legitimate request gets a spurious 401. This walks every
// combination of binding roles against both request kinds.
func TestCaptureGateAgreesWithEnforcementPredicate(t *testing.T) {
	t.Parallel()

	roles := []string{"all", "subscriber", "publisher"}

	for _, first := range roles {
		for _, second := range roles {
			for _, publish := range []bool{false, true} {
				name := first + "+" + second + "/publish=" + strconv.FormatBool(publish)

				t.Run(name, func(t *testing.T) {
					t.Parallel()

					bindings := []ClaimHeaderBinding{
						mustBinding(t, "tenants", "Tenant-ID", WithBindingRoles(first)),
						mustBinding(t, "regions", "X-Region", WithBindingRoles(second)),
					}
					h := boundHub(t, bindings...)

					anyApplies := slices.ContainsFunc(bindings, func(b ClaimHeaderBinding) bool {
						return b.appliesTo(publish)
					})

					role := roleSubscriber
					if publish {
						role = rolePublisher
					}

					newReq := func() *http.Request {
						r := httptest.NewRequest(http.MethodGet, defaultHubURL, nil)
						r.Header.Set("Authorization", bearerPrefix+createDummyJWTWithExtraClaims(t, role, []string{"*"},
							map[string]any{"tenants": []string{"acme"}, "regions": []string{"eu"}}))
						r.Header.Set("Tenant-ID", "acme")
						r.Header.Set("X-Region", "eu")

						return r
					}

					c, err := h.authorize(newReq(), publish)
					require.NoError(t, err)
					require.NotNil(t, c)
					assert.Equal(t, anyApplies, c.rawClaims != nil,
						"the capture gate must fire exactly when a binding will read the claims")

					// And the request must be authorized either way — never a spurious 401.
					_, err = h.authorizeAndBind(newReq(), publish)
					require.NoError(t, err, "a legitimate request must never be rejected by the nil-rawClaims guard")
				})
			}
		}
	}
}

// A claims object whose rawClaims map is absent cannot be evaluated. It must be malformed
// (never skippable) rather than absent: authorizeAndBind clears rawClaims once the bindings
// have run, so classifying it as absent would let on_missing=allow skip a binding on any
// future second evaluation of the same claims.
func TestClaimHeaderBindingStrippedClaimsIsMalformed(t *testing.T) {
	t.Parallel()

	b := mustBinding(t, "tenants", "Tenant-ID", WithBindingOnMissing("allow"))
	r := reqWithHeader(t, "Tenant-ID", []string{"acme"})

	reason, err := b.validate(r, &claims{}) // non-nil claims, nil rawClaims

	require.ErrorIs(t, err, ErrClaimHeaderRejected, "on_missing=allow must not skip a stripped claims object")
	assert.Equal(t, ReasonMalformed, reason)
}

// The cap is `> maxClaimHeaderValues`, so exactly maxClaimHeaderValues must be ACCEPTED.
// Testing only the rejecting side would let a `>` → `>=` slip pass CI while breaking every
// legitimate max-length claim.
func TestClaimHeaderBindingCapBoundaryAcceptsExactlyMax(t *testing.T) {
	t.Parallel()

	vals := make([]string, maxClaimHeaderValues)
	for i := range vals {
		vals[i] = "filler"
	}

	vals[0] = "acme"

	raw, err := json.Marshal(vals)
	require.NoError(t, err)

	b := mustBinding(t, "tenants", "Tenant-ID")
	reason, err := b.validate(reqWithHeader(t, "Tenant-ID", []string{"acme"}), claimsWithRaw(t, "tenants", string(raw)))

	require.NoError(t, err, "exactly maxClaimHeaderValues entries must be accepted")
	assert.Empty(t, string(reason))
}

// The claim, unlike the header, is looked up by exact key: JWT claim names are
// case-sensitive and must not be matched loosely. A near-miss name reads as absent.
func TestClaimHeaderBindingClaimNameIsExact(t *testing.T) {
	t.Parallel()

	b := mustBinding(t, "tenants", "Tenant-ID")

	for _, name := range []string{"Tenants", "tenant", "tenants "} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			reason, err := b.validate(reqWithHeader(t, "Tenant-ID", []string{"acme"}), claimsWithRaw(t, name, `["acme"]`))

			require.ErrorIs(t, err, ErrClaimHeaderRejected)
			assert.Equal(t, ReasonClaimAbsent, reason)
		})
	}
}

// Rejection errors must never leak the header or claim VALUES — they land in Debug logs and spans.
func TestClaimHeaderBindingErrorDoesNotLeakValues(t *testing.T) {
	t.Parallel()

	b := mustBinding(t, "tenants", "Tenant-ID")
	_, err := b.validate(reqWithHeader(t, "Tenant-ID", []string{"supersecret-header"}), claimsWithRaw(t, "tenants", `["topsecret-claim"]`))

	require.Error(t, err)
	assert.NotContains(t, err.Error(), "supersecret-header")
	assert.NotContains(t, err.Error(), "topsecret-claim")
	// but it must identify the binding (canonical header form) and the reason
	assert.Contains(t, err.Error(), "tenants")
	assert.Contains(t, err.Error(), "Tenant-Id")
	assert.Contains(t, err.Error(), string(ReasonMismatch))
}

func TestClaimHeaderBindingAppliesTo(t *testing.T) {
	t.Parallel()

	all := mustBinding(t, "c", "H")
	sub := mustBinding(t, "c", "H", WithBindingRoles("subscriber"))
	pub := mustBinding(t, "c", "H", WithBindingRoles("publisher"))

	assert.True(t, all.appliesTo(true))
	assert.True(t, all.appliesTo(false))
	assert.False(t, sub.appliesTo(true))
	assert.True(t, sub.appliesTo(false))
	assert.True(t, pub.appliesTo(true))
	assert.False(t, pub.appliesTo(false))
}

func TestNewClaimHeaderBindingValidation(t *testing.T) {
	t.Parallel()

	longName := strings.Repeat("a", maxBindingNameLen+1)

	testCases := []struct {
		name    string
		claim   string
		header  string
		opts    []BindingOption
		wantErr bool
	}{
		{name: "valid", claim: "tenants", header: "Tenant-ID"},
		{name: "empty claim", claim: "", header: "Tenant-ID", wantErr: true},
		{name: "empty header", claim: "tenants", header: "", wantErr: true},
		{name: "header with space", claim: "tenants", header: "Tenant ID", wantErr: true},
		{name: "header with colon", claim: "tenants", header: "Tenant:ID", wantErr: true},
		{name: "control char in header", claim: "tenants", header: "Tenant\x00ID", wantErr: true},
		{name: "control char in claim", claim: "ten\x00ants", header: "Tenant-ID", wantErr: true},
		{name: "over-long claim", claim: longName, header: "Tenant-ID", wantErr: true},
		{name: "over-long header", claim: "tenants", header: longName, wantErr: true},
		{name: "bad match enum", claim: "tenants", header: "Tenant-ID", opts: []BindingOption{WithBindingMatch("exat")}, wantErr: true},
		{name: "bad on_missing enum", claim: "tenants", header: "Tenant-ID", opts: []BindingOption{WithBindingOnMissing("mabye")}, wantErr: true},
		{name: "bad roles enum", claim: "tenants", header: "Tenant-ID", opts: []BindingOption{WithBindingRoles("subscribr")}, wantErr: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := NewClaimHeaderBinding(tc.claim, tc.header, tc.opts...)
			if tc.wantErr {
				assert.Error(t, err)

				return
			}

			assert.NoError(t, err)
		})
	}
}

// createDummyJWTWithExtraClaims mints a token carrying arbitrary TOP-LEVEL claims
// alongside the mercure claim. The typed `claims` struct has no field for them, so this
// signs a MapClaims — the existing createDummyAuthorizedJWTWithPayload cannot.
func createDummyJWTWithExtraClaims(tb testing.TB, r role, topics []string, extra map[string]any) string {
	tb.Helper()

	mercure := map[string]any{}

	var key []byte

	switch r {
	case rolePublisher:
		mercure["publish"] = topics
		key = []byte("publisher")
	case roleSubscriber:
		mercure["subscribe"] = topics
		key = []byte("subscriber")
	}

	mapClaims := jwt.MapClaims{"mercure": mercure}
	maps.Copy(mapClaims, extra)

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, mapClaims)

	signed, err := token.SignedString(key)
	require.NoError(tb, err)

	return signed
}

func boundHub(tb testing.TB, bindings ...ClaimHeaderBinding) *Hub {
	tb.Helper()

	h, err := NewHub(
		tb.Context(),
		WithClaimHeaderBindings(bindings...),
		WithSubscriberJWT([]byte("subscriber"), "HS256"),
		WithPublisherJWT([]byte("publisher"), "HS256"),
	)
	require.NoError(tb, err)

	return h
}

// authorizeAndBind is the single enforcement site: it wraps authorize() (which has 6
// return points) and runs every applicable binding.
func TestAuthorizeAndBind(t *testing.T) {
	t.Parallel()

	binding := mustBinding(t, "tenants", "Tenant-ID")
	subToken := func(tb testing.TB, tenants any) string {
		tb.Helper()

		return createDummyJWTWithExtraClaims(tb, roleSubscriber, []string{"*"}, map[string]any{"tenants": tenants})
	}

	t.Run("matching header is authorized", func(t *testing.T) {
		t.Parallel()

		r := httptest.NewRequest(http.MethodGet, "/", nil)
		r.Header.Set("Authorization", bearerPrefix+subToken(t, []string{"acme"}))
		r.Header.Set("Tenant-ID", "acme")

		c, err := boundHub(t, binding).authorizeAndBind(r, false)
		require.NoError(t, err)
		assert.NotNil(t, c)
	})

	t.Run("mismatched header is rejected", func(t *testing.T) {
		t.Parallel()

		r := httptest.NewRequest(http.MethodGet, "/", nil)
		r.Header.Set("Authorization", bearerPrefix+subToken(t, []string{"acme"}))
		r.Header.Set("Tenant-ID", "beta")

		c, err := boundHub(t, binding).authorizeAndBind(r, false)
		require.ErrorIs(t, err, ErrClaimHeaderRejected)
		assert.Nil(t, c)
	})

	t.Run("absent header is rejected fail-closed", func(t *testing.T) {
		t.Parallel()

		r := httptest.NewRequest(http.MethodGet, "/", nil)
		r.Header.Set("Authorization", bearerPrefix+subToken(t, []string{"acme"}))

		_, err := boundHub(t, binding).authorizeAndBind(r, false)
		require.ErrorIs(t, err, ErrClaimHeaderRejected)
	})

	// Anonymous requests carry no token, so there is no claim to bind: skipped, not 401.
	t.Run("anonymous request skips bindings", func(t *testing.T) {
		t.Parallel()

		h, err := NewHub(
			t.Context(),
			WithClaimHeaderBindings(binding),
			WithSubscriberJWT([]byte("subscriber"), "HS256"),
			WithPublisherJWT([]byte("publisher"), "HS256"),
			WithAnonymous(),
		)
		require.NoError(t, err)

		r := httptest.NewRequest(http.MethodGet, "/", nil) // no token, no header
		c, err := h.authorizeAndBind(r, false)

		require.NoError(t, err)
		assert.Nil(t, c)
	})

	// A publisher-scoped binding must not gate subscribe requests.
	t.Run("role scoping", func(t *testing.T) {
		t.Parallel()

		pubOnly := mustBinding(t, "tenants", "Tenant-ID", WithBindingRoles("publisher"))

		r := httptest.NewRequest(http.MethodGet, "/", nil)
		r.Header.Set("Authorization", bearerPrefix+subToken(t, []string{"acme"}))
		// no Tenant-ID header at all — would be header_absent if the binding applied

		_, err := boundHub(t, pubOnly).authorizeAndBind(r, false)
		require.NoError(t, err)
	})

	t.Run("two bindings are AND-combined", func(t *testing.T) {
		t.Parallel()

		region := mustBinding(t, "regions", "X-Region")
		token := createDummyJWTWithExtraClaims(t, roleSubscriber, []string{"*"}, map[string]any{
			"tenants": []string{"acme"},
			"regions": []string{"us-east"},
		})

		r := httptest.NewRequest(http.MethodGet, "/", nil)
		r.Header.Set("Authorization", bearerPrefix+token)
		r.Header.Set("Tenant-ID", "acme")
		r.Header.Set("X-Region", "eu-west") // second binding fails

		_, err := boundHub(t, binding, region).authorizeAndBind(r, false)
		require.ErrorIs(t, err, ErrClaimHeaderRejected)
	})

	// authorize() accepts the token via header, query or cookie; all converge here.
	t.Run("enforced across all auth carriers", func(t *testing.T) {
		t.Parallel()

		token := subToken(t, []string{"acme"})

		t.Run("query", func(t *testing.T) {
			t.Parallel()

			r := httptest.NewRequest(http.MethodGet, "/?authorization="+token, nil)
			r.Header.Set("Tenant-ID", "beta")

			_, err := boundHub(t, binding).authorizeAndBind(r, false)
			require.ErrorIs(t, err, ErrClaimHeaderRejected)
		})

		t.Run("cookie", func(t *testing.T) {
			t.Parallel()

			r := httptest.NewRequest(http.MethodGet, "/", nil)
			r.AddCookie(&http.Cookie{Name: defaultCookieName, Value: token})
			r.Header.Set("Tenant-ID", "beta")

			_, err := boundHub(t, binding).authorizeAndBind(r, false)
			require.ErrorIs(t, err, ErrClaimHeaderRejected)
		})

		// The cookie-POST carrier reaches validateJWT through the CSRF Origin check, a
		// distinct return point from the cookie-GET path above.
		t.Run("cookie POST with allowed origin", func(t *testing.T) {
			t.Parallel()

			h, err := NewHub(
				t.Context(),
				WithClaimHeaderBindings(binding),
				WithSubscriberJWT([]byte("subscriber"), "HS256"),
				WithPublisherJWT([]byte("publisher"), "HS256"),
				WithPublishOrigins([]string{"https://example.com"}),
			)
			require.NoError(t, err)

			r := httptest.NewRequest(http.MethodPost, "/", nil)
			r.AddCookie(&http.Cookie{Name: defaultCookieName, Value: token})
			r.Header.Set("Origin", "https://example.com")
			r.Header.Set("Tenant-ID", "beta")

			_, err = h.authorizeAndBind(r, false)
			require.ErrorIs(t, err, ErrClaimHeaderRejected)
		})
	})

	// A binding that passes must not touch the rejection counter. Without this, a stray
	// increment on the success path would go unnoticed.
	t.Run("an authorized request does not meter a rejection", func(t *testing.T) {
		t.Parallel()

		registry := prometheus.NewPedanticRegistry()
		m := NewPrometheusMetrics(registry)

		h, err := NewHub(
			t.Context(),
			WithClaimHeaderBindings(binding),
			WithSubscriberJWT([]byte("subscriber"), "HS256"),
			WithPublisherJWT([]byte("publisher"), "HS256"),
			WithMetrics(m),
		)
		require.NoError(t, err)

		r := httptest.NewRequest(http.MethodGet, defaultHubURL, nil)
		r.Header.Set("Authorization", bearerPrefix+subToken(t, []string{"acme"}))
		r.Header.Set("Tenant-ID", "acme")

		_, err = h.authorizeAndBind(r, false)
		require.NoError(t, err)

		assertCounterVecValue(t, 0, m.claimHeaderRejectedTotal, "tenants:Tenant-Id", string(ReasonMismatch))
	})
}

// Set-level checks live at the Hub, which is the only place that sees every binding —
// and they must hold for programmatic (Go) callers, not just the Caddy adapter.
func TestNewHubClaimHeaderBindingGuards(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	subJWT := func() Option { return WithSubscriberJWT([]byte("subscriber"), "HS256") }
	pubJWT := func() Option { return WithPublisherJWT([]byte("publisher"), "HS256") }

	all := mustBinding(t, "tenants", "Tenant-ID")
	subOnly := mustBinding(t, "tenants", "Tenant-ID", WithBindingRoles("subscriber"))
	pubOnly := mustBinding(t, "tenants", "Tenant-ID", WithBindingRoles("publisher"))

	t.Run("subscriber-covering binding without subscriber JWT fails", func(t *testing.T) {
		t.Parallel()

		_, err := NewHub(ctx, WithClaimHeaderBindings(all), pubJWT())
		require.ErrorIs(t, err, ErrInvalidClaimHeaderBinding)

		_, err = NewHub(ctx, WithClaimHeaderBindings(subOnly), pubJWT())
		require.ErrorIs(t, err, ErrInvalidClaimHeaderBinding)
	})

	t.Run("publisher-scoped binding without publisher JWT fails", func(t *testing.T) {
		t.Parallel()

		_, err := NewHub(ctx, WithClaimHeaderBindings(pubOnly), subJWT())
		require.ErrorIs(t, err, ErrInvalidClaimHeaderBinding)
	})

	t.Run("both JWTs present succeeds", func(t *testing.T) {
		t.Parallel()

		_, err := NewHub(ctx, WithClaimHeaderBindings(all), subJWT(), pubJWT())
		require.NoError(t, err)
	})

	// The two startup conditions that weaken a binding without invalidating it must be
	// announced, or an operator only learns of them during an incident. Deleting either
	// Warn call has to fail a test.
	t.Run("anonymous access skips subscriber bindings, with a warning", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer

		_, err := NewHub(ctx, WithClaimHeaderBindings(subOnly), subJWT(), WithAnonymous(),
			WithLogger(slog.New(slog.NewJSONHandler(&buf, nil))))
		require.NoError(t, err)

		assert.Contains(t, buf.String(), "anonymous requests carry no token")
	})

	t.Run("metrics that cannot report rejections are warned about", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer

		// NopMetrics does not implement AuthorizationRejectionReporter.
		_, err := NewHub(ctx, WithClaimHeaderBindings(all), subJWT(), pubJWT(),
			WithMetrics(NopMetrics{}), WithLogger(slog.New(slog.NewJSONHandler(&buf, nil))))
		require.NoError(t, err)

		assert.Contains(t, buf.String(), "does not implement AuthorizationRejectionReporter")
	})

	t.Run("a reporting metrics implementation is not warned about", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer

		_, err := NewHub(ctx, WithClaimHeaderBindings(all), subJWT(), pubJWT(),
			WithMetrics(NewPrometheusMetrics(prometheus.NewPedanticRegistry())),
			WithLogger(slog.New(slog.NewJSONHandler(&buf, nil))))
		require.NoError(t, err)

		assert.NotContains(t, buf.String(), "AuthorizationRejectionReporter")
	})

	// A ClaimHeaderBinding{} is constructible cross-package despite unexported fields.
	t.Run("zero-value binding is rejected", func(t *testing.T) {
		t.Parallel()

		_, err := NewHub(ctx, WithClaimHeaderBindings(ClaimHeaderBinding{}), subJWT(), pubJWT())
		require.ErrorIs(t, err, ErrInvalidClaimHeaderBinding)
	})

	// Same claim+canonical header ⇒ ambiguous metric label; rejected regardless of options.
	t.Run("overlapping bindings are rejected", func(t *testing.T) {
		t.Parallel()

		other := mustBinding(t, "tenants", "tenant-id", WithBindingMatch("member"), WithBindingRoles("publisher"))

		_, err := NewHub(ctx, WithClaimHeaderBindings(all, other), subJWT(), pubJWT())
		require.ErrorIs(t, err, ErrInvalidClaimHeaderBinding)
	})

	t.Run("distinct bindings are accepted", func(t *testing.T) {
		t.Parallel()

		other := mustBinding(t, "regions", "X-Region")

		h, err := NewHub(ctx, WithClaimHeaderBindings(all, other), subJWT(), pubJWT())
		require.NoError(t, err)
		assert.Len(t, h.claimHeaderBindings, 2)
	})

	// The option must copy: a caller mutating its slice afterwards must not affect the Hub.
	t.Run("option defensively copies the slice", func(t *testing.T) {
		t.Parallel()

		bindings := []ClaimHeaderBinding{all}

		h, err := NewHub(ctx, WithClaimHeaderBindings(bindings...), subJWT(), pubJWT())
		require.NoError(t, err)

		bindings[0] = ClaimHeaderBinding{}

		require.Len(t, h.claimHeaderBindings, 1)
		assert.Equal(t, "tenants:Tenant-Id", h.claimHeaderBindings[0].id())
	})
}

// The header name is canonicalized at construction so lookup and the metric label are
// stable regardless of the casing an operator configured. Go's canonical form upper-cases
// only the first letter of each dash-separated token ("tenant-id" and "Tenant-ID" both
// become "Tenant-Id", exactly as "Last-Event-ID" becomes "Last-Event-Id").
func TestNewClaimHeaderBindingCanonicalizesHeader(t *testing.T) {
	t.Parallel()

	for _, configured := range []string{"tenant-id", "Tenant-ID", "TENANT-ID"} {
		b := mustBinding(t, "tenants", configured)
		assert.Equal(t, "Tenant-Id", b.header)
		assert.Equal(t, "tenants:Tenant-Id", b.id())
	}
}

// A rejection that flows through the real wrapper must be metered with the typed reason,
// not just Debug-logged.
func TestAuthorizeAndBindReportsRejectionMetric(t *testing.T) {
	t.Parallel()

	m := NewPrometheusMetrics(prometheus.NewRegistry())

	h, err := NewHub(
		t.Context(),
		WithClaimHeaderBindings(mustBinding(t, "tenants", "Tenant-ID")),
		WithMetrics(m),
		WithSubscriberJWT([]byte("subscriber"), "HS256"),
		WithPublisherJWT([]byte("publisher"), "HS256"),
	)
	require.NoError(t, err)

	token := createDummyJWTWithExtraClaims(t, roleSubscriber, []string{"*"}, map[string]any{"tenants": []string{"acme"}})

	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.Header.Set("Authorization", bearerPrefix+token)
	r.Header.Set("Tenant-ID", "beta")

	_, err = h.authorizeAndBind(r, false)
	require.ErrorIs(t, err, ErrClaimHeaderRejected)

	assertCounterVecValue(t, 1.0, m.claimHeaderRejectedTotal, "tenants:Tenant-Id", string(ReasonMismatch))
}

// A browser fetch carrying a bound header triggers a CORS preflight, and the hub's allowed
// request headers are otherwise a fixed list — so configured binding headers must be added,
// case-insensitively de-duplicated (the preflight header string is asserted verbatim).
func TestClaimHeaderBindingCORSAllowedHeaders(t *testing.T) {
	t.Parallel()

	corsHub := func(tb testing.TB, withCORS bool, bindings ...ClaimHeaderBinding) *Hub {
		tb.Helper()

		options := []Option{
			WithClaimHeaderBindings(bindings...),
			WithSubscriberJWT([]byte("subscriber"), "HS256"),
			WithPublisherJWT([]byte("publisher"), "HS256"),
		}
		if withCORS {
			options = append(options, WithCORSOrigins([]string{"https://example.com"}))
		}

		h, err := NewHub(tb.Context(), options...)
		require.NoError(tb, err)

		return h
	}

	preflight := func(tb testing.TB, h *Hub, requested string) *http.Response {
		tb.Helper()

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodOptions, defaultHubURL, nil)
		req.Header.Add("Origin", "https://example.com")
		req.Header.Add("Access-Control-Request-Headers", requested)
		req.Header.Add("Access-Control-Request-Method", http.MethodGet)
		h.ServeHTTP(w, req)

		return w.Result()
	}

	t.Run("bound header is allowed in preflight", func(t *testing.T) {
		t.Parallel()

		h := corsHub(t, true, mustBinding(t, "tenants", "Tenant-ID"))

		resp := preflight(t, h, "authorization,tenant-id")
		defer resp.Body.Close()

		assert.Contains(t, resp.Header.Get("Access-Control-Allow-Headers"), "tenant-id")
	})

	t.Run("without a binding the bound header stays disallowed", func(t *testing.T) {
		t.Parallel()

		h := corsHub(t, true)

		resp := preflight(t, h, "tenant-id")
		defer resp.Body.Close()

		assert.NotContains(t, resp.Header.Get("Access-Control-Allow-Headers"), "tenant-id")
	})

	t.Run("no bindings leaves the base list untouched", func(t *testing.T) {
		t.Parallel()

		h := corsHub(t, true)

		resp := preflight(t, h, "authorization,cache-control,last-event-id")
		defer resp.Body.Close()

		assert.Equal(t, "authorization,cache-control,last-event-id", resp.Header.Get("Access-Control-Allow-Headers"))
	})

	// No cors_origins ⇒ no CORS middleware at all; a binding must not install one.
	t.Run("no CORS middleware when origins unset", func(t *testing.T) {
		t.Parallel()

		h := corsHub(t, false, mustBinding(t, "tenants", "Tenant-ID"))

		resp := preflight(t, h, "tenant-id")
		defer resp.Body.Close()

		assert.Empty(t, resp.Header.Get("Access-Control-Allow-Origin"))
	})

	t.Run("a binding on an already-allowed header is not duplicated", func(t *testing.T) {
		t.Parallel()

		h := corsHub(t, true, mustBinding(t, "tok", "Authorization"))

		assert.Equal(t, baseCORSAllowedHeaders(), h.corsAllowedHeaders())
	})
}

// corsAllowedHeaders appends to the base list, so each call must hand back a fresh slice:
// were it a shared package-level var, the first hub configuring a binding would extend the
// base for every hub built afterwards.
func TestBaseCORSAllowedHeadersIsNotShared(t *testing.T) {
	t.Parallel()

	first := baseCORSAllowedHeaders()
	first[0] = "mutated"

	assert.Equal(t, "authorization", baseCORSAllowedHeaders()[0])
}
