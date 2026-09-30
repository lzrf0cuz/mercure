package mercure

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/json/jsontext"
	"errors"
	"log/slog"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// claimsWithRaw builds a *claims carrying only the raw top-level claims a binding reads.
// rawJSON of "" means the claim key is absent entirely.
func claimsWithRaw(tb testing.TB, name, rawJSON string) *claims {
	tb.Helper()

	c := &claims{rawClaims: map[string]jsontext.Value{}}
	if rawJSON != "" {
		c.rawClaims[name] = jsontext.Value(rawJSON)
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

// validateOne enforces the single binding b through enforceBindings, the production
// composition of its tiers. matched is the value the binding authorized, read back as the
// counted value (the binding is made to count subscribers), and reason is that of the rejection.
func validateOne(tb testing.TB, b ClaimHeaderBinding, r *http.Request, c *claims) (matched string, reason AuthzRejectReason, err error) {
	tb.Helper()

	b.countSubscribers = true
	h := &Hub{opt: &opt{claimHeaderBindings: []ClaimHeaderBinding{b}, metrics: NopMetrics{}}}

	counted, err := h.enforceBindings(r, c, !b.appliesTo(false))
	if be, ok := errors.AsType[*claimBindingError](err); ok {
		reason = be.reason
	}

	if err == nil && counted.value != "" {
		require.Equal(tb, b.id(), counted.binding)
	}

	return counted.value, reason, err
}

func mustBinding(tb testing.TB, claim, header string, opts ...BindingOption) ClaimHeaderBinding {
	tb.Helper()

	b, err := NewClaimHeaderBinding(claim, header, opts...)
	require.NoError(tb, err)

	return b
}

func mustValueBinding(tb testing.TB, claim string, values []string, opts ...BindingOption) ClaimHeaderBinding {
	tb.Helper()

	b, err := NewClaimValueBinding(claim, values, opts...)
	require.NoError(tb, err)

	return b
}

// createDummyJWTWithExtraClaims mints an RFC 9068 access token for the test issuer that
// grants the role's action on topics and carries arbitrary extra top-level claims. The
// typed claims struct has no field for them, so this signs a MapClaims, which
// mintAccessToken cannot.
func createDummyJWTWithExtraClaims(tb testing.TB, r role, topics []string, extra map[string]any) string {
	tb.Helper()

	action, key := actionSubscribe, []byte("subscriber")
	if r == rolePublisher {
		action, key = actionPublish, []byte("publisher")
	}

	mapClaims := jwt.MapClaims{
		"iss": testIssuer,
		"aud": testResourceIdentifier,
		"iat": time.Now().Unix(),
		"exp": time.Now().Add(time.Hour).Unix(),
		"authorization_details": []authorizationDetail{{
			Type:    authorizationDetailTypeMercure,
			Actions: []mercureAction{action},
			Topics:  stringsToDetailTopics(topics),
		}},
	}
	maps.Copy(mapClaims, extra)

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, mapClaims)
	token.Header["typ"] = atJWTType

	signed, err := token.SignedString(key)
	require.NoError(tb, err)

	return signed
}

func boundHub(tb testing.TB, bindings ...ClaimHeaderBinding) *Hub {
	tb.Helper()

	return createDummy(tb, WithClaimHeaderBindings(bindings...))
}

// issuerWith configures the test issuer with only the given roles.
func issuerWith(publisher, subscriber bool) Option {
	iss := Issuer{Identifier: testIssuer}
	if publisher {
		iss.Publisher = Static{Key: []byte("publisher"), Algorithm: "HS256"}
	}

	if subscriber {
		iss.Subscriber = Static{Key: []byte("subscriber"), Algorithm: "HS256"}
	}

	return WithIssuers([]Issuer{iss})
}

func TestClaimHeaderBindingValidate(t *testing.T) {
	t.Parallel()

	const claim, header = "groups", "Group-ID"

	testCases := []struct {
		name       string
		headerVals []string // nil ⇒ absent
		claimJSON  string   // "" ⇒ claim key absent
		opts       []BindingOption
		wantReason AuthzRejectReason // "" ⇒ allowed/skipped (no error)
		skipped    bool              // on_missing=allow skipped the binding, so nothing matched
	}{
		// Accepted.
		{name: "auto/array member", headerVals: []string{"red"}, claimJSON: `["red","blue"]`},
		{name: "auto/scalar exact", headerVals: []string{"red"}, claimJSON: `"red"`},
		{name: "member/array", headerVals: []string{"blue"}, claimJSON: `["red","blue"]`, opts: []BindingOption{WithBindingMatch("member")}},
		{name: "exact/scalar", headerVals: []string{"red"}, claimJSON: `"red"`, opts: []BindingOption{WithBindingMatch("exact")}},

		// Mismatch.
		{name: "auto/array not member", headerVals: []string{"zzz"}, claimJSON: `["red"]`, wantReason: ReasonMismatch},
		{name: "auto/scalar differs", headerVals: []string{"zzz"}, claimJSON: `"red"`, wantReason: ReasonMismatch},
		{name: "value comparison is case-sensitive", headerVals: []string{"RED"}, claimJSON: `["red"]`, wantReason: ReasonMismatch},

		// header_absent.
		{name: "header absent", headerVals: nil, claimJSON: `["red"]`, wantReason: ReasonHeaderAbsent},

		// claim_absent.
		{name: "claim key missing", headerVals: []string{"red"}, claimJSON: "", wantReason: ReasonClaimAbsent},
		{name: "claim null", headerVals: []string{"red"}, claimJSON: `null`, wantReason: ReasonClaimAbsent},
		{name: "claim empty scalar", headerVals: []string{"red"}, claimJSON: `""`, wantReason: ReasonClaimAbsent},
		{name: "claim empty array", headerVals: []string{"red"}, claimJSON: `[]`, wantReason: ReasonClaimAbsent},
		{name: "member + empty array", headerVals: []string{"red"}, claimJSON: `[]`, opts: []BindingOption{WithBindingMatch("member")}, wantReason: ReasonClaimAbsent},

		// Malformed header.
		{name: "header empty value", headerVals: []string{""}, claimJSON: `["red"]`, wantReason: ReasonMalformed},
		{name: "header multiple values", headerVals: []string{"red", "blue"}, claimJSON: `["red"]`, wantReason: ReasonMalformed},

		// Malformed claim.
		{name: "claim object", headerVals: []string{"red"}, claimJSON: `{"a":1}`, wantReason: ReasonMalformed},
		{name: "claim number", headerVals: []string{"red"}, claimJSON: `42`, wantReason: ReasonMalformed},
		{name: "claim non-string element", headerVals: []string{"red"}, claimJSON: `["red",1]`, wantReason: ReasonMalformed},
		{name: "claim null element", headerVals: []string{"red"}, claimJSON: `["red",null]`, wantReason: ReasonMalformed},
		{name: "claim bool element", headerVals: []string{"red"}, claimJSON: `["red",true]`, wantReason: ReasonMalformed},
		{name: "claim nested-array element", headerVals: []string{"red"}, claimJSON: `["red",["x"]]`, wantReason: ReasonMalformed},
		{name: "claim object element", headerVals: []string{"red"}, claimJSON: `["red",{}]`, wantReason: ReasonMalformed},
		{name: "claim empty-string element alone", headerVals: []string{"red"}, claimJSON: `[""]`, wantReason: ReasonMalformed},
		{name: "claim empty-string element mixed", headerVals: []string{"red"}, claimJSON: `["red",""]`, wantReason: ReasonMalformed},
		{name: "claim bool", headerVals: []string{"red"}, claimJSON: `true`, wantReason: ReasonMalformed},

		// Malformed: the claim's shape disagrees with the match mode.
		// exact demands a scalar, member demands an array. A claim of the other shape is
		// a config/token disagreement, not a value the operator meant to authorize.
		{name: "exact on array claim", headerVals: []string{"red"}, claimJSON: `["red"]`, opts: []BindingOption{WithBindingMatch("exact")}, wantReason: ReasonMalformed},
		{name: "exact on empty array claim", headerVals: []string{"red"}, claimJSON: `[]`, opts: []BindingOption{WithBindingMatch("exact")}, wantReason: ReasonMalformed},
		{name: "member on scalar claim", headerVals: []string{"red"}, claimJSON: `"red"`, opts: []BindingOption{WithBindingMatch("member")}, wantReason: ReasonMalformed},
		{name: "member on empty scalar claim", headerVals: []string{"red"}, claimJSON: `""`, opts: []BindingOption{WithBindingMatch("member")}, wantReason: ReasonMalformed},

		// Malformed beats on_missing=allow.
		{name: "allow + multiple headers still rejects", headerVals: []string{"red", "blue"}, claimJSON: `["red"]`, opts: []BindingOption{WithBindingOnMissing("allow")}, wantReason: ReasonMalformed},
		{name: "allow + empty header value still rejects", headerVals: []string{""}, claimJSON: `["red"]`, opts: []BindingOption{WithBindingOnMissing("allow")}, wantReason: ReasonMalformed},
		{name: "allow + malformed claim still rejects", headerVals: []string{"red"}, claimJSON: `{"a":1}`, opts: []BindingOption{WithBindingOnMissing("allow")}, wantReason: ReasonMalformed},

		// A malformed operand on the side opposite an absent one. These cases fail if
		// absence is ever checked before malformedness; the single-sided cases above
		// would still pass.
		{name: "allow + header absent + malformed claim rejects", headerVals: nil, claimJSON: `{"a":1}`, opts: []BindingOption{WithBindingOnMissing("allow")}, wantReason: ReasonMalformed},
		{name: "allow + claim absent + multiple headers rejects", headerVals: []string{"red", "blue"}, claimJSON: "", opts: []BindingOption{WithBindingOnMissing("allow")}, wantReason: ReasonMalformed},
		{name: "allow + header absent + exact on array rejects", headerVals: nil, claimJSON: `["red"]`, opts: []BindingOption{WithBindingOnMissing("allow"), WithBindingMatch("exact")}, wantReason: ReasonMalformed},

		// on_missing=allow skips genuine absence.
		{name: "allow + header absent skips", headerVals: nil, claimJSON: `["red"]`, opts: []BindingOption{WithBindingOnMissing("allow")}, skipped: true},
		{name: "allow + claim absent skips", headerVals: []string{"red"}, claimJSON: "", opts: []BindingOption{WithBindingOnMissing("allow")}, skipped: true},
		{name: "allow + both absent skips", headerVals: nil, claimJSON: "", opts: []BindingOption{WithBindingOnMissing("allow")}, skipped: true},

		// A request fault is decided before the claim is read.
		{name: "reject + both absent ⇒ header_absent", headerVals: nil, claimJSON: "", wantReason: ReasonHeaderAbsent},
		{name: "reject + header absent + malformed claim ⇒ header_absent", headerVals: nil, claimJSON: `{"a":1}`, wantReason: ReasonHeaderAbsent},
		{name: "multiple headers + malformed claim ⇒ malformed", headerVals: []string{"red", "blue"}, claimJSON: `{"a":1}`, wantReason: ReasonMalformed},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := mustBinding(t, claim, header, tc.opts...)
			matched, reason, err := validateOne(t, b, reqWithHeader(t, header, tc.headerVals), claimsWithRaw(t, claim, tc.claimJSON))

			if tc.wantReason == "" {
				require.NoError(t, err)
				assert.Empty(t, string(reason))

				// The matched value labels mercure_subscribers_by_binding_value, so a skip must not
				// report one.
				if tc.skipped {
					assert.Empty(t, matched)
				} else {
					assert.Equal(t, tc.headerVals[0], matched)
				}

				return
			}

			require.Error(t, err)
			assert.Equal(t, tc.wantReason, reason)
			assert.Empty(t, matched)
		})
	}
}

// An over-cap claim array is malformed rather than an oversized authorized set.
func TestClaimHeaderBindingRejectsOversizedClaimArray(t *testing.T) {
	t.Parallel()

	vals := make([]string, 0, maxClaimHeaderValues+1)
	for range maxClaimHeaderValues + 1 {
		vals = append(vals, "x")
	}

	raw, err := json.Marshal(vals)
	require.NoError(t, err)

	b := mustBinding(t, "groups", "Group-ID")
	_, reason, err := validateOne(t, b, reqWithHeader(t, "Group-ID", []string{"x"}), claimsWithRaw(t, "groups", string(raw)))

	require.Error(t, err)
	assert.Equal(t, ReasonMalformed, reason)
}

// Claims that never hold a string or string array cannot be a binding target, since
// every request would evaluate malformed, so construction rejects them. A string-valued
// registered claim like `sub` stays bindable.
func TestNewClaimHeaderBindingRejectsUnbindableClaims(t *testing.T) {
	t.Parallel()

	// Known at config time to be malformed: Mercure's object claims, the
	// authorization_details array of objects and the RFC 7519 numeric-date claims.
	for _, claim := range []string{"mercure", "https://mercure.rocks/", "authorization_details", "exp", "nbf", "iat"} {
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

// jsonTagName returns the JSON name of a struct field, read from its tag.
func jsonTagName(tb testing.TB, typ reflect.Type, field string) string {
	tb.Helper()

	f, ok := typ.FieldByName(field)
	require.True(tb, ok, "%s.%s must exist", typ.Name(), field)

	return strings.Split(f.Tag.Get("json"), ",")[0]
}

// The unbindable-claim constants must stay in lockstep with the json tags on the claims
// struct: if a tag is renamed, the reject would silently stop matching and a claim that never
// holds a string would become bindable. The legacy mercure tags are checked in
// deprecated_claim builds, the only ones that declare them.
func TestReservedClaimsMatchStructTags(t *testing.T) {
	t.Parallel()

	assert.Equal(t, authorizationDetailsClaim, jsonTagName(t, reflect.TypeFor[claims](), "AuthorizationDetails"))
}

// Dashboards and alerts key on the reason label values, so they are a wire contract: a
// renamed constant must fail here, not silently split a time series.
func TestAuthzRejectReasonLabelValues(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "header_absent", string(ReasonHeaderAbsent))
	assert.Equal(t, "claim_absent", string(ReasonClaimAbsent))
	assert.Equal(t, "mismatch", string(ReasonMismatch))
	assert.Equal(t, "malformed", string(ReasonMalformed))
}

// Header name lookup is case-insensitive (HTTP), while the value compare is not.
func TestClaimHeaderBindingHeaderNameIsCaseInsensitive(t *testing.T) {
	t.Parallel()

	b := mustBinding(t, "groups", "Group-ID")
	// sent with a differently-cased field name
	_, reason, err := validateOne(t, b, reqWithHeader(t, "group-id", []string{"red"}), claimsWithRaw(t, "groups", `["red"]`))

	require.NoError(t, err)
	assert.Empty(t, string(reason))
}

// The three match modes must be pairwise distinguishable, or an operator who selects one
// is not choosing anything. auto accepts either shape; exact demands a scalar; member
// demands an array. This test fails if member ever silently degrades into an alias of auto.
func TestClaimHeaderBindingMatchModesAreDistinct(t *testing.T) {
	t.Parallel()

	const scalar, array = `"red"`, `["red"]`

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

			b := mustBinding(t, "groups", "Group-ID", WithBindingMatch(tc.mode))

			for shape, claimJSON := range map[string]string{"scalar": scalar, "array": array} {
				_, reason, err := validateOne(t, b, reqWithHeader(t, "Group-ID", []string{"red"}), claimsWithRaw(t, "groups", claimJSON))

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

// Bindings must be enforced on the publish path too, not only on subscribe.
func TestAuthorizeAndBindEnforcesOnPublish(t *testing.T) {
	t.Parallel()

	binding := mustBinding(t, "groups", "Group-ID")

	publishReq := func(tb testing.TB, group string) *http.Request {
		tb.Helper()

		r := httptest.NewRequest(http.MethodPost, defaultHubURL, nil)
		r.Header.Set("Authorization", bearerPrefix+createDummyJWTWithExtraClaims(tb, rolePublisher, []string{"*"}, map[string]any{"groups": []string{"red"}}))

		if group != "" {
			r.Header.Set("Group-ID", group)
		}

		return r
	}

	t.Run("matching header is authorized", func(t *testing.T) {
		t.Parallel()

		c, err := boundHub(t, binding).authorizeAndBind(publishReq(t, "red"), true)
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

		subOnly := mustBinding(t, "groups", "Group-ID", WithBindingRoles("subscriber"))

		c, err := boundHub(t, subOnly).authorizeAndBind(publishReq(t, "evil"), true)
		require.NoError(t, err)
		assert.NotNil(t, c)
	})
}

// A token-bearing request whose header mismatches must be rejected outright, not
// downgraded to an anonymous connection.
func TestAuthorizeAndBindRejectsMismatchEvenWhenAnonymousAllowed(t *testing.T) {
	t.Parallel()

	h := createAnonymousDummy(t, WithClaimHeaderBindings(mustBinding(t, "groups", "Group-ID")))

	r := httptest.NewRequest(http.MethodGet, defaultHubURL, nil)
	r.Header.Set("Authorization", bearerPrefix+createDummyJWTWithExtraClaims(t, roleSubscriber, []string{"*"}, map[string]any{"groups": []string{"red"}}))
	r.Header.Set("Group-ID", "evil")

	c, err := h.authorizeAndBind(r, false)

	require.ErrorIs(t, err, ErrClaimHeaderRejected, "a mismatched token must be rejected, not fall back to anonymous")
	assert.Nil(t, c)
}

// Capturing every top-level claim costs a second decode per authenticated request, and the
// map is then pinned to each subscriber for the connection's lifetime. Deployments with no
// binding configured must not pay either cost, and a hub that does bind must release the
// map once the bindings have consumed it.
func TestRawClaimsAreCapturedOnlyWhenBoundAndReleasedAfterUse(t *testing.T) {
	t.Parallel()

	token := createDummyJWTWithExtraClaims(t, roleSubscriber, []string{"*"}, map[string]any{"groups": []string{"red"}})

	req := func() *http.Request {
		r := httptest.NewRequest(http.MethodGet, defaultHubURL, nil)
		r.Header.Set("Authorization", bearerPrefix+token)
		r.Header.Set("Group-ID", "red")

		return r
	}

	// Observed through authorize(), not authorizeAndBind(): the wrapper releases the map on
	// every path, so asserting on its output cannot tell "never captured" from "captured
	// then cleared".
	t.Run("no bindings configured captures nothing", func(t *testing.T) {
		t.Parallel()

		c, err := createDummy(t).authorize(req(), false)
		require.NoError(t, err)
		require.NotNil(t, c)
		assert.Nil(t, c.rawClaims, "a hub with no bindings must not pay the raw-claim capture")
		assert.NotEmpty(t, c.authz.subscribeMatchers(), "the typed claims must still decode")
	})

	t.Run("a binding configured captures the map", func(t *testing.T) {
		t.Parallel()

		c, err := boundHub(t, mustBinding(t, "groups", "Group-ID")).authorize(req(), false)
		require.NoError(t, err)
		require.NotNil(t, c)
		assert.JSONEq(t, `["red"]`, string(c.rawClaims["groups"]), "a hub with a binding must capture the raw claims")
		assert.NotEmpty(t, c.authz.subscribeMatchers(), "the capture must not disturb the typed decode")
	})

	// The capture is gated on the bindings that apply to this request, so a
	// publisher-scoped binding must not tax every subscribe request.
	t.Run("an inapplicable binding captures nothing", func(t *testing.T) {
		t.Parallel()

		pubOnly := mustBinding(t, "groups", "Group-ID", WithBindingRoles("publisher"))

		c, err := boundHub(t, pubOnly).authorize(req(), false)
		require.NoError(t, err)
		require.NotNil(t, c)
		assert.Nil(t, c.rawClaims, "a publisher-scoped binding must not capture on a subscribe request")
	})

	// ...but the request must still be authorized, not rejected by the nil-rawClaims guard.
	t.Run("an inapplicable binding still authorizes the request", func(t *testing.T) {
		t.Parallel()

		pubOnly := mustBinding(t, "groups", "Group-ID", WithBindingRoles("publisher"))

		c, err := boundHub(t, pubOnly).authorizeAndBind(req(), false)
		require.NoError(t, err, "a skipped binding must not reject via the nil-rawClaims guard")
		assert.NotNil(t, c)
	})

	t.Run("bindings configured release the map after enforcement", func(t *testing.T) {
		t.Parallel()

		c, err := boundHub(t, mustBinding(t, "groups", "Group-ID")).authorizeAndBind(req(), false)
		require.NoError(t, err)
		require.NotNil(t, c)
		assert.Nil(t, c.rawClaims, "rawClaims must be released once the bindings have run")
	})

	// selectVerifier pre-parses the token before its signature is checked. That parse must
	// not capture: only the parse jwt.ParseWithClaims verifies may.
	t.Run("a zero claims value captures nothing", func(t *testing.T) {
		t.Parallel()

		var pre claims

		_, _, err := jwt.NewParser().ParseUnverified(token, &pre)
		require.NoError(t, err)
		assert.Nil(t, pre.rawClaims)
	})
}

// rawClaims must be populated whenever a binding will call evalClaim on this request,
// because evalClaim reads a nil map on a live *claims as malformed. If capturesRawClaims
// and enforceBindings ever disagree about which bindings apply, a legitimate request is
// rejected. This walks every combination of binding roles against both request kinds.
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
						mustBinding(t, "groups", "Group-ID", WithBindingRoles(first)),
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
							map[string]any{"groups": []string{"red"}, "regions": []string{"eu"}}))
						r.Header.Set("Group-ID", "red")
						r.Header.Set("X-Region", "eu")

						return r
					}

					c, err := h.authorize(newReq(), publish)
					require.NoError(t, err)
					require.NotNil(t, c)
					assert.Equal(t, anyApplies, c.rawClaims != nil,
						"the capture gate must fire exactly when a binding will read the claims")

					// And the request must be authorized either way, never spuriously rejected.
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

	b := mustBinding(t, "groups", "Group-ID", WithBindingOnMissing("allow"))
	r := reqWithHeader(t, "Group-ID", []string{"red"})

	_, reason, err := validateOne(t, b, r, &claims{}) // non-nil claims, nil rawClaims

	require.ErrorIs(t, err, ErrClaimHeaderRejected, "on_missing=allow must not skip a stripped claims object")
	assert.Equal(t, ReasonMalformed, reason)
}

// The cap is `> maxClaimHeaderValues`, so exactly maxClaimHeaderValues entries are accepted.
func TestClaimHeaderBindingCapBoundaryAcceptsExactlyMax(t *testing.T) {
	t.Parallel()

	vals := make([]string, maxClaimHeaderValues)
	for i := range vals {
		vals[i] = "filler"
	}

	vals[0] = "red"

	raw, err := json.Marshal(vals)
	require.NoError(t, err)

	b := mustBinding(t, "groups", "Group-ID")
	_, reason, err := validateOne(t, b, reqWithHeader(t, "Group-ID", []string{"red"}), claimsWithRaw(t, "groups", string(raw)))

	require.NoError(t, err, "exactly maxClaimHeaderValues entries must be accepted")
	assert.Empty(t, string(reason))
}

// The claim, unlike the header, is looked up by exact key: JWT claim names are
// case-sensitive and must not be matched loosely. A near-miss name reads as absent.
func TestClaimHeaderBindingClaimNameIsExact(t *testing.T) {
	t.Parallel()

	b := mustBinding(t, "groups", "Group-ID")

	for _, name := range []string{"Groups", "group", "groups "} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, reason, err := validateOne(t, b, reqWithHeader(t, "Group-ID", []string{"red"}), claimsWithRaw(t, name, `["red"]`))

			require.ErrorIs(t, err, ErrClaimHeaderRejected)
			assert.Equal(t, ReasonClaimAbsent, reason)
		})
	}
}

// Rejection errors must never carry the header or claim values: they reach the logs and spans.
func TestClaimHeaderBindingErrorDoesNotLeakValues(t *testing.T) {
	t.Parallel()

	b := mustBinding(t, "groups", "Group-ID")
	_, _, err := validateOne(t, b, reqWithHeader(t, "Group-ID", []string{"supersecret-header"}), claimsWithRaw(t, "groups", `["topsecret-claim"]`))

	require.Error(t, err)
	assert.NotContains(t, err.Error(), "supersecret-header")
	assert.NotContains(t, err.Error(), "topsecret-claim")
	// but it must identify the binding (canonical header form) and the reason
	assert.Contains(t, err.Error(), "groups")
	assert.Contains(t, err.Error(), "Group-Id")
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
		{name: "valid", claim: "groups", header: "Group-ID"},
		{name: "empty claim", claim: "", header: "Group-ID", wantErr: true},
		{name: "empty header", claim: "groups", header: "", wantErr: true},
		{name: "header with space", claim: "groups", header: "Group ID", wantErr: true},
		{name: "header with colon", claim: "groups", header: "Group:ID", wantErr: true},
		{name: "wildcard header", claim: "groups", header: "*", wantErr: true},
		{name: "header containing an asterisk", claim: "groups", header: "X-*"},
		{name: "Host header", claim: "groups", header: "Host", wantErr: true},
		{name: "Host header in lower case", claim: "groups", header: "host", wantErr: true},
		{name: "header starting with Host", claim: "groups", header: "Host-Group"},
		{name: "control char in header", claim: "groups", header: "Group\x00ID", wantErr: true},
		{name: "control char in claim", claim: "gro\x00ups", header: "Group-ID", wantErr: true},
		{name: "over-long claim", claim: longName, header: "Group-ID", wantErr: true},
		{name: "over-long header", claim: "groups", header: longName, wantErr: true},
		{name: "bad match enum", claim: "groups", header: "Group-ID", opts: []BindingOption{WithBindingMatch("exat")}, wantErr: true},
		{name: "bad on_missing enum", claim: "groups", header: "Group-ID", opts: []BindingOption{WithBindingOnMissing("mabye")}, wantErr: true},
		{name: "bad roles enum", claim: "groups", header: "Group-ID", opts: []BindingOption{WithBindingRoles("subscribr")}, wantErr: true},
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

// authorizeAndBind runs every applicable binding after authorize().
func TestAuthorizeAndBind(t *testing.T) {
	t.Parallel()

	binding := mustBinding(t, "groups", "Group-ID")
	subToken := func(tb testing.TB, groups any) string {
		tb.Helper()

		return createDummyJWTWithExtraClaims(tb, roleSubscriber, []string{"*"}, map[string]any{"groups": groups})
	}

	t.Run("matching header is authorized", func(t *testing.T) {
		t.Parallel()

		r := httptest.NewRequest(http.MethodGet, "/", nil)
		r.Header.Set("Authorization", bearerPrefix+subToken(t, []string{"red"}))
		r.Header.Set("Group-ID", "red")

		c, err := boundHub(t, binding).authorizeAndBind(r, false)
		require.NoError(t, err)
		assert.NotNil(t, c)
	})

	t.Run("mismatched header is rejected", func(t *testing.T) {
		t.Parallel()

		r := httptest.NewRequest(http.MethodGet, "/", nil)
		r.Header.Set("Authorization", bearerPrefix+subToken(t, []string{"red"}))
		r.Header.Set("Group-ID", "blue")

		c, err := boundHub(t, binding).authorizeAndBind(r, false)
		require.ErrorIs(t, err, ErrClaimHeaderRejected)
		assert.Nil(t, c)
	})

	t.Run("absent header is rejected fail-closed", func(t *testing.T) {
		t.Parallel()

		r := httptest.NewRequest(http.MethodGet, "/", nil)
		r.Header.Set("Authorization", bearerPrefix+subToken(t, []string{"red"}))

		_, err := boundHub(t, binding).authorizeAndBind(r, false)
		require.ErrorIs(t, err, ErrClaimHeaderRejected)
	})

	// Anonymous requests carry no token, so there is no claim to bind: skipped, not rejected.
	t.Run("anonymous request skips bindings", func(t *testing.T) {
		t.Parallel()

		h := createAnonymousDummy(t, WithClaimHeaderBindings(binding))

		r := httptest.NewRequest(http.MethodGet, "/", nil) // no token, no header
		c, err := h.authorizeAndBind(r, false)

		require.NoError(t, err)
		assert.Nil(t, c)
	})

	// A publisher-scoped binding must not gate subscribe requests.
	t.Run("role scoping", func(t *testing.T) {
		t.Parallel()

		pubOnly := mustBinding(t, "groups", "Group-ID", WithBindingRoles("publisher"))

		r := httptest.NewRequest(http.MethodGet, "/", nil)
		r.Header.Set("Authorization", bearerPrefix+subToken(t, []string{"red"}))
		// No Group-ID header at all, which would be header_absent if the binding applied.

		_, err := boundHub(t, pubOnly).authorizeAndBind(r, false)
		require.NoError(t, err)
	})

	t.Run("two bindings are AND-combined", func(t *testing.T) {
		t.Parallel()

		region := mustBinding(t, "regions", "X-Region")
		token := createDummyJWTWithExtraClaims(t, roleSubscriber, []string{"*"}, map[string]any{
			"groups":  []string{"red"},
			"regions": []string{"us-east"},
		})

		r := httptest.NewRequest(http.MethodGet, "/", nil)
		r.Header.Set("Authorization", bearerPrefix+token)
		r.Header.Set("Group-ID", "red")
		r.Header.Set("X-Region", "eu-west") // second binding fails

		_, err := boundHub(t, binding, region).authorizeAndBind(r, false)
		require.ErrorIs(t, err, ErrClaimHeaderRejected)
	})

	// authorize() accepts the token via the Authorization header or the cookie; all
	// converge here. The legacy query parameter is covered only in deprecated_claim builds
	// (see TestAuthorizeAndBindLegacyQueryParam).
	t.Run("enforced across all auth carriers", func(t *testing.T) {
		t.Parallel()

		token := subToken(t, []string{"red"})

		t.Run("cookie", func(t *testing.T) {
			t.Parallel()

			r := httptest.NewRequest(http.MethodGet, "/", nil)
			r.AddCookie(&http.Cookie{Name: defaultCookieName, Value: token})
			r.Header.Set("Group-ID", "blue")

			_, err := boundHub(t, binding).authorizeAndBind(r, false)
			require.ErrorIs(t, err, ErrClaimHeaderRejected)
		})

		// The cookie-POST carrier reaches validateJWT through the CSRF Origin check, a
		// distinct return point from the cookie-GET path above.
		t.Run("cookie POST with allowed origin", func(t *testing.T) {
			t.Parallel()

			h := createDummy(t, WithClaimHeaderBindings(binding), WithPublishOrigins([]string{"https://example.com"}))

			r := httptest.NewRequest(http.MethodPost, "/", nil)
			r.AddCookie(&http.Cookie{Name: defaultCookieName, Value: token})
			r.Header.Set("Origin", "https://example.com")
			r.Header.Set("Group-ID", "blue")

			_, err := h.authorizeAndBind(r, false)
			require.ErrorIs(t, err, ErrClaimHeaderRejected)
		})
	})

	// A binding that passes must not touch the rejection counter. Without this, a stray
	// increment on the success path would go unnoticed.
	t.Run("an authorized request does not meter a rejection", func(t *testing.T) {
		t.Parallel()

		m := NewPrometheusMetrics(prometheus.NewPedanticRegistry())
		h := createDummy(t, WithClaimHeaderBindings(binding), WithMetrics(m))

		r := httptest.NewRequest(http.MethodGet, defaultHubURL, nil)
		r.Header.Set("Authorization", bearerPrefix+subToken(t, []string{"red"}))
		r.Header.Set("Group-ID", "red")

		_, err := h.authorizeAndBind(r, false)
		require.NoError(t, err)

		assert.Equal(t, 0, countSeries(m.claimHeaderRejectedTotal))
	})
}

// Set-level checks live at the Hub, the only place that sees every binding, so they
// hold for Go callers as well as the Caddy adapter.
func TestNewHubClaimHeaderBindingGuards(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	resource := WithResourceIdentifier(testResourceIdentifier)

	all := mustBinding(t, "groups", "Group-ID")
	subOnly := mustBinding(t, "groups", "Group-ID", WithBindingRoles("subscriber"))
	pubOnly := mustBinding(t, "groups", "Group-ID", WithBindingRoles("publisher"))

	t.Run("subscriber-covering binding without a subscriber verifier fails", func(t *testing.T) {
		t.Parallel()

		_, err := NewHub(ctx, resource, WithClaimHeaderBindings(all), issuerWith(true, false))
		require.ErrorIs(t, err, ErrInvalidClaimHeaderBinding)

		_, err = NewHub(ctx, resource, WithClaimHeaderBindings(subOnly), issuerWith(true, false))
		require.ErrorIs(t, err, ErrInvalidClaimHeaderBinding)
	})

	t.Run("publisher-scoped binding without a publisher verifier fails", func(t *testing.T) {
		t.Parallel()

		_, err := NewHub(ctx, resource, WithClaimHeaderBindings(pubOnly), issuerWith(false, true))
		require.ErrorIs(t, err, ErrInvalidClaimHeaderBinding)
	})

	t.Run("both verifiers present succeeds", func(t *testing.T) {
		t.Parallel()

		_, err := NewHub(ctx, resource, WithClaimHeaderBindings(all), issuerWith(true, true))
		require.NoError(t, err)
	})

	// The two startup conditions that weaken a binding without invalidating it are
	// announced with a warning.
	t.Run("anonymous access skips subscriber bindings, with a warning", func(t *testing.T) {
		t.Parallel()

		valueOnly := mustValueBinding(t, "groups", []string{"red"}, WithBindingRoles("subscriber"))

		for _, b := range []ClaimHeaderBinding{subOnly, valueOnly} {
			var buf bytes.Buffer

			_, err := NewHub(ctx, resource, WithClaimHeaderBindings(b), issuerWith(false, true), WithAnonymous(),
				WithLogger(slog.New(slog.NewJSONHandler(&buf, nil))))
			require.NoError(t, err)

			assert.Contains(t, buf.String(), "anonymous subscribers carry no token and skip every binding check")
		}
	})

	t.Run("metrics that cannot report rejections are warned about", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer

		// noWriteFlushObserverMetrics is not NopMetrics and implements neither reporter.
		_, err := NewHub(ctx, resource, WithClaimHeaderBindings(all), issuerWith(true, true),
			WithMetrics(noWriteFlushObserverMetrics{}), WithLogger(slog.New(slog.NewJSONHandler(&buf, nil))))
		require.NoError(t, err)

		assert.Contains(t, buf.String(), "does not implement AuthorizationRejectionReporter")
	})

	t.Run("the default NopMetrics is not warned about", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer

		_, err := NewHub(ctx, resource, WithClaimHeaderBindings(all), issuerWith(true, true),
			WithLogger(slog.New(slog.NewJSONHandler(&buf, nil))))
		require.NoError(t, err)

		assert.NotContains(t, buf.String(), "RejectionReporter")
	})

	t.Run("a reporting metrics implementation is not warned about", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer

		_, err := NewHub(ctx, resource, WithClaimHeaderBindings(all), issuerWith(true, true),
			WithMetrics(NewPrometheusMetrics(prometheus.NewPedanticRegistry())),
			WithLogger(slog.New(slog.NewJSONHandler(&buf, nil))))
		require.NoError(t, err)

		assert.NotContains(t, buf.String(), "AuthorizationRejectionReporter")
	})

	// A ClaimHeaderBinding{} is constructible cross-package despite unexported fields.
	t.Run("zero-value binding is rejected", func(t *testing.T) {
		t.Parallel()

		_, err := NewHub(ctx, resource, WithClaimHeaderBindings(ClaimHeaderBinding{}), issuerWith(true, true))
		require.ErrorIs(t, err, ErrInvalidClaimHeaderBinding)
	})

	// Same claim+canonical header ⇒ ambiguous metric label; rejected regardless of options.
	t.Run("overlapping bindings are rejected", func(t *testing.T) {
		t.Parallel()

		other := mustBinding(t, "groups", "group-id", WithBindingMatch("member"), WithBindingRoles("publisher"))

		_, err := NewHub(ctx, resource, WithClaimHeaderBindings(all, other), issuerWith(true, true))
		require.ErrorIs(t, err, ErrInvalidClaimHeaderBinding)
	})

	t.Run("distinct bindings are accepted", func(t *testing.T) {
		t.Parallel()

		other := mustBinding(t, "regions", "X-Region")

		h, err := NewHub(ctx, resource, WithClaimHeaderBindings(all, other), issuerWith(true, true))
		require.NoError(t, err)
		assert.Len(t, h.claimHeaderBindings, 2)
	})

	// The option must copy: a caller mutating its slice afterwards must not affect the Hub.
	t.Run("option defensively copies the slice", func(t *testing.T) {
		t.Parallel()

		bindings := []ClaimHeaderBinding{all}

		h, err := NewHub(ctx, resource, WithClaimHeaderBindings(bindings...), issuerWith(true, true))
		require.NoError(t, err)

		bindings[0] = ClaimHeaderBinding{}

		require.Len(t, h.claimHeaderBindings, 1)
		assert.Equal(t, "groups:Group-Id", h.claimHeaderBindings[0].id())
	})
}

// The header name is canonicalized at construction so lookup and the metric label are
// stable regardless of the casing an operator configured. Go's canonical form upper-cases
// only the first letter of each dash-separated token ("group-id" and "Group-ID" both
// become "Group-Id", exactly as "Last-Event-ID" becomes "Last-Event-Id").
func TestNewClaimHeaderBindingCanonicalizesHeader(t *testing.T) {
	t.Parallel()

	for _, configured := range []string{"group-id", "Group-ID", "GROUP-ID"} {
		b := mustBinding(t, "groups", configured)
		assert.Equal(t, "Group-Id", b.header)
		assert.Equal(t, "groups:Group-Id", b.id())
	}
}

// A rejection that flows through the real wrapper must be metered with the typed reason,
// not just logged.
func TestAuthorizeAndBindReportsRejectionMetric(t *testing.T) {
	t.Parallel()

	m := NewPrometheusMetrics(prometheus.NewRegistry())
	h := createDummy(t, WithClaimHeaderBindings(mustBinding(t, "groups", "Group-ID")), WithMetrics(m))

	token := createDummyJWTWithExtraClaims(t, roleSubscriber, []string{"*"}, map[string]any{"groups": []string{"red"}})

	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.Header.Set("Authorization", bearerPrefix+token)
	r.Header.Set("Group-ID", "blue")

	_, err := h.authorizeAndBind(r, false)
	require.ErrorIs(t, err, ErrClaimHeaderRejected)

	assertCounterVecValue(t, 1.0, m.claimHeaderRejectedTotal, "groups:Group-Id", string(ReasonMismatch))
}

// countSeries reports how many series a collector currently exposes.
func countSeries(c prometheus.Collector) int {
	ch := make(chan prometheus.Metric, 64)
	c.Collect(ch)
	close(ch)

	return len(ch)
}

// A value binding compares the claim against the configured set and never reads a header:
// validateOne is called with a nil request, which a header lookup would dereference.
func TestClaimValueBindingValidate(t *testing.T) {
	t.Parallel()

	const claim = "groups"

	values := []string{"red", "blue"}

	testCases := []struct {
		name       string
		claimJSON  string // "" ⇒ claim key absent
		opts       []BindingOption
		wantReason AuthzRejectReason // "" ⇒ allowed/skipped
	}{
		{name: "auto/array intersects", claimJSON: `["other","blue"]`},
		{name: "auto/scalar in set", claimJSON: `"red"`},
		{name: "member/array intersects", claimJSON: `["red"]`, opts: []BindingOption{WithBindingMatch("member")}},
		{name: "exact/scalar in set", claimJSON: `"blue"`, opts: []BindingOption{WithBindingMatch("exact")}},

		{name: "auto/array no intersection", claimJSON: `["other","green"]`, wantReason: ReasonMismatch},
		{name: "auto/scalar not in set", claimJSON: `"green"`, wantReason: ReasonMismatch},
		{name: "comparison is case-sensitive", claimJSON: `["RED"]`, wantReason: ReasonMismatch},

		{name: "claim key missing", claimJSON: "", wantReason: ReasonClaimAbsent},
		{name: "claim null", claimJSON: `null`, wantReason: ReasonClaimAbsent},
		{name: "claim empty array", claimJSON: `[]`, wantReason: ReasonClaimAbsent},

		{name: "allow + claim key missing skips", claimJSON: "", opts: []BindingOption{WithBindingOnMissing("allow")}},
		{name: "allow + claim empty array skips", claimJSON: `[]`, opts: []BindingOption{WithBindingOnMissing("allow")}},

		{name: "member on scalar claim", claimJSON: `"red"`, opts: []BindingOption{WithBindingMatch("member")}, wantReason: ReasonMalformed},
		{name: "exact on array claim", claimJSON: `["red"]`, opts: []BindingOption{WithBindingMatch("exact")}, wantReason: ReasonMalformed},
		{name: "claim number", claimJSON: `42`, wantReason: ReasonMalformed},
		{name: "claim non-string element", claimJSON: `["red",1]`, wantReason: ReasonMalformed},
		{name: "allow + member on scalar still rejects", claimJSON: `"red"`, opts: []BindingOption{WithBindingMatch("member"), WithBindingOnMissing("allow")}, wantReason: ReasonMalformed},
		{name: "allow + exact on array still rejects", claimJSON: `["red"]`, opts: []BindingOption{WithBindingMatch("exact"), WithBindingOnMissing("allow")}, wantReason: ReasonMalformed},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := mustValueBinding(t, claim, values, tc.opts...)
			_, reason, err := validateOne(t, b, nil, claimsWithRaw(t, claim, tc.claimJSON))

			if tc.wantReason == "" {
				require.NoError(t, err)
				assert.Empty(t, string(reason))

				return
			}

			require.ErrorIs(t, err, ErrClaimHeaderRejected)
			assert.Equal(t, tc.wantReason, reason)
		})
	}

	t.Run("stripped raw claims are malformed, even under allow", func(t *testing.T) {
		t.Parallel()

		b := mustValueBinding(t, claim, values, WithBindingOnMissing("allow"))
		_, reason, err := validateOne(t, b, nil, &claims{})

		require.Error(t, err)
		assert.Equal(t, ReasonMalformed, reason)
	})
}

func TestNewClaimValueBindingValidation(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name    string
		claim   string
		values  []string
		opts    []BindingOption
		wantErr string // "" ⇒ valid
	}{
		{name: "one value", claim: "groups", values: []string{"red"}},
		{name: "several values", claim: "groups", values: []string{"red", "blue"}},
		{name: "empty claim", claim: "", values: []string{"red"}, wantErr: "claim name must not be empty"},
		{name: "nil values", claim: "groups", values: nil, wantErr: "at least one value is required"},
		{name: "no values", claim: "groups", values: []string{}, wantErr: "at least one value is required"},
		{name: "empty value", claim: "groups", values: []string{"red", ""}, wantErr: "values must not be empty"},
		{name: "duplicate value", claim: "groups", values: []string{"red", "blue", "red"}, wantErr: `duplicate value "red"`},
		{name: "mercure claim", claim: "mercure", values: []string{"x"}, wantErr: "cannot be bound"},
		{name: "namespaced mercure claim", claim: "https://mercure.rocks/", values: []string{"x"}, wantErr: "cannot be bound"},
		{name: "authorization_details claim", claim: "authorization_details", values: []string{"x"}, wantErr: "cannot be bound"},
		{name: "exp claim", claim: "exp", values: []string{"1"}, wantErr: "cannot be bound"},
		{name: "bad match", claim: "groups", values: []string{"red"}, opts: []BindingOption{WithBindingMatch("exat")}, wantErr: `unknown match "exat"`},
		{name: "bad on_missing", claim: "groups", values: []string{"red"}, opts: []BindingOption{WithBindingOnMissing("mabye")}, wantErr: `unknown on_missing "mabye"`},
		{name: "bad roles", claim: "groups", values: []string{"red"}, opts: []BindingOption{WithBindingRoles("subscribr")}, wantErr: `unknown roles "subscribr"`},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b, err := NewClaimValueBinding(tc.claim, tc.values, tc.opts...)
			if tc.wantErr == "" {
				require.NoError(t, err)
				assert.Equal(t, tc.values, *b.values)
				assert.Equal(t, tc.claim, b.id())

				return
			}

			require.ErrorIs(t, err, ErrInvalidClaimHeaderBinding)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

// The constructor copies the values, so a caller reusing its slice cannot change the policy.
func TestNewClaimValueBindingCopiesValues(t *testing.T) {
	t.Parallel()

	values := []string{"red", "blue"}
	b := mustValueBinding(t, "groups", values)

	values[0] = "evil"

	_, reason, err := validateOne(t, b, nil, claimsWithRaw(t, "groups", `["evil"]`))
	require.Error(t, err)
	assert.Equal(t, ReasonMismatch, reason)
	assert.Equal(t, []string{"red", "blue"}, *b.values)
}

// ClaimHeaderBinding is exported, so Go embedders may compare it with == or use it as a map
// key. The map literal compiles only while the type is comparable. For value bindings == is
// an identity check on the values pointer, not a deep compare: two value bindings built from
// equal value lists are not equal.
func TestClaimHeaderBindingIsComparable(t *testing.T) {
	t.Parallel()

	h := mustBinding(t, "groups", "Group-ID")
	v := mustValueBinding(t, "groups", []string{"red"})

	set := map[ClaimHeaderBinding]struct{}{h: {}, v: {}}

	assert.Len(t, set, 2)
	assert.NotEqual(t, h, v)

	_, ok := set[v]
	assert.True(t, ok)
}

func TestAuthorizeAndBindClaimValue(t *testing.T) {
	t.Parallel()

	token := func(tb testing.TB, r role, groups any) string {
		tb.Helper()

		extra := map[string]any{}
		if groups != nil {
			extra["groups"] = groups
		}

		return createDummyJWTWithExtraClaims(tb, r, []string{"*"}, extra)
	}

	request := func(tb testing.TB, tok string) *http.Request {
		tb.Helper()

		r := httptest.NewRequest(http.MethodGet, defaultHubURL, nil)
		r.Header.Set("Authorization", bearerPrefix+tok)

		return r
	}

	subOnly := mustValueBinding(t, "groups", []string{"red", "blue"}, WithBindingRoles("subscriber"))

	t.Run("subscriber in the set is authorized", func(t *testing.T) {
		t.Parallel()

		c, err := boundHub(t, subOnly).authorizeAndBind(request(t, token(t, roleSubscriber, []string{"blue"})), false)
		require.NoError(t, err)
		assert.NotNil(t, c)
	})

	t.Run("subscriber outside the set is rejected", func(t *testing.T) {
		t.Parallel()

		_, err := boundHub(t, subOnly).authorizeAndBind(request(t, token(t, roleSubscriber, []string{"green"})), false)
		require.ErrorIs(t, err, ErrClaimHeaderRejected)
	})

	// roles subscriber: a publisher whose token would fail the binding is not evaluated.
	t.Run("publisher is not evaluated under roles subscriber", func(t *testing.T) {
		t.Parallel()

		h := boundHub(t, subOnly)

		for _, groups := range []any{nil, []string{"green"}, "red"} {
			c, err := h.authorizeAndBind(request(t, token(t, rolePublisher, groups)), true)
			require.NoError(t, err)
			assert.NotNil(t, c)
		}
	})

	t.Run("publisher is evaluated under roles all", func(t *testing.T) {
		t.Parallel()

		all := mustValueBinding(t, "groups", []string{"red"})

		_, err := boundHub(t, all).authorizeAndBind(request(t, token(t, rolePublisher, []string{"green"})), true)
		require.ErrorIs(t, err, ErrClaimHeaderRejected)
	})

	// Rejections reach the claim-value counter with the typed reason, and never the
	// claim-header counter.
	t.Run("rejections are metered on the claim-value counter only", func(t *testing.T) {
		t.Parallel()

		m := NewPrometheusMetrics(prometheus.NewPedanticRegistry())
		h := createDummy(t, WithClaimHeaderBindings(mustValueBinding(t, "groups", []string{"red"}, WithBindingMatch("member"))), WithMetrics(m))

		for _, groups := range []any{[]string{"green"}, []string{"green"}, nil, "red"} {
			_, err := h.authorizeAndBind(request(t, token(t, roleSubscriber, groups)), false)
			require.ErrorIs(t, err, ErrClaimHeaderRejected)
		}

		_, err := h.authorizeAndBind(request(t, token(t, roleSubscriber, []string{"red"})), false)
		require.NoError(t, err)

		assert.Equal(t, 0, countSeries(m.claimHeaderRejectedTotal), "a value binding must never touch the claim-header counter")
		assert.Equal(t, 3, countSeries(m.claimValueRejectedTotal))
		assertCounterVecValue(t, 2, m.claimValueRejectedTotal, "groups", string(ReasonMismatch))
		assertCounterVecValue(t, 1, m.claimValueRejectedTotal, "groups", string(ReasonClaimAbsent))
		assertCounterVecValue(t, 1, m.claimValueRejectedTotal, "groups", string(ReasonMalformed))
	})

	// The reverse: a header binding next to a value binding still reports to its own counter.
	t.Run("header rejections are not metered on the claim-value counter", func(t *testing.T) {
		t.Parallel()

		m := NewPrometheusMetrics(prometheus.NewPedanticRegistry())
		h := createDummy(t, WithClaimHeaderBindings(
			mustValueBinding(t, "groups", []string{"red"}),
			mustBinding(t, "groups", "Group-ID"),
		), WithMetrics(m))

		r := request(t, token(t, roleSubscriber, []string{"red"}))
		r.Header.Set("Group-ID", "blue")

		_, err := h.authorizeAndBind(r, false)
		require.ErrorIs(t, err, ErrClaimHeaderRejected)

		assert.Equal(t, 0, countSeries(m.claimValueRejectedTotal))
		assertCounterVecValue(t, 1, m.claimHeaderRejectedTotal, "groups:Group-Id", string(ReasonMismatch))
	})
}

func TestNewHubClaimValueBindingGuards(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	resource := WithResourceIdentifier(testResourceIdentifier)

	groups := mustValueBinding(t, "groups", []string{"red"})

	// Checked in core, so Go callers are covered as well as the Caddy adapter.
	t.Run("two value bindings on the same claim are rejected", func(t *testing.T) {
		t.Parallel()

		other := mustValueBinding(t, "groups", []string{"blue"}, WithBindingRoles("publisher"))

		_, err := NewHub(ctx, resource, WithClaimHeaderBindings(groups, other), issuerWith(true, true))
		require.ErrorIs(t, err, ErrInvalidClaimHeaderBinding)
		assert.Contains(t, err.Error(), "overlapping bindings for groups")
	})

	t.Run("value bindings on different claims are accepted", func(t *testing.T) {
		t.Parallel()

		other := mustValueBinding(t, "regions", []string{"region1"})

		_, err := NewHub(ctx, resource, WithClaimHeaderBindings(groups, other), issuerWith(true, true))
		require.NoError(t, err)
	})

	t.Run("a value and a header binding on the same claim are accepted", func(t *testing.T) {
		t.Parallel()

		_, err := NewHub(ctx, resource, WithClaimHeaderBindings(groups, mustBinding(t, "groups", "Group-ID")), issuerWith(true, true))
		require.NoError(t, err)
	})

	// A claim name may contain ":", so a value binding's id can equal a header binding's.
	// They are different kinds and must not be reported as overlapping.
	t.Run("equal ids of different kinds do not overlap", func(t *testing.T) {
		t.Parallel()

		value := mustValueBinding(t, "a:B", []string{"x"})
		header := mustBinding(t, "a", "B")
		require.Equal(t, header.id(), value.id())

		_, err := NewHub(ctx, resource, WithClaimHeaderBindings(value, header), issuerWith(true, true))
		require.NoError(t, err)
	})

	t.Run("value binding without a subscriber verifier fails", func(t *testing.T) {
		t.Parallel()

		_, err := NewHub(ctx, resource, WithClaimHeaderBindings(groups), issuerWith(true, false))
		require.ErrorIs(t, err, ErrInvalidClaimHeaderBinding)
	})

	t.Run("metrics that cannot report value rejections are warned about", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer

		_, err := NewHub(ctx, resource, WithClaimHeaderBindings(groups), issuerWith(true, true),
			WithMetrics(noWriteFlushObserverMetrics{}), WithLogger(slog.New(slog.NewJSONHandler(&buf, nil))))
		require.NoError(t, err)

		assert.Contains(t, buf.String(), "does not implement ClaimValueRejectionReporter")
		assert.NotContains(t, buf.String(), "AuthorizationRejectionReporter", "no header binding is configured")
	})

	t.Run("the default NopMetrics is not warned about", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer

		_, err := NewHub(ctx, resource, WithClaimHeaderBindings(groups), issuerWith(true, true),
			WithLogger(slog.New(slog.NewJSONHandler(&buf, nil))))
		require.NoError(t, err)

		assert.NotContains(t, buf.String(), "RejectionReporter")
	})

	t.Run("a reporting metrics implementation is not warned about", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer

		_, err := NewHub(ctx, resource, WithClaimHeaderBindings(groups), issuerWith(true, true),
			WithMetrics(NewPrometheusMetrics(prometheus.NewPedanticRegistry())),
			WithLogger(slog.New(slog.NewJSONHandler(&buf, nil))))
		require.NoError(t, err)

		assert.NotContains(t, buf.String(), "RejectionReporter")
	})
}

func TestNewHubCountSubscribersRules(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	newHub := func(bindings ...ClaimHeaderBinding) error {
		_, err := NewHub(ctx, WithResourceIdentifier(testResourceIdentifier), issuerWith(true, true), WithClaimHeaderBindings(bindings...))

		return err
	}

	count := WithBindingCountSubscribers()

	t.Run("one header binding for subscribers is accepted", func(t *testing.T) {
		t.Parallel()

		require.NoError(t, newHub(mustBinding(t, "groups", "Group-ID", WithBindingRoles("subscriber"), count)))
		require.NoError(t, newHub(mustBinding(t, "groups", "Group-ID", count)))
		require.NoError(t, newHub(
			mustBinding(t, "groups", "Group-ID", count),
			mustBinding(t, "regions", "X-Region"),
		))
	})

	t.Run("two counting bindings are rejected", func(t *testing.T) {
		t.Parallel()

		err := newHub(
			mustBinding(t, "groups", "Group-ID", count),
			mustBinding(t, "regions", "X-Region", count),
		)
		require.ErrorIs(t, err, ErrInvalidClaimHeaderBinding)
		assert.Contains(t, err.Error(), "only one binding may count subscribers")
	})

	t.Run("a publisher-only binding is rejected", func(t *testing.T) {
		t.Parallel()

		err := newHub(mustBinding(t, "groups", "Group-ID", WithBindingRoles("publisher"), count))
		require.ErrorIs(t, err, ErrInvalidClaimHeaderBinding)
		assert.Contains(t, err.Error(), "must apply to subscribers")
	})

	t.Run("a value binding is rejected", func(t *testing.T) {
		t.Parallel()

		err := newHub(mustValueBinding(t, "groups", []string{"red"}, count))
		require.ErrorIs(t, err, ErrInvalidClaimHeaderBinding)
		assert.Contains(t, err.Error(), "only a header binding may count subscribers")
	})
}

// The counted binding value is the header value of the counting binding, with that binding's
// identity, and only when that binding matched.
func TestAuthorizeAndBindCounted(t *testing.T) {
	t.Parallel()

	request := func(tb testing.TB, token string, headers map[string]string) *http.Request {
		tb.Helper()

		r := httptest.NewRequest(http.MethodGet, "/", nil)
		if token != "" {
			r.Header.Set("Authorization", bearerPrefix+token)
		}

		for k, v := range headers {
			r.Header.Set(k, v)
		}

		return r
	}
	token := func(tb testing.TB, extra map[string]any) string {
		tb.Helper()

		return createDummyJWTWithExtraClaims(tb, roleSubscriber, []string{"*"}, extra)
	}

	counting := mustBinding(t, "groups", "Group-ID", WithBindingCountSubscribers())

	t.Run("a matched binding sets the counted value after raw claims are released", func(t *testing.T) {
		t.Parallel()

		r := request(t, token(t, map[string]any{"groups": []string{"red", "blue"}}), map[string]string{"Group-ID": "blue"})

		c, counted, err := boundHub(t, counting).authorizeAndBindCounted(r, false)
		require.NoError(t, err)
		require.NotNil(t, c)
		assert.Nil(t, c.rawClaims)
		assert.Equal(t, bindingValue{binding: "groups:Group-Id", value: "blue"}, counted)
	})

	t.Run("the counted value comes from the counting binding only", func(t *testing.T) {
		t.Parallel()

		r := request(t,
			token(t, map[string]any{"groups": []string{"red"}, "regions": []string{"region1"}}),
			map[string]string{"Group-ID": "red", "X-Region": "region1"})

		_, counted, err := boundHub(t, mustBinding(t, "regions", "X-Region"), counting).authorizeAndBindCounted(r, false)
		require.NoError(t, err)
		assert.Equal(t, bindingValue{binding: "groups:Group-Id", value: "red"}, counted)
	})

	t.Run("a binding that does not count sets no counted value", func(t *testing.T) {
		t.Parallel()

		r := request(t, token(t, map[string]any{"groups": []string{"red"}}), map[string]string{"Group-ID": "red"})

		_, counted, err := boundHub(t, mustBinding(t, "groups", "Group-ID")).authorizeAndBindCounted(r, false)
		require.NoError(t, err)
		assert.Empty(t, counted.value)
	})

	t.Run("a skip under on_missing allow sets no counted value", func(t *testing.T) {
		t.Parallel()

		allow := mustBinding(t, "groups", "Group-ID", WithBindingOnMissing("allow"), WithBindingCountSubscribers())

		// Header absent.
		c, counted, err := boundHub(t, allow).authorizeAndBindCounted(request(t, token(t, map[string]any{"groups": []string{"red"}}), nil), false)
		require.NoError(t, err)
		require.NotNil(t, c)
		assert.Empty(t, counted.value)

		// Claim absent, header present.
		c, counted, err = boundHub(t, allow).authorizeAndBindCounted(request(t, token(t, nil), map[string]string{"Group-ID": "red"}), false)
		require.NoError(t, err)
		require.NotNil(t, c)
		assert.Empty(t, counted.value)
	})

	t.Run("anonymous sets no counted value", func(t *testing.T) {
		t.Parallel()

		h := createAnonymousDummy(t, WithClaimHeaderBindings(counting))

		c, counted, err := h.authorizeAndBindCounted(request(t, "", map[string]string{"Group-ID": "red"}), false)
		require.NoError(t, err)
		assert.Nil(t, c)
		assert.Empty(t, counted.value)
	})

	t.Run("a rejection sets no counted value", func(t *testing.T) {
		t.Parallel()

		r := request(t, token(t, map[string]any{"groups": []string{"red"}}), map[string]string{"Group-ID": "blue"})

		c, counted, err := boundHub(t, counting).authorizeAndBindCounted(r, false)
		require.ErrorIs(t, err, ErrClaimHeaderRejected)
		assert.Nil(t, c)
		assert.Empty(t, counted.value)
	})
}

// countedCapturingMetrics records every subscriber reported connected, so a test can read
// the counted value the handler gave it.
type countedCapturingMetrics struct {
	*PrometheusMetrics

	mu        sync.Mutex
	connected []*LocalSubscriber
}

func (m *countedCapturingMetrics) SubscriberConnected(s *LocalSubscriber) {
	m.mu.Lock()
	m.connected = append(m.connected, s)
	m.mu.Unlock()

	m.PrometheusMetrics.SubscriberConnected(s)
}

func (m *countedCapturingMetrics) subscribers() []*LocalSubscriber {
	m.mu.Lock()
	defer m.mu.Unlock()

	return slices.Clone(m.connected)
}

// A subscriber authorized by the counting binding is counted under the header value while
// connected and uncounted when it disconnects. An anonymous subscriber on the same hub is not
// counted.
func TestSubscribeHandlerCountsSubscribersByBindingValue(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		reg := prometheus.NewRegistry()
		metrics := &countedCapturingMetrics{PrometheusMetrics: NewPrometheusMetrics(reg)}
		binding := mustBinding(t, "groups", "Group-ID", WithBindingRoles("subscriber"), WithBindingCountSubscribers())

		hub := hubShutdownTestHubWithOptions(t.Context(), t, 5*time.Minute, WithMetrics(metrics), WithClaimHeaderBindings(binding))
		transport, _ := hub.transport.(*LocalTransport)

		token := createDummyJWTWithExtraClaims(t, roleSubscriber, []string{"https://example.com/books/1"}, map[string]any{"groups": []string{"red", "blue"}})

		reqCtx, cancelReq := context.WithCancel(t.Context())

		go func() {
			req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?match=https://example.com/books/1", nil).WithContext(reqCtx)
			req.Header.Set("Authorization", bearerPrefix+token)
			req.Header.Set("Group-ID", "red")
			hub.SubscribeHandler(newSubscribeRecorder(), req)
		}()

		go func() {
			req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?match=https://example.com/books/1", nil).WithContext(reqCtx)
			req.Header.Set("Group-ID", "blue") // no token: anonymous
			hub.SubscribeHandler(newSubscribeRecorder(), req)
		}()

		waitSubscribers(t, transport, 2)
		synctest.Wait()

		assert.Equal(t, map[string]float64{"red": 1}, gatherBindingValues(t, reg))

		counted := map[bindingValue]bool{}

		for _, s := range metrics.subscribers() {
			counted[s.counted] = true

			if s.counted.value != "" {
				require.NotNil(t, s.Claims)
				assert.Nil(t, s.Claims.rawClaims, "the counted value must outlive the raw claims")
			}
		}

		assert.Equal(t, map[bindingValue]bool{{binding: "groups:Group-Id", value: "red"}: true, {}: true}, counted)

		cancelReq()
		synctest.Wait()

		assert.Empty(t, gatherBindingValues(t, reg))
	})
}

// Binding sets of bindingStatusHub.
const (
	setupHeaderAndValue = iota // groups ↔ Group-ID, then plan ∈ {gold}
	setupAllowAndValue         // the same, the header binding under on_missing allow
	setupTwoHeaders            // groups ↔ Group-ID, then regions ↔ Region
	setupHeaderOnly            // groups ↔ Group-ID
	setupValueOnly             // plan ∈ {gold}
)

// bindingStatusHub is a hub with the bindings of setup, all applying to every role, in their
// listed order or, when reversed, the other way round. The subscription API is enabled.
func bindingStatusHub(tb testing.TB, m Metrics, setup int, reversed bool) *Hub {
	tb.Helper()

	groups := mustBinding(tb, "groups", "Group-ID")
	plan := mustValueBinding(tb, "plan", []string{"gold"})

	var bindings []ClaimHeaderBinding

	switch setup {
	case setupHeaderAndValue:
		bindings = []ClaimHeaderBinding{groups, plan}
	case setupAllowAndValue:
		bindings = []ClaimHeaderBinding{mustBinding(tb, "groups", "Group-ID", WithBindingOnMissing("allow")), plan}
	case setupTwoHeaders:
		bindings = []ClaimHeaderBinding{groups, mustBinding(tb, "regions", "Region")}
	case setupHeaderOnly:
		bindings = []ClaimHeaderBinding{groups}
	case setupValueOnly:
		bindings = []ClaimHeaderBinding{plan}
	}

	if reversed {
		slices.Reverse(bindings)
	}

	return createDummy(tb, WithSubscriptions(), WithMetrics(m), WithClaimHeaderBindings(bindings...))
}

// The status mapping of a binding rejection, as the protocol's Error Responses section and
// RFC 6750 require it, through each real handler with real signed tokens:
//   - a valid token whose claim does not authorize the header value, whose claim holds no
//     allowed value, or that lacks the bound claim (on_missing reject) → 403
//     insufficient_scope, with a Bearer challenge;
//   - the bound header missing or malformed → 400, without a challenge;
//   - a bound claim present with an unusable type or shape → 401 invalid_token;
//   - no token or an invalid token → 401, unchanged.
//
// The answer never depends on binding order: a missing or malformed bound header answers 400
// whatever any claim holds, then a claim of unusable type answers 401 whatever the scope of
// the other bindings, and only then do scope failures answer 403. Each row also pins the one
// rejection metered: that of the first configured binding with a fault in the deciding tier.
func TestClaimBindingRejectionStatus(t *testing.T) {
	t.Parallel()

	const (
		topic           = "https://example.com/books/1"
		subscriptionAPI = defaultHubURL + subscriptionsPath
	)

	handlers := []struct {
		name  string
		role  role
		grant string
		serve func(h *Hub, token string, group, region []string) *http.Response
	}{
		{
			name: "subscribe", role: roleSubscriber, grant: topic,
			serve: func(h *Hub, token string, group, region []string) *http.Response {
				// A cancelled request lets an accepted subscription return at once.
				ctx, cancel := context.WithCancel(t.Context())
				cancel()

				req := httptest.NewRequestWithContext(ctx, http.MethodGet, defaultHubURL+"?match="+url.QueryEscape(topic), nil)
				setBindingRequest(req, token, group, region)

				w := newSubscribeRecorder()
				h.SubscribeHandler(w, req)

				return w.Result()
			},
		},
		{
			name: "publish", role: rolePublisher, grant: topic,
			serve: func(h *Hub, token string, group, region []string) *http.Response {
				form := url.Values{"topic": {topic}, "data": {"hi"}}
				req := httptest.NewRequest(http.MethodPost, defaultHubURL, strings.NewReader(form.Encode()))
				req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
				setBindingRequest(req, token, group, region)

				w := httptest.NewRecorder()
				h.PublishHandler(w, req)

				return w.Result()
			},
		},
		{
			name: "subscription API", role: roleSubscriber, grant: subscriptionAPI,
			serve: func(h *Hub, token string, group, region []string) *http.Response {
				req := httptest.NewRequest(http.MethodGet, subscriptionAPI, nil)
				setBindingRequest(req, token, group, region)

				w := httptest.NewRecorder()
				h.SubscriptionsHandler(w, req)

				return w.Result()
			},
		},
	}

	for _, hd := range handlers {
		for _, rw := range bindingStatusRows() {
			t.Run(hd.name+"/"+rw.name, func(t *testing.T) {
				t.Parallel()

				m := NewPrometheusMetrics(prometheus.NewPedanticRegistry())
				h := bindingStatusHub(t, m, rw.setup, rw.reversed)

				var token string
				if rw.claims != nil {
					token = createDummyJWTWithExtraClaims(t, hd.role, []string{hd.grant}, rw.claims)
				}

				if rw.invalid {
					token = token[:len(token)-8] + "12345678"
				}

				res := hd.serve(h, token, rw.header, rw.region)
				require.NoError(t, res.Body.Close())

				assert.Equal(t, rw.status, res.StatusCode)

				challenge := res.Header.Get("WWW-Authenticate")

				switch {
				case rw.status == http.StatusOK || rw.noChall:
					assert.Empty(t, challenge)
				case rw.code == "":
					assert.Equal(t, `Bearer resource_metadata="`+h.resourceMetadataURL+`"`, challenge)
				default:
					assert.Equal(t, `Bearer error="`+rw.code+`", resource_metadata="`+h.resourceMetadataURL+`"`, challenge)
				}

				assertBindingMetered(t, m, rw.metered)
			})
		}
	}
}

// bindingStatusRow is one row of TestClaimBindingRejectionStatus.
type bindingStatusRow struct {
	name     string
	setup    int            // binding set of bindingStatusHub
	reversed bool           // configure the bindings in reverse order
	claims   map[string]any // nil ⇒ no token at all
	invalid  bool           // corrupt the signature
	header   []string       // Group-ID values; nil ⇒ absent
	region   []string       // Region values; nil ⇒ absent
	status   int            // expected status
	code     string         // expected RFC 6750 error code; "" ⇒ none
	noChall  bool           // no WWW-Authenticate expected at all
	// metered is the rejection that was counted: "header:<reason>" (groups:Group-Id),
	// "region:<reason>" (regions:Region), "value:<reason>" (plan), or "" for none.
	metered string
}

// bindingStatusRows are the rows of TestClaimBindingRejectionStatus.
func bindingStatusRows() []bindingStatusRow {
	good := map[string]any{"groups": []string{"red"}, "plan": "gold"}
	with := func(k string, v any) map[string]any {
		m := maps.Clone(good)
		if v == nil {
			delete(m, k)
		} else {
			m[k] = v
		}

		return m
	}

	const bad = "silver" // a plan value the value binding does not allow

	objectClaim := map[string]any{"a": 1} // a claim of unusable type

	goodRegions := map[string]any{"groups": []string{"red"}, "regions": []string{"eu"}}
	withRegions := func(v any) map[string]any {
		m := maps.Clone(goodRegions)
		m["regions"] = v

		return m
	}

	return []bindingStatusRow{
		{name: "matching header and allowed value", claims: good, header: []string{"red"}, status: http.StatusOK},
		{name: "claim does not authorize the header value", claims: good, header: []string{"evil"}, status: http.StatusForbidden, code: bearerErrInsufficientScope, metered: "header:mismatch"},
		{name: "claim value not allowed", claims: with("plan", bad), header: []string{"red"}, status: http.StatusForbidden, code: bearerErrInsufficientScope, metered: "value:mismatch"},
		{name: "bound claim absent", claims: with("groups", nil), header: []string{"red"}, status: http.StatusForbidden, code: bearerErrInsufficientScope, metered: "header:claim_absent"},
		{name: "value-bound claim absent", claims: with("plan", nil), header: []string{"red"}, status: http.StatusForbidden, code: bearerErrInsufficientScope, metered: "value:claim_absent"},
		{name: "bound header missing", claims: good, header: nil, status: http.StatusBadRequest, noChall: true, metered: "header:header_absent"},
		{name: "bound header repeated", claims: good, header: []string{"red", "red"}, status: http.StatusBadRequest, noChall: true, metered: "header:malformed"},
		{name: "bound header empty", claims: good, header: []string{""}, status: http.StatusBadRequest, noChall: true, metered: "header:malformed"},
		{name: "bound claim is an object", claims: with("groups", objectClaim), header: []string{"red"}, status: http.StatusUnauthorized, code: bearerErrInvalidToken, metered: "header:malformed"},
		{name: "bound claim holds a number", claims: with("groups", []any{"red", 1}), header: []string{"red"}, status: http.StatusUnauthorized, code: bearerErrInvalidToken, metered: "header:malformed"},
		{name: "value-bound claim is a number", claims: with("plan", 42), header: []string{"red"}, status: http.StatusUnauthorized, code: bearerErrInvalidToken, metered: "value:malformed"},
		{name: "no token", claims: nil, header: []string{"red"}, status: http.StatusUnauthorized},
		{name: "invalid token", claims: good, invalid: true, header: []string{"red"}, status: http.StatusUnauthorized, code: bearerErrInvalidToken},

		// Mixed faults. A request fault wins over any claim fault, within one binding and
		// across bindings, whatever their order.
		{name: "header absent and its claim an object", claims: with("groups", objectClaim), header: nil, status: http.StatusBadRequest, noChall: true, metered: "header:header_absent"},
		{name: "header absent and value not allowed", claims: with("plan", bad), header: nil, status: http.StatusBadRequest, noChall: true, metered: "header:header_absent"},
		{name: "header absent and value not allowed, value binding first", claims: with("plan", bad), header: nil, reversed: true, status: http.StatusBadRequest, noChall: true, metered: "header:header_absent"},
		{name: "header absent and value claim a number", claims: with("plan", 42), header: nil, status: http.StatusBadRequest, noChall: true, metered: "header:header_absent"},
		{name: "header absent and value claim a number, value binding first", claims: with("plan", 42), header: nil, reversed: true, status: http.StatusBadRequest, noChall: true, metered: "header:header_absent"},

		// A claim of unusable type makes the token invalid, so it wins over a scope failure
		// of another binding, whatever their order.
		{name: "value not allowed and header claim an object", claims: map[string]any{"groups": objectClaim, "plan": bad}, header: []string{"red"}, status: http.StatusUnauthorized, code: bearerErrInvalidToken, metered: "header:malformed"},
		{name: "value not allowed and header claim an object, value binding first", claims: map[string]any{"groups": objectClaim, "plan": bad}, header: []string{"red"}, reversed: true, status: http.StatusUnauthorized, code: bearerErrInvalidToken, metered: "header:malformed"},

		// Two scope failures answer 403 in either order; the first configured binding is
		// the one metered.
		{name: "header value and plan both not allowed", claims: with("plan", bad), header: []string{"evil"}, status: http.StatusForbidden, code: bearerErrInsufficientScope, metered: "header:mismatch"},
		{name: "header value and plan both not allowed, value binding first", claims: with("plan", bad), header: []string{"evil"}, reversed: true, status: http.StatusForbidden, code: bearerErrInsufficientScope, metered: "value:mismatch"},

		// A scope failure of the header binding and a value claim of unusable type: 401.
		{name: "header value not authorized and value claim a number", claims: with("plan", 42), header: []string{"evil"}, status: http.StatusUnauthorized, code: bearerErrInvalidToken, metered: "value:malformed"},
		{name: "header value not authorized and value claim a number, value binding first", claims: with("plan", 42), header: []string{"evil"}, reversed: true, status: http.StatusUnauthorized, code: bearerErrInvalidToken, metered: "value:malformed"},

		// on_missing allow on the header binding: an absent header skips its scope check but
		// not its claim-shape check, and a malformed header still rejects.
		{name: "allow: header absent and its claim an object", setup: setupAllowAndValue, claims: with("groups", objectClaim), header: nil, status: http.StatusUnauthorized, code: bearerErrInvalidToken, metered: "header:malformed"},
		{name: "allow: header absent and a claim no header matches", setup: setupAllowAndValue, claims: with("groups", []string{"zzz"}), header: nil, status: http.StatusOK},
		{name: "allow: header repeated", setup: setupAllowAndValue, claims: good, header: []string{"red", "red"}, status: http.StatusBadRequest, noChall: true, metered: "header:malformed"},

		// Two header bindings, in both orders.
		{name: "two headers: first not authorized, second repeated", setup: setupTwoHeaders, claims: goodRegions, header: []string{"evil"}, region: []string{"eu", "eu"}, status: http.StatusBadRequest, noChall: true, metered: "region:malformed"},
		{name: "two headers: first not authorized, second repeated, reversed", setup: setupTwoHeaders, reversed: true, claims: goodRegions, header: []string{"evil"}, region: []string{"eu", "eu"}, status: http.StatusBadRequest, noChall: true, metered: "region:malformed"},
		{name: "two headers: first not authorized, second claim an object", setup: setupTwoHeaders, claims: withRegions(objectClaim), header: []string{"evil"}, region: []string{"eu"}, status: http.StatusUnauthorized, code: bearerErrInvalidToken, metered: "region:malformed"},
		{name: "two headers: first not authorized, second claim an object, reversed", setup: setupTwoHeaders, reversed: true, claims: withRegions(objectClaim), header: []string{"evil"}, region: []string{"eu"}, status: http.StatusUnauthorized, code: bearerErrInvalidToken, metered: "region:malformed"},
		// Two request faults: 400, and the first configured binding is the one metered.
		{name: "two headers: first absent, second repeated", setup: setupTwoHeaders, claims: goodRegions, header: nil, region: []string{"eu", "eu"}, status: http.StatusBadRequest, noChall: true, metered: "header:header_absent"},
		{name: "two headers: first absent, second repeated, reversed", setup: setupTwoHeaders, reversed: true, claims: goodRegions, header: nil, region: []string{"eu", "eu"}, status: http.StatusBadRequest, noChall: true, metered: "region:malformed"},

		// A single binding of each kind.
		{name: "header binding alone: header missing", setup: setupHeaderOnly, claims: good, header: nil, status: http.StatusBadRequest, noChall: true, metered: "header:header_absent"},
		{name: "value binding alone: value not allowed", setup: setupValueOnly, claims: with("plan", bad), header: nil, status: http.StatusForbidden, code: bearerErrInsufficientScope, metered: "value:mismatch"},
	}
}

// assertBindingMetered asserts that exactly the rejection metered describes (see
// bindingStatusRow) was metered, and nothing else.
func assertBindingMetered(t *testing.T, m *PrometheusMetrics, metered string) {
	t.Helper()

	kind, reason, _ := strings.Cut(metered, ":")

	headerSeries, valueSeries := 0, 0

	switch kind {
	case "header":
		headerSeries = 1

		assertCounterVecValue(t, 1, m.claimHeaderRejectedTotal, "groups:Group-Id", reason)
	case "region":
		headerSeries = 1

		assertCounterVecValue(t, 1, m.claimHeaderRejectedTotal, "regions:Region", reason)
	case "value":
		valueSeries = 1

		assertCounterVecValue(t, 1, m.claimValueRejectedTotal, "plan", reason)
	}

	assert.Equal(t, headerSeries, countSeries(m.claimHeaderRejectedTotal), "claim-header rejections metered")
	assert.Equal(t, valueSeries, countSeries(m.claimValueRejectedTotal), "claim-value rejections metered")
}

// setBindingRequest carries the token in the Authorization header ("" ⇒ none) and sets the
// Group-ID and Region values (nil ⇒ absent).
func setBindingRequest(r *http.Request, token string, groupIDs, regions []string) {
	if token != "" {
		r.Header.Set("Authorization", bearerPrefix+token)
	}

	for _, v := range groupIDs {
		r.Header.Add("Group-ID", v)
	}

	for _, v := range regions {
		r.Header.Add("Region", v)
	}
}

// A browser fetch carrying a bound header sends a CORS preflight first, so every bound header
// must be among the allowed request headers wherever CORS is installed. Exercised with a
// browser-shaped preflight through the real handler.
func TestClaimBindingCORSPreflight(t *testing.T) {
	t.Parallel()

	const base = "authorization,cache-control,last-event-id"

	withCORS := WithCORSOrigins([]string{"https://example.com"})
	group := WithClaimHeaderBindings(mustBinding(t, "groups", "Group-ID"))

	for _, tc := range []struct {
		name      string
		opts      []Option
		requested string
		// allowed is the expected Access-Control-Allow-Headers; "" ⇒ the preflight is refused.
		allowed string
	}{
		{
			name:      "a bound header is allowed",
			opts:      []Option{withCORS, group},
			requested: "authorization,group-id",
			allowed:   "authorization,group-id",
		},
		{
			name:      "every bound header is allowed",
			opts:      []Option{withCORS, WithClaimHeaderBindings(mustBinding(t, "groups", "Group-ID"), mustBinding(t, "regions", "Region"))},
			requested: "authorization,group-id,region",
			allowed:   "authorization,group-id,region",
		},
		{
			name:      "without a binding the header stays disallowed",
			opts:      []Option{withCORS},
			requested: "authorization,group-id",
		},
		{
			name:      "a value binding reads no header and adds none",
			opts:      []Option{withCORS, WithClaimHeaderBindings(mustValueBinding(t, "group-id", []string{"red"}))},
			requested: "authorization,group-id",
		},
		{
			name:      "the fixed headers stay allowed alongside a binding",
			opts:      []Option{withCORS, group},
			requested: base,
			allowed:   base,
		},
		{
			name:      "a binding on an already-allowed header is harmless",
			opts:      []Option{withCORS, WithClaimHeaderBindings(mustBinding(t, "tok", "Authorization"))},
			requested: base,
			allowed:   base,
		},
		// No cors_origins ⇒ no CORS middleware; a binding must not install one.
		{
			name:      "no CORS middleware when origins are unset",
			opts:      []Option{group},
			requested: "authorization,group-id",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := createDummy(t, tc.opts...)

			req := httptest.NewRequest(http.MethodOptions, defaultHubURL, nil)
			req.Header.Set("Origin", "https://example.com")
			req.Header.Set("Access-Control-Request-Method", http.MethodGet)
			req.Header.Set("Access-Control-Request-Headers", tc.requested)

			w := httptest.NewRecorder()
			h.ServeHTTP(w, req)

			resp := w.Result()
			require.NoError(t, resp.Body.Close())

			assert.Equal(t, tc.allowed, resp.Header.Get("Access-Control-Allow-Headers"))

			if tc.allowed == "" {
				assert.Empty(t, resp.Header.Get("Access-Control-Allow-Origin"), "a refused preflight must not allow the origin")
			} else {
				assert.Equal(t, "https://example.com", resp.Header.Get("Access-Control-Allow-Origin"))
			}
		})
	}
}
