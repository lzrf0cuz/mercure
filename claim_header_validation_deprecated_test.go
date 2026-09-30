//go:build deprecated_claim

package mercure

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"testing"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The legacy mercure claims are declared only in deprecated_claim builds; their unbindable
// constants must match the json tags there (see TestReservedClaimsMatchStructTags).
func TestReservedMercureClaimsMatchStructTags(t *testing.T) {
	t.Parallel()

	typ := reflect.TypeFor[deprecatedMercureClaims]()

	assert.Equal(t, reservedMercureClaim, jsonTagName(t, typ, "Mercure"))
	assert.Equal(t, reservedMercureNamespacedClaim, jsonTagName(t, typ, "MercureNamespaced"))
}

// In compatibility mode the deprecated "authorization" query parameter is one more
// credential carrier, and bindings apply to it as to the others. That carrier accepts
// only 0.x tokens, so the test token carries the legacy mercure claim.
func TestAuthorizeAndBindLegacyQueryParam(t *testing.T) {
	t.Parallel()

	h := createDummy(t, WithProtocolVersionCompatibility(8), WithClaimHeaderBindings(mustBinding(t, "groups", "Group-ID")))
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"mercure": map[string]any{"subscribe": []string{"*"}},
		"groups":  []string{"red"},
	}).SignedString([]byte("subscriber"))
	require.NoError(t, err)

	request := func(group string) *http.Request {
		r := httptest.NewRequest(http.MethodGet, "/?authorization="+url.QueryEscape(token), nil)
		r.Header.Set("Group-ID", group)

		return r
	}

	c, err := h.authorizeAndBind(request("red"), noSpan, false)
	require.NoError(t, err)
	require.NotNil(t, c)

	_, err = h.authorizeAndBind(request("blue"), noSpan, false)
	require.ErrorIs(t, err, ErrClaimHeaderRejected)
}
