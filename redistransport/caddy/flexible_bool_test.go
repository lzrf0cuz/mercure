package caddy

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParseFlexibleBool covers the Caddyfile-idiomatic boolean spellings
// added on top of strconv.ParseBool's default set. README claims operators
// can write `tls on` / `tls yes` / `tls off` / `tls no` — without this
// helper, those would fail with the bare strconv error, breaking the
// documented contract.
func TestParseFlexibleBool(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in        string
		want      bool
		wantError bool
	}{
		// Caddyfile-idiomatic.
		{"on", true, false},
		{"On", true, false},
		{"ON", true, false},
		{"off", false, false},
		{"Off", false, false},
		{"OFF", false, false},
		{"yes", true, false},
		{"Yes", true, false},
		{"YES", true, false},
		{"no", false, false},
		{"No", false, false},
		{"NO", false, false},
		// strconv.ParseBool defaults — preserved.
		{"true", true, false},
		{"false", false, false},
		{"True", true, false},
		{"False", false, false},
		{"TRUE", true, false},
		{"FALSE", false, false},
		{"1", true, false},
		{"0", false, false},
		{"t", true, false},
		{"f", false, false},
		{"T", true, false},
		{"F", false, false},
		// Garbage — error.
		{"", false, true},
		{"maybe", false, true},
		{"2", false, true},
		{"enabled", false, true}, // deliberate non-acceptance: forces a decision next time
		{"on ", false, true},     // strict: no whitespace tolerance (matches strconv's behavior)
	}
	for _, tc := range tests {
		t.Run("input="+tc.in, func(t *testing.T) {
			t.Parallel()

			got, err := parseFlexibleBool(tc.in)
			if tc.wantError {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}
