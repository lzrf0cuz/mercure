package redistransport

import (
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompareStreamIDs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		a, b string
		want bool
	}{
		{name: "a greater ms", a: "1700000000001-0", b: "1700000000000-0", want: true},
		{name: "b greater ms", a: "1700000000000-0", b: "1700000000001-0", want: false},
		{name: "equal ms, a greater seq", a: "1700000000000-5", b: "1700000000000-3", want: true},
		{name: "equal ms, b greater seq", a: "1700000000000-3", b: "1700000000000-5", want: false},
		{name: "equal", a: "1700000000000-5", b: "1700000000000-5", want: false},
		{name: "sequence 10 vs 2 (numeric, not lexicographic)", a: "1700000000000-10", b: "1700000000000-2", want: true},
		{name: "large sequence numbers", a: "1700000000000-999999", b: "1700000000000-999998", want: true},
		{name: "0-0 vs 0-0", a: "0-0", b: "0-0", want: false},
		{name: "1-0 vs 0-0", a: "1-0", b: "0-0", want: true},
		{name: "missing hyphen a", a: "1700000000000", b: "1700000000000-1", want: false},
		{name: "missing hyphen b", a: "1700000000000-1", b: "1700000000000", want: true},
		{name: "both missing hyphen", a: "1700000000001", b: "1700000000000", want: true},
		{name: "malformed returns false", a: "abc", b: "def", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := compareStreamIDs(tt.a, tt.b)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestNextStreamID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		id   string
		want string
	}{
		{name: "simple increment", id: "1700000000000-5", want: "1700000000000-6"},
		{name: "from zero", id: "1700000000000-0", want: "1700000000000-1"},
		{name: "large sequence", id: "1700000000000-999998", want: "1700000000000-999999"},
		{name: "missing hyphen", id: "1700000000000", want: "1700000000000-1"},
		{
			name: "sequence overflow",
			id:   "1700000000000-18446744073709551615",
			want: "1700000000001-0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := nextStreamID(tt.id)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestNextStreamIDMaxUint64(t *testing.T) {
	t.Parallel()

	// Verify MaxUint64 is correctly detected
	maxSeq := "1700000000000-" + formatUint64(math.MaxUint64)
	result := nextStreamID(maxSeq)
	assert.Equal(t, "1700000000001-0", result)
}

func formatUint64(v uint64) string {
	return strconv.FormatUint(v, 10)
}

func TestUUIDv7ToStreamID(t *testing.T) {
	t.Parallel()

	// Generate a real UUIDv7 and verify the stream ID is reasonable
	// UUIDv7 "0195278d-8fca-7c11-bfab-deadbeef1234" has a known timestamp
	streamID, ts, err := uuidv7ToStreamID("urn:uuid:0195278d-8fca-7c11-bfab-deadbeef1234", defaultOptions().clockSkewMarginMs())
	require.NoError(t, err)
	assert.Positive(t, ts)
	assert.NotEmpty(t, streamID)
	assert.Contains(t, streamID, "-0")

	// The stream ID should represent (timestamp - clockSkewMargin)
	expectedSeek := ts - defaultOptions().clockSkewMarginMs()
	expectedStreamID := strconv.FormatUint(expectedSeek, 10) + "-0"
	assert.Equal(t, expectedStreamID, streamID)
}

func TestUUIDv7ToStreamIDWithoutPrefix(t *testing.T) {
	t.Parallel()

	// Should work without urn:uuid: prefix
	streamID, ts, err := uuidv7ToStreamID("0195278d-8fca-7c11-bfab-deadbeef1234", defaultOptions().clockSkewMarginMs())
	require.NoError(t, err)
	assert.Positive(t, ts)
	assert.NotEmpty(t, streamID)
}

func TestUUIDv7ToStreamIDInvalid(t *testing.T) {
	t.Parallel()

	_, _, err := uuidv7ToStreamID("not-a-uuid", defaultOptions().clockSkewMarginMs())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid UUIDv7")
}

func TestUUIDv7ToStreamIDUnderflowProtection(t *testing.T) {
	t.Parallel()

	// A UUIDv7 with a very small timestamp (< clockSkewMargin) should not underflow
	// Use a UUID with timestamp = 0 (the first few bytes are all zeros)
	// We'll construct one manually: "00000000-0000-7000-8000-000000000000"
	streamID, ts, err := uuidv7ToStreamID("00000000-0000-7000-8000-000000000000", defaultOptions().clockSkewMarginMs())
	require.NoError(t, err)
	assert.Equal(t, uint64(0), ts)
	assert.Equal(t, "0-0", streamID) // Should not underflow to MaxUint64
}

func TestUUIDv7ToStreamIDZeroMargin(t *testing.T) {
	t.Parallel()

	streamID, ts, err := uuidv7ToStreamID("urn:uuid:0195278d-8fca-7c11-bfab-deadbeef1234", 0)
	require.NoError(t, err)
	assert.Positive(t, ts)
	// With zero margin, stream ID should use exact timestamp
	expected := strconv.FormatUint(ts, 10) + "-0"
	assert.Equal(t, expected, streamID)
}

func TestKeyHelper(t *testing.T) {
	t.Parallel()

	tests := []struct {
		stream string
		suffix string
		want   string
	}{
		{stream: "mercure", suffix: "", want: "{mercure}"},
		{stream: "mercure", suffix: ":lastEventID", want: "{mercure}:lastEventID"},
		{stream: "mercure", suffix: ":presence:node-abc", want: "{mercure}:presence:node-abc"},
		{stream: "custom", suffix: "", want: "{custom}"},
		{stream: "my-app", suffix: ":lastEventID", want: "{my-app}:lastEventID"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			t.Parallel()

			got := key(tt.stream, tt.suffix)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestParseServerInfo(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		info     string
		wantType string
		wantVer  string
	}{
		{
			name:     "redis 7",
			info:     "# Server\r\nredis_version:7.2.4\r\nos:Linux\r\n",
			wantType: "redis",
			wantVer:  "7.2.4",
		},
		{
			name:     "valkey 8 (only valkey_version)",
			info:     "# Server\r\nvalkey_version:8.1.6\r\ngcc_version:15.2.0\r\n",
			wantType: "valkey",
			wantVer:  "8.1.6",
		},
		{
			name:     "valkey wins when both present",
			info:     "redis_version:7.2.4\nvalkey_version:8.0.0\n",
			wantType: "valkey",
			wantVer:  "8.0.0",
		},
		{
			name:     "redis only",
			info:     "redis_version:6.2.14\n",
			wantType: "redis",
			wantVer:  "6.2.14",
		},
		{
			name:     "empty info",
			info:     "",
			wantType: "unknown",
			wantVer:  "",
		},
		{
			name:     "no version field",
			info:     "# Server\r\nos:Linux\r\n",
			wantType: "unknown",
			wantVer:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			si := parseServerInfo(tt.info)
			// Cast to string keeps a literal-bound canary: the expected
			// values stay as wire-format string literals so a typo in
			// serverTypeRedis / serverTypeValkey / serverTypeUnknown
			// breaks this test.
			assert.Equal(t, tt.wantType, string(si.serverType))
			assert.Equal(t, tt.wantVer, si.version)
		})
	}
}

func TestIsVersionAtLeast(t *testing.T) {
	t.Parallel()

	tests := []struct {
		version string
		major   int
		minor   int
		want    bool
	}{
		{"7.2.4", 6, 2, true},
		{"6.2.0", 6, 2, true},
		{"6.1.9", 6, 2, false},
		{"5.0.0", 6, 2, false},
		{"8.1.6", 6, 2, true}, // Valkey 8
		{"8.0.0", 7, 0, true}, // Valkey >= 7
		{"", 6, 2, false},
		{"6", 6, 2, false}, // single component
	}

	for _, tt := range tests {
		t.Run(tt.version, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, isVersionAtLeast(tt.version, tt.major, tt.minor))
		})
	}
}
