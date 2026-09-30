package redistransport

import (
	"math"
	"strconv"
	"strings"
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

	// A fixed UUIDv7 maps to its timestamp minus the clock-skew margin.
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

	// A UUIDv7 with timestamp 0, below the clock-skew margin, must clamp to 0-0
	// instead of underflowing.
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

// redisHashTag returns the Redis Cluster hash tag of k: the text between the
// first "{" and the first "}" after it. It returns "" when that text is empty
// or missing, in which case Redis hashes the whole key.
func redisHashTag(k string) string {
	open := strings.IndexByte(k, '{')
	if open < 0 {
		return ""
	}

	end := strings.IndexByte(k[open+1:], '}')
	if end <= 0 {
		return ""
	}

	return k[open+1 : open+1+end]
}

// TestStreamNameHashTag pins that validateOptions rejects a stream name exactly
// when key() would give its keys no hash tag. Every key of an accepted name
// carries the same tag, so the stream, lastEventID and presence keys share one
// Redis Cluster slot.
func TestStreamNameHashTag(t *testing.T) {
	t.Parallel()

	suffixes := []string{"", ":lastEventID", ":presence:node-abc", ":presence:*", ":presence:}x", ":presence:{y}"}
	names := []string{
		// Accepted.
		"mercure", "my-app", "a}b", "a}", "a}}b", "a{b", "{a", "{a}", "a{b}c", "{}",
		// Rejected.
		"}", "}mercure", "}}", "}{a}",
	}

	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			o := defaultOptions()
			WithStreamName(name)(o)

			err := validateOptions(o)
			rejected := err != nil

			if rejected {
				require.ErrorIs(t, err, ErrInvalidOptions)
				assert.Contains(t, err.Error(), "WithStreamName")
			}

			want := redisHashTag(key(name, ""))

			for _, suffix := range suffixes {
				k := key(name, suffix)
				tag := redisHashTag(k)

				assert.Equal(t, rejected, tag == "", "key %q: rejected=%v, tag %q", k, rejected, tag)

				if !rejected {
					assert.Equal(t, want, tag, "key %q", k)
				}
			}
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
			// Compare against wire-format string literals, so a typo in a
			// serverType* constant fails this test.
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
