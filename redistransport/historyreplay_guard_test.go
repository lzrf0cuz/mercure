package redistransport

import (
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
)

// TestStreamIDMilliseconds locks the ms-prefix parsing contract used by
// sampleHistoryWindow's gauge computation. (The future-ID guard predicate
// no longer reads stream IDs — it compares the requested UUIDv7's encoded
// ms against wall-clock now() — so this contract is single-consumer now.
// Keeping the test broad protects against silent regressions in
// sampleHistoryWindow.)
func TestStreamIDMilliseconds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		in     string
		wantMs int64
		wantOK bool
	}{
		{"normal", "1779055952268-0", 1_779_055_952_268, true},
		{"seq nonzero", "1779055952268-7", 1_779_055_952_268, true},
		{"large seq", "1779055952268-999999", 1_779_055_952_268, true},
		{"zero ms", "0-0", 0, true},
		{"no dash", "1779055952268", 0, false},
		{"leading dash", "-1779055952268-0", 0, false},
		{"empty", "", 0, false},
		{"non-numeric prefix", "abc-0", 0, false},
		{"empty prefix", "-0", 0, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			gotMs, gotOK := streamIDMilliseconds(tc.in)
			assert.Equal(t, tc.wantOK, gotOK, "ok mismatch for %q", tc.in)

			if tc.wantOK {
				assert.Equal(t, tc.wantMs, gotMs, "ms mismatch for %q", tc.in)
			}
		})
	}
}

// TestIsRequestedIDFutureDated covers the future-ID short-circuit branch
// of history replay. The predicate compares the requested UUIDv7's
// encoded wall-clock ms against an injected `nowMs` (the hub's wall
// clock at AddSubscriber time). Critically: it must NOT trip on already-
// published events whose UUIDv7 wall-clock time is at or before now —
// even if the listener has not yet caught up to them. The earlier
// implementation compared against `lastDispatchedStreamID` which
// conflated listener lag with wall-clock-future and could drop
// legitimate replays.
func TestIsRequestedIDFutureDated(t *testing.T) {
	t.Parallel()

	// Build a known-good UUIDv7 by hand so the test does not depend on
	// time-of-day. Format: <48-bit big-endian ms timestamp><12-bit ver/rand>...
	// uuidv7ToStreamID accepts the standard urn:uuid: prefix.
	const (
		// Arbitrary deterministic timestamps; pastMs is 200s earlier
		// than futureMs so the skew-margin branches stay distinguishable.
		futureMs = int64(1_779_055_200_000)
		pastMs   = int64(1_779_055_000_000)
	)

	// Synthesize a UUIDv7 string with the timestamp prefix. UUIDv7 layout:
	// 48-bit timestamp ms in the first 12 hex chars, then 4-bit version "7",
	// random bits. Minimal valid UUIDv7 suffices — parseable by uuidv7ToStreamID.
	makeUUIDv7 := func(ms int64) string {
		// 48-bit ms in 12 hex digits.
		hexMs := zeroPadHex(ms, 12)
		// version 7, three random hex digits, two more random hex digits,
		// 12-hex random suffix. Use zeros for determinism.
		return "urn:uuid:" + hexMs[:8] + "-" + hexMs[8:12] + "-7000-8000-000000000000"
	}

	futureID := makeUUIDv7(futureMs)
	pastID := makeUUIDv7(pastMs)

	tests := []struct {
		name        string
		reqID       string
		nowMs       int64
		clockSkewMs uint64
		want        bool
	}{
		{
			name:  "future ID past wall clock (no skew) -> guard fires",
			reqID: futureID,
			nowMs: pastMs, // hub clock is at pastMs; requested ID claims futureMs
			want:  true,
		},
		{
			name:        "future ID within skew margin -> guard does NOT fire",
			reqID:       futureID,
			nowMs:       pastMs,
			clockSkewMs: uint64(futureMs - pastMs + 1), // skew > delta
			want:        false,
		},
		{
			name:  "past ID older than wall clock -> guard does NOT fire",
			reqID: pastID,
			nowMs: futureMs,
			want:  false,
		},
		{
			name: "listener-lag regression: requested ID matches wall clock now -> guard does NOT fire " +
				"(prior bug: would fire when toStreamID lagged behind requested ID)",
			reqID: futureID,
			nowMs: futureMs, // wall clock has caught up; published event is current
			want:  false,
		},
		{
			name:  "empty requestedID -> guard does NOT fire",
			reqID: "",
			nowMs: pastMs,
			want:  false,
		},
		{
			name:  "earliest sentinel -> guard does NOT fire",
			reqID: "earliest",
			nowMs: pastMs,
			want:  false,
		},
		{
			name:  "non-UUIDv7 requestedID -> guard does NOT fire (fallthrough to legacy path)",
			reqID: "custom-event-id-not-a-uuid",
			nowMs: pastMs,
			want:  false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := isRequestedIDFutureDated(tc.reqID, tc.nowMs, tc.clockSkewMs)
			assert.Equal(t, tc.want, got)
		})
	}
}

// TestTruncateEventIDForObservability locks the bound applied to
// client-controlled Last-Event-ID values before they reach logs and span
// attributes. Last-Event-ID is bounded at HTTP ingress only by the server's
// max-header size (~1 MiB), so a single oversized header must not bloat a log
// line or trace. Truncation is rune-safe so a multi-byte value can't be
// sliced into invalid UTF-8.
func TestTruncateEventIDForObservability(t *testing.T) {
	t.Parallel()

	t.Run("short value passes through unchanged", func(t *testing.T) {
		t.Parallel()

		const id = "urn:uuid:0190a1b2-c3d4-7000-8000-000000000000" // 45 chars < cap
		assert.Equal(t, id, truncateEventIDForObservability(id))
	})

	t.Run("value at exactly the cap is unchanged", func(t *testing.T) {
		t.Parallel()

		id := strings.Repeat("a", maxObservedEventIDLen)
		assert.Equal(t, id, truncateEventIDForObservability(id))
	})

	t.Run("oversized ASCII value is capped", func(t *testing.T) {
		t.Parallel()

		got := truncateEventIDForObservability(strings.Repeat("a", maxObservedEventIDLen+50))
		assert.Equal(t, strings.Repeat("a", maxObservedEventIDLen)+"…(truncated)", got)
		assert.True(t, utf8.ValidString(got), "truncated output must be valid UTF-8")
	})

	t.Run("oversized multi-byte value truncates on a rune boundary", func(t *testing.T) {
		t.Parallel()

		// "é" is a 2-byte rune; a naive byte-slice cap could split it.
		// Build a string longer than the rune cap and assert the result
		// stays valid UTF-8. (Latin-1 "é" is not on gosmopolitan's
		// East-Asian watch list, so it lints clean.)
		got := truncateEventIDForObservability(strings.Repeat("é", maxObservedEventIDLen+10))
		assert.True(t, utf8.ValidString(got), "rune-safe truncation must not split a multi-byte rune")
		assert.Contains(t, got, "…(truncated)")
	})
}

// zeroPadHex formats n as lower-case hex padded with leading zeros to
// width chars; truncates the tail on overflow.
func zeroPadHex(n int64, width int) string {
	s := strconv.FormatInt(n, 16)
	for len(s) < width {
		s = "0" + s
	}

	if len(s) > width {
		s = s[len(s)-width:]
	}

	return s
}
