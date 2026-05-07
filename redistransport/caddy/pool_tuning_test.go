package caddy

import (
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestUnmarshalCaddyfile_PoolTuningDirectives parses the seven
// pool/timeout/retry directives and asserts each maps to its struct
// field. Lower-level than TestUnmarshalCaddyfileAllDirectives because
// this set has tighter parse contracts (integer vs duration) and is
// the operator surface for go-redis client tuning.
func TestUnmarshalCaddyfile_PoolTuningDirectives(t *testing.T) {
	t.Parallel()

	input := `redis {
		url redis://localhost:6379
		pool_size 50
		min_idle_conns 5
		pool_timeout 4s
		dial_timeout 3s
		read_timeout 2s
		write_timeout 2s
		max_retries 5
		presence_detail_byte_threshold 65536
	}`

	d := caddyfile.NewTestDispenser(input)

	r := &Redis{}
	require.NoError(t, r.UnmarshalCaddyfile(d))

	assert.Equal(t, 50, r.PoolSize)
	assert.Equal(t, 5, r.MinIdleConns)
	assert.Equal(t, "4s", r.PoolTimeout)
	assert.Equal(t, "3s", r.DialTimeout)
	assert.Equal(t, "2s", r.ReadTimeout)
	assert.Equal(t, "2s", r.WriteTimeout)
	assert.Equal(t, 5, r.MaxRetries)
	require.NotNil(t, r.PresenceDetailByteThreshold,
		"explicit presence_detail_byte_threshold must populate the pointer")
	assert.Equal(t, int64(65536), *r.PresenceDetailByteThreshold)
}

// TestUnmarshalCaddyfile_PresenceDetailByteThresholdZero locks the
// pointer-tri-state contract: explicit 0 sets a non-nil pointer to 0
// (disables the byte budget) so an operator can opt out, distinct
// from "directive omitted" (nil → Go-level default applies).
func TestUnmarshalCaddyfile_PresenceDetailByteThresholdZero(t *testing.T) {
	t.Parallel()

	input := `redis {
		url redis://localhost:6379
		presence_detail_byte_threshold 0
	}`

	d := caddyfile.NewTestDispenser(input)

	r := &Redis{}
	require.NoError(t, r.UnmarshalCaddyfile(d))
	require.NotNil(t, r.PresenceDetailByteThreshold,
		"explicit 0 must allocate the pointer (distinguishable from omission)")
	assert.Equal(t, int64(0), *r.PresenceDetailByteThreshold,
		"explicit 0 is the documented opt-out: byte budget disabled")
}

// TestApplyClientTuning_MapsToUniversalOptions exercises the
// translation from Redis struct fields to go-redis
// redis.UniversalOptions. Directives left at zero / "" must NOT touch
// the corresponding UniversalOptions field, so go-redis library
// defaults survive.
func TestApplyClientTuning_MapsToUniversalOptions(t *testing.T) {
	t.Parallel()

	r := &Redis{
		URL:          "redis://localhost:6379",
		PoolSize:     7,
		MinIdleConns: 2,
		PoolTimeout:  "1500ms",
		DialTimeout:  "800ms",
		ReadTimeout:  "250ms",
		WriteTimeout: "300ms",
		MaxRetries:   4,
	}

	uo, _, err := r.buildClientOptions()
	require.NoError(t, err)

	assert.Equal(t, 7, uo.PoolSize)
	assert.Equal(t, 2, uo.MinIdleConns)
	assert.Equal(t, 1500*time.Millisecond, uo.PoolTimeout)
	assert.Equal(t, 800*time.Millisecond, uo.DialTimeout)
	assert.Equal(t, 250*time.Millisecond, uo.ReadTimeout)
	assert.Equal(t, 300*time.Millisecond, uo.WriteTimeout)
	assert.Equal(t, 4, uo.MaxRetries)
}

// TestApplyClientTuning_LibraryDefaultsPreserved — every unset
// directive must leave the corresponding UniversalOptions field at
// its zero value, which go-redis interprets as "library default."
// Without this test, a future refactor that eagerly sets fields to
// zero (or to local "defaults") would silently override go-redis's
// own defaults.
func TestApplyClientTuning_LibraryDefaultsPreserved(t *testing.T) {
	t.Parallel()

	r := &Redis{URL: "redis://localhost:6379"}

	uo, _, err := r.buildClientOptions()
	require.NoError(t, err)

	assert.Equal(t, 0, uo.PoolSize, "unset pool_size must leave PoolSize at zero (library default)")
	assert.Equal(t, 0, uo.MinIdleConns)
	assert.Equal(t, time.Duration(0), uo.PoolTimeout)
	assert.Equal(t, time.Duration(0), uo.DialTimeout)
	assert.Equal(t, time.Duration(0), uo.ReadTimeout)
	assert.Equal(t, time.Duration(0), uo.WriteTimeout)
	assert.Equal(t, 0, uo.MaxRetries)
}

// TestApplyClientTuning_InvalidDuration — malformed duration value in
// any timeout directive surfaces a clear "invalid <name>" error
// rather than silently being treated as zero.
func TestApplyClientTuning_InvalidDuration(t *testing.T) {
	t.Parallel()

	r := &Redis{URL: "redis://localhost:6379", DialTimeout: "not-a-duration"}

	_, _, err := r.buildClientOptions()
	require.Error(t, err)

	assert.Contains(t, err.Error(), "dial_timeout",
		"error should name the offending directive so operators can locate it")
}

// TestApplyClientTuning_IntegerKnobsUnchangedOnError asserts the
// partial-mutation contract: when validation fails, integer fields on
// the *uo passed in must remain at their caller-supplied values.
// applyClientTuning defers integer-field assignment until after
// errors.Join returns nil, so an invalid Caddyfile cannot leak a
// half-tuned UniversalOptions to downstream code.
func TestApplyClientTuning_IntegerKnobsUnchangedOnError(t *testing.T) {
	t.Parallel()

	r := &Redis{
		URL:          "redis://localhost:6379",
		PoolSize:     -1, // forces errors.Join return
		MinIdleConns: 7,  // would normally apply
		MaxRetries:   5,  // would normally apply
	}

	uo := &redis.UniversalOptions{
		PoolSize:     999, // sentinel — must survive the failed Provision
		MinIdleConns: 888,
		MaxRetries:   777,
	}

	err := r.applyClientTuning(uo)
	require.Error(t, err, "applyClientTuning must error on pool_size=-1")

	assert.Equal(t, 999, uo.PoolSize,
		"PoolSize must NOT be applied when validation fails")
	assert.Equal(t, 888, uo.MinIdleConns,
		"MinIdleConns must NOT be applied when validation fails")
	assert.Equal(t, 777, uo.MaxRetries,
		"MaxRetries must NOT be applied when validation fails")
}

// TestApplyClientTuning_JoinsMultipleErrors asserts that when multiple
// directives fail validation simultaneously, applyClientTuning surfaces
// every failure in a single joined error so the operator can fix them
// in one round-trip instead of N.
func TestApplyClientTuning_JoinsMultipleErrors(t *testing.T) {
	t.Parallel()

	r := &Redis{
		URL:          "redis://localhost:6379",
		PoolSize:     -1,
		MinIdleConns: -2,
		MaxRetries:   -5, // -1 is the documented sentinel; -5 is a typo.
		DialTimeout:  "-1s",
		ReadTimeout:  "garbage",
	}

	_, _, err := r.buildClientOptions()
	require.Error(t, err)

	for _, want := range []string{
		"pool_size=-1",
		"min_idle_conns=-2",
		"max_retries=-5",
		"dial_timeout=-1s",
		"read_timeout",
	} {
		assert.Contains(t, err.Error(), want,
			"joined error must name every failing directive (missing %q)", want)
	}
}
