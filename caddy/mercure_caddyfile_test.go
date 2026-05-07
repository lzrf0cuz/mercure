package caddy

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/dunglas/mercure"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestUnmarshalCaddyfileSubscriberListCacheSize covers the
// subscriber_list_cache_size directive's parser. Three cases:
//
//   - happy path: a normal positive int parses and sets the field;
//   - overflow path: a uint64 above math.MaxInt is rejected with the
//     ErrSubscriberListCacheSizeOverflow sentinel (errors.Is-checked);
//   - non-numeric path: a malformed value surfaces the underlying
//     strconv.ParseUint error via Dispenser.WrapErr.
//
// The overflow guard exists because strconv.ParseUint(_, 10, 64) returns a
// uint64; on 32-bit platforms the subsequent int(s) would silently wrap.
// Without this test the bound check at caddy/mercure.go is dead — golangci-
// lint's gosec G115 was satisfied without an exercised path.
func TestUnmarshalCaddyfileSubscriberListCacheSize(t *testing.T) {
	t.Parallel()

	overflow := strconv.FormatUint(uint64(1)<<63, 10) // 9223372036854775808 — exactly MaxInt64+1, guaranteed > math.MaxInt on every supported platform.

	cases := []struct {
		name      string
		input     string
		wantSize  *int
		wantErrIs error
		wantErr   bool
	}{
		{
			name:     "happy path",
			input:    "mercure {\n\tsubscriber_list_cache_size 1000\n}",
			wantSize: new(1000),
		},
		{
			name:      "overflow rejected",
			input:     "mercure {\n\tsubscriber_list_cache_size " + overflow + "\n}",
			wantErrIs: ErrSubscriberListCacheSizeOverflow,
			wantErr:   true,
		},
		{
			name:    "non-numeric rejected",
			input:   "mercure {\n\tsubscriber_list_cache_size not-a-number\n}",
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			d := caddyfile.NewTestDispenser(tc.input)

			var m Mercure

			err := m.UnmarshalCaddyfile(d)
			if tc.wantErr {
				require.Error(t, err)

				if tc.wantErrIs != nil {
					require.ErrorIs(t, err, tc.wantErrIs,
						"overflow must wrap %T so callers can errors.Is against the configured failure mode", tc.wantErrIs)
				}

				return
			}

			require.NoError(t, err)
			require.NotNil(t, m.SubscriberListCacheSize)
			assert.Equal(t, *tc.wantSize, *m.SubscriberListCacheSize)
		})
	}
}

// TestUnmarshalCaddyfileSubscriberOutBuffer covers the subscriber_out_buffer
// directive's parser: a positive int sets the field, a negative value is
// rejected with an explicit message, a non-numeric value surfaces the strconv
// error, and an omitted directive leaves the field nil (so the hub default
// applies).
func TestUnmarshalCaddyfileSubscriberOutBuffer(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		input    string
		wantSize *int
		wantNil  bool
		wantErr  bool
	}{
		{
			name:     "happy path",
			input:    "mercure {\n\tsubscriber_out_buffer 64\n}",
			wantSize: new(64),
		},
		{
			name:    "negative rejected",
			input:   "mercure {\n\tsubscriber_out_buffer -1\n}",
			wantErr: true,
		},
		{
			name:    "positive below the floor rejected",
			input:   "mercure {\n\tsubscriber_out_buffer 8\n}",
			wantErr: true,
		},
		{
			name:     "explicit zero means default",
			input:    "mercure {\n\tsubscriber_out_buffer 0\n}",
			wantSize: new(0),
		},
		{
			name:    "non-numeric rejected",
			input:   "mercure {\n\tsubscriber_out_buffer not-a-number\n}",
			wantErr: true,
		},
		{
			name:    "omitted leaves field nil",
			input:   "mercure {\n}",
			wantNil: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			d := caddyfile.NewTestDispenser(tc.input)

			var m Mercure

			err := m.UnmarshalCaddyfile(d)
			if tc.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			if tc.wantNil {
				assert.Nil(t, m.SubscriberOutBuffer)

				return
			}

			require.NotNil(t, m.SubscriberOutBuffer)
			assert.Equal(t, *tc.wantSize, *m.SubscriberOutBuffer)
		})
	}
}

// TestUnmarshalCaddyfilePublishTimeout covers the publish_timeout directive: a
// valid duration sets the field, a malformed one surfaces the parse error, and
// an omitted directive leaves it nil (so the timeout stays disabled).
func TestUnmarshalCaddyfilePublishTimeout(t *testing.T) {
	t.Parallel()

	t.Run("happy path", func(t *testing.T) {
		t.Parallel()

		d := caddyfile.NewTestDispenser("mercure {\n\tpublish_timeout 30s\n}")

		var m Mercure

		require.NoError(t, m.UnmarshalCaddyfile(d))
		require.NotNil(t, m.PublishTimeout)
		assert.Equal(t, caddy.Duration(30*time.Second), *m.PublishTimeout)
	})

	t.Run("malformed rejected", func(t *testing.T) {
		t.Parallel()

		d := caddyfile.NewTestDispenser("mercure {\n\tpublish_timeout not-a-duration\n}")

		var m Mercure

		require.Error(t, m.UnmarshalCaddyfile(d))
	})

	t.Run("omitted leaves field nil", func(t *testing.T) {
		t.Parallel()

		d := caddyfile.NewTestDispenser("mercure {\n}")

		var m Mercure

		require.NoError(t, m.UnmarshalCaddyfile(d))
		assert.Nil(t, m.PublishTimeout)
	})
}

// TestProvisionMissingSubscriberListCacheSizeCtxKey locks the sentinel
// contract for ErrSubscriberListCacheSizeMissing on the Local transport.
// Bolt has the same wrapping shape; one transport's Provision is enough to
// pin the cross-package errors.Is reachability — if a refactor ever
// switches the wrap from %w to %v, this test fails.
//
// Constructing a bare caddy.Context (no WithValue chain) is the smallest
// way to reach the missing-key path without spinning up a full Caddy server.
func TestProvisionMissingSubscriberListCacheSizeCtxKey(t *testing.T) {
	t.Parallel()

	// caddy.Context's exported fields are constructible from outside the
	// package; the WithValue path is what would ordinarily attach the cache
	// size. Without that, ctx.Value returns nil and Provision must reject.
	ctx, cancel := caddy.NewContext(caddy.Context{Context: context.Background()})
	t.Cleanup(cancel)

	l := &Local{}

	err := l.Provision(ctx)
	require.Error(t, err, "Provision must reject when subscriber-list cache size is absent from context")
	require.ErrorIs(t, err, ErrSubscriberListCacheSizeMissing,
		"sentinel must be reachable via errors.Is so tests and operators can branch on the wiring failure mode")
}

// TestCaddyContextWithValueLosesMetricsRegistry locks the v2.11.3 framework
// bug that the Mercure.Provision defensive-capture comment cites — pinning
// it as a positive control for the workaround. caddy.Context.WithValue
// returns a new caddy.Context wrapping the original context.Context but
// does NOT copy the non-exported metricsRegistry field, so derived
// contexts return nil from GetMetricsRegistry.
//
// Without this regression test, the load-bearing capture at
// caddy/mercure.go:204-205 has no enforcement: a refactor that moved the
// `metricsRegistry := ctx.GetMetricsRegistry()` line below any WithValue
// call would silently detach transport metrics and produce a healthy module
// whose /metrics scrapes find no mercure_* series.
//
// If this test ever starts failing in the "after WithValue" branch (i.e.
// `registryAfter` becomes non-nil), the framework bug has been fixed upstream
// and the defensive capture in Provision can be removed. The bug source
// lives in caddyserver/caddy/v2 Context.WithValue (modules/caddy/context.go
// at the time of writing) — git blame on the upstream file is the path of
// least resistance to find the fix commit when this assertion flips.
func TestCaddyContextWithValueLosesMetricsRegistry(t *testing.T) {
	t.Parallel()

	ctx, cancel := caddy.NewContext(caddy.Context{Context: context.Background()})
	t.Cleanup(cancel)

	registryBefore := ctx.GetMetricsRegistry()
	require.NotNil(t, registryBefore,
		"baseline: a freshly-constructed caddy.Context must expose a non-nil metricsRegistry — otherwise the framework's lifecycle is broken and the subsequent Nil-after-WithValue assertion is vacuous")

	// Type-key chosen to match Provision's actual WithValue pattern.
	type sentinelKey struct{}

	derived := ctx.WithValue(sentinelKey{}, "any-value")

	require.NotNil(t, derived.Value(sentinelKey{}),
		"baseline: ctx.WithValue must propagate context.Value lookups for the supplied key — otherwise the derived context is broken")

	registryAfter := derived.GetMetricsRegistry()
	assert.Nil(t, registryAfter,
		"caddy.Context.WithValue v2.11.3 does NOT copy the metricsRegistry field — if this assertion ever fails the framework bug is fixed and the defensive capture in Provision (caddy/mercure.go:204) can be removed")
}

// Compile-time anchors for the sentinel errors locked by this test file.
// These force the unexported sentinel to keep its symbol (so removing it
// triggers a build break here, not a silent loss of pool-mismatch
// reporting), and pin the exported sentinels at package scope so they
// remain reachable through errors.Is at every call site.
var (
	_       = errTransportPoolDestructorMismatch
	_ error = ErrSubscriberListCacheSizeMissing
	_ error = ErrSubscriberListCacheSizeOverflow
)

// Compile-time assertion that *mercure.LocalTransport satisfies the
// mercure.Transport interface — this submodule's Local{} wraps it.
var _ mercure.Transport = (*mercure.LocalTransport)(nil)
