package caddy

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCaddyModuleInfo(t *testing.T) {
	t.Parallel()

	r := Redis{}
	info := r.CaddyModule()
	assert.Equal(t, "http.handlers.mercure.redis", string(info.ID))
	assert.NotNil(t, info.New)

	// New() should return a *Redis.
	m := info.New()
	_, ok := m.(*Redis)
	assert.True(t, ok)
}

func TestUnmarshalCaddyfileMinimal(t *testing.T) {
	t.Parallel()

	input := `redis {
		url redis://localhost:6379
	}`

	d := caddyfile.NewTestDispenser(input)
	r := &Redis{}
	err := r.UnmarshalCaddyfile(d)
	require.NoError(t, err)

	assert.Equal(t, "redis://localhost:6379", r.URL)
	assert.Empty(t, r.Stream, "stream should not be set when omitted")
	assert.Zero(t, r.MaxLength, "max_length should not be set when omitted")
}

func TestUnmarshalCaddyfileAllDirectives(t *testing.T) {
	t.Parallel()

	input := `redis {
		url redis://localhost:6379
		stream mystream
		max_length 5000
		encoding msgpack
		presence_ttl 120s
		presence_interval 60s
		zombie_gc_interval 10m
		clock_skew_margin 3s
		event_ttl 24h
		cleanup_interval 5m
		health_interval 15s
		health_threshold 5
		xread_count 200
		xread_block 2s
		dispatch_shards 4
		subscriber_rate_limit 1000
		subscriber_rate_burst 3000
		history_replay_concurrency 25
		presence_detail_threshold 5000
	}`

	d := caddyfile.NewTestDispenser(input)
	r := &Redis{}
	err := r.UnmarshalCaddyfile(d)
	require.NoError(t, err)

	assert.Equal(t, "redis://localhost:6379", r.URL)
	assert.Equal(t, "mystream", r.Stream)
	assert.Equal(t, int64(5000), r.MaxLength)
	assert.Equal(t, "msgpack", r.Encoding)
	assert.Equal(t, "120s", r.PresenceTTL)
	assert.Equal(t, "60s", r.PresenceInterval)
	assert.Equal(t, "10m", r.ZombieGCInterval)
	assert.Equal(t, "3s", r.ClockSkewMargin)
	assert.Equal(t, "24h", r.EventTTL)
	assert.Equal(t, "5m", r.CleanupInterval)
	assert.Equal(t, "15s", r.HealthInterval)
	assert.Equal(t, 5, r.HealthThreshold)
	assert.Equal(t, int64(200), r.XReadCount)
	assert.Equal(t, "2s", r.XReadBlock)
	require.NotNil(t, r.DispatchShards)
	assert.Equal(t, 4, *r.DispatchShards)
	assert.InDelta(t, float64(1000), r.SubscriberRateLimit, 0.001)
	assert.Equal(t, 3000, r.SubscriberRateBurst)
	assert.Equal(t, 25, r.HistoryReplayConcurrency)
	assert.Equal(t, 5000, r.PresenceDetailThreshold)
}

func TestUnmarshalCaddyfileMissingURL(t *testing.T) {
	t.Parallel()

	input := `redis {
		stream test
	}`

	d := caddyfile.NewTestDispenser(input)
	r := &Redis{}
	err := r.UnmarshalCaddyfile(d)
	require.NoError(t, err)
	assert.Empty(t, r.URL)
}

func TestUnmarshalCaddyfileInvalidMaxLength(t *testing.T) {
	t.Parallel()

	input := `redis {
		url redis://localhost:6379
		max_length notanumber
	}`

	d := caddyfile.NewTestDispenser(input)
	r := &Redis{}
	err := r.UnmarshalCaddyfile(d)
	require.Error(t, err)
}

func TestUnmarshalCaddyfileInvalidHealthThreshold(t *testing.T) {
	t.Parallel()

	input := `redis {
		url redis://localhost:6379
		health_threshold notanumber
	}`

	d := caddyfile.NewTestDispenser(input)
	r := &Redis{}
	err := r.UnmarshalCaddyfile(d)
	require.Error(t, err)
}

func TestUnmarshalCaddyfileInvalidXReadCount(t *testing.T) {
	t.Parallel()

	input := `redis {
		url redis://localhost:6379
		xread_count notanumber
	}`

	d := caddyfile.NewTestDispenser(input)
	r := &Redis{}
	err := r.UnmarshalCaddyfile(d)
	require.Error(t, err)
}

func TestUnmarshalCaddyfileInvalidDispatchShards(t *testing.T) {
	t.Parallel()

	input := `redis {
		url redis://localhost:6379
		dispatch_shards notanumber
	}`

	d := caddyfile.NewTestDispenser(input)
	r := &Redis{}
	err := r.UnmarshalCaddyfile(d)
	require.Error(t, err)
}

// TestUnmarshalCaddyfileExplicitZeroDispatchShards pins that explicit
// `dispatch_shards 0` sets a non-nil pointer to 0 (README: 0 = NumCPU),
// distinct from omitting the directive (nil, the library default of 1).
func TestUnmarshalCaddyfileExplicitZeroDispatchShards(t *testing.T) {
	t.Parallel()

	input := `redis {
		url redis://localhost:6379
		dispatch_shards 0
	}`

	d := caddyfile.NewTestDispenser(input)
	r := &Redis{}
	require.NoError(t, r.UnmarshalCaddyfile(d))
	require.NotNil(t, r.DispatchShards,
		"explicit 0 must allocate the pointer (distinguishable from omission)")
	assert.Equal(t, 0, *r.DispatchShards,
		"explicit 0 is the documented auto-detect-to-NumCPU opt-in")
}

// TestAppendNumericOptionsDispatchShardsTriState pins that appendNumericOptions
// emits WithDispatchShards for any explicit value, including 0 (the NumCPU
// opt-in), and omits it when the directive is absent. Each Redis below sets
// only DispatchShards, so that is the only option it can emit.
func TestAppendNumericOptionsDispatchShardsTriState(t *testing.T) {
	t.Parallel()

	t.Run("omitted emits no option", func(t *testing.T) {
		t.Parallel()

		r := &Redis{}
		opts, err := r.appendNumericOptions(nil)
		require.NoError(t, err)
		assert.Empty(t, opts,
			"nil DispatchShards must leave the library default (1) untouched")
	})

	t.Run("explicit zero emits the option", func(t *testing.T) {
		t.Parallel()

		r := &Redis{DispatchShards: new(0)}
		opts, err := r.appendNumericOptions(nil)
		require.NoError(t, err)
		assert.Len(t, opts, 1,
			"dispatch_shards 0 must emit WithDispatchShards(0) so the library auto-detects NumCPU")
	})

	t.Run("explicit positive emits the option", func(t *testing.T) {
		t.Parallel()

		r := &Redis{DispatchShards: new(4)}
		opts, err := r.appendNumericOptions(nil)
		require.NoError(t, err)
		assert.Len(t, opts, 1,
			"explicit dispatch_shards must emit WithDispatchShards")
	})
}

// TestAppendNumericOptionsRejectsNegativeDispatchShards verifies an operator
// typo like `dispatch_shards -5` fails fast at the Caddy boundary instead of
// silently auto-detecting to NumCPU (the Go API's <=0 contract). Mirrors the
// max_length / event_ttl negative-rejection guards.
func TestAppendNumericOptionsRejectsNegativeDispatchShards(t *testing.T) {
	t.Parallel()

	r := &Redis{DispatchShards: new(-5)}
	_, err := r.appendNumericOptions(nil)
	require.ErrorIs(t, err, errNegativeOptionValue)
	assert.Contains(t, err.Error(), directiveDispatchShards)
}

// TestAppendNumericOptionsJoinsNegativeErrors verifies every negative numeric
// directive is reported in one joined error, not just the first.
func TestAppendNumericOptionsJoinsNegativeErrors(t *testing.T) {
	t.Parallel()

	r := &Redis{
		DispatchShards:              new(-1),
		HealthThreshold:             -2,
		XReadCount:                  -3,
		HistoryReplayConcurrency:    -4,
		SubscriberRateLimit:         -1.5,
		SubscriberRateBurst:         -5,
		PublisherRateLimit:          -2.5,
		PublisherRateBurst:          -6,
		PresenceDetailThreshold:     -7,
		PresenceDetailByteThreshold: new(int64(-8)),
		SubscriptionsMaxSubscribers: new(-9),
		SubscriberMaxCount:          new(-10),
	}
	_, err := r.appendNumericOptions(nil)
	require.ErrorIs(t, err, errNegativeOptionValue)

	for _, want := range []string{
		directiveDispatchShards, directiveHealthThreshold, directiveXReadCount,
		directiveHistoryReplayConcurrency, directiveSubscriberRateLimit,
		directiveSubscriberRateBurst, directivePublisherRateLimit, directivePublisherRateBurst,
		directivePresenceDetailThreshold, directivePresenceByteThreshold,
		directiveSubscriptionsMaxSubscribers, directiveSubscriberMaxCount,
	} {
		assert.Contains(t, err.Error(), want,
			"joined error must name every negative directive (missing %q)", want)
	}
}

// TestBuildClientOptionsRejectsNegativeDB verifies a negative db index fails
// fast rather than being passed to go-redis (Redis database indexes are
// non-negative).
func TestBuildClientOptionsRejectsNegativeDB(t *testing.T) {
	t.Parallel()

	r := &Redis{URL: "redis://localhost:6379", DB: new(-1)}
	_, _, err := r.buildClientOptions()
	require.ErrorIs(t, err, errNegativeOptionValue)
	assert.Contains(t, err.Error(), "db=")
}

func TestUnmarshalCaddyfileUnknownDirective(t *testing.T) {
	t.Parallel()

	input := `redis {
		url redis://localhost:6379
		unknown_option value
	}`

	d := caddyfile.NewTestDispenser(input)
	r := &Redis{}
	err := r.UnmarshalCaddyfile(d)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown redis transport directive")
}

func TestParseDuration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		want    time.Duration
		wantErr bool
	}{
		{name: "empty", input: "", want: 0},
		{name: "seconds", input: "30s", want: 30 * time.Second},
		{name: "minutes", input: "5m", want: 5 * time.Minute},
		{name: "hours", input: "24h", want: 24 * time.Hour},
		{name: "zero", input: "0s", want: 0},
		{name: "invalid", input: "notaduration", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := parseDuration(tt.input)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestGetTransportNil(t *testing.T) {
	t.Parallel()

	r := &Redis{}
	assert.Nil(t, r.GetTransport())
}

// TestUnmarshalCaddyfileMultiAddress verifies the `addresses` directive
// captures multiple seed nodes and, without master_name, builds Cluster
// options carrying the auth, db and tls settings.
func TestUnmarshalCaddyfileMultiAddress(t *testing.T) {
	t.Parallel()

	input := `redis {
		addresses redis-1:6379 redis-2:6379 redis-3:6379
		username myuser
		password mypassword
		db 2
		tls
	}`

	d := caddyfile.NewTestDispenser(input)
	r := &Redis{}
	require.NoError(t, r.UnmarshalCaddyfile(d))

	assert.Equal(t, []string{"redis-1:6379", "redis-2:6379", "redis-3:6379"}, r.Addresses)
	assert.Equal(t, "myuser", r.Username)
	assert.Equal(t, "mypassword", r.Password)
	require.NotNil(t, r.DB)
	assert.Equal(t, 2, *r.DB)
	assert.True(t, r.TLS)

	uo, warnings, err := r.buildClientOptions()
	require.NoError(t, err)
	assert.Empty(t, warnings, "addresses path emits no URL-override warnings")
	assert.Equal(t, []string{"redis-1:6379", "redis-2:6379", "redis-3:6379"}, uo.Addrs)
	assert.Equal(t, "myuser", uo.Username)
	assert.Equal(t, "mypassword", uo.Password)
	assert.Equal(t, 2, uo.DB)
	assert.Empty(t, uo.MasterName, "master_name unset → Cluster mode (no MasterName)")

	tlsCfg, err := r.buildTLSConfig(uo.TLSConfig)
	require.NoError(t, err)
	assert.NotNil(t, tlsCfg, "tls directive should produce a TLSConfig")
}

// TestUnmarshalCaddyfileSentinel verifies that setting master_name alongside
// `addresses` selects Sentinel topology at the UniversalOptions level.
func TestUnmarshalCaddyfileSentinel(t *testing.T) {
	t.Parallel()

	input := `redis {
		addresses sentinel-1:26379 sentinel-2:26379
		master_name mymaster
		password sentinelsecret
	}`

	d := caddyfile.NewTestDispenser(input)
	r := &Redis{}
	require.NoError(t, r.UnmarshalCaddyfile(d))

	uo, warnings, err := r.buildClientOptions()
	require.NoError(t, err)
	assert.Empty(t, warnings)
	assert.Equal(t, "mymaster", uo.MasterName, "master_name → Sentinel topology")
	assert.Equal(t, "sentinelsecret", uo.Password)
	assert.Len(t, uo.Addrs, 2)
}

// TestUnmarshalCaddyfileURLAndAddressesConflict verifies that setting both
// `url` and `addresses` is a config error — they configure the same thing
// two different ways.
func TestUnmarshalCaddyfileURLAndAddressesConflict(t *testing.T) {
	t.Parallel()

	r := &Redis{
		URL:       "redis://localhost:6379",
		Addresses: []string{"redis-1:6379", "redis-2:6379"},
	}

	_, _, err := r.buildClientOptions()
	require.ErrorIs(t, err, errURLAndAddresses)
}

// TestUnmarshalCaddyfileSeparateAuthOverridesURL verifies that explicit
// `username` / `password` / `db` directives win over values parsed from a
// `url` directive — the env-var-injection use case where the URL has
// placeholder credentials and the real values come from env.
func TestUnmarshalCaddyfileSeparateAuthOverridesURL(t *testing.T) {
	t.Parallel()

	// Build the URL by concatenation so gosec G101 does not flag the fixture
	// credentials.
	urlWithStubCreds := "redis://" + "placeholder" + ":" + "placeholder" + "@localhost:6379/2"
	input := `redis {
		url ` + urlWithStubCreds + `
		username realuser
		password realpassword
		db 5
	}`

	d := caddyfile.NewTestDispenser(input)
	r := &Redis{}
	require.NoError(t, r.UnmarshalCaddyfile(d))

	uo, warnings, err := r.buildClientOptions()
	require.NoError(t, err)
	assert.Equal(t, "realuser", uo.Username, "explicit username should override URL userinfo")
	assert.Equal(t, "realpassword", uo.Password, "explicit password should override URL userinfo")
	assert.Equal(t, 5, uo.DB, "explicit db should override URL path")
	assert.Len(t, warnings, 3, "username, password, db all conflict and should each warn")
}

// TestBuildClientOptionsNoWarningWhenURLHasNoCreds verifies the env-var-injection
// happy path: a URL with no userinfo plus explicit `password` / `username`
// directives produces zero warnings (nothing to override).
func TestBuildClientOptionsNoWarningWhenURLHasNoCreds(t *testing.T) {
	t.Parallel()

	db := 3
	r := &Redis{
		URL:      "redis://localhost:6379",
		Username: "fromenv",
		Password: "fromenv",
		DB:       &db,
	}

	uo, warnings, err := r.buildClientOptions()
	require.NoError(t, err)
	assert.Empty(t, warnings, "URL had no creds → no conflict, no warnings")
	assert.Equal(t, "fromenv", uo.Username)
	assert.Equal(t, "fromenv", uo.Password)
	assert.Equal(t, 3, uo.DB)
}

// TestBuildClientOptionsNoDBWarningWhenURLDBIsZero pins a known gap:
// redis.ParseURL returns DB=0 both for a missing path and for `/0`, so an
// explicit `db N` overriding `/0` applies without a warning.
func TestBuildClientOptionsNoDBWarningWhenURLDBIsZero(t *testing.T) {
	t.Parallel()

	db := 5
	r := &Redis{
		URL: "redis://localhost:6379/0",
		DB:  &db,
	}

	uo, warnings, err := r.buildClientOptions()
	require.NoError(t, err)
	assert.Equal(t, 5, uo.DB, "explicit db still applies")
	assert.Empty(t, warnings, "URL DB=0 indistinguishable from unset; no warning")
}

// TestBuildClientOptionsNoWarningWhenURLAndExplicitMatch verifies that
// agreeing values do not warn; only divergence does.
func TestBuildClientOptionsNoWarningWhenURLAndExplicitMatch(t *testing.T) {
	t.Parallel()

	const sameUser = "same"

	const samePass = "same"

	db := 2
	r := &Redis{
		URL:      "redis://" + sameUser + ":" + samePass + "@localhost:6379/2",
		Username: sameUser,
		Password: samePass,
		DB:       &db,
	}

	_, warnings, err := r.buildClientOptions()
	require.NoError(t, err)
	assert.Empty(t, warnings, "matching values do not constitute a conflict")
}

// TestUnmarshalCaddyfileDBExplicitZeroOverridesURL verifies that `db 0`
// overrides a non-zero DB parsed from the URL, which the pointer-typed DB
// field makes expressible.
func TestUnmarshalCaddyfileDBExplicitZeroOverridesURL(t *testing.T) {
	t.Parallel()

	input := `redis {
		url redis://localhost:6379/2
		db 0
	}`

	d := caddyfile.NewTestDispenser(input)
	r := &Redis{}
	require.NoError(t, r.UnmarshalCaddyfile(d))
	require.NotNil(t, r.DB, "explicit `db 0` must store a non-nil pointer")
	assert.Equal(t, 0, *r.DB)

	uo, warnings, err := r.buildClientOptions()
	require.NoError(t, err)
	assert.Equal(t, 0, uo.DB, "explicit `db 0` must override URL `/2`")
	assert.Len(t, warnings, 1, "URL DB=2 + explicit DB=0 disagree → one warning")
	assert.Contains(t, warnings[0], "db", "warning should mention `db`")
}

// TestUnmarshalCaddyfileDBOmittedKeepsURLValue is the negative half:
// when no `db` directive is set, the URL's DB should win (no override).
func TestUnmarshalCaddyfileDBOmittedKeepsURLValue(t *testing.T) {
	t.Parallel()

	input := `redis {
		url redis://localhost:6379/2
	}`

	d := caddyfile.NewTestDispenser(input)
	r := &Redis{}
	require.NoError(t, r.UnmarshalCaddyfile(d))
	assert.Nil(t, r.DB, "omitted `db` directive must leave DB nil")

	uo, warnings, err := r.buildClientOptions()
	require.NoError(t, err)
	assert.Equal(t, 2, uo.DB, "URL DB must apply when no override is set")
	assert.Empty(t, warnings, "no override → no warning")
}

// TestUnmarshalCaddyfileAddressSingular verifies the singular `address`
// alias for `addresses` (single value form).
func TestUnmarshalCaddyfileAddressSingular(t *testing.T) {
	t.Parallel()

	input := `redis {
		address localhost:6379
	}`

	d := caddyfile.NewTestDispenser(input)
	r := &Redis{}
	require.NoError(t, r.UnmarshalCaddyfile(d))
	assert.Equal(t, []string{"localhost:6379"}, r.Addresses)
}

// TestUnmarshalCaddyfileAddressesPluralAccumulates verifies that repeated
// `addresses` lines accumulate hosts, as the singular `address` alias does.
func TestUnmarshalCaddyfileAddressesPluralAccumulates(t *testing.T) {
	t.Parallel()

	input := `redis {
		addresses redis-1:6379 redis-2:6379
		addresses redis-3:6379
	}`

	d := caddyfile.NewTestDispenser(input)
	r := &Redis{}
	require.NoError(t, r.UnmarshalCaddyfile(d))
	assert.Equal(t,
		[]string{"redis-1:6379", "redis-2:6379", "redis-3:6379"},
		r.Addresses,
		"repeated `addresses` lines should accumulate, not overwrite")
}

// TestBuildClientOptionsInvalidURL verifies that a malformed URL surfaces
// the redis.ParseURL error wrapped with the "redis transport:" prefix.
func TestBuildClientOptionsInvalidURL(t *testing.T) {
	t.Parallel()

	r := &Redis{URL: "not://a-valid-redis-url"}
	_, _, err := r.buildClientOptions()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "redis transport: invalid URL",
		"buildClientOptions should wrap redis.ParseURL errors with the transport prefix")
}

// TestUnmarshalCaddyfileAddressMultipleLines verifies that repeated `address`
// directives accumulate (matching the multi-host semantics of `addresses`).
func TestUnmarshalCaddyfileAddressMultipleLines(t *testing.T) {
	t.Parallel()

	input := `redis {
		address redis-1:6379
		address redis-2:6379
		address redis-3:6379
	}`

	d := caddyfile.NewTestDispenser(input)
	r := &Redis{}
	require.NoError(t, r.UnmarshalCaddyfile(d))
	assert.Equal(t, []string{"redis-1:6379", "redis-2:6379", "redis-3:6379"}, r.Addresses)
}

// TestUnmarshalCaddyfileNumericBoolPlaceholderRejected pins the error a
// runtime placeholder on a numeric or boolean directive gets: it names the
// directive and points to the quoted "{$VAR}" and {$VAR:default} forms,
// instead of a bare strconv parse error.
func TestUnmarshalCaddyfileNumericBoolPlaceholderRejected(t *testing.T) {
	t.Parallel()

	for _, directive := range []string{
		"max_length", "health_threshold", "dispatch_shards", "db",
		"presence_detail_byte_threshold", "subscriber_rate_limit",
		"tls", "tls_insecure_skip_verify",
	} {
		t.Run(directive, func(t *testing.T) {
			t.Parallel()

			d := caddyfile.NewTestDispenser("redis {\n\t" + directive + " {env.MERCURE_TEST_VALUE}\n}")
			err := (&Redis{}).UnmarshalCaddyfile(d)
			require.Error(t, err)
			assert.Contains(t, err.Error(), directive+" {env.MERCURE_TEST_VALUE}")
			assert.Contains(t, err.Error(), "take literals")
			assert.Contains(t, err.Error(), `write "{$VAR}" (quoted, so that an unset variable leaves an empty argument and startup fails)`)
			assert.Contains(t, err.Error(), "or {$VAR:default}")
		})
	}
}

// TestUnmarshalCaddyfileBareGobRejected pins the error the removed bare `gob`
// directive gets: it points to `encoding gob`.
func TestUnmarshalCaddyfileBareGobRejected(t *testing.T) {
	t.Parallel()

	d := caddyfile.NewTestDispenser("redis {\n\tgob\n}")
	err := (&Redis{}).UnmarshalCaddyfile(d)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "use `encoding gob`")
}

// TestUnmarshalCaddyfilePublisherRateLimit verifies the publisher_rate_limit
// + publisher_rate_burst directives parse correctly.
func TestUnmarshalCaddyfilePublisherRateLimit(t *testing.T) {
	t.Parallel()

	input := `redis {
		url redis://localhost:6379
		publisher_rate_limit 1500.5
		publisher_rate_burst 7500
	}`

	d := caddyfile.NewTestDispenser(input)
	r := &Redis{}
	require.NoError(t, r.UnmarshalCaddyfile(d))

	assert.InEpsilon(t, 1500.5, r.PublisherRateLimit, 0.001)
	assert.Equal(t, 7500, r.PublisherRateBurst)
}

// writeTLSFixtures generates an ed25519-signed CA + client cert/key triple in
// t.TempDir and returns the file paths. ed25519 is the fastest key type
// available in stdlib — keeps this helper cheap when called per-test.
func writeTLSFixtures(t *testing.T) (caFile, certFile, keyFile string) {
	t.Helper()

	dir := t.TempDir()

	caPub, caPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}

	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, caPub, caPriv)
	require.NoError(t, err)

	caFile = filepath.Join(dir, "ca.pem")
	require.NoError(t, os.WriteFile(caFile,
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}), 0o600))

	clientPub, clientPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	clientTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "test-client"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}

	clientDER, err := x509.CreateCertificate(rand.Reader, clientTmpl, caTmpl, clientPub, caPriv)
	require.NoError(t, err)

	certFile = filepath.Join(dir, "client.pem")
	require.NoError(t, os.WriteFile(certFile,
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: clientDER}), 0o600))

	clientKeyDER, err := x509.MarshalPKCS8PrivateKey(clientPriv)
	require.NoError(t, err)

	keyFile = filepath.Join(dir, "client.key")
	require.NoError(t, os.WriteFile(keyFile,
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: clientKeyDER}), 0o600))

	return caFile, certFile, keyFile
}

// TestBuildClientOptionsResolvesEnvVarPassword verifies that
// `password {env.VAR}` runs through the Caddy replacer and the resolved value
// reaches go-redis.
func TestBuildClientOptionsResolvesEnvVarPassword(t *testing.T) {
	// t.Setenv is incompatible with t.Parallel — the env-resolving
	// tests run sequentially.
	t.Setenv("MERCURE_TEST_PASSWORD", "actualsecret")

	// Split the placeholder literal so gosec G101 doesn't flag this as
	// a hardcoded credential — same pattern as
	// TestUnmarshalCaddyfileSeparateAuthOverridesURL.
	envRef := "{env." + "MERCURE_TEST_PASSWORD" + "}"
	r := &Redis{
		URL:      "redis://localhost:6379",
		Password: envRef,
	}
	uo, _, err := r.buildClientOptions()
	require.NoError(t, err)
	assert.Equal(t, "actualsecret", uo.Password,
		"{env.VAR} should resolve to the env-var value, not the literal placeholder")
}

// TestBuildClientOptionsResolvesEnvVarUsername mirrors the password
// test for username.
func TestBuildClientOptionsResolvesEnvVarUsername(t *testing.T) {
	t.Setenv("MERCURE_TEST_USERNAME", "actualuser")

	r := &Redis{
		URL:      "redis://localhost:6379",
		Username: "{env.MERCURE_TEST_USERNAME}",
	}
	uo, _, err := r.buildClientOptions()
	require.NoError(t, err)
	assert.Equal(t, "actualuser", uo.Username)
}

// TestBuildClientOptionsResolvesEnvVarURL covers the URL-as-env-var
// case (rare but supported for fully env-driven configs).
func TestBuildClientOptionsResolvesEnvVarURL(t *testing.T) {
	t.Setenv("MERCURE_TEST_URL", "redis://resolvedhost:6380")

	r := &Redis{URL: "{env.MERCURE_TEST_URL}"}
	uo, _, err := r.buildClientOptions()
	require.NoError(t, err)
	assert.Equal(t, []string{"resolvedhost:6380"}, uo.Addrs)
}

// TestBuildClientOptionsResolvesEnvVarAddresses verifies per-element
// resolution of the addresses slice — `addresses {env.A1} {env.A2}`
// must resolve each entry independently.
func TestBuildClientOptionsResolvesEnvVarAddresses(t *testing.T) {
	t.Setenv("MERCURE_TEST_ADDR1", "node1:6379")
	t.Setenv("MERCURE_TEST_ADDR2", "node2:6379")

	r := &Redis{
		Addresses: []string{"{env.MERCURE_TEST_ADDR1}", "{env.MERCURE_TEST_ADDR2}"},
	}
	uo, _, err := r.buildClientOptions()
	require.NoError(t, err)
	assert.Equal(t, []string{"node1:6379", "node2:6379"}, uo.Addrs)
}

// TestBuildClientOptionsUnresolvedURL verifies that an unset env var
// in `url` fast-fails instead of passing an empty URL to ParseURL
// (which would surface a less helpful "empty url" error).
func TestBuildClientOptionsUnresolvedURL(t *testing.T) {
	t.Parallel()

	r := &Redis{URL: "{env.MERCURE_TEST_DEFINITELY_UNSET_URL_XYZ}"}
	_, _, err := r.buildClientOptions()
	require.Error(t, err)
	require.ErrorIs(t, err, errPlaceholderUnresolved)
	assert.Contains(t, err.Error(), "url")
}

// TestBuildClientOptionsUnresolvedPassword verifies fast-fail for
// credential typos — `password {env.REDIS_PASWORD}` (typo) would
// otherwise resolve to "" and silently bypass authentication.
func TestBuildClientOptionsUnresolvedPassword(t *testing.T) {
	t.Parallel()

	envRef := "{env." + "MERCURE_TEST_DEFINITELY_UNSET_PASSWORD_XYZ" + "}"
	r := &Redis{
		URL:      "redis://localhost:6379",
		Password: envRef,
	}
	_, _, err := r.buildClientOptions()
	require.Error(t, err)
	require.ErrorIs(t, err, errPlaceholderUnresolved)
	assert.Contains(t, err.Error(), "password")
}

// TestAppendBasicOptionsResolvesEnvVarStream verifies `stream {env.X}`
// resolves before WithStreamName is appended. The option closure is opaque,
// so TestAppendBasicOptionsUnresolvedStream is what shows resolution runs here.
func TestAppendBasicOptionsResolvesEnvVarStream(t *testing.T) {
	t.Setenv("MERCURE_TEST_STREAM_NAME", "stream-a-mercure")

	r := &Redis{Stream: "{env.MERCURE_TEST_STREAM_NAME}"}
	opts, err := r.appendBasicOptions(nil)
	require.NoError(t, err)
	assert.Len(t, opts, 1, "stream option should be appended when set")
}

// TestAppendBasicOptionsUnresolvedStream verifies an unset placeholder
// in `stream` fast-fails rather than silently falling back to the default.
func TestAppendBasicOptionsUnresolvedStream(t *testing.T) {
	t.Parallel()

	r := &Redis{Stream: "{env.MERCURE_TEST_DEFINITELY_UNSET_STREAM_XYZ}"}
	_, err := r.appendBasicOptions(nil)
	require.ErrorIs(t, err, errPlaceholderUnresolved)
	assert.Contains(t, err.Error(), "stream")
}

// TestAppendBasicOptionsResolvesEnvVarEncoding mirrors the stream test for
// encoding.
func TestAppendBasicOptionsResolvesEnvVarEncoding(t *testing.T) {
	t.Setenv("MERCURE_TEST_ENCODING", "msgpack")

	r := &Redis{Encoding: "{env.MERCURE_TEST_ENCODING}"}
	opts, err := r.appendBasicOptions(nil)
	require.NoError(t, err)
	assert.Len(t, opts, 1, "encoding option should be appended when set")
}

// TestAppendBasicOptionsUnresolvedEncoding verifies fast-fail for
// `encoding {env.UNSET}` — silently defaulting to JSON would surprise
// operators expecting a specific codec.
func TestAppendBasicOptionsUnresolvedEncoding(t *testing.T) {
	t.Parallel()

	r := &Redis{Encoding: "{env.MERCURE_TEST_DEFINITELY_UNSET_ENCODING_XYZ}"}
	_, err := r.appendBasicOptions(nil)
	require.ErrorIs(t, err, errPlaceholderUnresolved)
	assert.Contains(t, err.Error(), "encoding")
}

// TestAppendDurationOptionsResolvesEnvVar verifies `presence_ttl {env.X}`
// resolves before the duration parser sees it.
func TestAppendDurationOptionsResolvesEnvVar(t *testing.T) {
	t.Setenv("MERCURE_TEST_PRESENCE_TTL", "90s")

	r := &Redis{PresenceTTL: "{env.MERCURE_TEST_PRESENCE_TTL}"}
	opts, err := r.appendDurationOptions(nil)
	require.NoError(t, err)
	assert.Len(t, opts, 1, "presence_ttl option should be appended")
}

// TestAppendDurationOptionsUnresolvedFastFails verifies an unset
// placeholder in any duration field surfaces as
// errPlaceholderUnresolved instead of a parse-empty-string error.
func TestAppendDurationOptionsUnresolvedFastFails(t *testing.T) {
	t.Parallel()

	r := &Redis{EventTTL: "{env.MERCURE_TEST_DEFINITELY_UNSET_EVENT_TTL_XYZ}"}
	_, err := r.appendDurationOptions(nil)
	require.ErrorIs(t, err, errPlaceholderUnresolved)
	assert.Contains(t, err.Error(), "event_ttl")
}

// TestAppendBasicOptionsRejectsNegativeMaxLength verifies an operator typo
// like `max_length -1` fails fast instead of being dropped, which would leave
// retention unlimited.
func TestAppendBasicOptionsRejectsNegativeMaxLength(t *testing.T) {
	t.Parallel()

	r := &Redis{MaxLength: -1}
	_, err := r.appendBasicOptions(nil)
	require.ErrorIs(t, err, errNegativeOptionValue)
	assert.Contains(t, err.Error(), directiveMaxLength)
}

// TestAppendBasicOptionsAcceptsZeroMaxLength documents that 0 means
// "unlimited" and is a valid (no-op) value, not a misconfiguration.
func TestAppendBasicOptionsAcceptsZeroMaxLength(t *testing.T) {
	t.Parallel()

	r := &Redis{MaxLength: 0}
	opts, err := r.appendBasicOptions(nil)
	require.NoError(t, err)
	assert.Empty(t, opts, "zero is the default; no option should be emitted")
}

// TestAppendPositiveDurationOptionRejectsNegative verifies an operator
// typo like `event_ttl -1s` fails fast instead of being dropped silently
// by the d > 0 gate.
func TestAppendPositiveDurationOptionRejectsNegative(t *testing.T) {
	t.Parallel()

	r := &Redis{EventTTL: "-1s"}
	_, err := r.appendDurationOptions(nil)
	require.ErrorIs(t, err, errNegativeOptionValue)
	assert.Contains(t, err.Error(), "event_ttl")
}

// TestAppendDurationOptionsZombieGCResolves verifies that a placeholder in
// zombie_gc_interval, a duration where 0 is allowed, resolves before parsing.
func TestAppendDurationOptionsZombieGCResolves(t *testing.T) {
	t.Setenv("MERCURE_TEST_ZOMBIE_GC", "10m")

	r := &Redis{ZombieGCInterval: "{env.MERCURE_TEST_ZOMBIE_GC}"}
	opts, err := r.appendDurationOptions(nil)
	require.NoError(t, err)
	assert.Len(t, opts, 1, "zombie_gc_interval option should be appended")
}

// TestAppendDurationOptionsZombieGCUnresolved verifies that an unset
// placeholder in zombie_gc_interval fails with errPlaceholderUnresolved.
func TestAppendDurationOptionsZombieGCUnresolved(t *testing.T) {
	t.Parallel()

	r := &Redis{ZombieGCInterval: "{env.MERCURE_TEST_DEFINITELY_UNSET_ZOMBIE_XYZ}"}
	_, err := r.appendDurationOptions(nil)
	require.ErrorIs(t, err, errPlaceholderUnresolved)
	assert.Contains(t, err.Error(), "zombie_gc_interval")
}

// A reload that changes `encoding` must not reuse the pooled transport: the
// Caddy hub never calls SetCodec, so the transport keeps the codec it was
// built with, and a config with another encoding needs a transport of its own.
func TestPoolKeyDistinguishesEncoding(t *testing.T) {
	t.Parallel()

	keyJSON, err := (&Redis{URL: "redis://localhost:6379", Encoding: "json"}).poolKey("")
	require.NoError(t, err)
	keyMsgpack, err := (&Redis{URL: "redis://localhost:6379", Encoding: "msgpack"}).poolKey("")
	require.NoError(t, err)

	assert.NotEqual(t, keyJSON, keyMsgpack)
}

// TestPoolKeyDistinguishesPointerTriState pins that an explicit zero on a
// pointer field and an omitted directive give different pool keys, so the two
// configs never share a transport (for `db`, that would read the wrong Redis
// database).
func TestPoolKeyDistinguishesPointerTriState(t *testing.T) {
	t.Parallel()

	key := func(r *Redis) string {
		k, err := r.poolKey("")
		require.NoError(t, err)

		return k
	}

	t.Run("dispatch_shards 0 vs omitted", func(t *testing.T) {
		t.Parallel()

		omitted := &Redis{URL: "redis://localhost:6379"}
		explicit := &Redis{URL: "redis://localhost:6379", DispatchShards: new(0)}
		assert.NotEqual(t, key(omitted), key(explicit),
			"`dispatch_shards 0` (NumCPU) must not share a pool key with an omitted directive (default 1)")
	})

	t.Run("presence_detail_byte_threshold 0 vs omitted", func(t *testing.T) {
		t.Parallel()

		omitted := &Redis{URL: "redis://localhost:6379"}
		explicit := &Redis{URL: "redis://localhost:6379", PresenceDetailByteThreshold: new(int64(0))}
		assert.NotEqual(t, key(omitted), key(explicit),
			"explicit `presence_detail_byte_threshold 0` (disabled) must not share a pool key with omitted (default 512 KiB)")
	})

	t.Run("db 0 vs omitted", func(t *testing.T) {
		t.Parallel()

		omitted := &Redis{URL: "redis://localhost:6379/2"}
		explicit := &Redis{URL: "redis://localhost:6379/2", DB: new(0)}
		assert.NotEqual(t, key(omitted), key(explicit),
			"`db 0` must not share a pool key with an omitted directive (which would inherit the URL's DB 2)")
	})
}

// TestDispatchShardsZeroSurvivesJSONRoundTrip pins that a non-nil
// pointer-to-0 survives omitempty: it marshals as `"dispatch_shards":0` and
// rehydrates as &0, not nil. Caddy round-trips the adapted config through JSON
// before Provision.
func TestDispatchShardsZeroSurvivesJSONRoundTrip(t *testing.T) {
	t.Parallel()

	original := &Redis{URL: "redis://localhost:6379", DispatchShards: new(0)}
	require.Empty(t, original.Password, "test fixture must not carry secret-shaped values into json.Marshal")

	b, err := json.Marshal(original) //nolint:gosec // test fixture has no Password value; full struct round-trip is intentional
	require.NoError(t, err)
	assert.Contains(t, string(b), `"dispatch_shards":0`,
		"a non-nil pointer-to-0 must serialize, not be omitted by omitempty")

	var rehydrated Redis
	require.NoError(t, json.Unmarshal(b, &rehydrated))
	require.NotNil(t, rehydrated.DispatchShards, "explicit 0 must survive the JSON round-trip as non-nil")
	assert.Equal(t, 0, *rehydrated.DispatchShards)
}

// TestSkipVerifyWarningCAFileNotSet covers skipVerifyWarning without
// tls_ca_file: the MITM warning alone.
func TestSkipVerifyWarningCAFileNotSet(t *testing.T) {
	t.Parallel()

	msg := skipVerifyWarning(false)
	assert.Contains(t, msg, "tls verification disabled")
	assert.Contains(t, msg, "vulnerable to MITM")
	assert.NotContains(t, msg, "tls_ca_file is set but ignored",
		"the CA-ignored note should appear only when tls_ca_file is also set")
}

// TestSkipVerifyWarningCAFileSet: with tls_ca_file set, one message carries
// both the MITM warning and the "tls_ca_file is set but ignored" note.
func TestSkipVerifyWarningCAFileSet(t *testing.T) {
	t.Parallel()

	msg := skipVerifyWarning(true)
	assert.Contains(t, msg, "tls verification disabled")
	assert.Contains(t, msg, "tls_ca_file is set but ignored")
}

// TestBuildTLSConfigPlaceholderResolvesEmpty verifies that a directive that
// resolves to empty after placeholder substitution (typically
// `{env.UNSET_VAR}`) fails fast instead of falling back to the system CA pool.
func TestBuildTLSConfigPlaceholderResolvesEmpty(t *testing.T) {
	t.Parallel()

	// Construct a literal that the Caddy replacer recognizes (env
	// namespace) but whose variable is unset — should resolve to empty.
	r := &Redis{TLSCAFile: "{env.MERCURE_TEST_DEFINITELY_UNSET_VAR_XYZ}"}
	_, err := r.buildTLSConfig(nil)
	require.Error(t, err)
	require.ErrorIs(t, err, errPlaceholderUnresolved)
	assert.Contains(t, err.Error(), "tls_ca_file")
}

// TestBuildTLSConfigPlaceholderResolvesEmptyForCert covers the
// fast-fail path for the cert side of tls_client_auth — symmetric
// behavior matters because half-set state would otherwise be
// reachable from a Caddyfile via empty-resolving placeholders.
func TestBuildTLSConfigPlaceholderResolvesEmptyForCert(t *testing.T) {
	t.Parallel()

	_, _, keyFile := writeTLSFixtures(t)

	r := &Redis{
		TLSClientAuthCertFile: "{env.MERCURE_TEST_UNSET_CERT_XYZ}",
		TLSClientAuthKeyFile:  keyFile,
	}
	_, err := r.buildTLSConfig(nil)
	require.Error(t, err)
	require.ErrorIs(t, err, errPlaceholderUnresolved)
	assert.Contains(t, err.Error(), "tls_client_auth (cert)")
}

// TestUnmarshalCaddyfileTLSSubBlockRejected verifies that an attempt to use
// a nested `tls { ... }` sub-block is rejected with a hint pointing at the
// flat-directive surface. The transport's TLS configuration is intentionally
// flat (matching the rest of the directive surface); a sub-block would
// otherwise produce the opaque Caddy error "unknown directive: {".
func TestUnmarshalCaddyfileTLSSubBlockRejected(t *testing.T) {
	t.Parallel()

	input := `redis {
		url redis://localhost:6379
		tls {
			ca_file /etc/ssl/ca.pem
		}
	}`

	d := caddyfile.NewTestDispenser(input)
	r := &Redis{}
	err := r.UnmarshalCaddyfile(d)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nested sub-blocks are not supported",
		"the parser should explain the flat-directive constraint")
	assert.Contains(t, err.Error(), "tls_ca_file",
		"the error should name the flat replacements operators are looking for")
}

// TestUnmarshalCaddyfileTLSDirectives covers parsing of the four TLS
// directives: tls_ca_file, tls_client_auth, tls_server_name,
// tls_insecure_skip_verify.
func TestUnmarshalCaddyfileTLSDirectives(t *testing.T) {
	t.Parallel()

	input := `redis {
		url rediss://localhost:6379
		tls_ca_file /etc/ssl/ca.pem
		tls_client_auth /etc/ssl/client.pem /etc/ssl/client.key
		tls_server_name redis.internal
		tls_insecure_skip_verify
	}`

	d := caddyfile.NewTestDispenser(input)
	r := &Redis{}
	require.NoError(t, r.UnmarshalCaddyfile(d))

	assert.Equal(t, "/etc/ssl/ca.pem", r.TLSCAFile)
	assert.Equal(t, "/etc/ssl/client.pem", r.TLSClientAuthCertFile)
	assert.Equal(t, "/etc/ssl/client.key", r.TLSClientAuthKeyFile)
	assert.Equal(t, "redis.internal", r.TLSServerName)
	assert.True(t, r.TLSInsecureSkipVerify)
}

// TestUnmarshalCaddyfileTLSClientAuthMissingArg verifies the 2-arg directive
// rejects a single argument.
func TestUnmarshalCaddyfileTLSClientAuthMissingArg(t *testing.T) {
	t.Parallel()

	input := `redis {
		tls_client_auth /only/cert.pem
	}`

	d := caddyfile.NewTestDispenser(input)
	r := &Redis{}
	assert.Error(t, r.UnmarshalCaddyfile(d), "single arg should fail — both cert and key required")
}

// TestUnmarshalCaddyfileTLSClientAuthExtraArg verifies the 2-arg directive
// rejects a third argument.
func TestUnmarshalCaddyfileTLSClientAuthExtraArg(t *testing.T) {
	t.Parallel()

	input := `redis {
		tls_client_auth /cert.pem /key.pem /extra.pem
	}`

	d := caddyfile.NewTestDispenser(input)
	r := &Redis{}
	assert.Error(t, r.UnmarshalCaddyfile(d), "third arg should fail — directive is exactly 2 args")
}

// TestBuildTLSConfigCAFile verifies tls_ca_file produces a TLSConfig with a
// non-nil RootCAs pool (replacing the default system pool).
func TestBuildTLSConfigCAFile(t *testing.T) {
	t.Parallel()

	caFile, _, _ := writeTLSFixtures(t)

	r := &Redis{TLSCAFile: caFile}
	cfg, err := r.buildTLSConfig(nil)
	require.NoError(t, err)
	require.NotNil(t, cfg)
	assert.NotNil(t, cfg.RootCAs, "tls_ca_file should populate RootCAs")
	assert.Equal(t, uint16(tls.VersionTLS12), cfg.MinVersion, "fresh config should pin TLS 1.2 floor")
}

// TestBuildTLSConfigClientAuth verifies tls_client_auth loads the cert/key
// pair into TLSConfig.Certificates.
func TestBuildTLSConfigClientAuth(t *testing.T) {
	t.Parallel()

	_, certFile, keyFile := writeTLSFixtures(t)

	r := &Redis{
		TLSClientAuthCertFile: certFile,
		TLSClientAuthKeyFile:  keyFile,
	}
	cfg, err := r.buildTLSConfig(nil)
	require.NoError(t, err)
	require.NotNil(t, cfg)
	require.Len(t, cfg.Certificates, 1, "tls_client_auth should append one cert/key pair")
}

// TestBuildTLSConfigAllOptions verifies all four directives together.
func TestBuildTLSConfigAllOptions(t *testing.T) {
	t.Parallel()

	caFile, certFile, keyFile := writeTLSFixtures(t)

	r := &Redis{
		TLSCAFile:             caFile,
		TLSClientAuthCertFile: certFile,
		TLSClientAuthKeyFile:  keyFile,
		TLSServerName:         "redis.example.com",
		TLSInsecureSkipVerify: true,
	}
	cfg, err := r.buildTLSConfig(nil)
	require.NoError(t, err)
	require.NotNil(t, cfg)
	assert.NotNil(t, cfg.RootCAs)
	assert.Len(t, cfg.Certificates, 1)
	assert.Equal(t, "redis.example.com", cfg.ServerName)
	assert.True(t, cfg.InsecureSkipVerify)
}

// TestBuildTLSConfigCAFileNotExist verifies a missing CA file returns a
// wrapped error.
func TestBuildTLSConfigCAFileNotExist(t *testing.T) {
	t.Parallel()

	r := &Redis{TLSCAFile: "/nonexistent/ca.pem"}
	_, err := r.buildTLSConfig(nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "tls_ca_file")
}

// TestBuildTLSConfigCAFileInvalidPEM verifies a file with no valid PEM
// blocks (e.g., binary garbage or an empty file) errors explicitly.
func TestBuildTLSConfigCAFileInvalidPEM(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	caFile := filepath.Join(dir, "garbage.pem")
	require.NoError(t, os.WriteFile(caFile, []byte("not a pem block"), 0o600))

	r := &Redis{TLSCAFile: caFile}
	_, err := r.buildTLSConfig(nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no valid PEM certificates")
}

// TestBuildTLSConfigClientAuthMismatch verifies that a cert/key mismatch
// produces an error without the encrypted-key hint, which would mislead here.
func TestBuildTLSConfigClientAuthMismatch(t *testing.T) {
	t.Parallel()

	_, certFile, _ := writeTLSFixtures(t)
	// Generate a SECOND key pair; pair its key with the first cert → mismatch.
	_, mismatchedPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	mismatchedKeyDER, err := x509.MarshalPKCS8PrivateKey(mismatchedPriv)
	require.NoError(t, err)

	dir := t.TempDir()
	mismatchedKeyFile := filepath.Join(dir, "mismatched.key")
	require.NoError(t, os.WriteFile(mismatchedKeyFile,
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: mismatchedKeyDER}), 0o600))

	r := &Redis{
		TLSClientAuthCertFile: certFile,
		TLSClientAuthKeyFile:  mismatchedKeyFile,
	}
	_, err = r.buildTLSConfig(nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "tls_client_auth")
	assert.NotContains(t, err.Error(), "encrypted PEM keys",
		"cert/key mismatch should not attach the encrypted-key hint")
}

// TestBuildTLSConfigClientAuthEncryptedKey verifies the encrypted-key hint is
// attached when LoadX509KeyPair fails to parse the private key, using an
// ENCRYPTED PRIVATE KEY PEM block the standard library cannot read.
func TestBuildTLSConfigClientAuthEncryptedKey(t *testing.T) {
	t.Parallel()

	_, certFile, _ := writeTLSFixtures(t)

	// "ENCRYPTED PRIVATE KEY" PEM block — Go's tls.LoadX509KeyPair rejects
	// these with "tls: failed to parse private key", which is the signature
	// our hint targets. The body contents are arbitrary because parse fails
	// before the bytes are interpreted.
	dir := t.TempDir()
	encryptedKeyFile := filepath.Join(dir, "encrypted.key")
	require.NoError(t, os.WriteFile(encryptedKeyFile,
		pem.EncodeToMemory(&pem.Block{Type: "ENCRYPTED PRIVATE KEY", Bytes: []byte("garbage")}), 0o600))

	r := &Redis{
		TLSClientAuthCertFile: certFile,
		TLSClientAuthKeyFile:  encryptedKeyFile,
	}
	_, err := r.buildTLSConfig(nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "encrypted PEM keys",
		"parse-private-key error should attach the decrypt hint")
	assert.Contains(t, err.Error(), "openssl pkcs8")
}

// TestBuildTLSConfigClientAuthOnlyCert verifies that a half-set client auth
// pair (cert without key) errors. Reachable via direct JSON config; the
// Caddyfile parser's atomic 2-arg form prevents the half-set state at parse
// time.
func TestBuildTLSConfigClientAuthOnlyCert(t *testing.T) {
	t.Parallel()

	_, certFile, _ := writeTLSFixtures(t)

	r := &Redis{TLSClientAuthCertFile: certFile}
	_, err := r.buildTLSConfig(nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "both cert and key files")
}

// TestBuildTLSConfigEnabledByTLSFlag verifies that the bare `tls` flag
// produces a TLSConfig with the TLS 1.2 floor.
func TestBuildTLSConfigEnabledByTLSFlag(t *testing.T) {
	t.Parallel()

	r := &Redis{TLS: true}
	cfg, err := r.buildTLSConfig(nil)
	require.NoError(t, err)
	require.NotNil(t, cfg)
	assert.Equal(t, uint16(tls.VersionTLS12), cfg.MinVersion)
}

// TestBuildTLSConfigInsecureSkipVerify verifies the directive sets the flag
// and triggers nothing else (no CA, no cert).
func TestBuildTLSConfigInsecureSkipVerify(t *testing.T) {
	t.Parallel()

	r := &Redis{TLSInsecureSkipVerify: true}
	cfg, err := r.buildTLSConfig(nil)
	require.NoError(t, err)
	require.NotNil(t, cfg)
	assert.True(t, cfg.InsecureSkipVerify)
	assert.Nil(t, cfg.RootCAs)
	assert.Empty(t, cfg.Certificates)
}

// TestBuildTLSConfigLayersOntoExisting verifies that an existing TLSConfig
// (typically from a rediss:// URL) is extended in place, not replaced. The
// existing config comes from redis.ParseURL, as in production.
func TestBuildTLSConfigLayersOntoExisting(t *testing.T) {
	t.Parallel()

	caFile, _, _ := writeTLSFixtures(t)

	parsed, err := redis.ParseURL("rediss://localhost:6379?skip_verify=true")
	require.NoError(t, err)
	require.NotNil(t, parsed.TLSConfig, "rediss:// URL must produce a TLSConfig")
	require.True(t, parsed.TLSConfig.InsecureSkipVerify, "?skip_verify=true must propagate to TLSConfig")

	r := &Redis{TLSCAFile: caFile, TLSServerName: "override.example"}
	cfg, err := r.buildTLSConfig(parsed.TLSConfig)
	require.NoError(t, err)
	require.NotNil(t, cfg)
	assert.Same(t, parsed.TLSConfig, cfg, "should mutate existing config in place, not allocate fresh")
	assert.True(t, cfg.InsecureSkipVerify, "existing InsecureSkipVerify preserved across layering")
	assert.NotNil(t, cfg.RootCAs, "tls_ca_file added to existing config")
	assert.Equal(t, "override.example", cfg.ServerName, "tls_server_name set on existing config")
}

// TestTLSRequestedFalseWhenAllUnset verifies tlsRequested() is false when no
// TLS directive is set; buildTLSConfig is called only when it is true or the
// URL already produced a TLSConfig.
func TestTLSRequestedFalseWhenAllUnset(t *testing.T) {
	t.Parallel()

	r := &Redis{URL: "redis://localhost:6379"}
	assert.False(t, r.tlsRequested(), "no TLS-related fields → tlsRequested() returns false")
}

// TestTLSRequestedTrueForEachTrigger covers each directive that should
// independently flip tlsRequested() to true.
func TestTLSRequestedTrueForEachTrigger(t *testing.T) {
	t.Parallel()

	cases := map[string]Redis{
		"tls flag":             {TLS: true},
		"tls_ca_file":          {TLSCAFile: "/ca.pem"},
		"tls_client_auth cert": {TLSClientAuthCertFile: "/cert.pem"},
		"tls_client_auth key":  {TLSClientAuthKeyFile: "/key.pem"},
		"tls_server_name":      {TLSServerName: "redis.internal"},
		"insecure_skip_verify": {TLSInsecureSkipVerify: true},
	}

	for name, r := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			assert.True(t, r.tlsRequested())
		})
	}
}

// TestUnmarshalCaddyfileSubscriptionsMaxSubscribers covers the cap directive and
// its pointer tri-state: an explicit `subscriptions_max_subscribers 0` (disable
// the cap) must be distinguishable from omission (transport default 100000).
func TestUnmarshalCaddyfileSubscriptionsMaxSubscribers(t *testing.T) {
	t.Parallel()

	t.Run("explicit value", func(t *testing.T) {
		t.Parallel()

		d := caddyfile.NewTestDispenser("redis {\n\turl redis://localhost:6379\n\tsubscriptions_max_subscribers 50000\n}")
		r := &Redis{}
		require.NoError(t, r.UnmarshalCaddyfile(d))
		require.NotNil(t, r.SubscriptionsMaxSubscribers)
		assert.Equal(t, 50000, *r.SubscriptionsMaxSubscribers)
	})

	t.Run("explicit zero disables the cap", func(t *testing.T) {
		t.Parallel()

		d := caddyfile.NewTestDispenser("redis {\n\turl redis://localhost:6379\n\tsubscriptions_max_subscribers 0\n}")
		r := &Redis{}
		require.NoError(t, r.UnmarshalCaddyfile(d))
		require.NotNil(t, r.SubscriptionsMaxSubscribers,
			"explicit 0 must allocate the pointer (distinguishable from omission)")
		assert.Equal(t, 0, *r.SubscriptionsMaxSubscribers)
	})

	t.Run("omitted leaves nil", func(t *testing.T) {
		t.Parallel()

		d := caddyfile.NewTestDispenser("redis {\n\turl redis://localhost:6379\n}")
		r := &Redis{}
		require.NoError(t, r.UnmarshalCaddyfile(d))
		assert.Nil(t, r.SubscriptionsMaxSubscribers, "omitted → nil → transport default applies")
	})
}

func TestUnmarshalCaddyfileAdmissionDirectives(t *testing.T) {
	t.Parallel()

	t.Run("max_count explicit / zero / omitted (pointer tri-state)", func(t *testing.T) {
		t.Parallel()

		d := caddyfile.NewTestDispenser("redis {\n\turl redis://localhost:6379\n\tsubscriber_max_count 50000\n}")
		r := &Redis{}
		require.NoError(t, r.UnmarshalCaddyfile(d))
		require.NotNil(t, r.SubscriberMaxCount)
		assert.Equal(t, 50000, *r.SubscriberMaxCount)

		d = caddyfile.NewTestDispenser("redis {\n\turl redis://localhost:6379\n\tsubscriber_max_count 0\n}")
		r = &Redis{}
		require.NoError(t, r.UnmarshalCaddyfile(d))
		require.NotNil(t, r.SubscriberMaxCount, "explicit 0 must allocate the pointer")
		assert.Equal(t, 0, *r.SubscriberMaxCount)

		d = caddyfile.NewTestDispenser("redis {\n\turl redis://localhost:6379\n}")
		r = &Redis{}
		require.NoError(t, r.UnmarshalCaddyfile(d))
		assert.Nil(t, r.SubscriberMaxCount, "omitted → nil → transport default")
	})

	t.Run("duration directives parse into their string fields", func(t *testing.T) {
		t.Parallel()

		d := caddyfile.NewTestDispenser("redis {\n\turl redis://localhost:6379\n" +
			"\tsubscriber_admission_timeout 5s\n\tsubscriber_registration_timeout 10s\n\tsubscriber_retry_after 3s\n}")
		r := &Redis{}
		require.NoError(t, r.UnmarshalCaddyfile(d))
		assert.Equal(t, "5s", r.SubscriberAdmissionTimeout)
		assert.Equal(t, "10s", r.SubscriberRegTimeout)
		assert.Equal(t, "3s", r.SubscriberRetryAfter)
	})
}

func TestAppendDurationOptionsAdmissionZeroSemantics(t *testing.T) {
	t.Parallel()

	// retry_after is always applied when set (0 = omit the header, distinct from
	// omitted = transport default 2s).
	opts, err := (&Redis{SubscriberRetryAfter: "0s"}).appendDurationOptions(nil)
	require.NoError(t, err)
	assert.Len(t, opts, 1, "subscriber_retry_after 0 must still produce an option")

	// admission_timeout (and registration_timeout) skip 0: 0 is the transport
	// default (fail-fast / unbounded), so omitting the option is equivalent.
	opts, err = (&Redis{SubscriberAdmissionTimeout: "0s"}).appendDurationOptions(nil)
	require.NoError(t, err)
	assert.Empty(t, opts, "subscriber_admission_timeout 0 = default = no option")

	// A positive admission timeout produces an option.
	opts, err = (&Redis{SubscriberAdmissionTimeout: "5s"}).appendDurationOptions(nil)
	require.NoError(t, err)
	assert.Len(t, opts, 1)

	// Negative is rejected.
	_, err = (&Redis{SubscriberRetryAfter: "-1s"}).appendDurationOptions(nil)
	require.ErrorIs(t, err, errNegativeOptionValue)
}
