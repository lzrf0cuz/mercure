package caddy

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/dunglas/mercure"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBoltCleanupFrequency(t *testing.T) {
	t.Parallel()

	for block, want := range map[string]float64{
		"bolt {\n\tsize 5\n}":                          mercure.BoltDefaultCleanupFrequency,
		"bolt {\n\tsize 5\n\tcleanup_frequency 0\n}":   0,
		"bolt {\n\tsize 5\n\tcleanup_frequency 0.2\n}": 0.2,
	} {
		var b Bolt
		require.NoError(t, b.UnmarshalCaddyfile(caddyfile.NewTestDispenser(block)))
		assert.InDelta(t, want, b.cleanupFrequency(), 0)
	}
}

func TestBoltReloadReusesTransport(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mercure.db")

	wd, err := os.Getwd()
	require.NoError(t, err)

	relative, err := filepath.Rel(wd, path)
	require.NoError(t, err)

	a := &Bolt{Path: path}
	require.NoError(t, a.Provision(hubContext(t, "a")))
	t.Cleanup(func() { assert.NoError(t, a.Cleanup()) })

	for _, spelling := range []string{path, dir + "/./mercure.db", dir + "//mercure.db", relative} {
		b := &Bolt{Path: spelling}
		require.NoError(t, b.Provision(hubContext(t, "a")), spelling)
		assert.Same(t, a.transport, b.transport, spelling)
		require.NoError(t, b.Cleanup())
	}
}

func TestBoltReloadChangedOptions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mercure.db")

	a := &Bolt{Path: path}
	require.NoError(t, a.Provision(hubContext(t, "a")))
	t.Cleanup(func() { assert.NoError(t, a.Cleanup()) })

	// Writing the default bucket name is not a change.
	same := &Bolt{Path: path, BucketName: "updates"}
	require.NoError(t, same.Provision(hubContext(t, "a")))
	assert.Same(t, a.transport, same.transport)
	require.NoError(t, same.Cleanup())

	frequency := 0.5

	for _, tc := range []struct {
		want      string
		bolt      *Bolt
		cacheSize int
	}{
		{`bucket_name "updates" -> "other"`, &Bolt{Path: path, BucketName: "other"}, 0},
		{"size 0 -> 5", &Bolt{Path: path, Size: 5}, 0},
		{"cleanup_frequency 0.3 -> 0.5", &Bolt{Path: path, CleanupFrequency: &frequency}, 0},
		{"subscriber_list_cache_size 0 -> 5", &Bolt{Path: path}, 5},
	} {
		ctx := hubContext(t, "a")

		err := tc.bolt.Provision(ctx.WithValue(SubscriberListCacheSizeContextKey, tc.cacheSize))
		require.ErrorIs(t, err, errTransportOptionsChanged)
		require.ErrorContains(t, err, tc.want)
		require.ErrorContains(t, err, "restart Caddy")

		// Caddy calls Cleanup when Provision fails.
		require.NoError(t, tc.bolt.Cleanup())
	}

	refs, ok := TransportUsagePool.References(a.transportKey)
	assert.True(t, ok)
	assert.Equal(t, 1, refs, "a refused reload must leave the open transport to the running config")
}

// A reload builds the new config before it cleans up the old one, while the
// old config still holds the database file.
func TestBoltReloadInCaddy(t *testing.T) {
	t.Cleanup(func() { assert.NoError(t, caddy.Stop()) })

	dir := t.TempDir()
	config := func(transport string) []byte {
		return fmt.Appendf(nil, `{"admin":{"disabled":true,"config":{"persist":false}},"apps":{"http":{"servers":{"srv0":{"listen":["127.0.0.1:0"],"automatic_https":{"disable":true},"routes":[{"handle":[{"handler":"mercure","anonymous":true,"transport":%s,"issuers":[{"identifier":"https://example.com","publisher":{"jwt":{"key":"test-publisher-key","alg":"HS256"}}}]}]}]}}}}}`, transport)
	}

	require.NoError(t, caddy.Load(config(fmt.Sprintf(`{"name":"bolt","path":%q}`, filepath.Join(dir, "mercure.db"))), true))
	require.NoError(t, caddy.Load(config(fmt.Sprintf(`{"name":"bolt","path":%q}`, dir+"/./mercure.db")), true))
	require.ErrorContains(t, caddy.Load(config(fmt.Sprintf(`{"name":"bolt","path":%q,"size":5}`, dir+"/mercure.db")), true), "size 0 -> 5")

	refs, ok := TransportUsagePool.References(boltTransportKey{filepath.Join(dir, "mercure.db"), "default"})
	assert.True(t, ok)
	assert.Equal(t, 1, refs, "the running config must keep the open transport")
}
