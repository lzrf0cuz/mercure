package caddy

import (
	"context"
	"errors"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	"github.com/caddyserver/caddy/v2"
	"github.com/dunglas/mercure"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func hubContext(t *testing.T, name string) caddy.Context {
	t.Helper()

	ctx, cancel := caddy.NewContext(caddy.Context{Context: context.Background()})
	t.Cleanup(cancel)

	ctx = ctx.WithValue(HubNameContextKey, name)

	return ctx.WithValue(SubscriberListCacheSizeContextKey, 0)
}

func TestLocalTransportScopedByHub(t *testing.T) {
	provision := func(name string) *Local {
		l := &Local{}
		require.NoError(t, l.Provision(hubContext(t, name)))
		t.Cleanup(func() { assert.NoError(t, l.Cleanup()) })

		return l
	}

	a, b, a2 := provision("a"), provision("b"), provision("a")
	assert.NotSame(t, a.transport, b.transport)
	assert.Same(t, a.transport, a2.transport)
}

func TestLocalTransportChangedCacheSize(t *testing.T) {
	a := &Local{}
	require.NoError(t, a.Provision(hubContext(t, "a")))
	t.Cleanup(func() { assert.NoError(t, a.Cleanup()) })

	ctx := hubContext(t, "a")

	b := &Local{}
	err := b.Provision(ctx.WithValue(SubscriberListCacheSizeContextKey, 5))
	require.ErrorIs(t, err, errTransportOptionsChanged)
	require.ErrorContains(t, err, "subscriber_list_cache_size 0 -> 5")

	// Caddy calls Cleanup when Provision fails.
	require.NoError(t, b.Cleanup())

	refs, ok := TransportUsagePool.References(a.key)
	assert.True(t, ok)
	assert.Equal(t, 1, refs, "a refused reload must leave the transport to the running config")
}

func TestBoltTransportScopedByHub(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mercure.db")

	a := &Bolt{Path: path}
	require.NoError(t, a.Provision(hubContext(t, "a")))
	t.Cleanup(func() { assert.NoError(t, a.Cleanup()) })

	a2 := &Bolt{Path: path}
	require.NoError(t, a2.Provision(hubContext(t, "a")))
	t.Cleanup(func() { assert.NoError(t, a2.Cleanup()) })
	assert.Same(t, a.transport, a2.transport)

	b := &Bolt{Path: path}
	require.ErrorContains(t, b.Provision(hubContext(t, "b")), "give each hub its own path")
}

// fakeMetricsTransport records every registry it is bound to.
type fakeMetricsTransport struct {
	*mercure.LocalTransport

	mu         sync.Mutex
	registries []prometheus.Registerer
	err        error
}

func (f *fakeMetricsTransport) RegisterMetricsWith(registerer prometheus.Registerer) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.registries = append(f.registries, registerer)

	return f.err
}

func (f *fakeMetricsTransport) bound() []prometheus.Registerer {
	f.mu.Lock()
	defer f.mu.Unlock()

	return slices.Clone(f.registries)
}

// fakeMetricsModule is a pooled transport module, like Bolt and Local, whose
// transport implements TransportMetricsRegisterer.
type fakeMetricsModule struct {
	transport *fakeMetricsTransport
}

var (
	fakeMetricsCreated   []*fakeMetricsTransport
	fakeMetricsCreatedMu sync.Mutex
	registerFakeMetrics  = sync.OnceFunc(func() { caddy.RegisterModule(&fakeMetricsModule{}) })
)

func (*fakeMetricsModule) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "http.handlers.mercure.fakemetrics",
		New: func() caddy.Module { return new(fakeMetricsModule) },
	}
}

func (f *fakeMetricsModule) GetTransport() mercure.Transport {
	return f.transport
}

func (f *fakeMetricsModule) Provision(caddy.Context) error {
	destructor, _, err := TransportUsagePool.LoadOrNew("fakemetrics", func() (caddy.Destructor, error) {
		t := &fakeMetricsTransport{LocalTransport: mercure.NewLocalTransport(mercure.NewSubscriberList(0))}

		fakeMetricsCreatedMu.Lock()
		defer fakeMetricsCreatedMu.Unlock()

		fakeMetricsCreated = append(fakeMetricsCreated, t)

		return TransportDestructor[*fakeMetricsTransport]{Transport: t}, nil
	})
	if err != nil {
		return err
	}

	f.transport = destructor.(TransportDestructor[*fakeMetricsTransport]).Transport

	return nil
}

func (*fakeMetricsModule) Cleanup() error {
	_, err := TransportUsagePool.Delete("fakemetrics")

	return err
}

// A reload builds a new metrics registry and reuses the pooled transport:
// Provision must hand that transport each config's own registry, the one the
// hub's metrics are registered with.
func TestTransportMetricsBoundOnEachReload(t *testing.T) {
	registerFakeMetrics()
	t.Cleanup(func() { assert.NoError(t, caddy.Stop()) })

	fakeMetricsCreatedMu.Lock()
	fakeMetricsCreated = nil
	fakeMetricsCreatedMu.Unlock()

	config := []byte(`{"admin":{"disabled":true,"config":{"persist":false}},"apps":{"http":{"servers":{"srv0":{"listen":["127.0.0.1:0"],"automatic_https":{"disable":true},"routes":[{"handle":[{"handler":"mercure","name":"metrics","anonymous":true,"transport":{"name":"fakemetrics"},"issuers":[{"identifier":"https://example.com","publisher":{"jwt":{"key":"test-publisher-key","alg":"HS256"}}}]}]}]}}}}}`)

	require.NoError(t, caddy.Load(config, true))
	require.NoError(t, caddy.Load(config, true))

	fakeMetricsCreatedMu.Lock()
	created := slices.Clone(fakeMetricsCreated)
	fakeMetricsCreatedMu.Unlock()

	require.Len(t, created, 1, "the reload must reuse the pooled transport")

	registries := created[0].bound()
	require.Len(t, registries, 2, "each config load must bind the transport once")
	assert.NotSame(t, registries[0], registries[1], "a reload must bind the new config's registry")

	for i, r := range registries {
		require.NotNil(t, r, "load %d bound a nil registry", i)

		gatherer, ok := r.(prometheus.Gatherer)
		require.True(t, ok)

		families, err := gatherer.Gather()
		require.NoError(t, err)

		names := make([]string, 0, len(families))
		for _, f := range families {
			names = append(names, f.GetName())
		}

		assert.Contains(t, names, "mercure_subscribers_connected", "load %d: the bound registry must be the one the hub's metrics use", i)
	}
}

var errRegistryRejected = errors.New("registry rejected the collector")

func TestBindTransportMetricsPropagatesError(t *testing.T) {
	t.Parallel()

	transport := &fakeMetricsTransport{LocalTransport: mercure.NewLocalTransport(mercure.NewSubscriberList(0)), err: errRegistryRejected}

	require.ErrorIs(t, bindTransportMetrics(transport, prometheus.NewRegistry(), nil), errRegistryRejected)
}
