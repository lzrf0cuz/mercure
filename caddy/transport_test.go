package caddy

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/dunglas/mercure"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeMetricsTransport wraps a real mercure.LocalTransport to satisfy
// mercure.Transport, plus implements TransportMetricsRegisterer so we can
// assert the binding call from bindTransportMetrics.
type fakeMetricsTransport struct {
	mercure.Transport

	registerCalls int
	registerArg   prometheus.Registerer
	registerErr   error
}

func (f *fakeMetricsTransport) RegisterMetricsWith(reg prometheus.Registerer) error {
	f.registerCalls++
	f.registerArg = reg

	return f.registerErr
}

func newFakeMetricsTransport(t *testing.T) *fakeMetricsTransport {
	t.Helper()

	inner := mercure.NewLocalTransport(mercure.NewSubscriberList(0))

	t.Cleanup(func() { _ = inner.Close(context.Background()) })

	return &fakeMetricsTransport{Transport: inner}
}

// TestBindTransportMetricsCallsRegister asserts the late-binding helper
// drives RegisterMetricsWith with the registry the parent module captured.
// Locks the WithValue typed-nil regression contract: Provision captures
// the registry BEFORE the WithValue chain (so it's not lost to the
// framework gotcha), then hands it to transports via this helper.
func TestBindTransportMetricsCallsRegister(t *testing.T) {
	t.Parallel()

	transport := newFakeMetricsTransport(t)
	reg := prometheus.NewRegistry()

	require.NoError(t, bindTransportMetrics(transport, reg, nil))
	assert.Equal(t, 1, transport.registerCalls,
		"transport implementing TransportMetricsRegisterer must be called exactly once")
	assert.Same(t, prometheus.Registerer(reg), transport.registerArg,
		"the registry passed in must reach RegisterMetricsWith verbatim — a copy or wrapper indicates the WithValue regression")
}

// TestBindTransportMetricsSkipsNonImplementers confirms transports that
// do not implement TransportMetricsRegisterer (Bolt, upstream Local in real
// deployments) are silently skipped — the helper must not panic on the
// failed type assertion and must not return an error.
func TestBindTransportMetricsSkipsNonImplementers(t *testing.T) {
	t.Parallel()

	// LocalTransport does not implement TransportMetricsRegisterer in the
	// upstream module — only the fork's redistransport does. Use it directly
	// here to lock the "skip silently" contract for non-fork transports.
	inner := mercure.NewLocalTransport(mercure.NewSubscriberList(0))

	t.Cleanup(func() { _ = inner.Close(context.Background()) })

	reg := prometheus.NewRegistry()

	require.NoError(t, bindTransportMetrics(inner, reg, nil),
		"non-implementer must produce no error — type assertion is structural, not configured")
}

// errRegistryRejected is the canned sentinel TestBindTransportMetricsPropagatesError
// uses so the assertion can errors.Is-check the cause without coupling to the
// message. Lifted to package scope to satisfy err113 (no dynamic errors at
// the test site).
var errRegistryRejected = errors.New("registry rejected the collector")

// TestBindTransportMetricsPropagatesError asserts a non-nil RegisterMetricsWith
// error is wrapped (not swallowed) so Provision fails fast. Without this,
// pool-collector registration failures would surface as a healthy module
// whose transport metrics are detached from the scraped registry.
func TestBindTransportMetricsPropagatesError(t *testing.T) {
	t.Parallel()

	transport := newFakeMetricsTransport(t)
	transport.registerErr = errRegistryRejected

	err := bindTransportMetrics(transport, prometheus.NewRegistry(), nil)
	require.ErrorIs(t, err, errRegistryRejected,
		"underlying error must remain wrapped so callers can errors.Is against the transport's failure mode")
	assert.Contains(t, err.Error(), "transport metrics registration failed",
		"wrap must include the helper's prefix so logs identify the binding stage")
}

// TestBindTransportMetricsTypedNilRegistry covers the typed-nil
// *prometheus.Registry path Caddy hands sub-modules. The helper is a
// pass-through, so the typed-nil reaches the transport's RegisterMetricsWith
// — the transport (RedisTransport) is responsible for normalizing it. Lock
// the helper-side contract: it must NOT panic on the type assertion or
// preempt the registry value, and the transport must receive whatever
// registry was passed in (typed-nil included). The fork's RedisTransport
// has its own typed-nil branch tested in metrics_test.go's
// TestRegisterMetricsWithTypedNilRegisterer.
func TestBindTransportMetricsTypedNilRegistry(t *testing.T) {
	t.Parallel()

	transport := newFakeMetricsTransport(t)

	var nilReg *prometheus.Registry // typed-nil

	require.NoError(t, bindTransportMetrics(transport, nilReg, nil),
		"helper must pass typed-nil through without normalizing — that's the transport's job")
	assert.Equal(t, 1, transport.registerCalls,
		"helper must still invoke RegisterMetricsWith on a typed-nil so transport-side normalization runs")
}

// TestBindTransportMetricsLogsSkipBreadcrumb locks the operator-facing
// Debug breadcrumb the helper emits when a transport does not implement
// TransportMetricsRegisterer. Without the breadcrumb, an operator chasing
// "why are my transport metrics empty?" has no signal whether binding ran
// or short-circuited; renaming the log key or dropping the call would
// silently regress observability without any test failure.
func TestBindTransportMetricsLogsSkipBreadcrumb(t *testing.T) {
	t.Parallel()

	// Real LocalTransport — does not implement TransportMetricsRegisterer.
	inner := mercure.NewLocalTransport(mercure.NewSubscriberList(0))

	t.Cleanup(func() { _ = inner.Close(context.Background()) })

	var buf bytes.Buffer

	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	require.NoError(t, bindTransportMetrics(inner, prometheus.NewRegistry(), logger))

	output := buf.String()
	assert.Contains(t, output, "transport metrics binding skipped",
		"Debug breadcrumb must fire when type assertion fails so operators can tell binding skipped from binding succeeded")
	assert.Contains(t, output, "transport_type",
		"breadcrumb must include the transport_type label so operators can identify which transport bypassed binding")
	assert.True(t, strings.Contains(output, "*mercure.LocalTransport") ||
		strings.Contains(output, "mercure.LocalTransport"),
		"transport_type label must reflect the actual transport's Go type — got: %s", output)
}

// TestBindTransportMetricsReloadIdempotent covers the Caddy-reload pattern:
// on a config reload the parent module reuses the same transport instance
// from TransportUsagePool. bindTransportMetrics is then called AGAIN with
// the same registry. The helper is a thin pass-through — it must call
// RegisterMetricsWith every time. Idempotency is the transport's
// responsibility (the real RedisTransport uses sync.Once; the fake here
// has no such gate and counts every call, which is the helper-level
// invariant under test).
func TestBindTransportMetricsReloadIdempotent(t *testing.T) {
	t.Parallel()

	transport := newFakeMetricsTransport(t)
	reg := prometheus.NewRegistry()

	require.NoError(t, bindTransportMetrics(transport, reg, nil))
	require.NoError(t, bindTransportMetrics(transport, reg, nil))

	assert.Equal(t, 2, transport.registerCalls,
		"helper must call RegisterMetricsWith every time — the transport-side gate (sync.Once on real RedisTransport) handles no-op semantics, not the helper")
	assert.Same(t, prometheus.Registerer(reg), transport.registerArg,
		"reload must pass the same registry instance both times")
}
