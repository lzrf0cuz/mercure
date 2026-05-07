package mercure

import (
	"errors"

	"github.com/dunglas/mercure/common"
	"github.com/prometheus/client_golang/prometheus"
)

const metricsPath = "/metrics"

// labelReason names the Prometheus label carrying a closed reason taxonomy. Shared by the
// disconnect, publish-failure and claim-header-rejection collectors.
const labelReason = "reason"

type Metrics interface {
	// SubscriberConnected collects metrics about subscriber connections.
	SubscriberConnected(s *LocalSubscriber)
	// SubscriberDisconnected collects metrics about subscriber disconnections.
	SubscriberDisconnected(s *LocalSubscriber)
	// UpdatePublished collects metrics about update publications.
	UpdatePublished(u *Update)
}

// DisconnectReason classifies why a subscriber's connection ended.
// SubscribeHandler observes the exit path and reports it via the optional
// DisconnectReasonReporter interface. The taxonomy is closed (bounded label
// cardinality) so it is safe as a Prometheus label.
type DisconnectReason string

const (
	// DisconnectReasonClientClosed: the request's context was cancelled
	// (TCP close from the client, browser tab closed, or upstream proxy
	// dropped the connection). The dominant healthy steady-state reason.
	DisconnectReasonClientClosed DisconnectReason = "client_closed"
	// DisconnectReasonHubShutdown: the hub's root context was cancelled
	// (Caddy "stopping" event, SIGTERM, graceful config reload). Only
	// fires when writeTimeout=0 — otherwise the per-connection deadline
	// closes the subscriber on its own and reports as write_timeout.
	DisconnectReasonHubShutdown DisconnectReason = "hub_shutdown"
	// DisconnectReasonWriteTimeout: the per-connection write deadline
	// (writeTimeout, optionally shortened by JWT expiry) was reached.
	// This is a clean periodic recycle, not an error condition.
	DisconnectReasonWriteTimeout DisconnectReason = "write_timeout"
	// DisconnectReasonWriteFailed: a write to the response stream returned
	// an error (broken pipe, deadline exceeded mid-write, …). Distinct
	// from write_timeout — write_timeout is "deadline reached cleanly
	// before any further write was attempted".
	DisconnectReasonWriteFailed DisconnectReason = "write_failed"
	// DisconnectReasonTransportEnded: the subscriber's update channel was
	// closed by the transport (e.g. Bolt or Redis transport's
	// disconnectAllSubscribers during Close, or backpressure-triggered
	// handleFullChan). The connection itself was still healthy.
	DisconnectReasonTransportEnded DisconnectReason = "transport_ended"
	// DisconnectReasonTransportError: transport.RemoveSubscriber returned
	// a non-nil error during shutdown. Pairs with the existing
	// "Failed to remove subscriber on shutdown" Error log.
	DisconnectReasonTransportError DisconnectReason = "transport_error"
	// DisconnectReasonUnknown: defensive fallback when no specific reason
	// was set before the SubscribeHandler returned. Should never fire in
	// practice; non-zero rate is a code-coverage gap to investigate.
	DisconnectReasonUnknown DisconnectReason = "unknown"
)

// DisconnectReasonReporter is the optional extension to Metrics that lets
// the hub report a per-disconnect reason label. The interface is opt-in:
// implementations that want only the legacy SubscriberDisconnected hook
// keep working unchanged.
//
// PrometheusMetrics implements it; SubscribeHandler type-asserts and
// prefers this method over SubscriberDisconnected when both are available.
type DisconnectReasonReporter interface {
	SubscriberDisconnectedWithReason(s *LocalSubscriber, reason DisconnectReason)
}

// WriteFlushObserver is the optional extension to Metrics that lets the hub
// record per-event write+flush latency on the SSE delivery hot path. The
// interface is opt-in: implementations that don't care about latency just
// don't implement it and SubscribeHandler skips the timing.
//
// PrometheusMetrics implements it. The hub type-asserts h.metrics once per
// h.write call (cheap, branch-predicted) and reports outcome ok|failed so
// failed writes stay distinguishable from healthy ones in the histogram —
// a slow broken-pipe failure looks very different from a slow healthy write.
//
// Outcome ok|failed is intentionally a two-value label: failed collapses
// every error mode in h.write (deadline-set error, Write error, flush
// error, deadline-reset error) into one bucket. Operators can still
// disambiguate via the surrounding signals — the write/deadline helpers
// log the underlying error class, and mercure_subscriber_disconnects_total
// labels disconnects by reason — so widening the histogram's label set
// would only inflate cardinality without adding diagnostic value.
type WriteFlushObserver interface {
	ObserveWriteFlush(seconds float64, ok bool)
}

// PublishFailureReason classifies why a publish did not complete. The taxonomy
// is closed (bounded label cardinality) so it is safe as a Prometheus label.
type PublishFailureReason string

const (
	// PublishFailureReasonValidation: the update was rejected by topic/id/type
	// rules (topic count/length, reserved-topic forgery, CR/LF/NUL in
	// topic/id/type) — either by PublishHandler's early validateTopics or by
	// update.Validate() in Hub.Publish. A client error; never reached the transport.
	PublishFailureReasonValidation PublishFailureReason = "validation"
	// PublishFailureReasonTransport: the catch-all for dispatch failures that
	// are NOT an attributed publish_timeout — Redis/Valkey unreachable, XADD
	// failure, codec error, and also any deadline error not tagged with the
	// ErrPublishTimeout cause (parent-context cancellation, or a stall when
	// publish_timeout is unset).
	PublishFailureReasonTransport PublishFailureReason = "transport"
	// PublishFailureReasonTimeout: dispatch was aborted by publish_timeout —
	// the same condition PublishHandler maps to 504. Kept distinct from
	// transport so a stalled-dispatch alert doesn't blur with hard errors.
	PublishFailureReasonTimeout PublishFailureReason = "timeout"
)

// PublishFailureReporter is the optional extension to Metrics that lets the hub
// count failed publishes labeled by reason. Opt-in: an implementation that does
// not provide it keeps the original Metrics interface working unchanged, and
// Hub.Publish type-asserts h.metrics once per failed publish (cheap, off the
// success path). Pairs with mercure_updates_total (successful dispatches) so a
// publish failure rate is observable, not just log-visible.
//
// PrometheusMetrics implements it.
type PublishFailureReporter interface {
	UpdatePublishFailed(u *Update, reason PublishFailureReason)
}

// AuthorizationRejectionReporter is the optional extension to Metrics that lets the hub
// count claim-header binding rejections, labeled by binding and reason. Opt-in: an
// implementation that does not provide it keeps the base Metrics interface working
// unchanged, and authorizeAndBind type-asserts h.metrics once per rejection.
//
// This is the only meter for HTTP-layer authorization rejections: mercure_updates_total's
// failure counterpart explicitly excludes them, and auth failures are otherwise Debug-log
// only. Without it, a misconfigured binding that 401s an entire tenant is invisible until
// someone reports it, so NewHub warns at startup when bindings are configured and the
// Metrics implementation does not satisfy this interface. Per-request Warn logging is
// deliberately not used instead — an attacker could amplify it.
//
// PrometheusMetrics implements it.
type AuthorizationRejectionReporter interface {
	AuthorizationRejected(binding string, reason AuthzRejectReason)
}

type NopMetrics struct{}

func (NopMetrics) SubscriberConnected(_ *LocalSubscriber)    {}
func (NopMetrics) SubscriberDisconnected(_ *LocalSubscriber) {}
func (NopMetrics) UpdatePublished(_ *Update)                 {}

// PrometheusMetrics store Hub collected metrics.
type PrometheusMetrics struct {
	registry                   prometheus.Registerer
	subscribersTotal           prometheus.Counter
	subscribers                prometheus.Gauge
	updatesTotal               prometheus.Counter
	subscriberDisconnectsTotal *prometheus.CounterVec
	writeFlushLatency          *prometheus.HistogramVec
	updatesFailedTotal         *prometheus.CounterVec
	claimHeaderRejectedTotal   *prometheus.CounterVec
}

// NewPrometheusMetrics creates a Prometheus metrics collector.
// This method must be called only one time, or it will panic.
//
// Length is inherent: declaring every collector inline keeps the labels,
// help text, and bucket lists co-located with the field they back, which
// is the readable shape for Prometheus instrumentation. Splitting into
// per-metric helpers would scatter related opts across the file without
// reducing real complexity.
//
//nolint:funlen // see comment above.
func NewPrometheusMetrics(registry prometheus.Registerer) *PrometheusMetrics {
	if registry == nil {
		registry = prometheus.NewRegistry()
	}

	m := &PrometheusMetrics{
		registry: registry,
		subscribersTotal: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "mercure_subscribers_total",
				Help: "Total number of subscribers accepted by the hub since startup (cumulative — includes those that have since disconnected). Pair with mercure_subscribers_connected to compute churn.",
			},
		),
		subscribers: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "mercure_subscribers_connected",
				Help: "Number of subscribers currently connected to the hub (instantaneous).",
			},
		),
		updatesTotal: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "mercure_updates_total",
				Help: "Total number of updates accepted by the hub for dispatch since startup (cumulative; counts publish-time, not per-subscriber delivery).",
			},
		),
		subscriberDisconnectsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "mercure_subscriber_disconnects_total",
				Help: "Subscriber disconnections labeled by reason (client_closed | hub_shutdown | write_timeout | write_failed | transport_ended | transport_error | unknown). Pair with mercure_subscribers_connected for cause-tagged churn analysis.",
			},
			[]string{labelReason},
		),
		writeFlushLatency: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name: "mercure_subscribe_write_flush_seconds",
				Help: "Time spent in the per-event SSE write+flush path (set deadline → Write → Flush → reset deadline) labeled by outcome (ok|failed). Failed writes also record their latency so a slow broken-pipe stays distinguishable from a slow healthy write. Tail latency here pairs with mercure_redis_xreadgroup_latency_seconds to localize hub-side vs transport-side stalls.",
				// Buckets cover healthy SSE writes (sub-ms) up to dispatchTimeout-bound
				// failures. The explicit 5s bucket sits at DefaultDispatchTimeout so
				// the dispatch-deadline-exceeded p99 lands on a real boundary instead
				// of being interpolated across 2.5s..10s.
				Buckets: []float64{0.0001, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 1, 2.5, 5, 10},
			},
			[]string{"outcome"},
		),
		updatesFailedTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "mercure_updates_failed_total",
				Help: "Updates that failed to publish, labeled by reason (validation | transport | timeout). Counts validation + dispatch failures only — HTTP-layer rejections (auth, malformed request) are not included. Pair with mercure_updates_total for a success/failure ratio.",
			},
			[]string{labelReason},
		),
		claimHeaderRejectedTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "mercure_claim_header_rejected_total",
				Help: "Requests rejected by a claim-header authorization binding, labeled by binding (claim:Canonical-Header, e.g. tenants:Tenant-Id) and reason (header_absent | claim_absent | mismatch | malformed). Cardinality is bounded: the binding label is operator-configured, never request-controlled. Auth rejections are otherwise Debug-log only, so this is the signal that a misconfigured binding is 401ing a whole tenant — a sustained claim_absent/header_absent rate points at a token issuer or edge misconfiguration, while a mismatch spike may indicate token replay.",
			},
			[]string{"binding", labelReason},
		),
	}

	// https://github.com/caddyserver/caddy/pull/6820
	if err := m.registry.Register(m.subscribers); err != nil &&
		!errors.As(err, &prometheus.AlreadyRegisteredError{}) {
		panic(err)
	}

	if err := m.registry.Register(m.subscribersTotal); err != nil &&
		!errors.As(err, &prometheus.AlreadyRegisteredError{}) {
		panic(err)
	}

	if err := m.registry.Register(m.updatesTotal); err != nil &&
		!errors.As(err, &prometheus.AlreadyRegisteredError{}) {
		panic(err)
	}

	// Caddy reuses a Prometheus registry across config reloads (see
	// https://github.com/caddyserver/caddy/pull/6820). On reload, Register
	// returns AlreadyRegisteredError; we MUST adopt the registry's
	// existing collector — otherwise m.subscriberDisconnectsTotal points
	// at a freshly-allocated, unregistered CounterVec and post-reload
	// SubscriberDisconnectedWithReason calls increment a detached
	// collector that scrapes never see. The older counters above retain
	// the inherited "ignore AlreadyRegistered" pattern; this adoption
	// block is intentional only for the metrics added here.
	if err := m.registry.Register(m.subscriberDisconnectsTotal); err != nil {
		var existing prometheus.AlreadyRegisteredError
		if !errors.As(err, &existing) {
			panic(err)
		}

		adopted, ok := existing.ExistingCollector.(*prometheus.CounterVec)
		if !ok {
			panic("mercure: existing collector for mercure_subscriber_disconnects_total is not *prometheus.CounterVec")
		}

		m.subscriberDisconnectsTotal = adopted
	}

	// Same Caddy-reload adoption pattern as subscriberDisconnectsTotal: a
	// detached HistogramVec post-reload would silently swallow every
	// ObserveWriteFlush call.
	if err := m.registry.Register(m.writeFlushLatency); err != nil {
		var existing prometheus.AlreadyRegisteredError
		if !errors.As(err, &existing) {
			panic(err)
		}

		adopted, ok := existing.ExistingCollector.(*prometheus.HistogramVec)
		if !ok {
			panic("mercure: existing collector for mercure_subscribe_write_flush_seconds is not *prometheus.HistogramVec")
		}

		m.writeFlushLatency = adopted
	}

	// Same Caddy-reload adoption pattern: a detached CounterVec post-reload
	// would silently drop every UpdatePublishFailed call.
	if err := m.registry.Register(m.updatesFailedTotal); err != nil {
		var existing prometheus.AlreadyRegisteredError
		if !errors.As(err, &existing) {
			panic(err)
		}

		adopted, ok := existing.ExistingCollector.(*prometheus.CounterVec)
		if !ok {
			panic("mercure: existing collector for mercure_updates_failed_total is not *prometheus.CounterVec")
		}

		m.updatesFailedTotal = adopted
	}

	// Same Caddy-reload adoption pattern: a detached CounterVec post-reload would
	// silently drop every AuthorizationRejected call, hiding a misconfigured binding.
	if err := m.registry.Register(m.claimHeaderRejectedTotal); err != nil {
		var existing prometheus.AlreadyRegisteredError
		if !errors.As(err, &existing) {
			panic(err)
		}

		adopted, ok := existing.ExistingCollector.(*prometheus.CounterVec)
		if !ok {
			panic("mercure: existing collector for mercure_claim_header_rejected_total is not *prometheus.CounterVec")
		}

		m.claimHeaderRejectedTotal = adopted
	}

	if err := m.registry.Register(common.AppVersion.NewMetricsCollector()); err != nil &&
		!errors.As(err, &prometheus.AlreadyRegisteredError{}) {
		panic(err)
	}

	return m
}

func (m *PrometheusMetrics) SubscriberConnected(_ *LocalSubscriber) {
	m.subscribersTotal.Inc()
	m.subscribers.Inc()
}

func (m *PrometheusMetrics) SubscriberDisconnected(_ *LocalSubscriber) {
	m.subscribers.Dec()
}

// SubscriberDisconnectedWithReason decrements the subscribers gauge AND
// increments the per-reason disconnect counter. SubscribeHandler prefers
// this over SubscriberDisconnected via type-assertion when available, so
// implementations gain reason attribution without breaking the original
// Metrics interface.
func (m *PrometheusMetrics) SubscriberDisconnectedWithReason(_ *LocalSubscriber, reason DisconnectReason) {
	m.subscribers.Dec()
	m.subscriberDisconnectsTotal.WithLabelValues(string(reason)).Inc()
}

func (m *PrometheusMetrics) UpdatePublished(_ *Update) {
	m.updatesTotal.Inc()
}

// UpdatePublishFailed increments the per-reason publish-failure counter.
// Hub.Publish calls this via the optional PublishFailureReporter type-assertion
// when an update is rejected (validation) or fails to dispatch (transport /
// timeout), so failures are metered, not only logged.
func (m *PrometheusMetrics) UpdatePublishFailed(_ *Update, reason PublishFailureReason) {
	m.updatesFailedTotal.WithLabelValues(string(reason)).Inc()
}

// AuthorizationRejected increments the per-binding, per-reason rejection counter. The reason
// is passed typed — never inferred by parsing an error string.
func (m *PrometheusMetrics) AuthorizationRejected(binding string, reason AuthzRejectReason) {
	m.claimHeaderRejectedTotal.WithLabelValues(binding, string(reason)).Inc()
}

// ObserveWriteFlush records the time spent in one SSE write+flush cycle,
// labeled by outcome — "ok" when both the Write and the Flush succeeded
// and the surrounding deadline calls returned without error, "failed"
// when any of those steps returned an error. Failed observations are
// kept (not skipped) so the histogram retains the latency distribution
// of error paths, which is what makes it useful for distinguishing
// "broken pipe returned in 200µs" from "deadline exceeded near dispatchTimeout".
func (m *PrometheusMetrics) ObserveWriteFlush(seconds float64, ok bool) {
	outcome := "ok"
	if !ok {
		outcome = "failed"
	}

	m.writeFlushLatency.WithLabelValues(outcome).Observe(seconds)
}
