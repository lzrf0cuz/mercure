package mercure

import (
	"errors"
	"fmt"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// labelReason names the Prometheus label carrying a closed reason taxonomy. Shared by the
// disconnect, publish-failure, claim-header rejection and claim-value rejection collectors.
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
	// dropped the connection).
	DisconnectReasonClientClosed DisconnectReason = "client_closed"
	// DisconnectReasonHubShutdown: the hub's root context was cancelled.
	// Caddy cancels it on its "stopping" event, which fires on a stop
	// (caddy stop, SIGTERM) and, for the old config, on every applied reload
	// (a changed or forced config; Caddy skips an unchanged one). Only fires
	// when writeTimeout=0: otherwise the loop does not watch that context,
	// and the connection stays open until its write deadline
	// (write_timeout), the client leaves (client_closed), a write fails
	// (write_failed), or its transport is closed (transport_ended): by a
	// reload that drops or changes the transport, or by a stop once Caddy's
	// grace_period has ended.
	DisconnectReasonHubShutdown DisconnectReason = "hub_shutdown"
	// DisconnectReasonWriteTimeout: the per-connection write deadline
	// (writeTimeout, optionally shortened by JWT expiry) was reached.
	// With writeTimeout=0 it also fires when the token's exp alone set the
	// deadline. This is a clean periodic recycle, not an error condition.
	DisconnectReasonWriteTimeout DisconnectReason = "write_timeout"
	// DisconnectReasonWriteFailed: a write to the response stream returned
	// an error (broken pipe, deadline exceeded mid-write, …). Distinct
	// from write_timeout — write_timeout is "deadline reached cleanly
	// before any further write was attempted".
	DisconnectReasonWriteFailed DisconnectReason = "write_failed"
	// DisconnectReasonTransportEnded: the subscriber's update channel was
	// closed by the transport (e.g. Local's, Bolt's or Redis transport's
	// Close, or backpressure-triggered handleFullChan). The connection
	// itself was still healthy. Under Caddy with writeTimeout>0, a reload
	// that drops or changes the hub's transport closes the old one and
	// reports this; a reload that keeps the transport reuses it, so the
	// connection stays open. On a stop the transport is closed only after
	// the servers have finished shutting down: with Caddy's grace_period
	// set, subscribers still open when it ends report this; with the
	// default unlimited grace period the stop waits for each of them to end
	// another way (write_timeout, client_closed, write_failed).
	DisconnectReasonTransportEnded DisconnectReason = "transport_ended"
	// DisconnectReasonTransportError: recorded only when
	// transport.RemoveSubscriber returns an error other than
	// ErrClosedTransport on an exit that has no other reason. Pairs with the
	// "Failed to remove subscriber on shutdown" Error log. The in-tree
	// transports only return ErrClosedTransport from RemoveSubscriber, so in
	// practice it comes from a third-party transport.
	DisconnectReasonTransportError DisconnectReason = "transport_error"
	// DisconnectReasonUnknown: defensive fallback when no specific reason
	// was set before the SubscribeHandler returned. Should never fire in
	// practice; non-zero rate is a code-coverage gap to investigate.
	DisconnectReasonUnknown DisconnectReason = "unknown"
)

// DisconnectReasonReporter is the optional extension to Metrics that lets
// the hub report a per-disconnect reason label. The interface is opt-in:
// implementations that only provide SubscriberDisconnected keep working unchanged.
//
// PrometheusMetrics implements it; SubscribeHandler type-asserts and
// prefers this method over SubscriberDisconnected when both are available.
type DisconnectReasonReporter interface {
	SubscriberDisconnectedWithReason(s *LocalSubscriber, reason DisconnectReason)
}

// WriteFlushObserver is the optional extension to Metrics that lets the hub
// record per-event write+flush latency on the SSE delivery path. PrometheusMetrics
// implements it. The outcome is ok or failed; failed covers every error in
// h.write (deadline set, Write, Flush, deadline reset), and the write helpers'
// logs and mercure_subscriber_disconnects_total tell the failure modes apart.
type WriteFlushObserver interface {
	ObserveWriteFlush(seconds float64, ok bool)
}

// PublishFailureReason classifies why a publish did not complete. The taxonomy
// is closed (bounded label cardinality) so it is safe as a Prometheus label.
type PublishFailureReason string

const (
	// PublishFailureReasonValidation: the update's content was rejected before
	// dispatch — by PublishHandler's early checks (missing, too many or invalid
	// topics, invalid retry) or by any update.Validate() rejection in
	// Hub.Publish, or by a transport refusing the update as too large to store
	// (ErrCodecPayloadTooLarge, 413). A client error; nothing was stored.
	// ParseForm failures (unreadable or oversized body, 400/413) and auth
	// rejections are not counted.
	PublishFailureReasonValidation PublishFailureReason = "validation"
	// PublishFailureReasonTransport: the catch-all for dispatch failures that
	// are not an attributed publish_timeout: Redis/Valkey unreachable, XADD
	// failure, codec error, and also any deadline error not tagged with the
	// ErrPublishTimeout cause (parent-context cancellation, or a stall when
	// publish_timeout is unset). It also counts a publish refused because the
	// transport is closed, e.g. during shutdown.
	PublishFailureReasonTransport PublishFailureReason = "transport"
	// PublishFailureReasonTimeout: dispatch was aborted by publish_timeout —
	// the same condition PublishHandler maps to 504. Kept distinct from
	// transport so a stalled-dispatch alert doesn't blur with hard errors.
	PublishFailureReasonTimeout PublishFailureReason = "timeout"
)

// PublishFailureReporter is the optional extension to Metrics that lets the hub
// count failed publishes labeled by reason. Opt-in: Hub.Publish and
// PublishHandler's early checks type-assert h.metrics once per failed publish.
// Pairs with mercure_updates_total (successful dispatches) so a publish failure
// rate is observable, not just log-visible.
//
// PrometheusMetrics implements it.
type PublishFailureReporter interface {
	UpdatePublishFailed(u *Update, reason PublishFailureReason)
}

// AuthzRejectReason classifies why a binding rejected a request. The taxonomy is closed
// (header_absent | claim_absent | mismatch | malformed), so it is safe as a Prometheus label.
type AuthzRejectReason string

// AuthorizationRejectionReporter is the optional extension to Metrics that lets the hub
// count claim-header binding rejections, labeled by binding and reason. Opt-in: an
// implementation that does not provide it keeps the base Metrics interface working
// unchanged, and reportRejection, called by enforceBindings, type-asserts h.metrics once per
// rejection.
//
// It is the hub's only meter that attributes a header-binding rejection to its binding and
// reason, so NewHub warns at startup when a header binding is configured and the Metrics
// implementation does not satisfy this interface. Value-binding rejections are metered by
// ClaimValueRejectionReporter.
//
// PrometheusMetrics implements it.
type AuthorizationRejectionReporter interface {
	AuthorizationRejected(binding string, reason AuthzRejectReason)
}

// ClaimValueRejectionReporter is the optional extension to Metrics that lets the hub count
// rejections by a claim-value binding (NewClaimValueBinding), labeled by claim and reason.
// The reason is claim_absent, mismatch or malformed; a value binding reads no header, so
// header_absent never occurs. It is opt-in and type-asserted once per rejection, like
// AuthorizationRejectionReporter, which never receives value-binding rejections.
//
// PrometheusMetrics implements it.
type ClaimValueRejectionReporter interface {
	ClaimValueRejected(claim string, reason AuthzRejectReason)
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
	claimValueRejectedTotal    *prometheus.CounterVec
	subscribersByBindingValue  *bindingValueSubscribers
}

// bindingValue is the header value matched by the binding that counts subscribers, with the
// identity of that binding (the claim:Canonical-Header string of the binding label of
// mercure_claim_header_rejected_total). An empty value means the subscriber is not counted.
type bindingValue struct {
	binding string
	value   string
}

// bindingValueSubscribers counts connected subscribers per binding value and exposes the counts
// as the mercure_subscribers_by_binding_value gauge. The counts and the gauge are one registered
// collector, so every PrometheusMetrics sharing a registry adopts the counts together with the
// gauge. mu covers both.
type bindingValueSubscribers struct {
	mu     sync.Mutex
	counts map[bindingValue]int
	gauge  *prometheus.GaugeVec
}

func newBindingValueSubscribers() *bindingValueSubscribers {
	return &bindingValueSubscribers{
		counts: make(map[bindingValue]int),
		gauge: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "mercure_subscribers_by_binding_value",
				Help: "Connected subscribers per binding value, labeled by binding (claim:Canonical-Header, as in mercure_claim_header_rejected_total) and value (the header value matched by the binding that counts subscribers).",
			},
			[]string{"binding", "value"},
		),
	}
}

func (b *bindingValueSubscribers) Describe(ch chan<- *prometheus.Desc) {
	b.gauge.Describe(ch)
}

func (b *bindingValueSubscribers) Collect(ch chan<- prometheus.Metric) {
	b.gauge.Collect(ch)
}

func (b *bindingValueSubscribers) connected(bv bindingValue) {
	if bv.value == "" {
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	n := b.counts[bv] + 1
	b.counts[bv] = n
	b.gauge.WithLabelValues(bv.binding, bv.value).Set(float64(n))
}

// disconnected decrements the value's count. The last disconnect deletes the series, and a
// value with no count is ignored, so the gauge never goes negative.
func (b *bindingValueSubscribers) disconnected(bv bindingValue) {
	if bv.value == "" {
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	n, ok := b.counts[bv]

	switch {
	case !ok:
		return
	case n > 1:
		b.counts[bv] = n - 1
		b.gauge.WithLabelValues(bv.binding, bv.value).Set(float64(n - 1))
	default:
		delete(b.counts, bv)
		b.gauge.DeleteLabelValues(bv.binding, bv.value)
	}
}

// NewPrometheusMetrics creates a Prometheus metrics collector.
// Several instances may share one registry: each adopts the collectors already registered
// on it (see registerOrAdopt).
func NewPrometheusMetrics(registry prometheus.Registerer) *PrometheusMetrics {
	if registry == nil {
		registry = prometheus.NewRegistry()
	}

	m := &PrometheusMetrics{
		registry: registry,
		subscribersTotal: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "mercure_subscribers_total",
				Help: "Total number of subscribers accepted by the hub since startup (cumulative; includes those that have since disconnected). Pair with mercure_subscribers_connected to compute churn.",
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
		subscriberDisconnectsTotal: newDisconnectsCounterVec(),
		writeFlushLatency:          newWriteFlushHistogramVec(),
		updatesFailedTotal:         newUpdatesFailedCounterVec(),
		claimHeaderRejectedTotal:   newClaimHeaderRejectedCounterVec(),
		claimValueRejectedTotal:    newClaimValueRejectedCounterVec(),
	}

	// Several hubs in one Caddy config share that config's registry
	// (https://github.com/caddyserver/caddy/pull/6820), and a library user may pass one
	// registry to several PrometheusMetrics. registerOrAdopt makes every instance update the
	// registered collector rather than an unregistered copy that no scrape sees.
	m.subscribers = registerOrAdopt(m.registry, m.subscribers, "mercure_subscribers_connected")
	m.subscribersTotal = registerOrAdopt(m.registry, m.subscribersTotal, "mercure_subscribers_total")
	m.updatesTotal = registerOrAdopt(m.registry, m.updatesTotal, "mercure_updates_total")
	m.subscriberDisconnectsTotal = registerOrAdopt(m.registry, m.subscriberDisconnectsTotal, "mercure_subscriber_disconnects_total")
	m.writeFlushLatency = registerOrAdopt(m.registry, m.writeFlushLatency, "mercure_subscribe_write_flush_seconds")
	m.updatesFailedTotal = registerOrAdopt(m.registry, m.updatesFailedTotal, "mercure_updates_failed_total")
	m.claimHeaderRejectedTotal = registerOrAdopt(m.registry, m.claimHeaderRejectedTotal, "mercure_claim_header_rejected_total")
	m.claimValueRejectedTotal = registerOrAdopt(m.registry, m.claimValueRejectedTotal, "mercure_claim_value_rejected_total")
	m.subscribersByBindingValue = registerOrAdopt(m.registry, newBindingValueSubscribers(), "mercure_subscribers_by_binding_value")

	appVersion := currentAppVersion()
	registerOrAdopt(m.registry, appVersion.NewMetricsCollector(), "mercure_version_info")

	return m
}

func newDisconnectsCounterVec() *prometheus.CounterVec {
	return prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "mercure_subscriber_disconnects_total",
			Help: "Subscriber disconnections labeled by reason (client_closed | hub_shutdown | write_timeout | write_failed | transport_ended | transport_error | unknown). Pair with mercure_subscribers_connected for cause-tagged churn analysis.",
		},
		[]string{labelReason},
	)
}

func newWriteFlushHistogramVec() *prometheus.HistogramVec {
	return prometheus.NewHistogramVec(
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
	)
}

func newUpdatesFailedCounterVec() *prometheus.CounterVec {
	return prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "mercure_updates_failed_total",
			Help: "Updates that failed to publish, labeled by reason (validation | transport | timeout). Counts validation + dispatch failures only. Auth rejections and unreadable or oversized request bodies are not included; an update whose encoding exceeds the transport's cap counts as validation. Pair with mercure_updates_total for a success/failure ratio.",
		},
		[]string{labelReason},
	)
}

func newClaimHeaderRejectedCounterVec() *prometheus.CounterVec {
	return prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "mercure_claim_header_rejected_total",
			Help: "Requests rejected by a claim-header authorization binding (require_claim_header), labeled by binding (claim:Canonical-Header, e.g. groups:Group-Id) and reason (header_absent | claim_absent | mismatch | malformed). header_absent answers 400, claim_absent and mismatch 403; malformed answers 400 for the bound header and 401 for the bound claim.",
		},
		[]string{"binding", labelReason},
	)
}

func newClaimValueRejectedCounterVec() *prometheus.CounterVec {
	return prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "mercure_claim_value_rejected_total",
			Help: "Requests rejected by a claim-value binding (require_claim_value), labeled by claim and reason (claim_absent | mismatch | malformed). claim_absent and mismatch answer 403, malformed 401.",
		},
		[]string{"claim", labelReason},
	)
}

// registerOrAdopt registers c or, when the registry already holds a collector with the same
// descriptors, returns that one. name is used in panic messages only. Registering a different
// collector under a mercure_* name on the hub's registry is unsupported.
func registerOrAdopt[T prometheus.Collector](registry prometheus.Registerer, c T, name string) T {
	err := registry.Register(c)
	if err == nil {
		return c
	}

	var existing prometheus.AlreadyRegisteredError
	if !errors.As(err, &existing) {
		panic(err)
	}

	adopted, ok := existing.ExistingCollector.(T)
	if !ok {
		panic(fmt.Sprintf("mercure: existing collector for %s is not %T", name, c))
	}

	return adopted
}

func (m *PrometheusMetrics) SubscriberConnected(s *LocalSubscriber) {
	m.subscribersTotal.Inc()
	m.subscribers.Inc()
	m.subscribersByBindingValue.connected(s.counted)
}

func (m *PrometheusMetrics) SubscriberDisconnected(s *LocalSubscriber) {
	m.subscribers.Dec()
	m.subscribersByBindingValue.disconnected(s.counted)
}

// SubscriberDisconnectedWithReason decrements the subscribers and per-binding-value
// gauges and increments the per-reason disconnect counter. SubscribeHandler
// prefers it over SubscriberDisconnected when available.
func (m *PrometheusMetrics) SubscriberDisconnectedWithReason(s *LocalSubscriber, reason DisconnectReason) {
	m.subscribers.Dec()
	m.subscribersByBindingValue.disconnected(s.counted)
	m.subscriberDisconnectsTotal.WithLabelValues(string(reason)).Inc()
}

func (m *PrometheusMetrics) UpdatePublished(_ *Update) {
	m.updatesTotal.Inc()
}

// UpdatePublishFailed increments the per-reason publish-failure counter.
// Hub.Publish and PublishHandler's early checks call this via the optional
// PublishFailureReporter type-assertion when an update is rejected (validation)
// or fails to dispatch (transport / timeout), so failures are metered, not only
// logged.
func (m *PrometheusMetrics) UpdatePublishFailed(_ *Update, reason PublishFailureReason) {
	m.updatesFailedTotal.WithLabelValues(string(reason)).Inc()
}

// AuthorizationRejected increments the per-binding, per-reason rejection counter. The reason
// is passed typed, never inferred by parsing an error string.
func (m *PrometheusMetrics) AuthorizationRejected(binding string, reason AuthzRejectReason) {
	m.claimHeaderRejectedTotal.WithLabelValues(binding, string(reason)).Inc()
}

// ClaimValueRejected increments the per-claim, per-reason counter for claim-value bindings.
func (m *PrometheusMetrics) ClaimValueRejected(claim string, reason AuthzRejectReason) {
	m.claimValueRejectedTotal.WithLabelValues(claim, string(reason)).Inc()
}

// ObserveWriteFlush records the time spent in one SSE write+flush cycle,
// labeled by outcome: "ok" when both the Write and the Flush succeeded
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
