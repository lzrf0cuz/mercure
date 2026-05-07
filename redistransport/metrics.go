package redistransport

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Metric-name constants. Exposed package-internally so tests can assert on
// the same identifier used at registration time — a typo here is a compile
// error rather than a silently-skipped assertion.
const (
	metricDispatchSubscribersMatched   = "mercure_redis_dispatch_subscribers_matched"
	metricPublishDurationSeconds       = "mercure_redis_publish_duration_seconds"
	metricHistoryReplayDurationSeconds = "mercure_redis_history_replay_duration_seconds"
	metricSubscriberAddTotal           = "mercure_redis_subscriber_add_total"
	metricSubscriberRemoveTotal        = "mercure_redis_subscriber_remove_total"
	metricSubscribersLostTotal         = "mercure_redis_subscribers_lost_total"
	metricHealthy                      = "mercure_redis_healthy"
	metricClockDriftSeconds            = "mercure_redis_clock_drift_seconds"
	metricConsumerLag                  = "mercure_redis_consumer_lag"
	metricConsumerPending              = "mercure_redis_consumer_pending"
	// "consumer_groups" not "_count" — _count suffix is reserved by
	// the Prometheus naming convention for histogram/summary observation
	// counters (promlinter enforces this).
	metricConsumerGroups              = "mercure_redis_consumer_groups"
	metricHistoryWindowSeconds        = "mercure_redis_history_window_seconds"
	metricDispatchZeroMatchTotal      = "mercure_redis_dispatch_zero_match_total"
	metricPreBindingMetricDropsTotal  = "mercure_redis_pre_binding_metric_drops_total"
	metricBuildInfo                   = "mercure_redis_build_info"
	metricPresenceReadErrorsTotal     = "mercure_redis_presence_read_errors_total"
	metricTTLCleanupErrorsTotal       = "mercure_redis_ttl_cleanup_errors_total"
	metricHistoryReplayFallbackTotal  = "mercure_redis_history_replay_fallback_total"
	metricHistoryReplayTruncatedTotal = "mercure_redis_history_replay_truncated_total"
	metricPresenceFallbackTotal       = "mercure_redis_presence_fallback_total"
	metricStreamDecodeErrorsTotal     = "mercure_redis_stream_decode_errors_total"
	metricHistoryCursorRejectedTotal  = "mercure_redis_history_cursor_rejected_total"
)

// Label-key constants for metric vectors.
const (
	labelKeyShard         = "shard"
	labelKeyKind          = "kind"
	labelKeyReason        = "reason"
	labelKeyTransportType = "transport_type"
	labelKeyBackendType   = "backend_type"
)

// transportTypeRedis is the value emitted as the transport_type label on every
// transport-side metric. Stable dashboard contract — independent from
// serverTypeRedis (which classifies INFO server output and could in principle
// rename to track upstream wording without touching dashboards). They happen
// to be equal today; do not collapse them.
const transportTypeRedis = "redis"

// swappableCounterFunc is a Prometheus collector that wraps a swappable
// value source for a monotonic counter. Used by the pre-binding-drops
// metric so that when safeRegister adopts an existing collector during
// Caddy config reload (registry persists across reloads, new transport
// instance binds to it), we can redirect the read-through closure to
// the *new* transport's atomic counter.
//
// Without this, the adopted collector keeps reading from the original
// transport's atomic — that transport is no longer in the pool, so its
// drop counter is frozen at its final value while the new transport's
// pre-binding drops go un-scraped. The whole purpose of this metric is
// to surface drops on each transport instance, so a stale-source binding
// silently defeats the audit trail.
type swappableCounterFunc struct {
	desc   *prometheus.Desc
	source atomic.Pointer[func() float64]
}

func newSwappableCounterFunc(opts prometheus.CounterOpts, source func() float64) *swappableCounterFunc {
	c := &swappableCounterFunc{
		desc: prometheus.NewDesc(
			prometheus.BuildFQName(opts.Namespace, opts.Subsystem, opts.Name),
			opts.Help,
			nil,
			opts.ConstLabels,
		),
	}
	c.setSource(source)

	return c
}

func (c *swappableCounterFunc) Describe(ch chan<- *prometheus.Desc) { ch <- c.desc }

func (c *swappableCounterFunc) Collect(ch chan<- prometheus.Metric) {
	var v float64
	if fn := c.source.Load(); fn != nil {
		v = (*fn)()
	}

	ch <- prometheus.MustNewConstMetric(c.desc, prometheus.CounterValue, v)
}

// setSource swaps the value-producing closure. Safe under concurrent
// scrapes because the underlying field is an atomic.Pointer.
//
// Panics on a nil source: silently storing nil would make Collect emit
// 0 (the nil branch of the Load) — exactly the silent-failure mode this
// type was built to prevent. The constructor and the rebind site both
// pass non-nil; a nil here is always a programmer error.
func (c *swappableCounterFunc) setSource(source func() float64) {
	if source == nil {
		panic("redistransport: swappableCounterFunc.setSource called with nil source")
	}

	c.source.Store(&source)
}

// Metrics provides Prometheus instrumentation for the Redis transport's
// hot paths: dispatch, XREADGROUP, history replay, subscriber lifecycle,
// and health/presence. All metrics use the "mercure_redis_" prefix.
type Metrics struct {
	// backendType is the detected backend server family ("redis", "valkey",
	// or "unknown") sourced from INFO at validateVersion time. Stamped as
	// the backend_type const label on every transport-emitted series so
	// dashboards can split deployments by backend identity. Captured at
	// newMetrics-construction time from the parent transport, which latches
	// it in NewRedisTransport before initMetrics runs. Read-only after
	// construction — see transportTypeLabels.
	backendType serverType

	// Dispatch path (sharded).
	dispatchDuration         *prometheus.HistogramVec
	dispatchSubscribersTotal prometheus.Histogram
	publishDuration          prometheus.Histogram
	// publishPayloadBytes records the size in bytes of each dispatched event
	// payload after codec encoding (the XADD message body sent to Redis).
	// During load tests, pair with mercure_redis_publish_duration_seconds and
	// the redis_exporter `redis_net_input_bytes_total` series to predict
	// bandwidth scaling and correlate latency spikes with payload-size shifts.
	publishPayloadBytes prometheus.Histogram

	// XREADGROUP path.
	xreadgroupLatency prometheus.Histogram
	xreadgroupBatch   prometheus.Histogram

	// History replay.
	historyReplayDuration   prometheus.Histogram
	historyReplayConcurrent prometheus.Gauge
	// historyReplayFallback counts replays that fell through Pass 1 (the
	// requested Last-Event-ID was past retention) and dispatched ALL stream
	// entries in Pass 2. A sustained non-zero rate is the operator signal
	// that retention (max_length / event_ttl) is too tight for the
	// reconnect window your clients exercise.
	historyReplayFallback prometheus.Counter
	// historyReplayTruncated counts replays cut short before delivering the
	// full backlog because a matched update couldn't be handed off (subscriber
	// disconnected — for any reason — or its buffer was full). The aggregate
	// companion to the mercure.history.truncated span attribute: a sustained
	// non-zero rate means reconnecting clients aren't receiving their complete
	// history (slow consumers, or churn during restarts).
	historyReplayTruncated prometheus.Counter

	// historyCursorRejected counts history-replay entries whose eventID stream
	// metadata carried forbidden SSE chars (CR/LF/NUL), so the Last-Event-Id
	// cursor was NOT advanced to it (the entry was still delivered with its
	// validated update.ID). The durable signal for that reject — its Warn log is
	// rate-sampled, so this counter, not the log, is authoritative. Should be
	// zero; non-zero indicates a tampered stream or a non-compliant hub.
	historyCursorRejected prometheus.Counter

	// Subscriber lifecycle.
	subscriberAddTotal    prometheus.Counter
	subscriberRateLimited prometheus.Counter
	subscriberRemoveTotal prometheus.Counter
	// publisherRateLimited counts Dispatch calls delayed by the publish
	// rate limiter (when WithPublisherRateLimit > 0). Symmetric with
	// subscriberRateLimited; non-zero rate is the signal that a publisher
	// is exceeding configured throughput.
	publisherRateLimited prometheus.Counter
	// addSubscriberDuration covers the full AddSubscriber call — rate-limit
	// wait + t.mu wait + shard-list insert + history replay setup. p99 rising
	// independent of rate-limit activity is the smoking gun for t.mu
	// contention under concurrent Add/Remove/dispatch (see load-test
	// Scenario 1 post-analysis for the mechanism).
	addSubscriberDuration prometheus.Histogram
	// removeSubscriberDuration covers RemoveSubscriber (t.mu wait +
	// shard-list Remove + metric bookkeeping). p99 climbing during ramp-down
	// under mass-disconnect is the signature for the Scenario 3 contention
	// pattern; baseline has no way to measure it directly.
	removeSubscriberDuration prometheus.Histogram
	// subscribersLost counts subscribers dropped by transport-initiated events
	// that bypass the normal RemoveSubscriber path. Use lostCounter() to emit;
	// the closed set of reason values is enumerated below.
	subscribersLost *prometheus.CounterVec

	// Shard balance.
	shardSubscribers *prometheus.GaugeVec

	// Health & operational.
	healthy      prometheus.Gauge
	streamLength prometheus.Gauge
	// clockDriftSeconds is the absolute drift between the hub's clock and
	// the Redis server's clock at connect time. UUIDv7 → stream-ID seek
	// in scanForEventID assumes drift is small relative to clockSkewMargin;
	// a drift Gauge gives operators a Prometheus-alertable signal where
	// before there was only a Warn log. Sampled once at NewRedisTransport.
	clockDriftSeconds prometheus.Gauge
	// consumerLag is the count of stream entries this node's consumer
	// group has yet to consume (XINFO GROUPS lag field). Sampled at the
	// healthCheck cadence; sustained non-zero is the canonical
	// "are-we-keeping-up?" signal that streamLength alone cannot answer.
	// Set to -1 when Redis returns lag=-1 (non-deterministic group state).
	consumerLag prometheus.Gauge
	// consumerPending is the number of XREADGROUP'd messages this node has
	// not yet XACK'd (PEL size). Sustained growth — beyond what
	// xreadgroup_batch_size churn explains — points at a hub that is
	// reading from Redis but failing to dispatch+ack downstream.
	consumerPending prometheus.Gauge
	// consumerGroupsCount is the number of consumer groups currently
	// attached to the stream (XINFO GROUPS length). One group per hub
	// instance, plus any zombies that haven't been GC'd. Together with
	// streamLength, gates the misconfiguration alert: publish rate
	// non-zero AND groups == 0 means the publisher is writing to a stream
	// no hub is reading. Sampled at health-check cadence; reports -1 on
	// XINFO error so dashboards can distinguish "scrape gap" from
	// "Redis returned zero groups."
	consumerGroupsCount prometheus.Gauge
	// historyWindowSeconds is the proactive counterpart to
	// historyReplayFallback. Computed at health-check cadence as the
	// elapsed time between the stream's first and last entry IDs
	// (Redis-stamped <ms>-<seq>), it reports the available history
	// window BEFORE clients start missing it. Alert when this falls
	// below the expected client reconnect duration to catch retention-
	// too-tight regressions proactively rather than reactively.
	// Reports 0 on empty stream; reports -1 when the head/tail entry
	// IDs cannot be parsed.
	historyWindowSeconds prometheus.Gauge
	// dispatchZeroMatchTotal counts XREADGROUP'd events that fanned out
	// to zero local subscribers. Multiplied by replica count, this is
	// the fleet-wide cost of fan-out without consumers — useful for
	// identifying chatty publishers whose topics nobody subscribes to.
	dispatchZeroMatchTotal prometheus.Counter

	// buildInfo is a constant-1 gauge labeled version/revision/go_version,
	// surfacing module provenance for cross-node comparison during rollouts.
	buildInfo *prometheus.GaugeVec

	// Presence.
	presencePayloadBytes prometheus.Histogram
	// presenceReadErrors is a CounterVec indexed by kind ("get", "unmarshal").
	// Non-zero rate indicates GetSubscribers is returning partial results —
	// some nodes' presence keys failed to read or parse and were skipped.
	presenceReadErrors *prometheus.CounterVec
	// presenceReadSkipped counts per-key skips that are operationally normal
	// (key expired mid-SCAN or node still initialising — empty value) but
	// would otherwise be invisible. A steady-state non-zero rate is expected;
	// alert on a sudden correlated drop in subscriber_connected + jump here.
	presenceReadSkipped prometheus.Counter
	// presenceReadCapped counts GetSubscribers calls REFUSED because the
	// cluster's subscriber count exceeded WithSubscriptionsMaxSubscribers — a
	// whole-request abort (returns mercure.ErrTooManySubscribers → hub 503),
	// distinct from the per-key presenceReadErrors. Non-zero means the
	// /subscriptions endpoint is unusable at the current fleet size: raise the
	// cap or stop polling it.
	presenceReadCapped prometheus.Counter
	// subscriberAdmissionRejected counts new subscribers SHED by the admission
	// gate (TryAdmit), labelled reason=rate|capacity. Non-zero means the hub is
	// returning 429 + Retry-After at the front door — over its accept-rate
	// (subscriber_rate_limit) or concurrent-count (subscriber_max_count) ceiling.
	subscriberAdmissionRejected *prometheus.CounterVec
	// streamDecodeErrors counts stream entries dropped on the consumer side
	// before dispatch. kind=data_type (the "data" field came back a non-string)
	// and kind=unmarshal (the codec rejected the bytes) are decode failures — a
	// non-zero rate signals producer/consumer codec drift or foreign data on the
	// stream key. kind=forbidden_sse_chars is a security drop: the payload decoded
	// fine but its id/type carried CR/LF/NUL that would forge SSE frames.
	streamDecodeErrors *prometheus.CounterVec
	// zombieGCErrors counts per-group failures during zombie-group cleanup
	// (Exists/Destroy). The GC is fail-safe (errors skip the group rather
	// than falsely destroying a live node's group), so persistent errors
	// would otherwise silently accumulate zombie groups without signal.
	zombieGCErrors prometheus.Counter

	// publishErrors counts Dispatch() failures, labeled by kind (encode or
	// publish). Nonzero rate indicates either a codec mismatch (encode) or
	// Redis rejecting the publish (publish — Lua script error, MAXLEN
	// validation, etc). Pair with publish_duration_seconds_count to derive
	// error-rate.
	publishErrors *prometheus.CounterVec

	// xreadgroupErrors counts XREADGROUP failures, labeled by kind. `nogroup`
	// fires when the consumer group was destroyed externally and we re-create
	// it — a recoverable blip. `other` fires on network errors, ACL changes,
	// or unexpected Redis responses — treat persistent non-zero as unhealthy.
	xreadgroupErrors *prometheus.CounterVec

	// presenceHeartbeatErrors counts failed presence-key SET calls. Each miss
	// consumes a slice of the presenceTTL budget; a sustained rate means other
	// nodes' GC will start destroying this node's consumer group.
	presenceHeartbeatErrors prometheus.Counter

	// presenceFallback counts presence-payload encoding decisions that
	// fell back to summary mode after detail-mode encoding crossed
	// presenceDetailByteThreshold. Counted at decision time inside
	// buildPresencePayload, BEFORE the Redis SET fires — a downstream
	// SET failure does not decrement; that failure is tracked by
	// presenceHeartbeatErrors. Labels: reason=byte_budget means the
	// count gate passed but the marshaled payload exceeded the byte
	// gate; reason=summary_marshal_failed means the summary fallback
	// itself failed to encode and the over-budget detail payload was
	// shipped as a last-resort liveness signal. Sustained non-zero on
	// the latter indicates payload shapes the codec cannot handle.
	presenceFallback *prometheus.CounterVec

	// preBindingMetricDrops surfaces RedisTransport.preBindingDrops as a
	// Prometheus counter via a read-through closure. The value source is
	// captured in RegisterMetricsWith (redis.go) and rebound on Caddy reload
	// adoption so a new transport instance against a persistent registry
	// always reports its own drops, not a prior instance's. Non-zero means
	// metric emissions were attempted before RegisterMetricsWith bound this
	// *Metrics; the operator should bind earlier in the lifecycle.
	preBindingMetricDrops *swappableCounterFunc

	// preBindingDropsValueSource is retained alongside preBindingMetricDrops
	// so registerAll can re-set the closure on the adopted collector
	// (Caddy-reload + new-transport scenario where safeRegister returns the
	// pre-existing instance from the registry). Without this stash the
	// rebinding could only happen at construction time, before adoption is
	// known.
	preBindingDropsValueSource func() float64

	// ttlCleanupErrors counts failures of the periodic XTRIM-by-MINID call
	// that enforces event_ttl. A sustained non-zero rate means stream
	// growth is unbounded despite the configured TTL; pair with
	// stream_length to catch the cause before disk pressure hits.
	ttlCleanupErrors prometheus.Counter

	// shardDispatchObs and shardSubGauges hold per-shard pre-resolved children
	// of dispatchDuration and shardSubscribers, populated by resolveShardChildren
	// at registration time. Resolving once at construction avoids a map lookup
	// on every dispatch/Add/Remove call — material at 10k+ msg/s. Indexed by
	// shard number 0..numShards-1.
	shardDispatchObs []prometheus.Observer
	shardSubGauges   []prometheus.Gauge
}

// publishErrorKind enumerates failure stages for Dispatch().
type publishErrorKind string

const (
	publishErrorEncode  publishErrorKind = "encode"
	publishErrorPublish publishErrorKind = "publish"
)

// recordPublishError increments publishErrors for kind. Safe on a nil *Metrics.
func (m *Metrics) recordPublishError(kind publishErrorKind) {
	if m == nil {
		return
	}

	m.publishErrors.WithLabelValues(string(kind)).Inc()
}

// xreadgroupErrorKind enumerates failure modes for the XREADGROUP listener.
type xreadgroupErrorKind string

const (
	// xreadgroupErrorNoGroup: consumer group was destroyed externally; the
	// listener re-creates it and resumes. Normal during controlled failover.
	xreadgroupErrorNoGroup xreadgroupErrorKind = "nogroup"
	// xreadgroupErrorNoGroupRecreateFailed: re-creating the group after
	// NOGROUP failed (ACL drift, read-only replica, etc). Listener loops back
	// and retries; a sustained non-zero rate means the group keeps getting
	// torn down and can't be rebuilt — investigate before the transport
	// silently ingests nothing.
	xreadgroupErrorNoGroupRecreateFailed xreadgroupErrorKind = "nogroup_recreate_failed"
	// xreadgroupErrorOther: any non-NOGROUP XREADGROUP error. Usually
	// connectivity or ACL; counted separately from nogroup so dashboards can
	// distinguish transient cluster churn from infrastructure-level issues.
	xreadgroupErrorOther xreadgroupErrorKind = "other"
	// xreadgroupErrorXAck: XAck failed for at least one message in a batch.
	// PEL entries will be re-delivered on the next XREADGROUP "0" drain, so
	// sustained failures inflate the pending list but don't lose data.
	xreadgroupErrorXAck xreadgroupErrorKind = "xack"
)

// recordXReadgroupError increments xreadgroupErrors for kind. Safe on nil *Metrics.
func (m *Metrics) recordXReadgroupError(kind xreadgroupErrorKind) {
	if m == nil {
		return
	}

	m.xreadgroupErrors.WithLabelValues(string(kind)).Inc()
}

// recordPresenceHeartbeatError increments presenceHeartbeatErrors. Safe on nil *Metrics.
func (m *Metrics) recordPresenceHeartbeatError() {
	if m == nil {
		return
	}

	m.presenceHeartbeatErrors.Inc()
}

// recordTTLCleanupError increments ttlCleanupErrors. Safe on nil *Metrics.
func (m *Metrics) recordTTLCleanupError() {
	if m == nil {
		return
	}

	m.ttlCleanupErrors.Inc()
}

// recordHistoryReplayFullScan increments historyReplayFallback. The metric
// name retains "fallback" for Prometheus dashboard stability; the Go method
// is named "FullScan" to match the Pass-2 / full-stream-replay vocabulary
// used in the surrounding code. Safe on nil *Metrics.
func (m *Metrics) recordHistoryReplayFullScan() {
	if m == nil {
		return
	}

	m.historyReplayFallback.Inc()
}

// recordHistoryReplayTruncated increments historyReplayTruncated, the aggregate
// companion to the mercure.history.truncated span attribute. Fires once per
// replay that ended before delivering the full backlog. Safe on nil *Metrics.
func (m *Metrics) recordHistoryReplayTruncated() {
	if m == nil {
		return
	}

	m.historyReplayTruncated.Inc()
}

// recordHistoryCursorRejected increments historyCursorRejected, fired once per
// history-replay entry whose eventID metadata was rejected for forbidden SSE
// chars (so the Last-Event-Id cursor was not advanced to it). Safe on nil *Metrics.
func (m *Metrics) recordHistoryCursorRejected() {
	if m == nil {
		return
	}

	m.historyCursorRejected.Inc()
}

// observeClockDrift sets clockDriftSeconds to the absolute value of d in
// seconds. Safe on nil *Metrics. Taking time.Duration (not float64) keeps
// the unit conversion inside the metric helper so callers cannot
// accidentally pass milliseconds, microseconds, or a negative value.
func (m *Metrics) observeClockDrift(d time.Duration) {
	if m == nil {
		return
	}

	if d < 0 {
		d = -d
	}

	m.clockDriftSeconds.Set(d.Seconds())
}

// markClockDriftUnknown sets clockDriftSeconds to NaN, signalling to
// Prometheus that the drift sample is missing rather than zero. Operators
// alerting on `clock_drift_seconds == 0` would otherwise see false-healthy
// state when the TIME query failed at startup.
func (m *Metrics) markClockDriftUnknown() {
	if m == nil {
		return
	}

	m.clockDriftSeconds.Set(math.NaN())
}

// streamDecodeErrorKind enumerates failure modes for streamDecodeErrors.
type streamDecodeErrorKind string

const (
	streamDecodeErrorDataType          streamDecodeErrorKind = "data_type"
	streamDecodeErrorUnmarshal         streamDecodeErrorKind = "unmarshal"
	streamDecodeErrorForbiddenSSEChars streamDecodeErrorKind = "forbidden_sse_chars"
)

// recordStreamDecodeError increments streamDecodeErrors for kind.
// Safe to call on a nil *Metrics.
func (m *Metrics) recordStreamDecodeError(kind streamDecodeErrorKind) {
	if m == nil {
		return
	}

	m.streamDecodeErrors.WithLabelValues(string(kind)).Inc()
}

// recordZombieGCError increments zombieGCErrors. Safe on a nil *Metrics.
func (m *Metrics) recordZombieGCError() {
	if m == nil {
		return
	}

	m.zombieGCErrors.Inc()
}

// presenceReadErrorKind enumerates the failure modes counted by
// presenceReadErrors. Closed set via named type to avoid typos in hot code.
type presenceReadErrorKind string

const (
	presenceReadErrorGet       presenceReadErrorKind = "get"
	presenceReadErrorUnmarshal presenceReadErrorKind = "unmarshal"
)

// recordPresenceReadError increments the presence_read_errors counter for kind.
// Safe to call on a nil *Metrics (no-ops). Unlike lostCounter, this does not
// return the underlying counter — presence reads are an error-path, so the
// per-call map lookup is noise; hot paths (shard dispatch) cache instead.
func (m *Metrics) recordPresenceReadError(kind presenceReadErrorKind) {
	if m == nil {
		return
	}

	m.presenceReadErrors.WithLabelValues(string(kind)).Inc()
}

// recordPresenceReadSkipped increments the presence_read_skipped counter.
// Safe to call on a nil *Metrics. See field docs for the operational signal.
func (m *Metrics) recordPresenceReadSkipped() {
	if m == nil {
		return
	}

	m.presenceReadSkipped.Inc()
}

// recordPresenceReadCapped increments the presence_read_capped counter.
// Safe to call on a nil *Metrics. See field docs for the operational signal.
func (m *Metrics) recordPresenceReadCapped() {
	if m == nil {
		return
	}

	m.presenceReadCapped.Inc()
}

// recordAdmissionRejected increments subscriber_admission_rejected_total for the
// given reason (admissionReasonRate / admissionReasonCapacity). Nil-safe.
func (m *Metrics) recordAdmissionRejected(reason string) {
	if m == nil {
		return
	}

	m.subscriberAdmissionRejected.WithLabelValues(reason).Inc()
}

// presenceFallbackReason enumerates the labels for
// presenceFallback. Closed set via named type.
type presenceFallbackReason string

const (
	// presenceFallbackByteBudget fires when the count gate
	// passed but the marshaled detail payload exceeded
	// presenceDetailByteThreshold.
	presenceFallbackByteBudget presenceFallbackReason = "byte_budget"
	// presenceFallbackSummaryMarshalFailed fires when the
	// summary fallback itself failed to marshal and the over-budget
	// detail payload was shipped as a last-resort liveness signal.
	presenceFallbackSummaryMarshalFailed presenceFallbackReason = "summary_marshal_failed"
	// presenceFallbackCountRace fires when the atomic count gate read
	// at-or-below the detail threshold but the live shard walk overran it
	// (membership grew between the lock-free count read and the walk under a
	// reconnect storm). The walk is bounded and the payload falls back to a
	// summary using a fresh count instead of marshaling an unbounded list.
	presenceFallbackCountRace presenceFallbackReason = "count_race"
)

// recordPresenceFallback increments presenceFallback for
// reason. Safe to call on a nil *Metrics.
func (m *Metrics) recordPresenceFallback(reason presenceFallbackReason) {
	if m == nil {
		return
	}

	m.presenceFallback.WithLabelValues(string(reason)).Inc()
}

// subscriberLossReason enumerates causes of transport-detected subscriber
// drops counted by the subscribers_lost counter. The named type + constants
// close the label universe so a typo can't silently create a new time series.
// Some reasons overlap with subscriberRemoveTotal (see lossReasonBackpressure);
// operators query subscribers_lost for cause-tagged alerting and
// subscriber_remove_total for plain accounting.
type subscriberLossReason string

const (
	// lossReasonShutdown: emitted once per subscriber forcibly disconnected by
	// disconnectAllSubscribers. RemoveSubscriber short-circuits on closed
	// transport, so these drops are NOT also counted in subscriberRemoveTotal.
	lossReasonShutdown subscriberLossReason = "shutdown"
	// lossReasonHistoryReplayFailed: emitted once per subscriber that was added
	// but whose history replay hit an XRANGE error, causing AddSubscriber to
	// remove the subscriber before it saw a single live event. The hub returns
	// 503 to the client and never calls RemoveSubscriber, so these drops are
	// NOT also counted in subscriberRemoveTotal.
	lossReasonHistoryReplayFailed subscriberLossReason = "history_replay_failed"
	// lossReasonBackpressure: emitted when LocalSubscriber.Dispatch returns
	// false during live dispatch — either because handleFullChan disconnected
	// the subscriber on a full out-channel (the dominant case, slow consumer)
	// or because a concurrent path had already disconnected it (rare race
	// with client-disconnect / Close). Surfaces slow-consumer drops as a
	// metric; previously visible only via the warn log "Subscriber
	// disconnected: unable to receive updates fast enough" (localsubscriber.go).
	// The SSE handler observes the closed channel, exits, and DOES call
	// RemoveSubscriber, so backpressure events are ALSO counted in
	// subscriberRemoveTotal — operators distinguish via this label.
	lossReasonBackpressure subscriberLossReason = "backpressure"
)

// lostCounter returns the subscribers_lost child counter for the given reason,
// or nil when m is nil (metrics disabled). Callers in hot loops should cache
// the return value rather than re-resolving per iteration; loss accounting
// now flows through markLost which performs one resolution per loss event
// (rare). Nil-safe so call sites do not have to repeat the t.metrics.Load()
// guard around every increment.
func (m *Metrics) lostCounter(r subscriberLossReason) prometheus.Counter {
	if m == nil {
		return nil
	}

	return m.subscribersLost.WithLabelValues(string(r))
}

// constLabelsFor returns the ConstLabels stamped on every transport-emitted
// metric, sourced from a backendType. Centralizes the {transport_type,
// backend_type} contract so the Metrics struct and the poolStatsCollector
// (which lives outside Metrics but shares the label set) use identical
// labels. Empty input is normalized to serverTypeUnknown so dashboards
// always see a value rather than a missing label.
func constLabelsFor(backendType serverType) prometheus.Labels {
	if backendType == "" {
		backendType = serverTypeUnknown
	}

	return prometheus.Labels{
		labelKeyTransportType: transportTypeRedis,
		labelKeyBackendType:   string(backendType),
	}
}

// transportTypeLabels returns the ConstLabels for m.backendType. See
// constLabelsFor for the label contract. Returned fresh on each call;
// client_golang copies the map at descriptor construction.
func (m *Metrics) transportTypeLabels() prometheus.Labels {
	return constLabelsFor(m.backendType)
}

// newMetrics creates and registers all Prometheus metrics against reg,
// which must be non-nil. preBindingDropsValue is the value source for the
// pre-binding-drops CounterFunc — RegisterMetricsWith captures the
// transport's atomic.Uint64 here so a Prometheus scrape reads the running
// total without any additional locking. Pass nil to disable the
// pre-binding-drops metric (test fixtures that don't run via the transport
// constructor path). backendType is the detected backend server family
// ("redis", "valkey", or "unknown"), stamped as the backend_type const label
// on every collector built here; an empty string defaults to "unknown" so
// dashboards see a value rather than a missing series. Callers are expected
// to pre-check reg (see initMetrics) so metrics are only built when opt-in
// via WithPrometheusRegisterer.
func newMetrics(reg prometheus.Registerer, preBindingDropsValue func() float64, backendType serverType) *Metrics {
	if reg == nil {
		panic("redistransport: newMetrics called with nil registerer")
	}

	if backendType == "" {
		backendType = serverTypeUnknown
	}

	m := &Metrics{backendType: backendType}
	m.buildHistograms()
	m.buildCountersAndGauges()
	m.buildPreBindingDropsMetric(preBindingDropsValue)
	m.registerAll(reg)

	// Default healthy to 1 (we just started).
	m.healthy.Set(1)

	// Set build_info to 1 with provenance labels. Done after registerAll so
	// safeRegister's collector swap (on Prometheus AlreadyRegisteredError) is
	// already settled — otherwise we would Set() on the discarded original.
	bi := LookupBuildInfo()
	m.buildInfo.With(prometheus.Labels{
		"version":    bi.Version,
		"revision":   bi.Revision,
		"go_version": bi.GoVersion,
	}).Set(1)

	return m
}

// buildHistograms initializes all histogram collectors on m.
func (m *Metrics) buildHistograms() {
	m.dispatchDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:        "mercure_redis_dispatch_duration_seconds",
		Help:        "Time spent in shard worker per message (topic match + Dispatch).",
		Buckets:     []float64{.0001, .0005, .001, .0025, .005, .01, .025, .05},
		ConstLabels: m.transportTypeLabels(),
	}, []string{labelKeyShard})

	m.dispatchSubscribersTotal = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:        metricDispatchSubscribersMatched,
		Help:        "Number of subscribers matched per message across all shards.",
		Buckets:     []float64{1, 10, 100, 500, 1000, 5000, 10000, 50000, 100000},
		ConstLabels: m.transportTypeLabels(),
	})

	m.publishDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:        metricPublishDurationSeconds,
		Help:        "End-to-end Dispatch() latency (Lua script XADD + SET).",
		Buckets:     []float64{.0001, .0005, .001, .0025, .005, .01, .025, .05, .1},
		ConstLabels: m.transportTypeLabels(),
	})

	m.publishPayloadBytes = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:        "mercure_redis_publish_payload_bytes",
		Help:        "Size in bytes of dispatched event payloads after codec encoding (XADD message body). Observed before the Lua publish runs, so failed publishes (NOGROUP / OOM / network) still increment — the metric measures encoder output, not successful bytes-on-the-wire, which is the right signal for bandwidth prediction even during outages.",
		Buckets:     []float64{100, 500, 1000, 5000, 10000, 50000, 100000, 500000},
		ConstLabels: m.transportTypeLabels(),
	})

	m.xreadgroupLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "mercure_redis_xreadgroup_latency_seconds",
		// Includes the BLOCK wait. At idle this measures how long XREADGROUP
		// held the connection waiting for the next entry — NOT Redis
		// latency. Use as a top-of-funnel diagnostic, not a server-health
		// signal.
		Help:        "Time from XREADGROUP call to result (includes BLOCK wait — high values at idle traffic are expected).",
		Buckets:     []float64{.001, .005, .01, .05, .1, .5, 1, 2},
		ConstLabels: m.transportTypeLabels(),
	})

	m.xreadgroupBatch = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:        "mercure_redis_xreadgroup_batch_size",
		Help:        "Number of entries per XREADGROUP batch.",
		Buckets:     []float64{1, 5, 10, 25, 50, 100, 250, 500},
		ConstLabels: m.transportTypeLabels(),
	})

	m.historyReplayDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:        metricHistoryReplayDurationSeconds,
		Help:        "Time per complete history replay (paginated XRANGE).",
		Buckets:     []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1},
		ConstLabels: m.transportTypeLabels(),
	})

	m.presencePayloadBytes = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:        "mercure_redis_presence_payload_bytes",
		Help:        "Size of presence SET payload in bytes.",
		Buckets:     []float64{100, 500, 1000, 5000, 10000, 50000, 100000, 500000},
		ConstLabels: m.transportTypeLabels(),
	})

	m.buildSubscriberLifecycleHistograms()
}

// buildSubscriberLifecycleHistograms initializes add/remove duration
// histograms. Split from buildHistograms so each function's scope stays
// focused on a coherent instrumentation surface.
//
// Buckets align with publishDuration's range so ops can overlay all three
// on the same Grafana panel. The 100ms upper bound is deliberately forgiving
// — a p99 above it is a loud "investigate now" signal during ramp-up
// (TLS saturation propagating to transport) or ramp-down (mu contention).
func (m *Metrics) buildSubscriberLifecycleHistograms() {
	m.addSubscriberDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:        "mercure_redis_add_subscriber_duration_seconds",
		Help:        "End-to-end AddSubscriber latency (rate-limit wait + t.mu + shard insert + history replay setup).",
		Buckets:     []float64{.0001, .0005, .001, .0025, .005, .01, .025, .05, .1},
		ConstLabels: m.transportTypeLabels(),
	})

	m.removeSubscriberDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:        "mercure_redis_remove_subscriber_duration_seconds",
		Help:        "End-to-end RemoveSubscriber latency (t.mu wait + shard remove + metric bookkeeping).",
		Buckets:     []float64{.0001, .0005, .001, .0025, .005, .01, .025, .05, .1},
		ConstLabels: m.transportTypeLabels(),
	})
}

// buildCountersAndGauges initializes all counter and gauge collectors on m.
func (m *Metrics) buildCountersAndGauges() {
	m.buildSubscriberMetrics()
	m.buildHealthMetrics()
	m.buildErrorCounters()
	m.buildAdmissionMetrics()
	m.buildHistoryReplayCounters()
	m.buildBuildInfoMetric()
}

// buildAdmissionMetrics initializes the admission-gate (TryAdmit) metrics.
func (m *Metrics) buildAdmissionMetrics() {
	m.subscriberAdmissionRejected = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        "mercure_redis_subscriber_admission_rejected_total",
		Help:        "New subscribers shed by the admission gate (labels: reason=rate|capacity). Non-zero = the hub is returning 429 at the front door, over its accept-rate or concurrent-count ceiling.",
		ConstLabels: m.transportTypeLabels(),
	}, []string{labelKeyReason})
}

// buildSubscriberMetrics initializes subscriber-accounting metrics (add/remove/lost + per-shard gauges).
func (m *Metrics) buildSubscriberMetrics() {
	m.historyReplayConcurrent = prometheus.NewGauge(prometheus.GaugeOpts{
		Name:        "mercure_redis_history_replay_concurrent",
		Help:        "Current number of concurrent history replays.",
		ConstLabels: m.transportTypeLabels(),
	})

	m.subscriberAddTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name:        metricSubscriberAddTotal,
		Help:        "Total AddSubscriber calls.",
		ConstLabels: m.transportTypeLabels(),
	})

	m.subscriberRateLimited = prometheus.NewCounter(prometheus.CounterOpts{
		Name:        "mercure_redis_subscriber_rate_limited_total",
		Help:        "AddSubscriber calls delayed by rate limiter.",
		ConstLabels: m.transportTypeLabels(),
	})

	m.publisherRateLimited = prometheus.NewCounter(prometheus.CounterOpts{
		Name:        "mercure_redis_publisher_rate_limited_total",
		Help:        "Dispatch calls delayed by the publish rate limiter.",
		ConstLabels: m.transportTypeLabels(),
	})

	m.subscriberRemoveTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name:        metricSubscriberRemoveTotal,
		Help:        "Total RemoveSubscriber calls.",
		ConstLabels: m.transportTypeLabels(),
	})

	m.subscribersLost = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        metricSubscribersLostTotal,
		Help:        "Subscribers dropped with operational cause label (shutdown, history_replay_failed, backpressure). Backpressure also appears in subscriber_remove_total; the other reasons do not.",
		ConstLabels: m.transportTypeLabels(),
	}, []string{labelKeyReason})

	m.shardSubscribers = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:        "mercure_redis_shard_subscribers",
		Help:        "Subscriber count per dispatch shard.",
		ConstLabels: m.transportTypeLabels(),
	}, []string{labelKeyShard})
}

// buildHealthMetrics initializes health-check gauges sampled during PING loop.
func (m *Metrics) buildHealthMetrics() {
	m.healthy = prometheus.NewGauge(prometheus.GaugeOpts{
		Name:        metricHealthy,
		Help:        "1 = healthy, 0 = unhealthy (based on health check PINGs).",
		ConstLabels: m.transportTypeLabels(),
	})

	m.streamLength = prometheus.NewGauge(prometheus.GaugeOpts{
		Name:        "mercure_redis_stream_length",
		Help:        "Current Redis stream length (sampled during health check). Reflects retention size — NOT consumer-group lag.",
		ConstLabels: m.transportTypeLabels(),
	})

	m.clockDriftSeconds = prometheus.NewGauge(prometheus.GaugeOpts{
		Name:        metricClockDriftSeconds,
		Help:        "Absolute clock drift between hub and Redis server at connect time. Drift > clockSkewMargin (default 5s) breaks UUIDv7 timestamp ordering for history replay.",
		ConstLabels: m.transportTypeLabels(),
	})

	m.consumerLag = prometheus.NewGauge(prometheus.GaugeOpts{
		Name:        metricConsumerLag,
		Help:        "Stream entries this node's consumer group has not yet consumed (XINFO GROUPS lag). Sustained non-zero = hub falling behind real-time. Reports -1 when Redis cannot compute lag deterministically.",
		ConstLabels: m.transportTypeLabels(),
	})

	m.consumerPending = prometheus.NewGauge(prometheus.GaugeOpts{
		Name:        metricConsumerPending,
		Help:        "Messages this node XREADGROUP'd but has not XACK'd yet (PEL size). Sustained growth indicates dispatch/ack pipeline backup beyond normal batch churn.",
		ConstLabels: m.transportTypeLabels(),
	})

	m.consumerGroupsCount = prometheus.NewGauge(prometheus.GaugeOpts{
		Name:        metricConsumerGroups,
		Help:        "Number of consumer groups attached to the stream (XINFO GROUPS length). One group per hub instance plus any not-yet-GC'd zombies. Gates the 'publisher writing to a stream nobody reads' alert via (publish_rate > 0 AND groups == 0). Reports -1 on XINFO error.",
		ConstLabels: m.transportTypeLabels(),
	})

	m.historyWindowSeconds = prometheus.NewGauge(prometheus.GaugeOpts{
		Name:        metricHistoryWindowSeconds,
		Help:        "Elapsed wall-clock seconds between the stream's first and last entry IDs (Redis-stamped <ms>-<seq>). Reports the available history window proactively — alert when this falls below expected client reconnect duration to catch retention-too-tight regressions before clients miss. Reports 0 on empty stream; -1 when head/tail entry IDs cannot be parsed.",
		ConstLabels: m.transportTypeLabels(),
	})

	m.dispatchZeroMatchTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name:        metricDispatchZeroMatchTotal,
		Help:        "XREADGROUP'd events that fanned out to zero local subscribers. Multiplied by replica count, surfaces fleet-wide fan-out cost of chatty publishers whose topics nobody subscribes to.",
		ConstLabels: m.transportTypeLabels(),
	})
}

// resolveShardChildren pre-resolves the per-shard observer/gauge children so
// the dispatch hot path avoids a WithLabelValues map lookup per message. Must
// be called once after registerAll, with numShards > 0. Idempotent: calling
// twice with the same numShards returns identical observer references because
// HistogramVec/GaugeVec memoize children by signature.
func (m *Metrics) resolveShardChildren(numShards int) {
	m.shardDispatchObs = make([]prometheus.Observer, numShards)
	m.shardSubGauges = make([]prometheus.Gauge, numShards)

	for i := range numShards {
		label := strconv.Itoa(i)
		m.shardDispatchObs[i] = m.dispatchDuration.WithLabelValues(label)
		m.shardSubGauges[i] = m.shardSubscribers.WithLabelValues(label)
	}
}

// buildPreBindingDropsMetric initializes the pre-binding-drops collector
// using the swappable wrapper so registerAll can re-bind the value source
// on Caddy-reload adoption. valueSource is the transport's atomic counter
// exposed as float64; nil disables the metric (no collector built,
// registerAll skips it).
func (m *Metrics) buildPreBindingDropsMetric(valueSource func() float64) {
	if valueSource == nil {
		return
	}

	m.preBindingDropsValueSource = valueSource
	m.preBindingMetricDrops = newSwappableCounterFunc(prometheus.CounterOpts{
		Name:        metricPreBindingMetricDropsTotal,
		Help:        "Metric-emission attempts that landed before RegisterMetricsWith bound the collector. Non-zero means the late-binding window covered live traffic — bind metrics earlier (direct API: WithPrometheusRegisterer; Caddy: parent metrics_registry). Read-through, so a scrape always reflects the running atomic total even after binding.",
		ConstLabels: m.transportTypeLabels(),
	}, valueSource)
}

// buildBuildInfoMetric initializes the build_info gauge.
func (m *Metrics) buildBuildInfoMetric() {
	m.buildInfo = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:        metricBuildInfo,
		Help:        "Redistransport build provenance (constant 1; labels: version, revision, go_version).",
		ConstLabels: m.transportTypeLabels(),
	}, []string{"version", "revision", "go_version"})
}

// buildErrorCounters initializes per-operation error counters for presence,
// stream decode, and zombie-GC failures.
func (m *Metrics) buildErrorCounters() {
	m.presenceReadErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        metricPresenceReadErrorsTotal,
		Help:        "Per-key failures in GetSubscribers (labels: kind=get|unmarshal). Non-zero indicates partial presence results.",
		ConstLabels: m.transportTypeLabels(),
	}, []string{labelKeyKind})

	m.presenceReadSkipped = prometheus.NewCounter(prometheus.CounterOpts{
		Name:        "mercure_redis_presence_read_skipped_total",
		Help:        "Presence keys skipped in GetSubscribers due to expected transient states (missing key / empty value).",
		ConstLabels: m.transportTypeLabels(),
	})

	m.presenceReadCapped = prometheus.NewCounter(prometheus.CounterOpts{
		Name:        "mercure_redis_presence_read_capped_total",
		Help:        "GetSubscribers calls refused because the subscriber count exceeded subscriptions_max_subscribers (returns 503, not partial results).",
		ConstLabels: m.transportTypeLabels(),
	})

	m.streamDecodeErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        metricStreamDecodeErrorsTotal,
		Help:        "Stream entries dropped on the consumer side before dispatch (labels: kind=data_type|unmarshal|forbidden_sse_chars). forbidden_sse_chars: the decoded update carried a CR/LF/NUL in id or type that would forge SSE frames — dropped as defense-in-depth against an unvalidated or tampered stream entry.",
		ConstLabels: m.transportTypeLabels(),
	}, []string{labelKeyKind})

	m.zombieGCErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Name:        "mercure_redis_zombie_gc_errors_total",
		Help:        "Per-group failures during zombie consumer-group cleanup. Persistent non-zero may indicate zombie accumulation.",
		ConstLabels: m.transportTypeLabels(),
	})

	m.publishErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        "mercure_redis_publish_errors_total",
		Help:        "Dispatch() failures (labels: kind=encode|publish). Divide by publish_duration_seconds_count to derive publish error rate.",
		ConstLabels: m.transportTypeLabels(),
	}, []string{labelKeyKind})

	m.xreadgroupErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        "mercure_redis_xreadgroup_errors_total",
		Help:        "XREADGROUP failures (labels: kind=nogroup|nogroup_recreate_failed|xack|other). `nogroup` is a recoverable blip; `nogroup_recreate_failed` means re-create after NOGROUP failed (ACL/replica) — investigate before silent ingest stop; `xack` means batch XAck failed (PEL grows but no data loss); `other` indicates connectivity/ACL problems.",
		ConstLabels: m.transportTypeLabels(),
	}, []string{labelKeyKind})

	m.ttlCleanupErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Name:        metricTTLCleanupErrorsTotal,
		Help:        "Failures of the periodic XTRIM-by-MINID call that enforces event_ttl. Persistent non-zero means stream growth is unbounded despite configured TTL.",
		ConstLabels: m.transportTypeLabels(),
	})

	m.presenceHeartbeatErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Name:        "mercure_redis_presence_heartbeat_errors_total",
		Help:        "Failed presence-key SET calls. A sustained rate risks presenceTTL expiry and zombie-group GC of this node from other nodes.",
		ConstLabels: m.transportTypeLabels(),
	})

	m.presenceFallback = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        metricPresenceFallbackTotal,
		Help:        "Presence-payload encoding decisions that fell back to summary mode. Counted at decision time, before the Redis SET — a subsequent SET failure does NOT decrement. Labels: reason=byte_budget (count-gate passed but detail payload crossed presenceDetailByteThreshold) | summary_marshal_failed (summary fallback itself failed; over-budget detail shipped as last-resort liveness) | count_race (lock-free count gate read at-or-below threshold but the live walk overran it; bounded walk fell back to summary).",
		ConstLabels: m.transportTypeLabels(),
	}, []string{labelKeyReason})
}

// buildHistoryReplayCounters builds the history-replay outcome counters. These
// are operational signals (not errors): fallback = Pass 1 missed → full-stream
// rescan; truncated = backlog not fully delivered. Each is the aggregate
// companion to a mercure.history.* span attribute.
func (m *Metrics) buildHistoryReplayCounters() {
	m.historyReplayFallback = prometheus.NewCounter(prometheus.CounterOpts{
		Name:        metricHistoryReplayFallbackTotal,
		Help:        "Replays that fell through Pass 1 because the requested Last-Event-ID was past retention, triggering a Pass 2 full-stream replay. Sustained rate means retention is too tight for the reconnect window.",
		ConstLabels: m.transportTypeLabels(),
	})

	m.historyReplayTruncated = prometheus.NewCounter(prometheus.CounterOpts{
		Name:        metricHistoryReplayTruncatedTotal,
		Help:        "Replays cut short before delivering the full backlog because a matched update couldn't be handed off (subscriber disconnected, for any reason, or buffer full). Sustained rate means reconnecting clients miss history (slow consumers or restart churn). Aggregate companion to the mercure.history.truncated span attribute.",
		ConstLabels: m.transportTypeLabels(),
	})

	m.historyCursorRejected = prometheus.NewCounter(prometheus.CounterOpts{
		Name:        metricHistoryCursorRejectedTotal,
		Help:        "History-replay entries whose eventID stream metadata carried forbidden SSE chars (CR/LF/NUL), so the Last-Event-Id cursor was not advanced (the entry was still delivered with its validated update.ID). Should be zero; non-zero indicates a tampered stream or a non-compliant hub. The Warn log for this event is rate-sampled — this counter is the authoritative signal.",
		ConstLabels: m.transportTypeLabels(),
	})
}

// registerAll registers every collector on m with reg. Caddy reuses the same
// Prometheus registry across Provision calls on config reload, so Register
// may return AlreadyRegisteredError; safeRegister swaps in the registry's
// existing collector in that case — otherwise m would hold detached
// collectors that increment forever without being scraped. The per-domain
// helpers below mirror the build* split so registration stays grouped with
// construction; every built collector must appear in exactly one of them.
func (m *Metrics) registerAll(reg prometheus.Registerer) {
	m.registerDispatchMetrics(reg)
	m.registerHistoryReplayMetrics(reg)
	m.registerSubscriberMetrics(reg)
	m.registerHealthMetrics(reg)
	m.registerPresenceAndErrorMetrics(reg)
	m.registerPreBindingDropsMetric(reg)
}

func (m *Metrics) registerDispatchMetrics(reg prometheus.Registerer) {
	m.dispatchDuration = safeRegister(reg, m.dispatchDuration)
	m.dispatchSubscribersTotal = safeRegister(reg, m.dispatchSubscribersTotal)
	m.publishDuration = safeRegister(reg, m.publishDuration)
	m.publishPayloadBytes = safeRegister(reg, m.publishPayloadBytes)
	m.xreadgroupLatency = safeRegister(reg, m.xreadgroupLatency)
	m.xreadgroupBatch = safeRegister(reg, m.xreadgroupBatch)
	m.dispatchZeroMatchTotal = safeRegister(reg, m.dispatchZeroMatchTotal)
}

func (m *Metrics) registerHistoryReplayMetrics(reg prometheus.Registerer) {
	m.historyReplayDuration = safeRegister(reg, m.historyReplayDuration)
	m.historyReplayConcurrent = safeRegister(reg, m.historyReplayConcurrent)
	m.historyReplayFallback = safeRegister(reg, m.historyReplayFallback)
	m.historyReplayTruncated = safeRegister(reg, m.historyReplayTruncated)
	m.historyCursorRejected = safeRegister(reg, m.historyCursorRejected)
	m.historyWindowSeconds = safeRegister(reg, m.historyWindowSeconds)
}

func (m *Metrics) registerSubscriberMetrics(reg prometheus.Registerer) {
	m.subscriberAddTotal = safeRegister(reg, m.subscriberAddTotal)
	m.subscriberRateLimited = safeRegister(reg, m.subscriberRateLimited)
	m.publisherRateLimited = safeRegister(reg, m.publisherRateLimited)
	m.subscriberRemoveTotal = safeRegister(reg, m.subscriberRemoveTotal)
	m.addSubscriberDuration = safeRegister(reg, m.addSubscriberDuration)
	m.removeSubscriberDuration = safeRegister(reg, m.removeSubscriberDuration)
	m.subscribersLost = safeRegister(reg, m.subscribersLost)
	m.shardSubscribers = safeRegister(reg, m.shardSubscribers)
}

func (m *Metrics) registerHealthMetrics(reg prometheus.Registerer) {
	m.healthy = safeRegister(reg, m.healthy)
	m.streamLength = safeRegister(reg, m.streamLength)
	m.clockDriftSeconds = safeRegister(reg, m.clockDriftSeconds)
	m.consumerLag = safeRegister(reg, m.consumerLag)
	m.consumerPending = safeRegister(reg, m.consumerPending)
	m.consumerGroupsCount = safeRegister(reg, m.consumerGroupsCount)
	m.buildInfo = safeRegister(reg, m.buildInfo)
}

func (m *Metrics) registerPresenceAndErrorMetrics(reg prometheus.Registerer) {
	m.presencePayloadBytes = safeRegister(reg, m.presencePayloadBytes)
	m.presenceReadErrors = safeRegister(reg, m.presenceReadErrors)
	m.presenceReadSkipped = safeRegister(reg, m.presenceReadSkipped)
	m.presenceReadCapped = safeRegister(reg, m.presenceReadCapped)
	m.subscriberAdmissionRejected = safeRegister(reg, m.subscriberAdmissionRejected)
	m.streamDecodeErrors = safeRegister(reg, m.streamDecodeErrors)
	m.zombieGCErrors = safeRegister(reg, m.zombieGCErrors)
	m.publishErrors = safeRegister(reg, m.publishErrors)
	m.xreadgroupErrors = safeRegister(reg, m.xreadgroupErrors)
	m.presenceHeartbeatErrors = safeRegister(reg, m.presenceHeartbeatErrors)
	m.presenceFallback = safeRegister(reg, m.presenceFallback)
	m.ttlCleanupErrors = safeRegister(reg, m.ttlCleanupErrors)
}

func (m *Metrics) registerPreBindingDropsMetric(reg prometheus.Registerer) {
	if m.preBindingMetricDrops != nil {
		adopted := safeRegister(reg, m.preBindingMetricDrops)
		// On Caddy reload, the registry persists across module instances; if
		// a prior transport already registered the metric, safeRegister
		// returns its collector. The prior collector still references the
		// prior transport's atomic — redirect the closure to this transport
		// so its drops are scraped instead of a now-detached counter.
		// Idempotent on first registration (sets the same closure twice).
		adopted.setSource(m.preBindingDropsValueSource)
		m.preBindingMetricDrops = adopted
	}
}

// safeRegister registers c with reg and returns the live collector to use.
// On prometheus.AlreadyRegisteredError it returns the registry's pre-existing
// collector so callers stay attached to the scraped object — critical on
// Caddy config reloads where the registry persists across Mercure Provision()
// calls. On any other error, or a type mismatch between the new and existing
// collector, it panics: both indicate a programming error we want to surface
// immediately rather than silently drop metrics.
//
// The generic T is the whole point: it lets each caller keep its concrete
// collector type after adoption. The ireturn linter's config allows this
// pattern via the `generic` allowlist entry.
func safeRegister[T prometheus.Collector](reg prometheus.Registerer, c T) T {
	err := reg.Register(c)
	if err == nil {
		return c
	}

	var are prometheus.AlreadyRegisteredError
	if !errors.As(err, &are) {
		panic(err)
	}

	existing, ok := are.ExistingCollector.(T)
	if !ok {
		// safeRegister's type-mismatch branch only fires on a developer error
		// (registered a Gauge where a Counter exists, etc.) at construction —
		// %T on both sides gives enough context to locate the offender in
		// newMetrics without the complexity of introspecting FQName.
		panic(fmt.Errorf(
			"redistransport: registry already holds a different collector type (new=%T, existing=%T): %w",
			c, are.ExistingCollector, err,
		))
	}

	return existing
}
