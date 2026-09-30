package redistransport

import (
	"errors"
	"fmt"
	"math"
	"strconv"
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
	metricBuildInfo                   = "mercure_redis_build_info"
	metricPresenceReadErrorsTotal     = "mercure_redis_presence_read_errors_total"
	metricTTLCleanupErrorsTotal       = "mercure_redis_ttl_cleanup_errors_total"
	metricHistoryReplayFallbackTotal  = "mercure_redis_history_replay_fallback_total"
	metricHistoryReplayTruncatedTotal = "mercure_redis_history_replay_truncated_total"
	metricPresenceFallbackTotal       = "mercure_redis_presence_fallback_total"
	metricStreamDecodeErrorsTotal     = "mercure_redis_stream_decode_errors_total"
)

// Label-key constants for metric vectors.
const (
	labelKeyShard         = "shard"
	labelKeyKind          = "kind"
	labelKeyReason        = "reason"
	labelKeyTransportType = "transport_type"
	labelKeyBackendType   = "backend_type"
)

// transportTypeRedis is the transport_type label value. It is a dashboard
// contract kept separate from serverTypeRedis, which classifies INFO output;
// do not merge them.
const transportTypeRedis = "redis"

// Metrics provides Prometheus instrumentation for the Redis transport's
// hot paths: dispatch, XREADGROUP, history replay, subscriber lifecycle,
// and health/presence. All metrics use the "mercure_redis_" prefix.
type Metrics struct {
	// backendType is the backend_type const label ("redis", "valkey" or
	// "unknown"); read-only after construction.
	backendType serverType

	// Dispatch path (sharded).
	dispatchDuration         *prometheus.HistogramVec
	dispatchSubscribersTotal prometheus.Histogram
	publishDuration          prometheus.Histogram
	// publishPayloadBytes records the codec-encoded size of each published update
	// (the XADD message body).
	publishPayloadBytes prometheus.Histogram

	// XREADGROUP path.
	xreadgroupLatency prometheus.Histogram
	xreadgroupBatch   prometheus.Histogram

	// History replay.
	historyReplayDuration   prometheus.Histogram
	historyReplayConcurrent prometheus.Gauge
	// historyReplayFallback counts replays whose Last-Event-ID was no longer in
	// the stream (Pass 1 missed), so Pass 2 replayed the whole stream. A sustained
	// rate means retention (max_length / event_ttl) is shorter than clients'
	// reconnect window.
	historyReplayFallback prometheus.Counter
	// historyReplayTruncated counts replays that stopped early because a matched
	// update could not be delivered (subscriber gone or buffer full); it
	// aggregates the mercure.history.truncated span attribute.
	historyReplayTruncated prometheus.Counter

	// Subscriber lifecycle.
	subscriberAddTotal    prometheus.Counter
	subscriberRemoveTotal prometheus.Counter
	// publisherRateLimited counts Dispatch calls delayed by the publish
	// rate limiter (when WithPublisherRateLimit > 0). Non-zero rate is the
	// signal that a publisher is exceeding configured throughput.
	publisherRateLimited prometheus.Counter
	// addSubscriberDuration covers the whole AddSubscriber call: t.mu wait,
	// shard-list insert and history replay. A rising p99 points at t.mu contention.
	addSubscriberDuration prometheus.Histogram
	// removeSubscriberDuration covers RemoveSubscriber: t.mu wait, shard-list
	// removal and metric bookkeeping.
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
	// clockDriftSeconds is the absolute drift between the hub's and the Redis
	// server's clocks, sampled once at startup. History replay's UUIDv7 seek
	// assumes it stays below clockSkewMargin.
	clockDriftSeconds prometheus.Gauge
	// consumerLag is the number of stream entries this node's group has not yet
	// read (XINFO GROUPS lag), sampled at the health-check cadence; -1 when Redis
	// reports the lag as unknown.
	consumerLag prometheus.Gauge
	// consumerPending is the number of XREADGROUP'd messages this node has
	// not yet XACK'd (PEL size). Sustained growth — beyond what
	// xreadgroup_batch_size churn explains — points at a hub that is
	// reading from Redis but failing to dispatch+ack downstream.
	consumerPending prometheus.Gauge
	// consumerGroupsCount is the number of consumer groups on the stream (one per
	// hub plus any zombies not yet removed), sampled at the health-check cadence;
	// -1 when XINFO GROUPS fails. Zero groups with a non-zero publish rate means
	// no hub reads the stream.
	consumerGroupsCount prometheus.Gauge
	// historyWindowSeconds is the time between the stream's first and last entry
	// IDs, sampled at the health-check cadence: the history still available for
	// replay. Alert when it falls below the clients' reconnect window. 0 for an
	// empty stream, -1 when the head or tail XRANGE fails.
	historyWindowSeconds prometheus.Gauge
	// dispatchZeroMatchTotal counts stream entries that matched no local
	// subscriber.
	dispatchZeroMatchTotal prometheus.Counter

	// buildInfo is a constant-1 gauge labeled version/revision/go_version,
	// surfacing module provenance for cross-node comparison during rollouts.
	buildInfo *prometheus.GaugeVec

	// Presence.
	presencePayloadBytes prometheus.Histogram
	// presenceReadErrors counts presence read failures by kind: "get" is a failed
	// MGET (GetSubscribers returns an error; counted once per key in the batch),
	// "unmarshal" is a key that could not be decoded and was skipped.
	presenceReadErrors *prometheus.CounterVec
	// presenceReadCapped counts GetSubscribers calls refused because the cluster's
	// subscriber count exceeded WithSubscriptionsMaxSubscribers (the hub answers
	// 503). Non-zero means /subscriptions is unusable at this fleet size: raise the
	// cap or stop polling it.
	presenceReadCapped prometheus.Counter
	// subscriberAdmissionRejected counts new subscribers rejected with 429 by the
	// admission gate (TryAdmit), labelled reason=rate|capacity.
	subscriberAdmissionRejected *prometheus.CounterVec
	// streamDecodeErrors counts stream entries dropped on the consumer side
	// before dispatch. kind=data_type (the "data" field came back a non-string)
	// and kind=unmarshal (the codec rejected the bytes) are decode failures — a
	// non-zero rate signals producer/consumer codec drift or foreign data on the
	// stream key. kind=forbidden_sse_chars is a security drop: the payload decoded
	// fine but its id/type is not a valid SSE field value
	// (mercure.ValidSSEFieldValue) and could forge SSE frames.
	streamDecodeErrors *prometheus.CounterVec
	// zombieGCErrors counts per-group failures during zombie-group cleanup
	// (Exists/Destroy). The GC is fail-safe (errors skip the group rather
	// than falsely destroying a live node's group), so persistent errors
	// would otherwise silently accumulate zombie groups without signal.
	zombieGCErrors prometheus.Counter

	// publishErrors counts Dispatch() failures, labeled by kind (encode or
	// publish). Nonzero rate indicates either a codec failure (encode) or
	// Redis rejecting the publish (publish — Lua script error, MAXLEN
	// validation, etc). An update rejected as too large
	// (mercure.ErrCodecPayloadTooLarge) is a client error and is not counted.
	// Pair with publish_duration_seconds_count to derive error-rate.
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

	// presenceFallback counts heartbeats that fell back to summary mode, by reason
	// (see presenceFallbackReason). It is counted before the SET; SET failures are
	// counted in presenceHeartbeatErrors.
	presenceFallback *prometheus.CounterVec

	// ttlCleanupErrors counts failures of the periodic XTRIM-by-MINID call
	// that enforces event_ttl. A sustained non-zero rate means stream
	// growth is unbounded despite the configured TTL; pair with
	// stream_length to catch the cause before Redis runs out of memory.
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

// recordHistoryReplayFullScan increments historyReplayFallback (a Pass 2
// full-stream replay). Safe on nil *Metrics.
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

// recordPresenceReadError increments presence_read_errors for kind. Safe on a
// nil *Metrics.
func (m *Metrics) recordPresenceReadError(kind presenceReadErrorKind) {
	if m == nil {
		return
	}

	m.presenceReadErrors.WithLabelValues(string(kind)).Inc()
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
	// presenceFallbackCountRace fires when the lock-free count read was at or
	// below the threshold but the walk found more members; the payload falls back
	// to a summary.
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
	// lossReasonShutdown: emitted for each subscriber forcibly disconnected by
	// disconnectAllSubscribers that no other path counted first (markLost's
	// CAS). RemoveSubscriber returns ErrClosedTransport once closed (re-checked
	// under t.mu), so these drops are NOT also counted in subscriberRemoveTotal.
	lossReasonShutdown subscriberLossReason = "shutdown"
	// lossReasonHistoryReplayFailed: emitted when a subscriber's history
	// replay fails (e.g. an XRANGE error or the registration timeout). The
	// subscriber is listed during the replay, so live events may have been
	// dispatched to it, and backpressure or shutdown may count it first
	// (markLost's CAS). It stays listed until the hub's RemoveSubscriber after
	// the 503, so these drops are ALSO counted in subscriberRemoveTotal, unless
	// the transport closes first.
	lossReasonHistoryReplayFailed subscriberLossReason = "history_replay_failed"
	// lossReasonBackpressure: LocalSubscriber.Dispatch returned false during live
	// dispatch, because handleFullChan disconnected a slow subscriber or another
	// path had already disconnected it. The SSE handler then calls
	// RemoveSubscriber, so these drops are usually also in subscriberRemoveTotal,
	// unless the transport closes first.
	lossReasonBackpressure subscriberLossReason = "backpressure"
)

// lostCounter returns the subscribers_lost child for reason r, or nil when m
// is nil.
func (m *Metrics) lostCounter(r subscriberLossReason) prometheus.Counter {
	if m == nil {
		return nil
	}

	return m.subscribersLost.WithLabelValues(string(r))
}

// constLabelsFor returns the {transport_type, backend_type} const labels shared
// by Metrics and poolStatsCollector. An empty backendType becomes "unknown".
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

// newMetrics builds every collector, labelled with backendType ("unknown" when
// empty), and registers it on reg, which must be non-nil.
func newMetrics(reg prometheus.Registerer, backendType serverType) *Metrics {
	if reg == nil {
		panic("redis transport: newMetrics called with nil registerer")
	}

	m := &Metrics{backendType: backendType}
	m.buildHistograms()
	m.buildCountersAndGauges()
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
		Help:        "Size in bytes of each update after codec encoding (the XADD message body). Observed before the publish runs, so failed publishes are included.",
		Buckets:     []float64{100, 500, 1000, 5000, 10000, 50000, 100000, 500000},
		ConstLabels: m.transportTypeLabels(),
	})

	m.xreadgroupLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:        "mercure_redis_xreadgroup_latency_seconds",
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

// buildSubscriberLifecycleHistograms builds the add/remove duration histograms,
// with publishDuration's buckets so they can share a panel.
func (m *Metrics) buildSubscriberLifecycleHistograms() {
	m.addSubscriberDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:        "mercure_redis_add_subscriber_duration_seconds",
		Help:        "End-to-end AddSubscriber latency (t.mu + shard insert + history replay).",
		Buckets:     []float64{.0001, .0005, .001, .0025, .005, .01, .025, .05, .1},
		ConstLabels: m.transportTypeLabels(),
	})

	m.removeSubscriberDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:        "mercure_redis_remove_subscriber_duration_seconds",
		Help:        "End-to-end RemoveSubscriber latency (t.mu wait + shard remove + metric bookkeeping), observed only for removals counted in subscriber_remove_total.",
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
		Help:        "AddSubscriber calls that listed the subscriber. Calls rejected on a closed transport are excluded; adds whose history replay then failed are included.",
		ConstLabels: m.transportTypeLabels(),
	})

	m.publisherRateLimited = prometheus.NewCounter(prometheus.CounterOpts{
		Name:        "mercure_redis_publisher_rate_limited_total",
		Help:        "Dispatch calls delayed by the publish rate limiter.",
		ConstLabels: m.transportTypeLabels(),
	})

	m.subscriberRemoveTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name:        metricSubscriberRemoveTotal,
		Help:        "RemoveSubscriber calls that removed a listed subscriber from an open transport, including the hub's removal after a failed history replay. Redundant removals, removals of a subscriber that is not listed, and removals after Close are not counted.",
		ConstLabels: m.transportTypeLabels(),
	})

	m.subscribersLost = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        metricSubscribersLostTotal,
		Help:        "Subscribers dropped with operational cause label (shutdown, history_replay_failed, backpressure). Backpressure and history_replay_failed usually also appear in subscriber_remove_total, but not when the transport closes before the removal; shutdown never does.",
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
		Help:        "Seconds between the stream's first and last entry IDs: the history available for replay. Alert when it falls below the clients' reconnect window. 0 on an empty stream; -1 when the head or tail XRANGE fails.",
		ConstLabels: m.transportTypeLabels(),
	})

	m.dispatchZeroMatchTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name:        metricDispatchZeroMatchTotal,
		Help:        "Stream entries that matched no local subscriber.",
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
		Help:        "Presence read failures in GetSubscribers (labels: kind=get|unmarshal). get: an MGET failed and the call returned an error (counted once per key); unmarshal: a key could not be decoded and was skipped.",
		ConstLabels: m.transportTypeLabels(),
	}, []string{labelKeyKind})

	m.presenceReadCapped = prometheus.NewCounter(prometheus.CounterOpts{
		Name:        "mercure_redis_presence_read_capped_total",
		Help:        "GetSubscribers calls refused because the subscriber count exceeded subscriptions_max_subscribers (returns 503, not partial results).",
		ConstLabels: m.transportTypeLabels(),
	})

	m.streamDecodeErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        metricStreamDecodeErrorsTotal,
		Help:        "Stream entries dropped on the consumer side before dispatch (labels: kind=data_type|unmarshal|forbidden_sse_chars). forbidden_sse_chars: the decoded update's id or type is not a valid SSE field value (invalid UTF-8, a control character such as CR/LF/NUL, or a Unicode format character) and could forge SSE frames — dropped as defense-in-depth against an unvalidated or tampered stream entry.",
		ConstLabels: m.transportTypeLabels(),
	}, []string{labelKeyKind})

	m.zombieGCErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Name:        "mercure_redis_zombie_gc_errors_total",
		Help:        "Per-group failures during zombie consumer-group cleanup. Persistent non-zero may indicate zombie accumulation.",
		ConstLabels: m.transportTypeLabels(),
	})

	m.publishErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        "mercure_redis_publish_errors_total",
		Help:        "Dispatch() failures (labels: kind=encode|publish). An update rejected as too large (over the codec cap) is a client error and is not counted. Divide by publish_duration_seconds_count to derive publish error rate.",
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
		Help:        "Heartbeats that fell back to summary presence, counted before the Redis SET. Labels: reason=byte_budget (the detail payload exceeded presence_detail_byte_threshold) | count_race (membership grew past the threshold between the count read and the walk).",
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
}

// registerAll registers every collector with reg through safeRegister. Each
// built collector must appear in exactly one register* helper.
func (m *Metrics) registerAll(reg prometheus.Registerer) {
	m.registerDispatchMetrics(reg)
	m.registerHistoryReplayMetrics(reg)
	m.registerSubscriberMetrics(reg)
	m.registerHealthMetrics(reg)
	m.registerPresenceAndErrorMetrics(reg)
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
	m.historyWindowSeconds = safeRegister(reg, m.historyWindowSeconds)
}

func (m *Metrics) registerSubscriberMetrics(reg prometheus.Registerer) {
	m.subscriberAddTotal = safeRegister(reg, m.subscriberAddTotal)
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

// collectors returns every prometheus.Collector field of m, in registerAll
// order. RegisterMetricsWith registers this list on each registry after the
// first. TestMetricsCollectorListMatchesStructFields and
// TestMetricsCollectorsAreUnique keep it complete and duplicate-free.
func (m *Metrics) collectors() []prometheus.Collector {
	return []prometheus.Collector{
		m.dispatchDuration,
		m.dispatchSubscribersTotal,
		m.publishDuration,
		m.publishPayloadBytes,
		m.xreadgroupLatency,
		m.xreadgroupBatch,
		m.dispatchZeroMatchTotal,
		m.historyReplayDuration,
		m.historyReplayConcurrent,
		m.historyReplayFallback,
		m.historyReplayTruncated,
		m.historyWindowSeconds,
		m.subscriberAddTotal,
		m.publisherRateLimited,
		m.subscriberRemoveTotal,
		m.addSubscriberDuration,
		m.removeSubscriberDuration,
		m.subscribersLost,
		m.shardSubscribers,
		m.healthy,
		m.streamLength,
		m.clockDriftSeconds,
		m.consumerLag,
		m.consumerPending,
		m.consumerGroupsCount,
		m.buildInfo,
		m.presencePayloadBytes,
		m.presenceReadErrors,
		m.presenceReadCapped,
		m.subscriberAdmissionRejected,
		m.streamDecodeErrors,
		m.zombieGCErrors,
		m.publishErrors,
		m.xreadgroupErrors,
		m.presenceHeartbeatErrors,
		m.presenceFallback,
		m.ttlCleanupErrors,
	}
}

// safeRegister registers c with reg and returns the live collector to use.
// On prometheus.AlreadyRegisteredError it returns the registry's pre-existing
// collector so callers stay attached to the scraped object — critical when
// multiple RedisTransport instances share one Prometheus registry (e.g. two
// hub directives provisioned from the same Caddy config load). On any other
// error, or a type mismatch between the new and existing collector, it
// panics: both indicate a programming error we want to surface immediately
// rather than silently drop metrics.
//
// The type parameter keeps each caller's concrete collector type.
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
		panic(fmt.Errorf(
			"redis transport: registry already holds a different collector type (new=%T, existing=%T): %w",
			c, are.ExistingCollector, err,
		))
	}

	return existing
}
