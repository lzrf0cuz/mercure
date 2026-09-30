package redistransport

import (
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// ErrInvalidOptions is returned by NewRedisTransport when the configured
// options fail validation. Individual violations are joined via errors.Join
// so operators see every problem in one error.
var ErrInvalidOptions = errors.New("redis transport: invalid option")

// Default values applied by defaultOptions when WithX overrides are absent.
const (
	defaultStreamName = "mercure"
	defaultEncoding   = "json"
)

// Soft-warning thresholds. Outside these, a WithX value is legal but likely
// unintended; the transport logs a Warn at startup so misconfiguration is
// visible. Within them, it's treated as an advanced knob the operator owns.
const (
	// xreadBlock below this is treated as busy-polling.
	xreadBlockBusyPollThreshold = 10 * time.Millisecond
	// dispatchShards above this is hard-clamped at initShards (not warned here).
	suspiciousDispatchShardsMax = 256
	// xreadCount above this triggers a memory-pressure warning — the listener
	// buffers up to N entries per BLOCK call before fan-out completes.
	suspiciousXReadCountMax = 10_000
	// clockSkewMargin above this triggers a wide-replay-scan warning — history
	// replay seeks back this much wall-clock per reconnect.
	suspiciousClockSkewMarginMax = 1 * time.Hour
	// healthThreshold at this value is flappy on noisy networks (a single
	// failed PING flips Ready to unhealthy with no flap suppression).
	flappyHealthThreshold = 1
	// subscriberRetryAfter above this is an unusually long client back-off hint.
	suspiciousRetryAfterMax = 5 * time.Minute
)

// validateOptions enforces strict invariants that would otherwise cause silent
// incorrect behavior at runtime: negative TTL/maxLength, a presence interval
// that does not leave headroom before presenceTTL (self-eviction), or an
// unknown codec encoding. Multiple failures are joined so callers see every
// problem at once.
func validateOptions(o *options) error {
	var errs []error

	if o.eventTTL < 0 {
		errs = append(errs, fmt.Errorf("%w: WithEventTTL must be >= 0 (got %v) — a negative TTL would make every cleanup trim the whole stream", ErrInvalidOptions, o.eventTTL))
	}

	if o.maxLength < 0 {
		errs = append(errs, fmt.Errorf("%w: WithMaxLength must be >= 0 (got %d) — Redis rejects negative MAXLEN, every publish would fail", ErrInvalidOptions, o.maxLength))
	}

	if o.subscriptionsMaxSubscribers < 0 {
		errs = append(errs, fmt.Errorf("%w: WithSubscriptionsMaxSubscribers must be >= 0 (got %d) — 0 disables the cap, negative is meaningless", ErrInvalidOptions, o.subscriptionsMaxSubscribers))
	}

	if o.subscriberMaxCount < 0 {
		errs = append(errs, fmt.Errorf("%w: WithSubscriberMaxCount must be >= 0 (got %d) — 0 disables the concurrent-count ceiling, negative is meaningless", ErrInvalidOptions, o.subscriberMaxCount))
	}

	if o.subscriberAdmissionTimeout < 0 {
		errs = append(errs, fmt.Errorf("%w: WithSubscriberAdmissionTimeout must be >= 0 (got %v) — 0 is fail-fast, negative is meaningless", ErrInvalidOptions, o.subscriberAdmissionTimeout))
	}

	if o.subscriberRegTimeout < 0 {
		errs = append(errs, fmt.Errorf("%w: WithSubscriberRegistrationTimeout must be >= 0 (got %v) — 0 disables the bound, negative is meaningless", ErrInvalidOptions, o.subscriberRegTimeout))
	}

	if o.subscriberRetryAfter < 0 {
		errs = append(errs, fmt.Errorf("%w: WithSubscriberRetryAfter must be >= 0 (got %v) — it is the base retry delay, negative is meaningless", ErrInvalidOptions, o.subscriberRetryAfter))
	}

	if !o.skipPresenceIntervalCheck && o.presenceInterval >= o.presenceTTL {
		errs = append(errs, fmt.Errorf("%w: WithPresenceInterval (%v) must be strictly less than WithPresenceTTL (%v) — at equal values a single scheduling jitter self-evicts this node's presence key", ErrInvalidOptions, o.presenceInterval, o.presenceTTL))
	}

	// Report an unknown encoding with the other failures. The error wraps both
	// ErrInvalidOptions and mercure.ErrUnsupportedCodec.
	if _, err := newCodec(o.encoding); err != nil {
		errs = append(errs, fmt.Errorf("%w: WithEncoding: %w", ErrInvalidOptions, err))
	}

	return errors.Join(errs...)
}

// warnSuspiciousOptions logs a Warn for legal-but-likely-misconfigured option
// values. These do not fail startup because advanced operators may have a
// valid reason (e.g. test harnesses with tight intervals), but the warning
// surfaces the value so typos or stale configs become visible.
func warnSuspiciousOptions(o *options, logger *slog.Logger) {
	if o.xreadBlock > 0 && o.xreadBlock < xreadBlockBusyPollThreshold {
		logger.Warn(
			"redis transport: WithXReadBlock is very short — XREADGROUP will busy-poll, wasting Redis RTTs",
			"xread_block", o.xreadBlock,
			"recommended_minimum", xreadBlockBusyPollThreshold,
		)
	}

	if o.xreadCount > suspiciousXReadCountMax {
		logger.Warn(
			"redis transport: WithXReadCount is very high — listener buffers up to N entries per BLOCK call (memory pressure during fan-out)",
			"xread_count", o.xreadCount,
			"recommended_maximum", suspiciousXReadCountMax,
		)
	}

	if o.clockSkewMargin > suspiciousClockSkewMarginMax {
		logger.Warn(
			"redis transport: WithClockSkewMargin is very large — history replay scans a wide stream window per reconnect",
			"clock_skew_margin", o.clockSkewMargin,
			"recommended_maximum", suspiciousClockSkewMarginMax,
		)
	}

	if o.healthThreshold == flappyHealthThreshold {
		logger.Warn(
			"redis transport: WithHealthThreshold is 1 — single failed PING flips Ready to unhealthy with no flap suppression (consider 2-3 on noisy networks)",
			"health_threshold", o.healthThreshold,
		)
	}

	if o.subscriberAdmissionTimeout > 0 && o.subscriberRateLimit == 0 {
		logger.Warn(
			"redis transport: WithSubscriberAdmissionTimeout has no effect without WithSubscriberRateLimit > 0 — there is no rate token to wait for, so the smoothing wait is inert",
			"subscriber_admission_timeout", o.subscriberAdmissionTimeout,
		)
	}

	if o.subscriberRetryAfter > suspiciousRetryAfterMax {
		logger.Warn(
			"redis transport: WithSubscriberRetryAfter is very large — the 429 Retry-After header tells clients to back off this long",
			"subscriber_retry_after", o.subscriberRetryAfter,
			"recommended_maximum", suspiciousRetryAfterMax,
		)
	}
}

// Option configures a RedisTransport (works with both Redis and Valkey).
//
// Each WithX function normalizes its own input:
//
//   - Duration/int options with a "sensible positive default" ignore values
//     ≤ 0 and keep the default (e.g. WithPresenceTTL, WithHealthInterval).
//   - Options whose 0 has a distinct meaning (disabled/unlimited/auto) store
//     0 verbatim (e.g. WithMaxLength, WithEventTTL, WithZombieGCInterval,
//     WithSubscriberRateLimit, WithDispatchShards).
//   - String and pointer options ignore empty/nil and keep the default,
//     except WithPrometheusRegisterer which stores nil verbatim so callers
//     can disable metrics mid-build.
//
// NewRedisTransport runs validateOptions (strict) and warnSuspiciousOptions
// (soft) after every option has been applied:
//
//   - Strict (returns ErrInvalidOptions, refuses to construct): eventTTL < 0,
//     maxLength < 0, a negative subscriptionsMaxSubscribers,
//     subscriberMaxCount, subscriberAdmissionTimeout, subscriberRegTimeout or
//     subscriberRetryAfter, presenceInterval ≥ presenceTTL, encoding not in
//     {json, gob, msgpack}.
//   - Soft (logs Warn at startup): xreadBlock < 10ms (busy polling),
//     xreadCount > 10 000 (per-batch memory pressure),
//     clockSkewMargin > 1h (wide replay scan), healthThreshold == 1
//     (flappy on noisy networks), subscriberAdmissionTimeout without a
//     subscriber rate limit, subscriberRetryAfter > 5m, an explicit
//     historyReplayConcurrency at or above the go-redis pool size.
//   - Hard cap at initShards: dispatchShards > 256 clamps to 256 with a
//     Warn log (auto-detect via NumCPU passes through the same cap).
type Option func(*options)

type options struct {
	streamName                  string
	maxLength                   int64
	encoding                    string
	presenceTTL                 time.Duration
	presenceInterval            time.Duration
	zombieGCInterval            time.Duration
	clockSkewMargin             time.Duration
	eventTTL                    time.Duration
	cleanupInterval             time.Duration
	healthInterval              time.Duration
	healthThreshold             int
	xreadCount                  int64
	xreadBlock                  time.Duration
	dispatchShards              int
	subscriptionsMaxSubscribers int
	subscriberMaxCount          int
	subscriberAdmissionTimeout  time.Duration
	subscriberRegTimeout        time.Duration
	subscriberRetryAfter        time.Duration
	subscriberRateLimit         float64
	subscriberRateBurst         int
	publisherRateLimit          float64
	publisherRateBurst          int
	historyReplayConcurrency    int
	presenceDetailThreshold     int
	presenceDetailByteThreshold int64
	prometheusRegisterer        prometheus.Registerer
	logger                      *slog.Logger
	skipVersionCheck            bool
	skipPresenceIntervalCheck   bool
}

func defaultOptions() *options {
	return &options{
		streamName:       defaultStreamName,
		maxLength:        0, // unlimited
		encoding:         defaultEncoding,
		presenceTTL:      60 * time.Second,
		presenceInterval: 30 * time.Second,
		zombieGCInterval: 5 * time.Minute,
		clockSkewMargin:  5 * time.Second,
		// 1h of time-based retention bounds the stream when neither max_length nor
		// event_ttl is set; 0 opts out.
		eventTTL:        time.Hour, // 0 to opt out
		cleanupInterval: 5 * time.Minute,
		healthInterval:  10 * time.Second,
		healthThreshold: 3,
		xreadCount:      100,
		xreadBlock:      time.Second,
		dispatchShards:  1,
		// Cap GetSubscribers materialization so a huge fleet can't OOM the hub by
		// listing the whole cluster; 0 disables. See WithSubscriptionsMaxSubscribers.
		subscriptionsMaxSubscribers: 100_000,
		// Admission control (TryAdmit) is off by default: with no ceiling and no
		// rate limit the gate only tracks a live count.
		subscriberMaxCount:         0, // 0 = no concurrent-count ceiling
		subscriberAdmissionTimeout: 0, // 0 = fail-fast (Allow only, no smoothing wait)
		// 0 = registration (history replay) is unbounded. ~5s is a reasonable value
		// when enabling admission.
		subscriberRegTimeout: 0,
		subscriberRetryAfter: 2 * time.Second, // base for the jittered Retry-After
		subscriberRateLimit:  0,               // disabled (0 = no rate limit)
		subscriberRateBurst:  5000,
		publisherRateLimit:   0, // disabled (0 = no rate limit)
		publisherRateBurst:   5000,
		// 0 = derive from the client's pool size at NewRedisTransport (see
		// defaultHistoryReplayConcurrency).
		historyReplayConcurrency: 0,
		// Above 1k subscribers the detail payload can reach several MB per heartbeat,
		// blocking single-threaded Redis on each SET. presenceDetailByteThreshold is
		// the direct check; this count gate is a cheap pre-filter.
		presenceDetailThreshold: 1_000,
		// Byte budget for the detail payload; larger payloads fall back to summary
		// mode. 0 disables it.
		presenceDetailByteThreshold: 512 * 1024,
		logger:                      slog.Default(),
	}
}

// clockSkewMarginMs returns clockSkewMargin in milliseconds, clamped at 0.
func (o *options) clockSkewMarginMs() uint64 {
	ms := o.clockSkewMargin.Milliseconds()
	if ms < 0 {
		return 0
	}

	return uint64(ms)
}

// WithStreamName sets the Redis Stream key name.
//
//   - Default: "mercure".
//   - Empty string is ignored (default preserved).
//   - The name is wrapped in `{...}` hash tags so Redis Cluster pins the
//     stream, lastEventID, and presence keys to a single slot.
func WithStreamName(name string) Option {
	return func(o *options) {
		if name != "" {
			o.streamName = name
		}
	}
}

// WithMaxLength sets the approximate MAXLEN for stream trimming.
//
//   - Default: 0 (unlimited; stream trimming disabled for length).
//   - Negative values are rejected by NewRedisTransport (Redis would refuse
//     every publish otherwise).
//   - Uses Redis's ~ (approximate) trimming for performance — the stream may
//     briefly exceed n by a small margin between trims.
func WithMaxLength(n int64) Option {
	return func(o *options) {
		o.maxLength = n
	}
}

// WithEncoding sets the codec encoding.
//
//   - Default: "json".
//   - Valid: "json", "gob", "msgpack".
//   - Empty string is ignored (default preserved).
//   - Unknown encodings are rejected by NewRedisTransport via NewCodec.
func WithEncoding(enc string) Option {
	return func(o *options) {
		if enc != "" {
			o.encoding = enc
		}
	}
}

// WithPresenceTTL sets the TTL for presence keys.
//
//   - Default: 60s.
//   - Values ≤ 0 are ignored (default preserved).
//   - If a node fails to renew before this TTL expires, other nodes' GC
//     will destroy its consumer group.
//   - Must be strictly greater than WithPresenceInterval; see that option.
func WithPresenceTTL(ttl time.Duration) Option {
	return func(o *options) {
		if ttl > 0 {
			o.presenceTTL = ttl
		}
	}
}

// WithPresenceInterval sets the heartbeat interval for presence renewal.
//
//   - Default: 30s.
//   - Values ≤ 0 are ignored (default preserved).
//   - Must be strictly less than WithPresenceTTL — NewRedisTransport rejects
//     configurations where interval ≥ TTL because a single scheduling jitter
//     would self-evict this node's presence key.
func WithPresenceInterval(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.presenceInterval = d
		}
	}
}

// WithZombieGCInterval sets the interval for periodic zombie consumer group cleanup.
//
//   - Default: 5m.
//   - 0 disables periodic GC; startup GC still runs once per process.
//   - Negative values are accepted but equivalent to 0 (the launch gate
//     requires d > 0 to start the goroutine).
func WithZombieGCInterval(d time.Duration) Option {
	return func(o *options) {
		o.zombieGCInterval = d
	}
}

// WithClockSkewMargin sets the margin for UUIDv7 timestamp seek.
//
//   - Default: 5s.
//   - Accepts 0 (disabled; use the exact UUIDv7 timestamp).
//   - Negative values are ignored (default preserved).
func WithClockSkewMargin(d time.Duration) Option {
	return func(o *options) {
		if d >= 0 {
			o.clockSkewMargin = d
		}
	}
}

// WithEventTTL sets the TTL-based stream cleanup duration.
//
//   - Default: 1h (safe — bounds stream growth even when operators forget
//     to set explicit retention; matches typical SSE client reconnect
//     ceilings).
//   - Pass 0 to opt out of TTL-based trimming entirely (combined with
//     WithMaxLength(0) the stream is unbounded — see the "Both zero is
//     unsafe in production" warning emitted at startup).
//   - Negative values are rejected by NewRedisTransport (negative TTL would
//     compute a future MINID, erasing the entire stream every cleanup tick).
//   - Paired with WithCleanupInterval: entries older than eventTTL are
//     trimmed via XTRIM MINID on every cleanup tick.
func WithEventTTL(ttl time.Duration) Option {
	return func(o *options) {
		o.eventTTL = ttl
	}
}

// WithCleanupInterval sets how often the TTL-cleanup goroutine runs.
//
//   - Default: 5m.
//   - Values ≤ 0 are ignored (default preserved).
//   - No-op if WithEventTTL is 0 — the TTL-cleanup goroutine is only
//     started when eventTTL > 0.
func WithCleanupInterval(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.cleanupInterval = d
		}
	}
}

// WithHealthInterval sets the health-check PING interval.
//
//   - Default: 10s.
//   - Values ≤ 0 are ignored (default preserved).
//   - On PING failure, WithHealthThreshold consecutive failures are required
//     before Ready() starts returning ErrTransportUnhealthy.
func WithHealthInterval(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.healthInterval = d
		}
	}
}

// WithHealthThreshold sets the number of consecutive PING failures required
// to mark the transport unhealthy.
//
//   - Default: 3.
//   - Values ≤ 0 are ignored (default preserved).
//   - Threshold=1 flips unhealthy on a single failed PING; acceptable for
//     very tight latency budgets but noisy under transient Redis hiccups.
func WithHealthThreshold(n int) Option {
	return func(o *options) {
		if n > 0 {
			o.healthThreshold = n
		}
	}
}

// WithXReadCount sets the batch size for XREADGROUP calls.
//
//   - Default: 100.
//   - Values ≤ 0 are ignored (default preserved).
//   - Higher values improve throughput at the cost of per-batch latency.
func WithXReadCount(n int64) Option {
	return func(o *options) {
		if n > 0 {
			o.xreadCount = n
		}
	}
}

// WithXReadBlock sets the XREADGROUP BLOCK timeout.
//
//   - Default: 1s.
//   - Values ≤ 0 are ignored (default preserved).
//   - Values below ~10ms log a Warn at startup — the read turns into busy
//     polling, wasting Redis RTTs.
//   - Lower values also make shutdown faster (the listener wakes up sooner
//     to observe ctx.Done()).
func WithXReadBlock(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.xreadBlock = d
		}
	}
}

// WithLogger sets the structured logger.
//
//   - Default: slog.Default().
//   - nil is ignored (default preserved); the transport never operates
//     without a logger.
func WithLogger(logger *slog.Logger) Option {
	return func(o *options) {
		if logger != nil {
			o.logger = logger
		}
	}
}

// WithDispatchShards sets the number of dispatch worker goroutines.
//
//   - Default: 1 (sequential dispatch — no shard worker goroutines spawned).
//   - Values ≤ 0 auto-detect to runtime.NumCPU().
//   - Hard ceiling at 256 (suspiciousDispatchShardsMax). Both explicit
//     overshoots and the auto-detect path on high-vCPU hosts clamp to the
//     ceiling at initShards time with a Warn log — this bounds the
//     per-shard Prometheus series fan-out via the {shard} label.
func WithDispatchShards(n int) Option {
	return func(o *options) {
		o.dispatchShards = n
	}
}

// WithSubscriberRateLimit sets the maximum rate of new subscriber additions
// per second.
//
//   - Default: 0 (disabled; no rate limiting).
//   - Negative values are ignored (default preserved).
//   - Zero disables the limiter entirely; WithSubscriberRateBurst has no
//     effect while the limit is 0.
//   - Uses a token bucket (golang.org/x/time/rate); burst via
//     WithSubscriberRateBurst.
//   - Enforced by the admission gate (TryAdmit), which sheds an over-rate
//     subscriber with 429. AddSubscriber does not apply it, so a direct
//     AddSubscriber call is not rate limited.
//   - Purpose: prevent thundering-herd storms on mass reconnect.
func WithSubscriberRateLimit(ratePerSecond float64) Option {
	return func(o *options) {
		if ratePerSecond >= 0 {
			o.subscriberRateLimit = ratePerSecond
		}
	}
}

// WithSubscriberMaxCount sets the per-node concurrent-subscriber ceiling
// enforced by the admission gate (TryAdmit). Over the ceiling, new subscribers
// are shed with 429 + Retry-After rather than accumulating until OOM. 0
// (default) disables the ceiling. Size it from the node's memory budget, not
// just the fd limit. NewRedisTransport rejects a negative value.
func WithSubscriberMaxCount(maxCount int) Option {
	return func(o *options) {
		o.subscriberMaxCount = maxCount
	}
}

// WithSubscriberAdmissionTimeout sets a bounded smoothing wait for a rate token
// before TryAdmit sheds (429). The wait observes the caller's context, so a
// client disconnecting mid-wait abandons it immediately. 0 (default) is pure
// fail-fast (Allow only). NewRedisTransport rejects a negative value.
func WithSubscriberAdmissionTimeout(d time.Duration) Option {
	return func(o *options) {
		o.subscriberAdmissionTimeout = d
	}
}

// WithSubscriberRegistrationTimeout bounds the post-admission registration
// (history replay), which runs under context.WithoutCancel, so a slow backend
// can't pin an admission slot indefinitely. 0 (default) disables the bound;
// ~5s is a reasonable value when enabling admission. NewRedisTransport rejects
// a negative value.
func WithSubscriberRegistrationTimeout(d time.Duration) Option {
	return func(o *options) {
		o.subscriberRegTimeout = d
	}
}

// WithSubscriberRetryAfter sets the base delay reported in the Retry-After
// header when the admission gate sheds a subscriber (429). The hub jitters it
// per response within [base, 1.5×base] so a static value can't re-synchronize a
// herd. Default 2s; 0 omits the Retry-After header entirely (opt out of the
// hint). NewRedisTransport rejects a negative value.
func WithSubscriberRetryAfter(d time.Duration) Option {
	return func(o *options) {
		o.subscriberRetryAfter = d
	}
}

// WithSubscriptionsMaxSubscribers caps how many subscribers GetSubscribers (the
// /subscriptions API) materializes before aborting with
// mercure.ErrTooManySubscribers, which the hub maps to 503. Without it, listing
// a huge fleet builds the whole cluster's subscriber slice in one request's heap
// → OOM. 0 disables the cap. Default 100000.
func WithSubscriptionsMaxSubscribers(maxSubscribers int) Option {
	return func(o *options) {
		o.subscriptionsMaxSubscribers = maxSubscribers
	}
}

// WithSubscriberRateBurst sets the burst allowance for the subscriber rate limiter.
//
//   - Default: 5000.
//   - Values ≤ 0 are ignored (default preserved).
//   - Has no effect when WithSubscriberRateLimit is 0 (rate limiter is not
//     constructed when rate == 0).
func WithSubscriberRateBurst(n int) Option {
	return func(o *options) {
		if n > 0 {
			o.subscriberRateBurst = n
		}
	}
}

// WithPublisherRateLimit sets the maximum publish rate per second.
//
//   - Default: 0 (disabled; no rate limiting).
//   - Negative values are ignored (default preserved).
//   - Zero disables the limiter; WithPublisherRateBurst has no effect when 0.
//   - Uses a token bucket (golang.org/x/time/rate).
//   - Purpose: protect Redis and downstream subscribers from publisher
//     spikes (e.g. a misbehaving service or replay job). Unlike
//     WithSubscriberRateLimit, which sheds, Dispatch waits for a token.
func WithPublisherRateLimit(ratePerSecond float64) Option {
	return func(o *options) {
		if ratePerSecond >= 0 {
			o.publisherRateLimit = ratePerSecond
		}
	}
}

// WithPublisherRateBurst sets the burst allowance for the publisher rate limiter.
//
//   - Default: 5000.
//   - Values ≤ 0 are ignored (default preserved).
//   - Has no effect when WithPublisherRateLimit is 0 (limiter not constructed).
func WithPublisherRateBurst(n int) Option {
	return func(o *options) {
		if n > 0 {
			o.publisherRateBurst = n
		}
	}
}

// WithHistoryReplayConcurrency sets the maximum number of concurrent
// history replay operations (XRANGE).
//
//   - Default: half the go-redis client's pool size, at least 1 and at most
//     20 (so a pool of 1 gets 1, with no headroom); 20 when the pool size is
//     not introspectable.
//   - Values ≤ 0 are ignored (default preserved).
//   - Each history replay issues multiple paginated XRANGE calls; limiting
//     concurrency keeps pool connections available for XREADGROUP, XADD,
//     and presence operations during reconnect storms. An explicit value at
//     or above the pool size logs a Warn at startup.
func WithHistoryReplayConcurrency(n int) Option {
	return func(o *options) {
		if n > 0 {
			o.historyReplayConcurrency = n
		}
	}
}

// WithPresenceDetailThreshold sets the maximum local subscriber count before
// presence switches from detailed to summary-only mode.
//
//   - Default: 1,000.
//   - Values ≤ 0 are ignored (default preserved).
//   - Below the threshold: the details the subscription API renders (ID,
//     subscribed matchers, their slugs and payloads) are serialized to the
//     presence key (array format).
//   - At or above the threshold: only the count is stored (presenceSummary
//     object) to avoid multi-MB SET payloads.
//   - See also WithPresenceDetailByteThreshold for the byte-budget companion
//     that catches large-payload subscribers blowing the payload below the
//     count threshold.
func WithPresenceDetailThreshold(n int) Option {
	return func(o *options) {
		if n > 0 {
			o.presenceDetailThreshold = n
		}
	}
}

// WithPresenceDetailByteThreshold sets the maximum marshaled byte size of
// the detail-mode presence payload before summary-mode fallback triggers.
// Byte-budget companion to WithPresenceDetailThreshold.
//
//   - Default: 524_288 (512 KiB).
//   - Values ≤ 0 are stored verbatim (≤ 0 disables the byte budget
//     entirely; presenceDetailThreshold's count gate is then the only
//     gate). Pass 0 to opt out explicitly.
//   - Computed AFTER json.Marshal of the detail payload, so it catches
//     the case where subscriber count is under the count threshold but
//     subscribers have long matcher lists / large payloads that blow
//     the payload past the byte ceiling.
//   - On fallback, the heartbeat tick re-marshals as summary and writes
//     that; the over-budget detail payload is discarded.
func WithPresenceDetailByteThreshold(n int64) Option {
	return func(o *options) {
		o.presenceDetailByteThreshold = n
	}
}

// withSkipPresenceIntervalCheck disables the presenceInterval < presenceTTL
// check, for tests that let a presence key expire on purpose.
func withSkipPresenceIntervalCheck() Option {
	return func(o *options) {
		o.skipPresenceIntervalCheck = true
	}
}

// withSkipVersionCheck disables the startup server-version check, for tests
// against miniredis, which does not implement INFO server.
func withSkipVersionCheck() Option {
	return func(o *options) {
		o.skipVersionCheck = true
	}
}

// WithSkipVersionCheckForTests lets tests in other packages build a
// RedisTransport against miniredis, which does not implement INFO server. It
// panics outside a test binary.
func WithSkipVersionCheckForTests() Option {
	if !testing.Testing() {
		panic("redis transport: WithSkipVersionCheckForTests called outside a test binary")
	}

	return withSkipVersionCheck()
}

// WithSkipPresenceIntervalCheckForTests lets tests in other packages set a
// presence interval at or above the presence TTL. It panics outside a test
// binary.
func WithSkipPresenceIntervalCheckForTests() Option {
	if !testing.Testing() {
		panic("redis transport: WithSkipPresenceIntervalCheckForTests called outside a test binary")
	}

	return withSkipPresenceIntervalCheck()
}

// WithPrometheusRegisterer sets the Prometheus registerer for metrics.
//
//   - Default: nil (metrics disabled).
//   - nil is explicitly stored (not ignored): passing nil after a previous
//     non-nil call disables metrics for subsequent NewRedisTransport calls
//     with the same options slice.
//   - Pass prometheus.DefaultRegisterer to enable metrics on the default
//     global registry.
//   - Pass a dedicated prometheus.NewRegistry() to isolate metrics per
//     transport instance — recommended in tests and when multiple
//     RedisTransports coexist in the same process, so each transport's
//     counters are scoped to its own registry rather than shared with peers.
func WithPrometheusRegisterer(reg prometheus.Registerer) Option {
	return func(o *options) {
		if isEffectivelyNilRegisterer(reg) {
			o.prometheusRegisterer = nil

			return
		}

		o.prometheusRegisterer = reg
	}
}

// isEffectivelyNilRegisterer reports whether reg is either an untyped nil
// interface or a typed-nil pointer (e.g. (*prometheus.Registry)(nil)).
//
// Caddy's ctx.GetMetricsRegistry() returns a typed-nil *prometheus.Registry
// during `caddy validate` and for sub-modules without a metrics registry; it
// compares != nil but panics on use.
func isEffectivelyNilRegisterer(reg prometheus.Registerer) bool {
	if reg == nil {
		return true
	}

	v := reflect.ValueOf(reg)

	return v.Kind() == reflect.Pointer && v.IsNil()
}
