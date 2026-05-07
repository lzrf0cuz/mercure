package redistransport

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"math"
	"math/rand/v2"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dunglas/mercure"
	"github.com/gofrs/uuid/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/time/rate"
)

// ErrClosedTransport is returned by Dispatch and AddSubscriber after Close.
var ErrClosedTransport = errors.New("redis transport: read/write on closed transport")

// Redis error-string fragments matched against error.Error() when the server
// surfaces them via untyped RESP error replies. Promoting to constants prevents
// silent breakage if a call site's literal is rewritten incorrectly.
const (
	redisErrNoGroup   = "NOGROUP"
	redisErrBusyGroup = "BUSYGROUP"
	redisErrNoSuchKey = "no such key"
)

// ErrUnsupportedServerVersion is returned when the server version is below the minimum required (Redis 6.2+ / Valkey 7.2+).
var ErrUnsupportedServerVersion = errors.New("redis transport: unsupported server version")

// ErrMissingVersionField is returned when INFO server responds without a parseable
// version field. Absent a version we cannot verify the minimum-floor contract
// (Redis 6.2+ / Valkey 7.2+) that XTRIM MINID depends on, so we fail fast rather
// than risk runtime errors on unsupported commands.
var ErrMissingVersionField = errors.New("redis transport: INFO server returned no parseable version field")

// errPoolCollectorAdoptFailed is returned by RegisterMetricsWith when
// reg.Register on poolStatsCollector reports AlreadyRegisteredError but the
// existing collector is NOT a *poolStatsCollector. Mirrors safeRegister's
// policy: refuse to silently scrape a foreign collector forever; instead
// fail Provision so the operator sees the integration conflict.
var errPoolCollectorAdoptFailed = errors.New("redis transport: pool stats collector adopt failed — existing collector is not *poolStatsCollector (programmer or integration error)")

// historyPageSize bounds memory during paginated XRANGE history replay.
// Each page buffers at most this many entries (~500KB at typical sizes).
const historyPageSize int64 = 1000

// Minimum server versions for Redis and Valkey.
const (
	minValkeyMajor = 7
	minValkeyMinor = 2
	minRedisMajor  = 6
	minRedisMinor  = 2
)

// numCoreBackgroundGoroutines is the count of always-running background goroutines
// started in NewRedisTransport: xreadgroupListener, presenceHeartbeat, healthCheck.
// TTL cleanup and zombie GC are optional and counted separately.
const numCoreBackgroundGoroutines = 3

// listenerStaleFloor is the minimum staleness threshold for Ready()'s
// listener-health gate. Ready computes the gate as 5 × xreadBlock; if
// that product is below this floor, the floor wins. Keeps readiness
// stable for operators who set sub-second xreadBlock values. 30s is
// below typical LB healthcheck intervals (60-300s) so a stuck listener
// is observed before the LB acts on its own cadence.
const listenerStaleFloor = 30 * time.Second

// serverType identifies a detected backend server family. Named type so
// the closed-set contract is enforced at compile time (matches the
// subscriberLossReason / publishErrorKind / xreadgroupErrorKind pattern
// used elsewhere in this package — a typo cannot silently create a new
// time series). Values are emitted unchanged as the `backend_type`
// Prometheus const label, so a rename here would break dashboard contract.
type serverType string

// Server-type identifiers reported by parseServerInfo.
const (
	serverTypeRedis   serverType = "redis"
	serverTypeValkey  serverType = "valkey"
	serverTypeUnknown serverType = "unknown"
)

// streamIDEarliest is the Redis stream sentinel for "before any entry" — used
// as the initial value of lastDispatchedStreamID and as a "no resume point"
// guard during history replay.
const streamIDEarliest = "0-0"

// Version string parsing bounds.
const (
	// versionPartsMax is the cap on dot-separated components split from a version
	// string (e.g. "7.2.4" → [7, 2, 4]).
	versionPartsMax = 3
	// versionPartsMin is the minimum components required for a valid version
	// (major.minor).
	versionPartsMin = 2
)

// RedisTransport implements mercure.Transport using Redis/Valkey Streams.
// All message delivery (including to local subscribers) flows through the
// stream via XREADGROUP — there is no local fast-path. This ensures
// all subscribers on all nodes see events in the same server-canonical order.
//
// Compatible with Redis 6.2+ and Valkey 7.2+. The transport auto-detects
// which server is connected at startup via INFO server.
type RedisTransport struct {
	client redis.UniversalClient
	// codec is published atomically: the XREADGROUP listener decodes (reads it)
	// without t.mu, and SetCodec (called by the hub during init, after the
	// listener has already started) writes it. atomic.Pointer over an interface
	// pointer avoids both the data race and atomic.Value's same-concrete-type
	// constraint (the constructor default and a SetCodec override may differ).
	codec  atomic.Pointer[mercure.Codec]
	logger *slog.Logger
	opts   *options
	// metrics is set lazily by RegisterMetricsWith — either from the
	// direct-construction path (initMetrics calling RegisterMetricsWith with
	// opts.prometheusRegisterer) or from the Caddy submodule path
	// (caddy/mercure.go calling RegisterMetricsWith with the parent module's
	// registry, post-GetTransport). Read via Load() so background goroutines
	// running before late binding completes see nil and skip emission rather
	// than panic on a partially-built collector.
	metrics     atomic.Pointer[Metrics]
	metricsOnce sync.Once

	// boundRegisterer holds the prometheus.Registerer that won metricsOnce.
	// Read by subsequent RegisterMetricsWith calls to detect silent drift —
	// a second call with a different registerer is ignored (sync.Once already
	// fired) but logs a Warn so operators chasing "metrics aren't on
	// registry X" find the trail. Same-instance second calls (the Caddy
	// TransportUsagePool reload pattern) are normal and stay silent.
	boundRegisterer atomic.Pointer[prometheus.Registerer]

	// backendType is the detected backend server family ("redis", "valkey",
	// or "unknown") sourced from INFO at validateVersion time. Latched once
	// during NewRedisTransport before initMetrics, then read by
	// RegisterMetricsWith → newMetrics → Metrics.transportTypeLabels to
	// stamp the backend_type const label on every transport-emitted series.
	// Plain serverType (not atomic) — the write in checkVersionFromInfo
	// happens during construction before *RedisTransport is published to
	// any other goroutine. Cross-goroutine reads (e.g., Caddy late-binding
	// RegisterMetricsWith) inherit the happens-before edge from however the
	// transport pointer was published (TransportUsagePool / module registry).
	backendType serverType

	// nodeID is a unique identifier for this transport instance,
	// generated on startup as a UUIDv4.
	nodeID string

	// nodeGroup is the consumer group name: "mercure:node:{nodeID}".
	// Each node gets its own consumer group so Redis Streams broadcasts
	// (not load-balances) to all nodes.
	nodeGroup string

	// lastDispatchedStreamID tracks the Redis Stream ID of the last
	// dispatched message. Written ONLY by the XREADGROUP goroutine (single
	// writer); read concurrently by AddSubscriber (the history/live boundary)
	// and the consumer-group resume path. Atomic so dispatch need not hold mu
	// across fan-out — see processStreamEntry for the advance-before-fan-out
	// ordering that keeps a joining subscriber gap-free without the lock.
	lastDispatchedStreamID atomic.Pointer[string]

	// afterMatchHookForTests, when set, runs in each dispatch path right after
	// the candidate match and before delivery. TEST-ONLY: the setter lives in a _test.go, so
	// this is nil in production (one atomic nil-load on the hot path). It lets a
	// test pin the exact window where a concurrently-joining subscriber misses
	// the live snapshot, proving the advance-before-fan-out cursor ordering keeps
	// delivery gap-free.
	afterMatchHookForTests atomic.Pointer[func(shardID int, u *mercure.Update)]

	// topicSelectorStore is received from the hub via SetTopicSelectorStore.
	// Injected into deserialized cross-node subscribers in GetSubscribers.
	// Uses atomic.Pointer for thread safety: written once during hub init,
	// read concurrently by GetSubscribers/unmarshalPresenceValue.
	topicSelectorStore atomic.Pointer[mercure.TopicSelectorStore]

	// Sharded dispatch state. shards[i] always holds the subscriber list for
	// shard i (len(shards) == numShards even when numShards == 1).
	// shardChans is nil and no workers are spawned when numShards == 1 —
	// dispatchToSubscribers uses shards[0] synchronously in that case.
	shards        []dispatchShard
	numShards     int
	shardChans    []chan *shardWork
	shardWorkPool *sync.Pool

	// ackIDsBuf is a reusable scratch slice for processStreamEntries's XAck
	// batch. Owned by the xreadgroupListener goroutine (single writer) — no
	// synchronization needed. Retained across batches to avoid per-batch
	// allocation.
	ackIDsBuf []string

	// addSubLimiter rate-limits new subscriber additions to prevent
	// thundering herd scenarios. Nil when rate limiting is disabled (rate=0).
	addSubLimiter *rate.Limiter

	// admissionLeasesHeld is the admission ceiling counter (TryAdmit): the count
	// of provisional+live admission slots. Deliberately independent of the
	// Redis-membership subscriberCount (different lifetime + looser race
	// contract since v0.0.4). Incremented by TryAdmit, decremented by the release
	// closure it returns (the hub handler's single defer).
	admissionLeasesHeld atomic.Int64

	// publishLimiter rate-limits Dispatch calls to protect Redis and
	// downstream subscribers from a misbehaving publisher / replay job.
	// Nil when rate limiting is disabled (publisherRateLimit=0).
	publishLimiter *rate.Limiter

	// dropLogLimiter caps the rate of consumer-side drop/reject log lines
	// (decode failures, forbidden-SSE-char drops, tampered-eventID cursor
	// rejects) so a flood of malformed or tampered stream entries can't drown
	// the logs. The per-kind counters remain the authoritative rate signal — only
	// the log is sampled, never the metric. Always on (unlike the opt-in limiters).
	dropLogLimiter *rate.Limiter

	// historyReplaySem limits concurrent history replay (XRANGE) operations
	// to prevent exhausting the Redis connection pool during reconnect storms.
	historyReplaySem chan struct{}

	// publishScript and cleanupScript are per-transport Lua script handles.
	// redis.Script caches the script SHA; go-redis' Run prefers EVALSHA and
	// falls back to EVAL on NOSCRIPT, so the script is compiled once per
	// Redis server regardless of how many transport instances hold it.
	publishScript *redis.Script
	cleanupScript *redis.Script

	closed     chan struct{}
	closedOnce sync.Once
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	mu         sync.RWMutex

	// listenerDone is closed by xreadgroupListener on exit. Close waits on
	// it before closing shard channels so no send can land on a closed chan.
	listenerDone chan struct{}

	// healthy is flipped true on the first successful PING and back to false
	// after healthThreshold consecutive PING failures. Kept separate from
	// metrics.healthy so Ready() can read it without the gauge's lock.
	healthy atomic.Bool

	// lastListenerActivityNanos is the UnixNano timestamp of the most recent
	// observed XREADGROUP cycle (returned batch OR clean BLOCK timeout).
	// Read by Ready() so readiness gates on listener-rotation, not just PING.
	// Seeded at startup so the first healthInterval does not false-trip.
	lastListenerActivityNanos atomic.Int64

	// subscriberCount is the count of subscribers on the shard lists while the
	// transport is running, maintained at the addSubscriberToList /
	// removeSubscriberFromList choke-points (both under t.mu, so add /
	// RemoveSubscriber / history-replay rollback all balance automatically; the
	// removal is membership-gated so it cannot drift negative). Lets presence
	// summary mode read the count in O(1) instead of walking every shard each
	// heartbeat; the admission ceiling (subscriber_max_count) will reuse it.
	// Not drained on Close — disconnectAllSubscribers tears down subscribers
	// without per-entry removal, so the count is only authoritative pre-Close.
	subscriberCount atomic.Int64

	// lostFlags tracks "already counted in subscribers_lost" per subscriber
	// to enforce at-most-one-increment across racing paths (Close's
	// disconnectAllSubscribers vs AddSubscriber's history-replay rollback).
	// Keyed by *mercure.LocalSubscriber, value is *atomic.Bool. Stored on
	// AddSubscriber, deleted on RemoveSubscriber happy-path; orphan entries
	// at Close are GC'd with the transport.
	lostFlags sync.Map

	// preBindingDrops counts metric-emission attempts that landed before
	// RegisterMetricsWith bound *Metrics. Surfaced as
	// mercure_redis_pre_binding_metric_drops_total once metrics are bound;
	// the read-through collector reads this atomic at every scrape so the
	// value reflects accumulation across the binding event. A consistently
	// non-zero value at startup means the late-binding window is covering
	// live traffic — the operator should bind metrics earlier (direct API:
	// WithPrometheusRegisterer; Caddy: metrics_registry on the parent).
	//
	// Incremented in two places: (a) metricsOrCountDrop adds 1 on every nil load,
	// covering single-emission sites; (b) presence.go's MGET-failure path
	// adds len(keys) directly, preserving per-key emission semantics for the
	// loop that would otherwise be undercounted. Either path can fire
	// multiple times per Dispatch (one Dispatch may attempt several
	// emissions), so the count is an upper bound on actual missed increments
	// rather than a one-to-one tally — the alarm signal is binary "any > 0".
	preBindingDrops atomic.Uint64
}

// Compile-time interface guards.
var (
	_ mercure.Transport                   = (*RedisTransport)(nil)
	_ mercure.TransportSubscribers        = (*RedisTransport)(nil)
	_ mercure.TransportTopicSelectorStore = (*RedisTransport)(nil)
	_ mercure.TransportCodec              = (*RedisTransport)(nil)
	_ mercure.TransportHealthChecker      = (*RedisTransport)(nil)
)

// ErrTransportUnhealthy is returned by Ready after healthThreshold
// consecutive PING failures. Transient: the next successful PING clears it.
var ErrTransportUnhealthy = errors.New("redis transport: health check reports unhealthy")

// closeClientOnInitError dedupes the cancel + client.Close + errors.Join
// pattern across NewRedisTransport's early-return paths. The stage
// argument (e.g., "version-check", "startup") names the originating
// failure so a chained Close-also-failed error stays attributable.
func closeClientOnInitError(client redis.UniversalClient, cancel context.CancelFunc, origErr error, stage string) error {
	cancel()

	if closeErr := client.Close(); closeErr != nil {
		return errors.Join(origErr, fmt.Errorf("redis transport: failed to close client after %s error: %w", stage, closeErr))
	}

	return origErr
}

// NewRedisTransport creates a new RedisTransport connected to the given Redis/Valkey client.
// The transport launches background goroutines for XREADGROUP, presence heartbeat,
// health check, and optionally TTL cleanup.
func NewRedisTransport(client redis.UniversalClient, opts ...Option) (*RedisTransport, error) {
	o := defaultOptions()
	for _, opt := range opts {
		opt(o)
	}

	if err := validateOptions(o); err != nil {
		return nil, err
	}

	warnSuspiciousOptions(o, o.logger)
	warnSuspiciousClientOptions(o, client, o.logger)

	codec, err := newCodec(o.encoding)
	if err != nil {
		return nil, err
	}

	nodeID := uuid.Must(uuid.NewV4()).String()
	ctx, cancel := context.WithCancel(context.Background())

	t := &RedisTransport{
		client:           client,
		logger:           o.logger,
		opts:             o,
		nodeID:           nodeID,
		nodeGroup:        nodeGroupName(nodeID),
		historyReplaySem: make(chan struct{}, o.historyReplayConcurrency),
		publishScript:    redis.NewScript(publishScriptText),
		cleanupScript:    redis.NewScript(cleanupScriptText),
		closed:           make(chan struct{}),
		cancel:           cancel,
		listenerDone:     make(chan struct{}),
		// Default to "unknown"; checkVersionFromInfo overrides on success.
		backendType: serverTypeUnknown,
	}

	// Seed the atomic cursor before any goroutine can read it (startup + the
	// listener run later). "0-0" means "no resume point / replay from earliest".
	earliest := streamIDEarliest
	t.lastDispatchedStreamID.Store(&earliest)

	// Seed the atomic codec before the listener starts (SetCodec may override it
	// later from the hub).
	t.codec.Store(&codec)

	t.initRateLimiter()
	// initShards must run before initMetrics: RegisterMetricsWith reads
	// t.numShards to size Metrics.shardDispatchObs / shardSubGauges. With
	// the late-binding atomic.Pointer model the constructor-path metric
	// build happens inside initMetrics → RegisterMetricsWith, and a 0-shard
	// slice would panic the first dispatchToSubscribers call.
	t.initShards()

	// validateVersion runs BEFORE initMetrics so t.backendType is latched
	// in time to stamp the backend_type const label on every collector.
	// Prometheus collector descriptors copy ConstLabels at construction
	// time (inside `NewHistogram` / `NewCounter` / `NewGauge` → `NewDesc`),
	// so the value must be set before `newMetrics` runs the `build*`
	// helpers — registration is too late.
	if err := t.validateVersion(ctx); err != nil {
		return nil, closeClientOnInitError(client, cancel, err, "version-check")
	}

	t.initMetrics()

	if err := t.startup(ctx); err != nil {
		return nil, closeClientOnInitError(client, cancel, err, "startup")
	}

	if t.numShards > 1 {
		t.startShardWorkers(ctx)
	}

	t.launchBackgroundGoroutines(ctx)

	return t, nil
}

// Dispatch implements mercure.Transport. Publishes an update to the Redis stream.
// All delivery (including to this node's local subscribers) is handled by the
// XREADGROUP goroutine — there is no local fast-path.
func (t *RedisTransport) Dispatch(ctx context.Context, update *mercure.Update) error {
	// Tag the active parent span (mercure.publish from the hub HTTP handler,
	// mercure.subscribe for subscription-side dispatches, or whatever caller
	// wraps Hub.Publish programmatically) BEFORE the closed-channel check so
	// even ErrClosedTransport traces carry the backend identification — that
	// answers "which transport failed it?" in failed-publish traces.
	tagTransportOnParent(ctx)

	select {
	case <-t.closed:
		return ErrClosedTransport
	default:
	}

	if err := t.waitForPublishToken(ctx); err != nil {
		return err
	}

	update.AssignUUID()

	data, err := (*t.codec.Load()).Marshal(update)
	if err != nil {
		t.metricsOrCountDrop().recordPublishError(publishErrorEncode)

		return fmt.Errorf("redis transport: encode failed: %w", err)
	}

	if m := t.metricsOrCountDrop(); m != nil {
		m.publishPayloadBytes.Observe(float64(len(data)))
	}

	publishStart := time.Now()

	if _, err := t.publishScript.Run(
		ctx, t.client,
		[]string{t.key(":lastEventID"), t.key("")},
		update.ID, t.opts.maxLength, string(data), t.nodeID,
	).Text(); err != nil {
		// If the transport closed mid-publish (Close cancels the root ctx
		// and then closes the client), surface the canonical sentinel
		// rather than the raw context/redis error so callers can rely on
		// errors.Is(err, ErrClosedTransport) regardless of timing.
		select {
		case <-t.closed:
			return ErrClosedTransport
		default:
		}

		t.metricsOrCountDrop().recordPublishError(publishErrorPublish)

		return fmt.Errorf("redis transport: publish failed: %w", err)
	}

	if m := t.metricsOrCountDrop(); m != nil {
		m.publishDuration.Observe(time.Since(publishStart).Seconds())
	}

	return nil
}

// AddSubscriber implements mercure.Transport. Adds a subscriber to the local list,
// replays history if requested, then transitions to live mode.
//
// When rate limiting is enabled (subscriberRateLimit > 0), AddSubscriber blocks
// until a token is available. This prevents thundering herd scenarios where
// thousands of clients reconnect simultaneously after a network blip.
func (t *RedisTransport) AddSubscriber(ctx context.Context, s *mercure.LocalSubscriber) error {
	addStart := time.Now()

	defer func() {
		if m := t.metricsOrCountDrop(); m != nil {
			m.addSubscriberDuration.Observe(time.Since(addStart).Seconds())
		}
	}()

	// Tag the active parent span (typically the hub's mercure.subscribe;
	// programmatic Hub.AddSubscriber callers may set a different parent)
	// before the closed-channel check — same rationale as Dispatch
	// (transport identity on the trace survives ErrClosedTransport returns).
	// The child history span carries its own mercure.transport attr; this
	// tag puts the same identification on the parent so trace queries that
	// scope by transport work.
	tagTransportOnParent(ctx)

	select {
	case <-t.closed:
		return ErrClosedTransport
	default:
	}

	if err := t.waitForRateLimitToken(ctx); err != nil {
		return err
	}

	// Re-check closed under t.mu to serialize against Close's
	// disconnectAllSubscribers. The top-of-function select is TOCTOU because
	// AddSubscriber runs outside t.wg.
	t.mu.Lock()
	select {
	case <-t.closed:
		t.mu.Unlock()

		return ErrClosedTransport
	default:
	}

	if m := t.metricsOrCountDrop(); m != nil {
		m.subscriberAddTotal.Inc()
	}

	t.addSubscriberToList(s)

	// Track this subscriber for at-most-one subscribers_lost increment.
	// Concurrent Close (disconnectAllSubscribers) and the rollback below
	// both call markLost on the same flag; only the CAS winner increments.
	flag := new(atomic.Bool)
	t.lostFlags.Store(s, flag)

	// Load the cursor AFTER addSubscriberToList (above): the happens-before edge
	// from joining the shard index before reading the cursor is what makes the
	// gap-free guarantee hold without holding mu across dispatch. If the live
	// fan-out for an event missed this subscriber (joined after that shard's
	// candidate snapshot), the cursor read here is guaranteed to already cover
	// it, so it replays via history. See processStreamEntry. The edge is supplied
	// by the topic index's RWMutex (add takes Lock, the dispatch snapshot takes
	// RLock); if that ever became lock-free without barriers, this would need an
	// explicit fence.
	toStreamID := *t.lastDispatchedStreamID.Load()
	t.mu.Unlock()

	// Releasing t.mu before dispatchHistory is intentional: the lock protects
	// shard-list mutation and serializes the add against Close's
	// disconnectAllSubscribers (the cursor is atomic, already captured above).
	// dispatchHistory mutates neither. Holding it across XRANGE pagination
	// (potentially seconds on a deep replay) would serialize every other
	// AddSubscriber + RemoveSubscriber across the hub. The correctness
	// contract — replayed events MUST land on the SSE wire before any live
	// event from this point onward — is enforced upstream by
	// mercure.LocalSubscriber, which buffers live Dispatch calls internally
	// and defers them to the wire until the subscriber's Ready() is
	// invoked at the end of replay. So the subscriber is on the shard list
	// (live events queueing) while history replay runs, and ordering stays
	// correct without our lock.
	if s.RequestLastEventID != "" {
		replayCtx, cancel := t.registrationContext(ctx)
		defer cancel()

		if err := t.dispatchHistory(replayCtx, s, toStreamID); err != nil {
			// Remove from shard list and balance the add we already counted.
			// markLost serializes against disconnectAllSubscribers via the
			// per-subscriber CAS, so this subscriber is counted exactly once
			// across the rollback and shutdown paths even if Close fires
			// while we're here.
			t.mu.Lock()
			t.removeSubscriberFromList(s)
			t.mu.Unlock()

			t.markLost(s, lossReasonHistoryReplayFailed)
			t.lostFlags.Delete(s)

			return err
		}
	} else {
		s.Ready(ctx)
	}

	return nil
}

// RemoveSubscriber implements mercure.Transport.
func (t *RedisTransport) RemoveSubscriber(_ context.Context, s *mercure.LocalSubscriber) error {
	removeStart := time.Now()

	defer func() {
		if m := t.metricsOrCountDrop(); m != nil {
			m.removeSubscriberDuration.Observe(time.Since(removeStart).Seconds())
		}
	}()

	select {
	case <-t.closed:
		return ErrClosedTransport
	default:
	}

	// Free the lostFlags entry under the same t.mu as the list removal —
	// normal removal is not a "lost" event, so no markLost call. Holding the
	// lock across both makes removeSubscriberFromList's membership gate and the
	// Delete atomic: two concurrent RemoveSubscriber(s) for the same subscriber
	// can't both observe the flag present and double-decrement subscriberCount.
	// Delete-after-removal order also keeps a concurrent disconnectAllSubscribers
	// Walk from seeing a stale flag (it removes s from the list first).
	t.mu.Lock()
	t.removeSubscriberFromList(s)
	t.lostFlags.Delete(s)
	t.mu.Unlock()

	if m := t.metricsOrCountDrop(); m != nil {
		m.subscriberRemoveTotal.Inc()
	}

	return nil
}

// closeTimeout is the hard deadline for Redis operations during Close.
// Applied uniformly regardless of caller-supplied ctx — see Close() for why.
const closeTimeout = 5 * time.Second

// Close implements mercure.Transport. Performs a graceful shutdown.
//
// The caller's ctx is intentionally IGNORED. Caddy's TransportDestructor
// invokes Close with caddy.ActiveContext(), which returns a zero-value
// caddy.Context during `caddy validate` (no config active). That zero value
// embeds a nil context.Context and panics with a nil pointer dereference
// when go-redis calls ctx.Done() internally. Bolt/LocalTransport's Close
// also ignore their ctx for parity reasons; we do the same and instead use
// a self-managed background ctx with a closeTimeout budget so Close can't
// wedge indefinitely on a stuck Redis.
//
// wg.Wait must precede disconnectAllSubscribers so no in-flight dispatch call
// races a subscriber being torn down. Shutdown-time cleanup errors (destroy
// consumer group, delete presence key, client close) are each logged AND
// accumulated into the joined return value so orchestrators see the full
// picture rather than just the last failure.
func (t *RedisTransport) Close(_ context.Context) (closeErr error) {
	// Background-derived ctx — see the function comment for why we ignore the
	// caller's ctx. Hoisted outside closedOnce.Do so cancel() is deferred at
	// the outer scope, not repeatedly constructed on re-entry.
	ctx, cancel := context.WithTimeout(context.Background(), closeTimeout)
	defer cancel()

	t.closedOnce.Do(func() {
		close(t.closed)

		t.cancel()

		// Wait for xreadgroupListener to exit BEFORE closing shard channels —
		// it's the only sender, so this guarantees no send-on-closed panic and
		// no race where a worker exits its drain while a listener send still
		// sits in the channel buffer (would deadlock shardedDispatch's
		// per-message wg.Wait).
		<-t.listenerDone

		// Now safe to close shard channels; workers exit their `for range ch`
		// loop after draining any already-queued work.
		t.closeShardChannels()

		t.wg.Wait()

		t.mu.Lock()
		t.disconnectAllSubscribers()
		t.mu.Unlock()

		// Each cleanup step logs on failure AND appends to errs so the joined
		// return value carries every non-nil cause; orchestrators that key
		// health off Close()'s return get the full picture, not just the last.
		var errs []error

		streamKey := t.key("")
		if err := t.client.XGroupDestroy(ctx, streamKey, t.nodeGroup).Err(); err != nil {
			t.logger.Warn("redis transport: failed to destroy consumer group on shutdown", "error", err)

			errs = append(errs, fmt.Errorf("redis transport: destroy consumer group: %w", err))
		}

		if err := t.client.Del(ctx, t.presenceKey(t.nodeID)).Err(); err != nil {
			t.logger.Warn("redis transport: failed to delete presence key on shutdown", "error", err)

			errs = append(errs, fmt.Errorf("redis transport: delete presence key: %w", err))
		}

		if err := t.client.Close(); err != nil {
			errs = append(errs, fmt.Errorf("redis transport: close client: %w", err))
		}

		closeErr = errors.Join(errs...)
	})

	return closeErr
}

// SetTopicSelectorStore implements mercure.TransportTopicSelectorStore.
// Called by the hub during its init to pass the TopicSelectorStore, which
// the transport injects into cross-node subscribers during GetSubscribers.
func (t *RedisTransport) SetTopicSelectorStore(store *mercure.TopicSelectorStore) {
	t.topicSelectorStore.Store(store)
}

// SetCodec implements mercure.TransportCodec.
// Called by the hub to override the codec configured via WithEncoding.
func (t *RedisTransport) SetCodec(codec mercure.Codec) {
	t.codec.Store(&codec)
}

// Ready implements mercure.TransportHealthChecker. It returns nil when the
// transport can accept traffic: not closed and the health-check loop has not
// marked it unhealthy. Used for readiness probes — on a Redis outage, Ready
// starts returning ErrTransportUnhealthy so the load balancer can drain
// traffic. Ready tolerates the brief startup window before the first PING
// lands because t.healthy is flipped to true atomically inside startup().
func (t *RedisTransport) Ready(ctx context.Context) error {
	select {
	case <-t.closed:
		return ErrClosedTransport
	case <-ctx.Done():
		return fmt.Errorf("redis transport: ready check cancelled: %w", ctx.Err())
	default:
	}

	if !t.healthy.Load() {
		return ErrTransportUnhealthy
	}

	// Listener-health gate: PING ok ≠ XREADGROUP rotating. Threshold is
	// max(5×xreadBlock, listenerStaleFloor) so transient pauses don't
	// flap readiness; XREADGROUP stamps on every cycle (including empty
	// BLOCK returns) so zero-traffic hubs still stamp every xreadBlock.
	threshold := max(5*t.opts.xreadBlock, listenerStaleFloor)

	last := time.Unix(0, t.lastListenerActivityNanos.Load())
	if since := time.Since(last); since > threshold {
		return fmt.Errorf("%w: listener idle %s (threshold %s)",
			ErrTransportUnhealthy, since.Round(time.Second), threshold)
	}

	return nil
}

// Live implements mercure.TransportHealthChecker. It returns nil unless the
// transport has been closed. A closed transport is terminal — a failing Live
// probe signals the process should be restarted, as the transport will not
// recover. Ready can flap with Redis outages; Live only fails on shutdown.
func (t *RedisTransport) Live(ctx context.Context) error {
	select {
	case <-t.closed:
		return ErrClosedTransport
	case <-ctx.Done():
		return fmt.Errorf("redis transport: live check cancelled: %w", ctx.Err())
	default:
		return nil
	}
}

// RegisterMetricsWith binds reg to this transport, constructing and registering
// every Prometheus collector and the go-redis pool-stats collector against it.
// The pool collector is read-through (values fetched at scrape time via
// client.PoolStats()), so no goroutine or state is added to the hot path. On
// an AlreadyRegisteredError (Caddy reload against a persistent registry), the
// pool collector swaps in this transport's client so scrapes don't keep
// returning stats from a closed transport's pool indefinitely.
//
// Implements caddy.TransportMetricsRegisterer (defined in the fork's caddy
// package) so the mercure caddy module can pass its parent registry — Caddy
// gives sub-modules a derived ctx whose GetMetricsRegistry() returns a
// typed-nil *prometheus.Registry, and that's what skips registration on the
// constructor path.
//
// Idempotent: only the first call with a non-nil registerer wins. Subsequent
// calls (e.g. constructor path then Caddy path) are no-ops. Returns nil on
// no-op (typed-nil/true-nil reg, or already-registered).
//
// Direct-API callers should invoke RegisterMetricsWith BEFORE the first
// AddSubscriber/Dispatch — adding subscribers pre-binding and removing them
// post-binding would drop shard_subscribers to a negative value because the
// Add path saw t.metrics.Load() == nil and skipped the Inc(). Not reachable
// via the Caddy submodule, which completes RegisterMetricsWith inside
// Provision before HTTP routes start serving.
func (t *RedisTransport) RegisterMetricsWith(reg prometheus.Registerer) error {
	if isEffectivelyNilRegisterer(reg) {
		// Operator breadcrumb: a typed-nil *prometheus.Registry is the same
		// path Caddy walks on `caddy validate` and for sub-modules whose ctx
		// did not inherit the metrics registry — both legitimate. But the
		// same code-path also fires when a misconfigured operator passes a
		// nil registry expecting metrics. Logging Debug (not Warn) keeps the
		// validate-time noise out of normal logs while leaving a trail for
		// "why are my metrics empty?" investigations.
		t.logger.Debug("redis transport: RegisterMetricsWith skipped — registerer is nil or typed-nil")

		return nil
	}

	var (
		registerErr error
		bound       bool
	)

	t.metricsOnce.Do(func() {
		bound = true

		// First call BEFORE newMetrics so the multi-stream Warn surfaces
		// even if downstream registration errs.
		warnIfRegistererAlreadyBoundToDifferentStream(reg, t.opts.streamName, t.logger)

		m := newMetrics(reg, t.preBindingDropsValueSource(), t.backendType)

		if err := t.registerOrAdoptPoolCollector(reg); err != nil {
			registerErr = err

			return
		}

		// Resolve per-shard child observers AFTER newMetrics has registered
		// dispatchDuration / shardSubscribers, so the slices reference the
		// live (post-AlreadyRegistered swap) collectors.
		m.resolveShardChildren(t.numShards)

		t.metrics.Store(m)
		t.boundRegisterer.Store(&reg)
	})

	if !bound {
		// metricsOnce already fired — second call. Drop the second registerer
		// silently if it's the same instance (the Caddy TransportUsagePool
		// reload pattern). Warn on a different instance so the operator sees
		// the silent-drift trail and either reuses the original registerer or
		// re-creates the transport. On non-comparable Registerer dynamic
		// types we cannot tell — leave a Debug breadcrumb so a "why no warn?"
		// investigation finds the trail without spamming default logs.
		if first := t.boundRegisterer.Load(); first != nil {
			differ, identityKnown := registerersDiffer(*first, reg)

			switch {
			case !identityKnown:
				t.logger.Debug("redis transport: could not determine registerer identity (non-comparable dynamic type); drift detection skipped",
					"type", fmt.Sprintf("%T", reg))
			case differ:
				t.logger.Warn("redis transport: RegisterMetricsWith called with a different registerer; metrics remain bound to the first registerer; second call ignored")
			}
		}
	}

	return registerErr
}

// TryAdmit implements mercure.Admitter: the front-door admission gate the hub
// handler calls before auth/allocation. Token-before-slot (admission design
// §3.3): reject immediately when clearly at the count ceiling (no token
// consumed), acquire a rate token (Allow fail-fast, or a bounded wait when
// subscriber_admission_timeout > 0), then CAS-acquire the count slot.
//
// On success it returns a context carrying the admitted marker (so AddSubscriber
// skips its legacy rate wait) and an idempotent release closure that frees the
// slot; the caller MUST defer release exactly once. On rejection it returns the
// original context, a nil release, and an *mercure.AdmissionError (429). The
// per-key admission helpers (admitRateToken/acquireAdmissionSlot/…) and the
// admittedContextKey marker live near waitForRateLimitToken below.
func (t *RedisTransport) TryAdmit(ctx context.Context) (context.Context, func(), error) {
	select {
	case <-t.closed:
		// Closed transport: not an admission decision — let the 503 path handle it.
		return ctx, nil, ErrClosedTransport
	default:
	}

	maxCount := t.opts.subscriberMaxCount

	// (1) Fast capacity check — reject without consuming a rate token.
	if maxCount > 0 && t.admissionLeasesHeld.Load() >= int64(maxCount) {
		t.metricsOrCountDrop().recordAdmissionRejected(admissionReasonCapacity)

		return ctx, nil, &mercure.AdmissionError{Reason: mercure.AdmissionCapacity, RetryAfter: t.admissionRetryAfter()}
	}

	// (2) Rate token: fail-fast, or a bounded wait on the caller's context.
	if !t.admitRateToken(ctx) {
		t.metricsOrCountDrop().recordAdmissionRejected(admissionReasonRate)

		return ctx, nil, &mercure.AdmissionError{Reason: mercure.AdmissionRate, RetryAfter: t.admissionRetryAfter()}
	}

	// (3) CAS-acquire the count slot, re-checking the ceiling atomically. A rate
	// token consumed at (2) is NOT returned if (3) then capacity-rejects — a rare
	// race under simultaneous rate+capacity pressure; the token replenishes, so
	// this is an accepted fast-path trade-off (token-before-slot avoids holding a
	// slot while waiting for a token, which would let waiters exhaust capacity).
	if !t.acquireAdmissionSlot(maxCount) {
		t.metricsOrCountDrop().recordAdmissionRejected(admissionReasonCapacity)

		return ctx, nil, &mercure.AdmissionError{Reason: mercure.AdmissionCapacity, RetryAfter: t.admissionRetryAfter()}
	}

	// Roll the slot back if a panic unwinds before we hand the closure out.
	committed := false

	defer func() {
		if !committed {
			t.admissionLeasesHeld.Add(-1)
		}
	}()

	// sync.OnceFunc → release is idempotent. The handler's single defer is the
	// only caller; the guard is cheap insurance against a future second site.
	release := sync.OnceFunc(func() { t.admissionLeasesHeld.Add(-1) })
	admitted := markAdmitted(ctx)
	committed = true

	return admitted, release, nil
}

// registerOrAdoptPoolCollector registers a new poolStatsCollector against
// reg, or — on AlreadyRegisteredError (same-identity Caddy reload) —
// adopts the existing collector and rebinds its client to t.client.
//
// On adoption with a different backendType, logs Warn. This is a
// programmer-error / same-identity-different-field guard, NOT the
// operator-facing cross-backend drift detector. Prometheus collector
// identity INCLUDES ConstLabel VALUES, so a Caddy reload that swaps
// backends (Redis URL → Valkey URL) produces collectors with a different
// identity — AlreadyRegisteredError doesn't fire, the original collectors
// stay on the registry frozen, and a parallel new collector set is
// registered. See redistransport/README.md § Metrics for the cross-
// backend ghost-series caveat operators need to know about.
//
// Returns nil on success or adoption, or a wrapped error when the
// existing collector is the wrong type (programmer or integration error:
// a foreign collector squatting on the FQ-name).
func (t *RedisTransport) registerOrAdoptPoolCollector(reg prometheus.Registerer) error {
	poolCollector := newPoolStatsCollector(t.client, t.backendType)

	err := reg.Register(poolCollector)
	if err == nil {
		return nil
	}

	var are prometheus.AlreadyRegisteredError
	if !errors.As(err, &are) {
		return fmt.Errorf("redis transport: failed to register pool stats collector: %w", err)
	}

	// Type-mismatch on AlreadyRegistered is a programmer/integration
	// error — a different collector type is registered under the same
	// name. Refuse to continue: silently scraping someone else's
	// collector forever would leave the transport reporting healthy
	// metrics binding while the pool stats are detached.
	existing, ok := are.ExistingCollector.(*poolStatsCollector)
	if !ok {
		return fmt.Errorf("%w: got %T", errPoolCollectorAdoptFailed, are.ExistingCollector)
	}

	if existing.backendType != t.backendType {
		t.logger.Warn("redis transport: pool stats collector adopted with stale backend_type label; restart hub to re-stamp labels on the new backend",
			"adopted_backend_type", existing.backendType,
			"detected_backend_type", t.backendType)
	}

	existing.setClient(t.client)

	return nil
}

// registererStreamBindings tracks the first stream name a Prometheus
// Registerer was used with, package-wide. Const labels on transport
// metrics are {transport_type, backend_type} only — stream name is NOT
// in the label set, so two RedisTransports sharing one Registerer with
// different stream names silently collapse into adopted collectors
// (safeRegister's AlreadyRegisteredError fallback), summing counters
// from two streams into a single dashboard series.
//
// warnIfRegistererAlreadyBoundToDifferentStream logs a Warn the first
// time a second stream name binds to the same registerer. Non-comparable
// Registerer dynamic types are silently skipped — same degraded-detection
// precedent as registerersDiffer.
//
// collision detection on a shared Registerer requires a shared lookup.
//
//nolint:gochecknoglobals // Process-wide by design: cross-transport
var (
	registererStreamBindings   = make(map[any]string)
	registererStreamBindingsMu sync.Mutex
)

// warnIfRegistererAlreadyBoundToDifferentStream records the (registerer,
// streamName) binding on first call and logs a loud Warn on subsequent
// calls for the same registerer that observe a different stream name.
// Same-stream re-binding (Caddy reload pattern) is silent. Non-comparable
// registerers are skipped to avoid a panic.
func warnIfRegistererAlreadyBoundToDifferentStream(reg prometheus.Registerer, streamName string, logger *slog.Logger) {
	regType := reflect.TypeOf(reg)
	if regType == nil || !regType.Comparable() {
		// Non-comparable registerer — cannot key the map without risking
		// a panic. Same degraded-detection trade-off as registerersDiffer.
		return
	}

	registererStreamBindingsMu.Lock()
	defer registererStreamBindingsMu.Unlock()

	if previous, ok := registererStreamBindings[reg]; ok {
		if previous != streamName {
			logger.Warn(
				"redis transport: registerer already bound to a different stream name — "+
					"transport metrics will silently merge across streams "+
					"(const labels are {transport_type, backend_type} only). "+
					"Use one Registerer per stream, or run one redistransport per Caddy process.",
				"first_stream", previous,
				"second_stream", streamName,
			)
		}

		return
	}

	registererStreamBindings[reg] = streamName
}

// registerersDiffer reports whether two prometheus.Registerer values
// represent different instances. The second return reports whether
// identity could be determined: false means the values share a
// non-comparable dynamic type (struct with slice/map/func — rare but
// spec-allowed for Registerer implementers), in which case a direct
// `a != b` interface comparison would panic. Common registerers are
// pointer-receivers (*prometheus.Registry, the Caddy-supplied wrapper) —
// comparable, fast path returns identityKnown=true. Callers use the
// identityKnown=false signal to log a degraded-detection breadcrumb
// instead of crashing a late-binding call.
func registerersDiffer(a, b prometheus.Registerer) (differ bool, identityKnown bool) {
	aType := reflect.TypeOf(a)

	bType := reflect.TypeOf(b)
	if aType != bType {
		// Different dynamic types: definitely a different registerer.
		return true, true
	}

	if !aType.Comparable() {
		// Same non-comparable type: cannot determine identity without
		// risking a panic. Caller surfaces the degraded-detection signal.
		return false, false
	}

	return a != b, true
}

// markLost increments subscribers_lost{reason=r} for s. The lostFlags map
// (populated in AddSubscriber) is the per-subscriber dedup token: when present,
// only the CAS winner among the racing loss paths (backpressure during live
// dispatch, AddSubscriber's history-replay rollback, and Close's
// disconnectAllSubscribers Walk) counts s.
//
// When the flag is ABSENT we still increment. Pre-lock-removal this branch was
// unreachable — dispatch held t.mu across the match→markLost, so a concurrent
// RemoveSubscriber could not delete the flag in between. Now dispatch holds no
// mu (the cursor is atomic, see processStreamEntry), so a shard worker can reach
// markLost on a stale snapshot AFTER RemoveSubscriber deleted the flag. That
// path carries a genuine backpressure loss (Dispatch returned false), so a
// no-op here would silently UNDER-count subscribers_lost exactly under the load
// this change targets. Counting unconditionally preserves the pre-removal
// behavior (a Dispatch=false during fan-out is attributed once). The only cost
// is a rare double-count: if a history-replay rollback (which deletes the flag
// after its own markLost) races a stale-snapshot backpressure markLost for the
// same subscriber, it is counted twice — once as history_replay_failed, once as
// backpressure. This is hub-reachable (a live event can backpressure a
// subscriber that is still replaying), but the window is narrow (replay-failure
// AND concurrent backpressure on the same sub) and the direction is a benign
// over-count, not the under-count the no-op caused.
func (t *RedisTransport) markLost(s *mercure.LocalSubscriber, reason subscriberLossReason) {
	counter := t.metricsOrCountDrop().lostCounter(reason)
	if counter == nil {
		return
	}

	if v, ok := t.lostFlags.Load(s); ok {
		if v.(*atomic.Bool).CompareAndSwap(false, true) {
			counter.Inc()
		}

		return
	}

	counter.Inc()
}

// waitForRateLimitToken blocks until the subscriber rate limiter grants a token
// or ctx is cancelled. No-op when rate limiting is disabled (limiter is nil).
func (t *RedisTransport) waitForRateLimitToken(ctx context.Context) error {
	// An HTTP subscriber that already passed TryAdmit's rate gate carries the
	// admitted marker; re-charging here would double-count it against the
	// limiter. Direct (non-admitted) AddSubscriber callers have no marker and
	// keep the legacy enforcement.
	if isAdmitted(ctx) {
		return nil
	}

	return waitForLimiter(ctx, t.addSubLimiter, t.metricsCounter(metricSubscriberRateLimited),
		"redis transport: subscriber rate limited")
}

// admittedContextKey marks a context whose subscriber already passed TryAdmit's
// rate gate, so AddSubscriber skips its legacy waitForLimiter (avoiding a
// double-charge on the HTTP path). Private to the transport: only TryAdmit can
// set it, so direct callers can't spoof it.
type admittedContextKey struct{}

func markAdmitted(ctx context.Context) context.Context {
	return context.WithValue(ctx, admittedContextKey{}, struct{}{})
}

func isAdmitted(ctx context.Context) bool {
	return ctx.Value(admittedContextKey{}) != nil
}

// Admission rejection reasons — the only label on the admission_rejected metric.
// The strings match mercure.AdmissionReason.String() for cross-referencing.
const (
	admissionReasonRate     = "rate"
	admissionReasonCapacity = "capacity"
)

// registrationContext bounds post-admission registration (history replay) under
// subscriber_registration_timeout so a slow/stuck backend can't pin an admission
// slot indefinitely. 0 (default) leaves it unbounded — the pre-admission-control
// behavior. The returned cancel is always safe to defer.
func (t *RedisTransport) registrationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if t.opts.subscriberRegTimeout <= 0 {
		return ctx, func() {}
	}

	return context.WithTimeout(ctx, t.opts.subscriberRegTimeout)
}

// admitRateToken acquires one subscriber rate token. With no limiter it always
// admits. Otherwise Allow() is fail-fast; when subscriber_admission_timeout > 0
// it falls back to a bounded Wait on the caller's context (so a client
// disconnecting mid-wait abandons it immediately).
func (t *RedisTransport) admitRateToken(ctx context.Context) bool {
	if t.addSubLimiter == nil || t.addSubLimiter.Allow() {
		return true
	}

	if t.opts.subscriberAdmissionTimeout <= 0 {
		return false
	}

	waitCtx, cancel := context.WithTimeout(ctx, t.opts.subscriberAdmissionTimeout)
	defer cancel()

	return t.addSubLimiter.Wait(waitCtx) == nil
}

// acquireAdmissionSlot increments admissionLeasesHeld, enforcing maxCount via a
// CAS loop so a concurrent burst can't over-admit. maxCount <= 0 means no
// ceiling — the slot is still counted (for observability) but never refused.
func (t *RedisTransport) acquireAdmissionSlot(maxCount int) bool {
	if maxCount <= 0 {
		t.admissionLeasesHeld.Add(1)

		return true
	}

	for {
		cur := t.admissionLeasesHeld.Load()
		if cur >= int64(maxCount) {
			return false
		}

		if t.admissionLeasesHeld.CompareAndSwap(cur, cur+1) {
			return true
		}
	}
}

// admissionRetryAfter is the base subscriber_retry_after jittered within
// [base, 1.5×base] so a static value can't re-synchronize a herd. The handler
// rounds it up to integer seconds for the Retry-After header.
func (t *RedisTransport) admissionRetryAfter() time.Duration {
	base := t.opts.subscriberRetryAfter
	if base <= 0 {
		return 0
	}

	// Weak RNG is correct here: this jitter de-synchronizes a reconnect herd,
	// it is not security-sensitive (no secrecy/unpredictability requirement).
	return base + time.Duration(rand.Int64N(int64(base)/2+1)) //nolint:gosec // non-crypto jitter
}

// waitForPublishToken blocks until the publish rate limiter grants a token
// or ctx is cancelled. No-op when rate limiting is disabled (limiter is nil).
func (t *RedisTransport) waitForPublishToken(ctx context.Context) error {
	return waitForLimiter(ctx, t.publishLimiter, t.metricsCounter(metricPublisherRateLimited),
		"redis transport: publish rate limited")
}

// waitForLimiter is the shared token-bucket wait used by AddSubscriber and
// Dispatch. limiter may be nil (no-op). counter is a nil-safe optional
// counter incremented after a real delay (post-Wait); cancelled or
// deadline-exceeded calls are not counted as delayed since no wait
// actually occurred. errPrefix wraps ctx.Err().
//
// Uses limiter.Wait(ctx) (rather than Reserve+timer) so a doomed
// reservation under a context with no remaining budget rejects without
// consuming a token, avoiding queue inflation under reconnect storms.
func waitForLimiter(ctx context.Context, limiter *rate.Limiter, counter prometheus.Counter, errPrefix string) error {
	if limiter == nil {
		return nil
	}

	if limiter.Allow() {
		return nil
	}

	if err := limiter.Wait(ctx); err != nil {
		return fmt.Errorf("%s: %w", errPrefix, err)
	}

	if counter != nil {
		counter.Inc()
	}

	return nil
}

// metricsCounter returns a nil-safe accessor for the named counter; nil
// when metrics are disabled. Keeps the limiter helper above metric-shape
// agnostic so it can be reused for any future rate-limited path.
type rateLimitedMetric int

const (
	metricSubscriberRateLimited rateLimitedMetric = iota
	metricPublisherRateLimited
)

func (t *RedisTransport) metricsCounter(which rateLimitedMetric) prometheus.Counter {
	metrics := t.metricsOrCountDrop()
	if metrics == nil {
		return nil
	}

	switch which {
	case metricSubscriberRateLimited:
		return metrics.subscriberRateLimited
	case metricPublisherRateLimited:
		return metrics.publisherRateLimited
	}

	return nil
}

// metricsOrCountDrop loads the bound *Metrics. When nil (RegisterMetricsWith has
// not yet fired), it bumps preBindingDrops so the late-binding window leaves
// an audit trail instead of silently swallowing emission attempts. Callers
// either dereference the result directly (the record* methods are nil-safe
// no-ops) or guard with `if m := t.metricsOrCountDrop(); m != nil { ... }` for
// blocks that touch raw collector fields.
//
// The counter accumulates beyond the binding event. Once metrics are bound,
// the running total is exposed via the read-through collector wired in
// newMetrics, so a Prometheus scrape of mercure_redis_pre_binding_metric_drops_total
// reports the same value the operator can correlate against startup
// timestamps. Any non-zero value is the alarm signal: the binding window
// covered live traffic.
func (t *RedisTransport) metricsOrCountDrop() *Metrics {
	m := t.metrics.Load()
	if m == nil {
		t.preBindingDrops.Add(1)
	}

	return m
}

// recordPreBindingDrops bumps the late-binding-window drop counter by n,
// without performing the metric load that metricsOrCountDrop does. The single
// caller (presence MGET failure) needs per-key drop accounting that
// metricsOrCountDrop's one-bump-per-call would understate by len(keys)-1; this
// helper gives that caller a way to express the bulk increment without
// reaching into the unexported atomic field directly. Keeping the
// abstraction here means future shape changes to the counter (rate-limited
// bumps, sharded counters, etc.) land in one place.
func (t *RedisTransport) recordPreBindingDrops(n uint64) {
	t.preBindingDrops.Add(n)
}

// preBindingDropsValueSource returns the read-through closure passed to
// newMetrics for the pre-binding-drops collector. Defined as a method on
// the transport so the wiring lives next to the preBindingDrops field —
// previously the closure was inline at the RegisterMetricsWith call site,
// which left a future contributor refactoring *Metrics with no signal that
// the field's surfaced metric depends on a closure built elsewhere.
func (t *RedisTransport) preBindingDropsValueSource() func() float64 {
	return func() float64 { return float64(t.preBindingDrops.Load()) }
}

// initRateLimiter wires up the subscriber-add and publish rate limiters
// when enabled in options. Each limiter is left nil when its rate is 0,
// so the hot path can short-circuit without an atomic load on the limiter
// state.
func (t *RedisTransport) initRateLimiter() {
	if t.opts.subscriberRateLimit > 0 {
		t.addSubLimiter = rate.NewLimiter(
			rate.Limit(t.opts.subscriberRateLimit),
			t.opts.subscriberRateBurst,
		)
	}

	if t.opts.publisherRateLimit > 0 {
		t.publishLimiter = rate.NewLimiter(
			rate.Limit(t.opts.publisherRateLimit),
			t.opts.publisherRateBurst,
		)
	}

	// Burst lets a genuine incident (codec drift, tampering) surface a handful of
	// examples immediately, then the rate settles to ~1/sec.
	t.dropLogLimiter = rate.NewLimiter(rate.Every(dropLogInterval), dropLogBurst)
}

const (
	// dropLogInterval / dropLogBurst bound the consumer-side drop/reject log rate.
	dropLogInterval = time.Second
	dropLogBurst    = 10
)

// allowLog reports whether a rate-limited log line may be emitted now. A nil
// limiter (e.g. in tests) always allows.
func allowLog(l *rate.Limiter) bool {
	return l == nil || l.Allow()
}

// initMetrics binds the constructor-path registerer (set via
// WithPrometheusRegisterer) into the transport via the same RegisterMetricsWith
// entry point used by the Caddy late-binding path. No-op when
// opts.prometheusRegisterer is nil. A registration failure is fatal — see
// RegisterMetricsWith for rationale; mirrors the pre-late-binding behavior
// where this function panicked on collector-register errors.
func (t *RedisTransport) initMetrics() {
	if err := t.RegisterMetricsWith(t.opts.prometheusRegisterer); err != nil {
		panic(err)
	}
}

// launchBackgroundGoroutines starts the always-running goroutines
// (xreadgroupListener, presenceHeartbeat, healthCheck) and any optional
// periodic workers (TTL cleanup, zombie GC).
func (t *RedisTransport) launchBackgroundGoroutines(ctx context.Context) {
	// Seed before listener start so Ready() during the first
	// healthInterval reads a recent stamp instead of the zero-value.
	t.lastListenerActivityNanos.Store(time.Now().UnixNano())

	t.wg.Add(numCoreBackgroundGoroutines)

	go t.xreadgroupListener(ctx)
	go t.presenceHeartbeat(ctx)
	go t.healthCheck(ctx)

	if t.opts.eventTTL > 0 {
		t.wg.Add(1)

		go t.ttlCleanup(ctx)
	}

	if t.opts.zombieGCInterval > 0 {
		t.wg.Add(1)

		go t.periodicZombieGC(ctx)
	}
}

// startup performs the server initialization sequence. The presence key must
// be written BEFORE zombie-group GC: otherwise another node's GC pass could
// see an absent presence key for this node and tear down this node's
// freshly-created consumer group.
func (t *RedisTransport) startup(ctx context.Context) error {
	streamKey := t.key("")

	// Invariant: validateVersion + backend detection have already run in
	// NewRedisTransport — t.backendType is latched and metrics are wired.

	// Mark healthy before launching background goroutines so Ready() returns
	// nil immediately. If the first runHealthPing lands a failure the loop
	// will flip this back to false after healthThreshold consecutive misses.
	t.healthy.Store(true)

	t.detectClockDrift(ctx)

	presenceKey := t.key(":presence:" + t.nodeID)
	if err := t.client.Set(ctx, presenceKey, "[]", t.opts.presenceTTL).Err(); err != nil {
		return fmt.Errorf("redis transport: failed to set initial presence key: %w", err)
	}

	tailEntries, err := t.client.XRevRangeN(ctx, streamKey, "+", "-", 1).Result()
	if err != nil {
		return fmt.Errorf("redis transport: failed to query stream tail: %w", err)
	}

	startID := "0"
	if len(tailEntries) > 0 {
		startID = tailEntries[0].ID
		tail := tailEntries[0].ID
		t.lastDispatchedStreamID.Store(&tail)
	}

	// Consumer-group creation is idempotent — BUSYGROUP is expected on restart
	// and deliberately caught rather than treated as a failure.
	err = t.client.XGroupCreateMkStream(ctx, streamKey, t.nodeGroup, startID).Err()
	if err != nil && !strings.Contains(err.Error(), redisErrBusyGroup) {
		return fmt.Errorf("redis transport: failed to create consumer group: %w", err)
	}

	t.gcZombieGroups(ctx, streamKey)

	if t.opts.maxLength == 0 && t.opts.eventTTL == 0 {
		t.logger.Warn("redis transport: both max_length and event_ttl are 0 — stream will grow without bound. Set max_length or event_ttl to prevent memory exhaustion in production.")
	}

	return nil
}

// serverInfo holds the detected server type and version.
type serverInfo struct {
	serverType serverType // serverTypeRedis, serverTypeValkey, or serverTypeUnknown
	version    string     // e.g., "7.2.4", "8.1.6"
}

// validateVersion checks that the connected server meets minimum version requirements:
//   - Redis:  ≥ 6.2 (required for XTRIM MINID)
//   - Valkey: ≥ 7.2 (forked from Redis 7.2.4; always satisfies 6.2 minimum)
//
// INFO failure is fatal in production — a server that cannot be verified to
// meet the floor risks runtime failures on XTRIM MINID. Tests using in-process
// fakes (miniredis) must opt into the bypass via withSkipVersionCheck.
func (t *RedisTransport) validateVersion(ctx context.Context) error {
	info, err := t.client.Info(ctx, "server").Result()
	if err != nil {
		if t.opts.skipVersionCheck {
			// Warn (not Debug) because the visible consequence is that the
			// backend_type Prometheus label remains "unknown" for the entire
			// transport lifetime — dashboards filtering on backend_type
			// silently miss this hub. Operators tracking a flaky bootstrap
			// need this signal at default log level.
			t.logger.Warn("redis transport: INFO query failed — version check skipped per withSkipVersionCheck; backend_type label will remain 'unknown' until restart", "error", err)

			return nil
		}

		return fmt.Errorf("redis transport: INFO server query failed — version check cannot proceed (use withSkipVersionCheck only in tests against in-process fakes): %w", err)
	}

	return t.checkVersionFromInfo(info)
}

// checkVersionFromInfo evaluates INFO server output against the minimum-version
// contract. Split from validateVersion so the parsing and decision logic can be
// exercised in unit tests without a live server. Logs at Info on supported
// versions, Warn on unknown server types, and returns ErrUnsupportedServerVersion
// or ErrMissingVersionField on violations.
func (t *RedisTransport) checkVersionFromInfo(info string) error {
	si := parseServerInfo(info)

	// Latch the detected backend before the version-empty check, so the
	// backend_type label captures "we saw valkey/redis with unparseable
	// version" rather than "we couldn't tell". newMetrics consumes this
	// at collector-construction time (called after this function returns).
	t.backendType = si.serverType

	if si.version == "" {
		if t.opts.skipVersionCheck {
			t.logger.Debug("redis transport: INFO returned no parseable version — skipped per withSkipVersionCheck")

			return nil
		}

		return fmt.Errorf("%w — cannot verify minimum floor", ErrMissingVersionField)
	}

	switch si.serverType {
	case serverTypeValkey:
		// Valkey forked from Redis 7.2.4 — it always satisfies the 6.2 minimum.
		// Verify ≥ 7.2 as a sanity check (no known Valkey below this exists).
		if !isVersionAtLeast(si.version, minValkeyMajor, minValkeyMinor) {
			return fmt.Errorf("%w (minimum: Valkey 7.2, got: %s)", ErrUnsupportedServerVersion, si.version)
		}

		t.logger.Info(
			"redis transport: connected",
			"server_type", si.serverType,
			"version", si.version,
		)

	case serverTypeRedis:
		if !isVersionAtLeast(si.version, minRedisMajor, minRedisMinor) {
			return fmt.Errorf("%w (minimum: Redis 6.2, got: %s)", ErrUnsupportedServerVersion, si.version)
		}

		t.logger.Info(
			"redis transport: connected",
			"server_type", si.serverType,
			"version", si.version,
		)

	case serverTypeUnknown:
		// Unknown server type (Dragonfly, KeyDB, MemoryDB, or future fork
		// speaking RESP) — assume wire-compatibility; warn instead of block.
		t.logger.Warn(
			"redis transport: unknown server type, assuming compatible",
			"server_type", si.serverType,
			"version", si.version,
		)
	}

	return nil
}

// parseServerInfo extracts the server type and version from INFO server output.
// Valkey emits `valkey_version:` as its primary version field. It also emits
// `redis_version:` for wire-protocol compatibility, but `valkey_version:` takes
// priority to correctly identify the server. When only `redis_version:` is
// present, the server is Redis.
func parseServerInfo(info string) serverInfo {
	var redisVer, valkeyVer string

	for line := range strings.SplitSeq(info, "\n") {
		line = strings.TrimSpace(line)
		if after, ok := strings.CutPrefix(line, "valkey_version:"); ok {
			valkeyVer = after
		} else if after, ok := strings.CutPrefix(line, "redis_version:"); ok {
			redisVer = after
		}
	}

	// Valkey takes priority — its INFO also emits redis_version for compat.
	if valkeyVer != "" {
		return serverInfo{serverType: serverTypeValkey, version: valkeyVer}
	}

	if redisVer != "" {
		return serverInfo{serverType: serverTypeRedis, version: redisVer}
	}

	return serverInfo{serverType: serverTypeUnknown, version: ""}
}

// isVersionAtLeast checks if a version string (e.g., "7.2.4") is >= major.minor.
// Unparseable components are treated as 0 (fail-closed: an unparseable "7.x.y"
// major will compare as 0 and the version check will reject the server).
func isVersionAtLeast(version string, major, minor int) bool {
	parts := strings.SplitN(version, ".", versionPartsMax)
	if len(parts) < versionPartsMin {
		return false
	}

	vmajor, _ := strconv.Atoi(parts[0])
	vminor, _ := strconv.Atoi(parts[1])

	if vmajor > major {
		return true
	}

	return vmajor == major && vminor >= minor
}

// detectClockDrift warns when server TIME differs from local time by more
// than a second. A large drift invalidates scanForEventID's seek-by-timestamp
// assumption (UUIDv7 → stream ID offset becomes unreliable).
func (t *RedisTransport) detectClockDrift(ctx context.Context) {
	redisTime, err := t.client.Time(ctx).Result()
	if err != nil {
		t.logger.Warn("redis transport: failed to get server TIME for clock drift detection", "error", err)
		// Mark drift unknown — gauge stays at NaN so PromQL alerts on
		// `clock_drift_seconds == 0` cannot misread "we never measured"
		// as "drift is healthy".
		t.metricsOrCountDrop().markClockDriftUnknown()

		return
	}

	drift := time.Since(redisTime).Abs()
	t.metricsOrCountDrop().observeClockDrift(drift)

	if drift > time.Second {
		t.logger.Warn(
			"redis transport: clock drift between app server and stream server",
			"drift_ms", drift.Milliseconds(),
			"threshold_ms", 1000,
		)
	}
}

// gcZombieGroups destroys consumer groups whose owning node's presence key has expired.
func (t *RedisTransport) gcZombieGroups(ctx context.Context, streamKey string) {
	groups, err := t.client.XInfoGroups(ctx, streamKey).Result()
	if err != nil {
		if isShutdownCancel(err) {
			return // graceful shutdown cancelled the GC pass — not a failure
		}

		if !isStreamNotExist(err) {
			// Root-enumeration failure means the entire GC pass is skipped —
			// worse than a per-group miss because EVERY zombie group stays.
			// Count it so sustained failures (ACL drift, cluster slot
			// migration mid-GC) are visible on dashboards.
			t.metricsOrCountDrop().recordZombieGCError()
			t.logger.Error("redis transport: failed to list consumer groups for GC", "error", err)
		}

		return
	}

	for _, g := range groups {
		if !strings.HasPrefix(g.Name, nodeGroupPrefix) {
			continue
		}

		groupNodeID := strings.TrimPrefix(g.Name, nodeGroupPrefix)
		nodePresenceKey := t.presenceKey(groupNodeID)

		exists, err := t.client.Exists(ctx, nodePresenceKey).Result()
		if err != nil {
			if isShutdownCancel(err) {
				return // graceful shutdown cancelled the GC pass — not a failure
			}

			t.logger.Error("redis transport: failed to check presence key during GC",
				"key", nodePresenceKey, "error", err)
			t.metricsOrCountDrop().recordZombieGCError()

			continue
		}

		if exists == 0 {
			if err := t.client.XGroupDestroy(ctx, streamKey, g.Name).Err(); err != nil {
				if isShutdownCancel(err) {
					return // graceful shutdown cancelled the GC pass — not a failure
				}

				t.logger.Warn("redis transport: failed to destroy zombie consumer group",
					"group", g.Name, "error", err)
				t.metricsOrCountDrop().recordZombieGCError()
			} else {
				t.logger.Info("redis transport: destroyed zombie consumer group", "group", g.Name)
			}
		}
	}
}

func isStreamNotExist(err error) bool {
	return err != nil && strings.Contains(err.Error(), redisErrNoSuchKey)
}

// isShutdownCancel reports whether err is a context cancellation — i.e. a
// graceful shutdown (Close cancelled the transport context) or, on the replay
// path, a subscriber disconnect. Background Redis ops in flight at that moment
// return this, and it must NOT be logged at ERROR or counted as a failure: it
// is expected, not a fault. A genuine stall surfaces as context.DeadlineExceeded
// (a per-operation timeout) and is deliberately NOT matched here, so real
// timeouts stay visible on logs and error metrics.
func isShutdownCancel(err error) bool {
	return errors.Is(err, context.Canceled)
}

// key builds a Redis key with hash tags for Cluster slot consistency.
func (t *RedisTransport) key(suffix string) string {
	return key(t.opts.streamName, suffix)
}

// presenceKey returns the presence SET key for the given nodeID, scoped to
// this transport's stream name so Cluster hash tags place it in the same slot.
func (t *RedisTransport) presenceKey(nodeID string) string {
	return t.key(":presence:" + nodeID)
}

func (t *RedisTransport) presenceKeyPattern() string {
	return t.key(":presence:*")
}

// dispatchHistory replays history entries to a subscriber using a two-pass
// paginated XRANGE approach. Pass 1 finds the exact event ID match and
// dispatches subsequent entries. If not found (trimmed), Pass 2 re-scans
// and dispatches ALL entries as best-effort replay.
//
// HistoryDispatched and Ready are called inside this function (not by the caller).
// Safe because LocalSubscriber.Ready() has an idempotency guard.
//
// On XRANGE failure mid-replay, returns a non-nil error so AddSubscriber can
// remove the subscriber; the client then reconnects with Last-Event-ID set to
// the last successful dispatch, and the next AddSubscriber fills the gap.
//
// Concurrency is bounded by historyReplaySem to prevent reconnect storms from
// exhausting the Redis connection pool with concurrent XRANGE operations.
func (t *RedisTransport) dispatchHistory(ctx context.Context, s *mercure.LocalSubscriber, toStreamID string) (err error) {
	// Mirrors the BoltDB transport's dispatchHistory: mercure.transport.history
	// span with a per-backend mercure.transport attribute. Defaults to
	// SpanKindInternal — the parent is whatever the caller set
	// (mercure.subscribe under Caddy `tracing`, the no-op tracer otherwise,
	// so there is no cost when tracing is off).
	//
	// Like the BoltDB transport, any non-nil return from this function flips
	// span status to codes.Error via recordSpanError. The named-return +
	// deferred-check shape covers all error paths in rt (semaphore
	// ctx-cancellation, XRANGE failures, state-machine errors); the BoltDB
	// transport guards a narrower path because its history is a single
	// bbolt.View call. Cross-backend dashboards filtering on
	// span.Status == ERROR catch Redis history failures the same way they
	// catch Bolt ones. DO NOT add `err := ...` inside the defer block — it
	// would shadow the function's named return and silently break the
	// status-on-error wiring.
	ctx, span := startSpan(ctx, "mercure.transport.history",
		trace.WithAttributes(
			attribute.String("mercure.transport", "redis"),
			attribute.String("mercure.subscriber.id", s.ID),
			// Last-Event-ID is client-supplied and bounded only by the HTTP
			// server's max-header size (~1 MiB), so cap it before it lands
			// on the span — a single oversized header should not bloat trace
			// storage. A valid UUIDv7 is 36 chars; the cap leaves room for
			// custom IDs. (Fork hardening over the BoltDB transport, which
			// records the raw value.)
			attribute.String("mercure.last_event_id.requested", truncateEventIDForObservability(s.RequestLastEventID)),
		))

	defer func() {
		if err != nil {
			recordSpanError(span, err)
		}

		span.End()
	}()

	// Acquire history replay semaphore to limit concurrent XRANGE operations.
	select {
	case t.historyReplaySem <- struct{}{}:
		defer func() { <-t.historyReplaySem }()
	case <-ctx.Done():
		return fmt.Errorf("redis transport: history replay cancelled: %w", ctx.Err())
	}

	if m := t.metricsOrCountDrop(); m != nil {
		m.historyReplayConcurrent.Inc()
		defer m.historyReplayConcurrent.Dec()

		historyStart := time.Now()

		defer func() {
			m.historyReplayDuration.Observe(time.Since(historyStart).Seconds())
		}()
	}

	rs := t.newHistoryReplayState(s, toStreamID)
	err = rs.run(ctx)

	// Surface the replay outcome on the span. Set on success and on any
	// failure that occurs after the replay state is built — a partial count
	// on a mid-stream XRANGE failure is still useful. (The pre-replay
	// semaphore-cancel return above happens before rs exists and emits none.)
	// IsRecording-gated so it's free when tracing is off.
	if span.IsRecording() {
		span.SetAttributes(
			attribute.Int("mercure.history.events_replayed", rs.eventsReplayed),
			attribute.Bool("mercure.history.future_dated", rs.path == replayPathFutureDated),
			attribute.Bool("mercure.history.full_scan", rs.path == replayPathFullScan),
			attribute.Bool("mercure.history.truncated", rs.truncated),
		)
	}

	return err
}

// maxObservedEventIDLen bounds how much of a client-supplied Last-Event-ID is
// copied into logs and span attributes. The value is bounded at HTTP ingress
// only by the server's max-header size (~1 MiB), so logging or tagging it
// verbatim lets a single oversized header bloat a log line or trace. A valid
// UUIDv7 ID is 36 chars; 80 leaves headroom for custom publisher IDs.
const maxObservedEventIDLen = 80

// truncateEventIDForObservability rune-safely caps a client-controlled
// event ID for safe inclusion in logs / span attributes. Truncation is
// observability-only — the full value is still used for the actual
// history-seek logic.
//
// The common case (a UUIDv7 ID, 36 bytes) is a single length check with no
// allocation. Only an over-cap value walks the string to find the rune
// boundary at the cap — deliberately avoiding `[]rune(id)`, which would
// eagerly allocate a rune slice the size of the whole (attacker-bounded,
// up to ~1 MiB) string just to truncate it.
func truncateEventIDForObservability(id string) string {
	if len(id) <= maxObservedEventIDLen { // byte len >= rune len, so this is a safe fast path
		return id
	}

	runes := 0
	for i := range id {
		if runes == maxObservedEventIDLen {
			return id[:i] + "…(truncated)"
		}

		runes++
	}

	// Fewer than maxObservedEventIDLen runes despite >maxObservedEventIDLen
	// bytes (multi-byte content): already within the rune cap.
	return id
}

// replayPath records which history-replay strategy ran. It is a single value
// (not two bools) so the future-dated and full-scan outcomes can't both be set
// at once — they are mutually exclusive (the future-ID guard sets found=true,
// which suppresses the Pass 2 fallback). dispatchHistory translates it into the
// mercure.history.future_dated / .full_scan span attributes at the boundary.
type replayPath uint8

const (
	replayPathExact       replayPath = iota // Default/zero: neither of the below — a clean Pass 1 match, "earliest", or a Pass 1 cut short before it could fall back.
	replayPathFutureDated                   // Future-ID guard tripped; both passes skipped.
	replayPathFullScan                      // Pass 1 missed → Pass 2 full-stream rescan was entered (may itself be cut short — see truncated).
)

// historyReplayState holds per-replay state for dispatchHistory.
type historyReplayState struct {
	t          *RedisTransport
	s          *mercure.LocalSubscriber
	streamKey  string
	toStreamID string

	requestedEventID      string
	seekStreamID          string
	found                 bool
	lastDispatchedEventID string
	skippedEntries        int

	// Replay outcome, surfaced as mercure.transport.history span attributes
	// by dispatchHistory. path is the mutually-exclusive strategy; truncated
	// is orthogonal to it.
	eventsReplayed int        // matched updates delivered to the subscriber
	path           replayPath // which replay strategy ran
	truncated      bool       // backlog not fully replayed: a matched update went undelivered (subscriber disconnected, for any reason, or buffer full)
}

func (t *RedisTransport) newHistoryReplayState(s *mercure.LocalSubscriber, toStreamID string) *historyReplayState {
	rs := &historyReplayState{
		t:                t,
		s:                s,
		streamKey:        t.key(""),
		toStreamID:       toStreamID,
		requestedEventID: s.RequestLastEventID,
		found:            s.RequestLastEventID == mercure.EarliestLastEventID,
	}

	rs.resolveSeekStreamID()

	return rs
}

// resolveSeekStreamID decodes the subscriber's requested UUIDv7 event ID into
// a stream-ID seek position, subtracting the configured clock-skew margin. A
// non-UUID RequestLastEventID is treated as EarliestLastEventID.
func (rs *historyReplayState) resolveSeekStreamID() {
	if rs.requestedEventID == mercure.EarliestLastEventID {
		rs.seekStreamID = "-"

		return
	}

	clockSkewMs := rs.t.opts.clockSkewMarginMs()

	seekID, _, err := uuidv7ToStreamID(rs.requestedEventID, clockSkewMs)
	if err != nil {
		rs.seekStreamID = "-" // Full scan fallback for custom (non-UUIDv7) IDs

		return
	}

	rs.seekStreamID = seekID
}

// run executes the two-pass history replay. Returns non-nil error if an
// XRANGE pass failed mid-stream — in that case HistoryDispatched/Ready are
// skipped so the subscriber is not advertised as caught-up, and the caller
// (AddSubscriber) cleans up the shard-list entry.
//
// For the normal and subscriber-disconnected paths, HistoryDispatched and
// Ready fire once at the end of run() with a consistent "" → EarliestLastEventID
// fallback. This is the single authoritative place the signal is emitted.
func (rs *historyReplayState) run(ctx context.Context) error {
	// Pre-scan guard for future-dated Last-Event-IDs: if the requested
	// UUIDv7's encoded wall-clock timestamp is strictly newer than the
	// hub's current wall clock (plus clockSkewMargin), the client is
	// presenting an ID from the future — typically a browser-cached ID
	// from a different environment, a client clock running fast, or a
	// load-test harness manufactured a UUIDv7 from now() against an
	// older hub. Pass 2 (the trimmed-ID fallback) would replay the full
	// retention window, which is wrong for a future-dated ID. Skip both
	// passes; the subscriber is already on the shard list, so live
	// events arriving after now will be delivered through the normal
	// path.
	//
	// Mark as "handled" so Pass 1 / Pass 2 are skipped via the else
	// branch + rs.found gate below. lastDispatchedEventID stays empty;
	// the tail logic at the end of run() falls back to
	// EarliestLastEventID for the response header, matching the
	// pre-existing "empty replay" semantics. Falling through (rather
	// than early-return) keeps HistoryDispatched / Ready in one
	// authoritative place.
	nowMs := time.Now().UnixMilli()

	var (
		disconnected bool
		err          error
	)

	if isRequestedIDFutureDated(rs.requestedEventID, nowMs, rs.t.opts.clockSkewMarginMs()) {
		rs.t.logger.Warn(
			"redis transport: history replay skipped: requested Last-Event-ID dated past the hub's wall clock (future ID, wrong-environment ID, or client clock skew). Subscriber will receive live events.",
			"subscriber", rs.s.ID,
			"requested_event_id", truncateEventIDForObservability(rs.requestedEventID),
			"now_ms", nowMs,
		)

		rs.path = replayPathFutureDated
		rs.found = true
	} else {
		// Pass 1: Paginated scan for exact event ID match.
		disconnected, err = rs.scanForEventID(ctx)
		if err != nil {
			return err
		}
	}

	// Pass 2 (trimmed-ID fallback): re-scan and dispatch ALL entries. Only
	// run if Pass 1 neither found the ID nor hit a disconnect. Pass 2's
	// `disconnected` return is ignored; Ready below is idempotent on a
	// closed subscriber. Log Info before calling so operators see the
	// fallback attempt even if replayAll fails.
	if !disconnected && !rs.found {
		rs.t.logger.Info(
			"redis transport: history replay falling back to full-stream Pass 2",
			"subscriber", rs.s.ID,
			"requested_event_id", truncateEventIDForObservability(rs.requestedEventID),
			"to_stream_id", rs.toStreamID,
		)

		if _, err = rs.replayAll(ctx); err != nil {
			return err
		}

		// Set path only after replayAll returns without an XRANGE error, so it
		// agrees with the recordHistoryReplayFullScan metric below: a failed
		// Pass 2 returns above (the span already flips to ERROR). Both mean
		// "Pass 2 ran", not "delivered everything" — a disconnect can still
		// cut Pass 2 short (truncated=true) while path stays full_scan.
		rs.path = replayPathFullScan
		rs.t.metricsOrCountDrop().recordHistoryReplayFullScan()
	}

	if rs.skippedEntries > 0 {
		rs.t.logger.Warn("redis transport: history replay skipped undeliverable entries (decode failure or forbidden SSE chars; breakdown in mercure_redis_stream_decode_errors_total{kind})",
			"count", rs.skippedEntries, "subscriber", rs.s.ID)
	}

	if rs.lastDispatchedEventID == "" {
		rs.lastDispatchedEventID = mercure.EarliestLastEventID
	}

	rs.s.HistoryDispatched(rs.lastDispatchedEventID)
	rs.s.Ready(ctx)

	return nil
}

// dispatchEntry decodes and dispatches a single history entry.
// Returns false if the subscriber disconnected or buffer is full.
func (rs *historyReplayState) dispatchEntry(ctx context.Context, entry redis.XMessage) bool {
	update := decodeStreamEntry(entry, *rs.t.codec.Load(), rs.t.logger, rs.t.metricsOrCountDrop(), rs.t.dropLogLimiter)
	if update == nil {
		rs.skippedEntries++

		return true
	}

	if rs.s.Match(update) {
		// Count only on successful delivery — Dispatch returns false when the
		// subscriber disconnected or its buffer is full, which stops the
		// replay and must NOT be counted as a delivered event (the span attr
		// is documented as "delivered", not "attempted").
		ok := rs.s.Dispatch(ctx, update, true)
		if ok {
			rs.eventsReplayed++
		} else {
			// Dispatch returns false for ANY disconnect — client close, hub
			// shutdown, ctx-cancel — or a full buffer; it can't tell them
			// apart (it doesn't consult ctx). All mean the same thing here:
			// the backlog wasn't fully replayed. Flag it so the span (which
			// stays Status=OK — a gone subscriber isn't a transport error)
			// records the cut-short replay rather than looking like a clean
			// finish. truncated is a completeness signal, not a backpressure
			// alert: the authoritative backpressure signal is the
			// subscribers_lost{reason="backpressure"} metric.
			rs.truncated = true
			rs.t.metricsOrCountDrop().recordHistoryReplayTruncated()
		}

		return ok
	}

	return true
}

// decodeStreamEntry unmarshals the "data" field of a stream entry into an
// Update. Returns nil on decode failure, after logging and recording the
// appropriate streamDecodeErrorKind metric. Callers handle their own
// cursor/skip bookkeeping — the live path advances lastDispatchedStreamID
// even on failure, the history path just increments a skip counter.
func decodeStreamEntry(entry redis.XMessage, codec mercure.Codec, logger *slog.Logger, metrics *Metrics, dropLogLimiter *rate.Limiter) *mercure.Update {
	dataStr, ok := entry.Values["data"].(string)
	if !ok {
		if allowLog(dropLogLimiter) {
			logger.Error(
				"redis transport: unexpected data type in stream entry",
				"id", entry.ID,
				"actual_type", fmt.Sprintf("%T", entry.Values["data"]),
			)
		}

		metrics.recordStreamDecodeError(streamDecodeErrorDataType)

		return nil
	}

	update, err := codec.Unmarshal([]byte(dataStr))
	if err != nil {
		if allowLog(dropLogLimiter) {
			logger.Error(
				"redis transport: unmarshal error in stream entry",
				"error", err,
				"id", entry.ID,
				"payload_bytes", len(dataStr),
			)
		}

		metrics.recordStreamDecodeError(streamDecodeErrorUnmarshal)

		return nil
	}

	// Receive-side SSE-injection guard. The update was validated at its origin
	// hub's Hub.Publish, but a heterogeneous or older hub, or direct tampering
	// with the Redis stream, could place an entry whose id or type holds a
	// CR/LF/NUL that would forge SSE frame boundaries into a subscriber's stream
	// (CWE-93). Guard only the fields written verbatim to the wire (id, type) via
	// ValidateSSEFields — NOT the full Validate(), which rejects the reserved
	// topics that hub-internal subscription events legitimately travel on.
	if err := update.ValidateSSEFields(); err != nil {
		if allowLog(dropLogLimiter) {
			// Log only the safe Redis stream ID — consistent with the data_type /
			// unmarshal drops above. The offending id/type value is NOT logged (it
			// carries the CR/LF/NUL), and neither are the update's topics: on this
			// tampered-entry path they are unvalidated attacker data. An operator can
			// XRANGE entry.ID to inspect the full entry out of band.
			logger.Error(
				"redis transport: dropping stream entry with forbidden SSE chars in id/type",
				"error", err,
				"id", entry.ID,
			)
		}

		metrics.recordStreamDecodeError(streamDecodeErrorForbiddenSSEChars)

		return nil
	}

	return update
}

// scanForEventID performs Pass 1: scan for exact event ID match and dispatch
// subsequent entries. Returns (disconnected, err) where:
//   - err != nil: XRANGE failed; caller must abort and surface the error.
//   - disconnected == true: subscriber disconnected mid-dispatch; caller should
//     skip Pass 2 but let run() call Ready (idempotent on a closed subscriber).
//   - both false: pass completed normally (rs.found indicates whether the
//     requested ID was located).
func (rs *historyReplayState) scanForEventID(ctx context.Context) (bool, error) {
	return rs.paginate(ctx, "pass 1", rs.seekStreamID, func(entry redis.XMessage) bool {
		if !rs.found {
			if id, _ := entry.Values["eventID"].(string); id == rs.requestedEventID {
				rs.found = true
			}

			return true
		}

		return rs.dispatchAndTrack(ctx, entry)
	})
}

// replayAll performs Pass 2: replay ALL entries as best-effort (event ID was trimmed).
// Same return-value contract as scanForEventID.
//
// Uses "-" (stream beginning) instead of seekStreamID to ensure no entries are
// missed. Redis XRANGE with a cursor before the stream's first entry returns
// from the first entry, but using "-" makes the intent explicit.
func (rs *historyReplayState) replayAll(ctx context.Context) (bool, error) {
	return rs.paginate(ctx, "pass 2", "-", func(entry redis.XMessage) bool {
		return rs.dispatchAndTrack(ctx, entry)
	})
}

// paginate drives the paginated XRANGE loop for both replay passes. yield is
// called for each entry; returning false signals "stop iteration — subscriber
// disconnected", which paginate surfaces as disconnected=true.
//
// Returns:
//   - (false, nil) on normal completion (stream exhausted or yield stayed true)
//   - (true, nil) when yield returned false (subscriber disconnected mid-dispatch)
//   - (false, err) when XRANGE itself failed; caller aborts replay and logs.
func (rs *historyReplayState) paginate(ctx context.Context, pass, startCursor string, yield func(redis.XMessage) bool) (bool, error) {
	cursor := startCursor

	for {
		entries, err := rs.t.client.XRangeN(ctx, rs.streamKey, cursor, rs.toStreamID, historyPageSize).Result()
		if err != nil {
			// A cancelled context here is a subscriber disconnect mid-replay (or
			// shutdown), not a replay fault — skip the ERROR log, but still
			// return the error so the caller aborts the replay cleanly.
			if !isShutdownCancel(err) {
				rs.t.logger.Error(
					"redis transport: history replay XRANGE failed",
					"pass", pass,
					"error", err,
					"cursor", cursor,
					"subscriber", rs.s.ID,
					"requested_event_id", truncateEventIDForObservability(rs.requestedEventID),
					"to_stream_id", rs.toStreamID,
				)
			}

			return false, fmt.Errorf("redis transport: history replay %s XRANGE failed at cursor %s: %w", pass, cursor, err)
		}

		if len(entries) == 0 {
			return false, nil
		}

		for _, entry := range entries {
			if !yield(entry) {
				return true, nil
			}
		}

		if int64(len(entries)) < historyPageSize {
			return false, nil
		}

		cursor = nextStreamID(entries[len(entries)-1].ID)
	}
}

// dispatchAndTrack is the shared per-entry action for both replay passes
// once they're in the "dispatch this entry" state: dispatch, record the
// event ID on success, signal disconnect on failure.
func (rs *historyReplayState) dispatchAndTrack(ctx context.Context, entry redis.XMessage) bool {
	if !rs.dispatchEntry(ctx, entry) {
		return false
	}

	// Guard the cursor: eventID is Redis-sourced metadata (a denormalized copy of
	// the update's id) that flows to the Last-Event-Id response header. The decoded
	// update.ID was already checked in decodeStreamEntry, but this separate copy is
	// not, so a tampered entry could pair a safe update.ID with a forbidden eventID.
	// A legitimate eventID equals the published update.ID and always passes.
	if id, ok := entry.Values["eventID"].(string); ok {
		if mercure.ValidSSEFieldValue(id) {
			rs.lastDispatchedEventID = id
		} else {
			// The entry was still delivered with its clean update.ID; only the
			// Last-Event-Id cursor is withheld. This is NOT a decode drop (nothing
			// was dropped) so it has its own counter — always recorded — while the
			// Warn is rate-sampled by the same limiter as the decode-drop logs, so a
			// decode flood can't hide this signal entirely.
			rs.t.metricsOrCountDrop().recordHistoryCursorRejected()

			if allowLog(rs.t.dropLogLimiter) {
				rs.t.logger.Warn(
					"redis transport: forbidden SSE chars in eventID cursor metadata; Last-Event-Id not advanced for this entry",
					"id", entry.ID,
					"subscriber", rs.s.ID,
				)
			}
		}
	}

	return true
}

// xreadgroupListener is the main dispatch goroutine. It reads messages from the
// Redis Stream via XREADGROUP and dispatches them to matching local subscribers.
//
//nolint:funlen // The loop is one cohesive read-classify-dispatch cycle; helpers would scatter the consumeBackoff / activity-stamp / consecutiveErrors state.
func (t *RedisTransport) xreadgroupListener(ctx context.Context) {
	// close(listenerDone) runs before wg.Done — Close's sequence
	// (wait listenerDone → close shard chans → wg.Wait) depends on that order.
	defer t.wg.Done()
	defer close(t.listenerDone)

	streamKey := t.key("")
	consumerName := "consumer-" + t.nodeID
	startID := ">"
	consecutiveErrors := 0

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		xreadStart := time.Now()

		entries, err := t.client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    t.nodeGroup,
			Consumer: consumerName,
			Streams:  []string{streamKey, startID},
			Count:    t.opts.xreadCount,
			Block:    t.opts.xreadBlock,
		}).Result()
		if err == nil {
			if m := t.metricsOrCountDrop(); m != nil {
				m.xreadgroupLatency.Observe(time.Since(xreadStart).Seconds())
			}
		}

		// Stamp on cycle completion (success + redis.Nil); errors don't
		// stamp so sustained failure trips the readiness gate.
		if err == nil || errors.Is(err, redis.Nil) {
			t.lastListenerActivityNanos.Store(time.Now().UnixNano())
		}

		if errors.Is(err, redis.Nil) {
			consecutiveErrors = 0

			continue
		}

		if err != nil {
			consecutiveErrors++

			shouldReturn, newStartID := t.handleXReadError(ctx, err, streamKey, consecutiveErrors)
			if shouldReturn {
				return
			}

			startID = newStartID

			continue
		}

		consecutiveErrors = 0

		// When draining PEL ("0") and no messages returned, PEL is empty.
		if startID == "0" && t.isPELEmpty(entries) {
			startID = ">"

			continue
		}

		startID = t.processStreamEntries(ctx, entries, streamKey, startID)
	}
}

// handleXReadError handles errors from XREADGROUP.
// Returns (true, _) if the listener should return, (false, newStartID) otherwise.
// consecutiveErrors drives a capped exponential backoff so a flapping
// Redis/Valkey doesn't produce a tight 1 RPS error log + reconnect loop.
func (t *RedisTransport) handleXReadError(ctx context.Context, err error, streamKey string, consecutiveErrors int) (bool, string) {
	if ctx.Err() != nil {
		return true, ""
	}

	if strings.Contains(err.Error(), redisErrNoGroup) {
		return t.recreateConsumerGroup(ctx, streamKey, consecutiveErrors)
	}

	t.metricsOrCountDrop().recordXReadgroupError(xreadgroupErrorOther)
	t.logger.Error("redis transport: xreadgroup error", "error", err)

	if !t.contextSleep(ctx, xreadErrorBackoff(consecutiveErrors)) {
		return true, ""
	}

	return false, "0" // Drain PEL on reconnect
}

// recreateConsumerGroup handles the NOGROUP case: the consumer group was
// destroyed (e.g. zombie GC on another node, or a manual XGROUP DESTROY), so
// re-create it from the last-dispatched cursor and resume reading new entries.
// Returns (true, "") when the listener should return — a graceful shutdown
// cancelled the recreate (not a failure: no metric/log), or shutdown raced the
// backoff after a genuine recreate failure — otherwise (false, ">").
func (t *RedisTransport) recreateConsumerGroup(ctx context.Context, streamKey string, consecutiveErrors int) (bool, string) {
	t.metricsOrCountDrop().recordXReadgroupError(xreadgroupErrorNoGroup)
	t.logger.Warn("redis transport: consumer group destroyed, re-creating",
		"group", t.nodeGroup)

	resumeID := *t.lastDispatchedStreamID.Load()
	if resumeID == streamIDEarliest {
		resumeID = "0"
	}

	if err := t.client.XGroupCreateMkStream(ctx, streamKey, t.nodeGroup, resumeID).Err(); err != nil {
		if isShutdownCancel(err) {
			return true, "" // graceful shutdown cancelled the recreate — not a failure
		}

		t.metricsOrCountDrop().recordXReadgroupError(xreadgroupErrorNoGroupRecreateFailed)
		t.logger.Error("redis transport: failed to re-create consumer group", "error", err)

		if !t.contextSleep(ctx, xreadErrorBackoff(consecutiveErrors)) {
			return true, ""
		}
	}

	return false, ">"
}

// xreadErrorBackoff returns a capped exponential delay for the listener's
// reconnect path. Starts at 1s, doubles each consecutive failure, capped
// at 30s to keep a flapping Redis from producing a tight error log loop
// while still recovering quickly when the outage is brief.
func xreadErrorBackoff(consecutiveErrors int) time.Duration {
	const (
		base    = time.Second
		ceiling = 30 * time.Second
	)

	if consecutiveErrors <= 1 {
		return base
	}

	// Shift safely: 1 << 30 already exceeds any reasonable ceiling.
	shift := min(consecutiveErrors-1, 30)

	d := base << shift
	if d > ceiling || d <= 0 {
		return ceiling
	}

	return d
}

// contextSleep sleeps for the given duration or returns early if the context is cancelled.
// Returns true if the sleep completed, false if the context was cancelled.
// Uses time.NewTimer + Stop so a cancelled sleep doesn't leak a timer until
// its deadline (matters on reconnect storms that retry this path rapidly).
func (t *RedisTransport) contextSleep(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func (t *RedisTransport) isPELEmpty(entries []redis.XStream) bool {
	for _, stream := range entries {
		if len(stream.Messages) > 0 {
			return false
		}
	}

	return true
}

// processStreamEntries processes stream messages: decodes, dispatches, and ACKs.
// Returns the next startID to use for XREADGROUP.
func (t *RedisTransport) processStreamEntries(ctx context.Context, entries []redis.XStream, streamKey, startID string) string {
	for _, stream := range entries {
		if m := t.metricsOrCountDrop(); m != nil {
			m.xreadgroupBatch.Observe(float64(len(stream.Messages)))
		}

		// XACK is deferred until the whole batch has fanned out (one round-trip
		// amortizes the ACK over the batch). Trade-off: PEL growth during the
		// fan-out window — slow dispatch delays the ACK, and a failed XAck forces
		// the "0" PEL-drain reprocess below. Keep xread_count conservative under
		// storms and alert on mercure_redis_consumer_pending so a growing PEL is
		// visible before it forces a drain.
		ackIDs := t.ackIDsBuf[:0]

		for _, entry := range stream.Messages {
			ackIDs = append(ackIDs, entry.ID)
			t.processStreamEntry(ctx, entry)
		}

		t.ackIDsBuf = ackIDs

		if len(ackIDs) > 0 {
			if nextID, bail := t.ackProcessedBatch(ctx, streamKey, ackIDs, startID); bail {
				return nextID
			}
		}
	}

	return startID
}

// ackProcessedBatch XACKs a fanned-out batch. It returns (nextStartID, true)
// when the listener loop should return:
//   - a cancelled context is a graceful shutdown (Close cancelled ctx while the
//     batch ACK was in flight), not an XAck failure — bail with startID without
//     inflating the XAck error metric or logging at ERROR; the pending entries
//     are reclaimed (re-delivered) on the next listener start, so nothing is lost.
//   - a genuine XAck failure logs, backs off, and returns "0" to drain the PEL on
//     reconnect (or startID if shutdown races the backoff).
//
// On success it returns ("", false) and the caller continues reading.
func (t *RedisTransport) ackProcessedBatch(ctx context.Context, streamKey string, ackIDs []string, startID string) (string, bool) {
	if err := t.client.XAck(ctx, streamKey, t.nodeGroup, ackIDs...).Err(); err != nil {
		if isShutdownCancel(err) {
			return startID, true
		}

		t.metricsOrCountDrop().recordXReadgroupError(xreadgroupErrorXAck)
		t.logger.Error("redis transport: batch XAck failed", "error", err, "count", len(ackIDs))

		if !t.contextSleep(ctx, time.Second) {
			// Shutdown signaled during sleep — bail instead of returning "0"
			// which would trigger a PEL drain on the already-closed listener.
			return startID, true
		}

		return "0", true
	}

	return "", false
}

// processStreamEntry decodes and dispatches a single stream entry to matching
// subscribers. Must be called from the XREADGROUP goroutine only (the single
// writer of lastDispatchedStreamID).
//
// Cursor protocol (NO mu held across fan-out — this is what frees
// AddSubscriber/RemoveSubscriber from waiting on the fan-out): advance
// lastDispatchedStreamID to this entry BEFORE fanning out, then dispatch
// synchronously. A subscriber that joins concurrently either (a) is in the shard
// index when a worker takes its candidate snapshot and gets the event live, or
// (b) joined after that snapshot — in which case its post-join cursor load
// (AddSubscriber) is guaranteed by the index-lock + atomic-cursor happens-before
// chain to already cover this entry, so it replays via history. Never neither:
// gap-free.
// The cost is a rare DUPLICATE at the join boundary (delivered live AND via
// replay), which is within the transport's at-least-once contract and reconciled
// by SSE Last-Event-ID.
//
// Fan-out stays SYNCHRONOUS: dispatchToSubscribers wg.Waits all shard workers
// before returning, and the caller XACKs only after this returns — so an entry
// is never acked (nor used as a group-recreate resume point) while a shard
// worker still owes its delivery.
func (t *RedisTransport) processStreamEntry(ctx context.Context, entry redis.XMessage) {
	// PEL dedup: skip entries already dispatched. This goroutine is the sole
	// writer, so a plain load of its own cursor is sufficient here.
	if !compareStreamIDs(entry.ID, *t.lastDispatchedStreamID.Load()) {
		return
	}

	id := entry.ID

	update := decodeStreamEntry(entry, *t.codec.Load(), t.logger, t.metricsOrCountDrop(), t.dropLogLimiter)
	if update == nil {
		// Corrupt entries still advance the cursor so the consumer group
		// makes forward progress past them.
		t.lastDispatchedStreamID.Store(&id)

		return
	}

	// Advance the cursor BEFORE fan-out (see the ordering note above), then
	// dispatch synchronously.
	t.lastDispatchedStreamID.Store(&id)
	t.dispatchToSubscribers(ctx, update)
}

// healthCheck monitors server connectivity via periodic PINGs.
func (t *RedisTransport) healthCheck(ctx context.Context) {
	defer t.wg.Done()

	ticker := time.NewTicker(t.opts.healthInterval)
	defer ticker.Stop()

	failures := 0

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			failures = t.runHealthPing(ctx, failures)
		}
	}
}

// healthPingTimeout caps a single health PING so a stalled connection pool
// (every conn hung) can't block the health loop indefinitely — Ready() depends
// on healthy flipping to false promptly.
const healthPingTimeout = 5 * time.Second

// minHealthPingTimeout floors the PING deadline so a small configured
// healthInterval can't shrink it below normal Redis RTT (cross-AZ, a GC/fork
// pause) and flip a healthy node unhealthy under load.
const minHealthPingTimeout = 1 * time.Second

// runHealthPing issues a single PING and updates failure/healthy state.
// Returns the updated consecutive-failure count.
func (t *RedisTransport) runHealthPing(ctx context.Context, failures int) int {
	// Clamp the deadline to [minHealthPingTimeout, healthPingTimeout] but never
	// above the configured interval: a fast-failover operator (sub-5s interval)
	// keeps prompt outage detection, while a tiny interval still can't drop the
	// deadline below the RTT floor and manufacture false-unhealthy flips. One
	// deadline bounds the WHOLE health cycle (PING + the stream-length and
	// consumer-lag samples below) so a hung connection can't block the loop on a
	// follow-on call after PING succeeds — the samples share the PING's budget.
	healthTimeout := max(min(t.opts.healthInterval, healthPingTimeout), minHealthPingTimeout)

	healthCtx, cancel := context.WithTimeout(ctx, healthTimeout)
	defer cancel()

	if err := t.client.Ping(healthCtx).Err(); err != nil {
		// Graceful shutdown cancelled the parent ctx mid-PING — not a health
		// failure. Don't count it or log at ERROR; the loop exits on ctx.Done().
		// A real PING stall surfaces as DeadlineExceeded (healthCtx's budget) and
		// still counts below.
		if isShutdownCancel(err) {
			return failures
		}

		failures++
		t.logger.Error("redis transport: health check PING failed",
			"error", err, "consecutive_failures", failures)

		if failures >= t.opts.healthThreshold {
			t.healthy.Store(false)

			if m := t.metricsOrCountDrop(); m != nil {
				m.healthy.Set(0)
			}
		}

		return failures
	}

	if failures >= t.opts.healthThreshold {
		t.logger.Info("redis transport: recovered, marking healthy",
			"previous_failures", failures)
	}

	t.healthy.Store(true)

	if m := t.metricsOrCountDrop(); m != nil {
		m.healthy.Set(1)

		// Sample stream length during successful health check.
		length, err := t.client.XLen(healthCtx, t.key("")).Result()

		switch {
		case err == nil:
			m.streamLength.Set(float64(length))
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			// Shutdown cancellation or the shared health budget expiring after a
			// slow PING — the gauge holds its last value, acceptable for a
			// stale-but-not-failing best-effort sample.
		default:
			// XLen can fail on ACL/permission changes while PING still
			// succeeds; surface so operators see drift between health and
			// stream_length rather than silently freezing the gauge.
			t.logger.Warn("redis transport: failed to sample stream length during health check", "error", err)
		}

		// Sample consumer-group lag + PEL size for "are-we-keeping-up?"
		// observability. Cheap (single XINFO GROUPS) and runs at the
		// existing healthInterval cadence — no new ticker.
		t.sampleConsumerLag(healthCtx, m)

		// Sample available history window. Proactive counterpart to the
		// reactive history_replay_fallback counter — surfaces "retention
		// is tightening" before clients start missing on reconnect.
		t.sampleHistoryWindow(ctx, m)
	}

	return 0
}

// sampleHistoryWindow computes the elapsed wall-clock seconds between the
// stream's oldest and newest entry IDs (Redis-stamped `<ms>-<seq>`).
// Cheap: two XRANGE-by-N calls (head and tail) — no full scan. Reports 0 on
// empty stream, -1 when either ID cannot be parsed. Logs Warn on XRANGE
// errors so retention-tight alerts do not stay silent on staleness.
//
//nolint:funlen // each branch decides one value of historyWindowSeconds (sentinel contract); splitting would scatter the matrix.
func (t *RedisTransport) sampleHistoryWindow(ctx context.Context, m *Metrics) {
	streamKey := t.key("")

	head, err := t.client.XRangeN(ctx, streamKey, "-", "+", 1).Result()
	if err != nil {
		switch {
		case isStreamNotExist(err):
			m.historyWindowSeconds.Set(0)
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			// Shutdown — hold last value.
		default:
			m.historyWindowSeconds.Set(-1)
			t.logger.Warn("redis transport: failed to sample history-window head", "error", err)
		}

		return
	}

	if len(head) == 0 {
		// Empty stream — no history yet.
		m.historyWindowSeconds.Set(0)

		return
	}

	tail, err := t.client.XRevRangeN(ctx, streamKey, "+", "-", 1).Result()
	if err != nil {
		switch {
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			// Shutdown — hold last value.
		default:
			m.historyWindowSeconds.Set(-1)
			t.logger.Warn("redis transport: failed to sample history-window tail", "error", err)
		}

		return
	}

	if len(tail) == 0 {
		// Race between head and tail XRANGE: a trim eliminated every entry
		// after head observed at least one. Sample as empty rather than
		// fabricating a negative window.
		m.historyWindowSeconds.Set(0)

		return
	}

	// Stream IDs are <ms>-<seq>; the millisecond prefix is the wall-clock
	// timestamp Redis stamped at XADD. Use those directly rather than
	// parsing UUIDv7 from the entry payload — they're authoritative and
	// monotonic per-stream regardless of producer clock drift.
	headMs, ok := streamIDMilliseconds(head[0].ID)
	if !ok {
		m.historyWindowSeconds.Set(-1)

		return
	}

	tailMs, ok := streamIDMilliseconds(tail[0].ID)
	if !ok {
		m.historyWindowSeconds.Set(-1)

		return
	}

	if tailMs < headMs {
		// Cannot happen for a healthy stream (tail is younger than head),
		// but defend against a corrupted stream-ID parser rather than
		// reporting a negative window.
		m.historyWindowSeconds.Set(-1)

		return
	}

	m.historyWindowSeconds.Set(float64(tailMs-headMs) / 1000.0)
}

// isRequestedIDFutureDated reports whether the subscriber's requested
// Last-Event-ID encodes a UUIDv7 wall-clock timestamp strictly newer
// than the hub's current wall clock (plus clockSkewMargin). Used by
// history replay to short-circuit Pass-2 full-replay when the client
// is presenting an ID that claims to be from the future — typical
// causes: a browser cached an ID from a different environment, a
// client clock running fast, a load-test harness manufactured a
// UUIDv7 from `now()` against an older hub.
//
// Compares against `nowMs` (the hub's wall-clock time in ms) rather
// than the Redis stream tail. The earlier "stream tail" comparison
// was unsound: it conflated "event not yet dispatched by the
// listener" with "event not yet published," so under listener lag
// (xreadBlock > clockSkewMargin) OR publisher-side clock skew, the
// guard could drop legitimate, already-published events. Wall-clock
// comparison is independent of listener progress, so the guard now
// catches only the wall-clock-future-dated IDs it was designed for.
//
// Returns false for non-UUIDv7 IDs (custom Last-Event-ID values),
// for EarliestLastEventID, for malformed inputs, and for any case
// where the comparison cannot be performed — the pre-existing
// two-pass replay path is the safe default for those.
func isRequestedIDFutureDated(requestedEventID string, nowMs int64, clockSkewMs uint64) bool {
	if requestedEventID == "" || requestedEventID == mercure.EarliestLastEventID {
		return false
	}

	// Strict UUIDv7 version check: uuidv7ToStreamID itself doesn't
	// enforce version=7, so a UUIDv4 / random-bit UUID would parse
	// successfully but the "timestamp" bits would be garbage. Without
	// this check, a v4 Last-Event-ID could randomly evaluate as
	// past-or-future relative to wall clock. For non-v7 inputs we fall
	// through to the legacy two-pass replay path — the conservative
	// choice.
	if !isUUIDv7(requestedEventID) {
		return false
	}

	_, requestedMs, err := uuidv7ToStreamID(requestedEventID, 0)
	if err != nil {
		return false
	}

	// Compare via subtraction rather than addition: nowMs + clockSkewMs
	// could overflow int64 if nowMs is adversarially large, whereas
	// subtracting two non-negative int64 timestamps stays in-range.
	// requestedMs and clockSkewMs come from uint64; clamp to
	// math.MaxInt64 to keep the signed math safe.
	const maxInt64 = uint64(math.MaxInt64)

	if requestedMs > maxInt64 || clockSkewMs > maxInt64 {
		return false
	}

	return int64(requestedMs)-nowMs > int64(clockSkewMs)
}

// isUUIDv7 reports whether eventID is a syntactically-valid UUID whose
// version nibble (4 high bits of byte 6, per RFC 9562 §5.7) is 7. Strips
// the urn:uuid: prefix to match Mercure's standard event-ID format.
func isUUIDv7(eventID string) bool {
	raw := strings.TrimPrefix(eventID, "urn:uuid:")

	u, err := uuid.FromString(raw)
	if err != nil {
		return false
	}

	return u[6]>>4 == 7
}

// streamIDMilliseconds extracts the millisecond timestamp prefix from a
// Redis stream ID (<ms>-<seq>). Returns false on malformed input
// (no dash, empty prefix, or non-numeric prefix).
func streamIDMilliseconds(streamID string) (int64, bool) {
	prefix, _, ok := strings.Cut(streamID, "-")
	if !ok || prefix == "" {
		return 0, false
	}

	ms, err := strconv.ParseInt(prefix, 10, 64)
	if err != nil {
		return 0, false
	}

	return ms, true
}

// sampleConsumerLag updates consumer_lag and consumer_pending gauges from
// XINFO GROUPS for this node's consumer group. Stream-not-exist resets
// gauges to 0 and logs at Info; ctx-cancel during shutdown holds the last
// value silently; any other XINFO GROUPS failure logs at Warn (matching
// streamLength's precedent — silent staleness on a Prometheus gauge would
// otherwise let "is the hub keeping up?" alerts lie indefinitely). m must
// be non-nil — caller (healthCheck) takes the t.metrics.Load() guard so
// this function can do straight-line gauge writes without re-checking.
func (t *RedisTransport) sampleConsumerLag(ctx context.Context, m *Metrics) {
	groups, err := t.client.XInfoGroups(ctx, t.key("")).Result()
	if err != nil {
		switch {
		case isStreamNotExist(err):
			// Stream never created yet — pre-publish state. Reset gauges to
			// 0/0 so dashboards show "fresh hub" rather than the last sample
			// from a previous lifecycle. Surface the reset as an Info log so
			// "stream vanished after running" doesn't look identical to
			// "everything caught up" on dashboards.
			m.consumerLag.Set(0)
			m.consumerPending.Set(0)
			m.consumerGroupsCount.Set(0)
			t.logger.Info("redis transport: consumer-group lag sample skipped; stream not present",
				"group", t.nodeGroup)
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			// Shutdown — hold last value.
		default:
			// Warn (not Debug) to match streamLength's precedent: XINFO GROUPS
			// can fail on ACL/permission changes while PING still succeeds;
			// silent staleness on a Prometheus gauge would make "is the hub
			// keeping up?" alerts lie indefinitely. Sentinel -1 distinguishes
			// "scrape gap" (series interpolates) from "we tried and failed"
			// (visible step down to -1 on the dashboard).
			m.consumerGroupsCount.Set(-1)
			t.logger.Warn("redis transport: failed to sample consumer-group lag",
				"error", err, "group", t.nodeGroup)
		}

		return
	}

	// consumer_groups: total groups on the stream, before the per-node
	// lag/pending narrowing below. Includes this node's own group, peer
	// groups, and any zombies not yet GC'd. The intended-fleet alert
	// compares this gauge to the operator's expected replica count;
	// publish-rate-non-zero AND this gauge = 0 catches the
	// "publisher writing to a stream nobody reads" misconfiguration.
	m.consumerGroupsCount.Set(float64(len(groups)))

	for _, g := range groups {
		if g.Name != t.nodeGroup {
			continue
		}

		// XINFO GROUPS may return Lag < 0 when Redis cannot compute it
		// deterministically (entries deleted from the middle of the stream
		// via XDEL, or pre-Redis-7.0 server). Surface the sentinel rather
		// than coercing to 0 so operators can spot the un-knowable case.
		lag, pending := consumerGroupGaugeValues(g)
		m.consumerLag.Set(lag)
		m.consumerPending.Set(pending)

		return
	}

	// Group does not (yet) exist on the stream — uninitialized lifecycle.
	m.consumerLag.Set(0)
	m.consumerPending.Set(0)
}

// consumerGroupGaugeValues maps the lag/pending fields of redis.XInfoGroup
// onto the consumer_lag / consumer_pending gauge values, in that order.
// Extracted from sampleConsumerLag so the field-mapping contract can be
// unit-tested with synthesized inputs — directly exercising it in a live
// transport requires controlling Redis lag/pending state, which races
// with the package's xreadgroupListener and goroutine teardown.
func consumerGroupGaugeValues(g redis.XInfoGroup) (lag, pending float64) {
	return float64(g.Lag), float64(g.Pending)
}

// jitterStart returns a deterministic per-node start offset in [0, d) used to
// desynchronize fleet-wide periodic maintenance so every hub doesn't hit the
// shared Redis primary in lockstep (presence write, TTL trim, zombie-group GC).
// Derived from the node ID (stable + unique per hub) via a 64-bit FNV hash, so
// it needs no PRNG (no weak-RNG concern) and is deterministic for tests. A
// 64-bit sum is used so the modulo spreads across the FULL interval: a 32-bit
// sum maxes at ~4.295s (its uint32 ns ceiling), which would silently collapse
// the offset to [0, ~4.295s) for the 30s/5m maintenance intervals and defeat
// the desync. Health PING is left unjittered so readiness stays prompt.
func (t *RedisTransport) jitterStart(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}

	h := fnv.New64a()
	_, _ = h.Write([]byte(t.nodeID))

	// Mask off the sign bit so the hash is always a valid non-negative int64,
	// then reduce modulo the (positive) interval. d > 0 here, so int64(d) is safe.
	return time.Duration(int64(h.Sum64()&math.MaxInt64) % int64(d))
}

// ttlCleanup periodically trims stream entries older than eventTTL.
func (t *RedisTransport) ttlCleanup(ctx context.Context) {
	defer t.wg.Done()

	// Desync fleet-wide cleanup off the shared primary.
	select {
	case <-ctx.Done():
		t.logger.Debug("redis transport: TTL cleanup cancelled before first tick (shutdown)")

		return
	case <-time.After(t.jitterStart(t.opts.cleanupInterval)):
	}

	ticker := time.NewTicker(t.opts.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			t.trimByTTL(ctx)
		}
	}
}

// trimByTTL runs a single XTRIM MINID pass trimming entries older than eventTTL.
// Extracted from ttlCleanup so it can be invoked directly by integration tests
// without waiting for the cleanup ticker.
func (t *RedisTransport) trimByTTL(ctx context.Context) {
	minID := strconv.FormatInt(time.Now().Add(-t.opts.eventTTL).UnixMilli(), 10) + "-0"

	if err := t.cleanupScript.Run(
		ctx, t.client,
		[]string{t.key("")},
		minID,
	).Err(); err != nil {
		if isShutdownCancel(err) {
			return // graceful shutdown cancelled the cleanup pass — not a failure
		}

		t.metricsOrCountDrop().recordTTLCleanupError()
		t.logger.Error("redis transport: TTL cleanup failed", "error", err)
	}
}

// warnSuspiciousClientOptions surfaces legal-but-likely-misconfigured option
// values that depend on the go-redis client (and therefore can't be checked
// at the options-only validation layer).
//
// Currently only checks WithHistoryReplayConcurrency vs the configured
// go-redis pool size: if concurrency reaches OR exceeds pool size,
// reconnect storms can exhaust the pool and starve XREADGROUP / XADD /
// presence operations. The predicate is `>=` (not `>`) because at exact
// equality every replay slot consumes a connection and other ops have
// zero headroom — head-of-line blocking begins immediately.
//
// Default historyReplayConcurrency is 20; default go-redis pool is
// 10 × runtime.GOMAXPROCS, so the warning fires on machines with ≤2
// usable cores when neither knob is tuned — exactly the case where an
// operator hasn't looked at either and would benefit from the heads-up.
//
// Returns silently for unknown client types. The covered set is
// *redis.Client (single-instance and Sentinel-via-failover),
// *redis.ClusterClient, and *redis.Ring (client-side consistent-hashed
// shards). Other concrete types yield 0 and skip the warning rather
// than firing against an unknown ceiling.
func warnSuspiciousClientOptions(o *options, client redis.UniversalClient, logger *slog.Logger) {
	poolSize := redisPoolSize(client)
	if poolSize <= 0 {
		return
	}

	if o.historyReplayConcurrency >= poolSize {
		logger.Warn(
			"redis transport: WithHistoryReplayConcurrency reaches or exceeds go-redis client pool size — reconnect storms can exhaust the pool, starving XREADGROUP/XADD/presence operations",
			"history_replay_concurrency", o.historyReplayConcurrency,
			"pool_size", poolSize,
			"recommended_action", "lower history_replay_concurrency or raise the redis client pool_size to leave headroom for non-replay operations",
		)
	}
}

// redisPoolSize introspects the configured go-redis pool size via type-switch.
// Returns 0 for client types whose pool size isn't reachable through the
// public API (the warning is then skipped silently rather than fired
// against an unknown ceiling).
func redisPoolSize(client redis.UniversalClient) int {
	switch c := client.(type) {
	case *redis.Client:
		if opts := c.Options(); opts != nil {
			return opts.PoolSize
		}
	case *redis.ClusterClient:
		if opts := c.Options(); opts != nil {
			return opts.PoolSize
		}
	case *redis.Ring:
		if opts := c.Options(); opts != nil {
			return opts.PoolSize
		}
	}

	return 0
}
