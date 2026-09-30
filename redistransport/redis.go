package redistransport

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"math"
	"math/rand/v2"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"weak"

	"github.com/dunglas/mercure"
	"github.com/gofrs/uuid/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/time/rate"
)

// ErrClosedTransport is returned after Close by Dispatch, AddSubscriber,
// RemoveSubscriber, GetSubscribers, TryAdmit, Ready and Live, and by a publish
// or history replay that fails once Close has started. It unwraps to
// mercure.ErrClosedTransport, which the hub checks for via errors.Is.
var ErrClosedTransport error = closedTransportError{}

// closedTransportError keeps this package's message.
type closedTransportError struct{}

func (closedTransportError) Error() string {
	return "redis transport: read/write on closed transport"
}

func (closedTransportError) Unwrap() error {
	return mercure.ErrClosedTransport
}

// Redis error-string fragments matched against untyped RESP error replies.
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

// listenerStaleFloor is the minimum listener-staleness threshold for Ready():
// the gate is max(5 × xreadBlock, listenerStaleFloor), so sub-second
// xreadBlock values do not make readiness flap.
const listenerStaleFloor = 30 * time.Second

// serverType identifies the backend server family. Its values are emitted as
// the backend_type label, so renaming one breaks dashboards.
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
	// metrics is published by the first successful RegisterMetricsWith (from
	// initMetrics, or from the Caddy module after construction) and never
	// replaced. Goroutines that run before then see nil and skip emission.
	metrics atomic.Pointer[Metrics]

	// metricsMu serializes RegisterMetricsWith. Hot paths never take it.
	metricsMu sync.Mutex

	// poolCollector is the pool-stats collector this transport registers on
	// every registry it is bound to (the one it created, or the one it
	// adopted on the first call). Set once. Guarded by metricsMu.
	poolCollector *poolStatsCollector

	// startupClockDrift holds detectClockDrift's sample while metrics are
	// unbound (the Caddy module binds them after construction); the first
	// RegisterMetricsWith records it and clears it. Guarded by metricsMu.
	startupClockDrift *clockDriftSample

	// backendType is the detected backend family, set by checkVersionFromInfo
	// during NewRedisTransport before the transport is shared and read-only
	// afterwards. newMetrics stamps it as the backend_type label.
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

	// afterMatchHookForTests, when set by a test, runs in each dispatch path
	// between the candidate match and delivery, so a test can join a subscriber in
	// that window. It is nil in production.
	afterMatchHookForTests atomic.Pointer[func(shardID int, u *mercure.Update)]

	// topicMatcherStore is received from the hub via SetTopicMatcherStore.
	// Injected into deserialized cross-node subscribers in GetSubscribers.
	// Uses atomic.Pointer for thread safety: written once during hub init,
	// read concurrently by GetSubscribers/unmarshalPresenceValue.
	topicMatcherStore atomic.Pointer[mercure.TopicMatcherStore]

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

	// addSubLimiter is the admission gate's (TryAdmit) accept-rate limiter.
	// Nil when rate limiting is disabled (rate=0).
	addSubLimiter *rate.Limiter

	// admissionLeasesHeld counts provisional and live admission slots (TryAdmit).
	// It is separate from subscriberCount, which has a different lifetime.
	// TryAdmit increments it; the release closure it returns decrements it.
	admissionLeasesHeld atomic.Int64

	// publishLimiter rate-limits Dispatch calls to protect Redis and
	// downstream subscribers from a misbehaving publisher / replay job.
	// Nil when rate limiting is disabled (publisherRateLimit=0).
	publishLimiter *rate.Limiter

	// dropLogLimiter caps the rate of consumer-side drop log lines (decode
	// failures, forbidden-SSE-char drops) so a flood of malformed or tampered
	// stream entries can't drown the logs. The per-kind counters remain the
	// authoritative rate signal — only the log is sampled, never the metric.
	// Always on (unlike the opt-in limiters).
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
	// removeSubscriberFromList choke-points, both called under t.mu. Removal
	// is membership-gated (see lostFlags), so a redundant or non-member
	// removal does not decrement it. Lets presence summary mode read the count
	// in O(1) instead of walking every shard each heartbeat.
	// Close's disconnectAllSubscribers takes the subscribers it disconnects
	// out of it and out of the shard gauges, leaving them listed; no removal
	// runs after it, as RemoveSubscriber re-checks closed under t.mu.
	subscriberCount atomic.Int64

	// lostFlags maps each listed *mercure.LocalSubscriber to an *atomic.Bool,
	// the "already counted in subscribers_lost" flag markLost CASes so racing
	// loss paths count a subscriber at most once while its entry exists.
	//
	// The keys are also the shard-list membership record. Every list change
	// pairs with its lostFlags change in one t.mu section: AddSubscriber Stores
	// beside addSubscriberToList, and RemoveSubscriber Deletes after
	// removeSubscriberFromList. For a holder of t.mu, an entry therefore
	// exists exactly when the subscriber is listed. A Delete after releasing
	// t.mu would let a concurrent RemoveSubscriber find the entry and remove
	// the subscriber a second time, driving subscriberCount and the shard
	// gauge negative. Close's walk deletes no entries; they are GC'd with the
	// transport.
	lostFlags sync.Map
}

// Compile-time interface guards.
var (
	_ mercure.Transport                  = (*RedisTransport)(nil)
	_ mercure.TransportSubscribers       = (*RedisTransport)(nil)
	_ mercure.TransportTopicMatcherStore = (*RedisTransport)(nil)
	_ mercure.TransportCodec             = (*RedisTransport)(nil)
	_ mercure.TransportHealthChecker     = (*RedisTransport)(nil)
	_ mercure.Admitter                   = (*RedisTransport)(nil)
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
// The transport starts goroutines for XREADGROUP, presence heartbeats and
// health checks, plus TTL cleanup, zombie-group GC and shard workers when
// enabled.
func NewRedisTransport(client redis.UniversalClient, opts ...Option) (*RedisTransport, error) {
	o := defaultOptions()
	for _, opt := range opts {
		opt(o)
	}

	if err := validateOptions(o); err != nil {
		return nil, err
	}

	warnSuspiciousOptions(o, o.logger)
	resolveClientOptions(o, client)

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
	// initShards must run before initMetrics: RegisterMetricsWith sizes the
	// per-shard metric slices from t.numShards.
	t.initShards()

	// validateVersion must run before initMetrics: it sets t.backendType, which
	// collectors copy into their const labels when built.
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
	// Tag the parent span before the closed check so even ErrClosedTransport
	// traces show which transport failed.
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
		t.metrics.Load().recordPublishError(publishErrorEncode)

		return fmt.Errorf("redis transport: encode failed: %w", err)
	}

	// Every node's codec Unmarshal rejects an entry over this size, so the
	// update would be stored but never delivered: reject it here instead. It
	// is a client error, which the hub answers 413 and counts as
	// updates_failed{reason="validation"}, so it is not a publish error: the
	// publish_errors alert must not fire on it.
	if len(data) > maxEncodedUpdateBytes {
		return fmt.Errorf("redis transport: encoded update is %d bytes, over %d: %w",
			len(data), maxEncodedUpdateBytes, mercure.ErrCodecPayloadTooLarge)
	}

	if m := t.metrics.Load(); m != nil {
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
			t.logger.Debug(
				"redis transport: publish failed on a closed transport; returning the closed-transport error",
				"error", err,
			)

			return ErrClosedTransport
		default:
		}

		t.metrics.Load().recordPublishError(publishErrorPublish)

		return fmt.Errorf("redis transport: publish failed: %w", deadlineCutError(ctx, err))
	}

	if m := t.metrics.Load(); m != nil {
		m.publishDuration.Observe(time.Since(publishStart).Seconds())
	}

	return nil
}

// maxEncodedUpdateBytes is the largest encoded update the built-in codecs'
// Unmarshal accepts (mercure.ErrCodecPayloadTooLarge above it; the hub keeps
// the constant unexported). TestMaxEncodedUpdateBytesMatchesCodecCap pins it.
const maxEncodedUpdateBytes = 64 << 20

// ctxTimerLagWait bounds how long deadlineCutError waits for ctx's own timer
// to mark ctx done once its deadline has passed. A standard context closes Done
// microseconds after its deadline, so the bound only costs time for a broken
// context whose Done never closes; a tighter one could be exceeded by scheduler
// or GC lag under load and turn a real publish timeout back into a 500.
const ctxTimerLagWait = 50 * time.Millisecond

// deadlineCutError reports a command that the socket cut at ctx's deadline as
// that deadline. With ContextTimeoutEnabled, go-redis derives the socket
// deadline from ctx, so the command fails with a socket deadline error rather
// than ctx.Err(); wrapping context.DeadlineExceeded around it lets callers that
// classify by the deadline (the hub's publish_timeout check → 504, the health
// samples' hold-last-value branch) see it. Any other error, a timeout while ctx
// is still live (read_timeout firing inside the budget) and a cancelled ctx are
// returned unchanged.
func deadlineCutError(ctx context.Context, err error) error {
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		return err
	}

	// The socket deadline is ctx's deadline, but ctx is marked done by its own
	// timer goroutine, which can run after the read returns. Once the deadline
	// has passed, give that timer a moment so ctx.Err() (and the hub's
	// context.Cause) is set; bounded, so a Context whose Done never closes
	// cannot hang the caller.
	if dl, ok := ctx.Deadline(); ok && ctx.Err() == nil && !time.Now().Before(dl) {
		timer := time.NewTimer(ctxTimerLagWait)

		select {
		case <-ctx.Done():
		case <-timer.C:
		}

		timer.Stop()
	}

	if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return err
	}

	return fmt.Errorf("%w: %w", context.DeadlineExceeded, err)
}

// AddSubscriber implements mercure.Transport. Adds a subscriber to the local list,
// replays history if requested, then transitions to live mode.
//
// AddSubscriber applies no rate limit: WithSubscriberRateLimit is enforced by
// the admission gate (TryAdmit), which the hub runs before it. When the
// history replay fails, s stays listed and the error is returned; the caller
// removes s, as the mercure.Transport contract requires.
func (t *RedisTransport) AddSubscriber(ctx context.Context, s *mercure.LocalSubscriber) error {
	addStart := time.Now()

	defer func() {
		if m := t.metrics.Load(); m != nil {
			m.addSubscriberDuration.Observe(time.Since(addStart).Seconds())
		}
	}()

	// Tag the parent span before the closed check, as Dispatch does.
	tagTransportOnParent(ctx)

	select {
	case <-t.closed:
		return ErrClosedTransport
	default:
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

	if m := t.metrics.Load(); m != nil {
		m.subscriberAddTotal.Inc()
	}

	t.addSubscriberToList(s)

	// Track this subscriber for at-most-one subscribers_lost increment.
	// Concurrent Close (disconnectAllSubscribers) and a failed history replay
	// below both call markLost on the same flag; only the CAS winner increments.
	// A dispatch that matches s before this Store cannot backpressure it: s is
	// not Ready, and its empty pre-ready queue holds at least
	// mercure.MinSubscriberOutBuffer updates. The exception is an s already
	// disconnected before the add: its Dispatch returns false, and markLost
	// counts it as a backpressure loss. The hub never adds such a subscriber.
	flag := new(atomic.Bool)
	t.lostFlags.Store(s, flag)

	// Load the cursor after addSubscriberToList; see processStreamEntry for why
	// this ordering keeps delivery gap-free.
	toStreamID := *t.lastDispatchedStreamID.Load()
	t.mu.Unlock()

	// Release t.mu before the replay: holding it across XRANGE pagination would
	// serialize every AddSubscriber and RemoveSubscriber. Ordering still holds
	// because LocalSubscriber queues live updates until Ready is called at the
	// end of the replay.
	if s.RequestLastEventID != "" {
		return t.replayHistory(ctx, s, toStreamID)
	}

	t.readyWithoutReplay(ctx, s)

	return nil
}

// RemoveSubscriber implements mercure.Transport.
//
// subscriber_remove_total and remove_subscriber_duration_seconds count only
// the removal of a listed subscriber from an open transport, including the
// caller's removal of a subscriber whose history replay failed.
func (t *RedisTransport) RemoveSubscriber(_ context.Context, s *mercure.LocalSubscriber) error {
	removeStart := time.Now()

	select {
	case <-t.closed:
		return ErrClosedTransport
	default:
	}

	// Remove the subscriber and its lostFlags entry in one t.mu section, so two
	// concurrent RemoveSubscriber calls cannot both see it listed (see lostFlags).
	// Normal removal is not a loss, so markLost is not called.
	t.mu.Lock()

	// Re-check closed under t.mu, as AddSubscriber does: a call that passed the
	// select above but takes t.mu after Close's disconnect walk would still
	// find s listed and count a removal the walk already counted in
	// subscribers_lost{shutdown}.
	select {
	case <-t.closed:
		t.mu.Unlock()

		return ErrClosedTransport
	default:
	}

	_, listed := t.lostFlags.Load(s)
	if listed {
		t.removeSubscriberFromList(s)
		t.lostFlags.Delete(s)
	}

	t.mu.Unlock()

	if !listed {
		return nil
	}

	// One load, so the counter and the histogram cannot diverge if metrics bind
	// between them.
	if m := t.metrics.Load(); m != nil {
		m.subscriberRemoveTotal.Inc()
		m.removeSubscriberDuration.Observe(time.Since(removeStart).Seconds())
	}

	return nil
}

// closeTimeout is the hard deadline for Redis operations during Close.
// Applied uniformly regardless of caller-supplied ctx — see Close() for why.
const closeTimeout = 5 * time.Second

// Close implements mercure.Transport. Performs a graceful shutdown.
//
// Close ignores the caller's ctx: during `caddy validate`, Caddy calls it with
// a zero caddy.Context whose embedded context is nil, which go-redis would
// dereference. Bolt and Local also ignore it. Close uses its own closeTimeout
// budget instead.
//
// wg.Wait precedes disconnectAllSubscribers so no in-flight dispatch races the
// teardown. Each cleanup failure is logged and included in the joined error.
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

// SetTopicMatcherStore implements mercure.TransportTopicMatcherStore.
// Called by the hub during its init to pass the TopicMatcherStore, which
// the transport injects into cross-node subscribers during GetSubscribers.
func (t *RedisTransport) SetTopicMatcherStore(store *mercure.TopicMatcherStore) {
	t.topicMatcherStore.Store(store)
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

// RegisterMetricsWith registers the transport's collectors and a pool-stats
// collector, read at scrape time, on reg. It implements the hub caddy
// package's TransportMetricsRegisterer: Caddy gives sub-modules a context
// without a metrics registry, so the mercure module passes its own.
//
// The collectors are built on the first call, and the same objects are
// registered on each later registry, so a transport reused across a Caddy
// reload appears on the new config's registry while the old one may still
// serve. A repeat call with a bound registry, and a nil or typed-nil reg, are
// no-ops. A later call returns any registration error other than
// AlreadyRegisteredError; registering other mercure_redis_* collectors on the
// first registry is unsupported and panics.
//
// Collector identity is {transport_type, backend_type}, so transports with the
// same backend collide on a shared registry. On the first call a collector a
// sibling already registered is adopted (safeRegister), and the adopted pool
// collector reads this transport's client; on later calls such collectors are
// skipped.
//
// Call it before the first AddSubscriber or Dispatch: subscribers added before
// binding are not counted in shard_subscribers, and removing them afterwards
// drives it negative. The Caddy module binds during Provision.
func (t *RedisTransport) RegisterMetricsWith(reg prometheus.Registerer) error {
	if isEffectivelyNilRegisterer(reg) {
		// Expected during `caddy validate` and for sub-modules without a registry.
		t.logger.Debug("redis transport: RegisterMetricsWith skipped — registerer is nil or typed-nil")

		return nil
	}

	t.metricsMu.Lock()
	defer t.metricsMu.Unlock()

	current := t.metrics.Load()
	if current == nil {
		return t.bindFirstRegisterer(reg)
	}

	return t.bindAdditionalRegisterer(reg, current)
}

// TryAdmit implements mercure.Admitter, the admission gate the hub runs before
// auth and allocation. It rejects at once when the count ceiling is reached,
// then takes a rate token (fail-fast, or a bounded wait when
// subscriber_admission_timeout > 0), then claims a count slot. It is the only
// place WithSubscriberRateLimit is enforced.
//
// On success it returns ctx and an idempotent release closure; defer it
// exactly once. On rejection it returns ctx, a nil release and an
// *mercure.AdmissionError (429).
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
		t.metrics.Load().recordAdmissionRejected(admissionReasonCapacity)

		return ctx, nil, &mercure.AdmissionError{Reason: mercure.AdmissionCapacity, RetryAfter: t.admissionRetryAfter()}
	}

	// (2) Rate token: fail-fast, or a bounded wait on the caller's context.
	if !t.admitRateToken(ctx) {
		t.metrics.Load().recordAdmissionRejected(admissionReasonRate)

		return ctx, nil, &mercure.AdmissionError{Reason: mercure.AdmissionRate, RetryAfter: t.admissionRetryAfter()}
	}

	// (3) Claim a slot, re-checking the ceiling with CAS. A token taken at (2) is
	// not returned if this fails; taking the token first avoids holding a slot
	// while waiting for one.
	if !t.acquireAdmissionSlot(maxCount) {
		t.metrics.Load().recordAdmissionRejected(admissionReasonCapacity)

		return ctx, nil, &mercure.AdmissionError{Reason: mercure.AdmissionCapacity, RetryAfter: t.admissionRetryAfter()}
	}

	// The release is idempotent.
	release := sync.OnceFunc(func() { t.admissionLeasesHeld.Add(-1) })

	return ctx, release, nil
}

// bindFirstRegisterer builds the transport's collector set and pool
// collector, registers them on reg (adopting a sibling's collector on
// AlreadyRegistered, see safeRegister), and publishes the set. Caller holds
// metricsMu.
func (t *RedisTransport) bindFirstRegisterer(reg prometheus.Registerer) error {
	// Warn before registering, so the warning is logged even if registration panics.
	warnIfRegistererAlreadyBoundToDifferentStream(reg, t.opts.streamName, t.logger)

	m := newMetrics(reg, t.backendType)

	// An adopted sibling's pool collector scrapes this transport's client from
	// now on; on a fresh registration this stores the client it already has.
	pool := safeRegister(reg, newPoolStatsCollector(t.client, t.backendType))
	pool.setClient(t.client)

	// Resolve per-shard child observers AFTER newMetrics has registered
	// dispatchDuration / shardSubscribers, so the slices reference the
	// live (post-AlreadyRegistered swap) collectors.
	m.resolveShardChildren(t.numShards)

	t.poolCollector = pool
	t.metrics.Store(m)

	if d := t.startupClockDrift; d != nil {
		d.record(m)

		t.startupClockDrift = nil
	}

	return nil
}

// bindAdditionalRegisterer registers the transport's own collectors and pool
// collector on reg. It changes nothing the hot path uses: a collector reg
// already holds as the same object is left alone, and one a sibling holds
// under the same identity is skipped (not adopted, not re-pointed). Silent
// when reg already held every collector (a repeat call); otherwise runs the
// stream-binding check and logs one Info line. Caller holds metricsMu.
func (t *RedisTransport) bindAdditionalRegisterer(reg prometheus.Registerer, current *Metrics) error {
	var registered, skipped int

	for _, c := range append(current.collectors(), t.poolCollector) {
		err := reg.Register(c)
		if err == nil {
			registered++

			continue
		}

		var are prometheus.AlreadyRegisteredError
		if !errors.As(err, &are) {
			return fmt.Errorf("redis transport: failed to register collector on additional registry: %w", err)
		}

		// Every collector of this transport is a pointer, so == cannot panic.
		if are.ExistingCollector != c {
			skipped++
		}
	}

	if registered == 0 && skipped == 0 {
		return nil
	}

	warnIfRegistererAlreadyBoundToDifferentStream(reg, t.opts.streamName, t.logger)

	t.logger.Info("redis transport: metrics registered on an additional registry",
		"registered", registered,
		"skipped", skipped)

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
// Keys: a *prometheus.Registry is keyed by a weak.Pointer so the map does
// not keep a discarded registry (e.g. the one from a replaced Caddy config
// load) alive; dead entries are pruned on each access. Other comparable
// Registerer types are keyed directly. Non-comparable Registerer dynamic
// types are silently skipped.
//
//nolint:gochecknoglobals // Process-wide: cross-transport collision detection on a shared Registerer needs one shared map.
var (
	registererStreamBindings   = make(map[any]string)
	registererStreamBindingsMu sync.Mutex
)

// warnIfRegistererAlreadyBoundToDifferentStream records the (registerer,
// streamName) binding on first call and logs a Warn on later calls for the
// same registerer with a different stream name.
// Same-stream re-binding — e.g. two hub directives sharing both a
// registerer and a stream name — is silent. Non-comparable registerers are
// skipped to avoid a panic.
func warnIfRegistererAlreadyBoundToDifferentStream(reg prometheus.Registerer, streamName string, logger *slog.Logger) {
	key, ok := streamBindingKey(reg)
	if !ok {
		// Non-comparable registerer — cannot key the map without risking
		// a panic. Degraded detection: no Warn for this registerer.
		return
	}

	registererStreamBindingsMu.Lock()
	defer registererStreamBindingsMu.Unlock()

	pruneDeadStreamBindings()

	if previous, ok := registererStreamBindings[key]; ok {
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

	registererStreamBindings[key] = streamName
}

// streamBindingKey returns the registererStreamBindings key for reg: a
// weak.Pointer for *prometheus.Registry, reg itself for other comparable
// dynamic types. ok is false for a non-comparable dynamic type.
func streamBindingKey(reg prometheus.Registerer) (key any, ok bool) {
	if r, isRegistry := reg.(*prometheus.Registry); isRegistry {
		return weak.Make(r), true
	}

	regType := reflect.TypeOf(reg)
	if regType == nil || !regType.Comparable() {
		return nil, false
	}

	return reg, true
}

// pruneDeadStreamBindings deletes entries whose weakly-held registry has
// been garbage-collected. Caller holds registererStreamBindingsMu.
func pruneDeadStreamBindings() {
	for key := range registererStreamBindings {
		if wp, isWeak := key.(weak.Pointer[prometheus.Registry]); isWeak && wp.Value() == nil {
			delete(registererStreamBindings, key)
		}
	}
}

// readyWithoutReplay completes AddSubscriber for a subscriber that requested
// no event to replay from.
func (t *RedisTransport) readyWithoutReplay(ctx context.Context, s *mercure.LocalSubscriber) {
	// A present but empty Last-Event-ID names no event, so nothing is replayed
	// (as Bolt, which finds no such event); the subscribe handler still blocks
	// on the response ID before sending headers.
	if s.RequestLastEventIDSet {
		s.HistoryDispatched(mercure.EarliestLastEventID)
	}

	s.Ready(ctx)
}

// replayHistory runs AddSubscriber's history replay for s. When the replay
// fails, s is counted in subscribers_lost (once, whichever of this and
// Close's walk wins markLost's CAS) and stays listed: the caller removes it,
// as the mercure.Transport contract requires, and that removal counts in
// subscriber_remove_total.
//
// A replay that fails once Close has started (Close closes the client under
// it) returns the closed-transport sentinel, as Dispatch does, and the replay
// error is logged at Debug.
func (t *RedisTransport) replayHistory(ctx context.Context, s *mercure.LocalSubscriber, toStreamID string) error {
	replayCtx, cancel := t.registrationContext(ctx)
	defer cancel()

	err := t.dispatchHistory(replayCtx, s, toStreamID)
	if err == nil {
		return nil
	}

	t.markLost(s, lossReasonHistoryReplayFailed)

	select {
	case <-t.closed:
		t.logger.Debug(
			"redis transport: history replay failed on a closed transport; returning the closed-transport error",
			"error", err,
			"subscriber", s.ID,
			"requested_event_id", truncateEventIDForObservability(s.RequestLastEventID),
		)

		return ErrClosedTransport
	default:
		return err
	}
}

// markLost increments subscribers_lost{reason=r} for s. The lostFlags map
// (populated in AddSubscriber) is the per-subscriber dedup token: when present,
// only the CAS winner among the racing loss paths (backpressure during live
// dispatch, AddSubscriber's failed history replay, and Close's
// disconnectAllSubscribers Walk) counts s.
//
// When the flag is absent the loss is still counted: dispatch holds no t.mu,
// so a shard worker can reach markLost with a stale candidate after
// RemoveSubscriber deleted the flag, and Dispatch=false there is a real
// backpressure loss. The cost is a rare double count of a subscriber already
// counted under another reason.
//
// While metrics are unbound nothing is counted, but the CAS still runs, so the
// loss is not counted once metrics are bound either.
func (t *RedisTransport) markLost(s *mercure.LocalSubscriber, reason subscriberLossReason) {
	if v, ok := t.lostFlags.Load(s); ok && !v.(*atomic.Bool).CompareAndSwap(false, true) {
		return
	}

	if m := t.metrics.Load(); m != nil {
		m.lostCounter(reason).Inc()
	}
}

// Admission rejection reasons — the only label on the admission_rejected metric.
// The strings match mercure.AdmissionReason.String() for cross-referencing.
const (
	admissionReasonRate     = "rate"
	admissionReasonCapacity = "capacity"
)

// registrationContext bounds post-admission registration (history replay) under
// subscriber_registration_timeout so a slow/stuck backend can't pin an admission
// slot indefinitely. 0 (default) leaves it unbounded. The returned cancel is
// always safe to defer.
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
	var counter prometheus.Counter
	if m := t.metrics.Load(); m != nil {
		counter = m.publisherRateLimited
	}

	return waitForLimiter(ctx, t.publishLimiter, counter, "redis transport: publish rate limited")
}

// waitForLimiter is the token-bucket wait Dispatch uses. limiter may be nil
// (no-op). counter is a nil-safe optional counter incremented after a real
// delay (post-Wait); cancelled or deadline-exceeded calls are not counted as
// delayed since no wait actually occurred. errPrefix wraps ctx.Err().
//
// limiter.Wait fails at once, without taking a token, when ctx's deadline is
// too close to get one.
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

// initMetrics binds the WithPrometheusRegisterer registry, if any, through
// RegisterMetricsWith. A registration error panics.
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
			// Warn: backend_type stays "unknown" for this transport's lifetime.
			t.logger.Warn("redis transport: INFO query failed — version check skipped per withSkipVersionCheck; backend_type label will remain 'unknown' until restart", "error", err)

			return nil
		}

		return fmt.Errorf("redis transport: INFO server query failed, cannot verify the server version: %w", err)
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
		t.recordClockDrift(clockDriftSample{})

		return
	}

	drift := time.Since(redisTime).Abs()
	t.recordClockDrift(clockDriftSample{drift: drift, known: true})

	if drift > time.Second {
		t.logger.Warn(
			"redis transport: clock drift between app server and stream server",
			"drift_ms", drift.Milliseconds(),
			"threshold_ms", 1000,
		)
	}
}

// clockDriftSample is one clock-drift measurement; known is false when the
// server TIME query failed.
type clockDriftSample struct {
	drift time.Duration
	known bool
}

// record sets the clock-drift gauge from s: the drift, or NaN when unknown.
func (s clockDriftSample) record(m *Metrics) {
	if s.known {
		m.observeClockDrift(s.drift)

		return
	}

	m.markClockDriftUnknown()
}

// recordClockDrift records the startup clock-drift sample. Metrics bound at
// construction (WithPrometheusRegisterer) get it at once. Otherwise the first
// RegisterMetricsWith records it: the Caddy module binds metrics only after
// constructing the transport, so this startup emission is expected before
// binding and is deferred rather than lost.
func (t *RedisTransport) recordClockDrift(s clockDriftSample) {
	t.metricsMu.Lock()
	defer t.metricsMu.Unlock()

	if m := t.metrics.Load(); m != nil {
		s.record(m)

		return
	}

	t.startupClockDrift = &s
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
			t.metrics.Load().recordZombieGCError()
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
			t.metrics.Load().recordZombieGCError()

			continue
		}

		if exists == 0 {
			if err := t.client.XGroupDestroy(ctx, streamKey, g.Name).Err(); err != nil {
				if isShutdownCancel(err) {
					return // graceful shutdown cancelled the GC pass — not a failure
				}

				t.logger.Warn("redis transport: failed to destroy zombie consumer group",
					"group", g.Name, "error", err)
				t.metrics.Load().recordZombieGCError()
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
// On XRANGE failure mid-replay it returns an error; the hub then removes the
// subscriber, and the client reconnects with the last ID it received.
//
// Concurrency is bounded by historyReplaySem to prevent reconnect storms from
// exhausting the Redis connection pool with concurrent XRANGE operations.
func (t *RedisTransport) dispatchHistory(ctx context.Context, s *mercure.LocalSubscriber, toStreamID string) (err error) {
	// Like Bolt's history replay, this records a mercure.transport.history span,
	// set to Error on any non-nil return through the named err.
	ctx, span := startSpan(ctx, "mercure.transport.history",
		trace.WithAttributes(
			attribute.String("mercure.transport", "redis"),
			attribute.String("mercure.subscriber.id", s.ID),
			// Client-supplied; truncated (see maxObservedEventIDLen).
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

	if m := t.metrics.Load(); m != nil {
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
// Values within the byte cap are returned as is; longer ones are cut at a rune
// boundary without converting the whole string to []rune.
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

	requestedEventID string
	seekStreamID     string
	found            bool
	// matched is set when Pass 1 finds the requested event ID in the stream.
	// found is also true for an `earliest` request and a future-dated ID,
	// which name no event in the stream.
	matched        bool
	skippedEntries int

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

// run executes the two-pass history replay. If an XRANGE fails it returns the
// error without calling HistoryDispatched or Ready; the hub then removes the
// subscriber.
//
// For the normal and subscriber-disconnected paths, HistoryDispatched (with
// responseLastEventID) and Ready fire once at the end of run(). This is the
// single authoritative place the signal is emitted.
func (rs *historyReplayState) run(ctx context.Context) error {
	// A requested UUIDv7 dated after now + clockSkewMargin names no event in the
	// stream, and Pass 2 would replay all retained history, so skip both passes;
	// the subscriber already receives live events. Fall through so
	// HistoryDispatched and Ready stay in one place (the response ID is
	// "earliest").
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
		rs.t.metrics.Load().recordHistoryReplayFullScan()
	}

	if rs.skippedEntries > 0 {
		rs.t.logger.Warn("redis transport: history replay skipped undeliverable entries (decode failure, or an id/type that is not a valid SSE field value; breakdown in mercure_redis_stream_decode_errors_total{kind})",
			"count", rs.skippedEntries, "subscriber", rs.s.ID)
	}

	rs.s.HistoryDispatched(rs.responseLastEventID())
	rs.s.Ready(ctx)

	return nil
}

// responseLastEventID returns the Mercure-Last-Event-ID answer: the ID of the
// event preceding the first event sent, or `earliest` when there is none
// (spec, "Reconnection, State Reconciliation, and Event Sourcing"), as the
// Bolt transport answers. When Pass 1 found the requested ID, replay sent the
// events after it, so the answer is that ID, whatever those events' topics.
// When `earliest` was requested, or the requested ID is unknown, trimmed (Pass
// 2 replays from the stream's first entry) or future-dated, the answer is
// `earliest`.
func (rs *historyReplayState) responseLastEventID() string {
	if rs.matched {
		return rs.requestedEventID
	}

	return mercure.EarliestLastEventID
}

// dispatchEntry decodes and dispatches a single history entry.
// Returns false if the subscriber disconnected or buffer is full.
func (rs *historyReplayState) dispatchEntry(ctx context.Context, entry redis.XMessage) bool {
	update := decodeStreamEntry(entry, *rs.t.codec.Load(), rs.t.logger, rs.t.metrics.Load(), rs.t.dropLogLimiter)
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
			// Dispatch returns false for any disconnect or a full buffer. The replay is
			// incomplete, so mark it truncated; the span stays OK because a departed
			// subscriber is not a transport error.
			rs.truncated = true
			rs.t.metrics.Load().recordHistoryReplayTruncated()
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
	// with the Redis stream, could place an entry whose id or type is not a
	// valid SSE field value (mercure.ValidSSEFieldValue: invalid UTF-8, a C0 or
	// C1 control character such as CR/LF/NUL, or a Unicode format character).
	// Written verbatim, it could forge SSE frame boundaries into a subscriber's
	// stream (CWE-93). Guard only the fields written verbatim to the
	// wire (id, type) via ValidateSSEFields — NOT the full Validate(), which
	// rejects the reserved topics that hub-internal subscription events
	// legitimately travel on.
	if err := update.ValidateSSEFields(); err != nil {
		if allowLog(dropLogLimiter) {
			// Log only the safe Redis stream ID — consistent with the data_type /
			// unmarshal drops above. The offending id/type value is NOT logged (it
			// carries the forbidden characters), and neither are the update's
			// topics: on this tampered-entry path they are unvalidated attacker
			// data. An operator can XRANGE entry.ID to inspect the full entry out
			// of band.
			logger.Error(
				"redis transport: dropping stream entry whose id/type is not a valid SSE field value (invalid UTF-8, a control character or a Unicode format character)",
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
				rs.matched = true
			}

			return true
		}

		return rs.dispatchEntry(ctx, entry)
	})
}

// replayAll performs Pass 2: replay ALL entries as best-effort (event ID was trimmed).
// Same return-value contract as scanForEventID.
//
// It starts at "-" so the whole retained stream is replayed.
func (rs *historyReplayState) replayAll(ctx context.Context) (bool, error) {
	return rs.paginate(ctx, "pass 2", "-", func(entry redis.XMessage) bool {
		return rs.dispatchEntry(ctx, entry)
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

// xreadgroupListener is the main dispatch goroutine. It reads messages from the
// Redis Stream via XREADGROUP and dispatches them to matching local subscribers.
//
//nolint:funlen // One read-classify-dispatch cycle; splitting it would scatter the startID, activity-stamp and consecutiveErrors state.
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
			if m := t.metrics.Load(); m != nil {
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

	t.metrics.Load().recordXReadgroupError(xreadgroupErrorOther)
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
	t.metrics.Load().recordXReadgroupError(xreadgroupErrorNoGroup)
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

		t.metrics.Load().recordXReadgroupError(xreadgroupErrorNoGroupRecreateFailed)
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
		if m := t.metrics.Load(); m != nil {
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

		t.metrics.Load().recordXReadgroupError(xreadgroupErrorXAck)
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
// Cursor protocol: lastDispatchedStreamID advances to this entry before the
// fan-out, and no lock is held across it. A subscriber joining concurrently
// either is in the shard index when the candidates are snapshotted and gets the
// entry live, or joined later, in which case the cursor AddSubscriber loads
// already covers the entry (index lock plus atomic cursor), so its
// Last-Event-ID replay delivers it; without a Last-Event-ID it starts after
// the entry. A Last-Event-ID subscriber can get the boundary entry twice,
// which the at-least-once contract allows.
//
// The fan-out is synchronous and the caller acknowledges only after it returns,
// so an entry is never acknowledged, or used as a group-recreate resume point,
// while a shard still owes its delivery.
func (t *RedisTransport) processStreamEntry(ctx context.Context, entry redis.XMessage) {
	// PEL dedup: skip entries already dispatched. This goroutine is the sole
	// writer, so a plain load of its own cursor is sufficient here.
	if !compareStreamIDs(entry.ID, *t.lastDispatchedStreamID.Load()) {
		return
	}

	id := entry.ID

	update := decodeStreamEntry(entry, *t.codec.Load(), t.logger, t.metrics.Load(), t.dropLogLimiter)
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
	// Clamp the deadline to [minHealthPingTimeout, healthPingTimeout], never above
	// the interval. The one deadline covers the PING and the samples below, so a
	// hung connection cannot stall the loop after PING succeeds.
	healthTimeout := max(min(t.opts.healthInterval, healthPingTimeout), minHealthPingTimeout)

	healthCtx, cancel := context.WithTimeout(ctx, healthTimeout)
	defer cancel()

	if err := t.client.Ping(healthCtx).Err(); err != nil {
		// Graceful shutdown cancelled the parent ctx mid-PING — not a health
		// failure. Don't count it or log at ERROR; the loop exits on ctx.Done().
		// A real PING stall at healthCtx's budget surfaces as DeadlineExceeded
		// or a socket deadline error; both count below.
		if isShutdownCancel(err) {
			return failures
		}

		failures++
		t.logger.Error("redis transport: health check PING failed",
			"error", err, "consecutive_failures", failures)

		if failures >= t.opts.healthThreshold {
			t.healthy.Store(false)

			if m := t.metrics.Load(); m != nil {
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

	if m := t.metrics.Load(); m != nil {
		m.healthy.Set(1)

		// Sample stream length during successful health check.
		length, err := t.client.XLen(healthCtx, t.key("")).Result()
		err = deadlineCutError(healthCtx, err)

		switch {
		case err == nil:
			m.streamLength.Set(float64(length))
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			// Shutdown cancellation or the shared health budget expiring (after
			// a slow PING, or cutting XLEN itself) — the gauge holds its last
			// value, acceptable for a stale-but-not-failing best-effort sample.
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

		// Sample the available history window.
		t.sampleHistoryWindow(healthCtx, m)
	}

	return 0
}

// sampleHistoryWindow computes the elapsed wall-clock seconds between the
// stream's oldest and newest entry IDs (Redis-stamped `<ms>-<seq>`).
// Cheap: two XRANGE-by-N calls (head and tail) — no full scan. Reports 0 on
// empty stream, -1 on an XRANGE error, which it also logs at Warn so
// retention-tight alerts do not stay silent on staleness. ctx is the health
// cycle's budget: when it expires or is cancelled, the gauge holds its value.
func (t *RedisTransport) sampleHistoryWindow(ctx context.Context, m *Metrics) {
	streamKey := t.key("")

	head, err := t.client.XRangeN(ctx, streamKey, "-", "+", 1).Result()
	if err != nil {
		err = deadlineCutError(ctx, err)

		switch {
		case isStreamNotExist(err):
			m.historyWindowSeconds.Set(0)
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			// Shutdown or the health budget expiring — hold last value.
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
		err = deadlineCutError(ctx, err)

		switch {
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			// Shutdown or the health budget expiring — hold last value.
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

	// A stream ID's millisecond part is Redis's XADD timestamp, unaffected by
	// publisher clock drift.
	headMs, _ := streamIDMilliseconds(head[0].ID)
	tailMs, _ := streamIDMilliseconds(tail[0].ID)

	m.historyWindowSeconds.Set(float64(tailMs-headMs) / 1000.0)
}

// isRequestedIDFutureDated reports whether requestedEventID is a UUIDv7 whose
// timestamp is more than clockSkewMs after nowMs, the hub's wall clock.
// Comparing with the wall clock rather than the stream tail keeps listener lag
// from discarding events that were already published. It returns false for
// non-UUIDv7, "earliest" and malformed IDs, which take the two-pass replay.
func isRequestedIDFutureDated(requestedEventID string, nowMs int64, clockSkewMs uint64) bool {
	if requestedEventID == "" || requestedEventID == mercure.EarliestLastEventID {
		return false
	}

	// uuidv7ToStreamID does not check the version, and a non-v7 UUID's timestamp
	// bits are random, so require version 7.
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

// sampleConsumerLag sets consumer_lag, consumer_pending and consumer_groups
// from XINFO GROUPS. m must be non-nil.
func (t *RedisTransport) sampleConsumerLag(ctx context.Context, m *Metrics) {
	groups, err := t.client.XInfoGroups(ctx, t.key("")).Result()
	if err != nil {
		err = deadlineCutError(ctx, err)

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
			// Shutdown or the health budget expiring — hold last value.
		default:
			// XINFO GROUPS can fail on ACL changes while PING succeeds; -1 shows the
			// failure instead of leaving a stale value.
			m.consumerGroupsCount.Set(-1)
			t.logger.Warn("redis transport: failed to sample consumer-group lag",
				"error", err, "group", t.nodeGroup)
		}

		return
	}

	// All groups on the stream: this node's, its peers' and zombies not yet removed.
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

// consumerGroupGaugeValues returns g's lag and pending counts as gauge values.
func consumerGroupGaugeValues(g redis.XInfoGroup) (lag, pending float64) {
	return float64(g.Lag), float64(g.Pending)
}

// jitterStart returns a per-node offset in [0, d), from an FNV-64 hash of the
// node ID, so periodic maintenance (presence write, TTL trim, zombie GC) does
// not hit Redis in lockstep across the fleet. The hash must be 64-bit: a
// 32-bit value cannot exceed ~4.3s as a Duration. Health PINGs are not
// jittered.
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

		t.metrics.Load().recordTTLCleanupError()
		t.logger.Error("redis transport: TTL cleanup failed", "error", err)
	}
}

// warnSuspiciousClientOptions surfaces legal-but-likely-misconfigured option
// values that depend on the go-redis client (and therefore can't be checked
// at the options-only validation layer).
//
// It warns when an explicit WithHistoryReplayConcurrency is at or above the
// go-redis pool size, which leaves no connection for XREADGROUP, XADD and
// presence during reconnect storms. The derived default is not checked.
// Client types whose pool size is unknown (see redisPoolSize) are skipped.
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

// resolveClientOptions warns about an explicit option that does not fit the
// client, then derives the client-dependent defaults: the warning must not
// see a derived default, which is chosen to fit.
func resolveClientOptions(o *options, client redis.UniversalClient) {
	warnSuspiciousClientOptions(o, client, o.logger)

	if o.historyReplayConcurrency <= 0 {
		o.historyReplayConcurrency = defaultHistoryReplayConcurrency(client)
	}
}

// maxDefaultHistoryReplayConcurrency caps the derived default.
const maxDefaultHistoryReplayConcurrency = 20

// defaultHistoryReplayConcurrency is the history_replay_concurrency used when
// none is set: half the client's pool size (the go-redis default is 10 ×
// GOMAXPROCS, 5 × for Cluster), at least 1 and at most
// maxDefaultHistoryReplayConcurrency. Below the cap, replays take at most half
// the pool, leaving the rest to XREADGROUP, XADD and presence; a pool of 1
// still gets 1, with no headroom. A client whose pool size is not
// introspectable gets the cap.
func defaultHistoryReplayConcurrency(client redis.UniversalClient) int {
	poolSize := redisPoolSize(client)
	if poolSize <= 0 {
		return maxDefaultHistoryReplayConcurrency
	}

	return min(max(poolSize/2, 1), maxDefaultHistoryReplayConcurrency)
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
