package redistransport

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/dunglas/mercure"
)

// shardChannelBufferSize sets the capacity of each shard's work channel.
// Small enough to bound memory under backpressure; large enough to absorb
// brief dispatch bursts without blocking the XREADGROUP goroutine.
const shardChannelBufferSize = 64

// dispatchShard holds a per-shard topic index (the live-dispatch matcher) plus
// the skipfilter SubscriberList (retained for Walk / presence / Len, which the
// index does not serve). Each shard worker goroutine exclusively reads its own
// shard during dispatch; Add/Remove are protected by the transport-level mu.
//
// candScratch and seenScratch are dispatch-path scratch buffers reused across
// work items. They are touched ONLY by this shard's dispatch goroutine (the
// shard worker when sharded, or the listener goroutine on the single-shard
// path) — never by Add/Remove — so they need no synchronization of their own.
// Their capacity is a high-water mark: a single large fan-out grows them and
// they do not shrink. The candScratch tail beyond the current match length may
// also pin now-removed *LocalSubscriber pointers until a later, equal-or-larger
// match overwrites those slots — bounded by the largest single fan-out, not a
// leak. Zeroing the tail every dispatch would cost O(cap) and defeat the
// zero-allocation goal, so it is deliberately not done.
//
// Pre-resolved labeled children of dispatchDuration and shardSubscribers live
// on Metrics (Metrics.shardDispatchObs / Metrics.shardSubGauges) rather than
// here, populated at registration time by Metrics.resolveShardChildren. Hot
// paths look them up via t.metrics.Load(); when metrics are not yet bound
// (Caddy late-binding window) the dispatch path simply skips the observation.
type dispatchShard struct {
	index       *topicIndex
	subscribers *mercure.SubscriberList
	candScratch []*mercure.LocalSubscriber
	seenScratch map[*mercure.LocalSubscriber]struct{}
}

// match returns the subscribers matching u via this shard's topic index,
// reusing the shard's scratch buffers. The returned slice aliases candScratch
// and is valid only until the next match call on this shard — safe because the
// single dispatch goroutine consumes it fully (synchronous fan-out) before
// matching the next update.
func (ds *dispatchShard) match(u *mercure.Update) []*mercure.LocalSubscriber {
	clear(ds.seenScratch)
	ds.candScratch = ds.index.matchedAppend(ds.candScratch[:0], ds.seenScratch, u)

	return ds.candScratch
}

// shardWork carries an update to a shard worker along with a WaitGroup the
// XREADGROUP goroutine waits on so fan-out completes synchronously before the
// entry is XACK'd (the cursor itself is advanced BEFORE fan-out — see
// processStreamEntry). matched accumulates per-shard match counts so the
// dispatch_subscribers_matched histogram can record one observation per message
// (the documented semantic) instead of one per shard.
type shardWork struct {
	update  *mercure.Update
	wg      *sync.WaitGroup
	matched atomic.Int64
}

// newShardWorkPool builds a sync.Pool that recycles shardWork+WaitGroup
// pairs so Dispatch's inner fan-out does not allocate per message.
// Correctness depends on shardedDispatch waiting for every wg.Done before
// Put — see the deferred wg.Wait there.
func newShardWorkPool() *sync.Pool {
	return &sync.Pool{
		New: func() any {
			return &shardWork{wg: &sync.WaitGroup{}}
		},
	}
}

// minCachePerShard is the floor for skipfilter cache entries per dispatch
// shard. Keeps individual shard caches useful even when the configured total
// would otherwise divide into tiny per-shard slices. Raising the floor above
// `subscriberListCacheSize / numShards` means the effective total cache
// exceeds the configured cap (see initShards for the Warn log that surfaces
// this to operators).
const minCachePerShard = 10_000

// initShards initializes per-shard SubscriberLists and dispatch channels.
// Must be called from NewRedisTransport before any goroutines are started.
func (t *RedisTransport) initShards() {
	numShards := t.opts.dispatchShards
	if numShards <= 0 {
		numShards = max(runtime.NumCPU(), 1)
	}

	// Hard ceiling: per-shard Prometheus series (dispatch_duration buckets +
	// shardSubscribers gauge) multiply quickly on high-vCPU hosts. The cap
	// is also what warnSuspiciousOptions documents — making it enforced
	// closes the gap where auto-detect bypassed the soft warn entirely.
	if numShards > suspiciousDispatchShardsMax {
		t.logger.Warn(
			"redis transport: WithDispatchShards capped to enforced ceiling",
			"requested", numShards,
			"ceiling", suspiciousDispatchShardsMax,
		)

		numShards = suspiciousDispatchShardsMax
	}

	t.numShards = numShards

	cacheSize := t.opts.subscriberListCacheSize
	cachePerShard := cacheSize / numShards

	if cachePerShard < minCachePerShard {
		effectiveTotal := numShards * minCachePerShard
		t.logger.Warn(
			"redis transport: per-shard cache floor raised effective subscriber list cache above configured size",
			"configured_cache_size", cacheSize,
			"dispatch_shards", numShards,
			"per_shard_floor", minCachePerShard,
			"effective_total_cache_size", effectiveTotal,
			"remediation", "raise WithSubscriberListCacheSize (to at least shards*floor) or lower WithDispatchShards to honor the configured cap",
		)

		cachePerShard = minCachePerShard
	}

	t.shards = make([]dispatchShard, numShards)
	for i := range numShards {
		t.shards[i].index = newTopicIndex()
		t.shards[i].subscribers = mercure.NewSubscriberList(cachePerShard)
		t.shards[i].seenScratch = make(map[*mercure.LocalSubscriber]struct{})
	}

	if numShards > 1 {
		t.shardChans = make([]chan *shardWork, numShards)
		for i := range numShards {
			t.shardChans[i] = make(chan *shardWork, shardChannelBufferSize)
		}

		t.shardWorkPool = newShardWorkPool()
	}
}

// shardFor returns the shard index for a given subscriber ID.
// Uses xxhash for fast, well-distributed hashing of UUIDv4 URNs.
func (t *RedisTransport) shardFor(subscriberID string) int {
	return int(xxhash.Sum64String(subscriberID) % uint64(t.numShards)) //nolint:gosec // numShards is always positive
}

// shardedDispatch broadcasts an update to all dispatch shard workers and waits
// for all of them to complete before returning. It is synchronous so the caller
// (processStreamEntry) can XACK only after every shard has delivered — the
// cursor itself is advanced BEFORE this fan-out, see processStreamEntry.
//
// On context cancel mid-fanout, shards that have already received the work
// will dispatch it and shards past the cancel index will NOT. This is only
// safe today because ctx is the transport's root context, cancelled solely
// by Close() — at which point disconnectAllSubscribers is already tearing
// down every local subscriber, so the dropped messages are moot. If this
// function is ever reused with a non-shutdown context, the skipped shards
// would silently drop messages; the Info log below makes that visible.
//
// Does NOT require t.mu; shard workers snapshot per-shard candidates under the
// topic index's RLock only. Correctness against concurrent AddSubscriber/
// RemoveSubscriber comes from processStreamEntry's advance-before-fan-out cursor
// ordering, not a mutex held across dispatch.
func (t *RedisTransport) shardedDispatch(ctx context.Context, update *mercure.Update) {
	work, ok := t.shardWorkPool.Get().(*shardWork)
	if !ok {
		// Unreachable: pool.New always returns *shardWork. Defensive path so a
		// future pool misconfiguration surfaces as a panic, not a silent drop.
		panic("redistransport: shardWorkPool yielded unexpected type")
	}

	work.update = update
	work.matched.Store(0)
	work.wg.Add(t.numShards)

	// wg.Wait before Put is required for pool safety: workers may still be
	// calling work.wg.Done after we return on the cancel path. Returning the
	// work to the pool before they finish would race wg.Add on the next Get.
	defer func() {
		work.wg.Wait()

		if m := t.metricsOrCountDrop(); m != nil {
			matched := work.matched.Load()
			m.dispatchSubscribersTotal.Observe(float64(matched))

			if matched == 0 {
				// Fleet-wide cost of fan-out without consumers. Each replica
				// reads the event from Redis and matches it against every
				// shard's topic index — matching zero is wasted I/O. Useful for
				// identifying chatty publishers whose topics nobody
				// subscribes to (e.g., presence/heartbeat traffic emitted
				// even when no debugger is connected). Increment here
				// (sharded path) and at the single-shard path below.
				m.dispatchZeroMatchTotal.Inc()
			}
		}

		work.update = nil
		t.shardWorkPool.Put(work)
	}()

	for i := range t.numShards {
		select {
		case t.shardChans[i] <- work:
		case <-ctx.Done():
			t.logger.Info("redis transport: shardedDispatch context cancelled mid-fanout",
				"shards_notified", i, "shards_skipped", t.numShards-i)

			for j := i; j < t.numShards; j++ {
				work.wg.Done()
			}

			return
		}
	}
}

// shardWorker is the per-shard dispatch goroutine. It receives updates from
// the XREADGROUP goroutine via its dedicated channel, matches them against its
// own topic index, and dispatches to matching subscribers.
//
// Per-subscriber message ordering is preserved because a subscriber is always
// assigned to the same shard, and the shard channel is FIFO.
//
// Worker lifecycle: exits when its shard channel is closed (by Close after
// the listener has stopped). On ctx.Done we stop dispatching but still drain
// the channel and call work.wg.Done on each item so shardedDispatch's
// per-message wg.Wait cannot hang on a buffered send in flight.
func (t *RedisTransport) shardWorker(ctx context.Context, id int) {
	defer t.wg.Done()

	ch := t.shardChans[id]
	shard := &t.shards[id]

	for work := range ch {
		if ctx.Err() != nil {
			// Shutting down — drain but skip dispatch so subscribers don't
			// see post-shutdown messages. Wg still has to be decremented.
			work.wg.Done()

			continue
		}

		// Snapshot metrics ONCE per work item — Caddy may have called
		// RegisterMetricsWith between two work items, but inside one we want
		// matched start/end times to come from the same Metrics instance.
		metrics := t.metricsOrCountDrop()

		var dispatchStart time.Time
		if metrics != nil {
			dispatchStart = time.Now()
		}

		// Lock-protocol note: this worker holds no t.mu. A stale candidate
		// snapshot CAN race a concurrent RemoveSubscriber that deletes s's
		// lostFlags entry between this match and the markLost below. markLost
		// still counts in that case (its no-flag branch increments) — a
		// Dispatch=false here is a genuine backpressure loss, so dropping it
		// would under-count subscribers_lost. Delivery correctness for a
		// concurrently-joining subscriber comes from processStreamEntry's
		// advance-before-fan-out cursor ordering plus the index snapshot's
		// RLock, not a lock held across dispatch.
		subscribers := shard.match(work.update)

		if hp := t.afterMatchHookForTests.Load(); hp != nil {
			(*hp)(id, work.update)
		}

		for _, sub := range subscribers {
			// Dispatch returning false signals handleFullChan disconnected
			// a slow consumer (the dominant case) or another path
			// concurrently disconnected the subscriber. markLost CAS-gates
			// to at-most-one increment per subscriber across racing paths.
			if !sub.Dispatch(ctx, work.update, false) {
				t.markLost(sub, lossReasonBackpressure)
			}
		}

		// Always accumulate so shardedDispatch can record one
		// dispatch_subscribers_matched observation per message; the
		// dispatch_duration histogram remains per-shard (its label is
		// what makes it useful for spotting hot shards).
		work.matched.Add(int64(len(subscribers)))

		if metrics != nil {
			metrics.shardDispatchObs[id].Observe(time.Since(dispatchStart).Seconds())
		}

		work.wg.Done()
	}
}

// addSubscriberToList routes s into its assigned shard's subscriber list.
//
// Counter incremented BEFORE the list insert (and, in removeSubscriberFromList,
// decremented AFTER the list delete) so a lock-free presence walk that races a
// membership change sees subscriberCount >= visible list membership — it can
// over-count but never under-count. Over-counting errs toward summary mode (and
// the bounded-walk count_race fallback); under-counting is the dangerous
// direction that would enter detail mode for a membership above the threshold.
func (t *RedisTransport) addSubscriberToList(s *mercure.LocalSubscriber) {
	shardIdx := t.shardFor(s.ID)
	t.subscriberCount.Add(1)
	// Index BEFORE the cursor Load in AddSubscriber (this runs under t.mu, before
	// that Load): the index write (Lock) is the happens-before edge that makes
	// gap-free delivery hold now that dispatch matches via the index, not the
	// skipfilter. See AddSubscriber and topicIndex.
	t.shards[shardIdx].index.add(s)
	t.shards[shardIdx].subscribers.Add(s)

	if m := t.metricsOrCountDrop(); m != nil {
		m.shardSubGauges[shardIdx].Inc()
	}
}

// removeSubscriberFromList removes s from its assigned shard's subscriber list.
// Idempotent: lostFlags carries an entry for exactly the subscribers currently
// on a shard list (Stored beside addSubscriberToList under t.mu, Deleted by the
// remove callers), so a redundant or non-member removal is a no-op rather than
// driving subscriberCount / the shard gauge negative. mercure.SubscriberList.Remove
// gives no "actually removed" signal, so the membership oracle has to be ours.
// Must be called with t.mu held (the add+Store pairing is t.mu-atomic).
func (t *RedisTransport) removeSubscriberFromList(s *mercure.LocalSubscriber) {
	if _, member := t.lostFlags.Load(s); !member {
		return
	}

	shardIdx := t.shardFor(s.ID)
	t.shards[shardIdx].index.remove(s)
	t.shards[shardIdx].subscribers.Remove(s)
	t.subscriberCount.Add(-1)

	if m := t.metricsOrCountDrop(); m != nil {
		m.shardSubGauges[shardIdx].Dec()
	}
}

// dispatchToSubscribers dispatches an update to all matching local subscribers.
// Does NOT require t.mu: gap-free delivery to a concurrently-joining subscriber
// comes from processStreamEntry advancing the atomic cursor BEFORE this fan-out
// (a subscriber that misses the live snapshot replays the entry via history),
// not from a lock held across dispatch. The candidate snapshot is taken under
// the topic index's RLock.
func (t *RedisTransport) dispatchToSubscribers(ctx context.Context, update *mercure.Update) {
	if t.numShards > 1 {
		t.shardedDispatch(ctx, update)

		return
	}

	// Sequential dispatch (single shard / backward-compat path). Records
	// dispatch_duration_seconds{shard="0"} so default-config users (no
	// explicit dispatch_shards) still get fan-out latency observability —
	// otherwise the histogram would only fire when sharding was opted into.
	shard := &t.shards[0]

	metrics := t.metricsOrCountDrop()

	var dispatchStart time.Time
	if metrics != nil {
		dispatchStart = time.Now()
	}

	subscribers := shard.match(update)

	if hp := t.afterMatchHookForTests.Load(); hp != nil {
		(*hp)(0, update)
	}

	for _, sub := range subscribers {
		// See shardWorker for the markLost rationale on Dispatch=false.
		if !sub.Dispatch(ctx, update, false) {
			t.markLost(sub, lossReasonBackpressure)
		}
	}

	if metrics != nil {
		metrics.dispatchSubscribersTotal.Observe(float64(len(subscribers)))
		metrics.shardDispatchObs[0].Observe(time.Since(dispatchStart).Seconds())

		if len(subscribers) == 0 {
			// See shardedDispatch deferred Observe for the zero-match
			// rationale: single-shard path mirrors the same increment so
			// the metric is consistent regardless of WithDispatchShards.
			metrics.dispatchZeroMatchTotal.Inc()
		}
	}
}

// disconnectAllSubscribers disconnects all subscribers across all shards.
// Called during Close; each disconnected subscriber goes through markLost
// to enforce at-most-one subscribers_lost increment across racing paths
// (concurrent history-replay rollback or backpressure dispatch). Subscribers
// already counted by an earlier path are skipped via CAS.
func (t *RedisTransport) disconnectAllSubscribers() {
	for i := range t.numShards {
		t.shards[i].subscribers.Walk(0, func(s *mercure.LocalSubscriber) bool {
			s.Disconnect()
			t.markLost(s, lossReasonShutdown)

			return true
		})
	}
}

func (t *RedisTransport) walkAllSubscribers(fn func(s *mercure.LocalSubscriber) bool) {
	for i := range t.numShards {
		t.shards[i].subscribers.Walk(0, fn)
	}
}

// startShardWorkers launches shard worker goroutines. Only called when numShards > 1.
func (t *RedisTransport) startShardWorkers(ctx context.Context) {
	for i := range t.numShards {
		t.wg.Add(1)

		go t.shardWorker(ctx, i)
	}

	t.logger.Info("redis transport: sharded dispatch enabled",
		"shards", t.numShards)
}

func (t *RedisTransport) closeShardChannels() {
	for _, ch := range t.shardChans {
		close(ch)
	}
}
