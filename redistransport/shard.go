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

// dispatchShard holds a shard's topic index, used for live dispatch, and a
// SubscriberList, used only for Walk (presence and Close); the list's match
// cache is disabled. Add/Remove run under the transport's mu.
//
// candScratch and seenScratch are reused across updates and touched only by
// the shard's dispatch goroutine. They keep their high-water capacity, and the
// candScratch tail may hold removed subscribers until overwritten; clearing it
// on every dispatch would cost O(cap).
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

// shardWork carries an update to the shard workers. The listener waits on wg
// so fan-out finishes before the entry is acknowledged (the cursor advances
// before fan-out; see processStreamEntry). matched sums per-shard match
// counts so dispatch_subscribers_matched records one observation per update.
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

// initShards initializes per-shard SubscriberLists and dispatch channels.
// Must be called from NewRedisTransport before any goroutines are started.
func (t *RedisTransport) initShards() {
	numShards := t.opts.dispatchShards
	if numShards <= 0 {
		numShards = max(runtime.NumCPU(), 1)
	}

	// Cap the shard count: each shard adds dispatch_duration buckets and a
	// shard_subscribers series.
	if numShards > suspiciousDispatchShardsMax {
		t.logger.Warn(
			"redis transport: dispatch shard count capped to the ceiling",
			"requested", numShards,
			"ceiling", suspiciousDispatchShardsMax,
		)

		numShards = suspiciousDispatchShardsMax
	}

	t.numShards = numShards

	t.shards = make([]dispatchShard, numShards)
	for i := range numShards {
		t.shards[i].index = newTopicIndex()
		// Dispatch matches through the topic index, never the list's cache.
		t.shards[i].subscribers = mercure.NewSubscriberList(-1)
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
// If ctx is cancelled mid-fan-out, shards not yet sent the work skip it. ctx
// is the transport's root context, cancelled only by Close, which disconnects
// every subscriber anyway.
//
// It does not require t.mu; see processStreamEntry for why dispatch is gap-free.
func (t *RedisTransport) shardedDispatch(ctx context.Context, update *mercure.Update) {
	work := t.shardWorkPool.Get().(*shardWork)

	work.update = update
	work.matched.Store(0)
	work.wg.Add(t.numShards)

	// wg.Wait before Put is required for pool safety: workers may still be
	// calling work.wg.Done after we return on the cancel path. Returning the
	// work to the pool before they finish would race wg.Add on the next Get.
	defer func() {
		work.wg.Wait()

		if m := t.metrics.Load(); m != nil {
			matched := work.matched.Load()
			m.dispatchSubscribersTotal.Observe(float64(matched))

			if matched == 0 {
				// See dispatchZeroMatchTotal; the single-shard path counts it too.
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

		// Load metrics once so both timestamps of this item use the same instance.
		metrics := t.metrics.Load()

		var dispatchStart time.Time
		if metrics != nil {
			dispatchStart = time.Now()
		}

		// No t.mu is held, so a candidate may have been removed concurrently; markLost
		// still counts it, because Dispatch=false is a real backpressure loss.
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
	// Index before AddSubscriber loads the cursor: the index Lock is the
	// happens-before edge for gap-free delivery (see topicIndex).
	t.shards[shardIdx].index.add(s)
	t.shards[shardIdx].subscribers.Add(s)

	if m := t.metrics.Load(); m != nil {
		m.shardSubGauges[shardIdx].Inc()
	}
}

// removeSubscriberFromList removes s from its assigned shard's subscriber list.
// Idempotent: a subscriber with no lostFlags entry is not listed (see
// lostFlags), so a redundant or non-member removal is a no-op rather than
// driving subscriberCount / the shard gauge negative. The caller Deletes the
// entry afterwards in the same t.mu section. mercure.SubscriberList.Remove
// gives no "actually removed" signal, so the membership oracle has to be ours.
// Must be called with t.mu held.
func (t *RedisTransport) removeSubscriberFromList(s *mercure.LocalSubscriber) {
	if _, member := t.lostFlags.Load(s); !member {
		return
	}

	shardIdx := t.shardFor(s.ID)
	t.shards[shardIdx].index.remove(s)
	t.shards[shardIdx].subscribers.Remove(s)

	t.subscriberCount.Add(-1)

	if m := t.metrics.Load(); m != nil {
		m.shardSubGauges[shardIdx].Dec()
	}
}

// dispatchToSubscribers dispatches update to all matching local subscribers.
// It does not require t.mu; see processStreamEntry for why dispatch is gap-free.
func (t *RedisTransport) dispatchToSubscribers(ctx context.Context, update *mercure.Update) {
	if t.numShards > 1 {
		t.shardedDispatch(ctx, update)

		return
	}

	// Single shard: dispatch inline, still recording
	// dispatch_duration_seconds{shard="0"}.
	shard := &t.shards[0]

	metrics := t.metrics.Load()

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
			// Counted on both paths, whatever WithDispatchShards is.
			metrics.dispatchZeroMatchTotal.Inc()
		}
	}
}

// disconnectAllSubscribers disconnects all subscribers across all shards.
// Called during Close; each disconnected subscriber goes through markLost
// to enforce at-most-one subscribers_lost increment across racing paths
// (a concurrent failed history replay or backpressure dispatch). Subscribers
// already counted by an earlier path are skipped via CAS.
//
// The disconnected subscribers stay listed (and in lostFlags) but no longer
// count: each shard's are subtracted from subscriberCount and from that
// shard's gauge, so neither reports them after Close, including to a
// successor transport that adopts the gauge. Subtracting rather than zeroing
// leaves a sibling transport's share of an adopted gauge intact. Must be
// called with t.mu held.
func (t *RedisTransport) disconnectAllSubscribers() {
	m := t.metrics.Load()

	for i := range t.numShards {
		var n int64

		t.shards[i].subscribers.Walk(0, func(s *mercure.LocalSubscriber) bool {
			s.Disconnect()
			t.markLost(s, lossReasonShutdown)

			n++

			return true
		})

		t.subscriberCount.Add(-n)

		if m != nil {
			m.shardSubGauges[i].Sub(float64(n))
		}
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
