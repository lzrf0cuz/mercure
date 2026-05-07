package redistransport

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/dunglas/mercure"
	"github.com/redis/go-redis/v9"
)

// presenceScanBatchSize is the COUNT hint passed to Redis SCAN when enumerating
// presence keys. SCAN's default is 10; 100 reduces round-trips on multi-node
// clusters without noticeable impact on Redis responsiveness.
const presenceScanBatchSize = 100

// presenceMGetBatchSize caps how many presence keys are fetched per MGET in
// readPresenceKeys. A single MGET over every node's key would, on a large fleet,
// be one big command that blocks single-threaded Redis for the whole fetch and
// buffers one large reply (each value is normally bounded by the best-effort
// presenceDetailByteThreshold, but can exceed it). 100
// bounds the command, the per-command block, and the reply, while amortizing RTT
// (a 1000-node fleet issues 10 MGETs, not one). Matches presenceScanBatchSize's
// magnitude; tunable if operator evidence warrants.
const presenceMGetBatchSize = 100

// presenceHeartbeat renews the presence key every presenceInterval.
// Each heartbeat serializes the current local subscriber list to JSON and
// writes it to the presence key with presenceTTL expiry.
//
// If the heartbeat fails to renew before presenceTTL expires, other nodes'
// GC (startup or periodic) may destroy this node's consumer group.
//
// The presence data is eventually consistent with up to presenceInterval
// seconds of staleness. Do not build correctness-critical logic on the
// presence API — it is intended for the subscriptions endpoint and debugging.
func (t *RedisTransport) presenceHeartbeat(ctx context.Context) {
	defer t.wg.Done()

	// Desync fleet-wide presence writes off the shared primary.
	select {
	case <-ctx.Done():
		t.logger.Debug("redis transport: presence heartbeat cancelled before first tick (shutdown)")

		return
	case <-time.After(t.jitterStart(t.opts.presenceInterval)):
	}

	ticker := time.NewTicker(t.opts.presenceInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			t.publishPresence(ctx)
		}
	}
}

// presenceSummary is a compact representation of a node's subscriber state.
// Used instead of full subscriber details when the subscriber count exceeds
// presenceDetailThreshold, preventing multi-megabyte SET payloads and GC
// pressure from json.Marshal at 100K+ subscribers.
type presenceSummary struct {
	NodeID          string                `json:"node_id"`
	SubscriberCount int                   `json:"subscriber_count"`
	Summary         bool                  `json:"summary"`
	Subscribers     []*mercure.Subscriber `json:"subscribers,omitempty"`
}

// publishPresence serializes local subscribers and writes the presence key.
// When the subscriber count exceeds presenceDetailThreshold, only the count
// is stored (summary mode) to avoid large SET payloads.
func (t *RedisTransport) publishPresence(ctx context.Context) {
	data, ok := t.buildPresencePayload()
	if !ok {
		// buildPresencePayload already Error-logged the marshal failure; count it
		// as a heartbeat error since this tick writes nothing and the presence
		// key drifts toward TTL expiry until a later tick succeeds.
		t.metricsOrCountDrop().recordPresenceHeartbeatError()

		return
	}

	if err := t.client.Set(ctx, t.presenceKey(t.nodeID), string(data), t.opts.presenceTTL).Err(); err != nil {
		// Context cancellation during shutdown is expected operation — log at
		// Debug so alert rules keyed on "presence heartbeat failed" Error logs
		// don't fire at every graceful exit.
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			t.logger.Debug("redis transport: presence heartbeat cancelled (shutdown)", "error", err)
		} else {
			t.metricsOrCountDrop().recordPresenceHeartbeatError()
			t.logger.Error("redis transport: presence heartbeat failed", "error", err)
		}
	}

	if m := t.metricsOrCountDrop(); m != nil {
		m.presencePayloadBytes.Observe(float64(len(data)))
	}
}

// buildPresencePayload serializes local subscribers into the presence payload
// (detail format below threshold; summary format above it). Returns the
// marshaled bytes and ok=true on success. ok=false means marshaling failed
// and the caller should skip the heartbeat (error is already logged).
//
// Does NOT acquire t.mu: mercure.SubscriberList wraps skipfilter.SkipFilter
// which has an internal sync.RWMutex across Add/Remove/Walk/MatchAny, so the
// per-shard walk is already serialized at that layer. Skipping t.mu avoids
// stalling AddSubscriber/RemoveSubscriber (which hold t.mu.Lock) during
// multi-shard walks at 100k+ subscribers — live dispatch never holds t.mu (it
// synchronizes through the per-shard topic index's own RLock).
func (t *RedisTransport) buildPresencePayload() ([]byte, bool) {
	// Decide mode from the atomic subscriber count (maintained at add/remove)
	// rather than walking every shard each tick. Above the detail threshold we
	// emit a summary and SKIP the O(n) walk entirely — the expensive large-fleet
	// path. The walk runs only in detail mode, where the count is bounded.
	count := int(t.subscriberCount.Load())

	if count > t.opts.presenceDetailThreshold {
		// Demoted from Info because buildPresencePayload runs every
		// presenceInterval tick — at Info this is a steady-state log spam
		// for any hub above the detail threshold. Operators who need to
		// confirm summary mode look at presence_payload_bytes histogram
		// (small + steady = summary mode) on the dashboard instead.
		t.logger.Debug(
			"redis transport: presence in summary mode",
			"subscriber_count", count,
			"threshold", t.opts.presenceDetailThreshold,
		)

		return t.marshalPresenceSummary(count)
	}

	subs, overflow := t.collectDetailSubscribers(count)
	if overflow {
		// count is read lock-free, so membership can outrun it: under a
		// reconnect storm many AddSubscribers complete between the read and the
		// walk, leaving the gate stale-low. Fall back to a summary rather than
		// marshaling the unbounded list the gate let through. The walk directly
		// observed at least len(subs)+1 members (it collected len(subs) then hit
		// one more), so report max(fresh count, len(subs)+1) — a re-read atomic
		// can still be stale-low, but the summary must never under-report below
		// what was just observed.
		reported := max(int(t.subscriberCount.Load()), len(subs)+1)

		t.logger.Debug(
			"redis transport: presence summary-mode fallback (membership outran the count gate)",
			"gate_count", count,
			"reported_count", reported,
			"threshold", t.opts.presenceDetailThreshold,
		)
		t.metricsOrCountDrop().recordPresenceFallback(presenceFallbackCountRace)

		return t.marshalPresenceSummary(reported)
	}

	data, err := json.Marshal(subs)
	if err != nil {
		t.logger.Error("redis transport: failed to marshal presence data", "error", err)

		return nil, false
	}

	// Byte budget caught what the count gate missed (long topic lists /
	// large JWT claims): re-marshal as summary for this write so
	// single-threaded Redis does not block on a multi-MB SET. 0 disables.
	if t.opts.presenceDetailByteThreshold > 0 && int64(len(data)) > t.opts.presenceDetailByteThreshold {
		summaryData, ok := t.marshalPresenceSummary(len(subs))
		if !ok {
			t.metricsOrCountDrop().recordPresenceFallback(presenceFallbackSummaryMarshalFailed)

			// Over-budget detail is better than no heartbeat. The
			// summary_marshal_failed counter surfaces this last-resort
			// path so it is not silent.
			return data, true
		}

		t.logger.Debug(
			"redis transport: presence summary-mode fallback (byte budget exceeded under count threshold)",
			"subscriber_count", len(subs),
			"detail_bytes", len(data),
			"byte_threshold", t.opts.presenceDetailByteThreshold,
		)
		t.metricsOrCountDrop().recordPresenceFallback(presenceFallbackByteBudget)

		return summaryData, true
	}

	return data, true
}

// collectDetailSubscribers walks the shard lists collecting per-subscriber
// detail, bounded at presenceDetailThreshold. overflow=true means membership
// outran the (lock-free) count gate and the caller should fall back to summary
// mode instead of marshaling an unbounded list. capHint sizes the slice; it is
// clamped at 0 because a transient negative count would otherwise panic make.
func (t *RedisTransport) collectDetailSubscribers(capHint int) (subs []mercure.Subscriber, overflow bool) {
	subs = make([]mercure.Subscriber, 0, max(capHint, 0))

	t.walkAllSubscribers(func(s *mercure.LocalSubscriber) bool {
		// >= (not >): detail mode is for count <= threshold, so a (threshold+1)th
		// member means membership outran the gate — overflow before appending it
		// rather than emitting one item over the configured detail limit.
		if len(subs) >= t.opts.presenceDetailThreshold {
			overflow = true

			return false
		}

		subs = append(subs, s.Subscriber)

		return true
	})

	return subs, overflow
}

// marshalPresenceSummary encodes a count-only summary payload. Returns ok=false
// (already Error-logged) when marshaling fails so the caller can skip the write.
func (t *RedisTransport) marshalPresenceSummary(count int) ([]byte, bool) {
	data, err := json.Marshal(presenceSummary{
		NodeID:          t.nodeID,
		SubscriberCount: count,
		Summary:         true,
	})
	if err != nil {
		t.logger.Error("redis transport: failed to marshal presence summary", "error", err)

		return nil, false
	}

	return data, true
}

// periodicZombieGC runs the same zombie consumer group cleanup as startup GC,
// but on a periodic basis. This ensures zombie groups from crashed nodes are
// cleaned up promptly even when no node restarts occur.
//
// Without periodic GC, a zombie group persists until the next node startup
// runs gcZombieGroups. In long-running deployments where nodes rarely restart,
// this could mean zombie groups linger indefinitely (consuming ~100 bytes of
// Redis memory each).
func (t *RedisTransport) periodicZombieGC(ctx context.Context) {
	defer t.wg.Done()

	// Desync fleet-wide zombie-group GC off the shared primary.
	select {
	case <-ctx.Done():
		t.logger.Debug("redis transport: zombie-group GC cancelled before first tick (shutdown)")

		return
	case <-time.After(t.jitterStart(t.opts.zombieGCInterval)):
	}

	ticker := time.NewTicker(t.opts.zombieGCInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			t.gcZombieGroups(ctx, t.key(""))
		}
	}
}

// GetSubscribers implements mercure.TransportSubscribers. Returns the last event ID
// and all active subscribers across the cluster by querying presence keys.
//
// Consistency model: The presence API is eventually consistent with up to
// presenceInterval seconds of staleness. A crashed node's subscribers may
// appear present for up to one TTL cycle (presenceTTL) after the crash.
// SCAN is not atomic and may return duplicates or miss keys during iteration —
// results are deduplicated by subscriber ID in Go.
//
// Memory bounds: subscriber materialization is bounded by
// subscriptionsMaxSubscribers (the cap aborts with ErrTooManySubscribers
// rather than build the whole cluster's list — see readPresenceKeys), and the
// caller's JSON-LD response is therefore O(returned subscribers) ≤ cap. Key
// enumeration is O(node count) — one presence key per hub node, which is orders
// of magnitude below the subscriber count and so is not separately capped.
func (t *RedisTransport) GetSubscribers(ctx context.Context) (string, []*mercure.Subscriber, error) {
	select {
	case <-t.closed:
		return "", nil, ErrClosedTransport
	default:
	}

	keys, err := t.scanPresenceKeys(ctx)
	if err != nil {
		return "", nil, err
	}

	// readPresenceKeys deduplicates during accumulation (and bounds the count).
	allSubscribers, err := t.readPresenceKeys(ctx, keys)
	if err != nil {
		return "", nil, err
	}

	// Get authoritative cluster-wide lastEventID. Missing key is normal on a
	// fresh deployment; logging Warn when subscribers ARE present surfaces
	// the abnormal case (operator FLUSHDB, maxmemory eviction, etc.) that
	// would otherwise silently rewind every client's Last-Event-ID.
	lastEventID, err := t.client.Get(ctx, t.key(":lastEventID")).Result()
	if errors.Is(err, redis.Nil) {
		if len(allSubscribers) > 0 {
			t.logger.Warn("redis transport: lastEventID key missing but subscribers present; replay will start from earliest",
				"subscriber_count", len(allSubscribers))
		}

		lastEventID = mercure.EarliestLastEventID
	} else if err != nil {
		return "", nil, fmt.Errorf("redis transport: failed to get lastEventID: %w", err)
	}

	return lastEventID, allSubscribers, nil
}

// scanPresenceKeys enumerates presence keys via SCAN (non-blocking, incremental).
// SCAN (not KEYS) so enumeration stays safe on shared Redis instances with
// millions of other keys. On a dedicated Mercure instance the total is
// typically single-digit (1 stream + 1 lastEventID + N node presence keys).
func (t *RedisTransport) scanPresenceKeys(ctx context.Context) ([]string, error) {
	var keys []string

	iter := t.client.Scan(ctx, 0, t.presenceKeyPattern(), presenceScanBatchSize).Iterator()
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
	}

	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("redis transport: presence scan failed: %w", err)
	}

	return keys, nil
}

// readPresenceKeys fetches all presence values in MGET batches of
// presenceMGetBatchSize (rather than one MGET over every key), unmarshals each,
// and returns the deduplicated subscribers. A failed MGET aborts with an error
// (no partial result) — matching the original single-MGET contract — so alerts
// keyed on presence_read_errors don't fire on client hangups; per-key read
// errors and skips are still counted.
//
// Subscribers are deduplicated DURING accumulation (SCAN can return a key twice)
// and the UNIQUE count is bounded by subscriptionsMaxSubscribers: once it would
// exceed the cap the call aborts with mercure.ErrTooManySubscribers (hub 503)
// rather than materialize the whole cluster into one request's heap → OOM. 0
// disables the cap. The cap is checked as each key is unmarshaled, so a cap hit
// stops decoding the rest of the batch immediately — transient heap is bounded by
// ~cap unique subscribers plus a single key's subscriber list, not the whole
// batch's decoded structs.
func (t *RedisTransport) readPresenceKeys(ctx context.Context, keys []string) ([]*mercure.Subscriber, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	maxSubs := t.opts.subscriptionsMaxSubscribers

	seen := make(map[string]struct{})

	var unique []*mercure.Subscriber

	for start := 0; start < len(keys); start += presenceMGetBatchSize {
		batch := keys[start:min(start+presenceMGetBatchSize, len(keys))]

		vals, err := t.mgetPresenceValues(ctx, batch)
		if err != nil {
			return nil, err
		}

		for i, raw := range vals {
			parsed, ok := t.decodePresenceValue(raw, batch[i])
			if !ok {
				continue
			}

			if mergeUniqueSubscribers(seen, &unique, parsed, maxSubs) {
				t.metricsOrCountDrop().recordPresenceReadCapped()

				return nil, fmt.Errorf("redis transport: subscriber count exceeds the configured cap (%d): %w",
					maxSubs, mercure.ErrTooManySubscribers)
			}
		}
	}

	return unique, nil
}

// mergeUniqueSubscribers appends the first-seen subscribers from parsed into
// *unique (deduplicating on ID via seen). It stops and returns true the moment a
// new unique subscriber would push the count past maxSubs (0 = no cap), so the
// caller can abort before decoding further keys.
func mergeUniqueSubscribers(
	seen map[string]struct{},
	unique *[]*mercure.Subscriber,
	parsed []*mercure.Subscriber,
	maxSubs int,
) bool {
	for _, s := range parsed {
		if _, dup := seen[s.ID]; dup {
			continue
		}

		if maxSubs > 0 && len(*unique) >= maxSubs {
			return true
		}

		seen[s.ID] = struct{}{}
		*unique = append(*unique, s)
	}

	return false
}

// mgetPresenceValues MGETs one batch of presence keys and returns the raw
// values, leaving unmarshaling to the caller so the cap can be enforced per key.
// MGET is safe on Redis Cluster because every presence key carries the
// `{streamName}` hash tag, pinning the whole batch to one slot. Context
// cancellation surfaces as a returned error. An MGET error returns the error
// after counting one presence_read_errors{kind="get"} per key in the batch.
func (t *RedisTransport) mgetPresenceValues(ctx context.Context, batch []string) ([]any, error) {
	vals, err := t.client.MGet(ctx, batch...).Result()
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, fmt.Errorf("redis transport: presence read cancelled: %w", err)
		}

		t.logger.Error("redis transport: failed to MGET presence keys", "count", len(batch), "error", err)
		// Record one error per key so the counter's rate reflects impact
		// (per-key metric semantics are preserved). Raw Load (not
		// metricsOrCountDrop) so we can issue the bulk pre-binding drop bump
		// once for the whole batch — metricsOrCountDrop's single increment
		// would understate by len(batch)-1 the per-key emissions the loop
		// below would have made. DO NOT replace the loop body's nil-safe
		// recordPresenceReadError call with t.metricsOrCountDrop().recordPresenceReadError —
		// that would double-count drops on top of the bulk bump.
		metrics := t.metrics.Load()
		if metrics == nil {
			t.recordPreBindingDrops(uint64(len(batch)))
		}

		for range batch {
			metrics.recordPresenceReadError(presenceReadErrorGet)
		}

		return nil, fmt.Errorf("redis transport: presence MGET failed: %w", err)
	}

	return vals, nil
}

// decodePresenceValue turns one raw MGET result into its subscribers. It returns
// ok=false (after counting the matching metric) for the two non-fatal per-key
// outcomes: a missing/empty value (presence_read_skipped — node starting up or
// key just expired) and an unmarshal failure
// (presence_read_errors{kind="unmarshal"}).
func (t *RedisTransport) decodePresenceValue(raw any, key string) ([]*mercure.Subscriber, bool) {
	val, ok := raw.(string)
	if !ok || val == "" {
		// MGET returns nil for missing keys, plus empty strings for
		// edge-case empty values. Node starting up or key just expired —
		// observable surge is the signal, steady-state nonzero is fine.
		t.metricsOrCountDrop().recordPresenceReadSkipped()

		return nil, false
	}

	parsed, err := t.unmarshalPresenceValue(val)
	if err != nil {
		t.logger.Error("redis transport: failed to unmarshal presence data", "key", key, "error", err)
		t.metricsOrCountDrop().recordPresenceReadError(presenceReadErrorUnmarshal)

		return nil, false
	}

	return parsed, true
}

// unmarshalPresenceValue parses a presence key value, handling both the
// detailed format (JSON array of subscribers) and the summary format
// (presenceSummary struct with only a count). When summary mode is detected,
// the subscribers field is empty — the count is logged but no Subscriber
// objects are returned for that node.
func (t *RedisTransport) unmarshalPresenceValue(val string) ([]*mercure.Subscriber, error) {
	raw := []byte(val)

	// Try summary format first (starts with '{').
	if len(raw) > 0 && raw[0] == '{' {
		var summary presenceSummary
		if err := json.Unmarshal(raw, &summary); err != nil {
			return nil, fmt.Errorf("unmarshal summary: %w", err)
		}

		if summary.Summary {
			t.logger.Debug(
				"redis transport: remote node in summary presence mode",
				"node", summary.NodeID,
				"subscriber_count", summary.SubscriberCount,
			)

			// Summary mode carries only the count; the Subscribers field is
			// typically nil and the caller falls back on the count for
			// dashboards. Returning whatever is present keeps the caller
			// contract simple.
			return summary.Subscribers, nil
		}
	}

	// Detailed format: JSON array of subscribers.
	var subs []*mercure.Subscriber
	if err := json.Unmarshal(raw, &subs); err != nil {
		return nil, fmt.Errorf("unmarshal detailed: %w", err)
	}

	// Inject the transport's TopicSelectorStore into deserialized subscribers.
	// Deserialized Subscribers are partial reconstructions — only
	// JSON-serializable exported fields survive the round-trip.
	// This prevents nil pointer panics when SubscriptionsHandler calls
	// MatchTopics() on cross-node subscribers for topic-filtered queries
	// (GET /.well-known/mercure/subscriptions/{topic}).
	if tss := t.topicSelectorStore.Load(); tss != nil {
		for _, s := range subs {
			s.SetTopicSelectorStore(tss)
		}
	}

	return subs, nil
}
