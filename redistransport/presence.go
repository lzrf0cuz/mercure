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

// presenceMGetBatchSize caps the keys fetched per MGET in readPresenceKeys,
// bounding each command's block time on single-threaded Redis and its reply
// size.
const presenceMGetBatchSize = 100

// presenceHeartbeat renews the presence key every presenceInterval.
// Each heartbeat serializes the current local subscriber list to JSON and
// writes it to the presence key with presenceTTL expiry.
//
// If the heartbeat fails to renew before presenceTTL expires, other nodes'
// GC (startup or periodic) may destroy this node's consumer group.
//
// Presence data can be up to presenceInterval stale. It serves the
// subscriptions endpoint and debugging; do not build correctness on it.
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
		t.metrics.Load().recordPresenceHeartbeatError()

		return
	}

	if err := t.client.Set(ctx, t.presenceKey(t.nodeID), string(data), t.opts.presenceTTL).Err(); err != nil {
		// Context cancellation during shutdown is expected operation — log at
		// Debug so alert rules keyed on "presence heartbeat failed" Error logs
		// don't fire at every graceful exit.
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			t.logger.Debug("redis transport: presence heartbeat cancelled (shutdown)", "error", err)
		} else {
			t.metrics.Load().recordPresenceHeartbeatError()
			t.logger.Error("redis transport: presence heartbeat failed", "error", err)
		}
	}

	if m := t.metrics.Load(); m != nil {
		m.presencePayloadBytes.Observe(float64(len(data)))
	}
}

// buildPresencePayload serializes local subscribers into the presence payload
// (detail format below threshold; summary format above it). Returns the
// marshaled bytes and ok=true on success. ok=false means marshaling failed
// and the caller should skip the heartbeat (error is already logged).
//
// It does not take t.mu: SubscriberList.Walk is safe for concurrent use, and
// holding t.mu would stall AddSubscriber and RemoveSubscriber during the walk.
func (t *RedisTransport) buildPresencePayload() ([]byte, bool) {
	// Choose the mode from the atomic subscriber count; summary mode skips the
	// O(n) walk.
	count := int(t.subscriberCount.Load())

	if count > t.opts.presenceDetailThreshold {
		// Debug, not Info: this runs on every heartbeat. presence_payload_bytes shows
		// whether summary mode is active.
		t.logger.Debug(
			"redis transport: presence in summary mode",
			"subscriber_count", count,
			"threshold", t.opts.presenceDetailThreshold,
		)

		return t.marshalPresenceSummary(count)
	}

	subs, overflow := t.collectDetailSubscribers(count)
	if overflow {
		// count is read without a lock, so membership can outgrow it before the walk.
		// Report at least the len(subs)+1 members the walk saw rather than marshal an
		// unbounded list.
		reported := max(int(t.subscriberCount.Load()), len(subs)+1)

		t.logger.Debug(
			"redis transport: presence summary-mode fallback (membership outran the count gate)",
			"gate_count", count,
			"reported_count", reported,
			"threshold", t.opts.presenceDetailThreshold,
		)
		t.metrics.Load().recordPresenceFallback(presenceFallbackCountRace)

		return t.marshalPresenceSummary(reported)
	}

	data, err := json.Marshal(subs)
	if err != nil {
		t.logger.Error("redis transport: failed to marshal presence data", "error", err)

		return nil, false
	}

	// Byte budget caught what the count gate missed (long matcher lists /
	// large subscription payloads): re-marshal as summary for this write so
	// single-threaded Redis does not block on a multi-MB SET. 0 disables.
	if t.opts.presenceDetailByteThreshold > 0 && int64(len(data)) > t.opts.presenceDetailByteThreshold {
		t.logger.Debug(
			"redis transport: presence summary-mode fallback (byte budget exceeded under count threshold)",
			"subscriber_count", len(subs),
			"detail_bytes", len(data),
			"byte_threshold", t.opts.presenceDetailByteThreshold,
		)
		t.metrics.Load().recordPresenceFallback(presenceFallbackByteBudget)

		return t.marshalPresenceSummary(len(subs))
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
		// A (threshold+1)th member means membership outgrew the count gate.
		if len(subs) >= t.opts.presenceDetailThreshold {
			overflow = true

			return false
		}

		// The entry is written to the shared Redis for every node's
		// subscription API, which renders the ID and the subscribed matchers,
		// slugs and payloads precomputed at registration. No node reads the
		// rest of the subscriber, so the entry leaves out the access token's
		// claims (iss, sub, jti, exp, authorization_details, ...), the private
		// topics it grants, and the requested Last-Event-ID.
		sub := s.Subscriber
		sub.Claims = nil
		sub.AllowedPrivateMatchers = nil
		sub.RequestLastEventID = ""
		sub.RequestLastEventIDSet = false

		subs = append(subs, sub)

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

// periodicZombieGC runs gcZombieGroups every zombieGCInterval, so groups left
// by crashed nodes are removed even when no node restarts.
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
// Presence is eventually consistent: a crashed node's subscribers can remain
// listed for up to presenceTTL, and SCAN may return a key twice (results are
// deduplicated by subscriber ID). The result is capped at
// subscriptionsMaxSubscribers (see readPresenceKeys).
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
// millions of other keys.
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

// readPresenceKeys fetches presence values in MGET batches of
// presenceMGetBatchSize and returns the deduplicated subscribers. A failed MGET
// aborts with an error; a key that fails to decode is skipped and counted.
//
// Subscribers are deduplicated as they are read, and the unique count is
// capped at subscriptionsMaxSubscribers (0 = no cap): past the cap the call
// returns mercure.ErrTooManySubscribers. The cap is checked per key, so heap use
// stays near the cap plus one key's subscribers.
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
				t.metrics.Load().recordPresenceReadCapped()

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
		// Count one error per key in the batch.
		metrics := t.metrics.Load()
		for range batch {
			metrics.recordPresenceReadError(presenceReadErrorGet)
		}

		return nil, fmt.Errorf("redis transport: presence MGET failed: %w", err)
	}

	return vals, nil
}

// decodePresenceValue turns one raw MGET result into its subscribers. It returns
// ok=false for the two non-fatal per-key outcomes: a missing/empty value (node
// starting up or key just expired) and an unmarshal failure, which it counts
// in presence_read_errors{kind="unmarshal"}.
func (t *RedisTransport) decodePresenceValue(raw any, key string) ([]*mercure.Subscriber, bool) {
	val, ok := raw.(string)
	if !ok || val == "" {
		// MGET returns nil for missing keys, plus empty strings for
		// edge-case empty values.
		return nil, false
	}

	parsed, err := t.unmarshalPresenceValue(val)
	if err != nil {
		t.logger.Error("redis transport: failed to unmarshal presence data", "key", key, "error", err)
		t.metrics.Load().recordPresenceReadError(presenceReadErrorUnmarshal)

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

			// Summary mode lists no subscribers.
			return summary.Subscribers, nil
		}
	}

	// Detailed format: JSON array of subscribers.
	var entries []json.RawMessage
	if err := json.Unmarshal(raw, &entries); err != nil {
		return nil, fmt.Errorf("unmarshal detailed: %w", err)
	}

	// Only JSON-serializable exported fields survive the round-trip, among them
	// the SubscribedMatchers, EscapedMatchers and SubscriptionPayloads the
	// subscription API renders. Subscriber has no setter for its unexported
	// TopicMatcherStore, so each entry is decoded into a Subscriber built with
	// NewSubscriber and the transport's store: the result is fully initialised,
	// so matching public topics on it works like on a local subscriber. The
	// entry carries no private-topic grants (see collectDetailSubscribers), so
	// it matches no private update.
	tms := t.topicMatcherStore.Load()

	subs := make([]*mercure.Subscriber, 0, len(entries))
	for _, entry := range entries {
		s := mercure.NewSubscriber(t.logger, tms)
		if err := json.Unmarshal(entry, s); err != nil {
			return nil, fmt.Errorf("unmarshal detailed: %w", err)
		}

		subs = append(subs, s)
	}

	return subs, nil
}
