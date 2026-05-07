package redistransport

import (
	"strings"
	"sync"

	"github.com/dunglas/mercure"
)

// topicIndex narrows live-dispatch candidates for topologies where the
// skipfilter SubscriberList cache thrashes — notably per-user topics, where
// each update carries a distinct topic set so the filter cache hit rate
// approaches zero and every event falls back to an O(subscribers) MatchTopics
// scan. The index buckets subscribers by their EXACT subscribed topics and
// keeps a separate set for pattern subscribers (wildcard "*" or URI templates,
// which cannot be resolved by an exact-string lookup). A dispatch then verifies
// the (small) candidate set with the real Subscriber.MatchTopics, yielding a
// result identical to SubscriberList.MatchAny for any non-empty update.
//
// Bucketing by SubscribedTopics is exhaustive because MatchTopics requires
// `subscribed` (some SubscribedTopics selector matches some update topic) as a
// NECESSARY condition — a subscriber that holds a topic only in
// AllowedPrivateTopics can never match, so it need not be indexed by it. The
// private (canAccess) check is preserved entirely by the MatchTopics verify.
// TestMatchTopicsRequiresSubscribedTopic pins this `subscribed`-is-necessary
// contract independently of MatchAny (the equivalence test runs the same
// MatchTopics on both sides, so it could not catch a shift in that contract).
//
// The index keeps its own RWMutex rather than reusing the SubscriberList lock:
// add/remove take Lock, a dispatch snapshot takes RLock. Because dispatch reads
// the index (not the skipfilter), this lock supplies the happens-before edge
// that makes gap-free delivery hold — see AddSubscriber, which performs the
// index write (via addSubscriberToList) before loading the dispatch cursor.
type topicIndex struct {
	mu sync.RWMutex
	// exact maps an exact subscribed topic to the set of subscribers that
	// subscribed to it. A set (not a slice) keeps add idempotent for repeated
	// selectors and removal O(1).
	exact map[string]map[*mercure.LocalSubscriber]struct{}
	// patterns holds every subscriber with at least one wildcard or
	// URI-template selector. These must be evaluated against every update.
	patterns map[*mercure.LocalSubscriber]struct{}
}

func newTopicIndex() *topicIndex {
	return &topicIndex{
		exact:    make(map[string]map[*mercure.LocalSubscriber]struct{}),
		patterns: make(map[*mercure.LocalSubscriber]struct{}),
	}
}

// isPatternSelector reports whether a topic selector cannot be resolved by exact
// string lookup: the reserved wildcard "*" or a URI template (contains "{").
// This MUST stay in lock-step with how MatchTopics evaluates selectors —
// TopicSelectorStore.match short-circuits on "*" and on topic==selector, and
// getRegexp (topicselector.go) treats a selector as a template only when it
// contains "{". TestIsPatternSelectorAgreesWithMatcher guards this coupling for
// the selector forms in its fixture (a new upstream reserved keyword must be
// added to that fixture to be caught — neither it nor the randomized equivalence
// test can synthesize an unknown future keyword).
//
// A "{"-containing selector that is NOT a valid URI template (e.g. "https://x/{")
// is conservatively classified as a pattern even though MatchTopics treats it as
// exact (getRegexp returns nil → exact equality only). That is correct — it is
// still verified by MatchTopics, so the result set is unchanged — but such a
// selector is scanned on every dispatch like any other pattern. This is the same
// O(patterns) cost the design already accepts for "*"/template subscribers, and
// it does not affect exact-topic topologies, so it is not specialized away (that
// would require parsing the template here and a uritemplate dependency).
func isPatternSelector(selector string) bool {
	return selector == "*" || strings.Contains(selector, "{")
}

// add indexes s under each of its subscribed selectors. A subscriber with both
// exact and pattern selectors lands in both structures; the dispatch dedup
// collapses it back to one candidate. A subscriber with no subscribed topics is
// not indexed (it can never match).
//
// Correct removal depends on s.SubscribedTopics being immutable between add and
// remove: remove reverses exactly the buckets add populated by walking the same
// slice. The hub sets a subscriber's topics once (SetTopics, before
// AddSubscriber) and never mutates them, so this holds.
func (idx *topicIndex) add(s *mercure.LocalSubscriber) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	for _, selector := range s.SubscribedTopics {
		if isPatternSelector(selector) {
			idx.patterns[s] = struct{}{}

			continue
		}

		bucket := idx.exact[selector]
		if bucket == nil {
			bucket = make(map[*mercure.LocalSubscriber]struct{})
			idx.exact[selector] = bucket
		}

		bucket[s] = struct{}{}
	}
}

// remove reverses add. It is idempotent: removing a non-member or re-removing is
// a no-op (map deletes are). Empty exact buckets are pruned so the map does not
// leak keys for churned-through per-user topics.
func (idx *topicIndex) remove(s *mercure.LocalSubscriber) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	for _, selector := range s.SubscribedTopics {
		if isPatternSelector(selector) {
			delete(idx.patterns, s)

			continue
		}

		bucket := idx.exact[selector]
		if bucket == nil {
			continue
		}

		delete(bucket, s)

		if len(bucket) == 0 {
			delete(idx.exact, selector)
		}
	}
}

// matchedAppend returns the subscribers in the index that match u, appended to
// dst — equivalent to SubscriberList.MatchAny(u) for any non-empty u.Topics.
//
// dst (passed as a zero-length reused slice) and seen (a cleared reused map) are
// caller-owned scratch; the calling shard worker is the only goroutine touching
// them, so no synchronization is needed on the scratch itself. Candidates are
// collected under RLock — the snapshot point for the gap-free guarantee — then
// the RLock is released before the MatchTopics verification, which reads only
// immutable subscriber fields (SubscribedTopics / AllowedPrivateTopics, set once
// at construction).
func (idx *topicIndex) matchedAppend(
	dst []*mercure.LocalSubscriber,
	seen map[*mercure.LocalSubscriber]struct{},
	u *mercure.Update,
) []*mercure.LocalSubscriber {
	// An update with no topics matches nothing. SubscriberList.MatchAny would
	// instead match wildcard subscribers because its encode/decode round-trips
	// an empty topic set to the literal topic "0"; matching a literal "0" is
	// meaningless. HTTP publish rejects a missing topic, so this is unreachable
	// over the wire; a programmatic Hub.Publish caller could still emit one. This
	// is the only intentional divergence from MatchAny.
	// If a topic-less update ever did reach dispatch, it matches zero subscribers
	// and is counted as a zero-match (dispatchZeroMatchTotal), so a publish-side
	// regression that emitted one would still be observable.
	if len(u.Topics) == 0 {
		return dst
	}

	idx.mu.RLock()

	for s := range idx.patterns {
		if _, dup := seen[s]; !dup {
			seen[s] = struct{}{}
			dst = append(dst, s)
		}
	}

	for _, topic := range u.Topics {
		for s := range idx.exact[topic] {
			if _, dup := seen[s]; !dup {
				seen[s] = struct{}{}
				dst = append(dst, s)
			}
		}
	}

	idx.mu.RUnlock()

	// Verify candidates against the real matcher, compacting in place so the
	// returned slice (and its backing scratch) holds only true matches. The
	// compaction aliases dst (matched := dst[:0], then append over the same
	// backing array); it is safe because the write index never overtakes the
	// read index — for each element, dst[i] is read before matched[j], j ≤ i, is
	// written. Do not parallelize this loop without removing the alias.
	matched := dst[:0]
	for _, s := range dst {
		if s.MatchTopics(u.Topics, u.Private) {
			matched = append(matched, s)
		}
	}

	return matched
}
