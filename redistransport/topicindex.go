package redistransport

import (
	"sync"

	"github.com/dunglas/mercure"
)

// topicIndex narrows live-dispatch candidates without the SubscriberList match
// cache, which misses on nearly every update when topics are per user. It
// buckets subscribers by their exact subscribed topics and keeps subscribers
// with a wildcard or non-exact matcher in a separate set. Candidates are then
// verified with Subscriber.MatchTopics, so the result equals
// SubscriberList.MatchAny for any update with topics.
//
// Indexing only SubscribedMatchers is enough because MatchTopics requires a
// subscribed matcher to match an update topic; the private-topic check happens
// in MatchTopics.
//
// add and remove take mu.Lock and dispatch snapshots take mu.RLock. This lock is
// the happens-before edge for gap-free delivery: AddSubscriber writes the index
// before it loads the dispatch cursor.
type topicIndex struct {
	mu sync.RWMutex
	// exact maps an exact subscribed topic to the set of subscribers that
	// subscribed to it. A set (not a slice) keeps add idempotent for repeated
	// matchers and removal O(1).
	exact map[string]map[*mercure.LocalSubscriber]struct{}
	// patterns holds every subscriber with at least one wildcard or
	// non-exact matcher. These must be evaluated against every update.
	patterns map[*mercure.LocalSubscriber]struct{}
}

func newTopicIndex() *topicIndex {
	return &topicIndex{
		exact:    make(map[string]map[*mercure.LocalSubscriber]struct{}),
		patterns: make(map[*mercure.LocalSubscriber]struct{}),
	}
}

// isPatternMatcher reports whether a topic matcher cannot be resolved by exact
// string lookup: the reserved wildcard "*" (which matches every topic whatever
// the matcher type) or any matcher type other than exact (URL Pattern, the
// deprecated v8 type, or a type added later). It must agree with
// TopicMatcherStore.matches, which accepts "*" for any topic and compares an
// exact matcher's pattern for equality.
//
// Classifying every non-exact type as a pattern is conservative: MatchTopics
// still verifies the candidate, so the result set is unchanged, at the
// O(patterns) cost the design already accepts for wildcard subscribers.
func isPatternMatcher(m mercure.TopicMatcher) bool {
	return m.Pattern == "*" || m.Type != mercure.MatcherTypeExact
}

// add indexes s under each of its subscribed matchers. A subscriber with both
// exact and pattern matchers lands in both structures; the dispatch dedup
// collapses it back to one candidate. A subscriber with no subscribed matchers
// is not indexed (it can never match).
//
// Correct removal depends on s.SubscribedMatchers being immutable between add
// and remove: remove reverses exactly the buckets add populated by walking the
// same slice. The hub sets a subscriber's matchers once (SetMatchers, before
// AddSubscriber) and never mutates them, so this holds.
func (idx *topicIndex) add(s *mercure.LocalSubscriber) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	for _, m := range s.SubscribedMatchers {
		if isPatternMatcher(m) {
			idx.patterns[s] = struct{}{}

			continue
		}

		bucket := idx.exact[m.Pattern]
		if bucket == nil {
			bucket = make(map[*mercure.LocalSubscriber]struct{})
			idx.exact[m.Pattern] = bucket
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

	for _, m := range s.SubscribedMatchers {
		if isPatternMatcher(m) {
			delete(idx.patterns, s)

			continue
		}

		bucket := idx.exact[m.Pattern]
		if bucket == nil {
			continue
		}

		delete(bucket, s)

		if len(bucket) == 0 {
			delete(idx.exact, m.Pattern)
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
// immutable subscriber fields (SubscribedMatchers / AllowedPrivateMatchers, set
// once at construction).
func (idx *topicIndex) matchedAppend(
	dst []*mercure.LocalSubscriber,
	seen map[*mercure.LocalSubscriber]struct{},
	u *mercure.Update,
) []*mercure.LocalSubscriber {
	// An update with no topics matches nothing here, whereas MatchAny would match
	// wildcard subscribers. HTTP publish requires a topic, so only a programmatic
	// Hub.Publish can send one; it is counted in dispatchZeroMatchTotal.
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

	// Compact the true matches into dst in place. The write index never passes
	// the read index, so the aliasing is safe while this loop stays sequential.
	matched := dst[:0]
	for _, s := range dst {
		if s.MatchTopics(u.Topics, u.Private) {
			matched = append(matched, s)
		}
	}

	return matched
}
