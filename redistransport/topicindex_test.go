package redistransport

import (
	"math/rand"
	"strconv"
	"sync"
	"testing"

	"github.com/dunglas/mercure"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// indexMatched runs the index's verified, deduped matcher for u with a fresh
// scratch set — the convenience form for tests. Production callers reuse
// per-shard scratch (see dispatchShard.match).
func indexMatched(idx *topicIndex, u *mercure.Update) []*mercure.LocalSubscriber {
	seen := make(map[*mercure.LocalSubscriber]struct{})

	return idx.matchedAppend(nil, seen, u)
}

func newIndexedSubscriber(t *testing.T, subscribed, allowedPrivate []string) *mercure.LocalSubscriber {
	t.Helper()

	s := mercure.NewLocalSubscriber("", testLogger(), testTopicMatcherStore())
	s.SetMatchers(topicMatchers(subscribed), topicMatchers(allowedPrivate))

	return s
}

// assertSameSet compares two subscriber slices as sets (order- and
// duplicate-insensitive). The index must never yield a different match set
// than SubscriberList.MatchAny, nor the same subscriber twice.
func assertSameSet(t *testing.T, want, got []*mercure.LocalSubscriber, ctx string) {
	t.Helper()

	wantSet := make(map[*mercure.LocalSubscriber]int, len(want))
	for _, s := range want {
		wantSet[s]++
	}

	gotSet := make(map[*mercure.LocalSubscriber]int, len(got))
	for _, s := range got {
		gotSet[s]++
		assert.LessOrEqual(t, gotSet[s], 1, "%s: subscriber yielded more than once (missing dedup)", ctx)
	}

	for s := range gotSet {
		_, ok := wantSet[s]
		assert.True(t, ok, "%s: index matched a subscriber MatchAny did not", ctx)
	}

	for s := range wantSet {
		_, ok := gotSet[s]
		assert.True(t, ok, "%s: index missed a subscriber MatchAny matched", ctx)
	}
}

func TestTopicIndexExactMatchAndMiss(t *testing.T) {
	t.Parallel()

	idx := newTopicIndex()
	sub := newIndexedSubscriber(t, []string{"https://example.com/foo"}, nil)
	idx.add(sub)

	hit := indexMatched(idx, &mercure.Update{Topics: []string{"https://example.com/foo"}})
	require.Len(t, hit, 1)
	assert.Same(t, sub, hit[0])

	miss := indexMatched(idx, &mercure.Update{Topics: []string{"https://example.com/bar"}})
	assert.Empty(t, miss, "exact subscriber must not match a different topic")
}

func TestTopicIndexWildcardMatchesAnyTopic(t *testing.T) {
	t.Parallel()

	idx := newTopicIndex()
	star := newIndexedSubscriber(t, []string{"*"}, nil)
	idx.add(star)

	for _, topic := range []string{"https://example.com/a", "anything", "x/y/z"} {
		got := indexMatched(idx, &mercure.Update{Topics: []string{topic}})
		require.Len(t, got, 1, "wildcard must match %q", topic)
		assert.Same(t, star, got[0])
	}
}

func TestTopicIndexTemplateMatch(t *testing.T) {
	t.Parallel()

	idx := newTopicIndex()
	tmpl := newIndexedSubscriber(t, []string{"https://example.com/users/{id}"}, nil)
	idx.add(tmpl)

	hit := indexMatched(idx, &mercure.Update{Topics: []string{"https://example.com/users/42"}})
	require.Len(t, hit, 1)
	assert.Same(t, tmpl, hit[0])

	miss := indexMatched(idx, &mercure.Update{Topics: []string{"https://example.com/books/42"}})
	assert.Empty(t, miss, "template must not match a non-conforming topic")
}

// TestTopicIndexPrivateRequiresSubscribedAndAllowed pins the private semantics:
// a subscriber matches a private update only when it is both subscribed to a
// topic and has that topic (or any update topic) in AllowedPrivateMatchers. A
// subscriber that only has the topic in AllowedPrivateMatchers (not subscribed)
// must never match — which is exactly why bucketing by SubscribedMatchers alone
// loses nothing.
func TestTopicIndexPrivateRequiresSubscribedAndAllowed(t *testing.T) {
	t.Parallel()

	idx := newTopicIndex()

	// Subscribed and allowed-private on the same topic: matches a private update.
	full := newIndexedSubscriber(t, []string{"https://example.com/p"}, []string{"https://example.com/p"})
	// Subscribed but not allowed-private: must fail canAccess on a private update.
	subOnly := newIndexedSubscriber(t, []string{"https://example.com/p"}, nil)
	// Allowed-private but not subscribed: must fail the subscribed condition,
	// and must never be a candidate (not bucketed under the topic at all).
	allowOnly := newIndexedSubscriber(t, []string{"https://example.com/other"}, []string{"https://example.com/p"})

	idx.add(full)
	idx.add(subOnly)
	idx.add(allowOnly)

	priv := indexMatched(idx, &mercure.Update{Topics: []string{"https://example.com/p"}, Private: true})
	require.Len(t, priv, 1, "only the subscribed+allowed subscriber may receive a private update")
	assert.Same(t, full, priv[0])

	pub := indexMatched(idx, &mercure.Update{Topics: []string{"https://example.com/p"}, Private: false})
	assert.Len(t, pub, 2, "a public update reaches both subscribers subscribed to the topic")
}

// TestTopicIndexPrivateCrossTopic covers a private update whose subscribed
// topic and private-grant topic are different entries: MatchTopics tracks
// subscribed and canAccess independently across all update topics, so this
// must match.
func TestTopicIndexPrivateCrossTopic(t *testing.T) {
	t.Parallel()

	idx := newTopicIndex()
	sub := newIndexedSubscriber(t,
		[]string{"https://example.com/sub"},
		[]string{"https://example.com/grant"})
	idx.add(sub)

	got := indexMatched(idx, &mercure.Update{
		Topics:  []string{"https://example.com/sub", "https://example.com/grant"},
		Private: true,
	})
	require.Len(t, got, 1,
		"subscribed via one update topic + private-granted via another must match")
	assert.Same(t, sub, got[0])
}

func TestTopicIndexDedupsAcrossTopicsAndSets(t *testing.T) {
	t.Parallel()

	idx := newTopicIndex()

	// In multiple exact buckets hit by one multi-topic update.
	multi := newIndexedSubscriber(t, []string{"https://example.com/a", "https://example.com/b"}, nil)
	// In both an exact bucket and the pattern set.
	mixed := newIndexedSubscriber(t, []string{"https://example.com/a", "*"}, nil)

	idx.add(multi)
	idx.add(mixed)

	got := indexMatched(idx, &mercure.Update{Topics: []string{"https://example.com/a", "https://example.com/b"}})
	// Each subscriber exactly once despite multiple index hits.
	assert.Len(t, got, 2, "candidates must be deduped — duplicate dispatch sends duplicate SSE events")
}

func TestTopicIndexRemove(t *testing.T) {
	t.Parallel()

	idx := newTopicIndex()
	exact := newIndexedSubscriber(t, []string{"https://example.com/x"}, nil)
	mixed := newIndexedSubscriber(t, []string{"https://example.com/x", "*"}, nil)

	idx.add(exact)
	idx.add(mixed)

	idx.remove(exact)
	idx.remove(mixed)

	got := indexMatched(idx, &mercure.Update{Topics: []string{"https://example.com/x"}})
	assert.Empty(t, got, "removed subscribers (exact and pattern) must not be candidates")
}

// TestTopicIndexEmptyTopicsMatchesNothing documents the single intentional
// divergence from SubscriberList.MatchAny: an update with no topics matches
// nothing, even a wildcard subscriber. MatchAny would match "*"/"0" because its
// encode/decode round-trips an empty set to the literal topic "0". HTTP publish
// rejects a missing topic, so this is unreachable over the wire (a programmatic
// Hub.Publish caller could still emit one).
func TestTopicIndexEmptyTopicsMatchesNothing(t *testing.T) {
	t.Parallel()

	idx := newTopicIndex()
	idx.add(newIndexedSubscriber(t, []string{"*"}, nil))

	assert.Empty(t, indexMatched(idx, &mercure.Update{Topics: nil}))
	assert.Empty(t, indexMatched(idx, &mercure.Update{Topics: []string{}}))
}

// TestTopicIndexEquivalentToMatchAny is the property test: over a
// large randomized population of subscribers (exact, wildcard, template, mixed,
// public/private) and randomized multi-topic updates, the index's verified
// match set must equal SubscriberList.MatchAny's set exactly.
func TestTopicIndexEquivalentToMatchAny(t *testing.T) {
	t.Parallel()

	rng := rand.New(rand.NewSource(1)) //nolint:gosec // deterministic test fixture, not security

	topics := []string{
		"https://example.com/u/1", "https://example.com/u/2", "https://example.com/u/3",
		"https://example.com/books/1", "https://example.com/books/2",
		"foo", "bar", "baz",
	}
	// Topics a subscriber may subscribe to: wildcard, templates (URL patterns
	// through topicMatchers), and exact topics.
	selectors := append([]string{
		"*",
		"https://example.com/u/{id}",
		"https://example.com/books/{id}",
	}, topics...)

	pickN := func(pool []string, n int) []string {
		out := make([]string, 0, n)
		for range n {
			out = append(out, pool[rng.Intn(len(pool))])
		}

		return out
	}

	sl := mercure.NewSubscriberList(10_000)
	idx := newTopicIndex()

	for range 300 {
		sub := mercure.NewLocalSubscriber("", testLogger(), testTopicMatcherStore())
		sub.SetMatchers(topicMatchers(pickN(selectors, 1+rng.Intn(3))), topicMatchers(pickN(selectors, rng.Intn(3))))
		sl.Add(sub)
		idx.add(sub)
	}

	for range 1000 {
		u := &mercure.Update{
			Topics:  pickN(topics, 1+rng.Intn(3)),
			Private: rng.Intn(2) == 0,
		}
		assertSameSet(t, sl.MatchAny(u), indexMatched(idx, u), "update "+u.Topics[0])
	}
}

// TestMatchTopicsRequiresSubscribedTopic pins the invariant the index relies
// on, independently of MatchAny: a topic only in AllowedPrivateMatchers never
// matches. The index buckets by SubscribedMatchers alone, and
// TestTopicIndexEquivalentToMatchAny cannot catch a change here because both
// of its sides call MatchTopics.
func TestMatchTopicsRequiresSubscribedTopic(t *testing.T) {
	t.Parallel()

	sub := newIndexedSubscriber(t,
		[]string{"https://example.com/subscribed"},
		[]string{"https://example.com/granted"})

	assert.False(t, sub.MatchTopics([]string{"https://example.com/granted"}, true),
		"a topic only in AllowedPrivateMatchers must not match — `subscribed` is necessary")

	assert.True(t, sub.MatchTopics(
		[]string{"https://example.com/subscribed", "https://example.com/granted"}, true,
	),
		"sanity: subscribed + private-granted across update topics must match")
}

// TestIsPatternMatcherAgreesWithMatcher pins that every matcher
// isPatternMatcher classifies as exact matches only its own pattern; a
// misclassified pattern would be bucketed under a literal key and its
// subscribers dropped. Pattern-classified matchers need no check: MatchTopics
// verifies them.
func TestIsPatternMatcherAgreesWithMatcher(t *testing.T) {
	t.Parallel()

	exact := func(p string) mercure.TopicMatcher {
		return mercure.TopicMatcher{Type: mercure.MatcherTypeExact, Pattern: p}
	}

	matchers := []mercure.TopicMatcher{
		exact("https://example.com/exact"), exact(""), exact("foo"), exact("a:b:c"),
		exact("https://example.com/{"), exact("https://example.com/{id}"), exact("https://example.com/:id"),
		exact("*"), // the wildcard: classified pattern, skipped below
		{Type: mercure.MatcherTypeURLPattern, Pattern: "https://example.com/:id"}, // pattern type
	}
	probes := []string{
		"https://example.com/exact", "https://example.com/other", "", "foo", "a:b:c",
		"https://example.com/{", "https://example.com/{id}", "https://example.com/:id",
		"https://example.com/42", "anything",
	}

	for _, m := range matchers {
		if isPatternMatcher(m) {
			continue // patterns legitimately match many topics
		}

		sub := mercure.NewLocalSubscriber("", testLogger(), testTopicMatcherStore())
		sub.SetMatchers([]mercure.TopicMatcher{m}, nil)

		for _, p := range probes {
			match := sub.MatchTopics([]string{p}, false)
			if p == m.Pattern {
				assert.True(t, match, "exact matcher %q must match itself", m.Pattern)
			} else {
				assert.False(t, match,
					"matcher %q is classified EXACT but matched a different topic %q — classification is unsafe", m.Pattern, p)
			}
		}
	}

	assert.True(t, isPatternMatcher(exact("*")), "the wildcard must be classified as a pattern")
	assert.True(t, isPatternMatcher(matchers[len(matchers)-1]), "a URL Pattern matcher must be classified as a pattern")
}

// TestTopicIndexWildcardPrivateGrant covers a private update whose canAccess
// grant comes from a wildcard in AllowedPrivateMatchers while the subscription is
// an exact topic — a branch the randomized equivalence test only reaches
// probabilistically.
func TestTopicIndexWildcardPrivateGrant(t *testing.T) {
	t.Parallel()

	idx := newTopicIndex()
	sub := newIndexedSubscriber(t, []string{"https://example.com/sub"}, []string{"*"})
	idx.add(sub)

	got := indexMatched(idx, &mercure.Update{
		Topics:  []string{"https://example.com/sub"},
		Private: true,
	})
	require.Len(t, got, 1, "exact subscription + wildcard private grant must match a private update")
	assert.Same(t, sub, got[0])
}

// TestMatchedAppendReusesScratchWithoutContamination calls matchedAppend
// repeatedly on the same dst and seen (indexMatched allocates fresh scratch)
// and pins that no call carries over another's results, including one that
// gathers more candidates than match and so compacts dst in place.
//
// Not t.Parallel: AllocsPerRun panics under a parallel test.
func TestMatchedAppendReusesScratchWithoutContamination(t *testing.T) {
	idx := newTopicIndex()
	a := newIndexedSubscriber(t, []string{"https://example.com/a"}, nil)
	b := newIndexedSubscriber(t, []string{"https://example.com/b"}, nil)
	c := newIndexedSubscriber(t, []string{"https://example.com/c"}, nil)

	idx.add(a)
	idx.add(b)
	idx.add(c)

	// Three subscribers on one shared topic; only two are granted for private
	// access. A private update gathers all three as candidates but matches two —
	// candidate count > match count, the case that drives the compaction.
	const shared = "https://example.com/shared"

	grantedA := newIndexedSubscriber(t, []string{shared}, []string{shared})
	grantedB := newIndexedSubscriber(t, []string{shared}, []string{shared})
	ungranted := newIndexedSubscriber(t, []string{shared}, nil)

	idx.add(grantedA)
	idx.add(grantedB)
	idx.add(ungranted)

	seen := make(map[*mercure.LocalSubscriber]struct{})

	var dst []*mercure.LocalSubscriber

	// Call 1: multi-match, grows the backing array.
	clear(seen)
	dst = idx.matchedAppend(dst[:0], seen, &mercure.Update{
		Topics: []string{"https://example.com/a", "https://example.com/b"},
	})
	assertSameSet(t, []*mercure.LocalSubscriber{a, b}, dst, "call 1 (multi-match)")

	// Call 2 (reused scratch): single match must not carry a/b over from the
	// first call's backing array or seen map.
	clear(seen)
	dst = idx.matchedAppend(dst[:0], seen, &mercure.Update{Topics: []string{"https://example.com/c"}})
	assertSameSet(t, []*mercure.LocalSubscriber{c}, dst, "call 2 (reused scratch, single match)")

	// Call 3 (reused scratch): 3 candidates gathered, only 2 pass MatchTopics, so
	// the in-place compaction filters a strict subset over the reused backing
	// array. Must return exactly the two granted subscribers.
	clear(seen)
	dst = idx.matchedAppend(dst[:0], seen, &mercure.Update{Topics: []string{shared}, Private: true})
	assertSameSet(t, []*mercure.LocalSubscriber{grantedA, grantedB}, dst, "call 3 (reused scratch, filtered private)")

	// Prove the reuse is actually allocation-free in steady state — the property
	// the scratch exists for. AllocsPerRun warms up once (growing dst to the
	// candidate count), then the measured runs must reuse the backing array and
	// the cleared map. The update is hoisted out so the closure allocates nothing
	// of its own.
	u := &mercure.Update{Topics: []string{shared}}
	allocs := testing.AllocsPerRun(100, func() {
		clear(seen)
		dst = idx.matchedAppend(dst[:0], seen, u)
	})
	assert.Zero(t, allocs, "matchedAppend must reuse scratch with zero allocations in steady state")
}

// TestTopicIndexConcurrentAddRemoveMatch stresses the index's own RWMutex: many
// goroutines add/remove subscribers while another loops matchedAppend. Under
// -race it flushes any unsynchronized access to the exact/patterns maps (e.g.
// the bucket-prune delete racing a reader). The reader overlaps the entire
// writer phase. Results are non-deterministic — the point is race-freedom and no
// panic.
func TestTopicIndexConcurrentAddRemoveMatch(t *testing.T) {
	t.Parallel()

	idx := newTopicIndex()
	tss := testTopicMatcherStore()

	const topic = "https://example.com/contended"

	subs := make([]*mercure.LocalSubscriber, 64)
	for i := range subs {
		s := mercure.NewLocalSubscriber("", testLogger(), tss)
		s.SetMatchers(topicMatchers([]string{topic}), nil)
		subs[i] = s
		idx.add(s)
	}

	stop := make(chan struct{})
	ready := make(chan struct{})

	var reader sync.WaitGroup

	reader.Go(func() {
		seen := make(map[*mercure.LocalSubscriber]struct{})

		var dst []*mercure.LocalSubscriber

		u := &mercure.Update{Topics: []string{topic}}

		signalled := false

		for {
			select {
			case <-stop:
				return
			default:
				clear(seen)
				dst = idx.matchedAppend(dst[:0], seen, u)
				_ = dst

				if !signalled {
					// Tell the test the reader is actively looping matchedAppend,
					// so the writer churn below is guaranteed to overlap it (else
					// scheduling could let writers finish before the reader runs,
					// silently weakening the -race guard).
					signalled = true

					close(ready)
				}
			}
		}
	})

	<-ready

	var writers sync.WaitGroup

	for w := range 4 {
		writers.Go(func() {
			for i := range 500 {
				s := subs[(w*500+i)%len(subs)]
				idx.remove(s)
				idx.add(s)
			}
		})
	}

	writers.Wait()
	close(stop)
	reader.Wait()
}

// BenchmarkPerUserTopicMatch contrasts the two dispatch matchers under the
// per-user-topic regime the index targets: many subscribers, each on a distinct
// exact topic, with events arriving for a different user each time. The
// skipfilter cache is sized well below the subscriber count, so MatchAny
// is miss-dominated and rebuilds an O(subscribers) bitmap per event; the index
// resolves the same match in O(1 + matches). Compare ns/op and allocs/op.
func BenchmarkPerUserTopicMatch(b *testing.B) {
	const (
		subscribers = 20_000
		listCache   = 2_000 // << subscribers: forces the cache-miss (thrash) regime
	)

	tss := testTopicMatcherStore()
	sl := mercure.NewSubscriberList(listCache)
	idx := newTopicIndex()
	topics := make([]string, subscribers)

	for i := range subscribers {
		topic := "https://example.com/u/" + strconv.Itoa(i)
		topics[i] = topic
		sub := mercure.NewLocalSubscriber("", testLogger(), tss)
		sub.SetMatchers(topicMatchers([]string{topic}), nil)
		sl.Add(sub)
		idx.add(sub)
	}

	b.Run("MatchAny", func(b *testing.B) {
		b.ReportAllocs()

		i := 0
		for b.Loop() {
			_ = sl.MatchAny(&mercure.Update{Topics: []string{topics[i%subscribers]}})
			i++
		}
	})

	b.Run("Index", func(b *testing.B) {
		b.ReportAllocs()

		seen := make(map[*mercure.LocalSubscriber]struct{})

		var dst []*mercure.LocalSubscriber

		i := 0

		for b.Loop() {
			clear(seen)
			dst = idx.matchedAppend(dst[:0], seen, &mercure.Update{Topics: []string{topics[i%subscribers]}})
			i++
		}
	})
}
