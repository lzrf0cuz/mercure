package mercure

import (
	"bytes"
	"log/slog"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDispatch(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	s := NewLocalSubscriber("1", slog.Default(), &TopicSelectorStore{})
	s.SubscribedTopics = []string{"https://example.com"}

	s.SubscribedTopics = []string{"https://example.com"}
	defer s.Disconnect()

	// Dispatch must be non-blocking
	// Messages coming from the history can be sent after live messages, but must be received first
	s.Dispatch(ctx, &Update{Topics: s.SubscribedTopics, Event: Event{ID: "3"}}, false)
	s.Dispatch(ctx, &Update{Topics: s.SubscribedTopics, Event: Event{ID: "1"}}, true)
	s.Dispatch(ctx, &Update{Topics: s.SubscribedTopics, Event: Event{ID: "4"}}, false)
	s.Dispatch(ctx, &Update{Topics: s.SubscribedTopics, Event: Event{ID: "2"}}, true)
	s.HistoryDispatched("")

	s.Ready(ctx)

	for i := 1; i <= 4; i++ {
		if u, ok := <-s.Receive(); ok && u != nil {
			assert.Equal(t, strconv.Itoa(i), u.ID)
		}
	}
}

func TestDisconnect(t *testing.T) {
	t.Parallel()

	s := NewLocalSubscriber("", slog.Default(), &TopicSelectorStore{})
	s.Disconnect()
	// can be called two times without crashing
	s.Disconnect()

	assert.False(t, s.Dispatch(t.Context(), &Update{}, false))
}

func TestLogSubscriber(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	s := NewLocalSubscriber("123", logger, &TopicSelectorStore{})
	s.SetTopics([]string{"https://example.com/bar"}, []string{"https://example.com/foo"})

	logger.Info("test", slog.Any("subscriber", s))

	log := buf.String()
	assert.Contains(t, log, `"last_event_id":"123"`)
	assert.Contains(t, log, `"topic_selectors":["https://example.com/foo"]`)
	assert.Contains(t, log, `"topics":["https://example.com/bar"]`)
}

func TestLogSubscriberTruncatesOversizedLastEventID(t *testing.T) {
	t.Parallel()

	// Last-Event-ID is client-supplied and bounded only by the HTTP server's
	// max-header size (~1 MiB), so LogValue must cap it before it lands in a
	// log record. The full value is still kept on the subscriber for replay.
	var buf bytes.Buffer

	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	oversized := strings.Repeat("a", maxObservedEventIDLen+50)
	s := NewLocalSubscriber(oversized, logger, &TopicSelectorStore{})

	logger.Info("test", slog.Any("subscriber", s))

	log := buf.String()
	assert.NotContains(t, log, oversized, "full oversized Last-Event-ID must not reach the log")
	assert.Contains(t, log, strings.Repeat("a", maxObservedEventIDLen)+`…(truncated)`)
	// The untruncated value remains available to history-replay logic.
	assert.Equal(t, oversized, s.RequestLastEventID)
}

func TestMatchTopic(t *testing.T) {
	t.Parallel()

	s := NewLocalSubscriber("", slog.Default(), &TopicSelectorStore{})
	s.SetTopics([]string{"https://example.com/no-match", "https://example.com/books/{id}"}, []string{"https://example.com/users/foo/{?topic}"})

	assert.False(t, s.Match(&Update{Topics: []string{"https://example.com/not-subscribed"}}))
	assert.False(t, s.Match(&Update{Topics: []string{"https://example.com/not-subscribed"}, Private: true}))
	assert.False(t, s.Match(&Update{Topics: []string{"https://example.com/no-match"}, Private: true}))
	assert.False(t, s.Match(&Update{Topics: []string{"https://example.com/books/1"}, Private: true}))
	assert.False(t, s.Match(&Update{Topics: []string{"https://example.com/books/1", "https://example.com/users/bar/?topic=https%3A%2F%2Fexample.com%2Fbooks%2F1"}, Private: true}))

	assert.True(t, s.Match(&Update{Topics: []string{"https://example.com/books/1"}}))
	assert.True(t, s.Match(&Update{Topics: []string{"https://example.com/books/1", "https://example.com/users/foo/?topic=https%3A%2F%2Fexample.com%2Fbooks%2F1"}, Private: true}))
}

func TestSubscriberDoesNotBlockWhenChanIsFull(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	s := NewLocalSubscriber("", slog.Default(), &TopicSelectorStore{})
	s.Ready(ctx)

	for i := 0; i <= defaultOutBufferLength; i++ {
		s.Dispatch(ctx, &Update{}, false)
	}

	for range s.Receive() { //nolint:revive
	}
}

func TestNewLocalSubscriberOutBuffer(t *testing.T) {
	t.Parallel()

	tss := &TopicSelectorStore{}

	t.Run("default when unconfigured", func(t *testing.T) {
		t.Parallel()

		s := NewLocalSubscriber("", slog.Default(), tss)
		assert.Equal(t, defaultOutBufferLength, s.outBufferLength)
		assert.Equal(t, defaultOutBufferLength, cap(s.out))
	})

	t.Run("custom value sizes the out channel", func(t *testing.T) {
		t.Parallel()

		s := NewLocalSubscriber("", slog.Default(), tss, withOutBuffer(32))
		assert.Equal(t, 32, s.outBufferLength)
		assert.Equal(t, 32, cap(s.out))
	})

	t.Run("internal withOutBuffer floors sub-minimum to the default", func(t *testing.T) {
		t.Parallel()

		// The public WithSubscriberOutBuffer Option REJECTS a sub-minimum value
		// (see TestWithSubscriberOutBufferRejectsSubMinimum); this asserts the
		// lower-level withOutBuffer keeps its defense-in-depth floor regardless.
		s := NewLocalSubscriber("", slog.Default(), tss, withOutBuffer(MinSubscriberOutBuffer-1))
		assert.Equal(t, defaultOutBufferLength, s.outBufferLength)
	})

	t.Run("zero from an unconfigured hub falls back to default", func(t *testing.T) {
		t.Parallel()

		s := NewLocalSubscriber("", slog.Default(), tss, withOutBuffer(0))
		assert.Equal(t, defaultOutBufferLength, s.outBufferLength)
	})
}

// TestLocalSubscriberLiveQueueHonorsConfiguredBuffer proves a smaller configured
// buffer sheds the pre-ready liveQueue earlier — the OOM-prevention guarantee, not
// just a channel-cap cosmetic. The subscriber is not Ready, so dispatches take the
// liveQueue path bounded by s.outBufferLength.
func TestLocalSubscriberLiveQueueHonorsConfiguredBuffer(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	s := NewLocalSubscriber("", slog.Default(), &TopicSelectorStore{}, withOutBuffer(MinSubscriberOutBuffer))

	for range MinSubscriberOutBuffer {
		assert.True(t, s.Dispatch(ctx, &Update{}, false), "updates up to the configured bound are queued")
	}

	assert.False(t, s.Dispatch(ctx, &Update{}, false), "exceeding the configured (small) bound sheds, well below the default 1000")
}

func TestSetTopicSelectorStore(t *testing.T) {
	t.Parallel()

	// Create a subscriber without a TopicSelectorStore (simulates JSON deserialization).
	s := &Subscriber{
		ID:               "test-sub",
		SubscribedTopics: []string{"https://example.com/books/{id}"},
	}

	// MatchTopics would panic with nil topicSelectorStore — set it first.
	tss := &TopicSelectorStore{}
	s.SetTopicSelectorStore(tss)

	// Now MatchTopics should work without panic.
	assert.True(t, s.MatchTopics([]string{"https://example.com/books/1"}, false))
	assert.False(t, s.MatchTopics([]string{"https://example.com/not-subscribed"}, false))
}

// TestMatchTopicsNilStoreGuard locks in the defensive nil-store behavior:
// a Subscriber that was deserialized cross-node (e.g. via the subscriptions
// API) and never had its unexported topicSelectorStore restored must NOT
// panic on MatchTopics. The guard returns false (no match) so the caller
// degrades gracefully — without it, any subscriber-side filter operation
// against such an instance hit nil-pointer dereference.
func TestMatchTopicsNilStoreGuard(t *testing.T) {
	t.Parallel()

	s := &Subscriber{
		ID:               "deserialized-sub",
		SubscribedTopics: []string{"https://example.com/books/{id}"},
	}

	// topicSelectorStore is nil — must not panic.
	require.NotPanics(t, func() {
		assert.False(t, s.MatchTopics([]string{"https://example.com/books/1"}, false))
		assert.False(t, s.MatchTopics([]string{"https://example.com/anything"}, true))
	})
}
