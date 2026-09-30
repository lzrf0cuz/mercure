package redistransport

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/dunglas/mercure"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// withRecorderRoot returns a context whose active span is a root span recorded
// by an in-memory SpanRecorder; transport spans started from that context are
// recorded too. Tests End() the root before reading the recorder, which lists
// only ended spans.
func withRecorderRoot(t *testing.T) (context.Context, trace.Span, *tracetest.SpanRecorder) {
	t.Helper()

	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))

	ctx, root := tp.Tracer("test").Start(context.Background(), "root")

	t.Cleanup(func() {
		// If the test didn't End() the root explicitly, end it on cleanup
		// so the recorder doesn't accumulate unfinished spans across
		// t.Parallel siblings. End() is idempotent in sdktrace, so a test
		// that already called root.End() is unaffected.
		root.End()
	})

	return ctx, root, rec
}

// TestDispatchTagsParentSpanWithTransport pins that Dispatch sets
// mercure.transport=redis on the active parent span (mercure.publish, or
// mercure.subscribe for subscription updates).
func TestDispatchTagsParentSpanWithTransport(t *testing.T) {
	t.Parallel()

	ctx, root, rec := withRecorderRoot(t)

	transport, _ := newTestTransport(t)
	require.NoError(t, transport.Dispatch(ctx, &mercure.Update{
		Topics: []string{"https://example.com/books/1"},
		Data:   "hello",
	}))

	root.End()

	assertTransportAttrOnRoot(t, rec)
}

// TestAddSubscriberTagsParentSpanWithTransport pins that AddSubscriber sets
// mercure.transport=redis on the active mercure.subscribe span.
func TestAddSubscriberTagsParentSpanWithTransport(t *testing.T) {
	t.Parallel()

	ctx, root, rec := withRecorderRoot(t)

	transport, _ := newTestTransport(t)

	s := mercure.NewLocalSubscriber("", nil, &mercure.TopicMatcherStore{})
	s.SetMatchers(topicMatchers([]string{"https://example.com/books/1"}), nil)

	require.NoError(t, transport.AddSubscriber(ctx, s))
	t.Cleanup(s.Disconnect)

	root.End()

	assertTransportAttrOnRoot(t, rec)
}

// TestDispatchHistoryEmitsTransportSpan pins the mercure.transport.history span:
// the same name and attributes as the Bolt transport's (TestBoltHistoryEmitsSpan),
// plus mercure.transport=redis.
func TestDispatchHistoryEmitsTransportSpan(t *testing.T) {
	t.Parallel()

	ctx, root, rec := withRecorderRoot(t)

	transport, _ := newTestTransport(t)

	topics := []string{"https://example.com/books/1"}
	for i := 1; i <= 3; i++ {
		require.NoError(t, transport.Dispatch(ctx, &mercure.Update{
			ID:     strconv.Itoa(i),
			Topics: topics,
		}))
	}

	s := mercure.NewLocalSubscriber(mercure.EarliestLastEventID, nil, &mercure.TopicMatcherStore{})
	s.SetMatchers(topicMatchers(topics), nil)

	require.NoError(t, transport.AddSubscriber(ctx, s))
	t.Cleanup(s.Disconnect)

	root.End()

	historySpan := findEndedSpan(rec, "mercure.transport.history")
	require.NotNil(t, historySpan, "mercure.transport.history span must be emitted by dispatchHistory; got %v", endedSpanNames(rec))

	attrs := spanAttrMap(historySpan)
	assert.Equal(t, "redis", attrs["mercure.transport"], "mercure.transport attr must be redis on the history span")
	assert.Equal(t, s.ID, attrs["mercure.subscriber.id"], "mercure.subscriber.id attr must match the subscriber that triggered the replay")
	// The Bolt transport's span carries mercure.last_event_id.requested too.
	assert.Equal(t, mercure.EarliestLastEventID, attrs["mercure.last_event_id.requested"],
		"mercure.last_event_id.requested attr must mirror the subscriber's request")

	// Replay outcome attributes. events_replayed is bounded, not exact: the live
	// listener can disconnect the subscriber mid-replay.
	// TestDispatchEntryExcludesFailedDispatch pins the count deterministically.
	replayed := spanAttrValue(t, historySpan, "mercure.history.events_replayed").AsInt64()
	assert.GreaterOrEqual(t, replayed, int64(0), "mercure.history.events_replayed must be present and non-negative")
	assert.LessOrEqual(t, replayed, int64(3), "events_replayed cannot exceed the 3 matching events dispatched")
	assert.False(t, spanAttrValue(t, historySpan, "mercure.history.future_dated").AsBool(), "earliest replay is not future-dated")
	assert.False(t, spanAttrValue(t, historySpan, "mercure.history.full_scan").AsBool(), "earliest replay resolves via Pass 1, no full-stream rescan")
}

// TestDispatchEntryExcludesFailedDispatch pins that events_replayed counts
// delivered updates, not attempts: a matched update whose Dispatch fails
// (here, to a disconnected subscriber) is not counted and marks the replay
// truncated.
func TestDispatchEntryExcludesFailedDispatch(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	transport, _ := newTestTransport(t, WithPrometheusRegisterer(reg))
	topics := []string{"https://example.com/books/1"}

	update := &mercure.Update{ID: "1", Data: "x", Topics: topics}
	data, err := (*transport.codec.Load()).Marshal(update)
	require.NoError(t, err)

	s := mercure.NewLocalSubscriber(mercure.EarliestLastEventID, nil, &mercure.TopicMatcherStore{})
	s.SetMatchers(topicMatchers(topics), nil)
	s.Disconnect() // Dispatch now returns false deterministically.

	rs := transport.newHistoryReplayState(s, "+")
	ok := rs.dispatchEntry(t.Context(), redis.XMessage{ID: "1-0", Values: map[string]any{"data": string(data)}})

	assert.False(t, ok, "Dispatch to a disconnected subscriber must return false")
	assert.Equal(t, 0, rs.eventsReplayed, "a matched-but-failed Dispatch must not increment events_replayed")
	assert.True(t, rs.truncated, "a matched-but-failed Dispatch must flag the replay as truncated")

	// dispatchEntry must also increment the truncated counter.
	families, err := reg.Gather()
	require.NoError(t, err)

	var truncatedCount float64

	for _, f := range families {
		if f.GetName() == metricHistoryReplayTruncatedTotal {
			truncatedCount = f.GetMetric()[0].GetCounter().GetValue()
		}
	}

	assert.InDelta(t, 1.0, truncatedCount, 1e-9,
		"dispatchEntry must increment mercure_redis_history_replay_truncated_total on a failed dispatch")
}

// TestDispatchEntryCountsSuccessfulDeliveries pins the success branch: each
// accepted update increments events_replayed and leaves truncated false.
func TestDispatchEntryCountsSuccessfulDeliveries(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	transport, _ := newTestTransport(t, WithPrometheusRegisterer(reg))
	topics := []string{"https://example.com/books/1"}

	// Connected subscriber, 1000-deep buffer: two history dispatches both fit.
	s := mercure.NewLocalSubscriber(mercure.EarliestLastEventID, nil, &mercure.TopicMatcherStore{})
	s.SetMatchers(topicMatchers(topics), nil)
	t.Cleanup(s.Disconnect)

	rs := transport.newHistoryReplayState(s, "+")

	for i := 1; i <= 2; i++ {
		u := &mercure.Update{ID: strconv.Itoa(i), Data: "x", Topics: topics}
		data, err := (*transport.codec.Load()).Marshal(u)
		require.NoError(t, err)
		ok := rs.dispatchEntry(t.Context(), redis.XMessage{ID: strconv.Itoa(i) + "-0", Values: map[string]any{"data": string(data)}})
		require.True(t, ok, "Dispatch to a connected subscriber with buffer headroom must succeed")
	}

	assert.Equal(t, 2, rs.eventsReplayed, "each accepted matched update must increment events_replayed (delivered, accumulating)")
	assert.False(t, rs.truncated, "successful deliveries must not flag the replay as truncated")

	// A clean replay must leave the truncated counter at 0.
	families, err := reg.Gather()
	require.NoError(t, err)

	var truncatedCount float64

	for _, f := range families {
		if f.GetName() == metricHistoryReplayTruncatedTotal {
			truncatedCount = f.GetMetric()[0].GetCounter().GetValue()
		}
	}

	assert.InDelta(t, 0.0, truncatedCount, 1e-9, "a clean replay must leave mercure_redis_history_replay_truncated_total at 0")
}

// TestDispatchHistorySpanOutcomeAttributes pins the two replay outcomes the
// history span reports beyond the Bolt transport's: the future-ID guard skip
// and the Pass 2 full-stream rescan.
func TestDispatchHistorySpanOutcomeAttributes(t *testing.T) {
	t.Parallel()

	topics := []string{"https://example.com/books/1"}

	t.Run("future-dated Last-Event-ID → future_dated=true, 0 replayed", func(t *testing.T) {
		t.Parallel()

		ctx, root, rec := withRecorderRoot(t)
		transport, _ := newTestTransport(t)

		require.NoError(t, transport.Dispatch(ctx, &mercure.Update{ID: "1", Topics: topics}))

		// A UUIDv7 dated 1h ahead trips the guard, so both passes are skipped.
		futureID := futureUUIDv7(time.Hour)
		s := mercure.NewLocalSubscriber(futureID, nil, &mercure.TopicMatcherStore{})
		s.SetMatchers(topicMatchers(topics), nil)
		require.NoError(t, transport.AddSubscriber(ctx, s))
		t.Cleanup(s.Disconnect)
		root.End()

		span := findEndedSpan(rec, "mercure.transport.history")
		require.NotNil(t, span, "history span must be emitted; got %v", endedSpanNames(rec))
		assert.True(t, spanAttrValue(t, span, "mercure.history.future_dated").AsBool(), "future-dated requested ID must set future_dated=true")
		assert.Equal(t, int64(0), spanAttrValue(t, span, "mercure.history.events_replayed").AsInt64(), "a skipped replay delivers no events")
		assert.False(t, spanAttrValue(t, span, "mercure.history.full_scan").AsBool(), "the guard short-circuits before Pass 2")
		assert.False(t, spanAttrValue(t, span, "mercure.history.truncated").AsBool(), "a skipped replay dispatches nothing, so it cannot be truncated")
	})

	t.Run("non-UUIDv7 custom ID → full_scan=true (Pass 2 fallback)", func(t *testing.T) {
		t.Parallel()

		ctx, root, rec := withRecorderRoot(t)
		transport, _ := newTestTransport(t)

		for i := 1; i <= 2; i++ {
			require.NoError(t, transport.Dispatch(ctx, &mercure.Update{ID: strconv.Itoa(i), Topics: topics}))
		}

		// A custom (non-UUIDv7) Last-Event-ID can't be decoded to a stream
		// seek position, so Pass 1 never matches it and the replay falls back
		// to the Pass 2 full-stream rescan.
		s := mercure.NewLocalSubscriber("custom-non-uuid-id", nil, &mercure.TopicMatcherStore{})
		s.SetMatchers(topicMatchers(topics), nil)
		require.NoError(t, transport.AddSubscriber(ctx, s))
		t.Cleanup(s.Disconnect)
		root.End()

		span := findEndedSpan(rec, "mercure.transport.history")
		require.NotNil(t, span, "history span must be emitted; got %v", endedSpanNames(rec))
		assert.True(t, spanAttrValue(t, span, "mercure.history.full_scan").AsBool(), "an undecodable custom ID must fall back to the Pass 2 full-stream rescan")
		assert.False(t, spanAttrValue(t, span, "mercure.history.future_dated").AsBool(), "a custom ID is not future-dated")
		// The full-scan path also emits truncated + a delivered count. Exact
		// values are timing-dependent (the live xreadgroupListener can disconnect
		// the subscriber mid-replay), so assert presence + a sane bound, not
		// fixed values. spanAttrValue fatals if the attribute is absent.
		_ = spanAttrValue(t, span, "mercure.history.truncated")
		replayed := spanAttrValue(t, span, "mercure.history.events_replayed").AsInt64()
		assert.GreaterOrEqual(t, replayed, int64(0))
		assert.LessOrEqual(t, replayed, int64(2), "full-scan replay can't deliver more than the 2 dispatched events")
	})
}

// TestDispatchHistorySpanRecordsErrorStatus pins that a failed replay sets the
// history span status to codes.Error with a description, as the Bolt transport
// does. Closing miniredis makes the replay's XRANGE fail.
func TestDispatchHistorySpanRecordsErrorStatus(t *testing.T) {
	t.Parallel()

	ctx, root, rec := withRecorderRoot(t)

	transport, mr := newTestTransport(t)

	// Dispatch one event so the stream key exists; if the stream is missing,
	// the replay path can short-circuit before reaching the XRANGE call we want
	// to fail.
	require.NoError(t, transport.Dispatch(ctx, &mercure.Update{
		ID:     "1",
		Topics: []string{"https://example.com/books/1"},
	}))

	// Kill the backend; the subsequent XRANGE inside dispatchHistory will fail.
	mr.Close()

	s := mercure.NewLocalSubscriber(mercure.EarliestLastEventID, nil, &mercure.TopicMatcherStore{})
	s.SetMatchers(topicMatchers([]string{"https://example.com/books/1"}), nil)

	// AddSubscriber returns an error because dispatchHistory's XRANGE failed.
	addErr := transport.AddSubscriber(ctx, s)
	require.Error(t, addErr, "AddSubscriber must surface XRANGE failures from dispatchHistory")
	// The error must come from the replay, not from an earlier failure.
	require.Contains(t, addErr.Error(), "history replay",
		"failure must come from the history-replay path")

	t.Cleanup(s.Disconnect)

	root.End()

	historySpan := findEndedSpan(rec, "mercure.transport.history")
	require.NotNil(t, historySpan, "mercure.transport.history span must still emit even on failure paths; got %v", endedSpanNames(rec))

	assert.Equal(t, codes.Error, historySpan.Status().Code,
		"history span status must be codes.Error on XRANGE failure")
	assert.NotEmpty(t, historySpan.Status().Description,
		"history span status description must carry the error message")
}

func assertTransportAttrOnRoot(t *testing.T, rec *tracetest.SpanRecorder) {
	t.Helper()

	rootSpan := findEndedSpan(rec, "root")
	require.NotNil(t, rootSpan, "root span must be in the recorder Ended set; got %v", endedSpanNames(rec))

	got := spanAttrMap(rootSpan)["mercure.transport"]
	assert.Equal(t, "redis", got, "root span must carry the mercure.transport=redis attribute set by Dispatch or AddSubscriber")
}

// findEndedSpan returns the first ended span named name, or nil. Each test
// here attaches one subscriber, so there is one span per name.
func findEndedSpan(rec *tracetest.SpanRecorder, name string) sdktrace.ReadOnlySpan {
	for _, span := range rec.Ended() {
		if span.Name() == name {
			return span
		}
	}

	return nil
}

func spanAttrMap(span sdktrace.ReadOnlySpan) map[string]string {
	out := make(map[string]string, len(span.Attributes()))
	for _, kv := range span.Attributes() {
		out[string(kv.Key)] = kv.Value.AsString()
	}

	return out
}

// spanAttrValue returns the typed attribute.Value for key (use AsInt64/AsBool
// on the result) — spanAttrMap is string-only and returns "" for Int/Bool
// attributes like the mercure.history.* outcome fields.
func spanAttrValue(t *testing.T, span sdktrace.ReadOnlySpan, key string) attribute.Value {
	t.Helper()

	for _, kv := range span.Attributes() {
		if string(kv.Key) == key {
			return kv.Value
		}
	}

	t.Fatalf("span %q missing attribute %q; have %v", span.Name(), key, spanAttrMap(span))

	return attribute.Value{}
}

// TestTagTransportOnParentIsRecordingGuard pins that tagTransportOnParent is
// safe on a context with no span.
func TestTagTransportOnParentIsRecordingGuard(t *testing.T) {
	t.Parallel()

	// context.Background has no span; SpanFromContext returns a non-recording span.
	require.NotPanics(t, func() { tagTransportOnParent(context.Background()) },
		"tagTransportOnParent must not panic on a context with no span")
}

func endedSpanNames(rec *tracetest.SpanRecorder) []string {
	spans := rec.Ended()
	names := make([]string, len(spans))

	for i, s := range spans {
		names[i] = s.Name()
	}

	return names
}
