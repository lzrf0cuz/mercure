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

// withRecorderRoot returns a context whose active span is a root span owned
// by an in-memory SpanRecorder. The rt startSpan helper at tracer.go resolves
// the TracerProvider via trace.SpanFromContext(ctx).TracerProvider(), so any
// rt operation invoked with the returned ctx has its spans captured by the
// returned recorder. The root span itself is returned so tests can End() it
// before reading the recorder (otherwise the recorder's Ended() set omits the
// parent that the rt tagging touched).
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

// TestDispatchTagsParentSpanWithTransport locks the wire: rt's Dispatch must
// annotate the active parent span (typically mercure.publish, but also
// mercure.subscribe for subscription-side update dispatches) with
// `mercure.transport=redis` so trace queries can filter by backend without
// a wall-clock-echo child span. Regression here means traces lose the
// transport-identification attribute and dashboards that scope by transport
// silently break.
func TestDispatchTagsParentSpanWithTransport(t *testing.T) {
	t.Parallel()

	ctx, root, rec := withRecorderRoot(t)

	transport, _ := newTestTransport(t)
	require.NoError(t, transport.Dispatch(ctx, &mercure.Update{
		Topics: []string{"https://example.com/books/1"},
		Event:  mercure.Event{Data: "hello"},
	}))

	root.End()

	assertTransportAttrOnRoot(t, rec)
}

// TestAddSubscriberTagsParentSpanWithTransport mirrors the Dispatch test for
// the subscribe path: rt's AddSubscriber must annotate the active
// (mercure.subscribe) span with `mercure.transport=redis`. Without this,
// subscribe traces are not filterable by transport even though the inner
// mercure.transport.history span carries the attr on a leaf.
func TestAddSubscriberTagsParentSpanWithTransport(t *testing.T) {
	t.Parallel()

	ctx, root, rec := withRecorderRoot(t)

	transport, _ := newTestTransport(t)

	s := mercure.NewLocalSubscriber("", nil, &mercure.TopicSelectorStore{})
	s.SetTopics([]string{"https://example.com/books/1"}, nil)

	require.NoError(t, transport.AddSubscriber(ctx, s))
	t.Cleanup(s.Disconnect)

	root.End()

	assertTransportAttrOnRoot(t, rec)
}

// TestDispatchHistoryEmitsTransportSpan locks the rt-side
// mercure.transport.history span shape. Mirrors the hub-side
// TestBoltHistoryEmitsSpan but exercises the Redis implementation: dispatch
// some events, request replay from "earliest", and assert one span ended
// with the bolt-compatible name + the rt-specific mercure.transport=redis
// attribute. Without this guard, a refactor that drops the span or changes
// the name (e.g. to mercure.transport.redis.history) silently breaks
// dashboards that aggregate history latency across backends.
func TestDispatchHistoryEmitsTransportSpan(t *testing.T) {
	t.Parallel()

	ctx, root, rec := withRecorderRoot(t)

	transport, _ := newTestTransport(t)

	topics := []string{"https://example.com/books/1"}
	for i := 1; i <= 3; i++ {
		require.NoError(t, transport.Dispatch(ctx, &mercure.Update{
			Event:  mercure.Event{ID: strconv.Itoa(i)},
			Topics: topics,
		}))
	}

	s := mercure.NewLocalSubscriber(mercure.EarliestLastEventID, nil, &mercure.TopicSelectorStore{})
	s.SetTopics(topics, nil)

	require.NoError(t, transport.AddSubscriber(ctx, s))
	t.Cleanup(s.Disconnect)

	root.End()

	historySpan := findEndedSpan(rec, "mercure.transport.history")
	require.NotNil(t, historySpan, "mercure.transport.history span must be emitted by rt dispatchHistory; got %v", endedSpanNames(rec))

	attrs := spanAttrMap(historySpan)
	assert.Equal(t, "redis", attrs["mercure.transport"], "mercure.transport attr must be redis on the rt history span")
	assert.Equal(t, s.ID, attrs["mercure.subscriber.id"], "mercure.subscriber.id attr must match the subscriber that triggered the replay")
	// Lock the third bolt-mirrored attr too — without this assertion a refactor that
	// drops mercure.last_event_id.requested (or renames it to e.g.
	// mercure.lastEventId.requested) passes the other two checks silently.
	assert.Equal(t, mercure.EarliestLastEventID, attrs["mercure.last_event_id.requested"],
		"mercure.last_event_id.requested attr must mirror the subscriber's request — drift loses cross-backend trace parity with the BoltDB transport's history span")

	// rt-specific enrichment beyond bolt: the replay outcome. The two booleans
	// are control-flow-deterministic for an "earliest" replay (no future-ID
	// skip, no Pass 2 rescan). events_replayed is bounded to [0,3] rather than
	// pinned to 3: newTestTransport runs a live xreadgroupListener that races
	// the replay (it can disconnect the subscriber mid-replay), so the exact
	// delivered count is timing-dependent (observed 0–3) — a positive floor
	// flakes. The [0,3] bound still catches a counter that double-counts or
	// tallies skipped/non-matching entries; the delivered-vs-attempted contract
	// is pinned deterministically by TestDispatchEntryExcludesFailedDispatch,
	// and the 0 case by the future-dated subtest below.
	replayed := spanAttrValue(t, historySpan, "mercure.history.events_replayed").AsInt64()
	assert.GreaterOrEqual(t, replayed, int64(0), "mercure.history.events_replayed must be present and non-negative")
	assert.LessOrEqual(t, replayed, int64(3), "events_replayed cannot exceed the 3 matching events dispatched")
	assert.False(t, spanAttrValue(t, historySpan, "mercure.history.future_dated").AsBool(), "earliest replay is not future-dated")
	assert.False(t, spanAttrValue(t, historySpan, "mercure.history.full_scan").AsBool(), "earliest replay resolves via Pass 1, no full-stream rescan")
}

// TestDispatchEntryExcludesFailedDispatch pins the "delivered, not attempted"
// contract of mercure.history.events_replayed: a matched update whose Dispatch
// fails (subscriber disconnected / buffer full) must NOT be counted. This is
// the one invariant the span-level tests cannot pin deterministically (the
// live-listener race makes the end-to-end count 0–3), so it is exercised
// directly: a disconnected subscriber makes Dispatch return false while Match
// still returns true, hitting the matched-but-failed branch exactly once.
// Without this, moving the increment out of the `if ok` block (counting
// attempts) passes every other test.
func TestDispatchEntryExcludesFailedDispatch(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	transport, _ := newTestTransport(t, WithPrometheusRegisterer(reg))
	topics := []string{"https://example.com/books/1"}

	update := &mercure.Update{Event: mercure.Event{ID: "1", Data: "x"}, Topics: topics}
	data, err := (*transport.codec.Load()).Marshal(update)
	require.NoError(t, err)

	s := mercure.NewLocalSubscriber(mercure.EarliestLastEventID, nil, &mercure.TopicSelectorStore{})
	s.SetTopics(topics, nil)
	s.Disconnect() // Dispatch now returns false deterministically.

	rs := transport.newHistoryReplayState(s, "+")
	ok := rs.dispatchEntry(t.Context(), redis.XMessage{ID: "1-0", Values: map[string]any{"data": string(data)}})

	assert.False(t, ok, "Dispatch to a disconnected subscriber must return false")
	assert.Equal(t, 0, rs.eventsReplayed, "a matched-but-failed Dispatch must NOT increment events_replayed")
	assert.True(t, rs.truncated, "a matched-but-failed Dispatch must flag the replay as truncated")

	// Also pin the wiring: dispatchEntry must increment the truncated counter,
	// not just the struct field. Without this, dropping the recordHistoryReplay
	// Truncated() call passes every other test (the metric unit test calls the
	// method directly; this drives it through dispatchEntry).
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

// TestDispatchEntryCountsSuccessfulDeliveries is the positive control for the
// success branch: matched updates a connected subscriber accepts increment
// events_replayed and leave truncated false. Without it, a regression that set
// truncated on the success branch — or stopped accumulating the count (= 1
// instead of ++) — passes every other test, since the failure-path test only
// exercises the else branch and the span-level tests can't pin an exact count.
func TestDispatchEntryCountsSuccessfulDeliveries(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	transport, _ := newTestTransport(t, WithPrometheusRegisterer(reg))
	topics := []string{"https://example.com/books/1"}

	// Connected subscriber, 1000-deep buffer: two history dispatches both fit.
	s := mercure.NewLocalSubscriber(mercure.EarliestLastEventID, nil, &mercure.TopicSelectorStore{})
	s.SetTopics(topics, nil)
	t.Cleanup(s.Disconnect)

	rs := transport.newHistoryReplayState(s, "+")

	for i := 1; i <= 2; i++ {
		u := &mercure.Update{Event: mercure.Event{ID: strconv.Itoa(i), Data: "x"}, Topics: topics}
		data, err := (*transport.codec.Load()).Marshal(u)
		require.NoError(t, err)
		ok := rs.dispatchEntry(t.Context(), redis.XMessage{ID: strconv.Itoa(i) + "-0", Values: map[string]any{"data": string(data)}})
		require.True(t, ok, "Dispatch to a connected subscriber with buffer headroom must succeed")
	}

	assert.Equal(t, 2, rs.eventsReplayed, "each accepted matched update must increment events_replayed (delivered, accumulating)")
	assert.False(t, rs.truncated, "successful deliveries must not flag the replay as truncated")

	// And the counter side: a clean replay must NOT touch the truncated
	// counter — pins that recordHistoryReplayTruncated stays in the failure
	// branch (a regression moving it to the success branch flips this to 2).
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

// TestDispatchHistorySpanOutcomeAttributes locks the two non-trivial replay
// outcomes the rt history span reports (and bolt cannot): the future-ID guard
// short-circuit and the Pass 2 full-stream rescan. Without this, a refactor
// that stops setting rs.path (replayPathFutureDated / replayPathFullScan) would
// silently blank these span attributes — operators would lose the per-trace
// signal for "replay skipped" vs "replay degraded to a full scan".
func TestDispatchHistorySpanOutcomeAttributes(t *testing.T) {
	t.Parallel()

	topics := []string{"https://example.com/books/1"}

	t.Run("future-dated Last-Event-ID → future_dated=true, 0 replayed", func(t *testing.T) {
		t.Parallel()

		ctx, root, rec := withRecorderRoot(t)
		transport, _ := newTestTransport(t)

		require.NoError(t, transport.Dispatch(ctx, &mercure.Update{Event: mercure.Event{ID: "1"}, Topics: topics}))

		// A UUIDv7 whose encoded ms timestamp is ~1h in the future trips the
		// guard (no skew margin can cover an hour), so both passes are skipped.
		// 48-bit ms in the first 12 hex digits, version 7 nibble, deterministic
		// rest (same layout as historyreplay_guard_test.go's makeUUIDv7).
		hexMs := zeroPadHex(time.Now().Add(time.Hour).UnixMilli(), 12)
		futureID := "urn:uuid:" + hexMs[:8] + "-" + hexMs[8:12] + "-7000-8000-000000000000"
		s := mercure.NewLocalSubscriber(futureID, nil, &mercure.TopicSelectorStore{})
		s.SetTopics(topics, nil)
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
			require.NoError(t, transport.Dispatch(ctx, &mercure.Update{Event: mercure.Event{ID: strconv.Itoa(i)}, Topics: topics}))
		}

		// A custom (non-UUIDv7) Last-Event-ID can't be decoded to a stream
		// seek position, so Pass 1 never matches it and the replay falls back
		// to the Pass 2 full-stream rescan.
		s := mercure.NewLocalSubscriber("custom-non-uuid-id", nil, &mercure.TopicSelectorStore{})
		s.SetTopics(topics, nil)
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

// TestDispatchHistorySpanRecordsErrorStatus is the failure-mode regression
// guard for the bolt-parity recordSpanError wiring on the
// mercure.transport.history span. Without it, a refactor that drops the
// `defer func() { if err != nil { recordSpanError(span, err) } }()` shape
// would silently make span status diverge from the BoltDB transport's
// history span — cross-backend dashboards filtering on span.Status would
// under-count Redis history failures to zero.
//
// We force a failure by closing miniredis (and the transport's client) before
// AddSubscriber attempts to replay history, so the XRANGE call inside
// dispatchHistory's run() returns an error.
func TestDispatchHistorySpanRecordsErrorStatus(t *testing.T) {
	t.Parallel()

	ctx, root, rec := withRecorderRoot(t)

	transport, mr := newTestTransport(t)

	// Dispatch one event so the stream key exists; if the stream is missing,
	// the replay path can short-circuit before reaching the XRANGE call we want
	// to fail.
	require.NoError(t, transport.Dispatch(ctx, &mercure.Update{
		Event:  mercure.Event{ID: "1"},
		Topics: []string{"https://example.com/books/1"},
	}))

	// Kill the backend; the subsequent XRANGE inside dispatchHistory will fail.
	mr.Close()

	s := mercure.NewLocalSubscriber(mercure.EarliestLastEventID, nil, &mercure.TopicSelectorStore{})
	s.SetTopics([]string{"https://example.com/books/1"}, nil)

	// AddSubscriber returns an error because dispatchHistory's XRANGE failed.
	addErr := transport.AddSubscriber(ctx, s)
	require.Error(t, addErr, "AddSubscriber must surface XRANGE failures from dispatchHistory")
	// Pin the failure path: the error must come from the XRANGE-replay code,
	// not from some earlier short-circuit (ctx-cancel, encode failure, etc.)
	// that would also produce a codes.Error span but fail to exercise the
	// recordSpanError-on-XRANGE wiring this test exists to guard.
	require.Contains(t, addErr.Error(), "history replay",
		"failure must be from the history-replay code path; without this assertion, a future refactor that fails earlier would still pass the span-status check below but no longer exercise recordSpanError on XRANGE")

	t.Cleanup(s.Disconnect)

	root.End()

	historySpan := findEndedSpan(rec, "mercure.transport.history")
	require.NotNil(t, historySpan, "mercure.transport.history span must still emit even on failure paths; got %v", endedSpanNames(rec))

	assert.Equal(t, codes.Error, historySpan.Status().Code,
		"history span status must be codes.Error on XRANGE failure — the BoltDB transport's dispatchHistory sets the same status; drift breaks cross-backend dashboards that filter on span.Status")
	assert.NotEmpty(t, historySpan.Status().Description,
		"history span status description must carry the error message — codes.Error with empty description is uninformative")
}

func assertTransportAttrOnRoot(t *testing.T, rec *tracetest.SpanRecorder) {
	t.Helper()

	rootSpan := findEndedSpan(rec, "root")
	require.NotNil(t, rootSpan, "root span must be in the recorder Ended set; got %v", endedSpanNames(rec))

	got := spanAttrMap(rootSpan)["mercure.transport"]
	assert.Equal(t, "redis", got, "root span must carry the mercure.transport=redis attribute set by the rt entry point (Dispatch or AddSubscriber)")
}

// findEndedSpan returns the FIRST ended span matching name, or nil. Current
// tests in this file attach exactly one subscriber per test, so the recorder
// holds exactly one span per name and "first" is unambiguous. A future
// multi-subscriber test that reuses this helper would silently lock the
// first span's attrs only; rename / refactor before that scenario lands.
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

// TestTagTransportOnParentIsRecordingGuard locks the documented "no-cost when
// tracing is off" behavior. A regression that dropped the IsRecording gate
// (e.g. someone "simplifying" the helper) would still pass the parent-tag
// tests above on real recorders, but would silently allocate an attribute
// KV on every Dispatch and AddSubscriber when no provider is wired. Test by
// calling against context.Background — no active span, IsRecording is false,
// the helper must early-return without panicking and without touching any
// span surface.
func TestTagTransportOnParentIsRecordingGuard(t *testing.T) {
	t.Parallel()

	// context.Background has no span; trace.SpanFromContext returns the OTel
	// noop span, IsRecording() == false. The helper must short-circuit
	// rather than calling SetAttributes on the noop span.
	require.NotPanics(t, func() { tagTransportOnParent(context.Background()) },
		"tagTransportOnParent must early-return on a non-recording ctx without panicking — the IsRecording gate is the cost-when-off contract")
}

func endedSpanNames(rec *tracetest.SpanRecorder) []string {
	spans := rec.Ended()
	names := make([]string, len(spans))

	for i, s := range spans {
		names[i] = s.Name()
	}

	return names
}
