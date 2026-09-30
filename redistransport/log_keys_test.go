package redistransport

// These tests pin the structured-log keys that carry a diagnostic value, such
// as a stream entry ID, a count, a consumer group or a Redis key. A log line
// that drops or renames one of these keys fails here.

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dunglas/mercure"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// jsonTestLogger returns a JSON logger at Debug level writing to buf.
func jsonTestLogger(buf *safeBuffer) *slog.Logger {
	return slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// logRecords returns the JSON log records in buf whose message is msg.
func logRecords(t *testing.T, buf *safeBuffer, msg string) []map[string]any {
	t.Helper()

	var records []map[string]any

	for line := range strings.SplitSeq(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}

		var record map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &record))

		if record["msg"] == msg {
			records = append(records, record)
		}
	}

	return records
}

// Each decodeStreamEntry drop logs the dropped entry's stream ID under
// stream_entry_id.
func TestDecodeStreamEntryLogsStreamEntryID(t *testing.T) {
	t.Parallel()

	codec := mercure.JSONCodec{}
	forbidden, err := codec.Marshal(&mercure.Update{ID: "a\nb"})
	require.NoError(t, err)

	for _, tc := range []struct {
		msg    string
		values map[string]any
	}{
		{"redis transport: unexpected data type in stream entry", map[string]any{"other": "x"}},
		{"redis transport: unmarshal error in stream entry", map[string]any{"data": "not json"}},
		{
			"redis transport: dropping stream entry whose id/type is not a valid SSE field value (invalid UTF-8, a control character or a Unicode format character)",
			map[string]any{"data": string(forbidden)},
		},
	} {
		buf := &safeBuffer{}
		entry := redis.XMessage{ID: "1-0", Values: tc.values}

		require.Nil(t, decodeStreamEntry(entry, codec, jsonTestLogger(buf), nil, nil))

		records := logRecords(t, buf, tc.msg)
		require.Len(t, records, 1, tc.msg)
		assert.Equal(t, "1-0", records[0]["stream_entry_id"], tc.msg)
	}
}

// A history replay that skips undeliverable entries logs how many under
// skipped_entry_count.
func TestHistoryReplayLogsSkippedEntryCount(t *testing.T) {
	t.Parallel()

	buf := &safeBuffer{}
	tr, _ := newTestTransport(t, WithLogger(jsonTestLogger(buf)))

	ctx := context.Background()
	id, err := tr.client.XAdd(ctx, &redis.XAddArgs{Stream: tr.key(""), Values: map[string]any{"other": "x"}}).Result()
	require.NoError(t, err)

	// Replay reads up to the listener's cursor; wait until the listener has
	// read past the entry (it advances the cursor over undecodable entries).
	require.Eventually(t, func() bool { return *tr.lastDispatchedStreamID.Load() == id }, 5*time.Second, 10*time.Millisecond)

	sub := mercure.NewLocalSubscriber(mercure.EarliestLastEventID, testLogger(), testTopicMatcherStore())
	sub.SetMatchers(topicMatchers([]string{"https://example.com/skipped"}), nil)
	require.NoError(t, tr.AddSubscriber(ctx, sub))

	const msg = "redis transport: history replay skipped undeliverable entries (decode failure, or an id/type that is not a valid SSE field value; breakdown in mercure_redis_stream_decode_errors_total{kind})"

	require.Eventually(t, func() bool { return len(logRecords(t, buf, msg)) > 0 }, 5*time.Second, 10*time.Millisecond)
	assert.InDelta(t, 1, logRecords(t, buf, msg)[0]["skipped_entry_count"], 0)
}

// A failed batch XACK logs the batch size under ack_entry_count.
func TestBatchXAckFailureLogsAckEntryCount(t *testing.T) {
	t.Parallel()

	buf := &safeBuffer{}
	hook := &cmdErrHook{errs: map[string]error{"xack": replyError("ERR injected XACK failure")}}
	hook.on.Store(true)
	tr := newHookedTestTransport(t, []redis.Hook{hook}, WithLogger(jsonTestLogger(buf)))

	require.NoError(t, tr.Dispatch(context.Background(), &mercure.Update{
		Topics: []string{"https://example.com/xack"},
		Data:   "x",
	}))

	const msg = "redis transport: batch XAck failed"

	require.Eventually(t, func() bool { return len(logRecords(t, buf, msg)) > 0 }, 5*time.Second, 10*time.Millisecond)
	assert.InDelta(t, 1, logRecords(t, buf, msg)[0]["ack_entry_count"], 0)
}

// timeOffsetHook shifts the server TIME reply by offset nanoseconds.
type timeOffsetHook struct{ offset atomic.Int64 }

func (*timeOffsetHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *timeOffsetHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		err := next(ctx, cmd)
		if tc, ok := cmd.(*redis.TimeCmd); ok && err == nil {
			tc.SetVal(tc.Val().Add(time.Duration(h.offset.Load())))
		}

		return err
	}
}

func (*timeOffsetHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

// detectClockDrift warns, with threshold_ms 1000, only when the server clock
// differs from the hub's by more than a second.
func TestDetectClockDriftWarnsAboveThreshold(t *testing.T) {
	t.Parallel()

	const msg = "redis transport: clock drift between app server and stream server"

	buf := &safeBuffer{}
	hook := &timeOffsetHook{}
	tr := newHookedTestTransport(t, []redis.Hook{hook}, WithLogger(jsonTestLogger(buf)))

	hook.offset.Store(int64(500 * time.Millisecond))
	tr.detectClockDrift(context.Background())
	assert.Empty(t, logRecords(t, buf, msg), "a 500ms drift is under the threshold")

	hook.offset.Store(int64(2 * time.Second))
	tr.detectClockDrift(context.Background())

	records := logRecords(t, buf, msg)
	require.Len(t, records, 1, "a 2s drift is over the threshold")
	assert.InDelta(t, 1000, records[0]["threshold_ms"], 0)
	assert.InDelta(t, 2000, records[0]["drift_ms"], 200)
}

// An undecodable presence value logs its key under presence_key.
func TestDecodePresenceValueLogsPresenceKey(t *testing.T) {
	t.Parallel()

	buf := &safeBuffer{}
	tr, _ := newTestTransport(t, WithLogger(jsonTestLogger(buf)))

	_, ok := tr.decodePresenceValue("not a presence value", "presence:x")
	require.False(t, ok)

	records := logRecords(t, buf, "redis transport: failed to unmarshal presence data")
	require.Len(t, records, 1)
	assert.Equal(t, "presence:x", records[0]["presence_key"])
}

// A failed presence check during zombie GC logs the key under presence_key.
func TestZombieGCPresenceCheckFailureLogsPresenceKey(t *testing.T) {
	t.Parallel()

	buf := &safeBuffer{}
	hook := &cmdErrHook{errs: map[string]error{"exists": replyError("ERR injected EXISTS failure")}}
	tr := newHookedTestTransport(t, []redis.Hook{hook}, WithLogger(jsonTestLogger(buf)))
	hook.on.Store(true)

	tr.gcMu.Lock()
	tr.gcZombieGroup(context.Background(), tr.key(""), nodeGroupPrefix+"deadnode")
	tr.gcMu.Unlock()

	records := logRecords(t, buf, "redis transport: failed to check presence key during GC")
	require.Len(t, records, 1)
	assert.Equal(t, tr.presenceKey("deadnode"), records[0]["presence_key"])
}

// Each destroyZombieGroup outcome logs the group under consumer_group.
func TestDestroyZombieGroupLogsConsumerGroup(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		setup  func(t *testing.T, tr *RedisTransport, hook *cmdErrHook, group, presenceKey string)
		msg    string
		inject bool
	}{
		{
			name: "destroyed",
			setup: func(t *testing.T, tr *RedisTransport, _ *cmdErrHook, group, _ string) {
				t.Helper()
				require.NoError(t, tr.client.XGroupCreateMkStream(context.Background(), tr.key(""), group, "$").Err())
			},
			msg: "redis transport: destroyed zombie consumer group",
		},
		{
			name: "kept",
			setup: func(t *testing.T, tr *RedisTransport, _ *cmdErrHook, group, presenceKey string) {
				t.Helper()
				require.NoError(t, tr.client.XGroupCreateMkStream(context.Background(), tr.key(""), group, "$").Err())
				require.NoError(t, tr.client.Set(context.Background(), presenceKey, "x", 0).Err())
			},
			msg: "redis transport: zombie GC kept group, presence key renewed",
		},
		{
			name:  "already destroyed",
			setup: func(*testing.T, *RedisTransport, *cmdErrHook, string, string) {},
			msg:   "redis transport: zombie GC found group already destroyed",
		},
		{
			name:   "script failure",
			setup:  func(*testing.T, *RedisTransport, *cmdErrHook, string, string) {},
			msg:    "redis transport: failed to destroy zombie consumer group",
			inject: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			buf := &safeBuffer{}
			hook := &cmdErrHook{errs: map[string]error{
				"evalsha": replyError("ERR injected script failure"),
				"eval":    replyError("ERR injected script failure"),
			}}
			tr := newHookedTestTransport(t, []redis.Hook{hook}, WithLogger(jsonTestLogger(buf)))

			group := nodeGroupPrefix + "zombie"
			presenceKey := tr.presenceKey("zombie")
			tc.setup(t, tr, hook, group, presenceKey)
			hook.on.Store(tc.inject)

			tr.gcMu.Lock()
			tr.destroyGroupIfAbsentScript = redis.NewScript(destroyGroupIfAbsentScriptText)
			tr.gcSuspects = make(map[string]time.Time)
			tr.destroyZombieGroup(context.Background(), tr.key(""), presenceKey, group)
			tr.gcMu.Unlock()

			records := logRecords(t, buf, tc.msg)
			require.Len(t, records, 1)
			assert.Equal(t, group, records[0]["consumer_group"])
		})
	}
}

// The NOGROUP recreate warning names this node's group under consumer_group.
func TestRecreateConsumerGroupLogsConsumerGroup(t *testing.T) {
	t.Parallel()

	buf := &safeBuffer{}
	tr, _ := newTestTransport(t, WithLogger(jsonTestLogger(buf)))

	require.NoError(t, tr.client.XGroupDestroy(context.Background(), tr.key(""), tr.nodeGroup).Err())

	const msg = "redis transport: consumer group destroyed, re-creating"

	require.Eventually(t, func() bool { return len(logRecords(t, buf, msg)) > 0 }, 5*time.Second, 10*time.Millisecond)
	assert.Equal(t, tr.nodeGroup, logRecords(t, buf, msg)[0]["consumer_group"])
}

// Both consumer-lag sampling log lines name this node's group under
// consumer_group.
func TestSampleConsumerLagLogsConsumerGroup(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		reply replyError
		msg   string
	}{
		{replyError("ERR no such key"), "redis transport: consumer-group lag sample skipped; stream not present"},
		{replyError("ERR injected XINFO failure"), "redis transport: failed to sample consumer-group lag"},
	} {
		buf := &safeBuffer{}
		hook := &cmdErrHook{errs: map[string]error{"xinfo": tc.reply}}
		tr := newHookedTestTransport(t, []redis.Hook{hook},
			WithLogger(jsonTestLogger(buf)), WithPrometheusRegisterer(prometheus.NewRegistry()))
		hook.on.Store(true)

		tr.sampleConsumerLag(context.Background(), tr.metrics.Load())

		records := logRecords(t, buf, tc.msg)
		require.Len(t, records, 1, tc.msg)
		assert.Equal(t, tr.nodeGroup, records[0]["consumer_group"], tc.msg)
	}
}

// Binding an additional registry logs how many collectors it registered and
// how many it skipped because a sibling had already registered them.
func TestRegisterMetricsWithLogsCollectorCounts(t *testing.T) {
	t.Parallel()

	buf := &safeBuffer{}
	tr, _ := newTestTransport(t, WithLogger(jsonTestLogger(buf)), WithPrometheusRegisterer(prometheus.NewRegistry()))

	require.NoError(t, tr.RegisterMetricsWith(prometheus.NewRegistry()))

	records := logRecords(t, buf, "redis transport: metrics registered on an additional registry")
	require.Len(t, records, 1)
	assert.Greater(t, records[0]["registered_collector_count"], float64(0))
	assert.InDelta(t, 0, records[0]["skipped_collector_count"], 0)
}

// A dispatch_shards above the ceiling logs the uncapped count and the ceiling.
func TestDispatchShardsCapLogsCounts(t *testing.T) {
	t.Parallel()

	buf := &safeBuffer{}
	newTestTransport(t, WithLogger(jsonTestLogger(buf)), WithDispatchShards(suspiciousDispatchShardsMax+1))

	records := logRecords(t, buf, "redis transport: dispatch shard count capped to the ceiling")
	require.Len(t, records, 1)
	assert.InDelta(t, suspiciousDispatchShardsMax+1, records[0]["uncapped_dispatch_shards"], 0)
	assert.InDelta(t, suspiciousDispatchShardsMax, records[0]["dispatch_shards_ceiling"], 0)
}

// A failed history-replay XRANGE logs the pass number and its cursor.
func TestHistoryReplayXRangeFailureLogsPassAndCursor(t *testing.T) {
	t.Parallel()

	buf := &safeBuffer{}
	hook := &cmdErrHook{errs: map[string]error{"xrange": replyError("ERR injected XRANGE failure")}}
	tr := newHookedTestTransport(t, []redis.Hook{hook}, WithLogger(jsonTestLogger(buf)))
	hook.on.Store(true)

	sub := mercure.NewLocalSubscriber(mercure.EarliestLastEventID, testLogger(), testTopicMatcherStore())
	sub.SetMatchers(topicMatchers([]string{"https://example.com/replay"}), nil)
	require.Error(t, tr.AddSubscriber(context.Background(), sub))

	records := logRecords(t, buf, "redis transport: history replay XRANGE failed")
	require.Len(t, records, 1)
	// An `earliest` request starts Pass 1 at the stream's first entry ("-").
	assert.InDelta(t, 1, records[0]["replay_pass"], 0)
	assert.Equal(t, "-", records[0]["replay_cursor"])
}

// Both presence summary-mode log lines carry the detail threshold.
func TestPresenceSummaryModeLogsDetailThreshold(t *testing.T) {
	t.Parallel()

	buf := &safeBuffer{}
	tr, _ := newTestTransport(t, WithLogger(jsonTestLogger(buf)), WithPresenceDetailThreshold(1))

	for range 2 {
		sub := mercure.NewLocalSubscriber("", testLogger(), testTopicMatcherStore())
		sub.SetMatchers(topicMatchers([]string{"https://example.com/presence"}), nil)
		require.NoError(t, tr.AddSubscriber(context.Background(), sub))
	}

	// Two members over a threshold of one: summary mode.
	tr.buildPresencePayload()

	// A count that lags membership: the walk overflows and falls back.
	tr.subscriberCount.Store(1)
	tr.buildPresencePayload()
	tr.subscriberCount.Store(2)

	for _, msg := range []string{
		"redis transport: presence in summary mode",
		"redis transport: presence summary-mode fallback (membership outran the count gate)",
	} {
		records := logRecords(t, buf, msg)
		require.Len(t, records, 1, msg)
		assert.InDelta(t, 1, records[0]["presence_detail_threshold"], 0, msg)
	}
}

// A remote node's summary-mode presence value logs its node ID under node_id.
func TestRemoteSummaryPresenceLogsNodeID(t *testing.T) {
	t.Parallel()

	buf := &safeBuffer{}
	tr, _ := newTestTransport(t, WithLogger(jsonTestLogger(buf)))

	_, err := tr.unmarshalPresenceValue(`{"node_id":"node-1","subscriber_count":3,"summary":true}`)
	require.NoError(t, err)

	records := logRecords(t, buf, "redis transport: remote node in summary presence mode")
	require.Len(t, records, 1)
	assert.Equal(t, "node-1", records[0]["node_id"])
}

// The startup version check logs the server's version under server_version.
func TestCheckVersionFromInfoLogsServerVersion(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		info, want string
	}{
		{"# Server\r\nredis_version:7.2.4\r\n", "7.2.4"},
		{"# Server\r\nredis_version:7.2.4\r\nvalkey_version:8.0.1\r\n", "8.0.1"},
	} {
		buf := &safeBuffer{}
		tr, _ := newTestTransport(t, WithLogger(jsonTestLogger(buf)))

		require.NoError(t, tr.checkVersionFromInfo(tc.info))

		records := logRecords(t, buf, "redis transport: connected")
		require.Len(t, records, 1, tc.info)
		assert.Equal(t, tc.want, records[0]["server_version"], tc.info)
	}
}

// Sharded dispatch logs its shard count under dispatch_shards.
func TestShardedDispatchLogsDispatchShards(t *testing.T) {
	t.Parallel()

	buf := &safeBuffer{}
	newTestTransport(t, WithLogger(jsonTestLogger(buf)), WithDispatchShards(3))

	records := logRecords(t, buf, "redis transport: sharded dispatch enabled")
	require.Len(t, records, 1)
	assert.InDelta(t, 3, records[0]["dispatch_shards"], 0)
}
