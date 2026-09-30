package redistransport

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/dunglas/mercure"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// jsonLogRecords decodes the JSON log lines in logs whose msg is msg.
func jsonLogRecords(t *testing.T, logs *safeBuffer, msg string) []map[string]any {
	t.Helper()

	var records []map[string]any

	for line := range strings.Lines(logs.String()) {
		var record map[string]any

		require.NoError(t, json.Unmarshal([]byte(line), &record))

		if record["msg"] == msg {
			records = append(records, record)
		}
	}

	return records
}

// Close takes the subscribers it disconnects out of subscriberCount and the
// shard gauges.
func TestCloseCountsSubscribersOut(t *testing.T) {
	t.Parallel()

	const (
		shards = 4
		nSubs  = 20
	)

	transport, _ := newShardedTestTransport(t, shards, WithPrometheusRegisterer(prometheus.NewRegistry()))
	tss := testTopicMatcherStore()
	ctx := context.Background()

	for range nSubs {
		s := mercure.NewLocalSubscriber("", testLogger(), tss)
		s.SetMatchers(topicMatchers([]string{"https://example.com/close-count"}), nil)
		require.NoError(t, transport.AddSubscriber(ctx, s))
	}

	require.EqualValues(t, nSubs, transport.subscriberCount.Load())
	require.NoError(t, transport.Close(ctx))

	assert.EqualValues(t, 0, transport.subscriberCount.Load(), "subscriberCount after Close")

	for shard, v := range shardGaugeValues(t, transport) {
		assert.InDelta(t, 0.0, v, 0, "shard %d gauge after Close", shard)
	}
}

var errInjectedPublish = errors.New("injected publish failure")

// closingEvalHook, once armed, runs onEval in place of the next EVAL/EVALSHA
// and fails that command.
type closingEvalHook struct {
	armed  atomic.Bool
	onEval func()
}

func (*closingEvalHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *closingEvalHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if (cmd.Name() == "evalsha" || cmd.Name() == "eval") && h.armed.CompareAndSwap(true, false) {
			h.onEval()

			return errInjectedPublish
		}

		return next(ctx, cmd)
	}
}

func (*closingEvalHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

// A publish that fails because Close ran under it returns the closed-transport
// sentinel, and the script error it replaces is logged at Debug.
func TestDispatchLogsPublishErrorReplacedByClosedSentinel(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	// Installed before NewRedisTransport so it cannot race the listener.
	hook := &closingEvalHook{}
	client.AddHook(hook)

	var logs safeBuffer

	transport, err := NewRedisTransport(
		client,
		withSkipVersionCheck(),
		WithLogger(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))),
		WithXReadBlock(50*time.Millisecond),
		WithHealthInterval(24*time.Hour),
		WithPresenceInterval(24*time.Hour),
		withSkipPresenceIntervalCheck(),
	)
	require.NoError(t, err)
	t.Cleanup(func() { transport.Close(context.Background()) })

	hook.onEval = func() { assert.NoError(t, transport.Close(context.Background())) }
	hook.armed.Store(true)

	err = transport.Dispatch(context.Background(), &mercure.Update{
		Topics: []string{"https://example.com/publish-close"},
		Data:   "x",
	})
	require.ErrorIs(t, err, ErrClosedTransport)

	records := jsonLogRecords(t, &logs, "redis transport: publish failed on a closed transport; returning the closed-transport error")
	require.Len(t, records, 1, "logs: %s", logs.String())
	assert.Equal(t, slog.LevelDebug.String(), records[0]["level"])
	assert.Contains(t, records[0]["error"], errInjectedPublish.Error())
}

// An oversized Last-Event-ID is truncated in each log line that reports it.
func TestRequestedEventIDTruncatedInLogs(t *testing.T) {
	t.Parallel()

	longID := strings.Repeat("x", maxObservedEventIDLen+20)
	wantID := longID[:maxObservedEventIDLen] + "…(truncated)"

	for _, tc := range []struct {
		name string
		msg  string
		run  func(t *testing.T, transport *RedisTransport, mr *miniredis.Miniredis, s *mercure.LocalSubscriber)
	}{
		{
			name: "replay failure on a closed transport",
			msg:  "redis transport: history replay failed on a closed transport; returning the closed-transport error",
			run: func(t *testing.T, transport *RedisTransport, _ *miniredis.Miniredis, s *mercure.LocalSubscriber) {
				t.Helper()

				require.NoError(t, transport.Close(context.Background()))
				require.ErrorIs(t, transport.replayHistory(context.Background(), s, streamIDEarliest), ErrClosedTransport)
			},
		},
		{
			name: "pass 2 fallback",
			msg:  "redis transport: history replay falling back to full-stream Pass 2",
			run: func(t *testing.T, transport *RedisTransport, _ *miniredis.Miniredis, s *mercure.LocalSubscriber) {
				t.Helper()

				require.NoError(t, transport.AddSubscriber(context.Background(), s))
			},
		},
		{
			name: "XRANGE failure",
			msg:  "redis transport: history replay XRANGE failed",
			run: func(t *testing.T, transport *RedisTransport, mr *miniredis.Miniredis, s *mercure.LocalSubscriber) {
				t.Helper()

				mr.SetError("ERR injected")
				defer mr.SetError("")

				require.Error(t, transport.AddSubscriber(context.Background(), s))
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var logs safeBuffer

			transport, mr := newTestTransport(t, WithLogger(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))))

			s := mercure.NewLocalSubscriber(longID, testLogger(), testTopicMatcherStore())
			s.SetMatchers(topicMatchers([]string{"https://example.com/truncation"}), nil)

			tc.run(t, transport, mr, s)

			records := jsonLogRecords(t, &logs, tc.msg)
			require.NotEmpty(t, records, "logs: %s", logs.String())

			for _, record := range records {
				assert.Equal(t, wantID, record["requested_event_id"])
			}
		})
	}
}

// Closing one of two transports that share a registry takes only its own
// subscribers out of the shared shard gauge.
func TestCloseLeavesSiblingShareOfSharedShardGauge(t *testing.T) {
	t.Parallel()

	const shardSubscribers = "mercure_redis_shard_subscribers"

	closing, _ := newTestTransport(t)
	sibling, _ := newTestTransport(t)
	reg := prometheus.NewRegistry()

	require.NoError(t, closing.RegisterMetricsWith(reg))
	require.NoError(t, sibling.RegisterMetricsWith(reg))

	addRebindSubscribers(t, closing, 2)
	addRebindSubscribers(t, sibling, 3)

	require.InDelta(t, 5, sumValues(gaugeSeriesValues(t, reg)[shardSubscribers]), 0, "precondition: the shared gauge counts both transports' subscribers")

	require.NoError(t, closing.Close(context.Background()))

	assert.InDelta(t, 3, sumValues(gaugeSeriesValues(t, reg)[shardSubscribers]), 0, "the sibling's share of the shared gauge must remain")
}
