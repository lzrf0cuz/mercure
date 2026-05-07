package redistransport

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/dunglas/mercure"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultOptions(t *testing.T) {
	t.Parallel()

	o := defaultOptions()

	assert.Equal(t, "mercure", o.streamName)
	assert.Equal(t, int64(0), o.maxLength)
	assert.Equal(t, "json", o.encoding)
	assert.Equal(t, 60*time.Second, o.presenceTTL)
	assert.Equal(t, 30*time.Second, o.presenceInterval)
	assert.Equal(t, 5*time.Minute, o.zombieGCInterval)
	assert.Equal(t, 5*time.Second, o.clockSkewMargin)
	assert.Equal(t, time.Hour, o.eventTTL)
	assert.Equal(t, 5*time.Minute, o.cleanupInterval)
	assert.Equal(t, 10*time.Second, o.healthInterval)
	assert.Equal(t, 3, o.healthThreshold)
	assert.Equal(t, int64(100), o.xreadCount)
	assert.Equal(t, time.Second, o.xreadBlock)
	assert.Equal(t, 1, o.dispatchShards)
	assert.Equal(t, 100_000, o.subscriberListCacheSize)
	assert.InDelta(t, 0.0, o.subscriberRateLimit, 1e-9)
	assert.Equal(t, 5000, o.subscriberRateBurst)
	assert.Equal(t, 20, o.historyReplayConcurrency)
	assert.Equal(t, 1_000, o.presenceDetailThreshold)
	assert.Equal(t, int64(512*1024), o.presenceDetailByteThreshold)
	assert.Nil(t, o.prometheusRegisterer)
	assert.NotNil(t, o.logger, "default logger should never be nil")
}

func TestClockSkewMarginMs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		d    time.Duration
		want uint64
	}{
		{"zero", 0, 0},
		{"sub-millisecond truncates to zero", 500 * time.Microsecond, 0},
		{"one millisecond", time.Millisecond, 1},
		{"one second", time.Second, 1000},
		{"default five seconds", 5 * time.Second, 5000},
		{"negative clamps to zero", -1 * time.Second, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			o := &options{clockSkewMargin: tc.d}
			assert.Equal(t, tc.want, o.clockSkewMarginMs())
		})
	}
}

func TestWithStreamName(t *testing.T) {
	t.Parallel()

	o := defaultOptions()
	WithStreamName("custom")(o)
	assert.Equal(t, "custom", o.streamName)

	WithStreamName("")(o)
	assert.Equal(t, "custom", o.streamName)
}

func TestWithMaxLength(t *testing.T) {
	t.Parallel()

	o := defaultOptions()
	WithMaxLength(1000)(o)
	assert.Equal(t, int64(1000), o.maxLength)

	// Zero is legal (means unlimited).
	WithMaxLength(0)(o)
	assert.Equal(t, int64(0), o.maxLength)
}

func TestWithEncoding(t *testing.T) {
	t.Parallel()

	o := defaultOptions()
	WithEncoding("msgpack")(o)
	assert.Equal(t, "msgpack", o.encoding)

	WithEncoding("")(o)
	assert.Equal(t, "msgpack", o.encoding)
}

func TestWithPresenceTTL(t *testing.T) {
	t.Parallel()

	o := defaultOptions()
	WithPresenceTTL(2 * time.Minute)(o)
	assert.Equal(t, 2*time.Minute, o.presenceTTL)

	WithPresenceTTL(0)(o)
	assert.Equal(t, 2*time.Minute, o.presenceTTL)
	WithPresenceTTL(-1)(o)
	assert.Equal(t, 2*time.Minute, o.presenceTTL)
}

func TestWithPresenceInterval(t *testing.T) {
	t.Parallel()

	o := defaultOptions()
	WithPresenceInterval(15 * time.Second)(o)
	assert.Equal(t, 15*time.Second, o.presenceInterval)

	WithPresenceInterval(0)(o)
	assert.Equal(t, 15*time.Second, o.presenceInterval)
}

func TestWithZombieGCInterval(t *testing.T) {
	t.Parallel()

	o := defaultOptions()

	// Any value accepted, including 0 (= disabled) and negative.
	WithZombieGCInterval(0)(o)
	assert.Equal(t, time.Duration(0), o.zombieGCInterval)
	WithZombieGCInterval(10 * time.Minute)(o)
	assert.Equal(t, 10*time.Minute, o.zombieGCInterval)
}

func TestWithClockSkewMargin(t *testing.T) {
	t.Parallel()

	o := defaultOptions()
	WithClockSkewMargin(2 * time.Second)(o)
	assert.Equal(t, 2*time.Second, o.clockSkewMargin)

	// Zero is legal (disabled).
	WithClockSkewMargin(0)(o)
	assert.Equal(t, time.Duration(0), o.clockSkewMargin)

	WithClockSkewMargin(-1)(o)
	assert.Equal(t, time.Duration(0), o.clockSkewMargin)
}

func TestWithEventTTL(t *testing.T) {
	t.Parallel()

	o := defaultOptions()

	WithEventTTL(30 * time.Minute)(o)
	assert.Equal(t, 30*time.Minute, o.eventTTL)
	WithEventTTL(0)(o)
	assert.Equal(t, time.Duration(0), o.eventTTL)
}

func TestWithCleanupInterval(t *testing.T) {
	t.Parallel()

	o := defaultOptions()
	WithCleanupInterval(time.Minute)(o)
	assert.Equal(t, time.Minute, o.cleanupInterval)

	WithCleanupInterval(0)(o)
	assert.Equal(t, time.Minute, o.cleanupInterval)
}

func TestWithHealthInterval(t *testing.T) {
	t.Parallel()

	o := defaultOptions()
	WithHealthInterval(5 * time.Second)(o)
	assert.Equal(t, 5*time.Second, o.healthInterval)

	WithHealthInterval(0)(o)
	assert.Equal(t, 5*time.Second, o.healthInterval)
}

func TestWithHealthThreshold(t *testing.T) {
	t.Parallel()

	o := defaultOptions()
	WithHealthThreshold(5)(o)
	assert.Equal(t, 5, o.healthThreshold)

	WithHealthThreshold(0)(o)
	assert.Equal(t, 5, o.healthThreshold)
	WithHealthThreshold(-1)(o)
	assert.Equal(t, 5, o.healthThreshold)
}

func TestWithXReadCount(t *testing.T) {
	t.Parallel()

	o := defaultOptions()
	WithXReadCount(200)(o)
	assert.Equal(t, int64(200), o.xreadCount)

	WithXReadCount(0)(o)
	assert.Equal(t, int64(200), o.xreadCount)
}

func TestWithXReadBlock(t *testing.T) {
	t.Parallel()

	o := defaultOptions()
	WithXReadBlock(500 * time.Millisecond)(o)
	assert.Equal(t, 500*time.Millisecond, o.xreadBlock)

	WithXReadBlock(0)(o)
	assert.Equal(t, 500*time.Millisecond, o.xreadBlock)
}

func TestWithLogger(t *testing.T) {
	t.Parallel()

	o := defaultOptions()

	custom := slog.Default()
	WithLogger(custom)(o)
	assert.Same(t, custom, o.logger)

	// Nil logger is ignored — previously-set custom logger is preserved.
	WithLogger(nil)(o)
	assert.Same(t, custom, o.logger)
}

func TestWithDispatchShards(t *testing.T) {
	t.Parallel()

	o := defaultOptions()

	// Any value stored verbatim — auto-detect (from 0) happens in initShards.
	WithDispatchShards(4)(o)
	assert.Equal(t, 4, o.dispatchShards)
	WithDispatchShards(0)(o)
	assert.Equal(t, 0, o.dispatchShards)
	WithDispatchShards(-1)(o)
	assert.Equal(t, -1, o.dispatchShards)
}

func TestWithSubscriberListCacheSize(t *testing.T) {
	t.Parallel()

	o := defaultOptions()
	WithSubscriberListCacheSize(50_000)(o)
	assert.Equal(t, 50_000, o.subscriberListCacheSize)

	WithSubscriberListCacheSize(0)(o)
	assert.Equal(t, 50_000, o.subscriberListCacheSize)
	WithSubscriberListCacheSize(-1)(o)
	assert.Equal(t, 50_000, o.subscriberListCacheSize)
}

func TestWithSubscriberRateLimit(t *testing.T) {
	t.Parallel()

	o := defaultOptions()
	WithSubscriberRateLimit(100)(o)
	assert.InDelta(t, 100.0, o.subscriberRateLimit, 1e-9)

	// Zero is legal (= disabled).
	WithSubscriberRateLimit(0)(o)
	assert.InDelta(t, 0.0, o.subscriberRateLimit, 1e-9)

	WithSubscriberRateLimit(-1)(o)
	assert.InDelta(t, 0.0, o.subscriberRateLimit, 1e-9)
}

func TestWithSubscriberRateBurst(t *testing.T) {
	t.Parallel()

	o := defaultOptions()
	WithSubscriberRateBurst(1000)(o)
	assert.Equal(t, 1000, o.subscriberRateBurst)

	WithSubscriberRateBurst(0)(o)
	assert.Equal(t, 1000, o.subscriberRateBurst)
}

func TestWithHistoryReplayConcurrency(t *testing.T) {
	t.Parallel()

	o := defaultOptions()
	WithHistoryReplayConcurrency(20)(o)
	assert.Equal(t, 20, o.historyReplayConcurrency)

	WithHistoryReplayConcurrency(0)(o)
	assert.Equal(t, 20, o.historyReplayConcurrency)
}

func TestWithPresenceDetailThreshold(t *testing.T) {
	t.Parallel()

	o := defaultOptions()
	WithPresenceDetailThreshold(1000)(o)
	assert.Equal(t, 1000, o.presenceDetailThreshold)

	WithPresenceDetailThreshold(0)(o)
	assert.Equal(t, 1000, o.presenceDetailThreshold)
}

// TestWithPrometheusRegistererTypedNil regression-tests the Caddy
// `caddy validate` path: ctx.GetMetricsRegistry() returns a typed-nil
// *prometheus.Registry when no admin server is bound. A typed-nil
// interface is != nil but panics on method dispatch, so initMetrics
// would crash inside newMetrics → safeRegister. The Option must
// translate typed-nil to a true nil so the initMetrics nil-guard
// catches it.
func TestWithPrometheusRegistererTypedNil(t *testing.T) {
	t.Parallel()

	o := defaultOptions()

	var reg *prometheus.Registry // typed nil

	WithPrometheusRegisterer(reg)(o)
	assert.Nil(t, o.prometheusRegisterer,
		"typed-nil *prometheus.Registry must be normalized to a true nil interface")
}

func TestWithPrometheusRegisterer(t *testing.T) {
	t.Parallel()

	o := defaultOptions()
	assert.Nil(t, o.prometheusRegisterer, "default registerer is nil (metrics disabled)")

	reg := prometheus.NewRegistry()
	WithPrometheusRegisterer(reg)(o)
	assert.Same(t, reg, o.prometheusRegisterer)

	// nil is explicitly stored (disables metrics after being enabled).
	WithPrometheusRegisterer(nil)(o)
	assert.Nil(t, o.prometheusRegisterer)
}

func TestOptionsCompose(t *testing.T) {
	t.Parallel()

	o := defaultOptions()
	opts := []Option{
		WithStreamName("alt"),
		WithEncoding("gob"),
		WithDispatchShards(8),
		WithPresenceTTL(90 * time.Second),
		WithHealthThreshold(5),
	}

	for _, opt := range opts {
		opt(o)
	}

	assert.Equal(t, "alt", o.streamName)
	assert.Equal(t, "gob", o.encoding)
	assert.Equal(t, 8, o.dispatchShards)
	assert.Equal(t, 90*time.Second, o.presenceTTL)
	assert.Equal(t, 5, o.healthThreshold)
}

func TestValidateOptionsDefault(t *testing.T) {
	t.Parallel()

	require.NoError(t, validateOptions(defaultOptions()))
}

func TestValidateOptionsRejectsNegativeEventTTL(t *testing.T) {
	t.Parallel()

	o := defaultOptions()
	o.eventTTL = -time.Second

	err := validateOptions(o)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrInvalidOptions)
	assert.Contains(t, err.Error(), "WithEventTTL")
}

func TestValidateOptionsRejectsNegativeMaxLength(t *testing.T) {
	t.Parallel()

	o := defaultOptions()
	o.maxLength = -1

	err := validateOptions(o)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrInvalidOptions)
	assert.Contains(t, err.Error(), "WithMaxLength")
}

func TestValidateOptionsRejectsNegativeSubscriptionsMaxSubscribers(t *testing.T) {
	t.Parallel()

	o := defaultOptions()
	o.subscriptionsMaxSubscribers = -1

	err := validateOptions(o)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrInvalidOptions)
	assert.Contains(t, err.Error(), "WithSubscriptionsMaxSubscribers")
}

func TestValidateOptionsRejectsNegativeAdmissionKnobs(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		mutate func(*options)
		want   string
	}{
		{"max count", func(o *options) { o.subscriberMaxCount = -1 }, "WithSubscriberMaxCount"},
		{"admission timeout", func(o *options) { o.subscriberAdmissionTimeout = -1 }, "WithSubscriberAdmissionTimeout"},
		{"registration timeout", func(o *options) { o.subscriberRegTimeout = -1 }, "WithSubscriberRegistrationTimeout"},
		{"retry after", func(o *options) { o.subscriberRetryAfter = -1 }, "WithSubscriberRetryAfter"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			o := defaultOptions()
			tc.mutate(o)

			err := validateOptions(o)
			require.Error(t, err)
			require.ErrorIs(t, err, ErrInvalidOptions)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}

func TestValidateOptionsRejectsEqualPresenceIntervalAndTTL(t *testing.T) {
	t.Parallel()

	o := defaultOptions()
	o.presenceInterval = o.presenceTTL

	err := validateOptions(o)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrInvalidOptions)
	assert.Contains(t, err.Error(), "WithPresenceInterval")
}

func TestValidateOptionsRejectsIntervalGreaterThanTTL(t *testing.T) {
	t.Parallel()

	o := defaultOptions()
	o.presenceInterval = 2 * o.presenceTTL

	err := validateOptions(o)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrInvalidOptions)
}

func TestValidateOptionsJoinsMultipleErrors(t *testing.T) {
	t.Parallel()

	o := defaultOptions()
	o.eventTTL = -1
	o.maxLength = -1
	o.presenceInterval = o.presenceTTL
	o.encoding = "xyz-unknown"
	o.subscriptionsMaxSubscribers = -1
	o.subscriberMaxCount = -1
	o.subscriberRetryAfter = -1

	err := validateOptions(o)
	require.Error(t, err)

	errMsg := err.Error()
	assert.Contains(t, errMsg, "WithEventTTL")
	assert.Contains(t, errMsg, "WithMaxLength")
	assert.Contains(t, errMsg, "WithPresenceInterval")
	assert.Contains(t, errMsg, "WithEncoding")
	assert.Contains(t, errMsg, "WithSubscriptionsMaxSubscribers")
	assert.Contains(t, errMsg, "WithSubscriberMaxCount")
	assert.Contains(t, errMsg, "WithSubscriberRetryAfter")
}

func TestValidateOptionsRejectsUnknownEncoding(t *testing.T) {
	t.Parallel()

	o := defaultOptions()
	o.encoding = "xyz-unknown"

	err := validateOptions(o)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrInvalidOptions)
	assert.Contains(t, err.Error(), "WithEncoding")
}

// TestValidateOptionsEncodingPreservesUpstreamSentinel pins the contract that
// callers can match BOTH ErrInvalidOptions (the aggregate config-failure
// category) AND mercure.ErrUnsupportedCodec (the precise upstream cause).
// Locks in that the error chain preserves the upstream sentinel when
// validateOptions wraps the codec error — without errors.Is on the codec
// sentinel, callers lose the precise cause and can only see the aggregate.
func TestValidateOptionsEncodingPreservesUpstreamSentinel(t *testing.T) {
	t.Parallel()

	o := defaultOptions()
	o.encoding = "xyz-unknown"

	err := validateOptions(o)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrInvalidOptions,
		"must wrap ErrInvalidOptions for aggregate config-failure handling")
	require.ErrorIs(t, err, mercure.ErrUnsupportedCodec,
		"must preserve mercure.ErrUnsupportedCodec for precise codec-error matching")
}

func TestValidateOptionsAcceptsKnownEncodings(t *testing.T) {
	t.Parallel()

	for _, enc := range []string{"json", "gob", "msgpack"} {
		t.Run(enc, func(t *testing.T) {
			t.Parallel()

			o := defaultOptions()
			o.encoding = enc
			require.NoError(t, validateOptions(o))
		})
	}
}

// captureLogger returns a logger that records all records written through it.
// Used to assert warnSuspiciousOptions emits the expected warnings without
// coupling tests to stderr formatting.
func captureLogger() (*slog.Logger, *[]slog.Record) {
	var records []slog.Record

	h := &captureHandler{records: &records}

	return slog.New(h), &records
}

type captureHandler struct {
	records *[]slog.Record
}

func (h *captureHandler) Enabled(_ context.Context, _ slog.Level) bool { return true }
func (h *captureHandler) Handle(_ context.Context, r slog.Record) error {
	*h.records = append(*h.records, r)

	return nil
}
func (h *captureHandler) WithAttrs(_ []slog.Attr) slog.Handler { return h }
func (h *captureHandler) WithGroup(_ string) slog.Handler      { return h }

func findWarnMessage(records []slog.Record, substr string) *slog.Record {
	for i := range records {
		r := records[i]
		if r.Level == slog.LevelWarn && strings.Contains(r.Message, substr) {
			return &r
		}
	}

	return nil
}

func TestWarnSuspiciousOptionsShortXReadBlock(t *testing.T) {
	t.Parallel()

	logger, records := captureLogger()

	o := defaultOptions()
	o.xreadBlock = 1 * time.Millisecond

	warnSuspiciousOptions(o, logger)

	assert.NotNil(t, findWarnMessage(*records, "WithXReadBlock"))
}

// Excessive dispatch_shards is now enforced (clamped) at initShards rather
// than warned in warnSuspiciousOptions. The behavioral assertion lives in
// TestShardedDispatchHardCapEnforced (shard_test.go); the Warn log
// emitted alongside the clamp is informational and not asserted here.

func TestWarnSuspiciousOptionsHighXReadCount(t *testing.T) {
	t.Parallel()

	logger, records := captureLogger()

	o := defaultOptions()
	o.xreadCount = suspiciousXReadCountMax + 1

	warnSuspiciousOptions(o, logger)

	assert.NotNil(t, findWarnMessage(*records, "WithXReadCount"))
}

func TestWarnSuspiciousOptionsLargeSubscriberListCache(t *testing.T) {
	t.Parallel()

	logger, records := captureLogger()

	o := defaultOptions()
	o.subscriberListCacheSize = suspiciousSubscriberListCacheSize + 1

	warnSuspiciousOptions(o, logger)

	assert.NotNil(t, findWarnMessage(*records, "WithSubscriberListCacheSize"))
}

func TestWarnSuspiciousOptionsLargeClockSkewMargin(t *testing.T) {
	t.Parallel()

	logger, records := captureLogger()

	o := defaultOptions()
	o.clockSkewMargin = suspiciousClockSkewMarginMax + time.Second

	warnSuspiciousOptions(o, logger)

	assert.NotNil(t, findWarnMessage(*records, "WithClockSkewMargin"))
}

func TestWarnSuspiciousOptionsFlappyHealthThreshold(t *testing.T) {
	t.Parallel()

	logger, records := captureLogger()

	o := defaultOptions()
	o.healthThreshold = flappyHealthThreshold

	warnSuspiciousOptions(o, logger)

	assert.NotNil(t, findWarnMessage(*records, "WithHealthThreshold"))
}

func TestWarnSuspiciousOptionsDefaultsSilent(t *testing.T) {
	t.Parallel()

	logger, records := captureLogger()

	warnSuspiciousOptions(defaultOptions(), logger)

	assert.Empty(t, *records, "default options must not produce suspicious-option warnings")
}

// TestWarnSuspiciousOptionsAtThresholdSilent pins the boundary semantics:
// each soft-warning predicate uses strict comparison (< for the busy-poll
// threshold, > for the upper bounds), so values exactly at the threshold
// are treated as legitimate and produce no warning. Off-by-one regressions
// would silently flip these to warnings, polluting startup logs.
func TestWarnSuspiciousOptionsAtThresholdSilent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		mut  func(*options)
	}{
		{
			name: "xreadBlock at busy-poll threshold",
			mut:  func(o *options) { o.xreadBlock = xreadBlockBusyPollThreshold },
		},
		{
			name: "xreadCount at suspicious-max",
			mut:  func(o *options) { o.xreadCount = suspiciousXReadCountMax },
		},
		{
			name: "subscriberListCacheSize at suspicious-max",
			mut:  func(o *options) { o.subscriberListCacheSize = suspiciousSubscriberListCacheSize },
		},
		{
			name: "clockSkewMargin at suspicious-max",
			mut:  func(o *options) { o.clockSkewMargin = suspiciousClockSkewMarginMax },
		},
		{
			name: "healthThreshold one above flappy boundary",
			mut:  func(o *options) { o.healthThreshold = flappyHealthThreshold + 1 },
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			logger, records := captureLogger()

			o := defaultOptions()
			tc.mut(o)

			warnSuspiciousOptions(o, logger)

			assert.Empty(t, *records,
				"value at boundary must not warn — predicate uses strict comparison")
		})
	}
}
