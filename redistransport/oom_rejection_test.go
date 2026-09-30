package redistransport

import (
	"context"
	"slices"
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

// replyError is a server error reply. Like go-redis's own reply errors, it
// implements redis.Error, so redis.HasErrorPrefix (Script.Run's NOSCRIPT
// check) recognizes it.
type replyError string

func (e replyError) Error() string { return string(e) }

func (replyError) RedisError() {}

// Error replies used by the tests below.
const (
	errOOMReply = replyError("OOM command not allowed when used memory > 'maxmemory'.")
	// errOOMReplyLua62 is the OOM reply as a Redis 6.2 Lua script wraps it.
	errOOMReplyLua62   = replyError("ERR Error running script (call to f_0123): @user_script:1: @user_script: 1: -OOM command not allowed when used memory > 'maxmemory'.")
	errNoScriptReply   = replyError("NOSCRIPT No matching script. Please use EVAL.")
	errOtherWriteReply = replyError("ERR injected non-OOM failure")
)

// cmdErrHook fails each command named in errs (go-redis lower-case name) with
// its error while on. With waitDone set, it first waits for the command's ctx
// to be done, so the error arrives once the caller's budget has expired. errs
// is not modified after construction.
type cmdErrHook struct {
	errs     map[string]error
	waitDone bool
	on       atomic.Bool
}

func (*cmdErrHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *cmdErrHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if err, ok := h.errs[cmd.Name()]; ok && h.on.Load() {
			if h.waitDone {
				<-ctx.Done()
			}

			cmd.SetErr(err)

			return err
		}

		return next(ctx, cmd)
	}
}

func (*cmdErrHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

// newHookedTestTransport is newTestTransport with hooks added to the client.
func newHookedTestTransport(t *testing.T, hooks []redis.Hook, opts ...Option) *RedisTransport {
	t.Helper()

	mr := miniredis.RunT(t)

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	for _, h := range hooks {
		client.AddHook(h)
	}

	transport, err := NewRedisTransport(client, slices.Concat([]Option{
		WithLogger(testLogger()),
		WithXReadBlock(50 * time.Millisecond),
		WithHealthInterval(24 * time.Hour),
		WithPresenceInterval(noHeartbeatInterval),
		withSkipPresenceIntervalCheck(),
		withSkipVersionCheck(),
	}, opts)...)
	require.NoError(t, err)
	t.Cleanup(func() { transport.Close(context.Background()) })

	return transport
}

// oomRejections returns the oom_rejections_total value for every operation.
func oomRejections(t *testing.T, m *Metrics) map[oomOperation]float64 {
	t.Helper()

	got := make(map[oomOperation]float64)
	for _, op := range []oomOperation{oomOperationPublish, oomOperationPresence, oomOperationGroupCreate} {
		got[op] = counterValueForTest(t, m.oomRejections.WithLabelValues(string(op)))
	}

	return got
}

// A write the server refuses with its OOM error is counted under the
// operation that issued it. This includes the OOM error that a Redis 6.2 Lua
// script embeds in its own error. Any other write error is not counted.
func TestOOMRejectionsCountedByOperation(t *testing.T) {
	t.Parallel()

	// run drives the write each operation issues.
	run := map[oomOperation]func(tr *RedisTransport) error{
		oomOperationPublish: func(tr *RedisTransport) error {
			return tr.Dispatch(context.Background(), &mercure.Update{Topics: []string{"https://example.com/oom"}, Data: "x"})
		},
		oomOperationPresence: func(tr *RedisTransport) error {
			tr.publishPresence(context.Background())

			return nil
		},
		oomOperationGroupCreate: func(tr *RedisTransport) error {
			tr.recreateConsumerGroup(context.Background(), tr.key(""), 1)

			return nil
		},
	}

	cases := []struct {
		name string
		op   oomOperation
		errs map[string]error
		want bool
	}{
		{"publish EVALSHA OOM", oomOperationPublish, map[string]error{"evalsha": errOOMReply}, true},
		{"publish EVAL OOM wrapped by a Redis 6.2 script", oomOperationPublish, map[string]error{"evalsha": errNoScriptReply, "eval": errOOMReplyLua62}, true},
		{"publish non-OOM", oomOperationPublish, map[string]error{"evalsha": errOtherWriteReply}, false},
		{"presence SET OOM", oomOperationPresence, map[string]error{"set": errOOMReply}, true},
		{"presence SET non-OOM", oomOperationPresence, map[string]error{"set": errOtherWriteReply}, false},
		{"XGROUP CREATE OOM", oomOperationGroupCreate, map[string]error{"xgroup": errOOMReply}, true},
		{"XGROUP CREATE non-OOM", oomOperationGroupCreate, map[string]error{"xgroup": errOtherWriteReply}, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			hook := &cmdErrHook{errs: tc.errs}
			tr := newHookedTestTransport(t, []redis.Hook{hook}, WithPrometheusRegisterer(prometheus.NewRegistry()))
			m := tr.metrics.Load()
			require.NotNil(t, m)

			hook.on.Store(true)

			if err := run[tc.op](tr); tc.op == oomOperationPublish {
				require.Error(t, err)
			}

			want := map[oomOperation]float64{oomOperationPublish: 0, oomOperationPresence: 0, oomOperationGroupCreate: 0}
			if tc.want {
				want[tc.op] = 1
			}

			assert.Equal(t, want, oomRejections(t, m))

			// Each failure is also counted in its per-site error counter.
			switch tc.op {
			case oomOperationPublish:
				assert.InDelta(t, 1, counterValueForTest(t, m.publishErrors.WithLabelValues(string(publishErrorPublish))), 0)
			case oomOperationPresence:
				assert.InDelta(t, 1, counterValueForTest(t, m.presenceHeartbeatErrors), 0)
			case oomOperationGroupCreate:
				assert.InDelta(t, 1, counterValueForTest(t,
					m.xreadgroupErrors.WithLabelValues(string(xreadgroupErrorNoGroupRecreateFailed))), 0)
			}
		})
	}
}

// oomSeries returns the oom_rejections_total samples reg exposes, keyed by
// their operation label.
func oomSeries(t *testing.T, reg *prometheus.Registry) map[string]float64 {
	t.Helper()

	families, err := reg.Gather()
	require.NoError(t, err)

	got := make(map[string]float64)

	for _, f := range families {
		if f.GetName() != metricOOMRejectionsTotal {
			continue
		}

		for _, m := range f.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() == labelKeyOperation {
					got[lp.GetValue()] = m.GetCounter().GetValue()
				}
			}
		}
	}

	return got
}

// Every operation's series is exposed at 0 from the bind on, so an
// increase() alert sees the first rejection.
func TestOOMRejectionsSeriesStartAtZero(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	newTestTransport(t, WithPrometheusRegisterer(reg))

	assert.Equal(t, map[string]float64{"publish": 0, "presence": 0, "group_create": 0}, oomSeries(t, reg))
}

// An OOM refusal of the initial presence SET or consumer-group create fails
// construction. The error is the call site's error wrapping the server reply,
// not an OOM-specific error. With metrics bound during construction
// (WithPrometheusRegisterer), the refusal is also counted.
func TestOOMRejectionsCountedAtStartup(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		cmd     string
		op      oomOperation
		wantErr string
	}{
		{"presence SET", "set", oomOperationPresence, "redis transport: failed to set initial presence key: " + string(errOOMReply)},
		{"XGROUP CREATE", "xgroup", oomOperationGroupCreate, "redis transport: failed to create consumer group: " + string(errOOMReply)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			hook := &cmdErrHook{errs: map[string]error{tc.cmd: errOOMReply}}
			hook.on.Store(true)

			client := redis.NewClient(&redis.Options{Addr: miniredis.RunT(t).Addr()})
			client.AddHook(hook)

			reg := prometheus.NewRegistry()

			tr, err := NewRedisTransport(client,
				WithLogger(testLogger()),
				WithPresenceInterval(noHeartbeatInterval),
				withSkipPresenceIntervalCheck(),
				withSkipVersionCheck(),
				WithPrometheusRegisterer(reg),
			)
			require.Nil(t, tr)
			require.EqualError(t, err, tc.wantErr)
			require.ErrorIs(t, err, errOOMReply)

			want := map[string]float64{"publish": 0, "presence": 0, "group_create": 0}
			want[string(tc.op)] = 1

			assert.Equal(t, want, oomSeries(t, reg))
		})
	}
}

// closeOnPublishHook runs closeFn inside the publish script call, then fails
// the call with the server's OOM reply: a publish refused while Close runs.
type closeOnPublishHook struct {
	closeFn func()
	on      atomic.Bool
}

func (*closeOnPublishHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *closeOnPublishHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if cmd.Name() != "evalsha" || !h.on.Load() {
			return next(ctx, cmd)
		}

		h.closeFn()
		cmd.SetErr(errOOMReply)

		return errOOMReply
	}
}

func (*closeOnPublishHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

// A publish the server refuses with OOM while Close runs still returns
// ErrClosedTransport and is not a publish error, and the OOM refusal is
// counted.
func TestOOMRejectionCountedWhenPublishRacesClose(t *testing.T) {
	t.Parallel()

	hook := &closeOnPublishHook{}
	tr := newHookedTestTransport(t, []redis.Hook{hook}, WithPrometheusRegisterer(prometheus.NewRegistry()))
	m := tr.metrics.Load()
	require.NotNil(t, m)

	hook.closeFn = func() { assert.NoError(t, tr.Close(context.Background())) }
	hook.on.Store(true)

	err := tr.Dispatch(context.Background(), &mercure.Update{Topics: []string{"https://example.com/oom"}, Data: "x"})
	require.ErrorIs(t, err, ErrClosedTransport)

	assert.InDelta(t, 1, counterValueForTest(t, m.oomRejections.WithLabelValues(string(oomOperationPublish))), 0)
	assert.Zero(t, counterValueForTest(t, m.publishErrors.WithLabelValues(string(publishErrorPublish))))
}
