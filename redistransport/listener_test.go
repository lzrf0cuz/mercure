package redistransport

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dunglas/mercure"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const errInjectedXRead = replyError("ERR injected XREADGROUP failure")

// xreadCall is one XREADGROUP the listener issued.
type xreadCall struct {
	id     string // the stream ID read from: ">" or "0"
	at     time.Time
	failed bool // failed by the hook
}

// xreadRecorder records every XREADGROUP and fails the next failNext of them.
type xreadRecorder struct {
	mu       sync.Mutex
	calls    []xreadCall
	failNext atomic.Int32
}

func (*xreadRecorder) DialHook(next redis.DialHook) redis.DialHook { return next }

func (r *xreadRecorder) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if cmd.Name() != "xreadgroup" {
			return next(ctx, cmd)
		}

		args := cmd.Args()
		call := xreadCall{id: fmt.Sprint(args[len(args)-1]), at: time.Now(), failed: r.takeFailure()}

		r.mu.Lock()
		r.calls = append(r.calls, call)
		r.mu.Unlock()

		if call.failed {
			cmd.SetErr(errInjectedXRead)

			return errInjectedXRead
		}

		return next(ctx, cmd)
	}
}

func (*xreadRecorder) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func (r *xreadRecorder) takeFailure() bool {
	for {
		n := r.failNext.Load()
		if n <= 0 {
			return false
		}

		if r.failNext.CompareAndSwap(n, n-1) {
			return true
		}
	}
}

func (r *xreadRecorder) snapshot() []xreadCall {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]xreadCall(nil), r.calls...)
}

// An idle listener keeps reading new entries (">") at the BLOCK cadence. A
// BLOCK timeout (redis.Nil) is a completed read, not an error. It does not
// back off or drain the PEL, and it refreshes the activity stamp the
// readiness gate checks.
func TestListenerIdleReadIsNotAnError(t *testing.T) {
	t.Parallel()

	rec := &xreadRecorder{}
	tr := newHookedTestTransport(t, []redis.Hook{rec})

	stale := time.Now().Add(-time.Hour).UnixNano()
	tr.lastListenerActivityNanos.Store(stale)

	start := len(rec.snapshot())

	// xread_block is 50ms; an error backoff would be 1s.
	require.Eventually(t, func() bool { return len(rec.snapshot()) >= start+4 }, 2*time.Second, 10*time.Millisecond,
		"an idle listener must re-read at the BLOCK cadence, without an error backoff")

	for _, c := range rec.snapshot()[start:] {
		assert.Equal(t, ">", c.id, "an idle read must not switch the listener to a PEL drain")
	}

	assert.Greater(t, tr.lastListenerActivityNanos.Load(), stale,
		"a BLOCK timeout must refresh the listener activity stamp")
}

// After a failed XREADGROUP the listener backs off for 1s, or 2s after a
// second consecutive failure. It then drains its PEL from "0" and returns to
// new entries (">") once the PEL is empty.
func TestListenerErrorBackoffThenPELDrain(t *testing.T) {
	t.Parallel()

	rec := &xreadRecorder{}
	newHookedTestTransport(t, []redis.Hook{rec})

	require.Eventually(t, func() bool { return len(rec.snapshot()) > 0 }, 5*time.Second, 10*time.Millisecond)
	rec.failNext.Store(2)

	var calls []xreadCall

	first := -1

	require.Eventually(t, func() bool {
		calls = rec.snapshot()

		if first < 0 {
			for i, c := range calls {
				if c.failed {
					first = i

					break
				}
			}
		}

		return first >= 0 && len(calls) >= first+4
	}, 8*time.Second, 20*time.Millisecond)

	failed1, failed2, drain, next := calls[first], calls[first+1], calls[first+2], calls[first+3]

	require.True(t, failed1.failed)
	require.True(t, failed2.failed)
	assert.Equal(t, "0", failed2.id, "after a failed read the listener must drain its PEL from 0")
	assert.Equal(t, "0", drain.id, "the PEL drain must be retried after the second failure")
	assert.False(t, drain.failed)
	assert.Equal(t, ">", next.id, "an empty PEL drain must switch the listener back to new entries")

	assert.GreaterOrEqual(t, failed2.at.Sub(failed1.at), 900*time.Millisecond, "first backoff is 1s")
	assert.GreaterOrEqual(t, drain.at.Sub(failed2.at), 1800*time.Millisecond,
		"the second consecutive failure must back off 2s")
}

// waitForReads waits until rec has recorded at least start+n+1 XREADGROUP
// calls. A call starts only after the previous one returned, so the n calls
// after the first start have returned.
func waitForReads(t *testing.T, rec *xreadRecorder, start, n int) {
	t.Helper()

	require.Eventually(t, func() bool { return len(rec.snapshot()) >= start+n+1 }, 5*time.Second, 5*time.Millisecond)
}

// Only a successful XREADGROUP is observed in the latency histogram: idle
// BLOCK timeouts (redis.Nil) are not, and one dispatched entry adds exactly
// one sample.
func TestListenerObservesXReadLatency(t *testing.T) {
	t.Parallel()

	rec := &xreadRecorder{}
	tr := newHookedTestTransport(t, []redis.Hook{rec}, WithPrometheusRegisterer(prometheus.NewRegistry()))

	samples := func() uint64 {
		m := &dto.Metric{}
		require.NoError(t, tr.metrics.Load().xreadgroupLatency.Write(m))

		return m.GetHistogram().GetSampleCount()
	}

	waitForReads(t, rec, 0, 1)

	before := samples()

	waitForReads(t, rec, len(rec.snapshot()), 4)
	assert.Equal(t, before, samples(), "idle reads (redis.Nil) must not be observed")

	require.NoError(t, tr.Dispatch(context.Background(), &mercure.Update{
		Topics: []string{"https://example.com/latency"},
		Data:   "x",
	}))

	require.Eventually(t, func() bool { return samples() > before }, 5*time.Second, 5*time.Millisecond,
		"a successful XREADGROUP must be observed in mercure_redis_xreadgroup_latency_seconds")

	waitForReads(t, rec, len(rec.snapshot()), 4)
	assert.Equal(t, before+1, samples(), "one dispatched entry is one successful read")
}

// A failed XREADGROUP does not refresh the listener activity stamp, so
// sustained failure trips the readiness gate.
func TestListenerErrorDoesNotStampActivity(t *testing.T) {
	t.Parallel()

	rec := &xreadRecorder{}
	tr := newHookedTestTransport(t, []redis.Hook{rec})

	// More failures than the test can reach, so every read from here on fails.
	const failForever = 1 << 20

	waitForReads(t, rec, 0, 1)
	rec.failNext.Store(failForever)

	// The first failed call comes after the in-flight idle read returned and
	// stamped.
	require.Eventually(t, func() bool {
		calls := rec.snapshot()

		return len(calls) > 0 && calls[len(calls)-1].failed
	}, 5*time.Second, 5*time.Millisecond)

	stale := time.Now().Add(-time.Hour).UnixNano()
	tr.lastListenerActivityNanos.Store(stale)

	// The next failed read comes after the 1s backoff; give its readGroup
	// time to return, then the stamp must still be stale.
	failed := len(rec.snapshot())
	waitForReads(t, rec, failed, 0)
	time.Sleep(200 * time.Millisecond)

	assert.Equal(t, stale, tr.lastListenerActivityNanos.Load(), "a failed read must not stamp listener activity")
}
