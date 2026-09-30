package redistransport

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dunglas/mercure"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newPingFaultTransport returns a transport whose health-check PING fails
// while the returned hook is on; every other command reaches miniredis.
// health_threshold is 1, so one failed runHealthPing marks it unhealthy.
func newPingFaultTransport(t *testing.T, opts ...Option) (*RedisTransport, *cmdErrHook) {
	t.Helper()

	return newPingFaultTransportWithHooks(t, nil, opts...)
}

// newPingFaultTransportWithHooks is newPingFaultTransport with extra hooks
// added to the client after the PING fault hook.
func newPingFaultTransportWithHooks(t *testing.T, extra []redis.Hook, opts ...Option) (*RedisTransport, *cmdErrHook) {
	t.Helper()

	hook := &cmdErrHook{errs: map[string]error{"ping": replyError("ERR injected PING failure")}}
	tr := newHookedTestTransport(t, append([]redis.Hook{hook}, extra...),
		append([]Option{WithHealthThreshold(1), WithPrometheusRegisterer(prometheus.NewRegistry())}, opts...)...)

	return tr, hook
}

// listenerStallHook parks the stream listener inside its next XREADGROUP
// while on, until the transport closes. The listener stamps its activity time
// only after an XREADGROUP returns, so once it is parked nothing refreshes the
// stamp.
type listenerStallHook struct {
	on      atomic.Bool
	entered chan struct{} // closed when the listener has parked
	once    sync.Once
}

func newListenerStallHook() *listenerStallHook {
	return &listenerStallHook{entered: make(chan struct{})}
}

func (*listenerStallHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *listenerStallHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if cmd.Name() != "xreadgroup" || !h.on.Load() {
			return next(ctx, cmd)
		}

		h.once.Do(func() { close(h.entered) })

		// Close cancels the listener's context.
		<-ctx.Done()
		cmd.SetErr(ctx.Err())

		return ctx.Err()
	}
}

func (*listenerStallHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

// stallListener parks tr's listener with hook, then stamps its last activity
// an hour ago. The stamp stays stale until tr closes.
func stallListener(t *testing.T, tr *RedisTransport, hook *listenerStallHook) {
	t.Helper()

	hook.on.Store(true)

	select {
	case <-hook.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the stream listener did not reach XREADGROUP")
	}

	tr.lastListenerActivityNanos.Store(time.Now().Add(-time.Hour).UnixNano())
}

// failPing makes tr PING-unhealthy through a real failed health check.
func failPing(t *testing.T, tr *RedisTransport, hook *cmdErrHook) {
	t.Helper()

	hook.on.Store(true)
	require.Equal(t, 1, tr.runHealthPing(context.Background(), 0))
	require.False(t, tr.healthy.Load())
}

// recoverPing makes tr healthy again through a successful health check.
func recoverPing(t *testing.T, tr *RedisTransport, hook *cmdErrHook) {
	t.Helper()

	hook.on.Store(false)
	require.Equal(t, 0, tr.runHealthPing(context.Background(), 1))
	require.True(t, tr.healthy.Load())
}

func admissionRejected(t *testing.T, tr *RedisTransport, reason string) float64 {
	t.Helper()

	return counterValueForTest(t, tr.metrics.Load().subscriberAdmissionRejected.WithLabelValues(reason))
}

// With the gate off (the default), a PING-unhealthy transport still admits.
func TestTryAdmitUnhealthyGateOffAdmits(t *testing.T) {
	t.Parallel()

	tr, hook := newPingFaultTransport(t)
	failPing(t, tr, hook)

	_, release, err := tr.TryAdmit(context.Background())
	require.NoError(t, err)
	require.NotNil(t, release)
	release()

	assert.Zero(t, admissionRejected(t, tr, admissionReasonUnhealthy))
}

// With the gate on, a PING-unhealthy transport refuses a new subscriber with
// ErrTransportUnhealthy before the capacity check, the rate token and the slot
// claim, and counts reason="unhealthy"; a successful PING admits again.
func TestTryAdmitUnhealthyGateOnRejects(t *testing.T) {
	t.Parallel()

	tr, hook := newPingFaultTransport(t,
		WithSubscriberRejectWhenUnhealthy(true),
		WithSubscriberMaxCount(1),
		// One token, refilled once a day: a consumed token stays consumed.
		WithSubscriberRateLimit(1.0/(24*60*60)),
		WithSubscriberRateBurst(1),
	)
	ctx := context.Background()

	failPing(t, tr, hook)

	tokens := tr.addSubLimiter.Tokens()

	_, release, err := tr.TryAdmit(ctx)
	require.ErrorIs(t, err, ErrTransportUnhealthy)
	require.Nil(t, release)

	var ae *mercure.AdmissionError
	require.NotErrorAs(t, err, &ae, "not an admission decision: the hub must answer 503, not 429")

	assert.InDelta(t, tokens, tr.addSubLimiter.Tokens(), 1e-6, "no rate token consumed")
	assert.Zero(t, tr.admissionLeasesHeld.Load(), "no slot claimed")
	assert.InDelta(t, 1, admissionRejected(t, tr, admissionReasonUnhealthy), 0)
	assert.Zero(t, admissionRejected(t, tr, admissionReasonCapacity))
	assert.Zero(t, admissionRejected(t, tr, admissionReasonRate))

	recoverPing(t, tr, hook)

	_, release, err = tr.TryAdmit(ctx)
	require.NoError(t, err, "a successful PING must reopen the gate")
	require.NotNil(t, release)

	// The gate runs before the capacity check: with the only slot held, an
	// unhealthy transport reports unhealthy, not capacity.
	failPing(t, tr, hook)

	_, _, err = tr.TryAdmit(ctx)
	require.ErrorIs(t, err, ErrTransportUnhealthy)
	assert.InDelta(t, 2, admissionRejected(t, tr, admissionReasonUnhealthy), 0)
	assert.Zero(t, admissionRejected(t, tr, admissionReasonCapacity))
	assert.Equal(t, int64(1), tr.admissionLeasesHeld.Load())

	release()
}

// A closed transport reports ErrClosedTransport, not the unhealthy gate.
func TestTryAdmitUnhealthyGateClosedFirst(t *testing.T) {
	t.Parallel()

	tr, hook := newPingFaultTransport(t, WithSubscriberRejectWhenUnhealthy(true))
	failPing(t, tr, hook)
	require.NoError(t, tr.Close(context.Background()))

	_, _, err := tr.TryAdmit(context.Background())
	require.ErrorIs(t, err, ErrClosedTransport)
	assert.Zero(t, admissionRejected(t, tr, admissionReasonUnhealthy))
}

// A stale listener alone (PING still answering) does not close the gate,
// although Ready reports it.
func TestTryAdmitUnhealthyGateIgnoresStaleListener(t *testing.T) {
	t.Parallel()

	stall := newListenerStallHook()
	tr, _ := newPingFaultTransportWithHooks(t, []redis.Hook{stall}, WithSubscriberRejectWhenUnhealthy(true))
	require.True(t, tr.healthy.Load())

	stallListener(t, tr, stall)
	require.ErrorIs(t, tr.Ready(context.Background()), ErrTransportUnhealthy, "precondition: Ready sees the stale listener")

	_, release, err := tr.TryAdmit(context.Background())
	require.NoError(t, err)
	require.NotNil(t, release)
	release()

	assert.Zero(t, admissionRejected(t, tr, admissionReasonUnhealthy))
}

// While the gate refuses new subscribers, an existing subscriber keeps
// receiving updates.
func TestTryAdmitUnhealthyGateKeepsExistingSubscribers(t *testing.T) {
	t.Parallel()

	tr, hook := newPingFaultTransport(t, WithSubscriberRejectWhenUnhealthy(true))
	ctx := context.Background()

	const topic = "https://example.com/unhealthy-gate"

	sub := mercure.NewLocalSubscriber("", testLogger(), testTopicMatcherStore())
	sub.SetMatchers(topicMatchers([]string{topic}), nil)
	require.NoError(t, tr.AddSubscriber(ctx, sub))

	failPing(t, tr, hook)

	_, _, err := tr.TryAdmit(ctx)
	require.ErrorIs(t, err, ErrTransportUnhealthy)

	require.NoError(t, tr.Dispatch(ctx, &mercure.Update{Topics: []string{topic}, Data: "still delivered"}))

	select {
	case u := <-sub.Receive():
		assert.Equal(t, "still delivered", u.Data)
	case <-time.After(5 * time.Second):
		t.Fatal("an existing subscriber stopped receiving updates while the gate was closed")
	}
}
