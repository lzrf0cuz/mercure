package redistransport

import (
	"context"
	"maps"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// seedZombieSuspectForTests records group as a GC suspect first seen at
// firstSeen, as a GC pass at that time would have.
func seedZombieSuspectForTests(tr *RedisTransport, group string, firstSeen time.Time) {
	tr.gcMu.Lock()
	tr.gcSuspects[group] = firstSeen
	tr.gcMu.Unlock()
}

// zombieSuspectsForTests returns a copy of the GC suspects.
func zombieSuspectsForTests(tr *RedisTransport) map[string]time.Time {
	tr.gcMu.Lock()
	defer tr.gcMu.Unlock()

	return maps.Clone(tr.gcSuspects)
}

// newZombieGCTransport starts a transport on mr with a 1m presence TTL, its
// own metrics registry and periodic GC disabled. The heartbeat never ticks;
// tests drive publishPresence and gcZombieGroups directly.
func newZombieGCTransport(t *testing.T, mr *miniredis.Miniredis, hooks ...redis.Hook) *RedisTransport {
	t.Helper()

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	for _, h := range hooks {
		client.AddHook(h)
	}

	transport, err := NewRedisTransport(client,
		withSkipVersionCheck(),
		WithLogger(testLogger()),
		WithXReadBlock(50*time.Millisecond),
		WithHealthInterval(24*time.Hour),
		WithPresenceInterval(noHeartbeatInterval),
		WithPresenceTTL(time.Minute),
		withSkipPresenceIntervalCheck(),
		WithZombieGCInterval(0),
		WithPrometheusRegisterer(prometheus.NewRegistry()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { transport.Close(context.Background()) })

	return transport
}

// addPeerGroup attaches a consumer group for a peer node that has no
// presence key, and returns the group name.
func addPeerGroup(t *testing.T, tr *RedisTransport, peerNodeID string) string {
	t.Helper()

	group := nodeGroupName(peerNodeID)
	require.NoError(t, tr.client.XGroupCreateMkStream(context.Background(), tr.key(""), group, "0").Err())

	return group
}

// streamGroupNames lists the consumer groups on the transport's stream.
func streamGroupNames(t *testing.T, tr *RedisTransport) []string {
	t.Helper()

	groups, err := tr.client.XInfoGroups(context.Background(), tr.key("")).Result()
	require.NoError(t, err)

	names := make([]string, 0, len(groups))
	for _, g := range groups {
		names = append(names, g.Name)
	}

	return names
}

// expiredSuspect is a first-seen time old enough for the next pass to destroy.
func expiredSuspect(tr *RedisTransport) time.Time {
	return time.Now().Add(-tr.opts.presenceTTL - time.Second)
}

func TestZombieGCNeverDestroysOwnGroup(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)
	tr := newZombieGCTransport(t, mr)
	ctx := context.Background()

	require.NoError(t, tr.client.Del(ctx, tr.presenceKey(tr.nodeID)).Err())

	tr.gcZombieGroups(ctx, tr.key(""))
	assert.NotContains(t, zombieSuspectsForTests(tr), tr.nodeGroup, "own group must never become a suspect")

	seedZombieSuspectForTests(tr, tr.nodeGroup, expiredSuspect(tr))
	tr.gcZombieGroups(ctx, tr.key(""))
	assert.Contains(t, streamGroupNames(t, tr), tr.nodeGroup, "own group must never be destroyed")
}

func TestZombieGCSkippedWhileOwnPresenceWritesFail(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)
	hook := &cmdErrHook{errs: map[string]error{"set": errOOMReply}}
	tr := newZombieGCTransport(t, mr, hook)
	ctx := context.Background()

	expired := addPeerGroup(t, tr, "peer-expired")
	fresh := addPeerGroup(t, tr, "peer-fresh")
	seedZombieSuspectForTests(tr, expired, expiredSuspect(tr))

	hook.on.Store(true)
	tr.publishPresence(ctx)

	skipped := counterValueForTest(t, tr.metrics.Load().zombieGCSkipped)

	tr.gcZombieGroups(ctx, tr.key(""))

	names := streamGroupNames(t, tr)
	assert.Contains(t, names, expired, "a node whose presence SET fails must not destroy peer groups")
	assert.Contains(t, names, fresh)
	assert.Empty(t, zombieSuspectsForTests(tr), "a skipped pass clears the suspects")
	assert.InDelta(t, skipped+1, counterValueForTest(t, tr.metrics.Load().zombieGCSkipped), 1e-9)
}

func TestZombieGCDestroysPeerOnlyAfterPresenceTTL(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)
	tr := newZombieGCTransport(t, mr)
	ctx := context.Background()

	dead := addPeerGroup(t, tr, "peer-dead")
	back := addPeerGroup(t, tr, "peer-back")
	gone := nodeGroupName("peer-gone")
	seedZombieSuspectForTests(tr, gone, time.Now())

	tr.gcZombieGroups(ctx, tr.key(""))

	first := zombieSuspectsForTests(tr)
	require.Contains(t, first, dead)
	require.Contains(t, first, back)
	assert.NotContains(t, first, gone, "a suspect no longer listed is pruned")
	assert.Subset(t, streamGroupNames(t, tr), []string{dead, back}, "the first strike only records")

	tr.gcZombieGroups(ctx, tr.key(""))

	second := zombieSuspectsForTests(tr)
	assert.Equal(t, first[dead], second[dead], "a later pass keeps the original first-seen time")
	assert.Subset(t, streamGroupNames(t, tr), []string{dead, back}, "a suspect younger than presenceTTL is kept")

	seedZombieSuspectForTests(tr, dead, expiredSuspect(tr))
	seedZombieSuspectForTests(tr, back, expiredSuspect(tr))
	require.NoError(t, tr.client.Set(ctx, tr.presenceKey("peer-back"), "[]", time.Minute).Err())

	tr.gcZombieGroups(ctx, tr.key(""))

	names := streamGroupNames(t, tr)
	assert.NotContains(t, names, dead, "a suspect older than presenceTTL is destroyed")
	assert.Contains(t, names, back, "a peer that renewed its presence key is kept")
	assert.Empty(t, zombieSuspectsForTests(tr))
}

func TestZombieGCDestroyKeepsGroupWhosePresenceReappeared(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)
	tr := newZombieGCTransport(t, mr)
	ctx := context.Background()

	group := addPeerGroup(t, tr, "peer-renewed")
	seedZombieSuspectForTests(tr, group, expiredSuspect(tr))
	require.NoError(t, tr.client.Set(ctx, tr.presenceKey("peer-renewed"), "[]", time.Minute).Err())

	tr.gcMu.Lock()
	tr.destroyZombieGroup(ctx, tr.key(""), tr.presenceKey("peer-renewed"), group)
	tr.gcMu.Unlock()

	assert.Contains(t, streamGroupNames(t, tr), group, "the destroy must re-check the presence key")
	assert.NotContains(t, zombieSuspectsForTests(tr), group)
}

func TestZombieGCResumesAfterPresenceRecovers(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)
	hook := &cmdErrHook{errs: map[string]error{"set": errOOMReply}}
	tr := newZombieGCTransport(t, mr, hook)
	ctx := context.Background()

	group := addPeerGroup(t, tr, "peer-dead")

	hook.on.Store(true)
	tr.publishPresence(ctx)
	hook.on.Store(false)

	// A success right after a failure is not enough: the failure is recent.
	tr.publishPresence(ctx)
	seedZombieSuspectForTests(tr, group, expiredSuspect(tr))
	tr.gcZombieGroups(ctx, tr.key(""))
	require.Contains(t, streamGroupNames(t, tr), group)

	// Age the failure past presenceTTL, then write presence again.
	tr.lease.mu.Lock()
	tr.lease.lastFailure = time.Now().Add(-tr.opts.presenceTTL - time.Second)
	tr.lease.mu.Unlock()
	tr.publishPresence(ctx)

	seedZombieSuspectForTests(tr, group, expiredSuspect(tr))
	tr.gcZombieGroups(ctx, tr.key(""))
	assert.NotContains(t, streamGroupNames(t, tr), group, "GC runs again once the lease is healthy")
}

// With periodic GC disabled, the startup pass only records suspects; the
// follow-up pass presenceTTL later destroys them.
func TestZombieGCFollowUpPassWhenPeriodicDisabled(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ctx := context.Background()

	group := nodeGroupName("peer-dead")
	require.NoError(t, client.XGroupCreateMkStream(ctx, key(defaultStreamName, ""), group, "0").Err())

	tr, err := NewRedisTransport(client,
		withSkipVersionCheck(),
		WithLogger(testLogger()),
		WithXReadBlock(50*time.Millisecond),
		WithHealthInterval(24*time.Hour),
		WithPresenceInterval(100*time.Millisecond),
		WithPresenceTTL(time.Second),
		WithZombieGCInterval(0),
	)
	require.NoError(t, err)
	t.Cleanup(func() { tr.Close(context.Background()) })

	require.Contains(t, zombieSuspectsForTests(tr), group, "startup pass records the suspect")
	require.Contains(t, streamGroupNames(t, tr), group, "startup pass does not destroy")

	assert.Eventually(t, func() bool {
		groups, err := client.XInfoGroups(ctx, tr.key("")).Result()
		if err != nil {
			return false
		}

		for _, g := range groups {
			if g.Name == group {
				return false
			}
		}

		return true
	}, 5*time.Second, 20*time.Millisecond, "the follow-up pass must destroy the dead peer's group")
}

// With presence_interval close to presence_ttl, the first heartbeat must
// renew the lease before the follow-up pass, or that pass is skipped and
// never retried. The jitter is a hash of the random node ID, so the test
// retries until it lands where only the post-jitter publish keeps the lease
// fresh at the follow-up pass, with at least 200ms of margin either way.
func TestZombieGCFollowUpPassWithPresenceIntervalNearTTL(t *testing.T) {
	t.Parallel()

	const (
		presenceTTL      = time.Second
		presenceInterval = 900 * time.Millisecond
	)

	ctx := context.Background()
	group := nodeGroupName("peer-dead")

	var (
		tr     *RedisTransport
		client *redis.Client
	)

	for range 30 {
		mr := miniredis.RunT(t)
		client = redis.NewClient(&redis.Options{Addr: mr.Addr()})
		require.NoError(t, client.XGroupCreateMkStream(ctx, key(defaultStreamName, ""), group, "0").Err())

		candidate, err := NewRedisTransport(client,
			withSkipVersionCheck(),
			WithLogger(testLogger()),
			WithXReadBlock(50*time.Millisecond),
			WithHealthInterval(24*time.Hour),
			WithPresenceInterval(presenceInterval),
			WithPresenceTTL(presenceTTL),
			WithZombieGCInterval(0),
			WithPrometheusRegisterer(prometheus.NewRegistry()),
		)
		require.NoError(t, err)

		// Without the post-jitter publish the first refresh lands at
		// jitter+interval >= 1.2s, after the 1s follow-up; with it the lease
		// is at most 700ms old then.
		if j := candidate.jitterStart(presenceInterval); j >= 300*time.Millisecond && j <= 800*time.Millisecond {
			tr = candidate

			break
		}

		candidate.Close(ctx)
	}

	require.NotNil(t, tr, "no node ID with a usable jitter in 30 attempts")
	t.Cleanup(func() { tr.Close(context.Background()) })

	require.Contains(t, zombieSuspectsForTests(tr), group, "startup pass records the suspect")

	assert.Eventually(t, func() bool {
		groups, err := client.XInfoGroups(ctx, tr.key("")).Result()
		if err != nil {
			return false
		}

		for _, g := range groups {
			if g.Name == group {
				return false
			}
		}

		return true
	}, 3*time.Second, 20*time.Millisecond, "the follow-up pass must destroy the dead peer's group")
	assert.Zero(t, counterValueForTest(t, tr.metrics.Load().zombieGCSkipped), "the follow-up pass must not be skipped")
}

// slowExistsHook delays EXISTS while on and records when the reply returned.
type slowExistsHook struct {
	on   atomic.Bool
	done atomic.Pointer[time.Time]
}

func (*slowExistsHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *slowExistsHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if cmd.Name() != "exists" || !h.on.Load() {
			return next(ctx, cmd)
		}

		time.Sleep(20 * time.Millisecond)

		err := next(ctx, cmd)
		done := time.Now()
		h.done.Store(&done)

		return err
	}
}

func (*slowExistsHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

// A suspect's first-seen time is when its presence key was observed absent,
// not when the pass started; otherwise a slow pass backdates it and a later
// pass destroys the group less than presenceTTL after the observation.
func TestZombieGCFirstSeenIsObservationTime(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)
	hook := &slowExistsHook{}
	tr := newZombieGCTransport(t, mr, hook)
	group := addPeerGroup(t, tr, "peer-dead")

	hook.on.Store(true)
	tr.gcZombieGroups(context.Background(), tr.key(""))
	hook.on.Store(false)

	done := hook.done.Load()
	require.NotNil(t, done, "the pass must check the peer's presence key")

	firstSeen, ok := zombieSuspectsForTests(tr)[group]
	require.True(t, ok)
	assert.False(t, firstSeen.Before(*done), "first seen %v precedes the EXISTS reply at %v", firstSeen, *done)
}

// An unhealthy lease at pass start skips the whole pass, even with no
// suspects to clear: no peer is looked at and the skip is counted.
func TestZombieGCSkippedAtPassStartRecordsNoSuspects(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)
	hook := &cmdErrHook{errs: map[string]error{"set": errOOMReply}}
	tr := newZombieGCTransport(t, mr, hook)
	ctx := context.Background()

	addPeerGroup(t, tr, "peer-dead")
	require.Empty(t, zombieSuspectsForTests(tr))

	hook.on.Store(true)
	tr.publishPresence(ctx)

	skipped := counterValueForTest(t, tr.metrics.Load().zombieGCSkipped)

	tr.gcZombieGroups(ctx, tr.key(""))

	assert.Empty(t, zombieSuspectsForTests(tr), "a skipped pass must not record suspects")
	assert.InDelta(t, skipped+1, counterValueForTest(t, tr.metrics.Load().zombieGCSkipped), 1e-9)
}

// leaseFailOnExistsHook records a presence write failure on lease when an
// EXISTS runs, once lease is set.
type leaseFailOnExistsHook struct{ lease atomic.Pointer[presenceLease] }

func (*leaseFailOnExistsHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *leaseFailOnExistsHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if l := h.lease.Load(); l != nil && cmd.Name() == "exists" {
			l.recordFailure()
		}

		return next(ctx, cmd)
	}
}

func (*leaseFailOnExistsHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

// A lease that turns unhealthy during a pass stops it before the next
// destroy, and the stop counts as a skip.
func TestZombieGCLeaseUnhealthyMidPassKeepsGroup(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)
	hook := &leaseFailOnExistsHook{}
	tr := newZombieGCTransport(t, mr, hook)
	ctx := context.Background()

	group := addPeerGroup(t, tr, "peer-dead")
	seedZombieSuspectForTests(tr, group, expiredSuspect(tr))

	skipped := counterValueForTest(t, tr.metrics.Load().zombieGCSkipped)

	hook.lease.Store(&tr.lease)
	tr.gcZombieGroups(ctx, tr.key(""))
	hook.lease.Store(nil)

	assert.Contains(t, streamGroupNames(t, tr), group, "a lease lost mid-pass must stop the destroy")
	assert.InDelta(t, skipped+1, counterValueForTest(t, tr.metrics.Load().zombieGCSkipped), 1e-9)
}
