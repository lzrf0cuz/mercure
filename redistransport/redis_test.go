package redistransport

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/dunglas/mercure"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
}

// Test-only sentinel errors used by table-driven tests via errors.Is.
var (
	errXInfoGroupsMissingStream = errors.New("ERR no such key")
	errLowercaseMissing         = errors.New("no such key 'x'")
	errWrongType                = errors.New("WRONGTYPE Operation against a key")
	errCapitalizedMissing       = errors.New("ERR No such key")
)

func testTopicMatcherStore() *mercure.TopicMatcherStore {
	tss, _ := mercure.NewTopicMatcherStore(0)

	return tss
}

// uriTemplateVar matches a "{name}" URI-template variable.
var uriTemplateVar = regexp.MustCompile(`\{(\w+)\}`)

// topicMatchers turns topics into subscriber matchers: exact, or, for a topic
// holding a "{name}" URI-template variable, the equivalent URL Pattern (":name").
func topicMatchers(topics []string) []mercure.TopicMatcher {
	if topics == nil {
		return nil
	}

	matchers := make([]mercure.TopicMatcher, len(topics))
	for i, topic := range topics {
		if strings.Contains(topic, "{") {
			matchers[i] = mercure.TopicMatcher{
				Type:    mercure.MatcherTypeURLPattern,
				Pattern: uriTemplateVar.ReplaceAllString(topic, ":$1"),
			}

			continue
		}

		matchers[i] = mercure.TopicMatcher{Type: mercure.MatcherTypeExact, Pattern: topic}
	}

	return matchers
}

func newTestTransport(t *testing.T, opts ...Option) (*RedisTransport, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	allOpts := slices.Concat([]Option{
		WithLogger(testLogger()),
		WithXReadBlock(50 * time.Millisecond),
		WithHealthInterval(24 * time.Hour),
		WithPresenceInterval(24 * time.Hour),
		// Test windows are far shorter than 24h so the presence heartbeat
		// ticker never fires. skipPresenceIntervalCheck bypasses the
		// production invariant (interval < TTL); miniredis skipVersionCheck
		// bypasses the otherwise-fatal INFO server check.
		withSkipPresenceIntervalCheck(),
		withSkipVersionCheck(),
	}, opts)
	transport, err := NewRedisTransport(client, allOpts...)
	require.NoError(t, err)
	t.Cleanup(func() { transport.Close(context.Background()) })

	return transport, mr
}

func TestRedisTransportDispatch(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t)
	update := &mercure.Update{
		Topics: []string{"https://example.com/books/1"},
		Data:   "test data",
	}
	err := transport.Dispatch(context.Background(), update)
	require.NoError(t, err)
	assert.NotEmpty(t, update.ID, "AssignUUID should have set the ID")
}

func TestRedisTransportClosed(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t)
	err := transport.Close(context.Background())
	require.NoError(t, err)

	update := &mercure.Update{
		Topics: []string{"https://example.com/test"},
		Data:   "test",
	}
	err = transport.Dispatch(context.Background(), update)
	assert.ErrorIs(t, err, ErrClosedTransport)
}

func TestRedisTransportDoubleClose(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t)
	err := transport.Close(context.Background())
	require.NoError(t, err)
	err = transport.Close(context.Background())
	require.NoError(t, err)
}

func TestRedisTransportSubscribeAndReceive(t *testing.T) {
	t.Parallel()

	transport, mr := newTestTransport(t)
	tss := testTopicMatcherStore()
	sub := mercure.NewLocalSubscriber("", testLogger(), tss)
	sub.SetMatchers(topicMatchers([]string{"https://example.com/books/{id}"}), nil)
	err := transport.AddSubscriber(context.Background(), sub)
	require.NoError(t, err)

	update := &mercure.Update{
		Topics: []string{"https://example.com/books/1"},
		Data:   "test book data",
	}
	err = transport.Dispatch(context.Background(), update)
	require.NoError(t, err)
	mr.FastForward(100 * time.Millisecond)

	select {
	case received := <-sub.Receive():
		assert.Equal(t, update.Data, received.Data)
		assert.Equal(t, update.Topics, received.Topics)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for update")
	}
}

func TestRedisTransportRemoveSubscriber(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t)
	tss := testTopicMatcherStore()
	sub := mercure.NewLocalSubscriber("", testLogger(), tss)
	sub.SetMatchers(topicMatchers([]string{"https://example.com/test"}), nil)
	err := transport.AddSubscriber(context.Background(), sub)
	require.NoError(t, err)
	err = transport.RemoveSubscriber(context.Background(), sub)
	require.NoError(t, err)
}

// removeMetrics returns subscriber_remove_total and the sample count of
// remove_subscriber_duration_seconds.
func removeMetrics(t *testing.T, transport *RedisTransport) (float64, uint64) {
	t.Helper()

	m := transport.metrics.Load()
	require.NotNil(t, m, "metrics must be bound")

	var total, duration dto.Metric

	require.NoError(t, m.subscriberRemoveTotal.Write(&total))
	require.NoError(t, m.removeSubscriberDuration.Write(&duration))

	return total.GetCounter().GetValue(), duration.GetHistogram().GetSampleCount()
}

// TestRemoveSubscriberCountsOnlyListedRemovals pins that subscriber_remove_total
// and the remove-duration histogram record a RemoveSubscriber only when the
// subscriber was listed: never for a subscriber that was never added, a
// redundant second removal, or a removal on a closed transport.
func TestRemoveSubscriberCountsOnlyListedRemovals(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t, WithPrometheusRegisterer(prometheus.NewRegistry()))
	tss := testTopicMatcherStore()
	ctx := context.Background()

	never := mercure.NewLocalSubscriber("", testLogger(), tss)
	never.SetMatchers(topicMatchers([]string{"https://example.com/test"}), nil)
	require.NoError(t, transport.RemoveSubscriber(ctx, never))

	total, count := removeMetrics(t, transport)
	assert.InDelta(t, 0.0, total, 0, "removing a never-added subscriber must not count a removal")
	assert.Equal(t, uint64(0), count, "removing a never-added subscriber must not observe a duration")

	added := mercure.NewLocalSubscriber("", testLogger(), tss)
	added.SetMatchers(topicMatchers([]string{"https://example.com/test"}), nil)
	require.NoError(t, transport.AddSubscriber(ctx, added))
	require.NoError(t, transport.RemoveSubscriber(ctx, added))

	total, count = removeMetrics(t, transport)
	assert.InDelta(t, 1.0, total, 0, "removing a listed subscriber must count exactly one removal")
	assert.Equal(t, uint64(1), count, "removing a listed subscriber must observe exactly one duration")

	require.NoError(t, transport.RemoveSubscriber(ctx, added))

	total, count = removeMetrics(t, transport)
	assert.InDelta(t, 1.0, total, 0, "a redundant removal must not count again")
	assert.Equal(t, uint64(1), count, "a redundant removal must not observe again")
	assert.EqualValues(t, 0, transport.subscriberCount.Load())

	listedAtClose := mercure.NewLocalSubscriber("", testLogger(), tss)
	listedAtClose.SetMatchers(topicMatchers([]string{"https://example.com/test"}), nil)
	require.NoError(t, transport.AddSubscriber(ctx, listedAtClose))
	require.NoError(t, transport.Close(ctx))
	require.ErrorIs(t, transport.RemoveSubscriber(ctx, listedAtClose), ErrClosedTransport)

	total, count = removeMetrics(t, transport)
	assert.InDelta(t, 1.0, total, 0, "a removal on a closed transport must not count")
	assert.Equal(t, uint64(1), count, "a removal on a closed transport must not observe a duration")
}

// lockTransportMu locks transport.mu and returns the matching unlock. If the
// test fails while holding it, a cleanup unlocks it; registered after
// newTestTransport's Close cleanup, it runs first (LIFO), so Close does not
// hang on t.mu.
func lockTransportMu(t *testing.T, transport *RedisTransport) func() {
	t.Helper()

	transport.mu.Lock()

	held := true

	t.Cleanup(func() {
		if held {
			transport.mu.Unlock()
		}
	})

	return func() {
		held = false

		transport.mu.Unlock()
	}
}

// waitBlockedOnMu waits until a goroutine started by the calling test is
// inside method's t.mu.Lock() (method is AddSubscriber or RemoveSubscriber):
// it has passed the unlocked closed check and is queued on the t.mu the test
// holds. The creator filter keeps calls from parallel tests from satisfying
// the wait.
func waitBlockedOnMu(t *testing.T, method string) {
	t.Helper()

	creator := "redistransport." + t.Name() + " in goroutine"

	require.Eventually(t, func() bool {
		buf := make([]byte, 1<<20)
		for {
			n := runtime.Stack(buf, true)
			if n < len(buf) {
				buf = buf[:n]

				break
			}

			buf = make([]byte, 2*len(buf))
		}

		for g := range strings.SplitSeq(string(buf), "\n\n") {
			if strings.Contains(g, "(*RedisTransport)."+method+"(") &&
				strings.Contains(g, "sync.(*RWMutex).Lock(") &&
				strings.Contains(g, creator) {
				return true
			}
		}

		return false
	}, 5*time.Second, time.Millisecond, "%s never blocked on t.mu", method)
}

// TestRemoveSubscriberRecheckClosedUnderMu pins RemoveSubscriber's closed
// re-check under t.mu (see RemoveSubscriber). As in
// TestAddSubscriberRecheckClosedUnderMu, Close's closedOnce body is replayed
// under a held t.mu, so a re-check before t.mu.Lock() fails too.
func TestRemoveSubscriberRecheckClosedUnderMu(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t, WithXReadBlock(time.Millisecond), WithPrometheusRegisterer(prometheus.NewRegistry()))
	tss := testTopicMatcherStore()

	s := mercure.NewLocalSubscriber("", testLogger(), tss)
	s.SetMatchers(topicMatchers([]string{"https://example.com/remove-race"}), nil)
	require.NoError(t, transport.AddSubscriber(context.Background(), s))

	unlock := lockTransportMu(t, transport)

	removeErr := make(chan error, 1)

	go func() {
		removeErr <- transport.RemoveSubscriber(context.Background(), s)
	}()

	waitBlockedOnMu(t, "RemoveSubscriber")

	// Close's closedOnce body, under the t.mu we already hold. Consuming
	// closedOnce makes t.Cleanup's later Close() a no-op.
	close(transport.closed)
	transport.cancel()
	<-transport.listenerDone
	transport.closeShardChannels()
	transport.wg.Wait()
	transport.disconnectAllSubscribers()
	transport.closedOnce.Do(func() {})
	unlock()

	require.ErrorIs(t, <-removeErr, ErrClosedTransport,
		"a Remove queued on t.mu across Close must observe the closed transport")

	total, count := removeMetrics(t, transport)
	assert.InDelta(t, 0.0, total, 0, "a shutdown drop must not also be counted in subscriber_remove_total")
	assert.Equal(t, uint64(0), count, "a shutdown drop must not observe a remove duration")

	var lostShutdown dto.Metric
	require.NoError(t, transport.metrics.Load().lostCounter(lossReasonShutdown).Write(&lostShutdown))
	assert.InDelta(t, 1.0, lostShutdown.GetCounter().GetValue(), 0,
		"the subscriber must be counted once in subscribers_lost{reason=shutdown}")
}

// TestRemoveSubscriberDurationCoversMuWait pins that remove_subscriber_duration
// is measured from the start of the call, so it includes the t.mu wait the
// metric exists to expose. The test holds t.mu for a known interval while a
// Remove is queued on it; the observed duration must be at least that long.
func TestRemoveSubscriberDurationCoversMuWait(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t, WithPrometheusRegisterer(prometheus.NewRegistry()))
	tss := testTopicMatcherStore()

	s := mercure.NewLocalSubscriber("", testLogger(), tss)
	s.SetMatchers(topicMatchers([]string{"https://example.com/remove-wait"}), nil)
	require.NoError(t, transport.AddSubscriber(context.Background(), s))

	const hold = 50 * time.Millisecond

	unlock := lockTransportMu(t, transport)

	removeErr := make(chan error, 1)

	go func() {
		removeErr <- transport.RemoveSubscriber(context.Background(), s)
	}()

	waitBlockedOnMu(t, "RemoveSubscriber")
	time.Sleep(hold)
	unlock()

	require.NoError(t, <-removeErr)

	var duration dto.Metric
	require.NoError(t, transport.metrics.Load().removeSubscriberDuration.Write(&duration))
	require.Equal(t, uint64(1), duration.GetHistogram().GetSampleCount())
	assert.GreaterOrEqual(t, duration.GetHistogram().GetSampleSum(), hold.Seconds(),
		"the remove duration must include the time spent waiting for t.mu")
}

// removeCountingTransport counts RemoveSubscriber calls and delegates
// everything to the embedded RedisTransport.
type removeCountingTransport struct {
	*RedisTransport

	removeCalls atomic.Int32
}

func (t *removeCountingTransport) RemoveSubscriber(ctx context.Context, s *mercure.LocalSubscriber) error {
	t.removeCalls.Add(1)

	return t.RedisTransport.RemoveSubscriber(ctx, s)
}

// TestHubReplayFailureIsRemovedByTheHub drives the hub's subscribe path into a
// history-replay failure. The transport counts the subscriber in
// subscribers_lost{reason="history_replay_failed"} and leaves it listed; the
// hub's RemoveSubscriber after the failed AddSubscriber takes it off the
// lists, counting one removal.
func TestHubReplayFailureIsRemovedByTheHub(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	// The registration timeout bounds the replay's XRANGE retries against the
	// closed server; the hub registers with a context that has no deadline.
	transport, mr := newTestTransport(
		t,
		WithPrometheusRegisterer(reg),
		WithSubscriberRegistrationTimeout(500*time.Millisecond),
	)

	for range 5 {
		require.NoError(t, transport.Dispatch(context.Background(), &mercure.Update{
			Topics: []string{"https://example.com/test"},
			Data:   "payload",
		}))
	}

	mr.Close()

	// Bring the server back once the assertions are done, so the transport's
	// Close cleanup (registered earlier, so it runs later) does not spend
	// closeTimeout against a dead server.
	t.Cleanup(func() { assert.NoError(t, mr.Restart()) })

	counting := &removeCountingTransport{RedisTransport: transport}

	hub, err := mercure.NewHub(
		context.Background(),
		mercure.WithTransport(counting),
		mercure.WithAnonymous(),
		mercure.WithLogger(testLogger()),
	)
	require.NoError(t, err)

	query := url.Values{}
	query.Set("match", "https://example.com/test")
	query.Set("last_event_id", mercure.EarliestLastEventID)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/.well-known/mercure?"+query.Encode(), nil)
	w := httptest.NewRecorder()
	hub.SubscribeHandler(w, req)

	require.Equal(t, http.StatusServiceUnavailable, w.Code, "a failed history replay must fail the subscription")
	require.EqualValues(t, 1, counting.removeCalls.Load(),
		"the hub must call RemoveSubscriber after the failed AddSubscriber")

	var lostReplay dto.Metric
	require.NoError(t, transport.metrics.Load().lostCounter(lossReasonHistoryReplayFailed).Write(&lostReplay))
	assert.InDelta(t, 1.0, lostReplay.GetCounter().GetValue(), 0,
		"the replay failure must be counted in subscribers_lost{reason=history_replay_failed}")

	total, count := removeMetrics(t, transport)
	assert.InDelta(t, 1.0, total, 0,
		"the hub's removal after the failed replay must be counted in subscriber_remove_total")
	assert.Equal(t, uint64(1), count, "the hub's removal must observe one remove duration")
	assert.EqualValues(t, 0, transport.subscriberCount.Load(), "the hub's removal must unlist the subscriber")
}

// TestConcurrentRemoveSubscriberCountsOnce races RemoveSubscriber calls for
// one listed subscriber: exactly one must remove and count it (see lostFlags).
// With the Delete after the Unlock, another call could remove and count it
// again. The window is a few instructions wide, hence many rounds. Not
// parallel: see raiseGOMAXPROCS.
func TestConcurrentRemoveSubscriberCountsOnce(t *testing.T) {
	raiseGOMAXPROCS(t, raceTestMinProcs)

	transport, _ := newTestTransport(t, WithPrometheusRegisterer(prometheus.NewRegistry()))
	tss := testTopicMatcherStore()
	ctx := context.Background()

	const (
		rounds   = 2000
		removers = 8
	)

	for round := range rounds {
		s := mercure.NewLocalSubscriber("", testLogger(), tss)
		s.SetMatchers(topicMatchers([]string{"https://example.com/concurrent-remove"}), nil)
		require.NoError(t, transport.AddSubscriber(ctx, s))

		start := make(chan struct{})

		var wg sync.WaitGroup

		for range removers {
			wg.Go(func() {
				<-start

				assert.NoError(t, transport.RemoveSubscriber(ctx, s))
			})
		}

		close(start)
		wg.Wait()

		total, count := removeMetrics(t, transport)
		require.InDelta(t, float64(round+1), total, 0, "round %d: concurrent removals of one subscriber must count one removal", round)
		require.Equal(t, uint64(round+1), count, "round %d: concurrent removals of one subscriber must observe one duration", round)
		require.EqualValues(t, 0, transport.subscriberCount.Load(), "round %d: the subscriber must be removed exactly once", round)
	}
}

// raceTestMinProcs is the GOMAXPROCS floor raiseGOMAXPROCS applies to the
// tests that race goroutines across a t.mu hand-off.
const raceTestMinProcs = 4

// raiseGOMAXPROCS raises GOMAXPROCS to at least minProcs for the rest of the
// test. With a single P, a goroutine woken by an Unlock does not run until the
// unlocker blocks or is preempted, so a test racing into the window after an
// Unlock passes against the bug it guards. GOMAXPROCS is process-wide, so the
// helper restores the previous value when the test ends, and the caller must
// not call t.Parallel: parallel tests start only after every sequential
// top-level test has finished.
func raiseGOMAXPROCS(t *testing.T, minProcs int) {
	t.Helper()

	if prev := runtime.GOMAXPROCS(0); prev < minProcs {
		runtime.GOMAXPROCS(minProcs)
		t.Cleanup(func() { runtime.GOMAXPROCS(prev) })
	}
}

// shardGaugeValues returns the value of each shard's shard_subscribers gauge.
func shardGaugeValues(t *testing.T, transport *RedisTransport) []float64 {
	t.Helper()

	m := transport.metrics.Load()
	require.NotNil(t, m, "metrics must be bound")

	values := make([]float64, len(m.shardSubGauges))

	for i, g := range m.shardSubGauges {
		var metric dto.Metric

		require.NoError(t, g.Write(&metric))

		values[i] = metric.GetGauge().GetValue()
	}

	return values
}

// TestHistoryReplayFailureAfterCloseReturnsClosedSentinel pins that a history
// replay failing after Close makes AddSubscriber return the closed-transport
// sentinel (which the hub matches as mercure.ErrClosedTransport), and that the
// subscriber is counted in subscribers_lost once: Close's disconnect walk
// counts it as a shutdown loss, and the failed replay's markLost then finds the
// flag already set.
func TestHistoryReplayFailureAfterCloseReturnsClosedSentinel(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(
		t,
		WithPrometheusRegisterer(prometheus.NewRegistry()),
		WithHistoryReplayConcurrency(1),
	)

	// Take the only replay slot so the replay waits on its context.
	transport.historyReplaySem <- struct{}{}

	s := mercure.NewLocalSubscriber(mercure.EarliestLastEventID, testLogger(), testTopicMatcherStore())
	s.SetMatchers(topicMatchers([]string{"https://example.com/replay-after-close"}), nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	addErr := make(chan error, 1)

	go func() { addErr <- transport.AddSubscriber(ctx, s) }()

	require.Eventually(t, func() bool { return transport.subscriberCount.Load() == 1 },
		5*time.Second, time.Millisecond, "the subscriber was never listed")

	require.NoError(t, transport.Close(context.Background()))
	cancel()

	var err error

	select {
	case err = <-addErr:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "AddSubscriber did not return after Close")
	}

	require.ErrorIs(t, err, ErrClosedTransport)
	require.ErrorIs(t, err, mercure.ErrClosedTransport)

	m := transport.metrics.Load()

	var lostShutdown, lostReplay dto.Metric
	require.NoError(t, m.lostCounter(lossReasonShutdown).Write(&lostShutdown))
	require.NoError(t, m.lostCounter(lossReasonHistoryReplayFailed).Write(&lostReplay))
	assert.InDelta(t, 1.0, lostShutdown.GetCounter().GetValue(), 0, "Close's walk must count the subscriber once")
	assert.InDelta(t, 0.0, lostReplay.GetCounter().GetValue(), 0, "the failed replay must not count it again")
	assert.EqualValues(t, 0, transport.subscriberCount.Load(), "Close must count the subscriber out once")
}

// TestHistoryReplayFailureBeforeCloseWalkCountsOnce pins the other order: the
// replay fails after Close closed t.closed but before its disconnect walk. The
// failed replay's markLost then runs first, so the loss counts as
// history_replay_failed and the walk, which still disconnects the subscriber
// and counts it out, does not count it again; AddSubscriber must return the
// closed-transport sentinel and log the replay error it replaces at Debug.
// An extra t.wg count holds Close at its wg.Wait, after close(t.closed) and
// before the walk, until AddSubscriber has returned. A cleanup releases that
// hold, and every wait is bounded, so a failure does not hang the test.
func TestHistoryReplayFailureBeforeCloseWalkCountsOnce(t *testing.T) {
	t.Parallel()

	var logs safeBuffer

	transport, _ := newTestTransport(
		t,
		WithPrometheusRegisterer(prometheus.NewRegistry()),
		WithHistoryReplayConcurrency(1),
		WithLogger(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))),
	)

	// Take the only replay slot so the replay waits on its context.
	transport.historyReplaySem <- struct{}{}

	t.Cleanup(func() { <-transport.historyReplaySem })

	s := mercure.NewLocalSubscriber(mercure.EarliestLastEventID, testLogger(), testTopicMatcherStore())
	s.SetMatchers(topicMatchers([]string{"https://example.com/replay-before-walk"}), nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	addErr := make(chan error, 1)

	go func() { addErr <- transport.AddSubscriber(ctx, s) }()

	require.Eventually(t, func() bool { return transport.subscriberCount.Load() == 1 },
		5*time.Second, time.Millisecond, "the subscriber was never listed")

	transport.wg.Add(1)

	var releaseOnce sync.Once

	releaseClose := func() { releaseOnce.Do(transport.wg.Done) }

	// Registered after newTestTransport's Close cleanup, so it runs first.
	t.Cleanup(releaseClose)

	closeErr := make(chan error, 1)

	go func() { closeErr <- transport.Close(context.Background()) }()

	select {
	case <-transport.closed:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "Close never closed t.closed")
	}

	cancel()

	var err error

	select {
	case err = <-addErr:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "AddSubscriber did not return while Close was held before its walk")
	}

	releaseClose()

	select {
	case err := <-closeErr:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		require.FailNow(t, "Close did not return once released")
	}

	require.ErrorIs(t, err, ErrClosedTransport)
	require.ErrorIs(t, err, mercure.ErrClosedTransport)

	m := transport.metrics.Load()

	var lostShutdown, lostReplay dto.Metric
	require.NoError(t, m.lostCounter(lossReasonShutdown).Write(&lostShutdown))
	require.NoError(t, m.lostCounter(lossReasonHistoryReplayFailed).Write(&lostReplay))
	assert.InDelta(t, 1.0, lostReplay.GetCounter().GetValue(), 0, "the failed replay must count the subscriber once")
	assert.InDelta(t, 0.0, lostShutdown.GetCounter().GetValue(), 0, "Close's walk must not count the subscriber again")
	assert.EqualValues(t, 0, transport.subscriberCount.Load(), "Close's walk must count the subscriber out")
	assert.True(t, slices.ContainsFunc(strings.Split(logs.String(), "\n"), func(line string) bool {
		return strings.Contains(line, "level=DEBUG") && strings.Contains(line, s.ID) &&
			strings.Contains(line, context.Canceled.Error())
	}), "the replay error replaced by the closed-transport sentinel must be logged at Debug with the subscriber")
}

// TestErrClosedTransportMatchesHubSentinel pins that every method returning
// ErrClosedTransport on a closed transport returns an error matching both
// this package's sentinel and mercure.ErrClosedTransport, which the hub
// checks for, with this package's message unchanged.
func TestErrClosedTransportMatchesHubSentinel(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t)
	ctx := context.Background()

	s := mercure.NewLocalSubscriber("", testLogger(), testTopicMatcherStore())
	s.SetMatchers(topicMatchers([]string{"https://example.com/closed"}), nil)
	require.NoError(t, transport.AddSubscriber(ctx, s))
	require.NoError(t, transport.Close(ctx))

	_, _, getErr := transport.GetSubscribers(ctx)
	_, _, admitErr := transport.TryAdmit(ctx)

	for name, err := range map[string]error{
		"Dispatch":         transport.Dispatch(ctx, &mercure.Update{Topics: []string{"https://example.com/closed"}}),
		"AddSubscriber":    transport.AddSubscriber(ctx, s),
		"RemoveSubscriber": transport.RemoveSubscriber(ctx, s),
		"GetSubscribers":   getErr,
		"TryAdmit":         admitErr,
		"Ready":            transport.Ready(ctx),
		"Live":             transport.Live(ctx),
	} {
		require.ErrorIs(t, err, ErrClosedTransport, name)
		require.ErrorIs(t, err, mercure.ErrClosedTransport, name)
		assert.EqualError(t, err, "redis transport: read/write on closed transport", name)
	}
}

// TestHubClosedTransportAddLogsOnceAtDebug drives the hub's subscribe path
// into AddSubscriber on a closed transport, as when Close lands between the
// hub's admission and its AddSubscriber. The hub logs the add failure at Debug
// when it matches mercure.ErrClosedTransport, then calls RemoveSubscriber and
// suppresses its error on the same match; this transport's sentinel must
// match, or the hub logs the failure, and a removal failure, at ERROR. The
// transport is wrapped so the hub sees no Admitter: TryAdmit on a closed
// transport would otherwise answer 503 before AddSubscriber is reached.
func TestHubClosedTransportAddLogsOnceAtDebug(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t)
	require.NoError(t, transport.Close(context.Background()))

	var logs safeBuffer

	hub, err := mercure.NewHub(
		context.Background(),
		mercure.WithTransport(struct{ mercure.Transport }{transport}),
		mercure.WithAnonymous(),
		mercure.WithLogger(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))),
	)
	require.NoError(t, err)

	query := url.Values{}
	query.Set("match", "https://example.com/closed")

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/.well-known/mercure?"+query.Encode(), nil)
	w := httptest.NewRecorder()
	hub.SubscribeHandler(w, req)

	require.Equal(t, http.StatusServiceUnavailable, w.Code)

	var debugMsgs, errorMsgs []string

	for line := range strings.Lines(logs.String()) {
		var record struct {
			Level string `json:"level"`
			Msg   string `json:"msg"`
		}

		require.NoError(t, json.Unmarshal([]byte(line), &record))

		switch record.Level {
		case slog.LevelDebug.String():
			debugMsgs = append(debugMsgs, record.Msg)
		case slog.LevelError.String():
			errorMsgs = append(errorMsgs, record.Msg)
		}
	}

	assert.Equal(t, 1, countOf(debugMsgs, "Unable to add subscriber"),
		"the hub must reach AddSubscriber and log its failure once, at Debug")
	assert.Empty(t, errorMsgs,
		"the hub must recognise the closed transport and log nothing at ERROR")
}

// TestHubClosedTransportExitLogsNoError closes the transport under a live
// subscriber, as a Caddy reload that drops the transport does. The subscribe
// ends as transport_ended; the hub's RemoveSubscriber and active:false
// subscription update then fail with this transport's closed sentinel, which
// must match mercure.ErrClosedTransport, or the hub logs both at ERROR.
func TestHubClosedTransportExitLogsNoError(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t)

	var logs safeBuffer

	hub, err := mercure.NewHub(
		t.Context(),
		mercure.WithTransport(transport),
		mercure.WithAnonymous(),
		mercure.WithSubscriptions(),
		mercure.WithLogger(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))),
	)
	require.NoError(t, err)

	srv := httptest.NewServer(hub)
	t.Cleanup(srv.Close)

	query := url.Values{}
	query.Set("match", "https://example.com/closed-exit")

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL+"/.well-known/mercure?"+query.Encode(), nil)
	require.NoError(t, err)

	resp, err := srv.Client().Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Eventually(t, func() bool { return transport.subscriberCount.Load() == 1 },
		5*time.Second, 10*time.Millisecond, "the subscriber must be added")

	// Hub construction logs at ERROR that the subscription API is disabled
	// (no subscriber verifier); only the logs from the Close on count.
	logsBeforeClose := len(logs.String())

	require.NoError(t, transport.Close(t.Context()))

	// The body ends once the handler, and its logging, has returned.
	_, err = io.Copy(io.Discard, resp.Body)
	require.NoError(t, err)

	var (
		reasons   []string
		errorMsgs []string
	)

	for line := range strings.Lines(logs.String()[logsBeforeClose:]) {
		var record struct {
			Level  string `json:"level"`
			Msg    string `json:"msg"`
			Reason string `json:"reason"`
		}

		require.NoError(t, json.Unmarshal([]byte(line), &record))

		if record.Msg == "Subscriber disconnected" {
			reasons = append(reasons, record.Reason)
		}

		if record.Level == slog.LevelError.String() {
			errorMsgs = append(errorMsgs, record.Msg)
		}
	}

	assert.Equal(t, []string{"transport_ended"}, reasons)
	assert.Empty(t, errorMsgs, "the hub must recognise the closed transport and log nothing at ERROR")
}

// A present but empty last_event_id names no event: the subscribe must send
// its headers, with Mercure-Last-Event-ID "earliest", replay nothing, and then
// deliver live updates. The hub blocks on the response ID before sending
// headers, so a transport that skips it for an empty ID hangs the request.
func TestHubEmptyLastEventIDDoesNotHang(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t)
	topic := "https://example.com/empty-last-event-id"

	require.NoError(t, transport.Dispatch(t.Context(), &mercure.Update{
		Topics: []string{topic},
		Data:   "history",
	}))
	require.Eventually(t, func() bool { return *transport.lastDispatchedStreamID.Load() != streamIDEarliest },
		5*time.Second, 10*time.Millisecond, "the listener must consume the history update")

	hub, err := mercure.NewHub(
		t.Context(),
		mercure.WithTransport(transport),
		mercure.WithAnonymous(),
		mercure.WithLogger(testLogger()),
	)
	require.NoError(t, err)

	// srv.Close waits for the handler, so it is deferred only once the headers
	// arrived: a hung subscribe then fails the test instead of blocking it.
	srv := httptest.NewServer(http.HandlerFunc(hub.SubscribeHandler))

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	query := url.Values{}
	query.Set("match", topic)
	query.Set("last_event_id", "")

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/.well-known/mercure?"+query.Encode(), nil)
	require.NoError(t, err)

	resp, err := srv.Client().Do(req)
	require.NoError(t, err, "the subscribe must send its headers, not hang")
	t.Cleanup(srv.Close)

	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, mercure.EarliestLastEventID, resp.Header.Get("Mercure-Last-Event-ID"))

	require.NoError(t, transport.Dispatch(t.Context(), &mercure.Update{
		Topics: []string{topic},
		Data:   "live",
	}))

	var dataLines []string

	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		if data, ok := strings.CutPrefix(scanner.Text(), "data: "); ok {
			dataLines = append(dataLines, data)

			break
		}
	}

	assert.Equal(t, []string{"live"}, dataLines, "an empty Last-Event-ID must replay nothing before the live update")
}

// TestHistoryReplayThenRemovalCountsOneRemoval pins the success-path
// counterpart of TestHubReplayFailureIsRemovedByTheHub: after a history
// replay that succeeds, the subscriber stays listed, so its removal counts
// exactly one removal and one duration, and nothing is counted as lost.
func TestHistoryReplayThenRemovalCountsOneRemoval(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t, WithPrometheusRegisterer(prometheus.NewRegistry()))
	tss := testTopicMatcherStore()
	ctx := context.Background()

	const topic = "https://example.com/replay-then-remove"

	initialCursor := *transport.lastDispatchedStreamID.Load()

	require.NoError(t, transport.Dispatch(ctx, &mercure.Update{
		Topics: []string{topic},
		Data:   "replayed",
	}))

	// The replay is bounded by the listener's cursor, so wait for the listener
	// to consume the entry; otherwise there is nothing to replay.
	require.Eventually(t, func() bool {
		return *transport.lastDispatchedStreamID.Load() != initialCursor
	}, 5*time.Second, time.Millisecond, "the listener never consumed the dispatched entry")

	s := mercure.NewLocalSubscriber(mercure.EarliestLastEventID, testLogger(), tss)
	s.SetMatchers(topicMatchers([]string{topic}), nil)
	require.NoError(t, transport.AddSubscriber(ctx, s))

	select {
	case u := <-s.Receive():
		require.NotNil(t, u)
		assert.Equal(t, "replayed", u.Data, "the history replay must deliver the dispatched entry")
	case <-time.After(5 * time.Second):
		require.Fail(t, "the history replay delivered nothing")
	}

	require.NoError(t, transport.RemoveSubscriber(ctx, s))

	total, count := removeMetrics(t, transport)
	assert.InDelta(t, 1.0, total, 0, "removing a subscriber after a successful replay must count one removal")
	assert.Equal(t, uint64(1), count, "removing a subscriber after a successful replay must observe one duration")
	assert.EqualValues(t, 0, transport.subscriberCount.Load())

	var lostReplay dto.Metric
	require.NoError(t, transport.metrics.Load().lostCounter(lossReasonHistoryReplayFailed).Write(&lostReplay))
	assert.InDelta(t, 0.0, lostReplay.GetCounter().GetValue(), 0, "a successful replay must not be counted as lost")
}

func TestRedisTransportConcurrentDispatch(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t)

	var wg sync.WaitGroup

	for range 50 {
		wg.Go(func() {
			update := &mercure.Update{
				Topics: []string{"https://example.com/concurrent"},
				Data:   "msg",
			}
			err := transport.Dispatch(context.Background(), update)
			assert.NoError(t, err)
		})
	}

	wg.Wait()
}

func TestHistoryReplayConcurrencyLimit(t *testing.T) {
	t.Parallel()

	// Verify the semaphore limits concurrent history replays.
	transport, mr := newTestTransport(
		t,
		WithHistoryReplayConcurrency(2),
	)
	tss := testTopicMatcherStore()

	// Dispatch a few messages so there's history to replay.
	for i := range 5 {
		update := &mercure.Update{
			Topics: []string{"https://example.com/history"},
			Data:   fmt.Sprintf("msg-%d", i),
		}
		require.NoError(t, transport.Dispatch(context.Background(), update))
	}

	mr.FastForward(200 * time.Millisecond)

	// Start 10 subscribers with LastEventID — they all need history replay.
	const numSubs = 10

	var wg sync.WaitGroup

	errs := make([]error, numSubs)

	for i := range numSubs {
		wg.Go(func() {
			sub := mercure.NewLocalSubscriber(mercure.EarliestLastEventID, testLogger(), tss)
			sub.SetMatchers(topicMatchers([]string{"https://example.com/history"}), nil)
			errs[i] = transport.AddSubscriber(context.Background(), sub)
		})
	}

	wg.Wait()

	// All should succeed — semaphore gates concurrency, doesn't reject.
	for i, err := range errs {
		assert.NoError(t, err, "subscriber %d history replay should succeed", i)
	}
}

// TestCheckVersionFromInfo covers checkVersionFromInfo's version gate and its
// backendType latch. Each subtest builds its own transport because the call
// writes t.backendType.
func TestCheckVersionFromInfo(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		info        string
		wantErr     error
		wantBackend serverType
	}{
		{
			name:        "redis 7 supported",
			info:        "# Server\r\nredis_version:7.2.4\r\n",
			wantErr:     nil,
			wantBackend: serverTypeRedis,
		},
		{
			name:        "redis 6.2 exactly at floor",
			info:        "redis_version:6.2.0\n",
			wantErr:     nil,
			wantBackend: serverTypeRedis,
		},
		{
			name:        "redis 6.1 below floor",
			info:        "redis_version:6.1.9\n",
			wantErr:     ErrUnsupportedServerVersion,
			wantBackend: serverTypeRedis, // latched even on error path
		},
		{
			name:        "redis 5.0 below floor",
			info:        "redis_version:5.0.14\n",
			wantErr:     ErrUnsupportedServerVersion,
			wantBackend: serverTypeRedis, // latched even on error path
		},
		{
			name:        "valkey 8 supported",
			info:        "valkey_version:8.1.6\n",
			wantErr:     nil,
			wantBackend: serverTypeValkey,
		},
		{
			name:        "valkey 7.2 exactly at floor",
			info:        "valkey_version:7.2.0\n",
			wantErr:     nil,
			wantBackend: serverTypeValkey,
		},
		{
			name:        "valkey 7.1 below floor",
			info:        "valkey_version:7.1.0\n",
			wantErr:     ErrUnsupportedServerVersion,
			wantBackend: serverTypeValkey, // latched even on error path
		},
		{
			name:        "unknown server type still permitted (warn)",
			info:        "dragonfly_version:1.0\nredis_version:6.2.0\n",
			wantErr:     nil,
			wantBackend: serverTypeRedis, // dragonfly_version isn't parsed; redis_version wins
		},
		{name: "both present, valkey wins", info: "redis_version:7.2.4\nvalkey_version:8.0.0\n", wantBackend: serverTypeValkey},
		{name: "neither present", info: "# Server\nos:Linux\n", wantBackend: serverTypeUnknown},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			transport, _ := newTestTransport(t)

			err := transport.checkVersionFromInfo(tc.info)
			// Latch must populate on every call, including error paths,
			// so dashboards can see what backend was tried even when
			// validateVersion rejects it.
			assert.Equal(t, tc.wantBackend, transport.backendType,
				"backendType latch must capture parseServerInfo's serverType regardless of version-gate outcome")

			if tc.wantErr == nil {
				require.NoError(t, err)

				return
			}

			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}

// TestCheckVersionFromInfoMissingFieldFatal verifies that an INFO response
// without any parseable version field is fatal in production (skipVersionCheck=false).
func TestCheckVersionFromInfoMissingFieldFatal(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	// Build transport without withSkipVersionCheck but short-circuit NewRedisTransport
	// by constructing the struct directly; we only need a valid receiver for
	// checkVersionFromInfo's option check.
	transport := &RedisTransport{
		opts:   &options{}, // skipVersionCheck = false
		logger: testLogger(),
		client: client,
	}

	err := transport.checkVersionFromInfo("# Server\r\nos:Linux\r\n")
	require.Error(t, err)
	require.ErrorIs(t, err, ErrMissingVersionField)
}

// TestCheckVersionFromInfoMissingFieldSkipped verifies skipVersionCheck bypass
// for the no-parseable-version branch (used by miniredis-backed tests).
func TestCheckVersionFromInfoMissingFieldSkipped(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t) // includes withSkipVersionCheck()

	err := transport.checkVersionFromInfo("# Server\r\nos:Linux\r\n")
	require.NoError(t, err)
}

// TestTTLCleanupTrimsExpiredEntries verifies that when WithEventTTL is enabled,
// the ttlCleanup goroutine issues XTRIM MINID with a timestamp in the past,
// removing entries older than the TTL. Uses miniredis FastForward to advance
// time deterministically.
func TestTTLCleanupTrimsExpiredEntries(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	transport, err := NewRedisTransport(
		client,
		WithLogger(testLogger()),
		WithXReadBlock(10*time.Millisecond),
		WithHealthInterval(24*time.Hour),
		WithPresenceInterval(24*time.Hour),
		WithEventTTL(100*time.Millisecond),
		WithCleanupInterval(50*time.Millisecond),
		withSkipPresenceIntervalCheck(),
		withSkipVersionCheck(),
	)
	require.NoError(t, err)
	t.Cleanup(func() { transport.Close(context.Background()) })

	// Publish several entries at t=0.
	for i := range 3 {
		update := &mercure.Update{
			Topics: []string{"https://example.com/ttl"},
			Data:   fmt.Sprintf("entry-%d", i),
		}
		require.NoError(t, transport.Dispatch(context.Background(), update))
	}

	// Advance miniredis time past TTL and wait for the cleanup tick.
	mr.FastForward(200 * time.Millisecond)

	// Poll until the stream is trimmed or the deadline passes. The cleanup
	// goroutine fires every 50ms so 500ms gives ~10 attempts.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		length, err := client.XLen(context.Background(), "{mercure}").Result()
		if err == nil && length < 3 {
			return
		}

		time.Sleep(20 * time.Millisecond)
	}

	t.Fatal("ttlCleanup did not trim expired entries within deadline")
}

// TestSetCodec verifies the codec can be swapped at runtime via the
// mercure.TransportCodec interface.
func TestSetCodec(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t)

	require.IsType(t, mercure.JSONCodec{}, *transport.codec.Load())

	transport.SetCodec(mercure.GobCodec{})
	assert.IsType(t, mercure.GobCodec{}, *transport.codec.Load())
}

// TestIsStreamNotExist covers the error-string heuristic used when a stream
// has been wiped (FLUSHDB, eviction). The string match is the only signal
// go-redis surfaces for this condition.
func TestIsStreamNotExist(t *testing.T) {
	t.Parallel()

	// isStreamNotExist is called on XInfoGroups errors, which Redis emits as
	// "ERR no such key" (lowercase). Case-sensitive match is intentional —
	// NOGROUP errors from XREADGROUP are a different error surface.
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "xinfogroups on missing stream", err: errXInfoGroupsMissingStream, want: true},
		{name: "wrapped no such key", err: fmt.Errorf("info groups: %w", errLowercaseMissing), want: true},
		{name: "unrelated error", err: errWrongType, want: false},
		{name: "capitalized does not match", err: errCapitalizedMissing, want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.want, isStreamNotExist(tc.err))
		})
	}
}

// TestIsPELEmpty verifies the pending-entry-list emptiness check used to decide
// whether an XREADGROUP batch contains new messages.
func TestIsPELEmpty(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t)

	assert.True(t, transport.isPELEmpty(nil), "nil slice is empty")
	assert.True(t, transport.isPELEmpty([]redis.XStream{}), "empty slice is empty")
	assert.True(t, transport.isPELEmpty([]redis.XStream{
		{Stream: "s", Messages: nil},
		{Stream: "s", Messages: []redis.XMessage{}},
	}), "streams with no messages are empty")
	assert.False(t, transport.isPELEmpty([]redis.XStream{
		{Stream: "s", Messages: []redis.XMessage{{ID: "1-0"}}},
	}), "stream with messages is not empty")
}

// TestDispatchToSubscribersHoldsLockThroughFanOut pins that
// dispatchToSubscribers, including the sharded path's fan-out and wg.Wait,
// never releases t.mu: called with t.mu held, it returns with t.mu still held.
// Dispatch does not need t.mu for correctness. Gap-free delivery to a
// subscriber that joins during a fan-out comes from the cursor:
// processStreamEntry advances lastDispatchedStreamID before the fan-out, and
// AddSubscriber reads it after joining the topic index, so a subscriber the
// live snapshot missed replays the entry from history (see
// dispatchToSubscribers and AddSubscriber). This pins that the dispatch path
// never releases a lock it does not own.
func TestDispatchToSubscribersHoldsLockThroughFanOut(t *testing.T) {
	t.Parallel()

	transport, _ := newShardedTestTransport(t, 4)

	update := &mercure.Update{
		Topics: []string{"https://example.com/lock-invariant"},
		ID:     "lock-invariant-1", Data: "x",
	}

	transport.mu.Lock()
	transport.dispatchToSubscribers(t.Context(), update)

	// The caller must still hold t.mu, so a TryLock from another goroutine fails.
	var concurrent atomic.Bool

	done := make(chan struct{})

	go func() {
		defer close(done)

		if transport.mu.TryLock() {
			concurrent.Store(true)
			transport.mu.Unlock()
		}
	}()

	<-done

	transport.mu.Unlock()

	assert.False(t, concurrent.Load(),
		"dispatchToSubscribers must return with t.mu still held by caller (post-fan-out)")
}

// TestWaitForLimiterFastPath verifies the Allow() fast path: no counter
// increment, no Wait() call.
func TestWaitForLimiterFastPath(t *testing.T) {
	t.Parallel()

	counter := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "waitforlimiter_fastpath_total",
		Help: "Test counter for waitForLimiter fast-path coverage.",
	})
	// rate.Inf means Allow() always returns true.
	limiter := rate.NewLimiter(rate.Inf, 1)

	require.NoError(t, waitForLimiter(t.Context(), limiter, counter, "test"))

	var m dto.Metric

	require.NoError(t, counter.Write(&m))
	assert.InDelta(t, 0.0, m.GetCounter().GetValue(), 0,
		"fast path (Allow=true) must not increment the rate-limited counter")
}

// TestWaitForLimiterSlowPathIncrementsCounter verifies the slow-path
// branch: when Allow() returns false, the rate-limited counter
// increments before Wait(ctx) blocks.
func TestWaitForLimiterSlowPathIncrementsCounter(t *testing.T) {
	t.Parallel()

	counter := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "waitforlimiter_slowpath_total",
		Help: "Test counter for waitForLimiter slow-path coverage.",
	})
	// 1 token/sec, burst 1: first call passes Allow, second forces Wait.
	limiter := rate.NewLimiter(rate.Limit(1), 1)

	require.NoError(t, waitForLimiter(t.Context(), limiter, counter, "test"))

	// Second call: Allow() returns false, Wait blocks ~1s — bound the
	// test with a deadline; the function returns once Wait wakes.
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	require.NoError(t, waitForLimiter(ctx, limiter, counter, "test"))

	var m dto.Metric

	require.NoError(t, counter.Write(&m))
	assert.InDelta(t, 1.0, m.GetCounter().GetValue(), 0,
		"slow path (Allow=false, Wait succeeds) must increment the rate-limited counter exactly once")
}

// TestWaitForLimiterCanceledCtxRejects verifies that a cancelled context is
// rejected without waiting for a token.
func TestWaitForLimiterCanceledCtxRejects(t *testing.T) {
	t.Parallel()

	// Slow limiter (1 token / 1000s, burst 1). Consume the burst, then
	// the next Allow() returns false and Wait would block ~1000s. The
	// cancelled ctx forces a fast-fail on Wait.
	limiter := rate.NewLimiter(rate.Limit(0.001), 1)
	require.True(t, limiter.Allow(), "burst should yield first token")

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err := waitForLimiter(ctx, limiter, nil, "test")
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
}

// TestWaitForLimiterNilLimiterNoOp verifies the optional-limiter shape:
// callers may pass nil to disable rate limiting entirely.
func TestWaitForLimiterNilLimiterNoOp(t *testing.T) {
	t.Parallel()

	require.NoError(t, waitForLimiter(t.Context(), nil, nil, "test"))
}

// TestDispatchAfterCloseAlwaysReturnsClosedSentinel races concurrent
// Dispatch goroutines against Close. After Close fires, every error
// returned by Dispatch must be ErrClosedTransport — never a wrapped
// `redis client is closed` or `context canceled` error. This binds the
// post-publish select that converts the race-window's redis/ctx error
// to the canonical sentinel.
func TestDispatchAfterCloseAlwaysReturnsClosedSentinel(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t)

	const dispatchers = 16

	var (
		started sync.WaitGroup
		done    sync.WaitGroup
		stop    atomic.Bool
	)

	started.Add(dispatchers)
	done.Add(dispatchers)

	errs := make(chan error, 1024)

	for i := range dispatchers {
		go func(id int) {
			defer done.Done()

			started.Done()

			for !stop.Load() {
				err := transport.Dispatch(context.Background(), &mercure.Update{
					Topics: []string{"https://example.com/race"},
					Data:   strconv.Itoa(id),
				})
				if err != nil {
					select {
					case errs <- err:
					default: // shed once buffer fills — we only need a sample
					}
				}
			}
		}(i)
	}

	started.Wait()
	// Close while dispatchers are mid-flight. The race window we care
	// about is between Dispatch's entry select(t.closed) and the
	// publishScript.Run call.
	require.NoError(t, transport.Close(context.Background()))

	stop.Store(true)
	done.Wait()
	close(errs)

	for err := range errs {
		require.ErrorIs(t, err, ErrClosedTransport,
			"every Dispatch error after Close must be ErrClosedTransport, got: %v", err)
	}
}

// TestAddSubscriberRecheckClosedUnderMu pins AddSubscriber's re-check of
// t.closed under t.mu. The top-of-function select is TOCTOU: a call that
// passes it before Close but takes t.mu after Close's disconnect walk would
// list its subscriber after the walk. Close walks only once (closedOnce), so
// that subscriber's out channel is never closed and it stays registered.
//
// A real Close cannot pin this. Close closes t.closed before it takes t.mu,
// so once it has finished, a re-check placed just before t.mu.Lock() also
// reads "closed" and rejects like the correct one. The test instead holds
// t.mu while AddSubscriber is queued on it and replays Close's closedOnce
// body under that lock, so only a re-check under t.mu sees "closed". The
// replayed steps run for real so no background goroutine (e.g. the
// xreadgroupListener) outlives the test and trips TestMain's goleak check.
func TestAddSubscriberRecheckClosedUnderMu(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t, WithXReadBlock(time.Millisecond))
	tss := testTopicMatcherStore()

	s := mercure.NewLocalSubscriber("", testLogger(), tss)
	s.SetMatchers(topicMatchers([]string{"https://example.com/add-race"}), nil)

	// Hold t.mu for the whole window described above.
	unlock := lockTransportMu(t, transport)

	addErr := make(chan error, 1)

	go func() {
		addErr <- transport.AddSubscriber(context.Background(), s)
	}()

	// AddSubscriber has passed the top select ("not closed") and is blocked on
	// t.mu, so any rejection (or leak) below comes from the re-check.
	waitBlockedOnMu(t, "AddSubscriber")

	// Close's closedOnce body, under the t.mu the test holds.
	close(transport.closed)
	transport.cancel()
	<-transport.listenerDone
	transport.closeShardChannels()
	transport.wg.Wait()
	transport.disconnectAllSubscribers()
	transport.closedOnce.Do(func() {}) // consume: makes t.Cleanup's later Close() a no-op
	unlock()

	err := <-addErr
	if err == nil {
		// Success here is the leak the re-check prevents: the subscriber was
		// listed after the walk, so nothing closes its channel.
		select {
		case _, open := <-s.Receive():
			assert.False(t, open,
				"AddSubscriber returned success after the disconnect walk had already run under t.mu — the re-check of t.closed was skipped, or ran before t.mu.Lock() instead of after it")
		default:
			t.Error("AddSubscriber returned success after the disconnect walk had already run under t.mu — the re-check of t.closed was skipped, or ran before t.mu.Lock() instead of after it")
		}

		return
	}

	assert.ErrorIs(t, err, ErrClosedTransport,
		"AddSubscriber must reject with ErrClosedTransport when its re-check runs under t.mu, after the disconnect walk has already run, got: %v", err)
}

// TestXReadErrorBackoffShape pins the listener's reconnect delay: 1s base,
// doubling per consecutive error, capped at 30s.
func TestXReadErrorBackoffShape(t *testing.T) {
	t.Parallel()

	cases := []struct {
		consecutiveErrors int
		want              time.Duration
	}{
		{consecutiveErrors: 0, want: time.Second},
		{consecutiveErrors: 1, want: time.Second},
		{consecutiveErrors: 2, want: 2 * time.Second},
		{consecutiveErrors: 3, want: 4 * time.Second},
		{consecutiveErrors: 4, want: 8 * time.Second},
		{consecutiveErrors: 5, want: 16 * time.Second},
		{consecutiveErrors: 6, want: 30 * time.Second}, // capped (would be 32s)
		{consecutiveErrors: 100, want: 30 * time.Second},
		{consecutiveErrors: 1_000_000, want: 30 * time.Second},
	}

	for _, tc := range cases {
		got := xreadErrorBackoff(tc.consecutiveErrors)
		assert.Equal(t, tc.want, got, "consecutiveErrors=%d", tc.consecutiveErrors)
	}
}

// TestRunHealthPingSuccess exercises the happy path of the health check body:
// PING succeeds, failures reset to 0, healthy gauge set to 1.
func TestRunHealthPingSuccess(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	transport, _ := newTestTransport(t, WithPrometheusRegisterer(reg))

	// Start from a degraded state to verify recovery path.
	failures := transport.runHealthPing(context.Background(), 5)
	assert.Equal(t, 0, failures, "successful PING resets failure count")
}

// failingHealthCtx returns a context already past its deadline, so an internal
// PING fails with context.DeadlineExceeded — a real health failure runHealthPing
// counts. It is not a cancelled context: a cancelled context is a
// graceful shutdown, which runHealthPing must not count (see isShutdownCancel and
// TestShutdownCancelSuppressesBackgroundErrors). Simulates a Redis outage fast,
// without a network wait.
func failingHealthCtx(t *testing.T) context.Context {
	t.Helper()

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Hour))
	t.Cleanup(cancel)

	return ctx
}

// TestRunHealthPingFailureIncrementsCounter exercises the failure path: an
// expired-deadline PING returns context.DeadlineExceeded immediately, which
// increments the consecutive-failure counter and flips the healthy gauge to 0
// once the threshold is crossed. The expired deadline (not a cancelled context,
// which would be a graceful shutdown the loop ignores) avoids go-redis
// connection-pool retry backoff (5 attempts x ~1s) that would make the test slow.
func TestRunHealthPingFailureIncrementsCounter(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	transport, _ := newTestTransport(
		t,
		WithPrometheusRegisterer(reg),
		WithHealthThreshold(2),
	)

	ctx := failingHealthCtx(t)

	failures := 0
	for range 3 {
		failures = transport.runHealthPing(ctx, failures)
	}

	assert.Equal(t, 3, failures, "three consecutive failures accumulate")
}

// TestHistoryReplayFallsBackToReplayAll verifies the Pass 2 fallback engages
// when a subscriber's Last-Event-ID was trimmed from the stream. The test
// publishes events, then adds a subscriber with a valid UUIDv7 Last-Event-ID
// that does not match any published event — Pass 1 scans and finds nothing,
// rs.found stays false, and run() invokes replayAll.
func TestHistoryReplayFallsBackToReplayAll(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	transport, _ := newTestTransport(t, WithPrometheusRegisterer(reg))

	// Publish three events with transport-generated IDs.
	for i := range 3 {
		require.NoError(t, transport.Dispatch(context.Background(), &mercure.Update{
			Topics: []string{"https://example.com/replay-all"},
			Data:   fmt.Sprintf("msg-%d", i),
		}))
	}

	// Subscribe with a valid UUIDv7 that was never dispatched. The timestamp
	// prefix places it in the past so seekStreamID scans from stream start.
	tss := testTopicMatcherStore()
	sub := mercure.NewLocalSubscriber("urn:uuid:00000000-0000-7000-8000-000000000000", testLogger(), tss)
	sub.SetMatchers(topicMatchers([]string{"https://example.com/replay-all"}), nil)

	require.NoError(t, transport.AddSubscriber(context.Background(), sub))

	// Pass 2 should have replayed all three events.
	received := 0

	for range 3 {
		select {
		case <-sub.Receive():
			received++
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for replayAll delivery; got %d", received)
		}
	}

	assert.Equal(t, 3, received)

	// The full-scan fallback counter must tick exactly once.
	families, err := reg.Gather()
	require.NoError(t, err)

	var fallbackCount float64

	for _, f := range families {
		if f.GetName() == metricHistoryReplayFallbackTotal {
			fallbackCount = f.GetMetric()[0].GetCounter().GetValue()
		}
	}

	assert.InDelta(t, 1.0, fallbackCount, 0,
		"history_replay_fallback_total should record exactly one full-scan after Pass 2 success")
}

// TestHistoryReplay_FutureUUIDv7SkipsPass2 asserts end-to-end: when a
// subscriber arrives with a Last-Event-ID whose UUIDv7 wall-clock
// timestamp is past the hub's `time.Now() + clockSkewMargin`, the
// transport must skip both Pass 1 and Pass 2, finalize the subscriber
// cleanly, and not increment the history_replay_fallback_total counter.
// The predicate has unit coverage in historyreplay_guard_test.go; this
// test exercises the full AddSubscriber → run() → HistoryDispatched flow
// against miniredis.
func TestHistoryReplay_FutureUUIDv7SkipsPass2(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	transport, _ := newTestTransport(t, WithPrometheusRegisterer(reg))

	for i := range 3 {
		require.NoError(t, transport.Dispatch(context.Background(), &mercure.Update{
			Topics: []string{"https://example.com/future-id"},
			Data:   fmt.Sprintf("msg-%d", i),
		}))
	}

	// A UUIDv7 dated 24h ahead, well past the default clockSkewMargin.
	futureID := futureUUIDv7(24 * time.Hour)

	tss := testTopicMatcherStore()
	sub := mercure.NewLocalSubscriber(futureID, testLogger(), tss)
	sub.SetMatchers(topicMatchers([]string{"https://example.com/future-id"}), nil)

	require.NoError(t, transport.AddSubscriber(context.Background(), sub),
		"AddSubscriber must succeed for a future-dated Last-Event-ID (no replay attempted)")

	// The guard skips Pass 2, so the full-scan counter stays at 0.
	families, err := reg.Gather()
	require.NoError(t, err)

	var fallbackCount float64

	for _, f := range families {
		if f.GetName() == metricHistoryReplayFallbackTotal && len(f.GetMetric()) > 0 {
			fallbackCount = f.GetMetric()[0].GetCounter().GetValue()
		}
	}

	assert.InDelta(t, 0.0, fallbackCount, 0,
		"future-dated Last-Event-ID must not trigger Pass 2 full-stream replay")

	// A live publish must still reach the subscriber, proving HistoryDispatched
	// and Ready ran. A unique token keeps the drain loop from stopping on a
	// catch-up event.
	const liveToken = "live-after-guard-3f8c1a2e"

	require.NoError(t, transport.Dispatch(context.Background(), &mercure.Update{
		Topics: []string{"https://example.com/future-id"},
		Data:   liveToken,
	}))

	var received []string

	deadline := time.After(2 * time.Second)

drainLoop:
	for {
		select {
		case u := <-sub.Receive():
			received = append(received, u.Data)
			if u.Data == liveToken {
				break drainLoop
			}
		case <-deadline:
			t.Fatalf("future-ID guard failed to finalize subscriber; live event never arrived. Received: %v", received)
		}
	}

	// Catch-up events from the pre-add publishes may also arrive
	// (listener-vs-AddSubscriber ordering); the live token must be among them.
	assert.Contains(t, received, liveToken,
		"the post-guard live publish must reach the subscriber")
}

// TestHistoryReplay_PublishedEventNotMisclassifiedAsFuture pins that an
// earliest request replays every published event: the future-ID guard does
// not fire for the earliest sentinel.
func TestHistoryReplay_PublishedEventNotMisclassifiedAsFuture(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	transport, _ := newTestTransport(t, WithPrometheusRegisterer(reg))

	for i := range 3 {
		require.NoError(t, transport.Dispatch(context.Background(), &mercure.Update{
			Topics: []string{"https://example.com/earliest-replay"},
			Data:   fmt.Sprintf("msg-%d", i),
		}))
	}

	// An earliest request replays from the start of the stream; the guard must
	// not fire for it.
	tss := testTopicMatcherStore()
	sub := mercure.NewLocalSubscriber(mercure.EarliestLastEventID, testLogger(), tss)
	sub.SetMatchers(topicMatchers([]string{"https://example.com/earliest-replay"}), nil)

	require.NoError(t, transport.AddSubscriber(context.Background(), sub))

	// All 3 pre-add events must be replayed.
	got := make(map[string]bool)

	deadline := time.After(2 * time.Second)

	for len(got) < 3 {
		select {
		case u := <-sub.Receive():
			got[u.Data] = true
		case <-deadline:
			t.Fatalf("earliest replay dropped events; received: %v", got)
		}
	}

	for i := range 3 {
		assert.True(t, got[fmt.Sprintf("msg-%d", i)],
			"msg-%d must be replayed (the guard must not fire on an earliest request)", i)
	}
}

// TestReceivePathDropsForbiddenEntryEndToEnd drives the REAL history-replay
// caller path (AddSubscriber → run → decodeStreamEntry) with a poisoned stream
// entry injected directly into the stream. It proves the guard's nil return
// actually prevents delivery to a live subscriber — not merely that
// decodeStreamEntry returns nil — and that the drop is counted. Complements the
// chokepoint-direct TestDecodeStreamEntryDropsForbiddenSSEChars.
func TestReceivePathDropsForbiddenEntryEndToEnd(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	transport, mr := newTestTransport(t, WithPrometheusRegisterer(reg))
	codec := *transport.codec.Load()
	streamKey := transport.key("")
	topic := "https://example.com/e2e-drop"

	clean, err := codec.Marshal(&mercure.Update{Topics: []string{topic}, ID: "urn:uuid:clean", Data: "good"})
	require.NoError(t, err)

	// Forbidden CR in the decoded id — the receive-side guard must drop this.
	poisoned, err := codec.Marshal(&mercure.Update{Topics: []string{topic}, ID: "x\rid: forged", Data: "bad"})
	require.NoError(t, err)

	_, err = mr.XAdd(streamKey, "*", []string{"eventID", "urn:uuid:clean", "data", string(clean)})
	require.NoError(t, err)

	_, err = mr.XAdd(streamKey, "*", []string{"eventID", "urn:uuid:bad", "data", string(poisoned)})
	require.NoError(t, err)

	tss := testTopicMatcherStore()
	sub := mercure.NewLocalSubscriber(mercure.EarliestLastEventID, testLogger(), tss)
	sub.SetMatchers(topicMatchers([]string{topic}), nil)
	require.NoError(t, transport.AddSubscriber(context.Background(), sub))

	got := map[string]bool{}

	select {
	case u := <-sub.Receive():
		got[u.Data] = true
	case <-time.After(2 * time.Second):
		t.Fatal("clean update was not replayed")
	}

	// Grace window: a wrongly-delivered poisoned entry would arrive around now.
	select {
	case u := <-sub.Receive():
		got[u.Data] = true
	case <-time.After(300 * time.Millisecond):
	}

	assert.True(t, got["good"], "the clean update must be replayed to the subscriber")
	assert.False(t, got["bad"], "the poisoned update must be dropped, never delivered")
	assert.GreaterOrEqual(t, forbiddenSSECharCount(t, reg), float64(1), "the forbidden drop must be counted")
}

// counterValue returns the value of a no-label counter metric by name (0 if absent).
func counterValue(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()

	families, err := reg.Gather()
	require.NoError(t, err)

	for _, f := range families {
		if f.GetName() == name && len(f.GetMetric()) > 0 {
			return f.GetMetric()[0].GetCounter().GetValue()
		}
	}

	return 0
}

// TestReadyAfterStartup verifies Ready returns nil once startup has set the
// healthy flag, before the first health-check PING has run.
func TestReadyAfterStartup(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t)

	require.NoError(t, transport.Ready(context.Background()))
	require.NoError(t, transport.Live(context.Background()))
}

// TestReadyAfterCloseReturnsError verifies Ready/Live both report the closed
// state with ErrClosedTransport so readiness and liveness probes flip
// immediately on shutdown.
func TestReadyAfterCloseReturnsError(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t)
	require.NoError(t, transport.Close(context.Background()))

	err := transport.Ready(context.Background())
	require.ErrorIs(t, err, ErrClosedTransport)

	err = transport.Live(context.Background())
	require.ErrorIs(t, err, ErrClosedTransport)
}

// TestReadyFlipsUnhealthyOnPingFailures verifies Ready returns
// ErrTransportUnhealthy after the health loop records healthThreshold
// consecutive PING failures. Simulates a real PING timeout (an expired-deadline
// context → DeadlineExceeded) so the test is fast and doesn't depend on network
// timeouts — not a cancelled context, which is a graceful shutdown the health
// loop intentionally ignores.
func TestReadyFlipsUnhealthyOnPingFailures(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t, WithHealthThreshold(2))

	require.NoError(t, transport.Ready(context.Background()), "healthy after startup")

	ctx := failingHealthCtx(t)

	failures := 0
	for range 3 {
		failures = transport.runHealthPing(ctx, failures)
	}

	err := transport.Ready(context.Background())
	require.ErrorIs(t, err, ErrTransportUnhealthy)

	// Live does not flip on Redis outages, only on Close.
	require.NoError(t, transport.Live(context.Background()),
		"Live stays nil during transient Redis outage")
}

// TestReadyRecoversAfterSuccessfulPing verifies Ready returns nil again once
// a subsequent PING succeeds after the transport was marked unhealthy.
func TestReadyRecoversAfterSuccessfulPing(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t, WithHealthThreshold(1))

	failures := transport.runHealthPing(failingHealthCtx(t), 0)
	require.GreaterOrEqual(t, failures, 1)

	require.ErrorIs(t, transport.Ready(context.Background()), ErrTransportUnhealthy)

	// Fresh context pings miniredis successfully → healthy flips back.
	_ = transport.runHealthPing(context.Background(), failures)
	require.NoError(t, transport.Ready(context.Background()))
}

// panickingCtx simulates Caddy's zero-value caddy.Context whose embedded
// context.Context is nil: every method panics on invocation. This is what
// go-redis triggers when the hub closes the transport during `caddy validate`.
type panickingCtx struct{}

func (panickingCtx) Deadline() (time.Time, bool) { panic("panickingCtx.Deadline called") }
func (panickingCtx) Done() <-chan struct{}       { panic("panickingCtx.Done called") }
func (panickingCtx) Err() error                  { panic("panickingCtx.Err called") }
func (panickingCtx) Value(any) any               { panic("panickingCtx.Value called") }

// TestCloseIgnoresCallerContext verifies Close tolerates a caller ctx whose
// internals would panic on any Done/Err/Deadline call. Caddy's
// TransportDestructor invokes Close(caddy.ActiveContext()) during
// `caddy validate`; that returns a zero-value caddy.Context with a nil
// embedded context, and go-redis's internal ctx.Done() call nil-derefs.
// Close uses its own background context instead.
func TestCloseIgnoresCallerContext(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t)

	assert.NotPanics(t, func() {
		_ = transport.Close(panickingCtx{})
	})
}

// TestRedisPoolSizeIntrospection pins the type-switch contract for
// warnSuspiciousClientOptions. The data-plane client types
// (*redis.Client for single-instance + Sentinel-via-failover,
// *redis.ClusterClient for Cluster mode, *redis.Ring for client-side
// sharded deployments) expose PoolSize through Options(); any other
// client type returns 0 so the warning fires only when we have
// authoritative information.
func TestRedisPoolSizeIntrospection(t *testing.T) {
	t.Parallel()

	t.Run("redis.Client", func(t *testing.T) {
		t.Parallel()

		c := redis.NewClient(&redis.Options{Addr: "localhost:0", PoolSize: 42})

		t.Cleanup(func() { _ = c.Close() })

		assert.Equal(t, 42, redisPoolSize(c))
	})

	t.Run("redis.ClusterClient", func(t *testing.T) {
		t.Parallel()

		c := redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:    []string{"localhost:0"},
			PoolSize: 30,
		})

		t.Cleanup(func() { _ = c.Close() })

		assert.Equal(t, 30, redisPoolSize(c))
	})

	t.Run("redis.Ring", func(t *testing.T) {
		t.Parallel()

		c := redis.NewRing(&redis.RingOptions{
			Addrs:    map[string]string{"shard1": "localhost:0"},
			PoolSize: 25,
		})

		t.Cleanup(func() { _ = c.Close() })

		assert.Equal(t, 25, redisPoolSize(c))
	})
}

// TestWarnSuspiciousClientOptionsHistoryReplayExceedsPool pins the
// soft-warning behavior: the predicate is `>=` (not `>`) because at exact
// equality every replay slot consumes a connection and other operations
// have zero pool headroom — head-of-line blocking is immediate. Strictly
// below the pool is silent; at-or-above warns.
func TestWarnSuspiciousClientOptionsHistoryReplayExceedsPool(t *testing.T) {
	t.Parallel()

	t.Run("exceeds pool: warns", func(t *testing.T) {
		t.Parallel()

		logger, records := captureLogger()

		c := redis.NewClient(&redis.Options{Addr: "localhost:0", PoolSize: 10})

		t.Cleanup(func() { _ = c.Close() })

		o := defaultOptions()
		o.historyReplayConcurrency = 50

		warnSuspiciousClientOptions(o, c, logger)

		assert.NotNil(t, findWarnMessage(*records, "WithHistoryReplayConcurrency"))
	})

	t.Run("equal to pool: warns (zero headroom = starvation)", func(t *testing.T) {
		t.Parallel()

		logger, records := captureLogger()

		c := redis.NewClient(&redis.Options{Addr: "localhost:0", PoolSize: 50})

		t.Cleanup(func() { _ = c.Close() })

		o := defaultOptions()
		o.historyReplayConcurrency = 50

		warnSuspiciousClientOptions(o, c, logger)

		assert.NotNil(t, findWarnMessage(*records, "WithHistoryReplayConcurrency"),
			"concurrency at pool size leaves zero headroom for non-replay ops")
	})

	t.Run("strictly below pool: silent", func(t *testing.T) {
		t.Parallel()

		logger, records := captureLogger()

		c := redis.NewClient(&redis.Options{Addr: "localhost:0", PoolSize: 100})

		t.Cleanup(func() { _ = c.Close() })

		o := defaultOptions()
		o.historyReplayConcurrency = 50

		warnSuspiciousClientOptions(o, c, logger)

		assert.Empty(t, *records, "concurrency strictly below pool size should not warn")
	})
}

// TestHistoryReplayConcurrencyDefaultFollowsPoolSize pins the derived
// default: half the go-redis pool, at least 1 and at most 20, so the default
// never trips the pool-exhaustion Warn. An explicit setting is kept as-is and
// still warns.
func TestHistoryReplayConcurrencyDefaultFollowsPoolSize(t *testing.T) {
	t.Parallel()

	const poolWarn = "WithHistoryReplayConcurrency reaches or exceeds"

	for _, tc := range []struct {
		name     string
		poolSize int
		opts     []Option
		want     int
		wantWarn bool
	}{
		{name: "1-vCPU default pool", poolSize: 10, want: 5},
		{name: "pool of one", poolSize: 1, want: 1},
		{name: "large pool is capped", poolSize: 400, want: maxDefaultHistoryReplayConcurrency},
		{name: "explicit setting kept", poolSize: 10, opts: []Option{WithHistoryReplayConcurrency(20)}, want: 20, wantWarn: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			logs := &safeBuffer{}
			client := redis.NewClient(&redis.Options{Addr: miniredis.RunT(t).Addr(), PoolSize: tc.poolSize})

			transport, err := NewRedisTransport(client, append([]Option{
				WithLogger(slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelWarn}))),
				WithXReadBlock(50 * time.Millisecond),
				WithHealthInterval(24 * time.Hour),
				WithPresenceInterval(24 * time.Hour),
				withSkipPresenceIntervalCheck(),
				withSkipVersionCheck(),
			}, tc.opts...)...)
			require.NoError(t, err)
			t.Cleanup(func() { transport.Close(context.Background()) })

			assert.Equal(t, tc.want, cap(transport.historyReplaySem))
			assert.Equal(t, tc.wantWarn, strings.Contains(logs.String(), poolWarn), "logs: %s", logs.String())
		})
	}
}

// TestReadyRespectsContext verifies Ready returns the caller's ctx error
// (wrapped) when the probe ctx is already cancelled, rather than misreporting
// health state.
func TestReadyRespectsContext(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := transport.Ready(ctx)
	require.ErrorIs(t, err, context.Canceled)
}

// countOf returns how many elements of msgs equal msg.
func countOf(msgs []string, msg string) int {
	n := 0

	for _, m := range msgs {
		if m == msg {
			n++
		}
	}

	return n
}
