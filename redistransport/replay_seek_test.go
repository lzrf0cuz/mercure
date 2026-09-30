package redistransport

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dunglas/mercure"
	"github.com/gofrs/uuid/v5"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const replaySeekTopic = "https://example.com/replay-seek"

// publishReplaySeekEvents publishes one update per byte of data, in order, with
// that byte as its data and the ID at the same index in ids. An empty ID lets
// the transport assign a UUIDv7. It returns the IDs the updates carry.
//
// It waits until the listener has run its subscriber match for the last
// update. The listener stores its cursor before that match, so a subscriber
// added between the two would get the last update from both the replay and
// live dispatch. After the match, it gets it from the replay only.
func publishReplaySeekEvents(t *testing.T, tr *RedisTransport, data string, ids []string) []string {
	t.Helper()
	require.Len(t, ids, len(data))

	last := data[len(data)-1:]
	matched := make(chan struct{})

	var once sync.Once

	tr.setAfterMatchHookForTests(func(_ int, u *mercure.Update) {
		if u.Data == last {
			once.Do(func() { close(matched) })
		}
	})

	got := make([]string, 0, len(ids))

	for i, id := range ids {
		u := &mercure.Update{ID: id, Topics: []string{replaySeekTopic}, Data: data[i : i+1]}
		require.NoError(t, tr.Dispatch(context.Background(), u))

		got = append(got, u.ID)
	}

	select {
	case <-matched:
	case <-time.After(5 * time.Second):
		t.Fatalf("the listener did not dispatch %q", last)
	}

	return got
}

// replayAfter subscribes with lastEventID and returns the data of every update
// the history replay delivered, in order. AddSubscriber returns once the replay
// has dispatched, and nothing is published afterwards, so the buffer holds the
// whole replay.
func replayAfter(t *testing.T, tr *RedisTransport, lastEventID string) []string {
	t.Helper()

	sub := mercure.NewLocalSubscriber(lastEventID, testLogger(), testTopicMatcherStore())
	sub.SetMatchers(topicMatchers([]string{replaySeekTopic}), nil)
	require.NoError(t, tr.AddSubscriber(context.Background(), sub))

	return drainData(sub)
}

// drainData returns the data of the updates buffered for sub, in order, without
// waiting for more.
func drainData(sub *mercure.LocalSubscriber) []string {
	var data []string

	for {
		select {
		case u := <-sub.Receive():
			data = append(data, u.Data)
		default:
			return data
		}
	}
}

// uuidV7At returns a UUIDv7 URN whose timestamp is at.
func uuidV7At(t *testing.T, at time.Time) string {
	t.Helper()

	u, err := uuid.NewV7AtTime(at)
	require.NoError(t, err)

	return "urn:uuid:" + u.String()
}

// A publisher may choose its own event IDs, such as the UUIDv4 URNs in the
// Mercure spec's examples. A non-v7 UUID's leading bits are random, not a
// timestamp, so the replay must not seek from them. It scans from the start of
// the stream, finds the ID, and replays only the events after it.
func TestHistoryReplayNonV7UUIDSeeksFromStreamStart(t *testing.T) {
	t.Parallel()

	const (
		v4ID     = "urn:uuid:9b2e1c4a-3f1d-4a8e-9c3b-2d7f6e5a4b3c"
		allOnes4 = "urn:uuid:ffffffff-ffff-4fff-bfff-ffffffffffff"
	)

	cases := []struct {
		name     string
		data     string
		ids      []string
		lastID   string
		expected []string
	}{
		{"v4 ID in the middle", "abcd", []string{"", "", v4ID, ""}, v4ID, []string{"d"}},
		{"all-ones v4 ID first", "abc", []string{allOnes4, "", ""}, allOnes4, []string{"b", "c"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tr, _ := newTestTransport(t)
			publishReplaySeekEvents(t, tr, tc.data, tc.ids)

			sub := mercure.NewLocalSubscriber(tc.lastID, testLogger(), testTopicMatcherStore())
			assert.Equal(t, "-", tr.newHistoryReplayState(sub, "+").seekStreamID)

			assert.Equal(t, tc.expected, replayAfter(t, tr, tc.lastID))
		})
	}
}

// Control: a UUIDv7 ID still seeks from its timestamp less the clock-skew
// margin, and the replay delivers the events after it.
func TestHistoryReplayV7UUIDSeeksFromTimestamp(t *testing.T) {
	t.Parallel()

	tr, _ := newTestTransport(t)
	ids := publishReplaySeekEvents(t, tr, "abcd", []string{"", "", "", ""})
	require.True(t, isUUIDv7(ids[2]))

	want, _, err := uuidv7ToStreamID(ids[2], tr.opts.clockSkewMarginMs())
	require.NoError(t, err)

	sub := mercure.NewLocalSubscriber(ids[2], testLogger(), testTopicMatcherStore())
	assert.Equal(t, want, tr.newHistoryReplayState(sub, "+").seekStreamID)

	assert.Equal(t, []string{"d"}, replayAfter(t, tr, ids[2]))
}

// A publisher whose clock runs ahead of Redis by more than the clock-skew
// margin dates its UUIDv7 past its stream entry, so the timestamp seek starts
// after the entry. Pass 1 then scans the entries before the seek, finds the ID,
// and replays exactly the events after it. Redis's clock is set an hour back,
// so the ID is not dated in the future at resume time.
func TestHistoryReplayV7UUIDAheadOfItsEntry(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		// atSeek publishes d with Redis's clock at the seek, so d's stream ID
		// equals the seek. The replay must deliver d exactly once. When false,
		// all entries precede the seek and the seek is past the last entry.
		atSeek bool
	}{
		{"seek past the last entry", false},
		{"entries before the seek and one at it", true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tr, mr := newTestTransport(t)
			margin := tr.opts.clockSkewMargin

			redisNow := time.Now().Add(-time.Hour)
			mr.SetTime(redisNow)

			// X is dated two margins after its entry, so its seek is one margin
			// after the entry.
			x := uuidV7At(t, redisNow.Add(2*margin))

			if tc.atSeek {
				publishReplaySeekEvents(t, tr, "abXc", []string{"", "", x, ""})
				mr.SetTime(redisNow.Add(margin))
				publishReplaySeekEvents(t, tr, "d", []string{""})
			} else {
				publishReplaySeekEvents(t, tr, "abXcd", []string{"", "", x, "", ""})
			}

			sub := mercure.NewLocalSubscriber(x, testLogger(), testTopicMatcherStore())
			rs := tr.newHistoryReplayState(sub, *tr.lastDispatchedStreamID.Load())

			if tc.atSeek {
				require.Equal(t, rs.seekStreamID, rs.toStreamID, "d's stream ID must be the seek")
			} else {
				require.True(t, compareStreamIDs(rs.seekStreamID, rs.toStreamID), "the seek must be past the last entry")
			}

			assert.Equal(t, []string{"c", "d"}, replayAfter(t, tr, x))
		})
	}
}

// xrangeRecorder records each XRANGE's bounds and how many entries it
// returned.
type xrangeRecorder struct {
	mu    sync.Mutex
	calls []xrangeCall
	on    atomic.Bool
}

type xrangeCall struct {
	start, end string
	entries    int
}

func (*xrangeRecorder) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *xrangeRecorder) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		err := next(ctx, cmd)

		if c, ok := cmd.(*redis.XMessageSliceCmd); ok && cmd.Name() == "xrange" && h.on.Load() {
			h.mu.Lock()
			args := cmd.Args()
			h.calls = append(h.calls, xrangeCall{start: args[2].(string), end: args[3].(string), entries: len(c.Val())})
			h.mu.Unlock()
		}

		return err
	}
}

func (*xrangeRecorder) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

// A hub-assigned UUIDv7 trimmed past retention still falls back to the Pass 2
// full replay. The scan before its seek costs one XRANGE that returns nothing:
// the seek is before the stream's first remaining entry.
func TestHistoryReplayTrimmedV7UUIDFallsBackToFullReplay(t *testing.T) {
	t.Parallel()

	rec := &xrangeRecorder{}
	tr := newHookedTestTransport(t, []redis.Hook{rec})
	ids := publishReplaySeekEvents(t, tr, "abcd", []string{"", "", "", ""})

	entries, err := tr.client.XRange(context.Background(), tr.key(""), "-", "+").Result()
	require.NoError(t, err)
	require.Len(t, entries, 4)
	require.NoError(t, tr.client.XTrimMinID(context.Background(), tr.key(""), entries[2].ID).Err())

	rec.on.Store(true)

	assert.Equal(t, []string{"c", "d"}, replayAfter(t, tr, ids[1]))

	rec.mu.Lock()
	defer rec.mu.Unlock()

	// Pass 1 from the seek, Pass 1 before the seek, then Pass 2 from the start.
	require.Len(t, rec.calls, 3, "%+v", rec.calls)

	seek, to := rec.calls[0].start, rec.calls[0].end
	assert.Equal(t, xrangeCall{start: "-", end: "(" + seek, entries: 0}, rec.calls[1],
		"the range before a trimmed ID's seek is empty")
	assert.Equal(t, xrangeCall{start: "-", end: to, entries: 2}, rec.calls[2])
}

// When the seek is past the replay's end (toStreamID, the listener's cursor
// when the subscriber joined), the scan before the seek stops at that end. The
// entries after it reach the subscriber through live dispatch, so the replay
// must not deliver them too.
func TestHistoryReplayRescanStopsAtReplayEnd(t *testing.T) {
	t.Parallel()

	tr, mr := newTestTransport(t)
	redisNow := time.Now().Add(-time.Hour)
	mr.SetTime(redisNow)

	x := uuidV7At(t, redisNow.Add(2*tr.opts.clockSkewMargin))
	publishReplaySeekEvents(t, tr, "abXcd", []string{"", "", x, "", ""})

	entries, err := tr.client.XRange(context.Background(), tr.key(""), "-", "+").Result()
	require.NoError(t, err)
	require.Len(t, entries, 5)

	// Replay up to c, as if the subscriber joined before the listener read d.
	sub := mercure.NewLocalSubscriber(x, testLogger(), testTopicMatcherStore())
	sub.SetMatchers(topicMatchers([]string{replaySeekTopic}), nil)

	rs := tr.newHistoryReplayState(sub, entries[3].ID)
	require.True(t, compareStreamIDs(rs.seekStreamID, rs.toStreamID), "the seek must be past the replay's end")
	require.NoError(t, rs.run(context.Background()))

	assert.Equal(t, []string{"c"}, drainData(sub))
}

// The scan before the seek pages through the stream like the other scans: an
// ID on a later page is found, and exactly the entries after it are replayed.
func TestHistoryReplayRescanFindsIDOnALaterPage(t *testing.T) {
	t.Parallel()

	tr, mr := newTestTransport(t)
	redisNow := time.Now().Add(-time.Hour)
	mr.SetTime(redisNow)

	// A full page of fillers on another topic, and one more, put X on the scan's
	// second page. Every filler carries the same bytes.
	const fillers = historyPageSize + 1

	data, err := (*tr.codec.Load()).Marshal(&mercure.Update{
		ID:     "filler",
		Topics: []string{"https://example.com/filler"},
		Data:   "f",
	})
	require.NoError(t, err)

	pipe := tr.client.Pipeline()

	for range fillers {
		pipe.XAdd(context.Background(), &redis.XAddArgs{
			Stream: tr.key(""),
			Values: map[string]any{"eventID": "filler", "data": string(data)},
		})
	}

	_, err = pipe.Exec(context.Background())
	require.NoError(t, err)

	x := uuidV7At(t, redisNow.Add(2*tr.opts.clockSkewMargin))
	publishReplaySeekEvents(t, tr, "Xcd", []string{x, "", ""})

	assert.Equal(t, []string{"c", "d"}, replayAfter(t, tr, x))
}
