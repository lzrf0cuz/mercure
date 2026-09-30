package redistransport

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/dunglas/mercure"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	replayHeaderTopic      = "https://example.com/replay-header"
	replayHeaderOtherTopic = "https://example.com/replay-header-other"
	replayHeaderLiveData   = "live-marker-7c1e"
	// replayHeaderUnknownID is a UUIDv7 dated in 1970, so it is neither in the
	// stream nor future-dated.
	replayHeaderUnknownID = "urn:uuid:00000000-0000-7000-8000-000000000000"
)

// replayHeaderStream is the history each case starts from: a and b on the
// subscribed topic, then c on another topic, so the stream's last entry is
// never one the subscriber receives.
type replayHeaderStream struct {
	a, b, c string
}

// runReplayResponseHeaderCases pins the Mercure-Last-Event-ID answer on
// history replay (spec: the event preceding the first event sent, or
// `earliest` when there is none), and what is replayed, through the hub's
// subscribe handler. newTransport returns a transport on a fresh stream.
func runReplayResponseHeaderCases(t *testing.T, newTransport func(t *testing.T) *RedisTransport) {
	t.Helper()

	cases := []struct {
		name string
		// trim is the MAXLEN the stream is trimmed to before subscribing; 0
		// leaves it untouched.
		trim       int64
		request    func(s replayHeaderStream) string
		wantHeader func(s replayHeaderStream) string
		wantReplay []string
	}{
		{
			name:       "found with newer events",
			request:    func(s replayHeaderStream) string { return s.a },
			wantHeader: func(s replayHeaderStream) string { return s.a },
			wantReplay: []string{"b"},
		},
		{
			name:       "found with newer events on other topics only",
			request:    func(s replayHeaderStream) string { return s.b },
			wantHeader: func(s replayHeaderStream) string { return s.b },
			wantReplay: nil,
		},
		{
			name:       "found with nothing newer",
			request:    func(s replayHeaderStream) string { return s.c },
			wantHeader: func(s replayHeaderStream) string { return s.c },
			wantReplay: nil,
		},
		{
			name:       "earliest",
			request:    func(replayHeaderStream) string { return mercure.EarliestLastEventID },
			wantHeader: func(replayHeaderStream) string { return mercure.EarliestLastEventID },
			wantReplay: []string{"a", "b"},
		},
		{
			name:       "unknown",
			request:    func(replayHeaderStream) string { return replayHeaderUnknownID },
			wantHeader: func(replayHeaderStream) string { return mercure.EarliestLastEventID },
			wantReplay: []string{"a", "b"},
		},
		{
			// The future-ID guard skips both passes: the ID names no event.
			name:       "future-dated",
			request:    func(replayHeaderStream) string { return futureUUIDv7(24 * time.Hour) },
			wantHeader: func(replayHeaderStream) string { return mercure.EarliestLastEventID },
			wantReplay: nil,
		},
		{
			name:       "trimmed",
			trim:       2,
			request:    func(s replayHeaderStream) string { return s.a },
			wantHeader: func(replayHeaderStream) string { return mercure.EarliestLastEventID },
			wantReplay: []string{"b"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			transport := newTransport(t)
			stream := publishReplayHeaderStream(t, transport)

			if tc.trim > 0 {
				require.NoError(t, transport.client.XTrimMaxLen(t.Context(), transport.key(""), tc.trim).Err())
			}

			header, replayed := subscribeAndReadReplay(t, transport, tc.request(stream))
			assert.Equal(t, tc.wantHeader(stream), header, "Mercure-Last-Event-ID")
			assert.Equal(t, tc.wantReplay, replayed, "replayed updates")
		})
	}
}

// futureUUIDv7 returns a UUIDv7 URN dated ahead from now.
func futureUUIDv7(ahead time.Duration) string {
	ms := uint64(time.Now().Add(ahead).UnixMilli())

	return fmt.Sprintf("urn:uuid:%08x-%04x-7000-8000-000000000000", (ms>>16)&0xFFFFFFFF, ms&0xFFFF)
}

// publishReplayHeaderStream publishes a, b (subscribed topic) and c (another
// topic), waits until the listener consumed c, and returns their IDs.
func publishReplayHeaderStream(t *testing.T, transport *RedisTransport) replayHeaderStream {
	t.Helper()

	publish := func(topic, data string) string {
		u := &mercure.Update{Topics: []string{topic}, Data: data}
		require.NoError(t, transport.Dispatch(t.Context(), u))

		return u.ID
	}

	s := replayHeaderStream{
		a: publish(replayHeaderTopic, "a"),
		b: publish(replayHeaderTopic, "b"),
		c: publish(replayHeaderOtherTopic, "c"),
	}

	tail, err := transport.client.XRevRangeN(t.Context(), transport.key(""), "+", "-", 1).Result()
	require.NoError(t, err)
	require.Len(t, tail, 1)

	// AddSubscriber replays up to the listener's cursor, so the listener must
	// have consumed the whole history first.
	require.Eventually(t, func() bool { return *transport.lastDispatchedStreamID.Load() == tail[0].ID },
		5*time.Second, 10*time.Millisecond, "the listener must consume the history")

	return s
}

// subscribeAndReadReplay subscribes to replayHeaderTopic from lastEventID and
// returns the Mercure-Last-Event-ID answer and the data of the updates sent
// before a live update published once the headers arrived.
func subscribeAndReadReplay(t *testing.T, transport *RedisTransport, lastEventID string) (string, []string) {
	t.Helper()

	hub, err := mercure.NewHub(
		t.Context(),
		mercure.WithTransport(transport),
		mercure.WithAnonymous(),
		mercure.WithLogger(testLogger()),
	)
	require.NoError(t, err)

	// srv.Close waits for the handler, so it is registered only once the
	// headers arrived and cancel is deferred, which ends the handler first.
	srv := httptest.NewServer(http.HandlerFunc(hub.SubscribeHandler))

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	query := url.Values{}
	query.Set("match", replayHeaderTopic)
	query.Set("last_event_id", lastEventID)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/.well-known/mercure?"+query.Encode(), nil)
	require.NoError(t, err)

	resp, err := srv.Client().Do(req)
	require.NoError(t, err)
	t.Cleanup(srv.Close)

	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	require.NoError(t, transport.Dispatch(t.Context(), &mercure.Update{
		Topics: []string{replayHeaderTopic},
		Data:   replayHeaderLiveData,
	}))

	var replayed []string

	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		data, ok := strings.CutPrefix(scanner.Text(), "data: ")
		if !ok {
			continue
		}

		if data == replayHeaderLiveData {
			return resp.Header.Get("Mercure-Last-Event-ID"), replayed
		}

		replayed = append(replayed, data)
	}

	require.FailNow(t, "the live update never arrived", "scanner error: %v; replayed so far: %v", scanner.Err(), replayed)

	return "", nil
}

// TestHistoryReplayResponseLastEventID runs the replay-header cases on
// miniredis; TestReal_HistoryReplayResponseLastEventID runs them on a real
// server.
func TestHistoryReplayResponseLastEventID(t *testing.T) {
	t.Parallel()

	runReplayResponseHeaderCases(t, func(t *testing.T) *RedisTransport {
		t.Helper()

		transport, _ := newTestTransport(t)

		return transport
	})
}
