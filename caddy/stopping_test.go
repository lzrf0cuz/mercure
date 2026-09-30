package caddy

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"testing/synctest"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"github.com/dunglas/mercure"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// exitAt is a time before any subscriber's write deadline, for the 30s
// write_timeout these tests use: see keepsDeadline in TestHandleStopping.
const exitAt = 10 * time.Second

// resetStopState clears the process-wide stop state the tests change.
func resetStopState() {
	hubsMu.Lock()
	defer hubsMu.Unlock()

	exitState.exitedAt = time.Time{}

	clear(exitState.retiredHubs)
}

func exitSignalFired() bool {
	hubsMu.Lock()
	defer hubsMu.Unlock()

	return !exitState.exitedAt.IsZero()
}

func isRetired(m *Mercure) bool {
	hubsMu.Lock()
	defer hubsMu.Unlock()

	_, ok := exitState.retiredHubs[m]

	return ok
}

func retiredHubCount() int {
	hubsMu.Lock()
	defer hubsMu.Unlock()

	return len(exitState.retiredHubs)
}

func inFlight(m *Mercure) int {
	m.requestsMu.Lock()
	defer m.requestsMu.Unlock()

	return m.requests
}

func isDone(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

func duration(d time.Duration) *caddy.Duration {
	c := caddy.Duration(d)

	return &c
}

// retireWithStuckRequest retires a module serving a request that never ends by
// itself, as a config reload does.
func retireWithStuckRequest(t *testing.T, drainTimeout time.Duration) *Mercure {
	t.Helper()

	m, _ := newStoppingTestModule(t, duration(30*time.Second), duration(drainTimeout))
	m.requestStarted()
	m.handleStopping(false)
	require.True(t, isRetired(m))

	return m
}

// runExitWait runs waitForRetiredHubs in the background. The returned channel
// receives the time it returned.
func runExitWait(t *testing.T) <-chan time.Time {
	t.Helper()

	returned := make(chan time.Time, 1)

	go func() {
		assert.NoError(t, waitForRetiredHubs(t.Context()))

		returned <- time.Now()
	}()

	return returned
}

// deadlineRecorder is a ResponseWriter that SubscribeHandler accepts: it
// flushes and takes a write deadline.
type deadlineRecorder struct {
	*httptest.ResponseRecorder
}

func (deadlineRecorder) SetWriteDeadline(time.Time) error {
	return nil
}

// newStoppingTestModule builds a module holding a hub configured from its
// write_timeout and drain_timeout, as Provision does. It returns the context
// that the module's cancel ends, which is the hub's context.
func newStoppingTestModule(t *testing.T, writeTimeout, drainTimeout *caddy.Duration) (*Mercure, context.Context) {
	t.Helper()

	tms, err := mercure.NewTopicMatcherStore(0)
	require.NoError(t, err)

	opts := []mercure.Option{
		mercure.WithAnonymous(),
		mercure.WithTopicMatcherStore(tms),
		mercure.WithDispatchTimeout(0),
		mercure.WithLogger(slog.New(slog.DiscardHandler)),
	}

	if writeTimeout != nil {
		opts = append(opts, mercure.WithWriteTimeout(time.Duration(*writeTimeout)))
	}

	if drainTimeout != nil {
		opts = append(opts, mercure.WithDrainTimeout(time.Duration(*drainTimeout)))
	}

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	h, err := mercure.NewHub(ctx, opts...)
	require.NoError(t, err)

	return &Mercure{WriteTimeout: writeTimeout, DrainTimeout: drainTimeout, hub: h, cancel: cancel}, ctx
}

// subscribe opens a subscription through m.ServeHTTP. The returned channel
// receives the time the request ended; the returned function ends it from the
// client side.
func subscribe(t *testing.T, m *Mercure) (<-chan time.Time, context.CancelFunc) {
	t.Helper()

	// The placeholders ServeHTTP reads the request's origin from.
	repl := caddy.NewReplacer()
	repl.Set("http.request.scheme", "https")
	repl.Set("http.request.hostport", "example.com")

	ctx, cancel := context.WithCancel(context.WithValue(t.Context(), caddy.ReplacerCtxKey, repl))
	t.Cleanup(cancel)

	req := httptest.NewRequestWithContext(ctx, http.MethodGet, defaultHubURL+"?match=https://example.com/books/1", nil)
	ended := make(chan time.Time, 1)

	go func() {
		assert.NoError(t, m.ServeHTTP(deadlineRecorder{httptest.NewRecorder()}, req, nil))

		ended <- time.Now()
	}()

	synctest.Wait()

	return ended, cancel
}

// TestHandleStopping drives the "stopping" handler, for an exit and for a
// reload, on a module serving one subscriber, and checks whether it fires the
// exit signal, cancels the hub, retires the module, and when the subscriber
// closes.
func TestHandleStopping(t *testing.T) {
	const (
		writeTimeout = 30 * time.Second
		drainTimeout = 4 * time.Second
		// When the "stopping" event fires, after the subscriber connected.
		stopAt = time.Second
	)

	// The subscriber's write deadline, randomized within [80%, 100%] of write_timeout.
	keepsDeadline := func(wt time.Duration) [2]time.Duration { return [2]time.Duration{wt * 8 / 10, wt} }
	// A drain closes the subscriber at a random time in (0, drain_timeout] after the event.
	drained := [2]time.Duration{stopAt + time.Nanosecond, stopAt + drainTimeout}
	atOnce := [2]time.Duration{stopAt, stopAt}

	for _, tc := range []struct {
		name         string
		exiting      bool
		writeTimeout *caddy.Duration
		drainTimeout *caddy.Duration
		cancels      bool
		retires      bool
		// The subscriber ends within [closes[0], closes[1]] after it connected.
		closes [2]time.Duration
	}{
		{
			name:    "exit, drain set: drains",
			exiting: true, writeTimeout: duration(writeTimeout), drainTimeout: duration(drainTimeout),
			closes: drained,
		},
		{
			name:    "exit, drain set, write_timeout 0: drains, does not cancel",
			exiting: true, writeTimeout: duration(0), drainTimeout: duration(drainTimeout),
			closes: drained,
		},
		{
			name:    "exit, drain unset: cancels, the subscriber keeps its write deadline",
			exiting: true, writeTimeout: duration(writeTimeout),
			cancels: true, closes: keepsDeadline(writeTimeout),
		},
		{
			name:    "exit, drain 0: cancels, the subscriber keeps its write deadline",
			exiting: true, writeTimeout: duration(writeTimeout), drainTimeout: duration(0),
			cancels: true, closes: keepsDeadline(writeTimeout),
		},
		{
			name:    "exit, drain unset, write_timeout 0: cancels, closing the subscriber",
			exiting: true, writeTimeout: duration(0),
			cancels: true, closes: atOnce,
		},
		{
			name:         "reload, drain set: cancels and retires, does not drain",
			writeTimeout: duration(writeTimeout), drainTimeout: duration(drainTimeout),
			cancels: true, retires: true, closes: keepsDeadline(writeTimeout),
		},
		{
			name:         "reload, drain set, default write_timeout: cancels and retires, does not drain",
			drainTimeout: duration(drainTimeout),
			cancels:      true, retires: true, closes: keepsDeadline(mercure.DefaultWriteTimeout),
		},
		{
			name:         "reload, drain set, write_timeout 0: cancels, closing the subscriber",
			writeTimeout: duration(0), drainTimeout: duration(drainTimeout),
			cancels: true, closes: atOnce,
		},
		{
			name:         "reload, drain unset: cancels",
			writeTimeout: duration(writeTimeout),
			cancels:      true, closes: keepsDeadline(writeTimeout),
		},
		{
			name:         "reload, drain 0: cancels",
			writeTimeout: duration(writeTimeout), drainTimeout: duration(0),
			cancels: true, closes: keepsDeadline(writeTimeout),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Cleanup(resetStopState)

			synctest.Test(t, func(t *testing.T) {
				m, hubCtx := newStoppingTestModule(t, tc.writeTimeout, tc.drainTimeout)

				start := time.Now()
				ended, _ := subscribe(t, m)

				time.Sleep(stopAt)
				m.handleStopping(tc.exiting)

				// require: a subscriber left open by a wrong action would keep the test running.
				require.Equal(t, tc.exiting, exitSignalFired(), "only an exit fires the exit signal")
				require.Equal(t, tc.cancels, isDone(hubCtx), "hub context cancelled only when expected")
				require.Equal(t, tc.retires, isRetired(m), "module retired only when expected")

				got := (<-ended).Sub(start)
				assert.GreaterOrEqual(t, got, tc.closes[0], "subscriber closed too early")
				assert.LessOrEqual(t, got, tc.closes[1], "subscriber closed too late")

				assert.False(t, isRetired(m), "a hub with no request in flight must not stay retired")
			})
		})
	}
}

// TestRetiredHubDrainedOnExit checks that a subscriber connected before a
// reload is drained when the process later exits, and that the exit hook
// returns only once it has closed.
func TestRetiredHubDrainedOnExit(t *testing.T) {
	t.Cleanup(resetStopState)

	const (
		drainTimeout = 4 * time.Second
		reloadAt     = time.Second
	)

	synctest.Test(t, func(t *testing.T) {
		m, _ := newStoppingTestModule(t, duration(30*time.Second), duration(drainTimeout))

		start := time.Now()
		ended, _ := subscribe(t, m)

		time.Sleep(reloadAt)
		m.handleStopping(false)
		require.True(t, isRetired(m))

		time.Sleep(exitAt - reloadAt)

		// The exit fires "stopping" only for the configuration loaded by
		// the reload, then runs the stop hook.
		successor, _ := newStoppingTestModule(t, duration(0), nil)
		successor.handleStopping(true)

		hookReturned := runExitWait(t)

		closed := <-ended
		assert.Greater(t, closed.Sub(start), exitAt, "the subscriber closed before the exit")
		assert.LessOrEqual(t, closed.Sub(start), exitAt+drainTimeout, "the subscriber was not drained")
		assert.Equal(t, closed, <-hookReturned, "the exit hook must return when the subscriber closes")
		assert.False(t, isRetired(m))
	})
}

// TestExitWaitIsBounded checks that the exit hook gives up on requests that
// never end, after each retired hub's own drain_timeout or when its context
// ends.
func TestExitWaitIsBounded(t *testing.T) {
	t.Run("drain_timeout", func(t *testing.T) {
		t.Cleanup(resetStopState)

		synctest.Test(t, func(t *testing.T) {
			short := retireWithStuckRequest(t, 2*time.Second)
			long := retireWithStuckRequest(t, 6*time.Second)

			time.Sleep(time.Second)
			fireExitSignal()
			time.Sleep(time.Second)

			start := time.Now()

			require.NoError(t, waitForRetiredHubs(t.Context()))
			assert.Equal(t, 5*time.Second, time.Since(start), "the wait must end with the longest drain_timeout, counted from the exit signal")

			short.requestEnded()
			long.requestEnded()
			assert.Zero(t, retiredHubCount())
		})
	})

	t.Run("context", func(t *testing.T) {
		t.Cleanup(resetStopState)

		synctest.Test(t, func(t *testing.T) {
			m := retireWithStuckRequest(t, 6*time.Second)

			fireExitSignal()

			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()

			start := time.Now()

			require.ErrorIs(t, waitForRetiredHubs(ctx), context.DeadlineExceeded)
			assert.Equal(t, time.Second, time.Since(start), "the wait must end with its context")

			m.requestEnded()
		})
	})
}

// TestRetiredHubIsDropped checks that a retired hub leaves retiredHubs once its
// last request ends, and that the requests its server accepts after the reload
// are counted, and drained once the process exits.
func TestRetiredHubIsDropped(t *testing.T) {
	const drainTimeout = 4 * time.Second

	t.Run("the last request ends", func(t *testing.T) {
		t.Cleanup(resetStopState)

		synctest.Test(t, func(t *testing.T) {
			m, _ := newStoppingTestModule(t, duration(30*time.Second), duration(drainTimeout))

			ended, end := subscribe(t, m)
			m.handleStopping(false)
			require.True(t, isRetired(m))

			// Without the exit signal, the hook returns at once.
			start := time.Now()

			require.NoError(t, waitForRetiredHubs(t.Context()))
			assert.Zero(t, time.Since(start))

			end()
			<-ended
			assert.Zero(t, retiredHubCount(), "a retired hub with no request in flight must be dropped")
		})
	})

	t.Run("requests accepted after the reload", func(t *testing.T) {
		t.Cleanup(resetStopState)

		synctest.Test(t, func(t *testing.T) {
			m, _ := newStoppingTestModule(t, duration(30*time.Second), duration(drainTimeout))

			m.handleStopping(false)
			assert.False(t, isRetired(m), "a hub with no request in flight is not retired")

			ended, end := subscribe(t, m)
			require.True(t, isRetired(m), "a request accepted after the reload must be counted")

			end()
			<-ended
			require.False(t, isRetired(m))

			time.Sleep(time.Second)
			fireExitSignal()

			exit := time.Now()
			ended, _ = subscribe(t, m)
			require.True(t, isRetired(m))

			require.NoError(t, waitForRetiredHubs(t.Context()))

			returned := time.Now()
			closed := <-ended
			assert.LessOrEqual(t, closed.Sub(exit), drainTimeout, "a request accepted after the exit signal must be drained")
			assert.Equal(t, closed, returned, "the exit hook must return when the request ends")
			assert.Zero(t, retiredHubCount())
		})
	})
}

// TestExitWaitInCaddy runs a reload and a stop through Caddy: the real
// "stopping" handler, the stop hook registration and its wait. Caddy cannot
// be made to exit here, so the test fires the exit signal itself.
func TestExitWaitInCaddy(t *testing.T) {
	// Cleanups run last to first: the request ends, Caddy stops, then the state
	// resets. The reset must follow requestEnded, which closes a channel that
	// retiredHubs holds.
	t.Cleanup(resetStopState)
	t.Cleanup(func() { assert.NoError(t, caddy.Stop()) })

	const drainTimeout = 500 * time.Millisecond

	config := fmt.Appendf(nil, `{"admin":{"disabled":true,"config":{"persist":false}},"apps":{"http":{"servers":{"srv0":{"listen":["127.0.0.1:0"],"automatic_https":{"disable":true},"routes":[{"handle":[{"handler":"mercure","anonymous":true,"transport":{"name":"local"},"write_timeout":"30s","drain_timeout":%q,"resource_identifier":%q,"issuers":[{"identifier":%q,"publisher":{"jwt":{"key":"!ChangeMe!","alg":"HS256"}}}]}]}]}}}}}`, drainTimeout.String(), caddyResourceIdentifier, caddyTrustedIssuer)

	require.NoError(t, caddy.Load(config, true))

	app, err := caddy.ActiveContext().App("http")
	require.NoError(t, err)

	httpApp := app.(*caddyhttp.App)
	srv := httpApp.Servers["srv0"]
	m := srv.Routes[0].Handlers[0].(*Mercure)

	// A publish whose body never ends stays in flight until the test closes it.
	body, bodyWriter := io.Pipe()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "http://"+srv.Listeners()[0].Addr().String()+defaultHubURL, body)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Authorization", bearerPrefix+publisherJWT)

	published := make(chan struct{})

	go func() {
		defer close(published)

		if resp, err := http.DefaultClient.Do(req); err == nil {
			_ = resp.Body.Close()
		}
	}()

	// Registered once the request goroutine runs: an early failure still ends
	// the request before the stop and the reset.
	t.Cleanup(func() {
		_ = bodyWriter.Close()

		<-published

		assert.Eventually(t, func() bool { return inFlight(m) == 0 }, 5*time.Second, 10*time.Millisecond)
	})

	require.Eventually(t, func() bool { return inFlight(m) == 1 }, 5*time.Second, 10*time.Millisecond)

	// A forced reload applies the same configuration again.
	require.NoError(t, caddy.Load(config, true))
	assert.False(t, exitSignalFired(), "a reload must not fire the exit signal")
	assert.True(t, isRetired(m), "a reload must retire a hub with a request in flight")

	fireExitSignal()

	require.NoError(t, caddy.Stop())

	stopped := time.Now()

	hubsMu.Lock()
	deadline := exitState.exitedAt.Add(drainTimeout)
	_, registered := exitState.exitWaitApps[httpApp]
	hubsMu.Unlock()

	assert.False(t, stopped.Before(deadline), "the stop must wait for the retired hub's drain_timeout")
	assert.False(t, registered, "Cleanup must forget the configuration's HTTP app")
	assert.True(t, isRetired(m), "the request is still in flight")

	require.NoError(t, bodyWriter.Close())
	<-published

	require.Eventually(t, func() bool { return retiredHubCount() == 0 }, 5*time.Second, 10*time.Millisecond)
}

// TestFireExitSignalDrainsEveryRetiredHub checks that the exit signal drains
// every hub that a chain of reloads retired, not only the first one it finds.
func TestFireExitSignalDrainsEveryRetiredHub(t *testing.T) {
	t.Cleanup(resetStopState)

	const (
		drainTimeout = 4 * time.Second
	)

	synctest.Test(t, func(t *testing.T) {
		start := time.Now()

		// Three modules, each replaced by a reload while it has a subscriber.
		ended := make([]<-chan time.Time, 0, 3)

		for range 3 {
			m, _ := newStoppingTestModule(t, duration(30*time.Second), duration(drainTimeout))

			e, _ := subscribe(t, m)
			ended = append(ended, e)

			m.handleStopping(false)
			require.True(t, isRetired(m))
		}

		require.Equal(t, 3, retiredHubCount())

		time.Sleep(exitAt)
		fireExitSignal()

		for i, e := range ended {
			closed := <-e
			assert.LessOrEqual(t, closed.Sub(start), exitAt+drainTimeout, "the subscriber of retired hub %d was not drained", i)
		}
	})
}

// TestExitWaitCoversEveryRetiredHub checks that the exit hook returns only
// once every retired hub is idle, not at the first one that is.
func TestExitWaitCoversEveryRetiredHub(t *testing.T) {
	t.Cleanup(resetStopState)

	synctest.Test(t, func(t *testing.T) {
		// retiredHubs is a map, so the hub the hook looks at first varies:
		// repeat until a hook that stopped at the first idle hub would show.
		for attempt := range 10 {
			resetStopState()

			hubs := []*Mercure{
				retireWithStuckRequest(t, time.Minute),
				retireWithStuckRequest(t, time.Minute),
				retireWithStuckRequest(t, time.Minute),
			}

			fireExitSignal()

			returned := runExitWait(t)

			for i, m := range hubs {
				time.Sleep(time.Second)
				synctest.Wait()

				select {
				case <-returned:
					require.Failf(t, "the hook returned early", "attempt %d: it returned with %d retired hub(s) still busy", attempt, len(hubs)-i)
				default:
				}

				m.requestEnded()
			}

			assert.NotZero(t, <-returned)
		}
	})
}

// TestExitWaitReturnsWhenSecondRequestEnds checks that a request starting on a
// retired hub, while the exit hook already waits on it, does not detach the
// hook from the hub: the hook returns when the hub's requests have ended.
func TestExitWaitReturnsWhenSecondRequestEnds(t *testing.T) {
	t.Cleanup(resetStopState)

	const drainTimeout = 10 * time.Second

	synctest.Test(t, func(t *testing.T) {
		m := retireWithStuckRequest(t, drainTimeout)

		fireExitSignal()

		start := time.Now()
		returned := runExitWait(t)

		time.Sleep(time.Second)
		m.requestStarted()

		time.Sleep(time.Second)
		m.requestEnded()
		m.requestEnded()

		assert.Equal(t, 2*time.Second, (<-returned).Sub(start), "the hook must return when the hub's requests end, not at the drain_timeout")
		assert.Zero(t, retiredHubCount())
	})
}

// TestExitSignalFiresOnce checks that a second exit signal neither moves
// exitedAt nor extends the wait.
func TestExitSignalFiresOnce(t *testing.T) {
	t.Cleanup(resetStopState)

	const drainTimeout = 4 * time.Second

	synctest.Test(t, func(t *testing.T) {
		m := retireWithStuckRequest(t, drainTimeout)

		start := time.Now()

		fireExitSignal()

		first := func() time.Time {
			hubsMu.Lock()
			defer hubsMu.Unlock()

			return exitState.exitedAt
		}()

		time.Sleep(2 * time.Second)
		fireExitSignal()

		hubsMu.Lock()
		second := exitState.exitedAt
		hubsMu.Unlock()

		assert.Equal(t, first, second, "a second exit signal must not move exitedAt")

		require.NoError(t, waitForRetiredHubs(t.Context()))
		assert.Equal(t, drainTimeout, time.Since(start), "the wait must count from the first exit signal")

		m.requestEnded()
	})
}

// TestExitWaitCountsFromLatestArrival checks that a request reaching a retired
// hub after the exit signal, which the hub drains relative to its arrival, is
// waited for up to drain_timeout counted from that arrival.
func TestExitWaitCountsFromLatestArrival(t *testing.T) {
	const drainTimeout = 4 * time.Second

	t.Run("hub already retired with a request in flight", func(t *testing.T) {
		t.Cleanup(resetStopState)

		synctest.Test(t, func(t *testing.T) {
			m := retireWithStuckRequest(t, drainTimeout)

			fireExitSignal()
			time.Sleep(3 * time.Second)

			m.requestStarted()

			start := time.Now()

			require.NoError(t, waitForRetiredHubs(t.Context()))
			assert.Equal(t, drainTimeout, time.Since(start), "the wait must count from the request's arrival")

			m.requestEnded()
			m.requestEnded()
		})
	})

	t.Run("hub added again after the exit signal", func(t *testing.T) {
		t.Cleanup(resetStopState)

		synctest.Test(t, func(t *testing.T) {
			m := retireWithStuckRequest(t, drainTimeout)

			fireExitSignal()
			m.requestEnded()
			require.False(t, isRetired(m))

			time.Sleep(3 * time.Second)

			m.requestStarted()
			require.True(t, isRetired(m))

			start := time.Now()

			require.NoError(t, waitForRetiredHubs(t.Context()))
			assert.Equal(t, drainTimeout, time.Since(start), "the wait must count from the hub being added again")

			m.requestEnded()
		})
	})
}
