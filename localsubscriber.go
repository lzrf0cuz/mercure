package mercure

import (
	"context"
	"log/slog"
	"net/url"
	"sync"
	"sync/atomic"

	"github.com/gofrs/uuid/v5"
)

// LocalSubscriber represents a client subscribed to a list of topics on the current hub.
type LocalSubscriber struct {
	Subscriber

	disconnected        atomic.Uint32
	out                 chan *Update
	mutex               sync.Mutex
	responseLastEventID chan string
	ready               atomic.Uint32
	liveQueue           []*Update
	// outBufferLength caps both the out channel and the pre-ready liveQueue. A
	// large fleet wants this small: ~8 KB × this × connections of buffer is
	// committed on connect, an OOM vector during a cold-start herd.
	outBufferLength int
}

const defaultOutBufferLength = 1000

// MinSubscriberOutBuffer floors the configurable out buffer: below this the
// stream drops live updates too eagerly. WithSubscriberOutBuffer and the
// Caddyfile parser reject a positive value under this floor; the withOutBuffer
// guard below is then a defense-in-depth invariant that also maps the
// zero-value (an unconfigured hub) to the default.
const MinSubscriberOutBuffer = 16

// localSubscriberOption configures a LocalSubscriber at construction. Unexported
// so the out-buffer size stays a hub-internal knob (set via the Hub's
// WithSubscriberOutBuffer); external NewLocalSubscriber callers keep the default.
type localSubscriberOption func(*localSubscriberConfig)

type localSubscriberConfig struct {
	outBufferLength int
}

func withOutBuffer(n int) localSubscriberOption {
	return func(c *localSubscriberConfig) {
		if n >= MinSubscriberOutBuffer {
			c.outBufferLength = n
		}
	}
}

// NewLocalSubscriber creates a new subscriber.
func NewLocalSubscriber(lastEventID string, logger *slog.Logger, topicSelectorStore *TopicSelectorStore, opts ...localSubscriberOption) *LocalSubscriber {
	cfg := localSubscriberConfig{outBufferLength: defaultOutBufferLength}
	for _, o := range opts {
		o(&cfg)
	}

	id := "urn:uuid:" + uuid.Must(uuid.NewV4()).String()
	s := &LocalSubscriber{
		Subscriber:          *NewSubscriber(logger, topicSelectorStore),
		responseLastEventID: make(chan string, 1),
		out:                 make(chan *Update, cfg.outBufferLength),
		outBufferLength:     cfg.outBufferLength,
	}

	s.ID = id
	s.EscapedID = url.QueryEscape(id)
	s.RequestLastEventID = lastEventID

	return s
}

// Dispatch an update to the subscriber.
// Security checks must (topics matching) be done before calling Dispatch,
// for instance by calling Match.
func (s *LocalSubscriber) Dispatch(ctx context.Context, u *Update, fromHistory bool) bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.disconnected.Load() > 0 {
		return false
	}

	if !fromHistory && s.ready.Load() < 1 {
		// Bound the pre-ready queue. If live traffic outruns history replay by
		// more than the out buffer could ever drain on Ready(), shed now (the
		// client reconnects and replays from a later point) rather than letting
		// liveQueue grow unbounded — under a reconnect storm that retains
		// replaying_subs × publish_rate × replay_latency pointers and OOMs.
		if len(s.liveQueue) >= s.outBufferLength {
			s.handleFullChan(ctx)

			return false
		}

		s.liveQueue = append(s.liveQueue, u)

		return true
	}

	select {
	case s.out <- u:
		return true
	default:
		s.handleFullChan(ctx)

		return false
	}
}

// Ready flips the ready flag to true and flushes queued live updates returning number of events flushed.
func (s *LocalSubscriber) Ready(ctx context.Context) (n int) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.disconnected.Load() > 0 || s.ready.Load() > 0 {
		return 0
	}

	for _, u := range s.liveQueue {
		select {
		case s.out <- u:
			n++
		default:
			s.ready.Store(1)
			s.handleFullChan(ctx)
			s.liveQueue = nil

			return n
		}
	}

	s.ready.Store(1)
	s.liveQueue = nil

	return n
}

// Receive returns a chan when incoming updates are dispatched.
func (s *LocalSubscriber) Receive() <-chan *Update {
	return s.out
}

// HistoryDispatched must be called when all messages coming from the history have been dispatched.
func (s *LocalSubscriber) HistoryDispatched(responseLastEventID string) {
	s.responseLastEventID <- responseLastEventID
}

// Disconnect disconnects the subscriber.
func (s *LocalSubscriber) Disconnect() {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.doDisconnect()
}

// handleFullChan disconnects the subscriber when the out channel is full.
func (s *LocalSubscriber) handleFullChan(ctx context.Context) {
	s.doDisconnect()

	// Attribute the drop to a specific client. The transport's
	// subscribers_lost{reason="backpressure"} metric tells operators it
	// happened; this log lets them identify which subscriber/topics so
	// the offending client can be investigated.
	if s.logger.Enabled(ctx, slog.LevelWarn) {
		s.logger.LogAttrs(
			ctx, slog.LevelWarn,
			"Subscriber disconnected: unable to receive updates fast enough",
			slog.Any("subscriber", &s.Subscriber),
		)
	}
}

func (s *LocalSubscriber) doDisconnect() {
	if s.disconnected.Load() > 0 {
		return // already disconnected
	}

	s.disconnected.Store(1)
	close(s.out)
}
