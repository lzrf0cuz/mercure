package mercure

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"uuid"
)

// LocalSubscriber represents a client subscribed to a list of topics on the current hub.
type LocalSubscriber struct {
	Subscriber

	disconnected        atomic.Bool
	out                 chan *Update
	mutex               sync.Mutex
	responseLastEventID chan string
	ready               atomic.Bool
	liveQueue           []*Update
	// counted is the header value matched by the binding that counts subscribers, with that
	// binding's identity; its value is empty when no binding counts it. Set once, before the
	// subscriber is added to the transport.
	counted bindingValue
}

// outBufferLength is the default capacity of the out channel, which also bounds
// the pre-ready liveQueue (see Dispatch). WithSubscriberOutBuffer overrides it.
const outBufferLength = 1000

// MinSubscriberOutBuffer floors the configurable out buffer: below this the
// stream drops live updates too eagerly. WithSubscriberOutBuffer and the
// Caddyfile parser reject a positive value under this floor.
const MinSubscriberOutBuffer = 16

// localSubscriberOption configures a LocalSubscriber at construction. Unexported
// so the out-buffer size stays a hub-internal knob (set via the Hub's
// WithSubscriberOutBuffer); external NewLocalSubscriber callers keep the default.
type localSubscriberOption func(*localSubscriberConfig)

type localSubscriberConfig struct {
	outBufferLength int
}

// withOutBuffer sets the out channel capacity; 0, from a hub without
// WithSubscriberOutBuffer, keeps the default.
func withOutBuffer(n int) localSubscriberOption {
	return func(c *localSubscriberConfig) {
		if n > 0 {
			c.outBufferLength = n
		}
	}
}

// NewLocalSubscriber creates a new subscriber.
func NewLocalSubscriber(lastEventID string, logger *slog.Logger, topicMatcherStore *TopicMatcherStore, opts ...localSubscriberOption) *LocalSubscriber {
	cfg := localSubscriberConfig{outBufferLength: outBufferLength}
	for _, o := range opts {
		o(&cfg)
	}

	id := "urn:uuid:" + uuid.NewV4().String()
	s := &LocalSubscriber{
		Subscriber:          *NewSubscriber(logger, topicMatcherStore),
		responseLastEventID: make(chan string, 1),
		out:                 make(chan *Update, cfg.outBufferLength),
	}

	s.ID = id
	s.EscapedID = escapeSubscriptionSegment(id)
	s.RequestLastEventID = lastEventID
	s.RequestLastEventIDSet = lastEventID != ""

	return s
}

// Dispatch an update to the subscriber.
// Security checks must (topics matching) be done before calling Dispatch,
// for instance by calling Match.
func (s *LocalSubscriber) Dispatch(ctx context.Context, u *Update, fromHistory bool) bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.disconnected.Load() {
		return false
	}

	if !fromHistory && !s.ready.Load() {
		// Ready would drop a queue that no longer fits in out, so stop buffering it now.
		if len(s.liveQueue) >= cap(s.out)-len(s.out) {
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

	if s.disconnected.Load() || s.ready.Load() {
		return 0
	}

	for _, u := range s.liveQueue {
		select {
		case s.out <- u:
			n++
		default:
			s.ready.Store(true)
			s.handleFullChan(ctx)
			s.liveQueue = nil

			return n
		}
	}

	s.ready.Store(true)
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

	if s.logger.Enabled(ctx, slog.LevelInfo) {
		s.logger.LogAttrs(ctx, slog.LevelInfo, "Subscriber unable to receive updates fast enough")
	}
}

func (s *LocalSubscriber) doDisconnect() {
	if s.disconnected.Load() {
		return // already disconnected
	}

	s.disconnected.Store(true)
	close(s.out)
}
