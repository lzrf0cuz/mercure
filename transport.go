package mercure

import (
	"context"
	"errors"
	"fmt"
)

// EarliestLastEventID is the reserved value representing the earliest available event id.
const EarliestLastEventID = "earliest"

// Transport provides methods to dispatch and persist updates.
type Transport interface {
	// Dispatch dispatches an update to all subscribers.
	//
	// It trusts u to be well-formed. Hub.Publish calls Validate before it
	// reaches here, so the bundled hub and PublishHandler are covered. A caller
	// that builds u from untrusted publisher input and dispatches it directly,
	// bypassing Hub.Publish, MUST call Update.Validate first — the full check,
	// including reserved-topic rejection. A Transport that instead reads
	// hub-internal updates from an out-of-process backend MUST reject a u whose
	// id or type fails Update.ValidateSSEFields (a CR/LF/NUL there forges SSE
	// frame boundaries into subscribers' streams, CWE-93); it uses that narrow
	// check, not Validate, because those updates ride reserved topics that
	// Validate rejects by design but that dispatch legitimately (subscription
	// events).
	Dispatch(ctx context.Context, u *Update) error

	// AddSubscriber adds a new subscriber to the transport.
	AddSubscriber(ctx context.Context, s *LocalSubscriber) error

	// RemoveSubscriber removes a subscriber from the transport.
	RemoveSubscriber(ctx context.Context, s *LocalSubscriber) error

	// Close closes the Transport.
	Close(ctx context.Context) error
}

// TransportSubscribers provides a method to retrieve the list of active subscribers.
type TransportSubscribers interface {
	// GetSubscribers gets the last event ID and the list of active subscribers at this time.
	GetSubscribers(ctx context.Context) (string, []*Subscriber, error)
}

// TransportTopicSelectorStore provides a method to pass the TopicSelectorStore to the transport.
type TransportTopicSelectorStore interface {
	SetTopicSelectorStore(store *TopicSelectorStore)
}

// TransportHealthChecker may be implemented by transports that support health checking.
// Transports that do not implement this interface are assumed to always be healthy.
type TransportHealthChecker interface {
	// Ready reports whether the transport can currently serve traffic.
	// Returns nil if healthy, or an error describing the problem.
	// This is typically used for readiness probes (e.g. Kubernetes).
	Ready(ctx context.Context) error

	// Live reports whether the transport is fundamentally operational.
	// Returns nil if alive, or an error if the transport has been unhealthy
	// for an extended period and should be restarted.
	// This is typically used for liveness probes (e.g. Kubernetes).
	Live(ctx context.Context) error
}

// TransportCodec provides a method to pass the Codec to the transport.
// Transports implementing this interface will receive the hub-configured codec
// automatically during hub initialization.
//
// CONTRACT: SetCodec is invoked exactly once at hub initialization, before
// any Dispatch/AddSubscriber/RemoveSubscriber call. Implementations are NOT
// required to be safe for concurrent SetCodec calls; callers other than the
// hub initializer must not invoke it. Implementations that store the codec
// for use by concurrent Dispatch/Unmarshal goroutines must publish the codec
// safely (e.g. via a sync.Mutex or sync/atomic) so that the happens-before
// relationship between SetCodec and the first read is preserved.
type TransportCodec interface {
	SetCodec(codec Codec)
}

// ErrClosedTransport is returned by the Transport's Dispatch and AddSubscriber methods after a call to Close.
var ErrClosedTransport = errors.New("hub: read/write on closed Transport")

// TransportError is returned when the Transport's DSN is invalid.
type TransportError struct {
	dsn string
	msg string
	err error
}

func (e *TransportError) Error() string {
	if e.msg == "" {
		if e.err == nil {
			return fmt.Sprintf("%q: invalid transport", e.dsn)
		}

		return fmt.Sprintf("%q: invalid transport: %s", e.dsn, e.err)
	}

	if e.err == nil {
		return fmt.Sprintf("%q: invalid transport: %s", e.dsn, e.msg)
	}

	return fmt.Sprintf("%q: %s: invalid transport: %s", e.dsn, e.msg, e.err)
}

func (e *TransportError) Unwrap() error {
	return e.err
}

func getSubscribers(sl *SubscriberList) (subscribers []*Subscriber) {
	sl.Walk(0, func(s *LocalSubscriber) bool {
		subscribers = append(subscribers, &s.Subscriber)

		return true
	})

	return subscribers
}
