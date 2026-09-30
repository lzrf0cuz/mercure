package mercure

import (
	"context"
	"fmt"
	"time"
)

// AdmissionReason categorises why TryAdmit refused a subscriber. It is the only
// label on the admission-rejection metric, so it stays a small closed set (low
// cardinality).
type AdmissionReason int

const (
	// AdmissionRate means the accept-rate budget (subscriber_rate_limit) is exhausted.
	AdmissionRate AdmissionReason = iota
	// AdmissionCapacity means the concurrent-count ceiling (subscriber_max_count) is reached.
	AdmissionCapacity
)

func (r AdmissionReason) String() string {
	switch r {
	case AdmissionRate:
		return "rate"
	case AdmissionCapacity:
		return "capacity"
	default:
		return "unknown"
	}
}

// AdmissionError is returned by [Admitter.TryAdmit] when a new subscriber is
// shed. SubscribeHandler maps it to 429 with a Retry-After header (RetryAfter
// rounded up to integer seconds); any other TryAdmit error gets a 503, like a
// failed AddSubscriber.
type AdmissionError struct {
	Reason AdmissionReason
	// RetryAfter is a short retry hint (seconds to minutes); an extreme value is
	// unsupported and makes the 429 go out without a Retry-After header.
	RetryAfter time.Duration
}

func (e *AdmissionError) Error() string {
	return fmt.Sprintf("subscriber admission rejected (%s); retry after %s", e.Reason, e.RetryAfter)
}

// Admitter is an optional interface a Transport may implement to shed new
// subscribers before the QUERY request body is read, before authentication and
// before allocation, when the hub is over its accept-rate or concurrent-count
// ceiling. A transport that does not implement Admitter admits everything.
//
// TryAdmit reserves an admission slot. On success it returns a derived context,
// a release closure, and a nil error; on rejection it returns the original
// context, a nil release, and an *AdmissionError carrying the reason and a
// retry delay.
//
// The caller must pass admittedCtx to the registration call (SubscribeHandler
// passes it to AddSubscriber with its cancellation removed and its values kept)
// and must call release once when the connection ends; release is idempotent.
// admittedCtx may carry a transport-private marker so the transport's own
// registration path does not charge the rate limit a second time; the caller
// treats it as opaque.
type Admitter interface {
	TryAdmit(ctx context.Context) (admittedCtx context.Context, release func(), err error)
}
