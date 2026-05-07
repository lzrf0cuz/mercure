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
	// AdmissionRate — the accept-rate budget (subscriber_rate_limit) is exhausted.
	AdmissionRate AdmissionReason = iota
	// AdmissionCapacity — the concurrent-count ceiling (subscriber_max_count) is reached.
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
// shed. SubscriptionHandler maps it to 429 with a Retry-After header (RetryAfter
// rounded up to integer seconds); any other registration error keeps its 503.
// The retry delay travels in the error rather than as a separate return value,
// so the interface returns (context, release, error) rather than threading the
// delay through a separate return value.
//
// Exported so operators with errors.As-aware middleware can branch on the
// admission failure mode (and its reason) without substring-matching.
type AdmissionError struct {
	Reason     AdmissionReason
	RetryAfter time.Duration
}

func (e *AdmissionError) Error() string {
	return fmt.Sprintf("subscriber admission rejected (%s); retry after %s", e.Reason, e.RetryAfter)
}

// Admitter is an OPTIONAL interface a Transport may implement to shed new
// subscriber admissions at the front door — before authentication and
// allocation — when the hub is over its accept-rate or concurrent-count ceiling.
// A transport that does not implement Admitter admits everything, preserving
// the default (no-admission-control) behavior.
//
// TryAdmit reserves an admission slot. On success it returns a derived context,
// a release closure, and a nil error; on rejection it returns the original
// context, a nil release, and an *AdmissionError carrying the reason and a
// retry delay.
//
// The caller MUST thread admittedCtx into the downstream registration call and
// MUST call release exactly once when the connection ends (release is
// idempotent, so a defensive double-call is a no-op). admittedCtx may carry a
// transport-private marker so the transport's own registration path does not
// re-apply a rate limit it already charged here (avoiding a double-charge); the
// caller treats it as opaque.
type Admitter interface {
	TryAdmit(ctx context.Context) (admittedCtx context.Context, release func(), err error)
}
