package mercure

import (
	"fmt"
	"strings"
)

//nolint:gochecknoglobals
var dataReplacer = strings.NewReplacer("\r\n", "\ndata: ", "\r", "\ndata: ", "\n", "\ndata: ")

// Event is the actual Server Sent Event that will be dispatched.
type Event struct {
	// The updates' data, encoded in the sever-sent event format: every line starts with the string "data: "
	// https://www.w3.org/TR/eventsource/#dispatchMessage
	Data string

	// The globally unique identifier corresponding to update
	ID string

	// The event type, will be attached to the "event" field
	Type string

	// The reconnection time
	Retry uint64
}

// String serializes the event in a "text/event-stream" representation.
//
// Type and ID are written verbatim as SSE field values; Data is line-escaped by
// dataReplacer, Retry is numeric (cannot carry control chars), and Topics are not
// part of the frame. Any STRING field added here that is written verbatim must
// also be covered by Update.ValidateSSEFields, the receive-side injection guard
// that trusts this method's field list.
func (e *Event) String() string {
	var b strings.Builder

	if e.Type != "" {
		_, _ = fmt.Fprintf(&b, "event: %s\n", e.Type)
	}

	if e.Retry != 0 {
		_, _ = fmt.Fprintf(&b, "retry: %d\n", e.Retry)
	}

	_, _ = fmt.Fprintf(&b, "id: %s\ndata: %s\n\n", e.ID, dataReplacer.Replace(e.Data))

	return b.String()
}
