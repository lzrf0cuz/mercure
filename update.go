package mercure

import (
	"log/slog"
	"sync"

	"github.com/gofrs/uuid/v5"
	"go.opentelemetry.io/otel/attribute"
)

// Update represents an update to send to subscribers.
type Update struct {
	// The Server-Sent Event to send.
	Event

	// The topics' Internationalized Resource Identifier (RFC3987) (will most likely be URLs).
	// The first one is the canonical IRI, while next ones are alternate IRIs.
	Topics []string

	// Private updates can only be dispatched to subscribers authorized to receive them.
	Private bool

	// To print debug information
	Debug bool

	// serializedOnce caches the SSE text representation. Event.String() produces
	// identical output for all subscribers receiving the same update (the event:,
	// id:, data: fields don't change per subscriber). Caching behind sync.Once
	// eliminates N-1 redundant serializations at N subscribers per message.
	//
	// CONTRACT: callers MUST treat *Update as immutable from the moment it is
	// passed to Hub.Dispatch / Transport.Dispatch. Mutating Event fields, Topics,
	// Private, or Debug after the first newSerializedUpdate() call will not
	// invalidate the cache, producing serialization drift across subscribers.
	// Producers that need to mutate should construct a fresh Update.
	// sync.Once fast-path is ~1ns (atomic load).
	serializedOnce     sync.Once
	serializedSSE      string
	serializedSSEBytes []byte
}

// LogValue returns the bounded slog representation of an Update.
//
// Publisher-controlled "type" and "data" are intentionally omitted from
// the default attr set: even with ingress validation bounding type at
// maxUpdateTypeBytes, defense-in-depth keeps publisher-controlled content
// out of default operator log streams. Both are gated behind the
// operator-opt-in Debug flag (matches the original upstream intent for
// data). Operators who want type on the log line should set u.Debug, or
// add type explicitly as a separate bounded attribute at the call site.
//
// id is publisher-controlled too but it's the load-bearing forensic key
// for correlating across log/span/transport — without it the line is
// unactionable. Bounded at maxUpdateIDBytes by Update.Validate (via Hub.Publish).
//
// Nil-safe: bolt.go's malformed-history-entry path may invoke this on a
// partially-decoded *Update, and json.Unmarshal can leave the pointer
// nil after a parse failure. Returning the zero-value GroupValue avoids
// a deref panic on the recovery log line.
func (u *Update) LogValue() slog.Value {
	if u == nil {
		return slog.GroupValue()
	}

	attrs := []slog.Attr{
		slog.String("id", u.ID),
		slog.Uint64("retry", u.Retry),
		slog.Any("topics", u.Topics),
		slog.Bool("private", u.Private),
		slog.Int("data_bytes", len(u.Data)),
	}

	if u.Debug {
		attrs = append(attrs, slog.String("type", u.Type), slog.String("data", u.Data))
	}

	return slog.GroupValue(attrs...)
}

type serializedUpdate struct {
	*Update

	event      string
	eventBytes []byte
}

// AssignUUID generates a new UUID an assign it to the given update if no ID is already set.
func (u *Update) AssignUUID() {
	if u.ID == "" {
		u.ID = "urn:uuid:" + uuid.Must(uuid.NewV7()).String()
	}
}

// SpanAttributes returns the OpenTelemetry attributes describing this update.
func (u *Update) SpanAttributes() []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, 3)
	if u.ID != "" {
		attrs = append(attrs, attribute.String("mercure.update.id", u.ID))
	}

	return append(
		attrs,
		attribute.StringSlice("mercure.topics", u.Topics),
		attribute.Bool("mercure.private", u.Private),
	)
}

func newSerializedUpdate(u *Update) *serializedUpdate {
	u.serializedOnce.Do(func() {
		u.serializedSSE = u.String()
		// Cache the wire bytes once so fan-out to N subscribers shares one
		// read-only slice instead of allocating []byte(serializedSSE) per write
		// (GC pressure ∝ fan-out). Full-cap ([:len:len]) so an accidental append
		// can never mutate the shared backing array; per the io.Writer contract
		// Write must not modify or retain the slice. Keep unexported, never mutated.
		b := []byte(u.serializedSSE)
		u.serializedSSEBytes = b[:len(b):len(b)]
	})

	return &serializedUpdate{u, u.serializedSSE, u.serializedSSEBytes}
}
