package mercure

import (
	"log/slog"
	"slices"
	"sync"
	"uuid"

	"go.opentelemetry.io/otel/attribute"
)

// Update represents an update to send to subscribers.
//
// An Update must not be mutated once it has been published: if the first
// publish already cached its SSE bytes and the transport fans out that same
// *Update pointer to live subscribers (Local, Bolt's live fan-out), a mutated
// republish is served stale to them, while Bolt persists and replays the new
// fields to history subscribers. Build a fresh Update instead. An Update must
// not be copied by value either: it contains a sync.Once, and a copy made
// after serialization carries the cache.
type Update struct {
	// The Server-Sent Event to send.
	Event

	// The topics' Internationalized Resource Identifier (RFC3987) (will most
	// likely be URLs). The first one is the canonical topic; any others are
	// alternate topics. The update is dispatched to subscribers matching
	// either the canonical topic or any alternate; a private update's
	// audience is the union of the audiences of all its topics.
	Topics []string

	// Private updates can only be dispatched to subscribers authorized to receive them.
	Private bool

	// To print debug information
	Debug bool

	// serializedOnce caches the SSE wire bytes, which are the same for every
	// subscriber receiving this *Update pointer (Local and Bolt's live fan-out).
	// Topics, Private and Debug are not part of the cached bytes. The cache is
	// never invalidated; see the type doc for the immutability contract.
	serializedOnce     sync.Once
	serializedSSEBytes []byte
}

// LogValue returns the slog representation of an Update, with a bounded
// default field set.
//
// The publisher-controlled type and data are logged only when u.Debug is set:
// Update.Validate does not bound the length of type. The hub sets u.Debug in
// debug mode (WithDebug; Caddy sets it at debug log level). topics and id are
// logged as-is; they are length-capped only by Update.Validate on the
// Hub.Publish path.
//
// LogValue is nil-safe: Bolt logs a possibly nil *Update when it cannot
// unmarshal a history entry, and slog would otherwise log "LogValue panicked".
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

	eventBytes []byte
}

// AssignUUID generates a new UUID an assign it to the given update if no ID is already set.
func (u *Update) AssignUUID() {
	if u.ID == "" {
		u.ID = "urn:uuid:" + uuid.NewV7().String()
	}
}

// SpanAttributes returns the OpenTelemetry attributes describing this update.
func (u *Update) SpanAttributes() []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, 3)
	if u.ID != "" {
		attrs = append(attrs, attribute.String("mercure.update.id", u.ID))
	}

	return append(attrs,
		attribute.StringSlice("mercure.topics", u.Topics),
		attribute.Bool("mercure.private", u.Private),
	)
}

func newSerializedUpdate(u *Update) *serializedUpdate {
	u.serializedOnce.Do(func() {
		// Cache the wire bytes once so all subscriber writes share one read-only
		// slice instead of allocating a copy per write. Only the bytes are kept:
		// the intermediate string is garbage once converted. slices.Clip caps the
		// slice so an append can't write into the shared array; io.Writer
		// implementations must not modify or keep it anyway.
		u.serializedSSEBytes = slices.Clip([]byte(u.String()))
	})

	return &serializedUpdate{u, u.serializedSSEBytes}
}
