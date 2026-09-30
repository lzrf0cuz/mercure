package mercure

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"unicode/utf8"

	wurl "github.com/nlnwa/whatwg-url/url"
	"go.opentelemetry.io/otel/trace"
)

type updateContextKeyType struct{}

var UpdateContextKey updateContextKeyType //nolint:gochecknoglobals

// The reserved-namespace test itself lives in reservedtopic.go.

// Element-count caps prevent DoS amplification when callers fit many
// topics/matchers inside a request whose byte size is within transport
// limits (Caddy request_body, Go MaxHeaderBytes).
const (
	maxClaimMatchers = 1000 // mercure.subscribe / mercure.publish array
	maxPublishTopics = 1000 // "topic" form fields on publish
	// Persisted IDs are echoed in response headers.
	maxEventIDLength = 1024
	// Match cache keys retain the full topic text.
	maxTopicLength = maxPatternLength
	// Subscribe-side matcher count is capped by maxMatcherCount
	// (subscribematchers.go).
)

// Sentinel errors returned by Publish. Callers can branch on them via
// errors.Is.
var (
	ErrReservedTopic     = errors.New(`topic value resolves into the reserved "/.well-known/mercure" namespace`)
	ErrReservedWildcard  = errors.New(`topic value "*" is reserved for the wildcard matcher and cannot be published`)
	ErrInvalidEventID    = errors.New(`"id" field is too long, contains a forbidden control character or invalid UTF-8, starts with "#", or is the reserved value "earliest"`)
	ErrInvalidEventType  = errors.New(`"type" field contains a forbidden control character or invalid UTF-8`)
	ErrReservedEventType = errors.New(`"type" field uses the reserved value "mercure"`)
	ErrInvalidTopic      = errors.New("topic is too long, or contains a forbidden control character or invalid UTF-8")
	ErrTooManyTopics     = errors.New("too many topics in update")
	ErrMissingTopic      = errors.New("update carries no topic")
	ErrInvalidData       = errors.New(`"data" field is not valid UTF-8`)
)

// ErrPublishTimeout is the cause attached to the dispatch context when a
// configured publish_timeout expires. PublishHandler matches it via
// context.Cause so that only a publish_timeout abort maps to 504; a
// context.DeadlineExceeded arriving from any other source (a parent context, a
// transport-internal deadline) keeps its normal mapping.
var ErrPublishTimeout = errors.New("publish dispatch exceeded publish_timeout")

// Validate enforces the publish-side input rules that protect subscribers
// from update forgery and SSE field injection. Hub.Publish calls it, so the
// bundled hub and PublishHandler are already covered.
//
// A caller that builds an Update from untrusted input (e.g. a publisher
// request) and dispatches it through a Transport directly, bypassing
// Hub.Publish, MUST call Validate with the same base URL used for topic matching
// and reject the update on error. The base must be an absolute URL with a host.
// Skipping it lets a CR, LF, or NUL in ID or Type inject arbitrary SSE
// fields into subscribers' streams (CWE-93). Validate also rejects the
// reserved "/.well-known/mercure" topic namespace, so it is meant for
// publisher input, not hub-internal updates such as subscription events.
func (u *Update) Validate(baseURL string) error {
	topics := u.Topics
	if len(topics) == 0 {
		return ErrMissingTopic
	}

	if len(topics) > maxPublishTopics {
		return ErrTooManyTopics
	}

	base, err := wurl.Parse(baseURL)
	if err != nil || base.Hostname() == "" {
		return fmt.Errorf("%w: %q", ErrInvalidBaseURL, baseURL)
	}

	for _, t := range topics {
		// Control characters are forbidden by the protocol; a NUL would also
		// collide with the match cache's topic-list separator.
		if !validProtocolString(t) || len(t) > maxTopicLength {
			return fmt.Errorf("%q: %w", t, ErrInvalidTopic)
		}

		if addressesReservedNamespace(t, base) {
			return fmt.Errorf("%q: %w", t, ErrReservedTopic)
		}

		// "*" is the reserved wildcard matcher pattern, so a topic literally
		// equal to "*" is not addressable by an Exact subscription; reject it
		// at publication rather than dispatch an unreachable update.
		if t == "*" {
			return fmt.Errorf("%q: %w", t, ErrReservedWildcard)
		}
	}

	// The id and type end up on the wire as SSE fields (and the id in the
	// Last-Event-ID header), so reject all control characters and invalid UTF-8,
	// not only CR/LF/NUL — matching the topic and matcher rules. "#" prefixes are
	// reserved for hub-generated fragment IDs and "earliest" for the reserved
	// last-event-id value; accepting either from a publisher would corrupt
	// reconnection cursors.
	if !validProtocolString(u.ID) || len(u.ID) > maxEventIDLength ||
		strings.HasPrefix(u.ID, "#") || u.ID == EarliestLastEventID {
		return ErrInvalidEventID
	}

	if !validProtocolString(u.Type) {
		return ErrInvalidEventType
	}

	// "mercure" is reserved for hub-generated events (subscription events set
	// it as the SSE event name); a publisher using it could inject forged
	// events into a client listening for that event type.
	if u.Type == reservedEventType {
		return ErrReservedEventType
	}

	// The protocol requires field values to be valid UTF-8; ParseForm does not
	// enforce it, so reject invalid data rather than dispatch it.
	if !utf8.ValidString(u.Data) {
		return ErrInvalidData
	}

	return nil
}

// ValidSSEFieldValue reports whether s is safe to write verbatim as an SSE
// field value: valid UTF-8 with no control or format characters (CWE-93).
func ValidSSEFieldValue(s string) bool {
	return validProtocolString(s)
}

// ValidateSSEFields checks the ID and Type written verbatim by Event.String.
// Transports receiving stored updates can use it without rejecting reserved topics.
func (u *Update) ValidateSSEFields() error {
	if !ValidSSEFieldValue(u.ID) {
		return ErrInvalidEventID
	}

	if !ValidSSEFieldValue(u.Type) {
		return ErrInvalidEventType
	}

	return nil
}

// Publish broadcasts the given update to all subscribers.
// The id field of the Update instance can be updated by the underlying Transport.
//
// Do not mutate the update after calling Publish, and do not copy it by value:
// if the first publish already cached the SSE bytes and the transport fans
// out the same pointer to live subscribers (Local, Bolt's live fan-out), a
// mutated re-publish is served stale to them, while Bolt persists and replays
// the new fields to history subscribers; a copy carries the cache along with
// its sync.Once regardless.
func (h *Hub) Publish(ctx context.Context, update *Update) error {
	ctx, span := startSpan(ctx, "mercure.publish", trace.WithSpanKind(trace.SpanKindProducer))
	// Deferred so the ID assigned by the transport via AssignUUID lands on the span.
	defer func() {
		if span.IsRecording() {
			span.SetAttributes(update.SpanAttributes()...)
		}

		span.End()
	}()

	if err := update.Validate(h.topicMatcherStore.base()); err != nil {
		if h.logger.Enabled(ctx, slog.LevelInfo) {
			h.logger.LogAttrs(ctx, slog.LevelInfo, "Rejected invalid update", slog.Any("error", err))
		}

		recordSpanError(span, err)
		h.recordPublishFailure(update, PublishFailureReasonValidation)

		return err
	}

	ctx = context.WithValue(ctx, UpdateContextKey, update)

	if err := h.transport.Dispatch(ctx, update); err != nil {
		// A closed transport is shutting down, not failing: Debug. An update
		// the transport refuses as too large is a client error (413, reason
		// validation): Info, like the other validation rejections.
		level := slog.LevelError

		switch {
		case errors.Is(err, ErrClosedTransport):
			level = slog.LevelDebug
		case errors.Is(err, ErrCodecPayloadTooLarge):
			level = slog.LevelInfo
		}

		if h.logger.Enabled(ctx, level) {
			h.logger.LogAttrs(ctx, level, "Failed to dispatch update", slog.Any("error", err))
		}

		recordSpanError(span, err)
		h.recordPublishFailure(update, publishFailureReasonForDispatch(ctx, err))

		return err //nolint:wrapcheck
	}

	h.metrics.UpdatePublished(update)

	if h.logger.Enabled(ctx, slog.LevelDebug) {
		h.logger.LogAttrs(ctx, slog.LevelDebug, "Update published")
	}

	return nil
}

// recordPublishFailure reports a failed publish to the optional
// PublishFailureReporter extension of h.metrics, when implemented.
func (h *Hub) recordPublishFailure(u *Update, reason PublishFailureReason) {
	if r, ok := h.metrics.(PublishFailureReporter); ok {
		r.UpdatePublishFailed(u, reason)
	}
}

// isPublishTimeout reports whether a Hub.Publish dispatch error is a
// publish_timeout abort: the dispatch context's cause is ErrPublishTimeout and
// the returned error is itself a deadline error (context.DeadlineExceeded) or
// the ErrPublishTimeout sentinel. Anchoring on the cause, not a bare
// DeadlineExceeded, keeps an unrelated context's deadline from being mistaken
// for a publish_timeout. PublishHandler (→ 504) and publishFailureReasonForDispatch
// (→ reason=timeout) share this single predicate so the two cannot drift apart
// and the timeout metric tracks 504s exactly.
func isPublishTimeout(ctx context.Context, err error) bool {
	return errors.Is(context.Cause(ctx), ErrPublishTimeout) &&
		(errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrPublishTimeout))
}

// publishFailureReasonForDispatch classifies a transport.Dispatch error for the
// failure metric: a publish_timeout abort (see isPublishTimeout) is timeout; an
// update the transport refuses as too large (ErrCodecPayloadTooLarge, answered
// 413) is validation, a client error like the other content rejections; every
// other dispatch failure (transport unreachable, XADD error, codec error, or
// any deadline error not tagged with the ErrPublishTimeout cause) falls into
// the transport catch-all.
func publishFailureReasonForDispatch(ctx context.Context, err error) PublishFailureReason {
	if isPublishTimeout(ctx, err) {
		return PublishFailureReasonTimeout
	}

	if errors.Is(err, ErrCodecPayloadTooLarge) {
		return PublishFailureReasonValidation
	}

	return PublishFailureReasonTransport
}

// PublishHandler allows publisher to broadcast updates to all subscribers.
//
//nolint:funlen,gocognit
func (h *Hub) PublishHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := startSpan(r.Context(), "mercure.publish", trace.WithSpanKind(trace.SpanKindProducer))

	var u *Update
	// Deferred so the ID assigned by the transport via AssignUUID lands on the span.
	defer func() {
		if u != nil && span.IsRecording() {
			span.SetAttributes(u.SpanAttributes()...)
		}

		span.End()
	}()

	r = r.WithContext(ctx)

	var claims *claims

	if h.publisherConfigured {
		var err error

		claims, err = h.authorizeAndBind(r, true)
		if err != nil || claims == nil {
			h.writeAuthError(w, r, err)

			if err != nil {
				recordSpanError(span, err)
			}

			return
		}
	}

	h.limitRequestBody(w, r)

	if err := r.ParseForm(); err != nil {
		status := http.StatusBadRequest

		if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
			status = http.StatusRequestEntityTooLarge
		}

		http.Error(w, http.StatusText(status), status)

		return
	}

	topics := r.PostForm["topic"]
	if len(topics) == 0 {
		http.Error(w, `Missing "topic" parameter`, http.StatusBadRequest)
		h.recordPublishFailure(&Update{}, PublishFailureReasonValidation)

		return
	}

	// Reject oversized topic lists before running canDispatch — otherwise
	// an authenticated publisher could force O(topics × matchers)
	// matching work on every request before being rejected by validate.
	// These early rejects never reach Hub.Publish, so they meter the
	// validation failure themselves (as do the missing-topic and invalid-retry
	// rejects). A ParseForm failure is a body read or size problem, not a
	// rejection of the update's content, so it is not metered.
	if len(topics) > maxPublishTopics {
		http.Error(w, ErrTooManyTopics.Error(), http.StatusBadRequest)
		h.recordPublishFailure(&Update{Topics: topics}, PublishFailureReasonValidation)

		return
	}

	// Reject cache-key collisions and oversized keys before authorization populates the cache.
	for _, t := range topics {
		if !validProtocolString(t) || len(t) > maxTopicLength {
			http.Error(w, fmt.Errorf("%q: %w", t, ErrInvalidTopic).Error(), http.StatusBadRequest)
			h.recordPublishFailure(&Update{Topics: topics}, PublishFailureReasonValidation)

			return
		}
	}

	var retry uint64

	if retryString := r.PostForm.Get("retry"); retryString != "" {
		var err error
		if retry, err = strconv.ParseUint(retryString, 10, 64); err != nil {
			http.Error(w, `Invalid "retry" parameter`, http.StatusBadRequest)
			h.recordPublishFailure(&Update{Topics: topics}, PublishFailureReasonValidation)

			return
		}
	}

	private := len(r.PostForm["private"]) != 0
	if claims != nil && !claims.authz.grantsAll(h.topicMatcherStore, actionPublish, topics) { //nolint:nestif
		if private {
			h.writeBearerError(w, r, bearerErrInsufficientScope, http.StatusForbidden)

			return
		}

		infoEnabled := h.logger.Enabled(ctx, slog.LevelInfo)
		if h.isBackwardCompatiblyEnabledWith(7) {
			if infoEnabled {
				h.logger.LogAttrs(ctx, slog.LevelInfo, `Deprecated: posting public updates to topics not granted to the token is deprecated since the version 7 of the protocol, grant the "*" topic to allow publishing on all topics.`)
			}
		} else {
			if infoEnabled {
				h.logger.LogAttrs(ctx, slog.LevelInfo, `Unsupported: posting public updates to topics not granted to the token is not supported anymore, grant the "*" topic to allow publishing on all topics or enable backward compatibility with the version 7 of the protocol.`)
			}

			h.writeBearerError(w, r, bearerErrInsufficientScope, http.StatusForbidden)

			return
		}
	}

	u = &Update{
		Topics:  topics,
		Private: private,
		Debug:   h.debug,
		Event:   Event{r.PostForm.Get("data"), r.PostForm.Get("id"), r.PostForm.Get("type"), retry},
	}

	// Detach from the request context so a publisher disconnecting mid-publish
	// does not abort the dispatch (the update would otherwise be lost). When a
	// publish timeout is configured, bound the now-detached dispatch so a stalled
	// transport cannot block this goroutine indefinitely. WithTimeoutCause tags
	// the expiry with ErrPublishTimeout so the 504 below fires only for this
	// handler's deadline, not a context.DeadlineExceeded from some other source.
	dispatchCtx := context.WithoutCancel(ctx)

	if h.publishTimeout > 0 {
		var cancel context.CancelFunc

		dispatchCtx, cancel = context.WithTimeoutCause(dispatchCtx, h.publishTimeout, ErrPublishTimeout)
		defer cancel()
	}

	// Validation, dispatch, logging and metrics live in Hub.Publish.
	if err := h.Publish(dispatchCtx, u); err != nil {
		switch {
		// A publish_timeout abort shows up as the dispatch context's cause. The
		// update may already be stored, so answer 504 instead of a plain
		// failure. The returned error must be a deadline error too, so a fast
		// unrelated error isn't reported as 504 when the deadline expires just
		// after it.
		case isPublishTimeout(dispatchCtx, err):
			http.Error(w, "publish timed out: the update may already have been published; if you sent an id, retry with the same one so subscribers can detect the duplicate", http.StatusGatewayTimeout)
		// The transport refused the update as too large to store and read back;
		// retrying it cannot succeed, unlike the 500 below.
		case errors.Is(err, ErrCodecPayloadTooLarge):
			http.Error(w, ErrCodecPayloadTooLarge.Error(), http.StatusRequestEntityTooLarge)
		case errors.Is(err, ErrReservedTopic), errors.Is(err, ErrReservedWildcard),
			errors.Is(err, ErrInvalidEventID), errors.Is(err, ErrInvalidEventType),
			errors.Is(err, ErrReservedEventType),
			errors.Is(err, ErrInvalidTopic), errors.Is(err, ErrTooManyTopics),
			errors.Is(err, ErrMissingTopic), errors.Is(err, ErrInvalidData):
			http.Error(w, err.Error(), http.StatusBadRequest)
		default:
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		}

		// Mirror the error onto the handler span too; Hub.Publish's child
		// span already records it, but leaving the parent span as success
		// is misleading.
		recordSpanError(span, err)

		return
	}

	// The body is the update id; the protocol requires this exact media type.
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")

	if _, err := io.WriteString(w, u.ID); err != nil {
		if h.logger.Enabled(ctx, slog.LevelInfo) {
			h.logger.LogAttrs(ctx, slog.LevelInfo, "Failed to write publish response", slog.Any("error", err))
		}

		return
	}
}
