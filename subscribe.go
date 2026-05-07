package mercure

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"strconv"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type subscriberContextKeyType struct{}

var SubscriberContextKey subscriberContextKeyType //nolint:gochecknoglobals

type responseController struct {
	http.ResponseController

	rw http.ResponseWriter

	// disconnectionTime is the JWT expiration date minus hub.dispatchTimeout, or time.Now() plus hub.writeTimeout minus hub.dispatchTimeout
	disconnectionTime time.Time
	// writeDeadline is the JWT expiration date or time.Now() + hub.writeTimeout
	writeDeadline time.Time
	hub           *Hub
	subscriber    *LocalSubscriber
}

func (rc *responseController) setDispatchWriteDeadline(ctx context.Context) bool {
	if rc.hub.dispatchTimeout == 0 {
		return true
	}

	deadline := time.Now().Add(rc.hub.dispatchTimeout)
	if deadline.After(rc.writeDeadline) {
		return true
	}

	if err := rc.SetWriteDeadline(deadline); err != nil {
		// Same level-gate / err-check conflation hazard as h.write below:
		// folding the level check into the if expression silently swallows
		// the deadline-set error at WARN+, leaving the caller to write
		// against a connection whose deadline could not be set. The
		// return-false must always fire on a real error; only the log
		// line is conditional on level.
		if rc.hub.logger.Enabled(ctx, slog.LevelInfo) {
			rc.hub.logger.LogAttrs(ctx, slog.LevelInfo, "Unable to set dispatch write deadline", slog.Any("error", err))
		}

		return false
	}

	return true
}

func (rc *responseController) setDefaultWriteDeadline(ctx context.Context) bool {
	if err := rc.SetWriteDeadline(rc.writeDeadline); err != nil {
		rc.hub.handleWriterError(ctx, err, "Error while setting default write deadline")

		return false
	}

	return true
}

func (rc *responseController) flush(ctx context.Context) bool {
	if err := rc.Flush(); err != nil {
		rc.hub.handleWriterError(ctx, err, "Error while flushing response")

		return false
	}

	return true
}

func (h *Hub) newResponseController(w http.ResponseWriter, s *LocalSubscriber) *responseController {
	wd := h.getWriteDeadline(s)

	return &responseController{
		*http.NewResponseController(w), // nolint:bodyclose
		w,
		wd.Add(-h.dispatchTimeout),
		wd,
		h,
		s,
	}
}

func (h *Hub) getWriteDeadline(s *LocalSubscriber) (deadline time.Time) {
	if h.writeTimeout != 0 {
		deadline = time.Now().Add(randomizeWriteDeadline(h.writeTimeout))
	}

	if s.Claims != nil && s.Claims.ExpiresAt != nil && (deadline.Equal(time.Time{}) || s.Claims.ExpiresAt.Before(deadline)) {
		now := time.Now()
		deadline = now.Add(randomizeWriteDeadline(s.Claims.ExpiresAt.Sub(now)))
	}

	return deadline
}

// SubscribeHandler creates a keep alive connection and sends the events to the subscribers.
//
//nolint:funlen,gocognit
func (h *Hub) SubscribeHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Front-door admission gate (before authorize + allocation). On reject it
	// has already written the response. `defer release()` is declared HERE,
	// before `defer h.shutdown` below, so LIFO frees the admission slot LAST —
	// after the subscriber's transport teardown — closing an over-admit window.
	admitCtx, release, ok := h.admitSubscriber(ctx, w, r)
	if !ok {
		return
	}

	if release != nil {
		defer release()
	}

	ctx = admitCtx

	s, rc := h.registerSubscriber(ctx, w, r)
	if s == nil {
		return
	}

	ctx = context.WithValue(ctx, SubscriberContextKey, &s.Subscriber)

	// Track why the SubscribeHandler loop exits so h.shutdown can label
	// the per-reason disconnect counter. Closure captures by reference,
	// so the value set in the loop below is visible at defer time.
	disconnectReason := DisconnectReasonUnknown

	defer func() {
		h.shutdown(ctx, s, disconnectReason)
	}()

	// setDefaultWriteDeadline is intentionally best-effort: ResponseWriter
	// wrappers that return http.ErrNotSupported (some HTTP/2 push proxies,
	// custom adapters) cannot set deadlines, but the SSE stream itself is
	// still functional. handleWriterError logs the failure inside; we
	// proceed and let the actual write+select loop classify any later
	// disconnect on its own merits.
	rc.setDefaultWriteDeadline(ctx)

	var (
		heartbeatTimer      *time.Timer
		heartbeatTimerC     <-chan time.Time
		disconnectionTimerC <-chan time.Time
	)

	if h.heartbeat != 0 {
		heartbeatTimer = time.NewTimer(h.heartbeat)
		defer heartbeatTimer.Stop()

		heartbeatTimerC = heartbeatTimer.C
	}

	if h.writeTimeout != 0 {
		disconnectionTimer := time.NewTimer(time.Until(rc.disconnectionTime))
		defer disconnectionTimer.Stop()

		disconnectionTimerC = disconnectionTimer.C
	}

	debugLevel := rc.hub.logger.Enabled(ctx, slog.LevelDebug)

	// On hub shutdown (Caddy "stopping" event, pod SIGTERM, …) we prefer to
	// let each subscriber drain on its own per-connection write deadline
	// (derived from writeTimeout, and optionally shortened by JWT expiry)
	// rather than closing everything at once — that spreads the reconnect
	// load at the same pace clients already experience in steady state,
	// instead of producing a synchronized storm on the ingress and the
	// transport. The orchestrator's grace period (k8s
	// terminationGracePeriodSeconds, etc.) remains the hard deadline.
	//
	// When writeTimeout is disabled (0) there is no disconnectionTimerC, so
	// the only way out on shutdown is still h.ctx.Done() — otherwise
	// http.Server.Shutdown would hang indefinitely on active handlers.
	var hubCtxDoneC <-chan struct{}
	if h.writeTimeout == 0 {
		hubCtxDoneC = h.ctx.Done()
	}

	for {
		select {
		case <-hubCtxDoneC:
			if debugLevel {
				rc.hub.logger.LogAttrs(ctx, slog.LevelDebug, "Hub is shutting down, closing connection")
			}

			disconnectReason = DisconnectReasonHubShutdown

			return
		case <-ctx.Done():
			if debugLevel {
				rc.hub.logger.LogAttrs(ctx, slog.LevelDebug, "Connection closed by the client")
			}

			disconnectReason = DisconnectReasonClientClosed

			return
		case <-heartbeatTimerC:
			// Send an SSE comment as a heartbeat, to prevent issues with some proxies
			// and old browsers. Inline []byte for the 2-byte literal; only the
			// KB-scale per-update bytes are worth caching (see newSerializedUpdate).
			if !h.write(ctx, rc, []byte(":\n")) {
				disconnectReason = DisconnectReasonWriteFailed

				return
			}

			heartbeatTimer.Reset(h.heartbeat)
		case <-disconnectionTimerC:
			// Cleanly close the HTTP connection before the write deadline to prevent client-side errors
			disconnectReason = DisconnectReasonWriteTimeout

			return
		case update, ok := <-s.Receive():
			if !ok {
				disconnectReason = DisconnectReasonTransportEnded

				return
			}

			if !h.write(ctx, rc, newSerializedUpdate(update).eventBytes) {
				disconnectReason = DisconnectReasonWriteFailed

				return
			}

			if heartbeatTimer != nil {
				if !heartbeatTimer.Stop() {
					<-heartbeatTimer.C
				}

				heartbeatTimer.Reset(h.heartbeat)
			}

			if debugLevel {
				// Bounded fields rather than slog.Any("update", update): see
				// Hub.Publish for the publisher-controlled Type rationale.
				// Topics intentionally dropped here — they're identical for
				// every subscriber receiving this update, multiplying the
				// log line cost by subscriber-count without forensic value.
				rc.hub.logger.LogAttrs(ctx, slog.LevelDebug, "Update sent",
					slog.String("update_id", update.ID),
					slog.Bool("private", update.Private),
					slog.Int("data_bytes", len(update.Data)))
			}
		}
	}
}

// registerSubscriber initializes the connection.
// admitSubscriber runs the front-door admission gate: a cheap abuse cap (the
// query-topic count, so a topic-flood can't burn the admission budget) followed
// by the transport's OPTIONAL Admitter. It returns the (possibly
// admission-marked) context, a release closure to defer (nil when the transport
// has no admission control), and ok=false — having already written the response
// — when rejected (400 for a topic flood, 429 for a shed admission, 503 for an
// unavailable transport).
//
// Only the *abuse* cap (too many topics) runs here. The "missing topic" 400 and
// the JWT auth stay in registerSubscriber, AFTER this gate, so an unauthenticated
// request keeps its auth-first response ordering — admission sheds on capacity,
// not on request validity.
func (h *Hub) admitSubscriber(ctx context.Context, w http.ResponseWriter, r *http.Request) (context.Context, func(), bool) {
	if len(r.URL.Query()["topic"]) > maxQueryTopics {
		http.Error(w, `Too many "topic" parameters.`, http.StatusBadRequest)

		return ctx, nil, false
	}

	admitter, ok := h.transport.(Admitter)
	if !ok {
		return ctx, nil, true // transport has no admission control — admit
	}

	admitCtx, release, err := admitter.TryAdmit(ctx)
	if err != nil {
		h.writeAdmissionError(ctx, w, err)

		return ctx, nil, false
	}

	return admitCtx, release, true
}

// writeAdmissionError maps a TryAdmit error to the HTTP response: an
// *AdmissionError → 429 with an integer Retry-After (RFC 9110 delay-seconds,
// rounded up); any other error (e.g. a closed transport) → 503.
func (h *Hub) writeAdmissionError(ctx context.Context, w http.ResponseWriter, err error) {
	var ae *AdmissionError
	if errors.As(err, &ae) {
		// Integer ceil without importing math: (d + 1s - 1ns) / 1s.
		if secs := int((ae.RetryAfter + time.Second - time.Nanosecond) / time.Second); secs > 0 {
			w.Header().Set("Retry-After", strconv.Itoa(secs))
		}

		http.Error(w, http.StatusText(http.StatusTooManyRequests), http.StatusTooManyRequests)
	} else {
		http.Error(w, http.StatusText(http.StatusServiceUnavailable), http.StatusServiceUnavailable)
	}

	if h.logger.Enabled(ctx, slog.LevelDebug) {
		h.logger.LogAttrs(ctx, slog.LevelDebug, "Subscriber admission rejected", slog.Any("error", err))
	}
}

func (h *Hub) registerSubscriber(ctx context.Context, w http.ResponseWriter, r *http.Request) (*LocalSubscriber, *responseController) { //nolint:funlen
	ctx, span := startSpan(ctx, "mercure.subscribe", trace.WithSpanKind(trace.SpanKindConsumer))
	defer span.End()

	s := NewLocalSubscriber(h.retrieveLastEventID(ctx, r), h.logger, h.topicSelectorStore, withOutBuffer(h.subscriberOutBuffer))

	var (
		privateTopics []string
		claims        *claims
	)

	if h.subscriberJWTKeyFunc != nil { //nolint:nestif
		var err error

		claims, err = h.authorizeAndBind(r, false)
		if claims != nil {
			s.Claims = claims
			privateTopics = claims.Mercure.Subscribe
		}

		if err != nil || (claims == nil && !h.anonymous) {
			http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)

			if h.logger.Enabled(ctx, slog.LevelDebug) {
				h.logger.LogAttrs(ctx, slog.LevelDebug, "Subscriber unauthorized", slog.Any("error", err))
			}

			if err != nil {
				recordSpanError(span, err)
			}

			return nil, nil
		}
	}

	topics := r.URL.Query()["topic"]
	if len(topics) == 0 {
		http.Error(w, `Missing "topic" parameter.`, http.StatusBadRequest)

		return nil, nil
	}

	if len(topics) > maxQueryTopics {
		http.Error(w, `Too many "topic" parameters.`, http.StatusBadRequest)

		return nil, nil
	}

	s.SetTopics(topics, privateTopics)

	if span.IsRecording() {
		span.SetAttributes(
			attribute.String("mercure.subscriber.id", s.ID),
			attribute.StringSlice("mercure.topics", topics),
		)
	}

	// Re-check cancellation right before the un-cancellable registration: a
	// client that disconnected during admission/auth should abandon here rather
	// than commit a WithoutCancel registration whose teardown then frees the
	// admission slot anyway (the handler's deferred release).
	if ctx.Err() != nil {
		return nil, nil
	}

	addCtx := context.WithoutCancel(ctx)
	h.dispatchSubscriptionUpdate(addCtx, s, true)

	if err := h.transport.AddSubscriber(addCtx, s); err != nil {
		http.Error(w, http.StatusText(http.StatusServiceUnavailable), http.StatusServiceUnavailable)
		h.dispatchSubscriptionUpdate(addCtx, s, false)

		if h.logger.Enabled(ctx, slog.LevelError) {
			h.logger.LogAttrs(ctx, slog.LevelError, "Unable to add subscriber", slog.Any("error", err))
		}

		recordSpanError(span, err)

		return nil, nil
	}

	h.sendHeaders(ctx, w, s)
	rc := h.newResponseController(w, s)
	rc.flush(ctx)

	if h.logger.Enabled(ctx, slog.LevelInfo) {
		if claims != nil && h.logger.Enabled(ctx, slog.LevelDebug) {
			h.logger.LogAttrs(
				ctx, slog.LevelInfo, "New subscriber",
				slog.Any("subscriber", &s.Subscriber),
				slog.Any("payload", claims.Mercure.Payload),
			)
		} else {
			h.logger.LogAttrs(
				ctx, slog.LevelInfo, "New subscriber",
				slog.Any("subscriber", &s.Subscriber),
			)
		}
	}

	h.metrics.SubscriberConnected(s)

	return s, rc
}

//nolint:gochecknoglobals
var (
	headerConnection   = []string{"keep-alive"}
	headerContentType  = []string{"text/event-stream"}
	headerCacheControl = []string{"private, no-cache, no-store, must-revalidate, max-age=0"}
	headerPragma       = []string{"no-cache"}
	headerExpire       = []string{"0"}

	headerXAccelBuffering = []string{"no"}
)

// sendHeaders sends correct HTTP headers to create a keep-alive connection.
func (h *Hub) sendHeaders(ctx context.Context, w http.ResponseWriter, s *LocalSubscriber) {
	header := w.Header()

	// Keep alive, useful only for HTTP 1 clients https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Keep-Alive
	header["Connection"] = headerConnection

	header["Content-Type"] = headerContentType

	// Disable cache, even for old browsers and proxies
	header["Cache-Control"] = headerCacheControl
	header["Pragma"] = headerPragma
	header["Expire"] = headerExpire

	// NGINX support https://www.nginx.com/resources/wiki/start/topics/examples/x-accel/#x-accel-buffering
	header["X-Accel-Buffering"] = headerXAccelBuffering

	if s.RequestLastEventID != "" {
		header["Last-Event-Id"] = []string{<-s.responseLastEventID}
	}

	// Write a comment in the body
	// Go currently doesn't provide a better way to flush the headers
	if _, err := w.Write([]byte{':', '\n'}); err != nil && h.logger.Enabled(ctx, slog.LevelInfo) {
		h.logger.LogAttrs(ctx, slog.LevelInfo, "Failed to write comment", slog.Any("error", err))
	}
}

// retrieveLastEventID extracts the Last-Event-ID from the corresponding HTTP header with a fallback on the query parameter.
func (h *Hub) retrieveLastEventID(ctx context.Context, r *http.Request) string {
	if id := r.Header.Get("Last-Event-ID"); id != "" {
		return id
	}

	query := r.URL.Query()
	if id := query.Get("lastEventID"); id != "" {
		return id
	}

	if legacyEventIDValues, present := query["Last-Event-ID"]; present { //nolint:nestif
		infoLevel := h.logger.Enabled(ctx, slog.LevelInfo)
		if h.isBackwardCompatiblyEnabledWith(7) {
			if infoLevel {
				h.logger.LogAttrs(ctx, slog.LevelInfo, "Deprecated: the 'Last-Event-ID' query parameter is deprecated since the version 8 of the protocol, use 'lastEventID' instead.")
			}

			if len(legacyEventIDValues) != 0 {
				return legacyEventIDValues[0]
			}
		} else if infoLevel {
			h.logger.LogAttrs(ctx, slog.LevelInfo, `Unsupported: the "Last-Event-ID"" query parameter is not supported anymore, use "lastEventID"" instead or enable backward compatibility with version 7 of the protocol.`)
		}
	}

	return ""
}

// Write sends the given bytes to the client.
// It returns false if the subscriber has been disconnected (e.g. timeout).
// For the per-update path, data is the cached wire bytes shared read-only across
// the concurrent fan-out, so callers must not mutate it.
func (h *Hub) write(ctx context.Context, rc *responseController, data []byte) bool {
	// Observer captured before the timed body so the observation always
	// fires (defer) regardless of which return path exits. Type assertion
	// is per-call because h.metrics is the Metrics interface (no narrower
	// extension type) — branch-predicted, identical to the
	// DisconnectReasonReporter type assertion in shutdown(). Steady state
	// is NopMetrics or PrometheusMetrics; the cost is negligible.
	observer, _ := h.metrics.(WriteFlushObserver)

	ok := false

	if observer != nil {
		start := time.Now()
		defer func() {
			observer.ObserveWriteFlush(time.Since(start).Seconds(), ok)
		}()
	}

	if !rc.setDispatchWriteDeadline(ctx) {
		return false
	}

	// Use rc.rw.Write, NOT io.WriteString: io.WriteString would dispatch to the
	// writer's promoted WriteString, changing which method the write path (and
	// tests that observe Write) see.
	if _, err := rc.rw.Write(data); err != nil {
		// The level-gate only conditions the log line; the return-false must
		// always fire on a write error. Folding the level check into the if
		// expression (the upstream shape) silently swallows write errors when
		// the operator runs at INFO+, misclassifying write_failed as
		// client_closed (when the request ctx eventually cancels) or unknown.
		if h.logger.Enabled(ctx, slog.LevelDebug) {
			h.logger.LogAttrs(ctx, slog.LevelDebug, "Failed to write comment", slog.Any("error", err))
		}

		return false
	}

	ok = rc.flush(ctx) && rc.setDefaultWriteDeadline(ctx)

	return ok
}

func (h *Hub) shutdown(ctx context.Context, s *LocalSubscriber, reason DisconnectReason) {
	// Notify that the client is closing the connection
	s.Disconnect()

	ctx = context.WithoutCancel(ctx)

	if err := h.transport.RemoveSubscriber(ctx, s); err != nil {
		if h.logger.Enabled(ctx, slog.LevelError) {
			h.logger.LogAttrs(ctx, slog.LevelError, "Failed to remove subscriber on shutdown", slog.Any("error", err))
		}

		// Override only when the loop didn't already classify the exit:
		// a write_failed → transport-RemoveSubscriber error chain should
		// keep the more specific write_failed attribution.
		if reason == DisconnectReasonUnknown {
			reason = DisconnectReasonTransportError
		}
	}

	h.dispatchSubscriptionUpdate(ctx, s, false)

	if h.logger.Enabled(ctx, slog.LevelInfo) {
		h.logger.LogAttrs(
			ctx, slog.LevelInfo, "Subscriber disconnected",
			slog.Any("subscriber", &s.Subscriber),
			slog.String("reason", string(reason)),
		)
	}

	if r, ok := h.metrics.(DisconnectReasonReporter); ok {
		r.SubscriberDisconnectedWithReason(s, reason)

		return
	}

	h.metrics.SubscriberDisconnected(s)
}

func (h *Hub) dispatchSubscriptionUpdate(ctx context.Context, s *LocalSubscriber, active bool) {
	if !h.subscriptions {
		return
	}

	for _, subscription := range s.getSubscriptions("", jsonldContext, active) {
		j, err := json.MarshalIndent(subscription, "", "  ")
		if err != nil {
			panic(err)
		}

		u := &Update{
			Topics:  []string{subscription.ID},
			Private: true,
			Debug:   h.debug,
			Event:   Event{Data: string(j)},
		}

		if err := h.transport.Dispatch(ctx, u); err != nil && h.logger.Enabled(ctx, slog.LevelError) {
			h.logger.LogAttrs(ctx, slog.LevelError, "Failed to dispatch update", slog.Any("update", u), slog.Any("subscription", subscription.ID), slog.Any("error", err))
		}
	}
}

// randomizeWriteDeadline generates a random duration between 80% and 100% of the original value.
// This is useful to avoid all subscribers disconnecting at the same time, which can lead to a thundering herd problem.
func randomizeWriteDeadline(originalValue time.Duration) time.Duration {
	minV := int64(float64(originalValue) * 0.80)
	maxV := int64(originalValue)

	// Ensure min is not greater than max. This handles cases where originalValue is very small (e.g., 1, 2, 3, 4).
	// For originalValue = 1, min becomes 0. For originalValue = 4, min becomes 3.
	// This shouldn't happen in practice, but it's a good safeguard.
	if minV > maxV {
		minV = maxV
	}

	// Calculate the range size. Add 1 because Int64N is exclusive of the upper bound.
	rangeSize := maxV - minV + 1

	// If rangeSize is 0 or less (e.g., if originalValue was 0), just return min (which would be 0).
	// rand.Int64N requires a positive argument.
	if rangeSize <= 0 {
		return time.Duration(minV)
	}

	// Generate a random number in the range [min, max]
	// rand.Int64n(n) returns a non-negative pseudo-random 64-bit integer in the half-open interval [0, n).
	// Adding 'min' shifts this result to the desired range [min, max].
	return time.Duration(rand.Int64N(rangeSize) + minV) //nolint:gosec
}

func (h *Hub) handleWriterError(ctx context.Context, err error, message string) {
	if errors.Is(err, http.ErrNotSupported) {
		panic(err)
	}

	if h.logger.Enabled(ctx, slog.LevelInfo) {
		h.logger.LogAttrs(ctx, slog.LevelInfo, message, slog.Any("error", err))
	}
}
