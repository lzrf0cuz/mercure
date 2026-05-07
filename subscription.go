package mercure

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/url"

	"github.com/gorilla/mux"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// ErrTransportDoesNotSupportSubscribers signals that initSubscription was
// invoked with a transport that doesn't implement TransportSubscribers.
// registerSubscriptionHandlers (handler.go) is the only mechanism that
// prevents this branch from being reached when the transport lacks the
// interface — it skips route registration entirely in that case. Reaching
// the 500-return below means that gate has regressed.
//
// Exported so operators with errors.Is-aware log/alert middleware can
// branch on the failure mode without substring-matching the log message.
var ErrTransportDoesNotSupportSubscribers = errors.New("transport does not implement TransportSubscribers")

// ErrTooManySubscribers signals that the cluster's subscriber list exceeds the
// transport's configured materialization cap, so GetSubscribers refused rather
// than build a response large enough to OOM the hub. The SubscriptionsHandler
// maps it to 503. Exported so the transport can return it (via errors.Is) and
// operators can branch on it.
var ErrTooManySubscribers = errors.New("too many subscribers to list")

const (
	jsonldContext            = "https://mercure.rocks/"
	subscriptionsPath        = "/subscriptions"
	subscriptionURL          = defaultHubURL + subscriptionsPath + "/{topic}/{subscriber}"
	subscriptionsForTopicURL = defaultHubURL + subscriptionsPath + "/{topic}"
	subscriptionsURL         = defaultHubURL + subscriptionsPath
)

var jsonldContentType = []string{"application/ld+json"} // nolint:gochecknoglobals

type subscription struct {
	Context     string `json:"@context,omitempty"`
	ID          string `json:"id"`
	Type        string `json:"type"`
	Subscriber  string `json:"subscriber"`
	Topic       string `json:"topic"`
	Active      bool   `json:"active"`
	LastEventID string `json:"lastEventID,omitempty"`
	Payload     any    `json:"payload,omitempty"`
}

type subscriptionCollection struct {
	Context       string         `json:"@context"`
	ID            string         `json:"id"`
	Type          string         `json:"type"`
	LastEventID   string         `json:"lastEventID"`
	Subscriptions []subscription `json:"subscriptions"`
}

func (h *Hub) SubscriptionsHandler(w http.ResponseWriter, r *http.Request) {
	span, currentURL, lastEventID, subscribers, ok := h.initSubscription(w, r)
	defer span.End()

	if !ok {
		return
	}

	w.WriteHeader(http.StatusOK)

	subscriptionCollection := subscriptionCollection{
		Context:       jsonldContext,
		ID:            currentURL,
		Type:          "Subscriptions",
		LastEventID:   lastEventID,
		Subscriptions: make([]subscription, 0),
	}

	vars := mux.Vars(r)

	t, _ := url.QueryUnescape(vars["topic"])
	for _, subscriber := range subscribers {
		subscriptionCollection.Subscriptions = append(subscriptionCollection.Subscriptions, subscriber.getSubscriptions(t, "", true)...)
	}

	j, err := json.MarshalIndent(subscriptionCollection, "", "  ")
	if err != nil {
		// Can't happen
		panic(err)
	}

	if _, err := w.Write(j); err != nil {
		ctx := r.Context()

		if h.logger.Enabled(ctx, slog.LevelInfo) {
			h.logger.LogAttrs(ctx, slog.LevelInfo, "Failed to write subscriptions response", slog.Any("error", err))
		}
	}
}

func (h *Hub) SubscriptionHandler(w http.ResponseWriter, r *http.Request) {
	span, _, lastEventID, subscribers, ok := h.initSubscription(w, r)
	defer span.End()

	if !ok {
		return
	}

	ctx := r.Context()
	vars := mux.Vars(r)
	s, _ := url.QueryUnescape(vars["subscriber"])
	t, _ := url.QueryUnescape(vars["topic"])

	for _, subscriber := range subscribers {
		if subscriber.ID != s {
			continue
		}

		for _, subscription := range subscriber.getSubscriptions(t, jsonldContext, true) {
			if subscription.Topic != t {
				continue
			}

			subscription.LastEventID = lastEventID

			j, err := json.MarshalIndent(subscription, "", "  ")
			if err != nil {
				panic(err)
			}

			if _, err := w.Write(j); err != nil && h.logger.Enabled(ctx, slog.LevelInfo) { //nolint:gosec
				h.logger.LogAttrs(ctx, slog.LevelInfo, "Failed to write subscription response", slog.Any("subscriber", subscriber), slog.Any("error", err))
			}

			return
		}
	}

	http.NotFound(w, r)
}

// writeGetSubscribersError writes the HTTP error, log, and span for a failed
// GetSubscribers. A fleet exceeding the transport's materialization cap
// (ErrTooManySubscribers) is a 503 — the endpoint can't serve a list that large
// right now — rather than a 500.
func (h *Hub) writeGetSubscribersError(ctx context.Context, w http.ResponseWriter, span trace.Span, err error) {
	status := http.StatusInternalServerError
	if errors.Is(err, ErrTooManySubscribers) {
		status = http.StatusServiceUnavailable
	}

	http.Error(w, http.StatusText(status), status)

	if h.logger.Enabled(ctx, slog.LevelError) {
		h.logger.LogAttrs(ctx, slog.LevelError, "Error retrieving subscribers", slog.Any("error", err))
	}

	recordSpanError(span, err)
}

func (h *Hub) initSubscription(w http.ResponseWriter, r *http.Request) (span trace.Span, currentURL, lastEventID string, subscribers []*Subscriber, ok bool) {
	ctx, span := startSpan(r.Context(), "mercure.subscriptions", trace.WithSpanKind(trace.SpanKindInternal))
	currentURL = r.URL.RequestURI()

	if h.subscriberJWTKeyFunc != nil {
		claims, err := h.authorizeAndBind(r, false)
		if err != nil || claims == nil || claims.Mercure.Subscribe == nil || !canReceive(h.topicSelectorStore, []string{currentURL}, claims.Mercure.Subscribe) {
			h.httpAuthorizationError(w, r, err)

			if err != nil {
				recordSpanError(span, err)
			}

			return span, "", "", nil, false
		}
	}

	transport, ok := h.transport.(TransportSubscribers)
	if !ok {
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)

		if h.logger.Enabled(ctx, slog.LevelError) {
			h.logger.LogAttrs(ctx, slog.LevelError,
				"Subscription handler invoked but transport does not implement TransportSubscribers (registerSubscriptionHandlers should have prevented this)",
				slog.Any("error", ErrTransportDoesNotSupportSubscribers))
		}

		// Tag the failure mode as a queryable span attribute so trace
		// consumers can filter without substring-matching the recorded
		// error message text. `error.type` is the OTel-standard attribute
		// name for error categorization.
		span.SetAttributes(attribute.String("error.type", "transport_does_not_support_subscribers"))
		recordSpanError(span, ErrTransportDoesNotSupportSubscribers)

		// Return empty currentURL (rather than the parsed RequestURI) for
		// shape-consistency with the other failure paths in this function —
		// neither caller reads currentURL when ok=false, but unconditional
		// "" makes the contract uniform.
		return span, "", "", nil, false
	}

	var err error

	lastEventID, subscribers, err = transport.GetSubscribers(ctx)
	if err != nil {
		h.writeGetSubscribersError(ctx, w, span, err)

		return span, currentURL, lastEventID, subscribers, false
	}

	if r.Header.Get("If-None-Match") == lastEventID {
		w.WriteHeader(http.StatusNotModified)

		// Per OpenTelemetry HTTP server semantic conventions, spans for
		// 1xx/2xx/3xx (and 4xx, which is client-side error) MUST stay at
		// the default Unset status; only 5xx server-side errors set Error.
		// The attribute below carries the distinguishing signal — a trace
		// query filtering on mercure.subscriptions.cache_hit=true
		// isolates etag-served requests without violating spec semantics.
		span.SetAttributes(attribute.Bool("mercure.subscriptions.cache_hit", true))

		return span, "", "", nil, false
	}

	header := w.Header()
	header["Content-Type"] = jsonldContentType
	header["ETag"] = []string{lastEventID}

	return span, currentURL, lastEventID, subscribers, true
}
