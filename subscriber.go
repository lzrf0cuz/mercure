package mercure

import (
	"log/slog"
	"net/url"
	"regexp"
)

// Subscriber represents a client subscribed to a list of topics on a remote or on the current hub.
type Subscriber struct {
	ID                     string
	EscapedID              string
	Claims                 *claims
	EscapedTopics          []string
	RequestLastEventID     string
	SubscribedTopics       []string
	SubscribedTopicRegexps []*regexp.Regexp
	AllowedPrivateTopics   []string
	AllowedPrivateRegexps  []*regexp.Regexp

	logger             *slog.Logger
	topicSelectorStore *TopicSelectorStore
}

func NewSubscriber(logger *slog.Logger, topicSelectorStore *TopicSelectorStore) *Subscriber {
	return &Subscriber{
		logger:             logger,
		topicSelectorStore: topicSelectorStore,
	}
}

// SetTopicSelectorStore sets the topic selector store on the subscriber.
// This is needed for deserialized cross-node subscribers that lack the
// unexported topicSelectorStore field after JSON round-tripping. Without
// the store, MatchTopics() would panic with a nil pointer dereference on
// topic-filtered subscription queries.
func (s *Subscriber) SetTopicSelectorStore(store *TopicSelectorStore) {
	s.topicSelectorStore = store
}

// SetTopics compiles topic selector regexps.
func (s *Subscriber) SetTopics(subscribedTopics, allowedPrivateTopics []string) {
	s.SubscribedTopics = subscribedTopics
	s.AllowedPrivateTopics = allowedPrivateTopics
	s.EscapedTopics = escapeTopics(subscribedTopics)
}

func escapeTopics(topics []string) []string {
	escapedTopics := make([]string, 0, len(topics))
	for _, topic := range topics {
		escapedTopics = append(escapedTopics, url.QueryEscape(topic))
	}

	return escapedTopics
}

// MatchTopics checks if the current subscriber can access to at least one of the given topics.
//
//nolint:gocognit
func (s *Subscriber) MatchTopics(topics []string, private bool) bool {
	if s.topicSelectorStore == nil {
		// Cross-node deserialized subscriber whose caller did not call
		// SetTopicSelectorStore. Treat as no match rather than panic.
		return false
	}

	var subscribed bool

	canAccess := !private

	for _, topic := range topics {
		if !subscribed {
			for _, ts := range s.SubscribedTopics {
				if s.topicSelectorStore.match(topic, ts) {
					subscribed = true

					if canAccess {
						return true
					}

					break
				}
			}
		}

		if !canAccess {
			for _, ts := range s.AllowedPrivateTopics {
				if s.topicSelectorStore.match(topic, ts) {
					canAccess = true

					if subscribed {
						return true
					}

					break
				}
			}
		}
	}

	return subscribed && canAccess
}

// Match checks if the current subscriber can receive the given update.
func (s *Subscriber) Match(u *Update) bool {
	return s.MatchTopics(u.Topics, u.Private)
}

// maxObservedEventIDLen bounds how much of a client-supplied Last-Event-ID is
// copied into a log record. Last-Event-ID arrives as an HTTP header / query
// param bounded only by the server's max-header size (~1 MiB), so logging it
// verbatim lets one oversized value bloat every log line that includes the
// subscriber (LogValue fans out to several call sites). A valid UUIDv7 ID is
// 36 chars; 80 leaves headroom for custom publisher IDs. redistransport caps
// the same client value to the same length on its observability paths — keep
// the two in sync.
const maxObservedEventIDLen = 80

// truncateEventIDForObservability rune-safely caps a client-controlled event
// ID for safe inclusion in logs. Truncation is observability-only — the full
// RequestLastEventID is still used for history-replay seeking. The common
// case (a UUIDv7 ID, 36 bytes) is a single length check with no allocation;
// only an over-cap value walks the string to find the rune boundary at the
// cap, deliberately avoiding the whole-string allocation a naive
// []rune(id)[:n] would incur on an attacker-bounded (~1 MiB) input.
func truncateEventIDForObservability(id string) string {
	if len(id) <= maxObservedEventIDLen { // byte len >= rune len, so this is a safe fast path
		return id
	}

	runes := 0
	for i := range id {
		if runes == maxObservedEventIDLen {
			return id[:i] + "…(truncated)"
		}

		runes++
	}

	// Fewer than maxObservedEventIDLen runes despite >maxObservedEventIDLen
	// bytes (multi-byte content): already within the rune cap.
	return id
}

func (s *Subscriber) LogValue() slog.Value {
	attrs := []slog.Attr{
		slog.String("id", s.ID),
		slog.String("last_event_id", truncateEventIDForObservability(s.RequestLastEventID)),
	}

	if s.AllowedPrivateTopics != nil {
		attrs = append(attrs, slog.Any("topic_selectors", s.AllowedPrivateTopics))
	}

	if s.SubscribedTopics != nil {
		attrs = append(attrs, slog.Any("topics", s.SubscribedTopics))
	}

	return slog.GroupValue(attrs...)
}

// getSubscriptions return the list of subscriptions associated to this subscriber.
func (s *Subscriber) getSubscriptions(topic, context string, active bool) []subscription {
	var subscriptions []subscription //nolint:prealloc

	for k, t := range s.SubscribedTopics {
		if topic != "" && (!s.MatchTopics([]string{topic}, false) || t != topic) {
			continue
		}

		subscription := subscription{
			Context:    context,
			ID:         "/.well-known/mercure/subscriptions/" + s.EscapedTopics[k] + "/" + s.EscapedID,
			Type:       "Subscription",
			Subscriber: s.ID,
			Topic:      t,
			Active:     active,
		}
		if s.Claims != nil && s.Claims.Mercure.Payload != nil {
			subscription.Payload = s.Claims.Mercure.Payload
		}

		subscriptions = append(subscriptions, subscription)
	}

	return subscriptions
}
