// Package mercure helps implement the Mercure protocol (https://mercure.rocks) in Go projects.
// It provides an implementation of a Mercure hub as an HTTP handler.
package mercure

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const (
	DefaultWriteTimeout    = 600 * time.Second
	DefaultDispatchTimeout = 5 * time.Second
	DefaultHeartbeat       = 40 * time.Second
)

// ErrUnsupportedProtocolVersion is returned when the version passed is unsupported.
var ErrUnsupportedProtocolVersion = errors.New("compatibility mode only supports protocol version 7")

// ErrInvalidSubscriberOutBuffer is returned by WithSubscriberOutBuffer for a
// positive value below MinSubscriberOutBuffer.
var ErrInvalidSubscriberOutBuffer = errors.New("subscriber out buffer must be 0 (use the default) or at least the minimum")

// Option instances allow to configure the library.
type Option func(o *opt) error

// WithAnonymous allows subscribers with no valid JWT.
func WithAnonymous() Option {
	return func(o *opt) error {
		o.anonymous = true

		return nil
	}
}

// WithDebug enables the debug mode.
func WithDebug() Option {
	return func(o *opt) error {
		o.debug = true

		return nil
	}
}

func WithUI() Option {
	return func(o *opt) error {
		o.ui = true

		return nil
	}
}

// WithDemo enables the demo.
func WithDemo() Option {
	return func(o *opt) error {
		o.demo = true
		o.ui = true

		return nil
	}
}

// WithMetrics enables collection of Prometheus metrics.
func WithMetrics(m Metrics) Option {
	return func(o *opt) error {
		o.metrics = m

		return nil
	}
}

// WithSubscriptions allows to dispatch updates when subscriptions are created or terminated.
func WithSubscriptions() Option {
	return func(o *opt) error {
		o.subscriptions = true

		return nil
	}
}

// WithLogger sets the logger to use.
func WithLogger(logger *slog.Logger) Option {
	return func(o *opt) error {
		o.logger = logger

		return nil
	}
}

// WithWriteTimeout sets maximum duration before closing the connection, defaults to 600s, set to 0 to disable.
func WithWriteTimeout(timeout time.Duration) Option {
	return func(o *opt) error {
		o.writeTimeout = timeout

		return nil
	}
}

// WithDispatchTimeout sets maximum dispatch duration of an update.
func WithDispatchTimeout(timeout time.Duration) Option {
	return func(o *opt) error {
		o.dispatchTimeout = timeout

		return nil
	}
}

// WithPublishTimeout bounds a single detached publish, disabled by default (0).
// The publish path detaches dispatch from the publisher's request context so a
// mid-publish disconnect does not abort the write; without a bound, a stalled
// transport (e.g. an unresponsive Redis) would block that goroutine
// indefinitely. The deadline covers the whole detached call — the transport
// dispatch including any publisher rate-limit wait. When it is exceeded the
// write may still have committed, so PublishHandler returns 504 — a retry is
// safe only for idempotent updates (a stable, publisher-supplied id).
func WithPublishTimeout(timeout time.Duration) Option {
	return func(o *opt) error {
		o.publishTimeout = timeout

		return nil
	}
}

// WithHeartbeat sets the frequency of the heartbeat, disabled by default.
func WithHeartbeat(interval time.Duration) Option {
	return func(o *opt) error {
		o.heartbeat = interval

		return nil
	}
}

// WithSubscriberOutBuffer sets the per-subscriber out-channel and pre-ready
// queue capacity (default 1000). Each connected subscriber commits this many
// *Update slots up front, so a large fleet may want it lower to cut steady-state
// memory and cold-start allocation pressure. 0 keeps the default; a positive
// value below MinSubscriberOutBuffer is rejected (a buffer that small sheds live
// updates), so a too-small setting fails loudly rather than silently reverting.
func WithSubscriberOutBuffer(size int) Option {
	return func(o *opt) error {
		if size != 0 && size < MinSubscriberOutBuffer {
			return fmt.Errorf("%w %d, got %d", ErrInvalidSubscriberOutBuffer, MinSubscriberOutBuffer, size)
		}

		o.subscriberOutBuffer = size

		return nil
	}
}

// WithPublisherJWTKeyFunc sets the function to use to parse and verify the publisher JWT.
func WithPublisherJWTKeyFunc(keyfunc jwt.Keyfunc) Option {
	return func(o *opt) error {
		o.publisherJWTKeyFunc = keyfunc

		return nil
	}
}

// WithSubscriberJWTKeyFunc sets the function to use to parse and verify the subscriber JWT.
func WithSubscriberJWTKeyFunc(keyfunc jwt.Keyfunc) Option {
	return func(o *opt) error {
		o.subscriberJWTKeyFunc = keyfunc

		return nil
	}
}

// WithPublisherJWT sets the JWT key and the signing algorithm to use for publishers.
func WithPublisherJWT(key []byte, alg string) Option {
	return func(o *opt) error {
		keyfunc, err := createJWTKeyfunc(key, alg)
		o.publisherJWTKeyFunc = keyfunc

		return err
	}
}

// WithSubscriberJWT sets the JWT key and the signing algorithm to use for subscribers.
func WithSubscriberJWT(key []byte, alg string) Option {
	return func(o *opt) error {
		keyfunc, err := createJWTKeyfunc(key, alg)
		o.subscriberJWTKeyFunc = keyfunc

		return err
	}
}

// WithAllowedHosts sets the allowed hosts.
func WithAllowedHosts(hosts []string) Option {
	return func(o *opt) error {
		o.allowedHosts = hosts

		return nil
	}
}

// nullOrigin is the literal "null" the spec mandates for sandboxed
// browsing contexts (data:, file://, srcdoc, opaque). Allow-listed
// alongside "*" by validateOrigins, which is the shared validator for
// WithPublishOrigins and WithCORSOrigins.
const nullOrigin = "null"

func validateOrigins(origins []string) error {
	for _, origin := range origins {
		switch origin {
		case "*", nullOrigin:
			continue
		}

		u, err := url.Parse(origin)
		if err != nil ||
			!u.IsAbs() ||
			u.Opaque != "" ||
			u.User != nil ||
			u.Path != "" ||
			u.RawQuery != "" ||
			u.Fragment != "" {
			return fmt.Errorf(`invalid origin, must be a URL having only a scheme, a host and optionally a port, "*" or "null": %w`, err)
		}
	}

	return nil
}

// WithPublishOrigins sets the origins allowed to publish updates.
func WithPublishOrigins(origins []string) Option {
	return func(o *opt) error {
		if err := validateOrigins(origins); err != nil {
			return err
		}

		// wildcard support has been adapted from https://github.com/rs/cors/blob/1084d89a16921942356d1c831fbe523426cf836e/cors.go#L171
		// Copyright (c) 2014 Olivier Poitrey <rs@dailymotion.com>
		// MIT licensed.
		for _, origin := range origins {
			// Note: for origins matching, the spec requires a case-sensitive matching.
			// As it may error-prone, we chose to ignore the spec here.
			origin = strings.ToLower(origin)
			if origin == "*" {
				// If "*" is present in the list, turn the whole list into a match all
				o.publishOriginsAll = true
				o.publishOrigins = nil
				o.publishWOrigins = nil

				break
			} else if prefix, suffix, found := strings.Cut(origin, "*"); found {
				// Split the origin in two: start and end string without the *
				w := wildcard{prefix, suffix}
				o.publishWOrigins = append(o.publishWOrigins, w)
			} else {
				o.publishOrigins = append(o.publishOrigins, origin)
			}
		}

		return nil
	}
}

// WithCORSOrigins sets the allowed CORS origins.
func WithCORSOrigins(origins []string) Option {
	return func(o *opt) error {
		if err := validateOrigins(origins); err != nil {
			return err
		}

		o.corsOrigins = origins

		return nil
	}
}

// WithTransport sets the transport to use.
func WithTransport(t Transport) Option {
	return func(o *opt) error {
		o.transport = t

		return nil
	}
}

// WithTopicSelectorStore sets the TopicSelectorStore instance to use.
func WithTopicSelectorStore(tss *TopicSelectorStore) Option {
	return func(o *opt) error {
		o.topicSelectorStore = tss

		return nil
	}
}

// WithCookieName sets the name of the authorization cookie (defaults to "mercureAuthorization").
func WithCookieName(cookieName string) Option {
	return func(o *opt) error {
		o.cookieName = cookieName

		return nil
	}
}

// WithProtocolVersionCompatibility sets the version of the Mercure protocol to be backward compatible with (only version 7 is supported).
func WithProtocolVersionCompatibility(protocolVersionCompatibility int) Option {
	return func(o *opt) error {
		switch protocolVersionCompatibility {
		case 7:
			o.protocolVersionCompatibility = protocolVersionCompatibility

			return nil
		default:
			return ErrUnsupportedProtocolVersion
		}
	}
}

// WithCodec sets the Codec used for serializing updates in transports.
// Defaults to JSONCodec if not set. Transports that implement TransportCodec
// will receive this codec automatically.
func WithCodec(c Codec) Option {
	return func(o *opt) error {
		o.codec = c

		return nil
	}
}

// opt contains the available options.
//
// If you change this, also update the Caddy module and the documentation.
type opt struct {
	transport                    Transport
	topicSelectorStore           *TopicSelectorStore
	codec                        Codec
	anonymous                    bool
	debug                        bool
	subscriptions                bool
	ui                           bool
	demo                         bool
	logger                       *slog.Logger
	writeTimeout                 time.Duration
	dispatchTimeout              time.Duration
	heartbeat                    time.Duration
	publisherJWTKeyFunc          jwt.Keyfunc
	subscriberJWTKeyFunc         jwt.Keyfunc
	metrics                      Metrics
	allowedHosts                 []string
	publishOriginsAll            bool
	publishOrigins               []string
	publishWOrigins              []wildcard
	corsOrigins                  []string
	cookieName                   string
	protocolVersionCompatibility int
	subscriberOutBuffer          int
	publishTimeout               time.Duration
	claimHeaderBindings          []ClaimHeaderBinding
}

func (o *opt) isBackwardCompatiblyEnabledWith(version int) bool {
	return o.protocolVersionCompatibility != 0 && version >= o.protocolVersionCompatibility
}

// Hub stores channels with clients currently subscribed and allows to dispatch updates.
type Hub struct {
	deprecatedHub
	*opt

	handler http.Handler
	ctx     context.Context //nolint:containedctx

	// demoInsecureWarned bounds the Demo handler's non-Secure-cookie warning
	// to once per hub instance (per process in the common single-hub case) via a CAS. The Demo endpoint is unauthenticated, so
	// without this an attacker hitting it over plain HTTP from a non-loopback
	// address could amplify the Warn into a log-volume DoS. The warning
	// signals a static misconfiguration (trusted_proxies / X-Forwarded-Proto),
	// so once is enough for an operator to act on. (atomic.Bool rather than
	// sync.Once so the log call stays in the handler scope with the request
	// context in view — sync.Once.Do's func() signature can't thread ctx.)
	demoInsecureWarned atomic.Bool
}

// NewHub creates a new Hub instance.
func NewHub(ctx context.Context, options ...Option) (*Hub, error) {
	opt := &opt{
		writeTimeout:    DefaultWriteTimeout,
		dispatchTimeout: DefaultDispatchTimeout,
		heartbeat:       DefaultHeartbeat,
	}

	for _, o := range options {
		if err := o(opt); err != nil {
			return nil, err
		}
	}

	if opt.logger == nil {
		opt.logger = slog.New(mercureHandler{slog.Default().Handler()})
	}

	if opt.topicSelectorStore == nil {
		tss, err := NewTopicSelectorStore(DefaultTopicSelectorStoreCacheSize)
		if err != nil {
			return nil, err
		}

		opt.topicSelectorStore = tss
	}

	if opt.transport == nil {
		opt.transport = NewLocalTransport(NewSubscriberList(DefaultSubscriberListCacheSize))
	}

	if ttss, ok := opt.transport.(TransportTopicSelectorStore); ok {
		ttss.SetTopicSelectorStore(opt.topicSelectorStore)
	}

	if opt.codec != nil {
		if tc, ok := opt.transport.(TransportCodec); ok {
			tc.SetCodec(opt.codec)
		}
	}

	if opt.metrics == nil {
		opt.metrics = NopMetrics{}
	}

	if opt.cookieName == "" {
		opt.cookieName = defaultCookieName
	}

	// Set-level validation of claim-header bindings: the Hub is the only place that sees
	// every binding, and it must guard programmatic callers too, not just the Caddy
	// adapter. Runs after the logger default (it may warn) and before initHandler (which
	// consumes the binding header names for CORS).
	if err := opt.validateClaimHeaderBindings(); err != nil {
		return nil, err
	}

	h := &Hub{opt: opt, ctx: ctx}
	h.initHandler()

	return h, nil
}

// Stop stops the hub.
func (h *Hub) Stop(ctx context.Context) error {
	if err := h.transport.Close(ctx); err != nil {
		return fmt.Errorf("transport error: %w", err)
	}

	return nil
}
