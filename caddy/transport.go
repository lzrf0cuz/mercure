package caddy

import (
	"errors"
	"fmt"
	"log/slog"

	"github.com/caddyserver/caddy/v2"
	"github.com/dunglas/mercure"
	"github.com/prometheus/client_golang/prometheus"
)

var TransportUsagePool = caddy.NewUsagePool() //nolint:gochecknoglobals

// ErrSubscriberListCacheSizeMissing is the sentinel returned when a transport
// Provision cannot read the SubscriberListCacheSize from caddy.Context — the
// parent mercure module is expected to inject it via WithValue. errors.Is on
// this lets tests assert the wiring contract without coupling to message text.
var ErrSubscriberListCacheSizeMissing = errors.New("subscriber-list cache size missing or wrong type in caddy context")

// errTransportPoolDestructorMismatch is returned when TransportUsagePool
// returns an entry whose concrete type does not match the expected
// TransportDestructor[T]. Unexported because firing this would be an
// internal programming error (two transports colliding on the same pool
// key with incompatible types) — not a contract surface external code
// should branch on.
var errTransportPoolDestructorMismatch = errors.New("transport pool destructor type mismatch")

// ErrSubscriberListCacheSizeOverflow is returned when the configured
// subscriber_list_cache_size exceeds math.MaxInt — fatal on 32-bit
// platforms long before reaching MaxUint64.
var ErrSubscriberListCacheSizeOverflow = errors.New("subscriber_list_cache_size exceeds platform int range")

type Transport interface {
	GetTransport() mercure.Transport
}

// TransportMetricsRegisterer is the optional interface a transport can
// implement to receive the parent mercure caddy module's Prometheus registry
// late, after Provision returns. Caddy gives sub-modules a derived ctx whose
// GetMetricsRegistry() returns a typed-nil *prometheus.Registry — that's why
// transports that try to bind metrics via WithPrometheusRegisterer at
// constructor time end up with metrics disabled. This interface lets the
// parent module pass its own non-nil registry after the transport has been
// constructed.
//
// Fork-only: not part of upstream mercure's Transport interface. Bolt and the
// upstream Local transport do not implement it (they have no transport-level
// metrics worth registering); the type-assertion in mercure.go's Provision is
// a no-op for those.
//
// Implementer contract:
//
//   - Idempotent: only the first call with a non-nil registry should register
//     collectors; subsequent calls (e.g. ctor path then Caddy path on the
//     same transport instance) must not double-register.
//   - Single-shot: must run BEFORE the transport's first user-facing
//     operation (AddSubscriber/Dispatch). The parent mercure module satisfies
//     this by calling RegisterMetricsWith inside Provision, before HTTP
//     routes start serving.
//   - Typed-nil tolerant: a typed-nil *prometheus.Registry argument is the
//     same as nil; implementers must treat it as a no-op rather than panic.
//   - Error semantics: returning a non-nil error fails Provision (the parent
//     module wraps it as `transport metrics registration failed`).
type TransportMetricsRegisterer interface {
	RegisterMetricsWith(registerer prometheus.Registerer) error
}

// bindTransportMetrics late-binds registry into transport when transport
// implements TransportMetricsRegisterer. Transports that do not opt in
// (Bolt, upstream Local) skip — the type assertion fails and the helper
// is a no-op, preserving behavior for transports without transport-level
// metrics. logger may be nil; if non-nil, a Debug breadcrumb is emitted
// on the skip path so an operator chasing "why are my transport metrics
// empty?" can see whether binding ran or short-circuited.
//
// Extracted from Provision so the late-binding contract can be exercised
// in isolation (no full Caddy module lifecycle required) — locks the
// regression where Caddy's ctx.WithValue chain replaces the metrics
// registry with a typed-nil. The fix at the call site (Provision captures
// registry BEFORE the WithValue chain) hands the real registry here.
func bindTransportMetrics(transport mercure.Transport, registry prometheus.Registerer, logger *slog.Logger) error {
	mt, ok := transport.(TransportMetricsRegisterer)
	if !ok {
		if logger != nil {
			logger.Debug("transport metrics binding skipped — transport does not implement TransportMetricsRegisterer",
				"transport_type", fmt.Sprintf("%T", transport))
		}

		return nil
	}

	if err := mt.RegisterMetricsWith(registry); err != nil {
		return fmt.Errorf("transport metrics registration failed: %w", err)
	}

	return nil
}

type TransportDestructor[T mercure.Transport] struct {
	Transport T
}

func (d TransportDestructor[T]) Destruct() error {
	return d.Transport.Close(caddy.ActiveContext()) //nolint:wrapcheck
}

type (
	subscriptionsKeyType        struct{}
	writeTimeoutKeyType         struct{}
	subscriberListCacheSizeType struct{}
)

var (
	SubscriptionsContextKey           = subscriptionsKeyType{}        //nolint:gochecknoglobals
	WriteTimeoutContextKey            = writeTimeoutKeyType{}         //nolint:gochecknoglobals
	SubscriberListCacheSizeContextKey = subscriberListCacheSizeType{} //nolint:gochecknoglobals
)
