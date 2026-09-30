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

// errTransportOptionsChanged refuses a pooled transport whose options differ
// from the ones it was created with: a reload keeps the pooled transport.
var errTransportOptionsChanged = errors.New("already open with different options")

type Transport interface {
	GetTransport() mercure.Transport
}

// TransportMetricsRegisterer is the optional interface a transport implements
// to register its Prometheus collectors with the metrics registry of the
// config that uses it. A transport module's Provision cannot do this itself:
// the caddy.Context it receives is derived with WithValue, which drops the
// registry, so GetMetricsRegistry returns nil there. The mercure module
// captures the registry before deriving that context and passes it here once
// the transport is loaded.
//
// Implementer contract:
//
//   - The registry is Caddy's per-config registry, a pedantic one
//     (prometheus.NewPedanticRegistry): a collector must describe exactly
//     the metrics it collects.
//   - Caddy builds a new registry for each config load and reuses a pooled
//     transport whose configuration did not change, so a reload calls this
//     again, on the same transport, with the new registry. Each distinct
//     registry must get the collectors; a repeat call with a registry already
//     bound is a no-op. Later calls must not change the collectors the
//     transport emits to: every registry gets the same ones.
//   - The call happens during Provision, before the hub serves requests.
//   - A non-nil error fails Provision, which fails the configuration load and
//     discards that config's registry, so an implementer need not roll back
//     what a failed call had already registered.
//   - A call can happen for a config whose Provision later fails, and whose
//     registry is then discarded. Do not retain the registry beyond
//     registering on it.
type TransportMetricsRegisterer interface {
	RegisterMetricsWith(registerer prometheus.Registerer) error
}

// bindTransportMetrics passes registry to transport when it implements
// TransportMetricsRegisterer, and is a no-op otherwise (Bolt, Local).
func bindTransportMetrics(transport mercure.Transport, registry prometheus.Registerer, logger *slog.Logger) error {
	mt, ok := transport.(TransportMetricsRegisterer)
	if !ok {
		if logger != nil {
			logger.Debug("transport metrics binding skipped: transport does not implement TransportMetricsRegisterer",
				slog.String("transport_type", fmt.Sprintf("%T", transport)))
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
	hubNameKeyType              struct{}
)

var (
	SubscriptionsContextKey           = subscriptionsKeyType{}        //nolint:gochecknoglobals
	WriteTimeoutContextKey            = writeTimeoutKeyType{}         //nolint:gochecknoglobals
	SubscriberListCacheSizeContextKey = subscriberListCacheSizeType{} //nolint:gochecknoglobals
	// HubNameContextKey carries the hub name: pooled transports must key by it
	// so that differently named hubs never share subscribers or history.
	HubNameContextKey = hubNameKeyType{} //nolint:gochecknoglobals
)

func hubName(ctx caddy.Context) string {
	name, _ := ctx.Value(HubNameContextKey).(string)

	return name
}
