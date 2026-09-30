package caddy

import (
	"fmt"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/dunglas/mercure"
)

type localTransportKey struct {
	hub string
}

// localDestructor keeps the subscriber list cache size the pooled transport
// was created with.
type localDestructor struct {
	TransportDestructor[*mercure.LocalTransport]

	subscriberListCacheSize int
}

func init() { //nolint:gochecknoinits
	caddy.RegisterModule(&Local{})
}

type Local struct {
	transport *mercure.LocalTransport
	key       localTransportKey
}

// CaddyModule returns the Caddy module information.
func (*Local) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "http.handlers.mercure.local",
		New: func() caddy.Module { return new(Local) },
	}
}

func (l *Local) GetTransport() mercure.Transport { //nolint:ireturn
	return l.transport
}

// Provision provisions l's configuration.
func (l *Local) Provision(ctx caddy.Context) error {
	l.key = localTransportKey{hubName(ctx)}
	cacheSize := ctx.Value(SubscriberListCacheSizeContextKey).(int)

	destructor, loaded, _ := TransportUsagePool.LoadOrNew(l.key, func() (caddy.Destructor, error) {
		return localDestructor{
			TransportDestructor[*mercure.LocalTransport]{Transport: mercure.NewLocalTransport(mercure.NewSubscriberList(cacheSize))},
			cacheSize,
		}, nil
	})

	// On failure, Caddy calls Cleanup, which releases the pooled transport.
	pooled := destructor.(localDestructor)
	if loaded && pooled.subscriberListCacheSize != cacheSize {
		return fmt.Errorf("the local transport of hub %q is %w (subscriber_list_cache_size %d -> %d): restart Caddy to change them; a reload keeps the running transport",
			l.key.hub, errTransportOptionsChanged, pooled.subscriberListCacheSize, cacheSize)
	}

	l.transport = pooled.Transport

	return nil
}

//nolint:wrapcheck
func (l *Local) Cleanup() error {
	_, err := TransportUsagePool.Delete(l.key)

	return err
}

// UnmarshalCaddyfile sets up the handler from Caddyfile tokens.
func (l *Local) UnmarshalCaddyfile(_ *caddyfile.Dispenser) error {
	return nil
}

var (
	_ caddy.Provisioner     = (*Local)(nil)
	_ caddy.CleanerUpper    = (*Local)(nil)
	_ caddyfile.Unmarshaler = (*Local)(nil)
)
