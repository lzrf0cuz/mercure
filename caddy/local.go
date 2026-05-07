package caddy

import (
	"fmt"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/dunglas/mercure"
)

type localTransportKeyStruct struct{}

var localTransportKey = localTransportKeyStruct{} //nolint:gochecknoglobals

func init() { //nolint:gochecknoinits
	caddy.RegisterModule(&Local{})
}

// Local is the Caddy module wrapping the upstream LocalTransport.
//
//nolint:recvcheck // Caddy module convention: CaddyModule() uses a value receiver so caddy.RegisterModule can register a Module value, while behavior methods take *Local to mutate state.
type Local struct {
	transport *mercure.LocalTransport
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
	cacheSize, ok := ctx.Value(SubscriberListCacheSizeContextKey).(int)
	if !ok {
		return fmt.Errorf("local transport: %w (key=%T)", ErrSubscriberListCacheSizeMissing, SubscriberListCacheSizeContextKey)
	}

	destructor, _, err := TransportUsagePool.LoadOrNew(localTransportKey, func() (caddy.Destructor, error) {
		return TransportDestructor[*mercure.LocalTransport]{
			Transport: mercure.NewLocalTransport(mercure.NewSubscriberList(cacheSize)),
		}, nil
	})
	if err != nil {
		return fmt.Errorf("local transport pool: %w", err)
	}

	td, ok := destructor.(TransportDestructor[*mercure.LocalTransport])
	if !ok {
		return fmt.Errorf("local transport: %w: pool returned %T, expected TransportDestructor[*mercure.LocalTransport]", errTransportPoolDestructorMismatch, destructor)
	}

	l.transport = td.Transport

	return nil
}

//nolint:wrapcheck
func (l *Local) Cleanup() error {
	_, err := TransportUsagePool.Delete(localTransportKey)

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
