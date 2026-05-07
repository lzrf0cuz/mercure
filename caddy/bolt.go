package caddy

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"path/filepath"
	"strconv"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/dunglas/mercure"
)

func init() { //nolint:gochecknoinits
	caddy.RegisterModule(&Bolt{})
}

// Bolt is the Caddy module wrapping the upstream BoltTransport.
//
//nolint:recvcheck // Caddy module convention: CaddyModule() uses a value receiver so caddy.RegisterModule can register a Module value, while behavior methods (Provision, Cleanup, UnmarshalCaddyfile, GetTransport) take *Bolt to mutate state.
type Bolt struct {
	Path             string  `json:"path,omitempty"`
	BucketName       string  `json:"bucket_name,omitempty"`
	Size             uint64  `json:"size,omitempty"`
	CleanupFrequency float64 `json:"cleanup_frequency,omitempty"`

	transport    *mercure.BoltTransport
	transportKey string
}

// CaddyModule returns the Caddy module information.
func (*Bolt) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "http.handlers.mercure.bolt",
		New: func() caddy.Module { return new(Bolt) },
	}
}

func (b *Bolt) GetTransport() mercure.Transport { //nolint:ireturn
	return b.transport
}

// Provision provisions b's configuration.
func (b *Bolt) Provision(ctx caddy.Context) error {
	if b.Path == "" {
		b.Path = filepath.Join(caddy.AppDataDir(), "mercure.db")
	}

	// Normalize Path before gob-encoding the struct into the pool key. Without
	// normalization, two configs pointing at the same file via different
	// spellings (relative "./mercure.db" vs absolute "/abs/.../mercure.db")
	// produce different pool keys, leading the second Provision into a bbolt
	// file-lock collision instead of sharing the already-open transport.
	absPath, err := filepath.Abs(b.Path)
	if err != nil {
		return fmt.Errorf("bolt transport: resolve absolute path: %w", err)
	}

	b.Path = absPath

	var key bytes.Buffer
	if err := gob.NewEncoder(&key).Encode(b); err != nil {
		return fmt.Errorf("bolt transport: encode pool key: %w", err)
	}

	b.transportKey = key.String()

	cacheSize, ok := ctx.Value(SubscriberListCacheSizeContextKey).(int)
	if !ok {
		return fmt.Errorf("bolt transport: %w (key=%T)", ErrSubscriberListCacheSizeMissing, SubscriberListCacheSizeContextKey)
	}

	destructor, _, err := TransportUsagePool.LoadOrNew(b.transportKey, func() (caddy.Destructor, error) {
		t, err := mercure.NewBoltTransport(
			mercure.NewSubscriberList(cacheSize),
			ctx.Slogger(),
			b.Path,
			b.BucketName,
			b.Size,
			b.CleanupFrequency,
		)
		if err != nil {
			return nil, fmt.Errorf("new bolt transport: %w", err)
		}

		return TransportDestructor[*mercure.BoltTransport]{Transport: t}, nil
	})
	if err != nil {
		return fmt.Errorf("bolt transport pool: %w", err)
	}

	td, ok := destructor.(TransportDestructor[*mercure.BoltTransport])
	if !ok {
		return fmt.Errorf("bolt transport: %w: pool returned %T, expected TransportDestructor[*mercure.BoltTransport]", errTransportPoolDestructorMismatch, destructor)
	}

	b.transport = td.Transport

	return nil
}

func (b *Bolt) Cleanup() error {
	if _, err := TransportUsagePool.Delete(b.transportKey); err != nil {
		return fmt.Errorf("bolt transport cleanup: %w", err)
	}

	return nil
}

// UnmarshalCaddyfile sets up the handler from Caddyfile tokens.
//
//nolint:wrapcheck
func (b *Bolt) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	for d.Next() {
		for d.NextBlock(0) {
			switch d.Val() {
			case "path":
				if !d.NextArg() {
					return d.ArgErr()
				}

				b.Path = d.Val()

			case "bucket_name":
				if !d.NextArg() {
					return d.ArgErr()
				}

				b.BucketName = d.Val()

			case "cleanup_frequency":
				if !d.NextArg() {
					return d.ArgErr()
				}

				f, e := strconv.ParseFloat(d.Val(), 64)
				if e != nil {
					return d.WrapErr(e)
				}

				b.CleanupFrequency = f

			case "size":
				if !d.NextArg() {
					return d.ArgErr()
				}

				s, e := strconv.ParseUint(d.Val(), 10, 64)
				if e != nil {
					return d.WrapErr(e)
				}

				b.Size = s
			}
		}
	}

	return nil
}

var (
	_ caddy.Provisioner     = (*Bolt)(nil)
	_ caddy.CleanerUpper    = (*Bolt)(nil)
	_ caddyfile.Unmarshaler = (*Bolt)(nil)
)
