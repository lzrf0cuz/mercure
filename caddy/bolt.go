package caddy

import (
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/dunglas/mercure"
	bolterrors "go.etcd.io/bbolt/errors"
)

func init() { //nolint:gochecknoinits
	caddy.RegisterModule(&Bolt{})
}

// boltTransportKey identifies a pooled Bolt transport by its database file,
// not by its options: Caddy provisions a reloaded config before it cleans up
// the old one, so a reload must reuse the transport that holds the file.
type boltTransportKey struct {
	path string
	hub  string
}

// boltOptions are the options a pooled Bolt transport was created with.
type boltOptions struct {
	bucketName              string
	size                    uint64
	cleanupFrequency        float64
	subscriberListCacheSize int
}

type boltDestructor struct {
	TransportDestructor[*mercure.BoltTransport]

	options boltOptions
}

type Bolt struct {
	Path             string   `json:"path,omitempty"`
	BucketName       string   `json:"bucket_name,omitempty"`
	Size             uint64   `json:"size,omitempty"`
	CleanupFrequency *float64 `json:"cleanup_frequency,omitempty"`

	transport    *mercure.BoltTransport
	transportKey boltTransportKey
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
//
//nolint:wrapcheck
func (b *Bolt) Provision(ctx caddy.Context) error {
	if b.Path == "" {
		b.Path = filepath.Join(caddy.AppDataDir(), "mercure.db")
	}

	// Abs also cleans the path, so "./x", "x" and "/cwd/x" share a key.
	path, err := filepath.Abs(b.Path)
	if err != nil {
		return err
	}

	b.transportKey = boltTransportKey{path, hubName(ctx)}

	bucketName := b.BucketName
	if bucketName == "" {
		bucketName = mercure.BoltDefaultBucketName
	}

	options := boltOptions{bucketName, b.Size, b.cleanupFrequency(), ctx.Value(SubscriberListCacheSizeContextKey).(int)}

	destructor, loaded, err := TransportUsagePool.LoadOrNew(b.transportKey, func() (caddy.Destructor, error) {
		t, err := mercure.NewBoltTransport(
			mercure.NewSubscriberList(options.subscriberListCacheSize),
			ctx.Slogger(),
			b.Path,
			options.bucketName,
			options.size,
			options.cleanupFrequency,
		)
		if errors.Is(err, bolterrors.ErrTimeout) {
			return nil, fmt.Errorf("%q is already open: give each hub its own path, and restart Caddy to rename a hub (a reload keeps the open database): %w", b.Path, err)
		}

		if err != nil {
			return nil, err
		}

		return boltDestructor{TransportDestructor[*mercure.BoltTransport]{Transport: t}, options}, nil
	})
	if err != nil {
		return err
	}

	// On failure, Caddy calls Cleanup, which releases the pooled transport.
	pooled := destructor.(boltDestructor)
	if diffs := pooled.options.diff(options); loaded && len(diffs) > 0 {
		return fmt.Errorf("%q is %w (%s): restart Caddy to change them; a reload keeps the open database", b.Path, errTransportOptionsChanged, strings.Join(diffs, ", "))
	}

	b.transport = pooled.Transport

	return nil
}

// diff lists the options that differ between o, the options the open
// transport was created with, and n.
func (o boltOptions) diff(n boltOptions) []string {
	var diffs []string

	if o.bucketName != n.bucketName {
		diffs = append(diffs, fmt.Sprintf("bucket_name %q -> %q", o.bucketName, n.bucketName))
	}

	if o.size != n.size {
		diffs = append(diffs, fmt.Sprintf("size %d -> %d", o.size, n.size))
	}

	if o.cleanupFrequency != n.cleanupFrequency {
		diffs = append(diffs, fmt.Sprintf("cleanup_frequency %v -> %v", o.cleanupFrequency, n.cleanupFrequency))
	}

	if o.subscriberListCacheSize != n.subscriberListCacheSize {
		diffs = append(diffs, fmt.Sprintf("subscriber_list_cache_size %d -> %d", o.subscriberListCacheSize, n.subscriberListCacheSize))
	}

	return diffs
}

//nolint:wrapcheck
func (b *Bolt) Cleanup() error {
	_, err := TransportUsagePool.Delete(b.transportKey)

	return err
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

				b.CleanupFrequency = &f

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

func (b *Bolt) cleanupFrequency() float64 {
	if b.CleanupFrequency == nil {
		return mercure.BoltDefaultCleanupFrequency
	}

	return *b.CleanupFrequency
}

var (
	_ caddy.Provisioner     = (*Bolt)(nil)
	_ caddy.CleanerUpper    = (*Bolt)(nil)
	_ caddyfile.Unmarshaler = (*Bolt)(nil)
)
