// Package caddy provides a handler for Caddy Server (https://caddyserver.com/)
// allowing to transform any Caddy instance into a Mercure hub.
package caddy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/MicahParks/keyfunc/v3"
	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/caddyconfig/httpcaddyfile"
	"github.com/caddyserver/caddy/v2/modules/caddyevents"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"github.com/dunglas/mercure"
)

const defaultHubURL = "/.well-known/mercure"

var (
	// AllowNoPublish, when true, allows not setting the publisher JWT and
	// disables the publish endpoint. Usually set in the init() function of Go
	// applications publishing programmatically via mercure.Publish() directly.
	//
	// EXPERIMENTAL — surface may change.
	AllowNoPublish bool //nolint:gochecknoglobals

	ErrCompatibility = errors.New("compatibility mode only supports protocol version 7")

	// hubs is a list of registered Mercure hubs, the key is the top-most subroute.
	hubs   = make(map[caddy.Module]*hubInfo) //nolint:gochecknoglobals
	hubsMu sync.Mutex                        //nolint:gochecknoglobals // guards the hubs registry; init-time singleton.
)

type hubInfo struct {
	hub       *mercure.Hub
	transport mercure.Transport
	name      string
}

func init() { //nolint:gochecknoinits
	caddy.RegisterModule(&Mercure{})
	httpcaddyfile.RegisterHandlerDirective("mercure", parseCaddyfile)
	httpcaddyfile.RegisterDirectiveOrder("mercure", "after", "encode")
}

// FindHub finds the Mercure hub configured for the current route.
//
// EXPERIMENTAL — surface may change.
func FindHub(modules []caddy.Module) *mercure.Hub {
	hubsMu.Lock()
	defer hubsMu.Unlock()

	for _, m := range modules {
		if info, ok := hubs[m]; ok {
			return info.hub
		}
	}

	if info := hubs[nil]; info != nil {
		return info.hub
	}

	return nil
}

type JWTConfig struct {
	Key string `json:"key,omitempty"`
	Alg string `json:"alg,omitempty"`
}

type TopicSelectorCacheConfig struct {
	// Deprecated: use Size instead.
	MaxEntriesPerShard int `json:"max_entries_per_shard,omitempty"`
	// Deprecated: no longer used.
	ShardCount uint64 `json:"shard_count,omitempty"`
	// Size is the maximum number of entries in the cache.
	Size int `json:"size,omitempty"`
}

// Mercure implements a Mercure hub as a Caddy module. Mercure is a protocol allowing to push data updates to web browsers and other HTTP clients in a convenient, fast, reliable and battery-efficient way.
//
//nolint:recvcheck // Caddy module convention: CaddyModule() uses a value receiver so caddy.RegisterModule receives a Module value, while every behavior method takes *Mercure to mutate state. Mixing is required by the framework.
type Mercure struct {
	deprecatedTransport //nolint:embeddedstructfieldcheck // build-tagged shim; the real fields live in mercure_deprecated_transport.go.

	// Human-readable name for this hub, used in health check endpoints and metrics.
	Name string `json:"name,omitempty"`

	// Allow subscribers with no valid JWT.
	Anonymous bool `json:"anonymous,omitempty"`

	// Dispatch updates when subscriptions are created or terminated
	Subscriptions bool `json:"subscriptions,omitempty"`

	// Enable the demo.
	Demo bool `json:"demo,omitempty"`

	// Enable the UI.
	UI bool `json:"ui,omitempty"`

	// Maximum duration before closing the connection, defaults to 600s, set to 0 to disable.
	WriteTimeout *caddy.Duration `json:"write_timeout,omitempty"`

	// Maximum dispatch duration of an update, defaults to 5s.
	DispatchTimeout *caddy.Duration `json:"dispatch_timeout,omitempty"`

	// Frequency of the heartbeat, defaults to 40s.
	Heartbeat *caddy.Duration `json:"heartbeat,omitempty"`

	// Maximum duration of a single dispatch, disabled by default. The publish
	// path detaches dispatch from the publisher's request so a mid-publish
	// disconnect does not abort the write; this bounds that detached dispatch so
	// a stalled transport cannot block indefinitely. On timeout the write may
	// still have committed, so the publisher receives 504.
	PublishTimeout *caddy.Duration `json:"publish_timeout,omitempty"`

	// JWT key and signing algorithm to use for publishers.
	PublisherJWT JWTConfig `json:"publisher_jwt,omitzero"`

	// JWK Set URL to use for publishers.
	PublisherJWKSURL string `json:"publisher_jwks_url,omitempty"`

	// JWT key and signing algorithm to use for subscribers.
	SubscriberJWT JWTConfig `json:"subscriber_jwt,omitzero"`

	// JWK Set URL to use for subscribers.
	SubscriberJWKSURL string `json:"subscriber_jwks_url,omitempty"`

	// Bindings requiring a JWT claim to agree with an HTTP header, e.g. the "tenants"
	// claim with the "Tenant-ID" header. Every applicable binding must hold or the
	// request is rejected with a 401.
	ClaimHeaderBindings []ClaimHeaderBindingConfig `json:"require_claim_headers,omitempty"`

	// Origins allowed to publish updates
	PublishOrigins []string `json:"publish_origins,omitempty"`

	// Allowed CORS origins.
	CORSOrigins []string `json:"cors_origins,omitempty"`

	// Deprecated: not used anymore.
	CacheShardSize *int64 `json:"cache_shard_size,omitempty"`

	// Triggers use of topic selector cache and avoidance of select priority queue.
	TopicSelectorCache *TopicSelectorCacheConfig `json:"cache,omitempty"`

	SubscriberListCacheSize *int `json:"subscriber_list_cache_size,omitempty"`

	// Per-subscriber out-channel and pre-ready queue capacity, defaults to 1000.
	// Each connected subscriber commits this many update slots up front, so a
	// large fleet may want it lower to cut memory. 0 keeps the default; a
	// positive value below 16 is rejected.
	SubscriberOutBuffer *int `json:"subscriber_out_buffer,omitempty"`

	// The name of the authorization cookie. Defaults to "mercureAuthorization".
	CookieName string `json:"cookie_name,omitempty"`

	// The version of the Mercure protocol to be backward compatible with (only version 7 is supported)
	ProtocolVersionCompatibility int `json:"protocol_version_compatibility,omitempty"`

	// The transport configuration.
	TransportRaw json.RawMessage `json:"transport,omitempty" caddy:"namespace=http.handlers.mercure inline_key=name"` //nolint:tagalign

	hub    *mercure.Hub
	logger *slog.Logger
	cancel context.CancelFunc
}

// CaddyModule returns the Caddy module information.
func (*Mercure) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "http.handlers.mercure",
		New: func() caddy.Module { return new(Mercure) },
	}
}

type stoppingHandlerFunc func()

func (s stoppingHandlerFunc) Handle(_ context.Context, _ caddy.Event) error {
	s()

	return nil
}

//nolint:wrapcheck
func (m *Mercure) Provision(ctx caddy.Context) (err error) { //nolint:funlen,gocognit,gocyclo,maintidx // wires hub + transport + JWT + JWKS + cache + metrics + lifecycle hooks; splitting fragments the lifecycle without reducing real complexity.
	// caddy/v2 quirk (verified against v2.11.3).
	//
	// caddy.Context embeds context.Context plus a non-exported metricsRegistry
	// field. caddy.Context.WithValue (called below to thread Subscriptions /
	// WriteTimeout / SubscriberListCacheSize) returns a NEW caddy.Context that
	// wraps the embedded context but does NOT copy the metricsRegistry field
	// — see context.go:WithValue, which constructs a fresh Context literal
	// omitting the field. The derived ctx.GetMetricsRegistry() therefore
	// returns nil. Capture the real registry BEFORE any WithValue call.
	//
	// Removing this capture (or moving it after the first WithValue) silently
	// detaches transport metrics: the transport receives a nil registerer,
	// RegisterMetricsWith no-ops, and /metrics scrapes find no transport-side
	// series with no error to surface the problem.
	//
	// The bindTransportMetrics → RegisterMetricsWith path that consumes
	// metricsRegistry is exercised end-to-end against a real
	// *redistransport.RedisTransport in TestBindTransportMetricsRealRedisTransport
	// (caddy/transport_redis_test.go), which asserts mercure_redis_* series
	// land on the registry. That test does NOT exercise this Provision path
	// directly — it constructs the transport in isolation and calls
	// bindTransportMetrics — so a refactor that moved the capture below
	// WithValue would not be caught by it. The defensive capture itself is
	// the regression guard.
	metricsRegistry := ctx.GetMetricsRegistry()
	metrics := mercure.NewPrometheusMetrics(metricsRegistry)

	if err := m.populateJWTConfig(); err != nil {
		return err
	}

	cacheSize := mercure.DefaultTopicSelectorStoreCacheSize

	if m.TopicSelectorCache != nil {
		switch {
		case m.TopicSelectorCache.Size > 0:
			cacheSize = m.TopicSelectorCache.Size
		case m.TopicSelectorCache.MaxEntriesPerShard > 0:
			// Backward compat: convert old per-shard config
			shardCount := m.TopicSelectorCache.ShardCount
			if shardCount == 0 {
				shardCount = 256
			}

			cacheSize = m.TopicSelectorCache.MaxEntriesPerShard * int(shardCount)
		case m.TopicSelectorCache.MaxEntriesPerShard < 0:
			cacheSize = 0
		}
	}

	tss, err := mercure.NewTopicSelectorStore(cacheSize)
	if err != nil {
		return err
	}

	ctx = ctx.WithValue(SubscriptionsContextKey, m.Subscriptions)
	ctx = ctx.WithValue(WriteTimeoutContextKey, m.WriteTimeout)

	if m.SubscriberListCacheSize == nil {
		ctx = ctx.WithValue(SubscriberListCacheSizeContextKey, mercure.DefaultSubscriberListCacheSize)
	} else {
		ctx = ctx.WithValue(SubscriberListCacheSizeContextKey, *m.SubscriberListCacheSize)
	}

	m.logger = slog.New(mercure.NewSlogHandler(ctx.Slogger().Handler()))

	var transport mercure.Transport
	if transport, err = m.createTransportDeprecated(); err != nil {
		return err
	}

	if transport == nil {
		var mod any
		if m.TransportRaw == nil {
			mod, err = ctx.LoadModuleByID("http.handlers.mercure.bolt", nil)
		} else {
			mod, err = ctx.LoadModule(m, "TransportRaw")
		}

		if err != nil {
			return err
		}

		transport = mod.(Transport).GetTransport()
	}

	if err := bindTransportMetrics(transport, metricsRegistry, m.logger); err != nil {
		return err
	}

	opts := []mercure.Option{
		mercure.WithLogger(m.logger),
		mercure.WithTopicSelectorStore(tss),
		mercure.WithTransport(transport),
		mercure.WithMetrics(metrics),
		mercure.WithCookieName(m.CookieName),
	}

	if m.logger.Enabled(ctx, slog.LevelDebug) {
		opts = append(opts, mercure.WithDebug())
	}

	if m.PublisherJWKSURL != "" {
		k, err := newJWKSetKeyfunc(ctx, m.PublisherJWKSURL)
		if err != nil {
			return fmt.Errorf("failed to retrieve publisher JWK Set: %w", err)
		}

		opts = append(opts, mercure.WithPublisherJWTKeyFunc(k.Keyfunc))
	} else if m.PublisherJWT.Key != "" {
		opts = append(opts, mercure.WithPublisherJWT([]byte(m.PublisherJWT.Key), m.PublisherJWT.Alg))
	}

	if m.SubscriberJWKSURL != "" {
		k, err := newJWKSetKeyfunc(ctx, m.SubscriberJWKSURL)
		if err != nil {
			return fmt.Errorf("failed to retrieve subscriber JWK Set: %w", err)
		}

		opts = append(opts, mercure.WithSubscriberJWTKeyFunc(k.Keyfunc))
	} else if m.SubscriberJWT.Key != "" {
		opts = append(opts, mercure.WithSubscriberJWT([]byte(m.SubscriberJWT.Key), m.SubscriberJWT.Alg))
	}

	bindings, err := m.claimHeaderBindings()
	if err != nil {
		return err
	}

	if len(bindings) > 0 {
		opts = append(opts, mercure.WithClaimHeaderBindings(bindings...))
	}

	if m.Anonymous {
		opts = append(opts, mercure.WithAnonymous())
	}

	if m.Demo {
		opts = append(opts, mercure.WithDemo())
	}

	if m.UI {
		opts = append(opts, mercure.WithUI())
	}

	if m.Subscriptions {
		opts = append(opts, mercure.WithSubscriptions())
	}

	if d := m.WriteTimeout; d != nil {
		opts = append(opts, mercure.WithWriteTimeout(time.Duration(*d)))
	}

	if d := m.DispatchTimeout; d != nil {
		opts = append(opts, mercure.WithDispatchTimeout(time.Duration(*d)))
	}

	if d := m.Heartbeat; d != nil {
		opts = append(opts, mercure.WithHeartbeat(time.Duration(*d)))
	}

	if d := m.PublishTimeout; d != nil {
		opts = append(opts, mercure.WithPublishTimeout(time.Duration(*d)))
	}

	if m.SubscriberOutBuffer != nil {
		opts = append(opts, mercure.WithSubscriberOutBuffer(*m.SubscriberOutBuffer))
	}

	if len(m.PublishOrigins) > 0 {
		opts = append(opts, mercure.WithPublishOrigins(m.PublishOrigins))
	}

	if len(m.CORSOrigins) > 0 {
		opts = append(opts, mercure.WithCORSOrigins(m.CORSOrigins))
	}

	if m.ProtocolVersionCompatibility != 0 {
		opts = append(opts, mercure.WithProtocolVersionCompatibility(m.ProtocolVersionCompatibility))
	}

	eventApp, err := ctx.App("events")
	if err != nil {
		return err
	}

	var c context.Context

	c, m.cancel = context.WithCancel(ctx)
	if err := eventApp.(*caddyevents.App).On("stopping", stoppingHandlerFunc(m.cancel)); err != nil {
		return err
	}

	h, err := mercure.NewHub(c, opts...)
	if err != nil {
		return err
	}

	m.hub = h

	name := m.Name
	if name == "" {
		name = "default"
	}

	info := &hubInfo{
		hub:       h,
		transport: transport,
		name:      name,
	}

	var found bool

	hubsMu.Lock()
	defer hubsMu.Unlock()

	for _, mod := range ctx.Modules() {
		if _, ok := mod.(*caddyhttp.Subroute); ok {
			hubs[mod] = info
			found = true

			break
		}
	}

	if !found {
		hubs[nil] = info
	}

	return nil
}

func (m *Mercure) Cleanup() error {
	if m.cancel != nil {
		m.cancel()
	}

	hubsMu.Lock()
	defer hubsMu.Unlock()

	for k, info := range hubs {
		if info.hub == m.hub {
			delete(hubs, k)
		}
	}

	return m.cleanupTransportDeprecated()
}

func (m *Mercure) ServeHTTP(w http.ResponseWriter, r *http.Request, next caddyhttp.Handler) error {
	if !strings.HasPrefix(r.URL.Path, defaultHubURL) {
		return next.ServeHTTP(w, r) //nolint:wrapcheck
	}

	m.hub.ServeHTTP(w, r)

	return nil
}

// UnmarshalCaddyfile sets up the handler from Caddyfile tokens.
//
//nolint:wrapcheck
func (m *Mercure) UnmarshalCaddyfile(d *caddyfile.Dispenser) (err error) { //nolint:maintidx,funlen,gocognit,gocyclo
	for d.Next() {
		for d.NextBlock(0) {
			switch d.Val() {
			case "name":
				if !d.NextArg() {
					return d.ArgErr()
				}

				m.Name = d.Val()

			case "anonymous":
				m.Anonymous = true

			case "demo":
				m.Demo = true

			case "ui":
				m.UI = true

			case "subscriptions":
				m.Subscriptions = true

			case "write_timeout":
				if m.WriteTimeout, err = parseDurationParameter(d); err != nil {
					return err
				}

			case "dispatch_timeout":
				if m.DispatchTimeout, err = parseDurationParameter(d); err != nil {
					return err
				}

			case "heartbeat":
				if m.Heartbeat, err = parseDurationParameter(d); err != nil {
					return err
				}

			case "publish_timeout":
				if m.PublishTimeout, err = parseDurationParameter(d); err != nil {
					return err
				}

			case "publisher_jwks_url":
				if !d.NextArg() {
					return d.ArgErr()
				}

				m.PublisherJWKSURL = d.Val()

			case "publisher_jwt":
				if !d.NextArg() {
					return d.ArgErr()
				}

				m.PublisherJWT.Key = d.Val()
				if d.NextArg() {
					m.PublisherJWT.Alg = d.Val()
				}

			case "subscriber_jwks_url":
				if !d.NextArg() {
					return d.ArgErr()
				}

				m.SubscriberJWKSURL = d.Val()

			case "subscriber_jwt":
				if !d.NextArg() {
					return d.ArgErr()
				}

				m.SubscriberJWT.Key = d.Val()
				if d.NextArg() {
					m.SubscriberJWT.Alg = d.Val()
				}

			case "publish_origins":
				m.PublishOrigins = d.RemainingArgs()
				if len(m.PublishOrigins) == 0 {
					return d.ArgErr()
				}

			case "cors_origins":
				m.CORSOrigins = d.RemainingArgs()
				if len(m.CORSOrigins) == 0 {
					return d.ArgErr()
				}

			case "transport":
				if !d.NextArg() {
					return d.ArgErr()
				}

				name := d.Val()
				modID := "http.handlers.mercure." + name

				unm, err := caddyfile.UnmarshalModule(d, modID)
				if err != nil {
					return err
				}

				t, ok := unm.(Transport)
				if !ok {
					return d.Errf(`module %s (%T) is not a supported transport implementation (requires "github.com/dunglas/mercure/caddy".Transport)`, modID, unm)
				}

				m.TransportRaw = caddyconfig.JSONModuleObject(t, "name", name, nil)

			case "transport_url":
				if !d.NextArg() {
					return d.ArgErr()
				}

				m.assignDeprecatedTransportURL(d.Val())

			case "topic_selector_cache":
				if !d.NextArg() {
					return d.ArgErr()
				}

				size, err := strconv.Atoi(d.Val())
				if err != nil {
					return d.WrapErr(err)
				}

				m.TopicSelectorCache = &TopicSelectorCacheConfig{Size: size}
			case "subscriber_list_cache_size":
				if !d.NextArg() {
					return d.ArgErr()
				}

				size, err := strconv.Atoi(d.Val())
				if err != nil {
					// Overflow on this platform (size > math.MaxInt) lands as
					// strconv.ErrRange; wrap with the typed sentinel so the
					// Caddyfile-overflow test (errors.Is) keeps its contract.
					if errors.Is(err, strconv.ErrRange) {
						return d.WrapErr(fmt.Errorf("%w: %s", ErrSubscriberListCacheSizeOverflow, d.Val()))
					}

					return d.WrapErr(err)
				}

				if size < 0 {
					return d.Errf("subscriber_list_cache_size must be >= 0, got %d", size)
				}

				m.SubscriberListCacheSize = &size

			case "subscriber_out_buffer":
				if !d.NextArg() {
					return d.ArgErr()
				}

				size, err := strconv.Atoi(d.Val())
				if err != nil {
					return d.WrapErr(err)
				}

				if size != 0 && size < mercure.MinSubscriberOutBuffer {
					return d.Errf("subscriber_out_buffer must be 0 (use the default) or at least %d, got %d", mercure.MinSubscriberOutBuffer, size)
				}

				m.SubscriberOutBuffer = &size

			case "cookie_name":
				if !d.NextArg() {
					return d.ArgErr()
				}

				m.CookieName = d.Val()

			case "protocol_version_compatibility":
				if !d.NextArg() {
					return d.ArgErr()
				}

				v, err := strconv.Atoi(d.Val())
				if err != nil {
					return d.WrapErr(err)
				}

				if v != 7 {
					return d.WrapErr(ErrCompatibility)
				}

				m.ProtocolVersionCompatibility = v

			case "require_claim_header":
				cfg, err := parseRequireClaimHeader(d)
				if err != nil {
					return err
				}

				m.ClaimHeaderBindings = append(m.ClaimHeaderBindings, cfg)

			default:
				// Without this, a misspelled directive is silently ignored — which for a
				// security directive such as require_claim_header means failing open.
				return d.Errf("unrecognized mercure subdirective %q", d.Val())
			}
		}
	}

	m.assignDeprecatedTransportURLForEnv()

	return nil
}

func (m *Mercure) populateJWTConfig() error {
	repl := caddy.NewReplacer()

	if m.PublisherJWKSURL == "" {
		m.PublisherJWT.Key = repl.ReplaceKnown(m.PublisherJWT.Key, "")

		if m.PublisherJWT.Key != "" {
			m.PublisherJWT.Alg = repl.ReplaceKnown(m.PublisherJWT.Alg, "HS256")
			if m.PublisherJWT.Alg == "" {
				m.PublisherJWT.Alg = "HS256"
			}
		} else if !AllowNoPublish {
			return errors.New("a JWT key or the URL of a JWK Set for publishers must be provided") //nolint:err113
		}
	}

	if m.SubscriberJWKSURL == "" {
		m.SubscriberJWT.Key = repl.ReplaceKnown(m.SubscriberJWT.Key, "")
		m.SubscriberJWT.Alg = repl.ReplaceKnown(m.SubscriberJWT.Alg, "HS256")

		if m.SubscriberJWT.Key == "" {
			if !m.Anonymous {
				return errors.New("a JWT key or the URL of a JWK Set for subscribers must be provided") //nolint:err113
			}
		}

		if m.SubscriberJWT.Alg == "" {
			m.SubscriberJWT.Alg = "HS256"
		}
	}

	return nil
}

// maxJWKSetFileBytes caps the size of a `file://`-loaded JWK Set. JWKS files
// are typically <10 KB; 1 MiB leaves room for unusually large key bundles
// without letting a misconfigured directive read a giant or special file.
const maxJWKSetFileBytes = 1 << 20

// jwksHTTPFetchTimeout bounds the synchronous first fetch of an HTTP(S) JWK
// Set at provision time. Without it the fail-fast switch below would let a
// slow or blackholing JWKS endpoint hang Caddy config load indefinitely
// (keyfunc's default is 1 minute, applied per URL).
const jwksHTTPFetchTimeout = 10 * time.Second

// JWK Set loading sentinels. Wrapped with %w so callers/tests can match via
// errors.Is and so err113 is satisfied without inline suppression.
var (
	errJWKSetFileHost       = errors.New(`file:// JWK Set URL host must be empty (file:///path) or "localhost"; literal IPs like "127.0.0.1"/"[::1]" are not accepted, omit the host entirely`)
	errJWKSetFileNotRegular = errors.New("JWK Set path is not a regular file")
	errJWKSetFileTooLarge   = errors.New("JWK Set file exceeds size cap")
)

// newJWKSetKeyfunc builds a Keyfunc from a JWK Set URL.
//
// file:// URLs point to a local JSON file containing a JWK Set; the file is
// read once at provision time, so rotating the keys requires a Caddy config
// reload. HTTP(S) URLs are fetched once at provision via keyfunc, then
// refreshed by its background goroutine.
//
// fail-fast: keyfunc's NewDefaultCtx defaults NoErrorReturnFirstHTTPReq=true,
// meaning a first-fetch failure (typo'd URL, unreachable host, wrong scheme)
// returns a usable-but-EMPTY key set and the hub boots — then rejects every
// JWT because no keys loaded. That "starts green, auth dead" mode is a nasty
// operator footgun. NewDefaultOverrideCtx with NoErrorReturnFirstHTTPReq=false
// surfaces the fetch error at Provision time so a broken JWKS URL fails the
// Caddy config load loudly instead.
//
//nolint:ireturn
func newJWKSetKeyfunc(ctx context.Context, rawURL string) (keyfunc.Keyfunc, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("invalid JWK Set URL %q: %w", rawURL, err)
	}

	if u.Scheme == "file" {
		return loadFileJWKSet(u)
	}

	failFast := false

	// Pass the long-lived ctx (keyfunc binds the background refresh goroutine
	// to it); HTTPTimeout — not a short ctx — bounds the synchronous first
	// fetch so a slow/blackholing JWKS endpoint can't hang Provision. A short
	// ctx here would also kill the refresh goroutine after the timeout.
	k, err := keyfunc.NewDefaultOverrideCtx(ctx, []string{rawURL}, keyfunc.Override{
		NoErrorReturnFirstHTTPReq: &failFast,
		HTTPTimeout:               jwksHTTPFetchTimeout,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to load JWK Set from %q: %w", rawURL, err)
	}

	return k, nil
}

// loadFileJWKSet reads a file:// JWK Set URL into a keyfunc.Keyfunc. The host
// must be empty or "localhost"; the path is filepath.Clean'd before open to
// normalise away "..". Only regular files are accepted — character / block
// devices, FIFOs, and sockets are rejected via the post-open Fstat check.
//
// The open is O_NONBLOCK: a blocking open(2) on a FIFO with no writer hangs
// forever (POSIX: the read end blocks until a writer appears), which would
// wedge Caddy's config load with no timeout to bound it. O_NONBLOCK makes the
// FIFO open return immediately so the Fstat IsRegular check below can reject
// it. On a regular file O_NONBLOCK is a no-op (regular-file reads never return
// EAGAIN), so the subsequent io.ReadAll behaves normally.
//
// Open-then-Fstat (rather than Stat-then-Open) is also TOCTOU-safe: the
// IsRegular check runs against the fd this function actually opened, not a
// path that could be swapped between a Stat and a later Open.
//
// Body size is then bounded with io.LimitReader + a post-read len check, so
// a regular file that grows between Fstat and Read cannot exceed the cap.
//
//nolint:ireturn // keyfunc.Keyfunc is the interface contract.
func loadFileJWKSet(u *url.URL) (keyfunc.Keyfunc, error) {
	// Host is restricted to empty or "localhost" (case-insensitive). Literal
	// loopback IPs (`127.0.0.1`, `[::1]`) are NOT accepted: file:// URLs
	// resolve the path component on the local filesystem regardless of
	// host, and accepting literal IPs invites confusion with HTTP URL
	// semantics. The error message enumerates rejected-but-loopback-equivalent
	// forms so operators don't waste time on the canonical form.
	switch strings.ToLower(u.Host) {
	case "", "localhost":
		// accepted
	default:
		return nil, fmt.Errorf("%w: got %q", errJWKSetFileHost, u.Host)
	}

	path := filepath.Clean(u.Path)

	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		// "failed to read JWK Set file" wording preserves the operator
		// log-grep contract from the pre-hardening shape. The "(open)"
		// qualifier disambiguates which step failed for log triage without
		// breaking existing alerts that grep on the leading phrase.
		return nil, fmt.Errorf("failed to read JWK Set file %q (open): %w", path, err)
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to read JWK Set file %q (fstat): %w", path, err)
	}

	if !fi.Mode().IsRegular() {
		return nil, fmt.Errorf("%w: %q (mode %s)", errJWKSetFileNotRegular, path, fi.Mode())
	}

	if fi.Size() > maxJWKSetFileBytes {
		return nil, fmt.Errorf("%w: %q is %d bytes, exceeds %d", errJWKSetFileTooLarge, path, fi.Size(), maxJWKSetFileBytes)
	}

	// LimitReader at cap+1 so reads of exactly cap bytes succeed and a file
	// that grows past cap between Fstat and Read trips the post-check below.
	b, err := io.ReadAll(io.LimitReader(f, maxJWKSetFileBytes+1))
	if err != nil {
		return nil, fmt.Errorf("failed to read JWK Set file %q: %w", path, err)
	}

	if len(b) > maxJWKSetFileBytes {
		return nil, fmt.Errorf("%w: %q grew past %d during read", errJWKSetFileTooLarge, path, maxJWKSetFileBytes)
	}

	k, err := keyfunc.NewJWKSetJSON(b)
	if err != nil {
		return nil, fmt.Errorf("failed to parse JWK Set file %q: %w", path, err)
	}

	return k, nil
}

// parseCaddyfile unmarshals tokens from h into a new Middleware.
func parseCaddyfile(h httpcaddyfile.Helper) (caddyhttp.MiddlewareHandler, error) { //nolint:ireturn
	m := new(Mercure)

	return m, m.UnmarshalCaddyfile(h.Dispenser)
}

// parseDurationParameter returns errors via Dispenser.ArgErr / Dispenser.WrapErr —
// those ARE the framework's wrappers, so re-wrapping with %w would double-wrap.
//
//nolint:wrapcheck // see comment above.
func parseDurationParameter(d *caddyfile.Dispenser) (*caddy.Duration, error) {
	if !d.NextArg() {
		return nil, d.ArgErr()
	}

	du, err := caddy.ParseDuration(d.Val())
	if err != nil {
		return nil, d.WrapErr(err)
	}

	cd := caddy.Duration(du)

	return &cd, nil
}

// Interface guards.
var (
	_ caddy.Provisioner           = (*Mercure)(nil)
	_ caddy.CleanerUpper          = (*Mercure)(nil)
	_ caddyhttp.MiddlewareHandler = (*Mercure)(nil)
	_ caddyfile.Unmarshaler       = (*Mercure)(nil)
)
