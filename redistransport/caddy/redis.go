// Package caddy provides a Caddy module wrapper for the Redis transport.
//
// It registers a Caddy module at "http.handlers.mercure.redis" that can be
// selected via the Caddyfile directive:
//
//	mercure {
//	    transport redis {
//	        url redis://localhost:6379
//	    }
//	}
package caddy

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	caddyv2 "github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/dunglas/mercure"
	mercurecaddy "github.com/dunglas/mercure/caddy"
	"github.com/lzrf0cuz/mercure/redistransport"
	"github.com/redis/go-redis/v9"
)

var (
	// errURLAndAddresses is returned when the Caddyfile sets both `url` and
	// `addresses`, which configure the same thing two ways.
	errURLAndAddresses = errors.New("redis transport: cannot set both `url` and `addresses` directives — pick one")

	// errTLSCANoPEM is returned when tls_ca_file points to a file that
	// contains no recognizable PEM CERTIFICATE blocks.
	errTLSCANoPEM = errors.New("redis transport: tls_ca_file contains no valid PEM certificates")

	// errTLSClientAuthIncomplete is returned when only one of the
	// tls_client_auth cert and key paths is set. A Caddyfile reaches this
	// through an empty quoted argument, for example an unset `"{$VAR}"`. It
	// also arises from direct JSON config. resolveDirective only catches a
	// non-empty value that resolves to empty.
	errTLSClientAuthIncomplete = errors.New("redis transport: tls_client_auth requires both cert and key files")

	// errPlaceholderUnresolved is returned when a non-empty directive value
	// resolves to an empty string after placeholder substitution (typically an
	// unset {env.VAR}), rather than silently dropping the setting.
	errPlaceholderUnresolved = errors.New("redis transport: directive resolved to empty after placeholder substitution (unset {env.VAR}?)")

	// errNegativeOptionValue is returned when a numeric directive, or a duration
	// directive other than zombie_gc_interval and clock_skew_margin, is negative.
	// A typo then fails instead of falling back to the default. max_retries also
	// accepts -1, which disables retries.
	errNegativeOptionValue = errors.New("redis transport: option must be >= 0")
)

// Caddyfile directive names used in both the parseDirective switch and the
// validation errors. Shared constants keep the two from drifting apart.
const (
	directiveTLS                         = "tls"
	directiveTLSInsecureSkipVerify       = "tls_insecure_skip_verify"
	directiveMaxLength                   = "max_length"
	directiveDispatchShards              = "dispatch_shards"
	directiveSubscriptionsMaxSubscribers = "subscriptions_max_subscribers"
	directiveSubscriberMaxCount          = "subscriber_max_count"
	directiveSubscriberAdmissionTimeout  = "subscriber_admission_timeout"
	directiveSubscriberRegTimeout        = "subscriber_registration_timeout"
	directiveSubscriberRetryAfter        = "subscriber_retry_after"
	directiveSubscriberRejectUnhealthy   = "subscriber_reject_when_unhealthy"
	directiveSubscriberRateLimit         = "subscriber_rate_limit"
	directiveXReadCount                  = "xread_count"
	directiveHealthThreshold             = "health_threshold"
	directiveHistoryReplayConcurrency    = "history_replay_concurrency"
	directiveSubscriberRateBurst         = "subscriber_rate_burst"
	directivePublisherRateLimit          = "publisher_rate_limit"
	directivePublisherRateBurst          = "publisher_rate_burst"
	directivePresenceDetailThreshold     = "presence_detail_threshold"
	directivePresenceByteThreshold       = "presence_detail_byte_threshold"
	directivePoolTimeout                 = "pool_timeout"
	directiveDialTimeout                 = "dial_timeout"
	directiveReadTimeout                 = "read_timeout"
	directiveWriteTimeout                = "write_timeout"
)

// Redis is a Caddy module that wraps redistransport.RedisTransport, following
// the hub's caddy/bolt.go and caddy/local.go modules.
//
// Caddy `{env.VAR}` runtime placeholders work on the string and duration
// directives: connection strings, credentials, TLS file paths and names,
// stream key, encoding and durations. Each resolves through the Caddy
// replacer (resolveDirective) when the options are built. An unset
// placeholder fails with errPlaceholderUnresolved, so a typo
// (`password {env.REDIS_PASWORD}`) surfaces as a Provision error rather than
// a silent empty value.
//
// Numeric and boolean directives (max_length, dispatch_shards, the rate
// limits and bursts, db, pool_size, tls, tls_insecure_skip_verify, and so
// on) take literals: a `{env.VAR}` value there is rejected at parse time.
// Use Caddyfile parse-time substitution (`dispatch_shards {$SHARDS}`) for
// env-driven values.
type Redis struct {
	// Redis URL, for example redis://localhost:6379 or rediss://... Mutually
	// exclusive with `addresses`. The URL form is the simplest config for
	// single-instance Redis; use `addresses` for Cluster or Sentinel topologies
	// where multiple seed nodes are required.
	URL string `json:"url,omitempty"`

	// Redis server addresses, for example "host1:6379 host2:6379 host3:6379".
	// The `addresses` directive takes several per line and the singular
	// `address` directive adds one; both accumulate across repeated lines.
	// Several addresses select go-redis Cluster mode, or Sentinel mode when
	// `master_name` is also set. Mutually exclusive with `url`.
	Addresses []string `json:"addresses,omitempty"`

	// Sentinel master name. When set, the addresses are treated as Sentinel
	// seed nodes and the failover-aware client is used. Empty for plain
	// Cluster or single-instance topologies.
	MasterName string `json:"master_name,omitempty"`

	// Username for Redis ACL (Redis 6+). Overrides the userinfo segment
	// of `url` when both are set. With `addresses` there is no URL, so it
	// applies on its own.
	Username string `json:"username,omitempty"`

	// Password for Redis AUTH or ACL. Overrides the userinfo segment of
	// `url` when both are set. With `addresses` there is no URL, so it
	// applies on its own. Prefer env-var injection (for example,
	// `password {env.REDIS_PASSWORD}`) over inline values.
	Password string `json:"password,omitempty"`

	// Database index (0–15 for standalone Redis; 0 for Cluster). Overrides
	// the path component of `url` when both are set. It is a pointer so an
	// explicit `db 0` can override a non-zero database parsed from the URL.
	// A plain int cannot tell `db 0` from an omitted directive.
	DB *int `json:"db,omitempty"`

	// TLS enables TLS connections with default settings (server name from
	// the address, MinVersion TLS 1.2). The `tls_ca_file`,
	// `tls_client_auth`, `tls_server_name`, and `tls_insecure_skip_verify`
	// directives also imply TLS without requiring the bare `tls` flag.
	TLS bool `json:"tls,omitempty"`

	// TLSCAFile is a path to a PEM file containing the trusted CA
	// certificates used to verify the Redis server's certificate. When set, it
	// replaces the system root pool (as `openssl -CAfile` does); to trust both,
	// bundle the system CAs into this file. Multi-cert PEM bundles are accepted.
	TLSCAFile string `json:"tls_ca_file,omitempty"`

	// TLSClientAuthCertFile is a path to a PEM client certificate, paired
	// with TLSClientAuthKeyFile. Set together via the `tls_client_auth
	// CERT KEY` Caddyfile directive (atomic two-arg form, matching Caddy's
	// `reverse_proxy` convention).
	TLSClientAuthCertFile string `json:"tls_client_auth_cert_file,omitempty"`

	// TLSClientAuthKeyFile is a path to the unencrypted PEM private key
	// for TLSClientAuthCertFile. Encrypted PEM keys are not supported by
	// Go's stdlib (decrypt with `openssl pkcs8 -in key.pem -nocrypt -out
	// key-decrypted.pem` first).
	TLSClientAuthKeyFile string `json:"tls_client_auth_key_file,omitempty"`

	// TLSServerName overrides the server name used for SNI and certificate
	// verification. When unset, go-redis derives the server name from the
	// dialed address. In Cluster mode go-redis applies one shared
	// TLSConfig.ServerName to all nodes. This directive therefore only works
	// for clusters whose nodes share a SAN, for example through a wildcard
	// certificate.
	TLSServerName string `json:"tls_server_name,omitempty"`

	// TLSInsecureSkipVerify disables certificate verification entirely.
	// Additive with the URL's `?skip_verify=true` query parameter: if
	// either is set, verification is disabled. Emits a Warn log at
	// startup; use only in trusted networks or for development.
	TLSInsecureSkipVerify bool `json:"tls_insecure_skip_verify,omitempty"`

	// Redis Stream key name (default: "mercure").
	Stream string `json:"stream,omitempty"`

	// Approximate MAXLEN for stream trimming (0 = unlimited).
	MaxLength int64 `json:"max_length,omitempty"`

	// Codec encoding: "json" (default), "gob", "msgpack".
	Encoding string `json:"encoding,omitempty"`

	// TTL for presence keys (default: "60s").
	PresenceTTL string `json:"presence_ttl,omitempty"`

	// Heartbeat interval for presence renewal (default: "30s").
	PresenceInterval string `json:"presence_interval,omitempty"`

	// Interval for periodic zombie consumer group GC (default: "5m"). "0"
	// disables periodic GC; the startup pass and one follow-up pass still run.
	ZombieGCInterval string `json:"zombie_gc_interval,omitempty"`

	// Clock skew margin for UUIDv7 timestamp seek (default: "5s"). A
	// Last-Event-ID dated further ahead is a future ID and gets no history
	// replay; see WithClockSkewMargin.
	ClockSkewMargin string `json:"clock_skew_margin,omitempty"`

	// TTL-based stream cleanup (default: "1h"; explicit "0" disables).
	EventTTL string `json:"event_ttl,omitempty"`

	// How often TTL cleanup runs (default: "5m").
	CleanupInterval string `json:"cleanup_interval,omitempty"`

	// Health check PING interval (default: "10s").
	HealthInterval string `json:"health_interval,omitempty"`

	// Consecutive PING failures before marking unhealthy (default: 3).
	HealthThreshold int `json:"health_threshold,omitempty"`

	// Number of messages per XREADGROUP call (default: 100).
	XReadCount int64 `json:"xread_count,omitempty"`

	// XREADGROUP BLOCK timeout (default: "1s", minimum: "1ms").
	XReadBlock string `json:"xread_block,omitempty"`

	// Number of dispatch worker goroutines (default: 1, 0 = NumCPU).
	// It is a pointer so an explicit `dispatch_shards 0` (auto-detect to
	// NumCPU) differs from an omitted directive (default 1). A plain int
	// would resolve an explicit 0 to the default instead of NumCPU.
	DispatchShards *int `json:"dispatch_shards,omitempty"`

	// Cap on how many subscribers the subscriptions API will list; a larger
	// result fails the request with 503 (default 100000; 0 disables the cap).
	// Pointer so an explicit `subscriptions_max_subscribers 0` is
	// distinguishable from "directive omitted".
	SubscriptionsMaxSubscribers *int `json:"subscriptions_max_subscribers,omitempty"`

	// Admission control (default off): per-node concurrent-subscriber ceiling;
	// over it, new subscribers get 429. 0 disables it.
	SubscriberMaxCount *int `json:"subscriber_max_count,omitempty"`

	// Bounded wait for a rate token before the gate sheds (default 0 = fail-fast).
	SubscriberAdmissionTimeout string `json:"subscriber_admission_timeout,omitempty"`

	// Bounds post-admission registration (history replay) so a slow backend can't
	// pin an admission slot (default 0 = unbounded). See
	// WithSubscriberRegistrationTimeout.
	SubscriberRegTimeout string `json:"subscriber_registration_timeout,omitempty"`

	// Base 429 Retry-After delay, jittered per response (default 2s; 0 omits the
	// header). An explicit 0 differs from an omitted directive, so a set value
	// is always applied.
	SubscriberRetryAfter string `json:"subscriber_retry_after,omitempty"`

	// Refuse new subscribers with 503 while health-check PINGs are failing
	// (default false). Existing subscribers are not affected.
	SubscriberRejectWhenUnhealthy bool `json:"subscriber_reject_when_unhealthy,omitempty"`

	// Max new subscribers/second (default: 0 = disabled).
	SubscriberRateLimit float64 `json:"subscriber_rate_limit,omitempty"`

	// Burst allowance for subscriber rate limiter (default: 5000).
	SubscriberRateBurst int `json:"subscriber_rate_burst,omitempty"`

	// Max concurrent history replay operations (default: half the go-redis
	// pool size, at least 1 and at most 20).
	HistoryReplayConcurrency int `json:"history_replay_concurrency,omitempty"`

	// Max subscribers before switching to summary presence (default: 1000).
	PresenceDetailThreshold int `json:"presence_detail_threshold,omitempty"`

	// Max marshaled bytes of detail-mode presence payload before
	// summary-mode fallback (default: 524288 = 512 KiB; explicit 0
	// disables the byte budget entirely). Pointer so an explicit
	// `presence_detail_byte_threshold 0` is distinguishable from
	// "directive omitted" (which falls back to the transport default).
	PresenceDetailByteThreshold *int64 `json:"presence_detail_byte_threshold,omitempty"`

	// Max publish operations/second (default: 0 = disabled). Symmetric
	// with subscriber_rate_limit on the publisher side.
	PublisherRateLimit float64 `json:"publisher_rate_limit,omitempty"`

	// go-redis client pool / timeout / retry knobs. Zero-valued fields
	// inherit go-redis library defaults.

	// PoolSize bounds the go-redis connection pool. 0 keeps the
	// library default (10 × GOMAXPROCS; 5 × GOMAXPROCS per node for Cluster).
	// Raise it when `mercure_redis_client_pool_timeouts_total` is
	// non-zero under load; bound it when Redis `maxclients` is close
	// to saturation (one server connection per pool entry ×
	// hub-replica count).
	PoolSize int `json:"pool_size,omitempty"`

	// MinIdleConns keeps a floor of idle connections warm. 0 keeps
	// the library default (0, so connections open lazily). Set a
	// non-zero value to amortize the connection-establishment cost
	// across the first few requests after a hub starts or after a
	// transport reload.
	MinIdleConns int `json:"min_idle_conns,omitempty"`

	// PoolTimeout caps how long a Dispatch / XREADGROUP /
	// XRANGE caller waits for a free pool entry before returning
	// ErrPoolTimeout. Empty / "0" keeps the library default
	// (ReadTimeout + 1s). Tighter values surface pool exhaustion as
	// publish errors sooner, at the cost of more frequent
	// transient-spike errors.
	PoolTimeout string `json:"pool_timeout,omitempty"`

	// DialTimeout caps how long a new connection establishment can
	// take. Empty / "0" keeps the library default (5s). Tighten in
	// low-latency environments; loosen on slow-DNS / VPN paths.
	DialTimeout string `json:"dial_timeout,omitempty"`

	// ReadTimeout caps how long a READ can block on the socket.
	// Empty / "0" keeps the library default (3s). Not applied to the
	// blocking XREADGROUP; go-redis gives it BLOCK + 10s.
	ReadTimeout string `json:"read_timeout,omitempty"`

	// WriteTimeout caps socket writes. Empty / "0" keeps the
	// library default (3s, equal to ReadTimeout).
	WriteTimeout string `json:"write_timeout,omitempty"`

	// MaxRetries bounds how many times go-redis will retry a
	// command on a transient failure. 0 keeps the library default
	// (3). -1 disables retries, which is useful when you want every
	// transient failure to surface in metrics rather than be silently
	// absorbed. In Cluster mode go-redis disables per-node retries unless this
	// directive sets a non-zero value, and it follows up to 3 redirects
	// regardless.
	MaxRetries int `json:"max_retries,omitempty"`

	// Burst allowance for publisher rate limiter (default: 5000).
	PublisherRateBurst int `json:"publisher_rate_burst,omitempty"`

	transport    *redistransport.RedisTransport
	transportKey string
}

// init registers this module with Caddy.
//
//nolint:gochecknoinits // Caddy framework requires init() for module registration
func init() {
	caddyv2.RegisterModule(Redis{})
}

// CaddyModule returns the Caddy module information.
func (Redis) CaddyModule() caddyv2.ModuleInfo {
	return caddyv2.ModuleInfo{
		ID:  "http.handlers.mercure.redis",
		New: func() caddyv2.Module { return new(Redis) },
	}
}

// GetTransport returns the underlying Mercure transport.
func (r *Redis) GetTransport() mercure.Transport {
	return r.transport
}

// Provision provisions r's configuration using the TransportUsagePool
// singleton pattern from caddy/bolt.go.
//
// json.Marshal (not gob) computes the key because gob elides a
// pointer-to-zero exactly as it elides a nil pointer. With gob, an explicit
// `dispatch_shards 0` (NumCPU) or `db 0` would get the same key as an omitted
// directive, silently sharing one transport across semantically different
// configs. json's omitempty drops nil pointers but preserves a non-nil
// pointer-to-zero, so the tri-state survives.
func (r *Redis) Provision(ctx caddyv2.Context) error {
	hubName, _ := ctx.Value(mercurecaddy.HubNameContextKey).(string)

	key, err := r.poolKey(hubName)
	if err != nil {
		return err
	}

	r.transportKey = key

	destructor, _, err := mercurecaddy.TransportUsagePool.LoadOrNew(r.transportKey, func() (caddyv2.Destructor, error) {
		opts, clientOpts, err := r.buildOptions(ctx.Slogger())
		if err != nil {
			return nil, err
		}

		// UniversalClient handles all three topologies based on options:
		//  - len(Addrs)==1, no MasterName selects single-instance (*redis.Client)
		//  - len(Addrs)>1, no MasterName  selects Cluster mode (*redis.ClusterClient)
		//  - MasterName set               selects Sentinel mode (failover *redis.Client)
		client := redis.NewUniversalClient(clientOpts)

		t, err := redistransport.NewRedisTransport(
			client,
			opts...,
		)
		if err != nil {
			return nil, err
		}

		return mercurecaddy.TransportDestructor[*redistransport.RedisTransport]{Transport: t}, nil
	})
	if err != nil {
		return err
	}

	r.transport = destructor.(mercurecaddy.TransportDestructor[*redistransport.RedisTransport]).Transport

	return nil
}

// Cleanup releases the pooled transport instance associated with this handler.
func (r *Redis) Cleanup() error {
	_, err := mercurecaddy.TransportUsagePool.Delete(r.transportKey)

	return err
}

// UnmarshalCaddyfile sets up the handler from Caddyfile tokens.
func (r *Redis) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	for d.Next() {
		for d.NextBlock(0) {
			if err := r.parseDirective(d); err != nil {
				return err
			}
		}
	}

	return nil
}

// parseDirective parses a single Caddyfile directive.
//
//nolint:funlen,gocyclo // flat dispatch over the directive names; each case is one line
func (r *Redis) parseDirective(d *caddyfile.Dispenser) error {
	switch d.Val() {
	case "url":
		return r.parseString(d, &r.URL)
	case "addresses":
		return r.parseStringSlice(d, &r.Addresses)
	case "address":
		return r.appendAddress(d)
	case "master_name":
		return r.parseString(d, &r.MasterName)
	case "username":
		return r.parseString(d, &r.Username)
	case "password":
		return r.parseString(d, &r.Password)
	case "db":
		return r.parseDB(d)
	case directiveTLS:
		return r.parseBool(d, &r.TLS)
	case "tls_ca_file":
		return r.parseString(d, &r.TLSCAFile)
	case "tls_client_auth":
		return r.parseClientAuth(d)
	case "tls_server_name":
		return r.parseString(d, &r.TLSServerName)
	case directiveTLSInsecureSkipVerify:
		return r.parseBool(d, &r.TLSInsecureSkipVerify)
	case "stream":
		return r.parseString(d, &r.Stream)
	case "encoding":
		return r.parseString(d, &r.Encoding)
	case "gob":
		return d.Err("the bare `gob` directive was removed; use `encoding gob`")
	case "presence_ttl":
		return r.parseString(d, &r.PresenceTTL)
	case "presence_interval":
		return r.parseString(d, &r.PresenceInterval)
	case "zombie_gc_interval":
		return r.parseString(d, &r.ZombieGCInterval)
	case "clock_skew_margin":
		return r.parseString(d, &r.ClockSkewMargin)
	case "event_ttl":
		return r.parseString(d, &r.EventTTL)
	case "cleanup_interval":
		return r.parseString(d, &r.CleanupInterval)
	case "health_interval":
		return r.parseString(d, &r.HealthInterval)
	case "xread_block":
		return r.parseString(d, &r.XReadBlock)
	case directiveMaxLength:
		return r.parseInt64(d, &r.MaxLength)
	case directiveXReadCount:
		return r.parseInt64(d, &r.XReadCount)
	case directiveHealthThreshold:
		return r.parseInt(d, &r.HealthThreshold)
	case directiveDispatchShards:
		return r.parsePtrInt(d, &r.DispatchShards)
	case directiveSubscriptionsMaxSubscribers:
		return r.parsePtrInt(d, &r.SubscriptionsMaxSubscribers)
	case directiveSubscriberMaxCount:
		return r.parsePtrInt(d, &r.SubscriberMaxCount)
	case directiveSubscriberAdmissionTimeout:
		return r.parseString(d, &r.SubscriberAdmissionTimeout)
	case directiveSubscriberRegTimeout:
		return r.parseString(d, &r.SubscriberRegTimeout)
	case directiveSubscriberRetryAfter:
		return r.parseString(d, &r.SubscriberRetryAfter)
	case directiveSubscriberRejectUnhealthy:
		return r.parseBool(d, &r.SubscriberRejectWhenUnhealthy)
	case directiveSubscriberRateBurst:
		return r.parseInt(d, &r.SubscriberRateBurst)
	case directiveHistoryReplayConcurrency:
		return r.parseInt(d, &r.HistoryReplayConcurrency)
	case directivePresenceDetailThreshold:
		return r.parseInt(d, &r.PresenceDetailThreshold)
	case directivePresenceByteThreshold:
		return r.parsePtrInt64(d, &r.PresenceDetailByteThreshold)
	case directiveSubscriberRateLimit:
		return r.parseFloat64(d, &r.SubscriberRateLimit)
	case directivePublisherRateBurst:
		return r.parseInt(d, &r.PublisherRateBurst)
	case directivePublisherRateLimit:
		return r.parseFloat64(d, &r.PublisherRateLimit)
	case "pool_size":
		return r.parseInt(d, &r.PoolSize)
	case "min_idle_conns":
		return r.parseInt(d, &r.MinIdleConns)
	case directivePoolTimeout:
		return r.parseString(d, &r.PoolTimeout)
	case directiveDialTimeout:
		return r.parseString(d, &r.DialTimeout)
	case directiveReadTimeout:
		return r.parseString(d, &r.ReadTimeout)
	case directiveWriteTimeout:
		return r.parseString(d, &r.WriteTimeout)
	case "max_retries":
		return r.parseInt(d, &r.MaxRetries)
	case "{":
		// A nested block opener at directive position means the operator
		// wrote something like `tls { ... }`, but this transport's directive
		// surface is intentionally flat. Return a hint instead of the opaque
		// "unknown redis transport directive: {" the default case would give.
		return d.Err("nested sub-blocks are not supported by the redis transport — " +
			"use flat directives (e.g., `tls_ca_file`, `tls_client_auth`, `tls_server_name`, `tls_insecure_skip_verify`)")
	default:
		return d.Errf("unknown redis transport directive: %s", d.Val())
	}
}

func (*Redis) parseString(d *caddyfile.Dispenser, target *string) error {
	if !d.NextArg() {
		return d.ArgErr()
	}

	*target = d.Val()

	return nil
}

// literalArg reads the single argument of the numeric or boolean directive
// d is on. These directives take literals: a runtime placeholder is rejected
// with a message naming the directive, rather than a bare parse error.
func literalArg(d *caddyfile.Dispenser) (string, error) {
	name := d.Val()
	if !d.NextArg() {
		return "", d.ArgErr()
	}

	return d.Val(), rejectPlaceholder(d, name)
}

// rejectPlaceholder fails when the current token of d, the argument of
// directive name, contains a Caddy placeholder.
func rejectPlaceholder(d *caddyfile.Dispenser, name string) error {
	if !strings.Contains(d.Val(), "{") {
		return nil
	}

	// Caddy substitutes {$VAR} as text before tokenizing, so an unset, unquoted
	// variable vanishes and shifts the arguments; quoting it leaves an empty
	// argument, which fails startup instead.
	return d.Errf(`%s %s: numeric and boolean directives take literals, not runtime placeholders; to use an environment variable, write "{$VAR}" (quoted, so that an unset variable leaves an empty argument and startup fails) or {$VAR:default}`, name, d.Val())
}

// parseInt reads one int literal.
func (*Redis) parseInt(d *caddyfile.Dispenser, target *int) error {
	val, err := literalArg(d)
	if err != nil {
		return err
	}

	n, err := strconv.Atoi(val)
	if err != nil {
		return d.WrapErr(err)
	}

	*target = n

	return nil
}

// parseInt64 mirrors parseInt for int64-typed fields.
func (*Redis) parseInt64(d *caddyfile.Dispenser, target *int64) error {
	val, err := literalArg(d)
	if err != nil {
		return err
	}

	n, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return d.WrapErr(err)
	}

	*target = n

	return nil
}

// parsePtrInt64 mirrors parseInt64 for pointer-typed int64 fields.
// See PresenceDetailByteThreshold field doc for the pointer rationale.
func (*Redis) parsePtrInt64(d *caddyfile.Dispenser, target **int64) error {
	val, err := literalArg(d)
	if err != nil {
		return err
	}

	n, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return d.WrapErr(err)
	}

	*target = &n

	return nil
}

// parsePtrInt mirrors parsePtrInt64 for pointer-typed int fields
// (allocating a fresh pointer rather than writing into a value field).
// See DispatchShards field doc for the pointer rationale.
func (*Redis) parsePtrInt(d *caddyfile.Dispenser, target **int) error {
	val, err := literalArg(d)
	if err != nil {
		return err
	}

	n, err := strconv.Atoi(val)
	if err != nil {
		return d.WrapErr(err)
	}

	*target = &n

	return nil
}

// parseFloat64 mirrors parseInt for float64-typed fields.
func (*Redis) parseFloat64(d *caddyfile.Dispenser, target *float64) error {
	val, err := literalArg(d)
	if err != nil {
		return err
	}

	f, err := strconv.ParseFloat(val, 64)
	if err != nil {
		return d.WrapErr(err)
	}

	*target = f

	return nil
}

// parseBool accepts a bare flag (`tls`) or an explicit truthy/falsy arg
// (`tls on` / `tls true`). Caddy's directive convention is that flag-style
// directives without an arg mean "true"; that's what `tls` alone implies.
func (*Redis) parseBool(d *caddyfile.Dispenser, target *bool) error {
	name := d.Val()
	if !d.NextArg() {
		*target = true

		return nil
	}

	if err := rejectPlaceholder(d, name); err != nil {
		return err
	}

	b, err := parseFlexibleBool(d.Val())
	if err != nil {
		return d.WrapErr(err)
	}

	*target = b

	return nil
}

// parseFlexibleBool accepts the strconv.ParseBool set
// ({true, false, 1, 0, t, f, T, F, TRUE, FALSE, True, False}) plus the
// Caddyfile-idiomatic on/off and yes/no spellings (case-insensitive), the
// boolean grammar documented in docs/configuration.md.
func parseFlexibleBool(val string) (bool, error) {
	switch strings.ToLower(val) {
	case "on", "yes":
		return true, nil
	case "off", "no":
		return false, nil
	default:
		return strconv.ParseBool(val)
	}
}

// parseStringSlice consumes all remaining args on the directive line and
// appends them to target, so repeated `addresses` lines accumulate
// consistently with the singular `address` alias. A single
// `addresses host1 host2 host3` line still works because RemainingArgs
// returns the whole list.
func (*Redis) parseStringSlice(d *caddyfile.Dispenser, target *[]string) error {
	args := d.RemainingArgs()
	if len(args) == 0 {
		return d.ArgErr()
	}

	*target = append(*target, args...)

	return nil
}

// appendAddress reads one host:port from the line and appends it to
// r.Addresses. Singular `address` is an alias for `addresses`; multiple
// `address` lines accumulate.
func (r *Redis) appendAddress(d *caddyfile.Dispenser) error {
	if !d.NextArg() {
		return d.ArgErr()
	}

	r.Addresses = append(r.Addresses, d.Val())

	return nil
}

// parseDB reads one int and stores it in r.DB as a fresh pointer, so an
// explicit `db 0` is distinguishable from "directive omitted". See the DB
// field doc for the pointer rationale.
func (r *Redis) parseDB(d *caddyfile.Dispenser) error {
	return r.parsePtrInt(d, &r.DB)
}

// poolKey derives the transport-sharing key from the config and the hub name,
// so differently named hubs never share a transport (as with Bolt and Local).
// Hubs with the same stream still share its Redis keys. See Provision for why
// the config is JSON-encoded.
func (r *Redis) poolKey(hubName string) (string, error) {
	// The password is part of the key so configs differing only in credentials do
	// not share a transport; the key is never logged. {env.VAR} placeholders are
	// not resolved first, so a literal and a placeholder for the same value yield
	// two transports.
	b, err := json.Marshal(r) //nolint:gosec // in-memory pool key, never logged/persisted
	if err != nil {
		return "", err
	}

	// The JSON object ends the config part unambiguously, so appending the
	// name cannot make two different (config, name) pairs collide.
	return string(b) + hubName, nil
}

// parseClientAuth handles the two-argument `tls_client_auth CERT KEY`
// directive. Matches Caddy's `reverse_proxy` convention of an atomic
// cert/key pair on a single line, eliminating the half-set error class.
func (r *Redis) parseClientAuth(d *caddyfile.Dispenser) error {
	var cert, key string
	if !d.Args(&cert, &key) {
		return d.ArgErr()
	}

	if d.NextArg() {
		return d.ArgErr()
	}

	r.TLSClientAuthCertFile = cert
	r.TLSClientAuthKeyFile = key

	return nil
}

// buildOptions converts the Caddyfile/JSON fields into transport options and
// go-redis UniversalOptions. logger receives the startup warnings and becomes
// the transport's logger.
func (r *Redis) buildOptions(logger *slog.Logger) ([]redistransport.Option, *redis.UniversalOptions, error) {
	clientOpts, overridden, err := r.buildClientOptions()
	if err != nil {
		return nil, nil, err
	}

	for _, directive := range overridden {
		logger.Warn("redis transport: explicit directive overrides the value parsed from `url`", "directive", directive)
	}

	if r.tlsRequested() || clientOpts.TLSConfig != nil {
		clientOpts.TLSConfig, err = r.buildTLSConfig(clientOpts.TLSConfig)
		if err != nil {
			return nil, nil, err
		}

		if clientOpts.TLSConfig.InsecureSkipVerify {
			// tls_ca_file_ignored: the CA file is loaded but never used to
			// verify the server.
			logger.Warn("redis transport: tls verification disabled, the connection is vulnerable to MITM",
				"tls_ca_file_ignored", r.TLSCAFile != "")
		}
	}

	// No WithPrometheusRegisterer: this module's context carries no metrics
	// registry, so the mercure module binds the transport to its config's
	// registry through TransportMetricsRegisterer after loading it.
	opts := []redistransport.Option{
		redistransport.WithLogger(logger),
	}

	opts, err = r.appendBasicOptions(opts)
	if err != nil {
		return nil, nil, err
	}

	opts, err = r.appendDurationOptions(opts)
	if err != nil {
		return nil, nil, err
	}

	opts, err = r.appendNumericOptions(opts)
	if err != nil {
		return nil, nil, err
	}

	return opts, clientOpts, nil
}

// buildClientOptions assembles redis.UniversalOptions from the Caddyfile
// fields. Resolution order:
//
//  1. If `addresses` is set, use it directly (Cluster or Sentinel mode, or a
//     single instance for one address without `master_name`). `url` must not
//     also be set.
//  2. Otherwise parse `url` (a single instance unless `master_name` is set).
//
// `master_name` then selects Sentinel. Explicit `username`, `password` and
// `db` override the URL's values. TLS files are loaded separately, in
// buildTLSConfig.
//
// Returns the names of the directives whose explicit value overrides a
// different URL-derived value. Layering explicit fields over URL-parsed values
// supports the env-var-injection pattern: placeholder credentials in the URL
// and real values from {env.VAR}. The caller logs a warning per name, so the
// conflict is visible at startup.
func (r *Redis) buildClientOptions() (*redis.UniversalOptions, []string, error) {
	conn, err := r.resolveConn(caddyv2.NewReplacer())
	if err != nil {
		return nil, nil, err
	}

	if len(conn.addresses) > 0 && conn.url != "" {
		return nil, nil, errURLAndAddresses
	}

	// ContextTimeoutEnabled makes go-redis cut an in-flight command at the
	// caller's context deadline (publish_timeout, the health-check budget)
	// instead of waiting for read_timeout. It only ever shortens a deadline;
	// callers without one keep the read/write timeouts.
	uo := &redis.UniversalOptions{ContextTimeoutEnabled: true}

	var overridden []string

	if len(conn.addresses) > 0 {
		uo.Addrs = conn.addresses
	} else {
		// Single-instance: parse the URL, then promote its values into a
		// UniversalOptions. ParseURL handles `redis://`, `rediss://`,
		// userinfo, and the path component (db number).
		parsed, err := redis.ParseURL(conn.url)
		if err != nil {
			return nil, nil, fmt.Errorf("redis transport: invalid URL: %w", err)
		}

		uo.Addrs = []string{parsed.Addr}
		uo.Username = parsed.Username
		uo.Password = parsed.Password
		uo.DB = parsed.DB
		uo.TLSConfig = parsed.TLSConfig

		overridden = urlOverriddenDirectives(parsed, conn, r.DB)
	}

	// Explicit fields override URL-derived values so secrets sourced from
	// {env.VAR} take precedence over anything baked into the URL.
	if conn.masterName != "" {
		uo.MasterName = conn.masterName
	}

	if conn.username != "" {
		uo.Username = conn.username
	}

	if conn.password != "" {
		uo.Password = conn.password
	}

	if r.DB != nil {
		uo.DB = *r.DB
	}

	// Pool / timeout / retry knobs. Zero-valued fields leave the
	// go-redis library defaults intact. Duration strings resolve
	// placeholders as the other duration fields do (PresenceTTL,
	// PresenceInterval, and so on), so {env.VAR} works.
	if err := r.applyClientTuning(uo); err != nil {
		return nil, overridden, err
	}

	return uo, overridden, nil
}

// applyClientTuning sets the go-redis pool, timeout and retry fields on uo.
// Durations go through resolveDirective and parseDuration; empty values keep
// the go-redis default. All validation errors are joined.
func (r *Redis) applyClientTuning(uo *redis.UniversalOptions) error {
	errs := r.validateIntegerKnobs()
	errs = append(errs, r.applyDurationKnobs(uo)...)

	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	// Assign the integer fields only after validation passes. applyDurationKnobs
	// has already set durations on uo, which is safe because the only caller
	// discards uo on error.
	if r.PoolSize != 0 {
		uo.PoolSize = r.PoolSize
	}

	if r.MinIdleConns != 0 {
		uo.MinIdleConns = r.MinIdleConns
	}

	if r.MaxRetries != 0 {
		uo.MaxRetries = r.MaxRetries
	}

	return nil
}

// validateIntegerKnobs returns one error per integer field that fails
// validation. Negative pool sizes panic inside go-redis pool creation
// (`make(chan struct{}, opt.PoolSize)`); rejecting at the Caddy boundary
// surfaces the failure as a config error rather than a process panic.
// max_retries=-1 is the documented sentinel for "disable retries" (go-redis
// treats -1 as zero retries); any other negative value is a typo.
func (r *Redis) validateIntegerKnobs() []error {
	var errs []error

	if r.PoolSize < 0 {
		errs = append(errs, fmt.Errorf("%w: pool_size=%d", errNegativeOptionValue, r.PoolSize))
	}

	if r.MinIdleConns < 0 {
		errs = append(errs, fmt.Errorf("%w: min_idle_conns=%d", errNegativeOptionValue, r.MinIdleConns))
	}

	if r.MaxRetries < -1 {
		errs = append(errs, fmt.Errorf("%w: max_retries=%d (use -1 to disable retries; any other negative value is unsupported)", errNegativeOptionValue, r.MaxRetries))
	}

	if r.DB != nil && *r.DB < 0 {
		errs = append(errs, fmt.Errorf("%w: db=%d (Redis database indexes are non-negative)", errNegativeOptionValue, *r.DB))
	}

	return errs
}

// applyDurationKnobs resolves and assigns every Caddyfile-supplied
// duration into uo, returning one error per failing field instead of
// short-circuiting on the first. Empty / unset directives are skipped.
func (r *Redis) applyDurationKnobs(uo *redis.UniversalOptions) []error {
	var errs []error

	repl := caddyv2.NewReplacer()
	durations := []struct {
		name  string
		raw   string
		field *time.Duration
	}{
		{directivePoolTimeout, r.PoolTimeout, &uo.PoolTimeout},
		{directiveDialTimeout, r.DialTimeout, &uo.DialTimeout},
		{directiveReadTimeout, r.ReadTimeout, &uo.ReadTimeout},
		{directiveWriteTimeout, r.WriteTimeout, &uo.WriteTimeout},
	}

	for _, td := range durations {
		if td.raw == "" {
			continue
		}

		d, err := resolveDurationDirective(repl, td.raw, td.name)
		if err != nil {
			errs = append(errs, err)

			continue
		}

		// A negative duration would either silently disable the timeout (for
		// example, a negative dial_timeout means no timeout in go-redis) or
		// expire immediately. Both surprise an operator who is tuning, so
		// reject it explicitly.
		if d < 0 {
			errs = append(errs, fmt.Errorf("%w: %s=%v", errNegativeOptionValue, td.name, d))

			continue
		}

		// d == 0 keeps the go-redis default: leave uo's zero value.
		if d == 0 {
			continue
		}

		*td.field = d
	}

	return errs
}

// urlOverriddenDirectives names each credential directive (`username`,
// `password`, `db`) where the URL and the directive set different non-empty
// values; the directive wins. It compares placeholder-resolved values.
// explicitDB is a pointer so an explicit `db 0` overriding a URL database is
// reported.
func urlOverriddenDirectives(parsed *redis.Options, conn resolvedConn, explicitDB *int) []string {
	var overridden []string

	if parsed.Username != "" && conn.username != "" && parsed.Username != conn.username {
		overridden = append(overridden, "username")
	}

	if parsed.Password != "" && conn.password != "" && parsed.Password != conn.password {
		overridden = append(overridden, "password")
	}

	if parsed.DB != 0 && explicitDB != nil && parsed.DB != *explicitDB {
		overridden = append(overridden, "db")
	}

	return overridden
}

// tlsRequested reports whether the `tls` flag or any `tls_*` directive is set.
func (r *Redis) tlsRequested() bool {
	return r.TLS || r.TLSInsecureSkipVerify ||
		r.TLSCAFile != "" || r.TLSClientAuthCertFile != "" ||
		r.TLSClientAuthKeyFile != "" || r.TLSServerName != ""
}

// resolveDirective runs a Caddyfile-supplied string value through the
// Caddy replacer and fails fast if a non-empty raw value resolved to
// empty, which typically means an unset `{env.VAR}` placeholder.
// Returns an empty string only when the raw value was already empty
// (that is, the directive was unset). Used uniformly for both file paths
// (TLS) and credential / connection string fields, so operator typos
// like `password {env.REDIS_PASWORD}` don't silently degrade auth.
func resolveDirective(repl *caddyv2.Replacer, raw, directive string) (string, error) {
	if raw == "" {
		return "", nil
	}

	resolved := repl.ReplaceKnown(raw, "")
	if resolved == "" {
		return "", fmt.Errorf("%w: %s = %q", errPlaceholderUnresolved, directive, raw)
	}

	return resolved, nil
}

// resolvedConn holds the placeholder-substituted connection-string
// fields used by buildClientOptions. Resolving up front means the conflict
// check between url and addresses, the URL parser and the override layering
// all see the effective values. For example, `password {env.X}` resolves to
// the secret, not the literal placeholder string.
type resolvedConn struct {
	url, username, password, masterName string
	addresses                           []string
}

// resolveConn substitutes Caddy placeholders for every connection
// string field. Fast-fails on `{env.UNSET}`-style placeholders that
// would otherwise pass empty strings to go-redis silently.
func (r *Redis) resolveConn(repl *caddyv2.Replacer) (resolvedConn, error) {
	var (
		c   resolvedConn
		err error
	)

	if c.url, err = resolveDirective(repl, r.URL, "url"); err != nil {
		return c, err
	}

	if c.username, err = resolveDirective(repl, r.Username, "username"); err != nil {
		return c, err
	}

	if c.password, err = resolveDirective(repl, r.Password, "password"); err != nil {
		return c, err
	}

	if c.masterName, err = resolveDirective(repl, r.MasterName, "master_name"); err != nil {
		return c, err
	}

	if len(r.Addresses) > 0 {
		c.addresses = make([]string, len(r.Addresses))

		for i, addr := range r.Addresses {
			if c.addresses[i], err = resolveDirective(repl, addr, "addresses"); err != nil {
				return c, err
			}
		}
	}

	return c, nil
}

// resolvedTLSPaths holds the placeholder-substituted TLS path/name fields.
type resolvedTLSPaths struct {
	caFile, certFile, keyFile, serverName string
}

// resolveTLSPaths runs each TLS path/name field through the replacer,
// fast-failing on any unset placeholder.
func (r *Redis) resolveTLSPaths(repl *caddyv2.Replacer) (resolvedTLSPaths, error) {
	var (
		p   resolvedTLSPaths
		err error
	)

	if p.caFile, err = resolveDirective(repl, r.TLSCAFile, "tls_ca_file"); err != nil {
		return p, err
	}

	if p.certFile, err = resolveDirective(repl, r.TLSClientAuthCertFile, "tls_client_auth (cert)"); err != nil {
		return p, err
	}

	if p.keyFile, err = resolveDirective(repl, r.TLSClientAuthKeyFile, "tls_client_auth (key)"); err != nil {
		return p, err
	}

	if p.serverName, err = resolveDirective(repl, r.TLSServerName, "tls_server_name"); err != nil {
		return p, err
	}

	return p, nil
}

// buildTLSConfig materializes the *tls.Config from the TLS Caddyfile
// directives. It layers onto an existing config, typically one produced by
// redis.ParseURL for a rediss:// URL. When the URL carries no TLS settings,
// it creates a fresh config with a TLS 1.2 floor.
//
// Caddy {env.VAR} placeholders in path and name fields are substituted with
// caddy.NewReplacer().ReplaceKnown, as caddy/mercure.go does for JWT key
// fields. Unlike that code, this fails fast on a placeholder that resolves to
// empty (see resolveDirective), because an empty cert path is meaningless and
// would otherwise silently degrade the configured TLS.
//
// Always returns a non-nil *tls.Config or an error. The caller is
// responsible for invoking this only when TLS is actually requested
// (see tlsRequested) or an existing config is being extended.
func (r *Redis) buildTLSConfig(existing *tls.Config) (*tls.Config, error) {
	paths, err := r.resolveTLSPaths(caddyv2.NewReplacer())
	if err != nil {
		return nil, err
	}

	cfg := existing
	if cfg == nil {
		cfg = &tls.Config{MinVersion: tls.VersionTLS12}
	}

	if paths.caFile != "" {
		if err := loadCAInto(cfg, paths.caFile); err != nil {
			return nil, err
		}
	}

	if paths.certFile != "" || paths.keyFile != "" {
		if err := loadClientAuthInto(cfg, paths.certFile, paths.keyFile); err != nil {
			return nil, err
		}
	}

	if paths.serverName != "" {
		cfg.ServerName = paths.serverName
	}

	if r.TLSInsecureSkipVerify {
		cfg.InsecureSkipVerify = true
	}

	return cfg, nil
}

// loadCAInto reads the PEM file at caFile and replaces cfg.RootCAs
// with a pool containing only those CAs (matches `openssl -CAfile`).
func loadCAInto(cfg *tls.Config, caFile string) error {
	pemBytes, err := os.ReadFile(caFile)
	if err != nil {
		return fmt.Errorf("redis transport: tls_ca_file: %w", err)
	}

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pemBytes) {
		return fmt.Errorf("%w: %q", errTLSCANoPEM, caFile)
	}

	cfg.RootCAs = pool

	return nil
}

// loadClientAuthInto loads the cert/key pair and appends to
// cfg.Certificates. The decrypt hint is attached only when the error text
// contains "parse private key", which an encrypted PKCS#8 key produces (as does
// any other unparsable key). For other errors, such as a missing file, a cert
// and key mismatch or no PEM data, the hint would mislead.
func loadClientAuthInto(cfg *tls.Config, certFile, keyFile string) error {
	if certFile == "" || keyFile == "" {
		return errTLSClientAuthIncomplete
	}

	kp, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		if strings.Contains(err.Error(), "parse private key") {
			return fmt.Errorf("redis transport: tls_client_auth: failed to load cert/key pair: %w "+
				"(note: encrypted PEM keys are not supported by Go stdlib; decrypt with "+
				"`openssl pkcs8 -in key.pem -nocrypt -out key-decrypted.pem`)", err)
		}

		return fmt.Errorf("redis transport: tls_client_auth: failed to load cert/key pair: %w", err)
	}

	cfg.Certificates = append(cfg.Certificates, kp)

	return nil
}

// appendBasicOptions adds plain-valued (non-parsed) options. Stream and
// Encoding, the string fields operators commonly inject from the environment,
// run through resolveDirective. So `stream {env.X}` and `encoding {env.X}`
// reach the transport with the resolved value, and an unset placeholder fails
// fast rather than silently falling back to the default stream or encoding.
func (r *Redis) appendBasicOptions(opts []redistransport.Option) ([]redistransport.Option, error) {
	repl := caddyv2.NewReplacer()

	if r.Stream != "" {
		stream, err := resolveDirective(repl, r.Stream, "stream")
		if err != nil {
			return nil, err
		}

		opts = append(opts, redistransport.WithStreamName(stream))
	}

	if r.MaxLength < 0 {
		return nil, fmt.Errorf("%w: max_length=%d (Redis rejects negative MAXLEN)", errNegativeOptionValue, r.MaxLength)
	}

	// Dropping an explicit `max_length 0` is safe only because the default is
	// also 0 (unbounded). If that default ever changes, make MaxLength *int64
	// (like DispatchShards / DB) so an explicit 0 isn't lost.
	if r.MaxLength > 0 {
		opts = append(opts, redistransport.WithMaxLength(r.MaxLength))
	}

	if r.Encoding != "" {
		encoding, err := resolveDirective(repl, r.Encoding, "encoding")
		if err != nil {
			return nil, err
		}

		opts = append(opts, redistransport.WithEncoding(encoding))
	}

	if r.SubscriberRejectWhenUnhealthy {
		opts = append(opts, redistransport.WithSubscriberRejectWhenUnhealthy(true))
	}

	return opts, nil
}

// appendDurationOptions parses every duration-string field and appends an
// Option for each non-empty, valid value. Each duration string runs through
// resolveDirective, so `presence_ttl {env.X}` reaches parseDuration with the
// resolved value. An unset placeholder fails fast instead of silently applying
// the transport's default. Returns an error on the first invalid duration
// string.
func (r *Redis) appendDurationOptions(opts []redistransport.Option) ([]redistransport.Option, error) {
	repl := caddyv2.NewReplacer()

	positiveDurations := []struct {
		name  string
		value string
		build func(time.Duration) redistransport.Option
	}{
		{"presence_ttl", r.PresenceTTL, redistransport.WithPresenceTTL},
		{"presence_interval", r.PresenceInterval, redistransport.WithPresenceInterval},
		{"cleanup_interval", r.CleanupInterval, redistransport.WithCleanupInterval},
		{"health_interval", r.HealthInterval, redistransport.WithHealthInterval},
		{"xread_block", r.XReadBlock, redistransport.WithXReadBlock},
		{directiveSubscriberAdmissionTimeout, r.SubscriberAdmissionTimeout, redistransport.WithSubscriberAdmissionTimeout},
		{directiveSubscriberRegTimeout, r.SubscriberRegTimeout, redistransport.WithSubscriberRegistrationTimeout},
	}
	for _, e := range positiveDurations {
		var err error

		opts, err = appendPositiveDurationOption(opts, repl, e.name, e.value, e.build)
		if err != nil {
			return nil, err
		}
	}

	return r.appendZeroAllowedDurationOptions(opts, repl)
}

// appendZeroAllowedDurationOptions appends the duration directives for which
// zero is meaningful (event_ttl, zombie_gc_interval, clock_skew_margin,
// subscriber_retry_after), so each one set is applied even at 0.
func (r *Redis) appendZeroAllowedDurationOptions(opts []redistransport.Option, repl *caddyv2.Replacer) ([]redistransport.Option, error) {
	// event_ttl accepts 0 (disables time-based retention); skipping zero
	// would silently fall back to the 1h Go default. Operators writing
	// `event_ttl 0` mean "no time-based bound", so pass it through verbatim.
	if r.EventTTL != "" {
		d, err := resolveDurationDirective(repl, r.EventTTL, "event_ttl")
		if err != nil {
			return nil, err
		}

		if d < 0 {
			return nil, fmt.Errorf("%w: event_ttl=%v", errNegativeOptionValue, d)
		}

		opts = append(opts, redistransport.WithEventTTL(d))
	}

	// zombie_gc_interval accepts zero (disable) and is always applied when set.
	if r.ZombieGCInterval != "" {
		d, err := resolveDurationDirective(repl, r.ZombieGCInterval, "zombie_gc_interval")
		if err != nil {
			return nil, err
		}

		opts = append(opts, redistransport.WithZombieGCInterval(d))
	}

	// clock_skew_margin accepts zero (no margin); a negative value is ignored.
	if r.ClockSkewMargin != "" {
		d, err := resolveDurationDirective(repl, r.ClockSkewMargin, "clock_skew_margin")
		if err != nil {
			return nil, err
		}

		if d >= 0 {
			opts = append(opts, redistransport.WithClockSkewMargin(d))
		}
	}

	// subscriber_retry_after accepts 0 (omit the Retry-After header), which
	// differs from omitted (transport default 2s), so it is always applied when
	// set.
	if r.SubscriberRetryAfter != "" {
		d, err := resolveDurationDirective(repl, r.SubscriberRetryAfter, directiveSubscriberRetryAfter)
		if err != nil {
			return nil, err
		}

		if d < 0 {
			return nil, fmt.Errorf("%w: %s=%v", errNegativeOptionValue, directiveSubscriberRetryAfter, d)
		}

		opts = append(opts, redistransport.WithSubscriberRetryAfter(d))
	}

	return opts, nil
}

// resolveDurationDirective resolves placeholders in a non-empty duration
// directive value and parses the result.
func resolveDurationDirective(repl *caddyv2.Replacer, value, name string) (time.Duration, error) {
	resolved, err := resolveDirective(repl, value, name)
	if err != nil {
		return 0, err
	}

	d, err := parseDuration(resolved)
	if err != nil {
		return 0, fmt.Errorf("redis transport: invalid %s: %w", name, err)
	}

	return d, nil
}

// appendPositiveDurationOption resolves placeholders in value, parses it as
// a duration, and appends the produced option when the resolved value is
// non-empty and strictly positive. A negative value is an error, and zero
// appends nothing, so the transport default applies.
func appendPositiveDurationOption(
	opts []redistransport.Option,
	repl *caddyv2.Replacer,
	name, value string,
	build func(time.Duration) redistransport.Option,
) ([]redistransport.Option, error) {
	if value == "" {
		return opts, nil
	}

	d, err := resolveDurationDirective(repl, value, name)
	if err != nil {
		return nil, err
	}

	if d < 0 {
		return nil, fmt.Errorf("%w: %s=%v", errNegativeOptionValue, name, d)
	}

	if d > 0 {
		opts = append(opts, build(d))
	}

	return opts, nil
}

// appendNumericOptions validates (via validateNumericKnobs) then appends the
// options derived from numeric fields. A negative value is rejected, not
// appended.
func (r *Redis) appendNumericOptions(opts []redistransport.Option) ([]redistransport.Option, error) {
	if errs := r.validateNumericKnobs(); len(errs) > 0 {
		return nil, errors.Join(errs...)
	}

	if r.HealthThreshold > 0 {
		opts = append(opts, redistransport.WithHealthThreshold(r.HealthThreshold))
	}

	if r.XReadCount > 0 {
		opts = append(opts, redistransport.WithXReadCount(r.XReadCount))
	}

	if r.DispatchShards != nil {
		opts = append(opts, redistransport.WithDispatchShards(*r.DispatchShards))
	}

	if r.SubscriptionsMaxSubscribers != nil {
		opts = append(opts, redistransport.WithSubscriptionsMaxSubscribers(*r.SubscriptionsMaxSubscribers))
	}

	if r.SubscriberMaxCount != nil {
		opts = append(opts, redistransport.WithSubscriberMaxCount(*r.SubscriberMaxCount))
	}

	if r.SubscriberRateLimit > 0 {
		opts = append(opts, redistransport.WithSubscriberRateLimit(r.SubscriberRateLimit))
	}

	if r.SubscriberRateBurst > 0 {
		opts = append(opts, redistransport.WithSubscriberRateBurst(r.SubscriberRateBurst))
	}

	if r.PublisherRateLimit > 0 {
		opts = append(opts, redistransport.WithPublisherRateLimit(r.PublisherRateLimit))
	}

	if r.PublisherRateBurst > 0 {
		opts = append(opts, redistransport.WithPublisherRateBurst(r.PublisherRateBurst))
	}

	if r.HistoryReplayConcurrency > 0 {
		opts = append(opts, redistransport.WithHistoryReplayConcurrency(r.HistoryReplayConcurrency))
	}

	if r.PresenceDetailThreshold > 0 {
		opts = append(opts, redistransport.WithPresenceDetailThreshold(r.PresenceDetailThreshold))
	}

	if r.PresenceDetailByteThreshold != nil {
		opts = append(opts, redistransport.WithPresenceDetailByteThreshold(*r.PresenceDetailByteThreshold))
	}

	return opts, nil
}

// validateNumericKnobs returns one error per numeric directive set to a
// negative value. The option setters silently ignore a negative (leaving the
// library default), and dispatch_shards would auto-detect to NumCPU, so
// without this an operator typo is invisible. Errors are collected (not
// short-circuited) so every bad value surfaces in one round-trip.
//
// max_length, event_ttl, subscriber_retry_after, the positive-only durations
// and the go-redis pool knobs have their own negative guards; zombie_gc_interval
// and clock_skew_margin have none. This covers the remaining numeric options.
func (r *Redis) validateNumericKnobs() []error {
	var errs []error

	addNeg := func(name string, v int64) {
		if v < 0 {
			errs = append(errs, fmt.Errorf("%w: %s=%d", errNegativeOptionValue, name, v))
		}
	}

	if r.DispatchShards != nil {
		addNeg(directiveDispatchShards, int64(*r.DispatchShards))
	}

	if r.SubscriptionsMaxSubscribers != nil {
		addNeg(directiveSubscriptionsMaxSubscribers, int64(*r.SubscriptionsMaxSubscribers))
	}

	if r.SubscriberMaxCount != nil {
		addNeg(directiveSubscriberMaxCount, int64(*r.SubscriberMaxCount))
	}

	addNeg(directiveHealthThreshold, int64(r.HealthThreshold))
	addNeg(directiveXReadCount, r.XReadCount)
	addNeg(directiveSubscriberRateBurst, int64(r.SubscriberRateBurst))
	addNeg(directivePublisherRateBurst, int64(r.PublisherRateBurst))
	addNeg(directiveHistoryReplayConcurrency, int64(r.HistoryReplayConcurrency))
	addNeg(directivePresenceDetailThreshold, int64(r.PresenceDetailThreshold))

	if r.PresenceDetailByteThreshold != nil {
		addNeg(directivePresenceByteThreshold, *r.PresenceDetailByteThreshold)
	}

	if r.SubscriberRateLimit < 0 {
		errs = append(errs, fmt.Errorf("%w: %s=%v", errNegativeOptionValue, directiveSubscriberRateLimit, r.SubscriberRateLimit))
	}

	if r.PublisherRateLimit < 0 {
		errs = append(errs, fmt.Errorf("%w: %s=%v", errNegativeOptionValue, directivePublisherRateLimit, r.PublisherRateLimit))
	}

	return errs
}

// parseDuration parses s; an empty s yields 0 and no error. The error is
// time.ParseDuration's, which already quotes s; callers add the directive name.
func parseDuration(s string) (time.Duration, error) {
	if s == "" {
		return 0, nil
	}

	return time.ParseDuration(s)
}

// Interface guards. TransportMetricsRegisterer is asserted on
// *redistransport.RedisTransport, the value GetTransport returns and the hub's
// bindTransportMetrics type-asserts, so a signature change fails the build.
var (
	_ caddyv2.Provisioner                     = (*Redis)(nil)
	_ caddyv2.CleanerUpper                    = (*Redis)(nil)
	_ caddyfile.Unmarshaler                   = (*Redis)(nil)
	_ mercurecaddy.Transport                  = (*Redis)(nil)
	_ mercurecaddy.TransportMetricsRegisterer = (*redistransport.RedisTransport)(nil)
)
