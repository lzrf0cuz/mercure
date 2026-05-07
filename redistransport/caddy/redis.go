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
	// `addresses` — they configure the same thing two ways.
	errURLAndAddresses = errors.New("redis transport: cannot set both `url` and `addresses` directives — pick one")

	// errTLSCANoPEM is returned when tls_ca_file points to a file that
	// contains no recognizable PEM CERTIFICATE blocks.
	errTLSCANoPEM = errors.New("redis transport: tls_ca_file contains no valid PEM certificates")

	// errTLSClientAuthIncomplete is returned when only one of the
	// tls_client_auth cert/key paths is set. From a Caddyfile this is
	// only reachable via direct JSON config — the parser's atomic
	// two-arg form prevents the half-set state, and resolveDirective catches
	// any placeholder that resolves to empty before we reach this check.
	errTLSClientAuthIncomplete = errors.New("redis transport: tls_client_auth requires both cert and key files")

	// errPlaceholderUnresolved is returned when a non-empty TLS
	// directive value resolves to an empty string after Caddy
	// placeholder substitution (typically `{env.VAR}` with VAR unset).
	// Surfacing this as a fast-fail prevents operator typos from
	// silently degrading TLS configuration — e.g.,
	// `tls_ca_file {env.CA_TYPO}` would otherwise resolve to "" and
	// fall through to "no custom CA configured" without warning.
	errPlaceholderUnresolved = errors.New("redis transport: directive resolved to empty after placeholder substitution (unset {env.VAR}?)")

	// errUnknownNumericDirective is unreachable in practice — every
	// directive that calls stashRawNumeric also has an applyNumericToken
	// case. Defined as a sentinel so the default-case test can target it
	// directly.
	errUnknownNumericDirective = errors.New("redis transport: internal error: unknown numeric directive in resolveNumericTokens")

	// errUnknownBoolDirective is the boolean counterpart to
	// errUnknownNumericDirective — every directive that calls
	// stashRawBool has an applyBoolToken case.
	errUnknownBoolDirective = errors.New("redis transport: internal error: unknown boolean directive in resolveBoolTokens")

	// errNegativeOptionValue is returned when a numeric or duration
	// directive resolves to a negative value. These are caught at the
	// Caddy parse layer rather than being silently dropped before
	// reaching the transport's strict validation in validateOptions —
	// otherwise an operator typo (`max_length -1`, `event_ttl -1s`)
	// would degrade to the default instead of failing fast.
	errNegativeOptionValue = errors.New("redis transport: option must be >= 0")
)

// encodingGob is the codec value emitted by the bare `gob` Caddyfile
// shortcut and the `encoding gob` long form, keeping dispatch and
// assignment paths consistent.
const encodingGob = "gob"

// Caddyfile directive names that appear in dispatch case clauses, error
// messages, and parser-helper labels — extracted to avoid string-literal
// drift between switch cases and the error-context arguments passed to
// parseBool / parseInt / parseFloat64.
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

// Redis is a Caddy module that wraps redistransport.RedisTransport.
// It mirrors the pattern from caddy/bolt.go and caddy/local.go in the
// upstream Mercure hub.
//
// Caddy `{env.VAR}` placeholders are supported on every Caddyfile
// directive value — connection strings, TLS file paths and names,
// stream key, encoding, durations, AND numeric tunables (max_length,
// dispatch_shards, xread_count, *_rate_limit, *_rate_burst,
// history_replay_concurrency, presence_detail_threshold,
// health_threshold, db). String / duration / connection / TLS values
// resolve through the Caddy replacer in their respective build steps;
// numeric values containing `{` at parse time are stashed in
// r.RawNumeric and resolved + strconv-parsed at Provision via
// resolveNumericTokens. Unset placeholders fast-fail uniformly via
// errPlaceholderUnresolved, so a typo
// (`dispatch_shards {env.SHARD_TYPO}`) surfaces as a clean Provision
// error rather than a silent zero or default.
//
// Boolean directives `tls` and `tls_insecure_skip_verify` ALSO support
// `{env.VAR}` placeholders via the same RawBool stash + Provision-time
// resolve mechanism as numerics — `tls {env.TLS_ENABLED}` works.
// `gob` is deliberately excluded because it is a flag-style shortcut
// for `encoding gob`, not a configuration knob; operators driving
// codec choice from env should use `encoding {env.CODEC}` directly.
type Redis struct {
	// Redis URL (e.g., redis://localhost:6379, rediss://...). Mutually
	// exclusive with `addresses`. URL form is the simplest config for
	// single-instance Redis; use `addresses` for Cluster/Sentinel topologies
	// where multiple seed nodes are required.
	URL string `json:"url,omitempty"`

	// Redis server addresses (e.g., "host1:6379 host2:6379 host3:6379").
	// Multi-address triggers go-redis Cluster mode unless `master_name`
	// is also set, in which case Sentinel mode is selected. Mutually
	// exclusive with `url`.
	Addresses []string `json:"addresses,omitempty"`

	// Sentinel master name. When set, the addresses are treated as Sentinel
	// seed nodes and the failover-aware client is used. Empty for plain
	// Cluster or single-instance topologies.
	MasterName string `json:"master_name,omitempty"`

	// Username for Redis ACL (Redis 6+). Overrides the userinfo segment
	// of `url` when both are set; standalone with `addresses`.
	Username string `json:"username,omitempty"`

	// Password for Redis AUTH or ACL. Overrides the userinfo segment of
	// `url` when both are set; standalone with `addresses`. Prefer
	// env-var injection (e.g., `password {env.REDIS_PASSWORD}`) over
	// inline values.
	Password string `json:"password,omitempty"`

	// Database index (0–15 for standalone Redis; 0 for Cluster). Overrides
	// the path component of `url` when both are set. Pointer so an
	// explicit `db 0` can override a non-zero DB parsed from the URL —
	// without a pointer, the int zero value would be indistinguishable
	// from "directive not set".
	DB *int `json:"db,omitempty"`

	// TLS enables TLS connections with default settings (server name from
	// the address, MinVersion TLS 1.2). The `tls_ca_file`,
	// `tls_client_auth`, `tls_server_name`, and `tls_insecure_skip_verify`
	// directives also imply TLS without requiring the bare `tls` flag.
	TLS bool `json:"tls,omitempty"`

	// TLSCAFile is a path to a PEM file containing the trusted CA
	// certificate(s) used to verify the Redis server's certificate. When
	// set, REPLACES the system root pool — operators who want both bundle
	// system CAs into this file. Matches `openssl -CAfile` semantics.
	// Multi-cert PEM bundles are accepted.
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
	// dialed address. Note: in Cluster mode go-redis applies a single
	// shared TLSConfig.ServerName across all nodes, so this directive only
	// works for clusters whose nodes share a SAN (e.g., wildcard certs).
	TLSServerName string `json:"tls_server_name,omitempty"`

	// TLSInsecureSkipVerify disables certificate verification entirely.
	// Logically additive with the URL's `?skip_verify=true` query param —
	// if either is set, verification is disabled. Emits a Warn log at
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

	// Interval for periodic zombie consumer group GC (default: "5m", "0" to disable).
	ZombieGCInterval string `json:"zombie_gc_interval,omitempty"`

	// Clock skew margin for UUIDv7 timestamp seek (default: "5s").
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

	// XREADGROUP BLOCK timeout (default: "1s").
	XReadBlock string `json:"xread_block,omitempty"`

	// Number of dispatch worker goroutines (default: 1, 0 = NumCPU).
	// Pointer so an explicit `dispatch_shards 0` (auto-detect to NumCPU) is
	// distinguishable from "directive omitted" (default 1): a non-pointer int
	// zero value cannot tell those apart and would resolve an explicit 0 to
	// the default instead of NumCPU.
	DispatchShards *int `json:"dispatch_shards,omitempty"`

	// Pointer so an explicit `subscriptions_max_subscribers 0` (disable the cap) is
	// distinguishable from "directive omitted" (transport default 100000).
	SubscriptionsMaxSubscribers *int `json:"subscriptions_max_subscribers,omitempty"`

	// Admission control (default-off). Per-task concurrent-subscriber ceiling →
	// 429 when exceeded; pointer so an explicit `0` (disable) is distinguishable
	// from omission (both happen to mean "no ceiling" here, but mirrors the other
	// pointer knobs and keeps the negative-rejection symmetric).
	SubscriberMaxCount *int `json:"subscriber_max_count,omitempty"`

	// Bounded wait for a rate token before the gate sheds (default 0 = fail-fast).
	SubscriberAdmissionTimeout string `json:"subscriber_admission_timeout,omitempty"`

	// Bounds post-admission registration (history replay) so a slow backend can't
	// pin an admission slot (default 0 = unbounded — the pre-admission behavior).
	SubscriberRegTimeout string `json:"subscriber_registration_timeout,omitempty"`

	// Base 429 Retry-After delay, jittered per response (default 2s; 0 omits the
	// header). Distinct 0 vs omitted, so it is always applied when set.
	SubscriberRetryAfter string `json:"subscriber_retry_after,omitempty"`

	// Max new subscribers/second (default: 0 = disabled).
	SubscriberRateLimit float64 `json:"subscriber_rate_limit,omitempty"`

	// Burst allowance for subscriber rate limiter (default: 5000).
	SubscriberRateBurst int `json:"subscriber_rate_burst,omitempty"`

	// Max concurrent history replay operations (default: 20).
	HistoryReplayConcurrency int `json:"history_replay_concurrency,omitempty"`

	// Max subscribers before switching to summary presence (default: 1000).
	PresenceDetailThreshold int `json:"presence_detail_threshold,omitempty"`

	// Max marshaled bytes of detail-mode presence payload before
	// summary-mode fallback (default: 524288 = 512 KiB; explicit 0
	// disables the byte budget entirely). Pointer so an explicit
	// `presence_detail_byte_threshold 0` is distinguishable from
	// "directive omitted" (which falls back to the Go default).
	PresenceDetailByteThreshold *int64 `json:"presence_detail_byte_threshold,omitempty"`

	// Max publish operations/second (default: 0 = disabled). Symmetric
	// with subscriber_rate_limit on the publisher side.
	PublisherRateLimit float64 `json:"publisher_rate_limit,omitempty"`

	// go-redis client pool / timeout / retry knobs. Zero-valued fields
	// inherit go-redis library defaults.

	// PoolSize bounds the go-redis connection pool. 0 keeps the
	// library default (10 × runtime.NumCPU on the client side).
	// Raise it when `mercure_redis_client_pool_timeouts_total` is
	// non-zero under load; bound it when Redis `maxclients` is close
	// to saturation (one server connection per pool entry ×
	// hub-replica count).
	PoolSize int `json:"pool_size,omitempty"`

	// MinIdleConns keeps a floor of idle connections warm. 0 keeps
	// the library default (0 — connections open lazily). Set this
	// to a non-zero value when you want to amortize the
	// connection-establishment cost across the first few requests
	// after a hub starts or after a transport reload.
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

	// ReadTimeout caps how long a non-XREADGROUP READ can block on
	// the socket. Empty / "0" keeps the library default (3s).
	// XREADGROUP BLOCK explicitly disables read timeout for itself,
	// so this affects every OTHER command (PING, XADD, XINFO,
	// XRANGE, …) but not the listener's blocking poll.
	ReadTimeout string `json:"read_timeout,omitempty"`

	// WriteTimeout caps socket writes. Empty / "0" keeps the
	// library default (3s, equal to ReadTimeout).
	WriteTimeout string `json:"write_timeout,omitempty"`

	// MaxRetries bounds how many times go-redis will retry a
	// command on a transient failure. 0 keeps the library default
	// (3). -1 disables retries (NoRetry — useful when you want
	// every transient failure to surface in metrics rather than be
	// silently absorbed).
	MaxRetries int `json:"max_retries,omitempty"`

	// Burst allowance for publisher rate limiter (default: 5000).
	PublisherRateBurst int `json:"publisher_rate_burst,omitempty"`

	transport    *redistransport.RedisTransport
	transportKey string

	// RawBool mirrors RawNumeric for boolean-flag directives that
	// contained `{env.VAR}` placeholders at parse time (`tls`,
	// `tls_insecure_skip_verify`). Same exported-with-JSON-tag
	// requirement (Caddy adapter JSON round-trip) and same
	// resolved-and-cleared-before-pool-key invariant. Empty when no
	// boolean directive used a placeholder.
	RawBool map[string]string `json:"raw_bool,omitempty"`

	// RawNumeric stores raw Caddyfile tokens for numeric directives
	// whose values contained `{env.VAR}`-style placeholders at parse
	// time. MUST be exported with a JSON tag — Caddy's adapter pipeline
	// JSON-marshals each module after Caddyfile parsing and
	// JSON-unmarshals into a fresh struct before calling Provision, so
	// an unexported field would be silently stripped during the
	// round-trip and the placeholder would never resolve in production.
	// Provision's resolveNumericTokens substitutes through
	// caddy.NewReplacer().ReplaceKnown, strconv-parses, writes the
	// result into the matching int / float64 / *int field, and CLEARS
	// the map so it doesn't pollute the JSON pool key. After
	// resolution two configs whose only difference is "literal 100" vs
	// "{env.X} resolving to 100" share a pool entry — same effective
	// configuration, same transport instance.
	RawNumeric map[string]string `json:"raw_numeric,omitempty"`
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
// Numeric and boolean placeholders are resolved BEFORE the pool key is
// computed so the key reflects effective configuration: two Caddyfiles
// using different env vars (`max_length {env.A}` vs `{env.B}`) get
// different pool keys, so a Caddyfile-text change reaches a fresh pool
// entry instead of reusing the previous transport (the README "Reload
// caveat" covers the {env.VAR}-value-change limitation). The
// RawNumeric/RawBool maps are cleared during resolution (and tagged
// omitempty), so the key reflects the resolved typed fields, not the raw
// placeholder tokens.
//
// json.Marshal (not gob) computes the key because gob elides a
// pointer-to-zero identically to a nil pointer — that would collapse an
// explicit `dispatch_shards 0` (NumCPU) or `db 0` against an omitted
// directive into the same key, silently sharing one transport across
// semantically different configs. json's omitempty drops nil pointers
// but preserves a non-nil pointer-to-zero, so the tri-state survives.
func (r *Redis) Provision(ctx caddyv2.Context) error {
	if err := r.resolveNumericTokens(); err != nil {
		return err
	}

	if err := r.resolveBoolTokens(); err != nil {
		return err
	}

	key, err := r.poolKey()
	if err != nil {
		return err
	}

	r.transportKey = key

	destructor, _, err := mercurecaddy.TransportUsagePool.LoadOrNew(r.transportKey, func() (caddyv2.Destructor, error) {
		opts, clientOpts, err := r.buildOptions(ctx)
		if err != nil {
			return nil, err
		}

		// UniversalClient handles all three topologies based on options:
		//  - len(Addrs)==1, no MasterName → single-instance (*redis.Client)
		//  - len(Addrs)>1, no MasterName  → Cluster mode (*redis.ClusterClient)
		//  - MasterName set                → Sentinel mode (*redis.FailoverClient)
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
// The cyclop/gocyclo/funlen exemption is intentional: this is a flat
// dispatch over ~35 trivial directive cases, each one a single-line
// delegation to the appropriate per-type parser. Cyclomatic-complexity
// metrics measure decision-logic depth, but a flat directive dispatcher
// has no nested decisions — only one branch per directive name. A
// dispatch-map alternative shifts the same data into a package-level
// map, which trades the cyclop warning for a gochecknoglobals warning
// without improving readability.
//
//nolint:cyclop,funlen,gocyclo // trivial dispatch over ~35 directive keys; each case is one line
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
		return r.parseDB(d, "db")
	case directiveTLS:
		return r.parseBool(d, &r.TLS, directiveTLS)
	case "tls_ca_file":
		return r.parseString(d, &r.TLSCAFile)
	case "tls_client_auth":
		return r.parseClientAuth(d)
	case "tls_server_name":
		return r.parseString(d, &r.TLSServerName)
	case directiveTLSInsecureSkipVerify:
		return r.parseBool(d, &r.TLSInsecureSkipVerify, directiveTLSInsecureSkipVerify)
	case "stream":
		return r.parseString(d, &r.Stream)
	case "encoding":
		return r.parseString(d, &r.Encoding)
	case encodingGob:
		return r.parseGobShortcut(d)
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
		return r.parseInt64(d, &r.MaxLength, directiveMaxLength)
	case directiveXReadCount:
		return r.parseInt64(d, &r.XReadCount, directiveXReadCount)
	case directiveHealthThreshold:
		return r.parseInt(d, &r.HealthThreshold, directiveHealthThreshold)
	case directiveDispatchShards:
		return r.parsePtrInt(d, &r.DispatchShards, directiveDispatchShards)
	case directiveSubscriptionsMaxSubscribers:
		return r.parsePtrInt(d, &r.SubscriptionsMaxSubscribers, directiveSubscriptionsMaxSubscribers)
	case directiveSubscriberMaxCount:
		return r.parsePtrInt(d, &r.SubscriberMaxCount, directiveSubscriberMaxCount)
	case directiveSubscriberAdmissionTimeout:
		return r.parseString(d, &r.SubscriberAdmissionTimeout)
	case directiveSubscriberRegTimeout:
		return r.parseString(d, &r.SubscriberRegTimeout)
	case directiveSubscriberRetryAfter:
		return r.parseString(d, &r.SubscriberRetryAfter)
	case directiveSubscriberRateBurst:
		return r.parseInt(d, &r.SubscriberRateBurst, directiveSubscriberRateBurst)
	case directiveHistoryReplayConcurrency:
		return r.parseInt(d, &r.HistoryReplayConcurrency, directiveHistoryReplayConcurrency)
	case directivePresenceDetailThreshold:
		return r.parseInt(d, &r.PresenceDetailThreshold, directivePresenceDetailThreshold)
	case directivePresenceByteThreshold:
		return r.parsePtrInt64(d, &r.PresenceDetailByteThreshold, directivePresenceByteThreshold)
	case directiveSubscriberRateLimit:
		return r.parseFloat64(d, &r.SubscriberRateLimit, directiveSubscriberRateLimit)
	case directivePublisherRateBurst:
		return r.parseInt(d, &r.PublisherRateBurst, directivePublisherRateBurst)
	case directivePublisherRateLimit:
		return r.parseFloat64(d, &r.PublisherRateLimit, directivePublisherRateLimit)
	case "pool_size":
		return r.parseInt(d, &r.PoolSize, "pool_size")
	case "min_idle_conns":
		return r.parseInt(d, &r.MinIdleConns, "min_idle_conns")
	case directivePoolTimeout:
		return r.parseString(d, &r.PoolTimeout)
	case directiveDialTimeout:
		return r.parseString(d, &r.DialTimeout)
	case directiveReadTimeout:
		return r.parseString(d, &r.ReadTimeout)
	case directiveWriteTimeout:
		return r.parseString(d, &r.WriteTimeout)
	case "max_retries":
		return r.parseInt(d, &r.MaxRetries, "max_retries")
	case "{":
		// A nested block opener at directive position means the operator
		// wrote something like `tls { ... }` — this transport's directive
		// surface is intentionally flat. Surface a hint instead of the
		// opaque "unknown directive: {" message Caddy would otherwise
		// produce via the default case.
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

// parseInt reads one int. When the argument contains a Caddy placeholder
// (`{env.VAR}` etc.) the raw token is stashed in r.RawNumeric under name;
// resolveNumericTokens substitutes and re-parses at Provision time. Any
// other unparseable value still errors at Caddyfile-adapt time so typos
// surface immediately.
func (r *Redis) parseInt(d *caddyfile.Dispenser, target *int, name string) error {
	if !d.NextArg() {
		return d.ArgErr()
	}

	val := d.Val()
	if strings.Contains(val, "{") {
		r.stashRawNumeric(name, val)

		return nil
	}

	n, err := strconv.Atoi(val)
	if err != nil {
		return d.WrapErr(err)
	}

	*target = n

	return nil
}

// parseInt64 mirrors parseInt for int64-typed fields.
func (r *Redis) parseInt64(d *caddyfile.Dispenser, target *int64, name string) error {
	if !d.NextArg() {
		return d.ArgErr()
	}

	val := d.Val()
	if strings.Contains(val, "{") {
		r.stashRawNumeric(name, val)

		return nil
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
func (r *Redis) parsePtrInt64(d *caddyfile.Dispenser, target **int64, name string) error {
	if !d.NextArg() {
		return d.ArgErr()
	}

	val := d.Val()
	if strings.Contains(val, "{") {
		r.stashRawNumeric(name, val)

		return nil
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
func (r *Redis) parsePtrInt(d *caddyfile.Dispenser, target **int, name string) error {
	if !d.NextArg() {
		return d.ArgErr()
	}

	val := d.Val()
	if strings.Contains(val, "{") {
		r.stashRawNumeric(name, val)

		return nil
	}

	n, err := strconv.Atoi(val)
	if err != nil {
		return d.WrapErr(err)
	}

	*target = &n

	return nil
}

// parseFloat64 mirrors parseInt for float64-typed fields.
func (r *Redis) parseFloat64(d *caddyfile.Dispenser, target *float64, name string) error {
	if !d.NextArg() {
		return d.ArgErr()
	}

	val := d.Val()
	if strings.Contains(val, "{") {
		r.stashRawNumeric(name, val)

		return nil
	}

	f, err := strconv.ParseFloat(val, 64)
	if err != nil {
		return d.WrapErr(err)
	}

	*target = f

	return nil
}

// stashRawNumeric stores a placeholder-containing numeric token for
// later Provision-time resolution. Lazily initializes the map.
func (r *Redis) stashRawNumeric(name, raw string) {
	if r.RawNumeric == nil {
		r.RawNumeric = make(map[string]string)
	}

	r.RawNumeric[name] = raw
}

// parseBool accepts a bare flag (`tls`) or an explicit truthy/falsy arg
// (`tls on` / `tls true`). Caddy's directive convention is that flag-style
// directives without an arg mean "true"; that's what `tls` alone implies.
// `{env.VAR}` placeholders are stashed in r.RawBool and resolved at
// Provision time (mirroring parseInt's placeholder handling) so
// operators can drive bool flags from env across deployment tiers.
func (r *Redis) parseBool(d *caddyfile.Dispenser, target *bool, name string) error {
	if !d.NextArg() {
		*target = true

		return nil
	}

	val := d.Val()
	if strings.Contains(val, "{") {
		r.stashRawBool(name, val)

		return nil
	}

	b, err := parseFlexibleBool(val)
	if err != nil {
		return d.WrapErr(err)
	}

	*target = b

	return nil
}

// parseFlexibleBool accepts the strconv.ParseBool set
// ({true, false, 1, 0, t, f, T, F, TRUE, FALSE, True, False}) plus the
// Caddyfile-idiomatic on/off and yes/no spellings (case-insensitive).
// Defining the alias set here keeps Caddyfile ergonomics consistent with
// the README's documented boolean grammar, and ensures the placeholder-
// resolved path (applyBoolToken) accepts the same set as the inline
// parseBool path.
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

// stashRawBool stores a placeholder-containing boolean token for
// later Provision-time resolution. Lazily initializes the map.
func (r *Redis) stashRawBool(name, raw string) {
	if r.RawBool == nil {
		r.RawBool = make(map[string]string)
	}

	r.RawBool[name] = raw
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

// parseDB reads one int and stores it in r.DB as a fresh pointer so an
// explicit `db 0` is distinguishable from "directive omitted" — the
// non-pointer int zero value cannot make that distinction. Placeholder
// values (`db {env.REDIS_DB}`) defer to resolveNumericTokens like the
// other numeric parsers.
func (r *Redis) parseDB(d *caddyfile.Dispenser, name string) error {
	if !d.NextArg() {
		return d.ArgErr()
	}

	val := d.Val()
	if strings.Contains(val, "{") {
		r.stashRawNumeric(name, val)

		return nil
	}

	n, err := strconv.Atoi(val)
	if err != nil {
		return d.WrapErr(err)
	}

	r.DB = &n

	return nil
}

// parseGobShortcut implements the bare `gob` flag — a shortcut for
// `encoding gob`. Bare `gob` enables; `gob true` is the explicit form.
// `gob false` is rejected because the previous "silent no-op"
// behaviour dropped operator intent without warning — a user writing
// `gob false` to disable gob got no feedback about the value being
// ignored. Operators who want to disable gob should omit the directive
// or use `encoding json` / `encoding msgpack` to choose a different
// codec explicitly.
//
// Placeholder values (`gob {env.X}`) are also rejected because `gob`
// is a flag-style shortcut, not a configuration knob — env-driven
// codec selection should use `encoding {env.CODEC}` (the long form)
// where placeholder support is well-defined.
func (r *Redis) parseGobShortcut(d *caddyfile.Dispenser) error {
	if !d.NextArg() {
		r.Encoding = encodingGob

		return nil
	}

	val := d.Val()
	if strings.Contains(val, "{") {
		return d.Err("`gob` is a flag-style shortcut and does not support {env.VAR} placeholders; " +
			"use `encoding {env.CODEC}` instead")
	}

	b, err := strconv.ParseBool(val)
	if err != nil {
		return d.WrapErr(err)
	}

	if !b {
		return d.Err("`gob false` is not supported; omit the `gob` directive or use " +
			"`encoding json` / `encoding msgpack` to disable gob explicitly")
	}

	r.Encoding = encodingGob

	return nil
}

// poolKey derives the transport-sharing key from the resolved config. It uses
// json.Marshal rather than gob: see Provision for why the nil-vs-pointer-to-zero
// distinction (e.g. omitted dispatch_shards vs `dispatch_shards 0`) must survive
// into the key.
func (r *Redis) poolKey() (string, error) {
	// Password is part of the key on purpose (credential-only-differing configs
	// must not share a transport); it is an in-memory map key, never logged.
	//
	// String fields carrying `{env.VAR}` placeholders are intentionally NOT
	// resolved before this key (numeric/bool directives ARE — see
	// RawNumeric/RawBool). So two configs differing only as `url redis://h`
	// vs `url {env.U}` (U=redis://h) do not share a transport. Accepted: the
	// only effect is a duplicate transport when a single process declares the
	// same backend twice in mixed literal/placeholder form (unusual); the
	// `{env.VAR}` reload-staleness is a separate Caddy platform limit.
	b, err := json.Marshal(r) //nolint:gosec // in-memory pool key, never logged/persisted
	if err != nil {
		return "", err
	}

	return string(b), nil
}

// resolveNumericTokens substitutes Caddy placeholders for every numeric
// directive whose Caddyfile value contained `{env.VAR}` and routes the
// resolved value into the corresponding struct field. Called from
// Provision before the JSON pool key is computed so the resolved
// values participate in the key — without this, configs differing only
// in env-driven numerics would compute identical keys (RawNumeric is
// the source of truth at parse time but must NOT survive into the
// key) and silently share a transport instance.
//
// After resolution the map is set to nil for two reasons: (1) it
// MUST NOT participate in the pool key, otherwise literal `100` and
// `{env.X} resolving to 100` would diverge despite expressing the
// same effective configuration; (2) makes Provision cleanly idempotent
// — a second call sees an empty map and the no-op fast path.
//
// The map is empty when no numeric directive used a placeholder — the
// fast common path stays a no-op. Each case parses with the same
// strconv routine the original parser would have used; differences
// would invite drift.
func (r *Redis) resolveNumericTokens() error {
	if len(r.RawNumeric) == 0 {
		return nil
	}

	repl := caddyv2.NewReplacer()

	for name, raw := range r.RawNumeric {
		resolved, err := resolveDirective(repl, raw, name)
		if err != nil {
			return err
		}

		if err := r.applyNumericToken(name, resolved); err != nil {
			return err
		}
	}

	r.RawNumeric = nil

	return nil
}

// resolveBoolTokens is the boolean counterpart to resolveNumericTokens.
// Substitutes Caddy placeholders for every boolean directive whose
// Caddyfile value contained `{env.VAR}` and writes the parsed bool
// into the matching struct field. Called from Provision before the
// JSON pool key — same pool-key-reflects-effective-config
// invariant as the numeric path. After resolution the map is cleared
// so two configs with `tls true` literal vs `tls {env.X}=true` share
// a transport pool entry.
func (r *Redis) resolveBoolTokens() error {
	if len(r.RawBool) == 0 {
		return nil
	}

	repl := caddyv2.NewReplacer()

	for name, raw := range r.RawBool {
		resolved, err := resolveDirective(repl, raw, name)
		if err != nil {
			return err
		}

		if err := r.applyBoolToken(name, resolved); err != nil {
			return err
		}
	}

	r.RawBool = nil

	return nil
}

// applyBoolToken parses a resolved boolean token and writes it into
// the matching struct field. Split from resolveBoolTokens to mirror
// applyNumericToken; the default case targets errUnknownBoolDirective.
// Uses parseFlexibleBool so {env.VAR} resolving to `on`/`off`/`yes`/`no`
// works the same as the inline parseBool path.
func (r *Redis) applyBoolToken(name, resolved string) error {
	b, err := parseFlexibleBool(resolved)
	if err != nil {
		return fmt.Errorf("redis transport: invalid %s after placeholder substitution: %w", name, err)
	}

	switch name {
	case directiveTLS:
		r.TLS = b
	case directiveTLSInsecureSkipVerify:
		r.TLSInsecureSkipVerify = b
	default:
		return fmt.Errorf("%w: %q", errUnknownBoolDirective, name)
	}

	return nil
}

// applyNumericToken parses a resolved numeric token and writes it into
// the matching struct field. Split from resolveNumericTokens so the
// outer loop stays small and lint-friendly.
func (r *Redis) applyNumericToken(name, resolved string) error {
	switch name {
	case "db":
		return r.applyResolvedInt(name, resolved, func(n int) { r.DB = &n })
	case directiveMaxLength:
		return r.applyResolvedInt64(name, resolved, func(n int64) { r.MaxLength = n })
	case directiveXReadCount:
		return r.applyResolvedInt64(name, resolved, func(n int64) { r.XReadCount = n })
	case directiveHealthThreshold:
		return r.applyResolvedInt(name, resolved, func(n int) { r.HealthThreshold = n })
	case directiveDispatchShards:
		return r.applyResolvedInt(name, resolved, func(n int) { r.DispatchShards = &n })
	case directiveSubscriptionsMaxSubscribers:
		return r.applyResolvedInt(name, resolved, func(n int) { r.SubscriptionsMaxSubscribers = &n })
	case directiveSubscriberMaxCount:
		return r.applyResolvedInt(name, resolved, func(n int) { r.SubscriberMaxCount = &n })
	case directiveSubscriberRateBurst:
		return r.applyResolvedInt(name, resolved, func(n int) { r.SubscriberRateBurst = n })
	case directiveHistoryReplayConcurrency:
		return r.applyResolvedInt(name, resolved, func(n int) { r.HistoryReplayConcurrency = n })
	case directivePresenceDetailThreshold:
		return r.applyResolvedInt(name, resolved, func(n int) { r.PresenceDetailThreshold = n })
	case directivePublisherRateBurst:
		return r.applyResolvedInt(name, resolved, func(n int) { r.PublisherRateBurst = n })
	case directiveSubscriberRateLimit:
		return r.applyResolvedFloat64(name, resolved, func(f float64) { r.SubscriberRateLimit = f })
	case directivePublisherRateLimit:
		return r.applyResolvedFloat64(name, resolved, func(f float64) { r.PublisherRateLimit = f })
	case directivePresenceByteThreshold:
		return r.applyResolvedInt64(name, resolved, func(n int64) { r.PresenceDetailByteThreshold = &n })
	case "pool_size":
		return r.applyResolvedInt(name, resolved, func(n int) { r.PoolSize = n })
	case "min_idle_conns":
		return r.applyResolvedInt(name, resolved, func(n int) { r.MinIdleConns = n })
	case "max_retries":
		return r.applyResolvedInt(name, resolved, func(n int) { r.MaxRetries = n })
	default:
		return fmt.Errorf("%w: %q", errUnknownNumericDirective, name)
	}
}

func (*Redis) applyResolvedInt(name, resolved string, set func(int)) error {
	n, err := strconv.Atoi(resolved)
	if err != nil {
		return fmt.Errorf("redis transport: invalid %s after placeholder substitution: %w", name, err)
	}

	set(n)

	return nil
}

func (*Redis) applyResolvedInt64(name, resolved string, set func(int64)) error {
	n, err := strconv.ParseInt(resolved, 10, 64)
	if err != nil {
		return fmt.Errorf("redis transport: invalid %s after placeholder substitution: %w", name, err)
	}

	set(n)

	return nil
}

func (*Redis) applyResolvedFloat64(name, resolved string, set func(float64)) error {
	f, err := strconv.ParseFloat(resolved, 64)
	if err != nil {
		return fmt.Errorf("redis transport: invalid %s after placeholder substitution: %w", name, err)
	}

	set(f)

	return nil
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

// buildOptions converts Caddyfile/JSON fields to a redistransport.Option
// slice and the redis UniversalOptions. Composition split into phased
// helpers; each handles one category of configuration. Numeric
// placeholder resolution happens in Provision before pool-key
// computation so this function can read int / float64 fields
// directly without re-checking r.RawNumeric.
func (r *Redis) buildOptions(ctx caddyv2.Context) ([]redistransport.Option, *redis.UniversalOptions, error) {
	clientOpts, warnings, err := r.buildClientOptions()
	if err != nil {
		return nil, nil, err
	}

	logger := ctx.Slogger()
	for _, w := range warnings {
		logger.Warn(w)
	}

	if r.tlsRequested() || clientOpts.TLSConfig != nil {
		clientOpts.TLSConfig, err = r.buildTLSConfig(clientOpts.TLSConfig)
		if err != nil {
			return nil, nil, err
		}

		if clientOpts.TLSConfig.InsecureSkipVerify {
			logger.Warn(skipVerifyWarning(r.TLSCAFile != ""))
		}
	}

	opts := []redistransport.Option{
		redistransport.WithLogger(logger),
		redistransport.WithPrometheusRegisterer(ctx.GetMetricsRegistry()),
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

	opts = appendCacheSizeOption(ctx, opts)

	return opts, clientOpts, nil
}

// buildClientOptions assembles redis.UniversalOptions from the Caddyfile
// fields. Resolution order:
//
//  1. If `addresses` is set: use it directly (Cluster or Sentinel mode).
//     `url` MUST NOT also be set — that's a config error.
//  2. Otherwise parse `url` (single-instance), then layer the explicit
//     `username` / `password` / `db` / `tls` overrides.
//
// Returns a slice of human-readable warnings when both URL-derived and
// explicit credentials are set and disagree. Layering explicit fields over
// URL-parsed values supports the env-var-injection pattern (placeholder
// credentials in the URL + real values from {env.VAR}); the warnings let
// operators see the conflict at startup so it can't fail silently. The
// caller is expected to log each warning.
//
// TLS materialization (CA file, client cert/key, server name, skip-verify)
// is handled separately in buildTLSConfig, called from buildOptions, since
// it requires file I/O at Provision time after Caddy placeholder
// substitution. This function stays string-pure.
func (r *Redis) buildClientOptions() (*redis.UniversalOptions, []string, error) {
	conn, err := r.resolveConn(caddyv2.NewReplacer())
	if err != nil {
		return nil, nil, err
	}

	if len(conn.addresses) > 0 && conn.url != "" {
		return nil, nil, errURLAndAddresses
	}

	uo := &redis.UniversalOptions{}

	var warnings []string

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

		warnings = collectURLOverrideWarnings(parsed, conn, r.DB)
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
	// go-redis library defaults intact. Duration strings are resolved
	// through the same Caddy replacer used for other Duration values
	// (PresenceTTL, PresenceInterval, …) so {env.VAR} placeholders work.
	if err := r.applyClientTuning(uo); err != nil {
		return nil, warnings, err
	}

	return uo, warnings, nil
}

// applyClientTuning populates the go-redis pool / timeout / retry fields
// on uo from the Caddyfile-supplied values. Each duration string passes
// through resolveDirective (Caddy replacer + numeric-token stash) and
// then parseDuration, mirroring presence_ttl and other duration
// directives. Empty / unset durations preserve the go-redis library
// default. All validation failures are joined via errors.Join so an
// operator sees every misconfiguration in one round-trip.
func (r *Redis) applyClientTuning(uo *redis.UniversalOptions) error {
	errs := r.validateIntegerKnobs()
	errs = append(errs, r.applyDurationKnobs(uo)...)

	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	// Apply integer fields only after all validation has passed so a
	// partially-applied UniversalOptions never escapes on the error
	// path. Note: applyDurationKnobs above mutates uo's duration fields
	// eagerly during its validation loop. That is safe because
	// buildClientOptions (the only caller) returns nil + the joined
	// error when this function fails, discarding uo before any
	// downstream code observes it. Centralizing the eager-vs-deferred
	// split here is a deliberate trade-off — duration parsing is
	// per-field and benefits from the natural fail-fast loop; integer
	// fields are simple assigns and can be deferred without
	// restructuring the validation pass.
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
// max_retries=-1 is the documented sentinel for "disable retries"
// (NoRetry in go-redis); any other negative value is a typo.
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

		resolved, err := resolveDirective(repl, td.raw, td.name)
		if err != nil {
			errs = append(errs, err)

			continue
		}

		d, err := parseDuration(resolved)
		if err != nil {
			errs = append(errs, fmt.Errorf("redis transport: invalid %s %q: %w", td.name, resolved, err))

			continue
		}

		// Negative durations would either silently disable the timeout
		// (e.g., negative dial_timeout = no timeout in go-redis) or
		// cause immediate-expiry behavior — both surprising failure
		// modes for an operator trying to tune. Reject explicitly.
		if d < 0 {
			errs = append(errs, fmt.Errorf("%w: %s=%v", errNegativeOptionValue, td.name, d))

			continue
		}

		// d == 0 ("preserve library default" per the field docstring):
		// skip the assignment so uo's zero-value survives. go-redis
		// currently treats time.Duration(0) as "use library default"
		// for these fields, but skipping makes the contract explicit
		// and removes the dependency on go-redis's internal default-
		// fallback behavior.
		if d == 0 {
			continue
		}

		*td.field = d
	}

	return errs
}

// collectURLOverrideWarnings reports every credential field where the URL
// supplied a non-empty value AND an explicit Caddyfile directive supplied a
// different non-empty value. Compares against POST-replacement credentials
// (conn.username/password) so warnings reflect the effective values that
// will be used, not the literal `{env.VAR}` placeholder strings.
//
// The presence of a warning means the explicit value will be used (our
// layering rule). Surfacing the conflict at startup gives operators a
// chance to confirm the intended value before it becomes a silent auth
// failure.
//
// explicitDB is a pointer so an explicit `db 0` overriding a URL DB N
// produces a warning — without the pointer, the zero-value int could not
// be distinguished from "directive omitted".
func collectURLOverrideWarnings(parsed *redis.Options, conn resolvedConn, explicitDB *int) []string {
	var warnings []string

	if parsed.Username != "" && conn.username != "" && parsed.Username != conn.username {
		warnings = append(warnings, "redis transport: explicit `username` directive overrides username parsed from `url`")
	}

	if parsed.Password != "" && conn.password != "" && parsed.Password != conn.password {
		warnings = append(warnings, "redis transport: explicit `password` directive overrides password parsed from `url`")
	}

	if parsed.DB != 0 && explicitDB != nil && parsed.DB != *explicitDB {
		warnings = append(warnings, "redis transport: explicit `db` directive overrides db parsed from `url`")
	}

	return warnings
}

// tlsRequested reports whether any TLS-related Caddyfile directive
// (bare `tls` flag or any `tls_*`) is set. The caller uses this to
// decide whether to invoke buildTLSConfig — if no TLS is requested
// AND no rediss://-derived config exists, buildTLSConfig is skipped
// entirely (no fictitious "no TLS" return value needed).
func (r *Redis) tlsRequested() bool {
	return r.TLS || r.TLSInsecureSkipVerify ||
		r.TLSCAFile != "" || r.TLSClientAuthCertFile != "" ||
		r.TLSClientAuthKeyFile != "" || r.TLSServerName != ""
}

// skipVerifyWarning returns the consolidated startup-log message for
// effective `InsecureSkipVerify=true` state. Pure (no Slogger
// dependency) so the conditional message composition is unit-testable.
func skipVerifyWarning(caFileSet bool) string {
	msg := "redis transport: tls verification disabled — connection is vulnerable to MITM"
	if caFileSet {
		msg += " (tls_ca_file is set but ignored)"
	}

	return msg
}

// resolveDirective runs a Caddyfile-supplied string value through the
// Caddy replacer and fails fast if a non-empty raw value resolved to
// empty — which typically means an unset `{env.VAR}` placeholder.
// Returns an empty string only when the raw value was already empty
// (i.e., the directive was unset). Used uniformly for both file paths
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
// fields used by buildClientOptions. Resolving up-front lets the
// URL/addresses conflict check, the URL parser, and the override
// layering all operate on the *effective* values an operator
// configured — `password {env.X}` resolves to the secret, not the
// literal placeholder string.
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
// directives. Layers onto an existing config (typically produced by
// redis.ParseURL for a rediss:// URL); creates a fresh config with a
// TLS 1.2 floor when no TLS settings are present in the URL.
//
// Caddy {env.VAR} placeholders in path/name fields are substituted via
// caddy.NewReplacer().ReplaceKnown — matches the pattern used in
// caddy/mercure.go for JWT key fields. Unlike that pattern, we
// fast-fail on placeholders that resolve to empty (see resolveDirective)
// because an empty cert path is meaningless and would otherwise
// silently degrade the configured TLS.
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
// cfg.Certificates. The decrypt hint is attached only when the
// underlying error signature matches an encrypted PKCS#8 key
// ("parse private key") — for other errors (file-not-found,
// cert/key mismatch, no PEM data) the hint would mislead.
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

// appendBasicOptions adds plain-valued (non-parsed) options. String fields
// that operators commonly env-var-inject (Stream, Encoding) run through
// resolveDirective so `stream {env.X}` and `encoding {env.X}` reach the
// transport with the resolved value, and an unset placeholder fast-fails
// rather than silently falling back to the default stream/encoding.
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

	return opts, nil
}

// appendDurationOptions parses every duration-string field and appends an
// Option for each non-empty, valid value. Each duration string runs through
// resolveDirective so `presence_ttl {env.X}` reaches parseDuration with the
// resolved value, and an unset placeholder fast-fails rather than silently
// applying the transport's default. Returns an error on the first invalid
// duration string.
//
//nolint:funlen // one cohesive duration-resolution block with three semantically distinct sub-cases (positive-only, zero-allowed event_ttl, zero-or-positive zombie_gc_interval / clock_skew_margin) that share the resolveDirective + parseDuration plumbing.
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

	// event_ttl accepts 0 (disables time-based retention); skipping zero
	// would silently fall back to the 1h Go default. Operators writing
	// `event_ttl 0` mean "no time-based bound" — pass it through verbatim.
	if r.EventTTL != "" {
		resolved, err := resolveDirective(repl, r.EventTTL, "event_ttl")
		if err != nil {
			return nil, err
		}

		d, err := parseDuration(resolved)
		if err != nil {
			return nil, fmt.Errorf("redis transport: invalid event_ttl: %w", err)
		}

		if d < 0 {
			return nil, fmt.Errorf("%w: event_ttl=%v", errNegativeOptionValue, d)
		}

		opts = append(opts, redistransport.WithEventTTL(d))
	}

	// zombie_gc_interval accepts zero (disable) and is always applied when set.
	if r.ZombieGCInterval != "" {
		resolved, err := resolveDirective(repl, r.ZombieGCInterval, "zombie_gc_interval")
		if err != nil {
			return nil, err
		}

		d, err := parseDuration(resolved)
		if err != nil {
			return nil, fmt.Errorf("redis transport: invalid zombie_gc_interval: %w", err)
		}

		opts = append(opts, redistransport.WithZombieGCInterval(d))
	}

	// clock_skew_margin accepts zero (no margin) but not negative.
	if r.ClockSkewMargin != "" {
		resolved, err := resolveDirective(repl, r.ClockSkewMargin, "clock_skew_margin")
		if err != nil {
			return nil, err
		}

		d, err := parseDuration(resolved)
		if err != nil {
			return nil, fmt.Errorf("redis transport: invalid clock_skew_margin: %w", err)
		}

		if d >= 0 {
			opts = append(opts, redistransport.WithClockSkewMargin(d))
		}
	}

	// subscriber_retry_after accepts 0 (omit the Retry-After header) — distinct
	// from omitted (transport default 2s) — so it is always applied when set.
	if r.SubscriberRetryAfter != "" {
		resolved, err := resolveDirective(repl, r.SubscriberRetryAfter, directiveSubscriberRetryAfter)
		if err != nil {
			return nil, err
		}

		d, err := parseDuration(resolved)
		if err != nil {
			return nil, fmt.Errorf("redis transport: invalid %s: %w", directiveSubscriberRetryAfter, err)
		}

		if d < 0 {
			return nil, fmt.Errorf("%w: %s=%v", errNegativeOptionValue, directiveSubscriberRetryAfter, d)
		}

		opts = append(opts, redistransport.WithSubscriberRetryAfter(d))
	}

	return opts, nil
}

// appendPositiveDurationOption resolves placeholders in value, parses it as
// a duration, and appends the produced option when the resolved value is
// non-empty and strictly positive.
func appendPositiveDurationOption(
	opts []redistransport.Option,
	repl *caddyv2.Replacer,
	name, value string,
	build func(time.Duration) redistransport.Option,
) ([]redistransport.Option, error) {
	if value == "" {
		return opts, nil
	}

	resolved, err := resolveDirective(repl, value, name)
	if err != nil {
		return nil, err
	}

	d, err := parseDuration(resolved)
	if err != nil {
		return nil, fmt.Errorf("redis transport: invalid %s: %w", name, err)
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
// max_length, event_ttl, the duration knobs, and the go-redis pool knobs have
// their own negative guards (appendBasicOptions / appendDurationOptions /
// validateIntegerKnobs); this covers the remaining transport numeric options.
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

// appendCacheSizeOption resolves the subscriber-list cache size from the Caddy
// context (set by the hub) and appends the corresponding option when present.
func appendCacheSizeOption(ctx caddyv2.Context, opts []redistransport.Option) []redistransport.Option {
	size, ok := ctx.Value(mercurecaddy.SubscriberListCacheSizeContextKey).(int)
	if !ok || size <= 0 {
		return opts
	}

	return append(opts, redistransport.WithSubscriberListCacheSize(size))
}

// parseDuration parses a duration string, returning 0 and an error for empty strings.
func parseDuration(s string) (time.Duration, error) {
	if s == "" {
		return 0, nil
	}

	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("parse duration %q: %w", s, err)
	}

	return d, nil
}

// Interface guards. mercurecaddy.TransportMetricsRegisterer is asserted on
// the concrete *redistransport.RedisTransport rather than *Redis: this caddy
// submodule's GetTransport() returns r.transport (the underlying transport),
// and that's what caddy/mercure.go type-asserts post-Provision. Catching the
// guarantee at compile time here means a rename or signature drift in the
// fork-only interface fails the redistransport/caddy build instead of
// silently degrading metrics binding at runtime.
var (
	_ caddyv2.Provisioner                     = (*Redis)(nil)
	_ caddyv2.CleanerUpper                    = (*Redis)(nil)
	_ caddyfile.Unmarshaler                   = (*Redis)(nil)
	_ mercurecaddy.Transport                  = (*Redis)(nil)
	_ mercurecaddy.TransportMetricsRegisterer = (*redistransport.RedisTransport)(nil)
)
