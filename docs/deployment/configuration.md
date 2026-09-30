---
title: "Mercure.rocks hub configuration: Caddyfile and environment variables"
description: "Configure the Mercure.rocks Hub with Caddyfile directives, environment variables, transports, CORS, and JWKS validation."
---

# Mercure configuration

The Mercure.rocks Hub is a [Caddy](https://caddyserver.com/) build with the Mercure module. Anything the [Caddy docs](https://caddyserver.com/docs/) describe also applies to this binary.

The bundled `Caddyfile` supports environment variables for common settings. For more control, write a custom Caddyfile or use [Caddy JSON configuration](https://caddyserver.com/docs/json/).

> [!NOTE]
> The bundled Caddyfiles set the `persist_config off` global option. They are loaded from a file (`-c <Caddyfile>`, never `--resume`), so Caddy's `autosave.json` would never be read, and writing it fails on a read-only root filesystem. Don't copy the option into a Caddy managed through the admin API and started with `--resume` (for example a `caddy-api.service` unit): there `autosave.json` is the live configuration.

## Minimal Caddyfile

```caddyfile
hub.example.com {
  mercure {
    issuer https://example.com {
      publisher {
        jwt {env.MERCURE_PUBLISHER_JWT_KEY}
      }
      subscriber {
        jwt {env.MERCURE_SUBSCRIBER_JWT_KEY}
      }
    }
    resource_identifier https://hub.example.com/.well-known/mercure
    cors_origins        https://example.com
  }

  respond "Not Found" 404
}
```

Each `issuer` binds a trusted issuer (the value accepted in the token `iss` claim, RFC 9068 §4) to its own verification material, so a token is verified only with the key(s) of the issuer it claims. Repeat the block to trust several issuers with distinct keys.

The identifier is the stable identifier of whoever signs the tokens: your app's URL when it signs them itself, or the authorization server's issuer identifier. Add `authorization_server` inside the block to advertise that issuer in the [protected resource metadata](../concepts/discovery.md).

Inside `publisher`/`subscriber`, use `jwt <key> [<algorithm>]` for a shared secret or public key, or `jwks_uri <url> [<algorithm>...]` for a JWK Set. The two are mutually exclusive.

The algorithm defaults to `HS256` only for a raw shared secret. A PEM-encoded key must state its algorithm, and that algorithm must not be an HMAC one: the hub refuses to start otherwise, because verifying with `HS*` would use the public key as the shared secret and let anyone holding it forge tokens.

An HMAC secret shorter than the hash output ([RFC 7518 §3.2](https://www.rfc-editor.org/rfc/rfc7518#section-3.2): 32 bytes for `HS256`, 48 for `HS384`, 64 for `HS512`) still works but logs a warning at startup; generate one with `openssl rand -base64 32`.

`resource_identifier` is the OAuth 2.0 audience that access tokens must carry in their `aud` claim (see [Authorization](../concepts/authorization.md)). Leave it unset and the hub derives it from each request (the public URL the client contacted), so a hub reachable through several domains needs no configuration; set it only to pin one canonical audience shared across every domain. A pinned value also becomes the `resource` of the [protected resource metadata](../concepts/discovery.md), and the scheme and host of the `resource_metadata` URL in `WWW-Authenticate` challenges are taken from it, so it must be a URL on a host that serves that metadata.

`resource_identifier` and `public_urls` answer different questions and are independent: `resource_identifier` sets the token audience, while `public_urls` restricts which origins the hub answers on (rejecting others with `421`). A single `resource_identifier` is what lets one token work across several public URLs, since a per-request-derived audience is specific to the host the client contacted.

Caddy provisions a Let's Encrypt certificate for `hub.example.com` automatically. To disable HTTPS (when behind a reverse proxy that terminates TLS), prefix the site address with `http://`:

```caddyfile
http://hub.example.com:80 {
  # ...
}
```

Setting the port to 80 also disables HTTPS implicitly.

## Mercure directives

| Directive                                  | Description                                                                                                                                                 | Default                         |
| ------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------- |
| `issuer <id> { … }`                        | Bind a trusted issuer to its verification material. Repeatable. See [issuer blocks](#issuer-blocks).                                                        |                                 |
| `resource_identifier <id>`                 | Pin the OAuth 2.0 resource identifier (token `aud`); unset, it is derived per request. See [Discovery](../concepts/discovery.md).                           | derived per request             |
| `public_urls <url...>`                     | Public URLs the hub answers on (scheme pinned); an unlisted origin gets `421 Misdirected Request`. Set it on a catch-all site.                              | site host matching              |
| `name <name>`                              | Hub name for health checks and transport sharing. Equal names share a transport; at most one hub per configuration may omit its name.                       | `default`                       |
| `anonymous`                                | Allow subscribers without a token to receive **public** updates.                                                                                            | off                             |
| `publish_origins <origin...>`              | Origins allowed to publish (cookie-based auth only). A wildcard must stay within one registrable domain, such as `https://*.example.com`.                   |                                 |
| `cors_origins <origin...>`                 | CORS allowed origins. See [CORS](#cors).                                                                                                                    |                                 |
| `require_claim_header <claim> <header>`    | Require a request header to agree with a token claim; takes an optional block. Repeatable. See [claim bindings](#claim-bindings).                           | off                             |
| `require_claim_value <claim> <value...>`   | Require a token claim to hold one of the values; takes an optional block. Repeatable. See [claim bindings](#claim-bindings).                                | off                             |
| `cookie_name <name>`                       | Cookie that carries the access token for browser clients. Use a prefix-less name only for local HTTP development.                                           | `__Secure-mercure_access_token` |
| `protocol_version_compatibility <version>` | Accept 0.x behaviors (`7` or `8`). Requires the `deprecated_topic` / `deprecated_claim` build tags. See [Upgrade](../UPGRADE.md).                           | off                             |
| `subscriptions`                            | Enable subscription events and the [subscription API](../concepts/active-subscriptions.md).                                                                 | off                             |
| `heartbeat <duration>`                     | Interval between SSE heartbeat comments. `0s` to disable.                                                                                                   | `40s`                           |
| `max_request_body_size <size>`             | Maximum size of publish and QUERY subscribe request bodies (e.g. `512KB`); larger requests get a `413`. `0` delegates to a reverse proxy.                   | `1MiB`                          |
| `transport <name> [{ <options...> }]`      | Transport configuration. See [Transports](#mercure-hub-transports).                                                                                         | `bolt`                          |
| `dispatch_timeout <duration>`              | Max time to dispatch one update to one subscriber. `0s` disables.                                                                                           | `5s`                            |
| `publish_timeout <duration>`               | Max time to dispatch one publish; on expiry `504` (update may be stored). Only [Redis](../../redistransport/README.md#configuration-reference) honours it; Bolt and Local ignore it. `0s` or negative disables.       | off                             |
| `subscriber_out_buffer <size>`             | Per-subscriber buffer, allocated up front; min 16, `0` = default. No upper bound is enforced: huge values waste memory or fail subscriptions.               | `1000`                          |
| `write_timeout <duration>`                 | Max duration of a subscriber connection. `0s` disables. See [Rolling updates](../production/rolling-updates.md).                                            | `600s`                          |
| `topic_matcher_cache <maxEntries>`         | Cache for topic matcher evaluations, sized in entries of ~100 B (longer topics count for more). `0` or negative disables it.                                | `100000`                        |
| `subscriber_list_cache_size <maxSize>`     | Subscriber list cache size, for Bolt and Local; no effect with Redis. `0` for unbounded.                                                                    | `100000`                        |
| `debugger`                                 | Serve the debugger UI at `/.well-known/mercure/debug/`; it uses a token you paste. No playground endpoints. Safe in production.                             | off                             |
| `playground`                               | Enable `debugger` **and** the insecure playground: `/playground/` endpoints, permissive defaults, an all-access UI token (static HMAC key only). Dev only.  | off                             |

`write_timeout` also bounds how long a stop takes. Caddy's default `grace_period` is unlimited, so `caddy stop` or `SIGTERM` waits until every open subscriber's connection reaches `write_timeout` (up to 10 minutes by default). Set Caddy's `grace_period` global option to bound it; subscribers still connected when it ends are disconnected. With `write_timeout 0`, the hub disconnects every subscriber as soon as a stop or an applied config reload begins; see [Rolling updates](../production/rolling-updates.md) for connection expiry in that mode. The Caddyfiles bundled with this repository's images also set a short `grace_period`; see [Rolling updates](../production/rolling-updates.md).

`subscriber_out_buffer` also caps how many history updates one connection replays before it is disconnected. A far-behind `Last-Event-ID` client then needs several reconnects: with Bolt or Redis no data is lost, with Local the overflow is dropped.

`debugger` serves a browser client that uses the token you provide. The hub serves it with `Content-Security-Policy: default-src 'self'`, so it reaches only hubs and topics on its own origin. `playground` adds an all-access token, minted only when the hub verifies with a static HMAC key (an application embedding the hub library must also set `WithPlaygroundTokenFunc`); otherwise paste one. It also adds permissive defaults: anonymous subscription, the subscriptions API, and `*` for `cors_origins` and `publish_origins` unless set. The UI's fixture-key and dev-secret copy tools work only there. Use `playground` only for development. For a protected hub, mint a scoped token with [`caddy mercure-token`](../concepts/authorization.md#minting-a-token).

The playground echo endpoints serve `.json` paths as `application/json`, `.jsonld` paths as `application/ld+json`, and all other paths as `text/plain; charset=utf-8`.
They return the `body` query parameter unchanged, with `X-Content-Type-Options: nosniff` and `Content-Security-Policy: sandbox; default-src 'none'` to prevent it from executing in the browser. These restrictions do not apply to the debugger UI.

### Issuer blocks

An `issuer` block binds a trusted issuer to its own verification material:

```caddyfile
issuer https://issuer-a.example {
  authorization_server            # advertise in the protected resource metadata
  publisher {
    jwt !ChangeThisSecret! HS256  # shared secret or PEM public key + algorithm
  }
  subscriber {
    jwks_uri https://issuer-a.example/jwks RS256  # JWK Set URL + allowed algorithms
  }
}

issuer https://issuer-b.example {
  publisher {
    jwks_uri https://issuer-b.example/jwks
  }
  subscriber {
    jwks_uri https://issuer-b.example/jwks
  }
}
```

| Sub-directive                     | Description                                                                                            |
| --------------------------------- | ------------------------------------------------------------------------------------------------------ |
| `authorization_server`            | Advertise this issuer in the [protected resource metadata](../concepts/discovery.md). Off by default.  |
| `publisher { … }`                 | Verification material for publisher tokens. Omit to reject publishing for this issuer.                 |
| `subscriber { … }`                | Verification material for subscriber tokens. Omit to reject subscribing for this issuer.               |
| `jwt <key> [<algorithm>]`         | Shared secret or PEM public key, plus algorithm. A PEM key must set a non-HMAC one (see above).        |
| `jwks_uri <url> [<algorithm>...]` | JWK Set URL and its allowed algorithms (defaults to the asymmetric allowlist). Accepts `file://` URLs. |

`jwt` and `jwks_uri` are mutually exclusive within a `publisher`/`subscriber` block.

> [!WARNING]
> The pre-1.0 top-level directives `publisher_jwt`, `subscriber_jwt`, `publisher_jwks_url` and `subscriber_jwks_url` are deprecated. They map to a single implicit issuer and only work in [compatibility mode](../UPGRADE.md); modern mode requires an `issuer` block.

## Mercure hub environment variables

The Docker image and the official Caddyfile read these:

| Variable                        | Description                                                                            | Default             |
| ------------------------------- | -------------------------------------------------------------------------------------- | ------------------- |
| `SERVER_NAME`                   | Site address. Use `:80` to bind without a hostname.                                    | `localhost`         |
| `MERCURE_PUBLISHER_JWT_KEY`     | Publisher verification secret or public key.                                           |                     |
| `MERCURE_PUBLISHER_JWT_ALG`     | Publisher algorithm.                                                                   | `HS256`             |
| `MERCURE_SUBSCRIBER_JWT_KEY`    | Subscriber verification secret or public key.                                          |                     |
| `MERCURE_SUBSCRIBER_JWT_ALG`    | Subscriber algorithm.                                                                  | `HS256`             |
| `MERCURE_TRUSTED_ISSUERS`       | Sets the `issuer` block identifier (the token `iss`).                                  | `https://localhost` |
| `MERCURE_ISSUER_MODE`           | Token verification: `keys` or `jwks`. Unset means `keys`; empty or unknown fails.      | `keys`              |
| `MERCURE_PUBLISHER_JWKS_URI`    | Publisher JWK Set URL (`jwks` mode). Accepts `file://`.                                |                     |
| `MERCURE_SUBSCRIBER_JWKS_URI`   | Subscriber JWK Set URL (`jwks` mode). Accepts `file://`.                               |                     |
| `MERCURE_EXTRA_DIRECTIVES`      | Additional Mercure directives. One per line.                                           |                     |
| `MERCURE_HUB_NAME`              | Hub `name`: per-hub health paths and transport pool key (this repository's Caddyfiles). | `default`           |
| `MERCURE_TRACING_DIRECTIVE`     | `tracing` emits [OpenTelemetry spans](../production/tracing.md) (this repository's Caddyfiles). |                     |
| `GLOBAL_OPTIONS`                | Caddy [global options](https://caddyserver.com/docs/caddyfile/options#global-options). |                     |
| `CADDY_EXTRA_CONFIG`            | [Snippets / named routes](https://caddyserver.com/docs/caddyfile/concepts#snippets).   |                     |
| `CADDY_SERVER_EXTRA_DIRECTIVES` | Caddyfile directives outside the `mercure` block.                                      |                     |
| `MERCURE_LICENSE`               | License key for [Self-Hosted Mercure](https://mercure.rocks/pricing).                  |                     |

The bundled Caddyfile verifies tokens with the `MERCURE_*_JWT_KEY` variables by default. To verify through JWK Sets only, set `MERCURE_ISSUER_MODE=jwks` and both URLs (they may differ). The URL variables take literal URLs: `{env.*}` placeholders are not substituted there. The hub fails to start if either is unset:

```console
MERCURE_ISSUER_MODE=jwks
MERCURE_TRUSTED_ISSUERS=https://issuer.example.com
MERCURE_PUBLISHER_JWKS_URI=https://issuer.example.com/publisher/jwks.json
MERCURE_SUBSCRIBER_JWKS_URI=https://issuer.example.com/subscriber/jwks.json
```

Use your deployment's secret store for credentials. The JWT directives accept runtime `{env.MY_SECRET}` placeholders. Placeholder support depends on the module: follow the [Enterprise transport examples](../production/high-availability.md#self-hosted-transports) for shared-backend credentials.

## Mercure hub transports

The transport stores history and (in clustered builds) synchronizes between nodes.

`bolt` and `local` are transport modules (`http.handlers.mercure.bolt`, `http.handlers.mercure.local`). Configure them only through the `mercure` handler's `transport` (the `transport` directive, or the handler's `transport` field in JSON), never as HTTP handlers. A route handler named `mercure.bolt` or `mercure.local` makes the configuration fail to load: at startup Caddy exits with a panic, and a load through the admin API is aborted, leaving the previous configuration running.

### Bolt transport (default, single-node)

```caddyfile
mercure {
  transport bolt {
    path /data/mercure.db
    size 0
    cleanup_frequency 0.3
  }
  # ...
}
```

| Option              | Description                                                                                                                   |
| ------------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| `path`              | Path to the BoltDB file. Default: `mercure.db` in Caddy's application data directory (`/data/caddy/mercure.db` in the image). |
| `bucket_name`       | Bucket name. Default: `updates`.                                                                                              |
| `cleanup_frequency` | Probability per publish of running history cleanup; set explicitly when using `size`. `0` (never) to `1` (always).            |
| `size`              | Retention target; cleanup is probabilistic. `0` for **unlimited** (default; bound only by disk size).                         |

With `size 0`, BoltDB does not prune history automatically. Set both `size` and a positive `cleanup_frequency` to enable retention cleanup.

A config reload keeps the open database, so changing `bucket_name`, `size` or `cleanup_frequency`, or renaming the hub that uses the file, needs a Caddy restart: such a reload is refused and the running configuration stays in place. The hub's `subscriber_list_cache_size` also needs a restart: a reload that changes it is refused for both the Bolt and Local transports. Validating the configuration (`caddy validate`) opens the database too, so it fails with an "already open" error while a running hub holds the same file.

### Local transport (no history)

`transport local` disables history entirely. Use it when reconnect replay isn't needed and you want the lowest possible memory footprint.

### Shared transports

[Mercure Enterprise](https://mercure.rocks/pricing) includes Redis/Valkey, PostgreSQL, Kafka, and Pulsar transports to distribute updates and share history across hub instances. See [transport configuration examples](../production/high-availability.md#self-hosted-transports). Prefer a managed hub? [Mercure Cloud](https://mercure.rocks/pricing) handles the infrastructure for you.

This repository also builds an open-source Redis/Valkey Streams transport. Its configuration is in the [Redis transport README](../../redistransport/README.md#configuration-reference).

## CORS

If the page that opens the SSE connection is on a different origin than the hub, you must list it in `cors_origins`:

```caddyfile
mercure {
  cors_origins https://app.example.com https://admin.example.com
}
```

`cors_origins *` allows cross-origin requests without cookies. For `EventSource` with `withCredentials: true`, list explicit origins. Sending an `Authorization` header with `fetch` also requires a successful CORS preflight.

The preflight allows the `Authorization`, `Cache-Control` and `Last-Event-ID` request headers, plus every header bound by a [`require_claim_header`](#claim-bindings) directive. Cross-origin clients can read the `Link`, `Mercure-Last-Event-Id`, `Accept-Query` and `Retry-After` response headers; `Retry-After` accompanies a `429` only from a transport that sheds subscribers (the Redis transport's admission control).

Avoid listing the literal `null` origin: browsers send `Origin: null` for sandboxed iframes, `data:` URLs, and local files, so allowlisting it would send credentialed responses to any such opaque context.

To avoid CORS, expose the hub on the **same origin** as your application: the same scheme, hostname, and port. Sibling subdomains are different origins. See [Reverse proxies](reverse-proxy.md).

## Claim bindings

A claim binding ties a request to a claim of its access token. `require_claim_header` requires a request header to agree with the claim, so a token issued for one group cannot be replayed against another. `require_claim_value` requires the claim to hold at least one of a fixed set of values, and reads no header. The hub attaches no meaning to the claim and header names, except that it refuses the few described below: claims that never hold a string or a string array, and the `*` and `Host` header names.

```caddyfile
mercure {
  issuer https://example.com {
    publisher {
      jwt {env.MERCURE_PUBLISHER_JWT_KEY}
    }
    subscriber {
      jwt {env.MERCURE_SUBSCRIBER_JWT_KEY}
    }
  }
  require_claim_header groups Group-ID {
    match      member
    on_missing reject
    roles      subscriber
    count_subscribers
  }
  require_claim_value plan pro enterprise {
    roles publisher
  }
}
```

With this configuration, a subscriber whose token carries `"groups": ["red", "blue"]` connects only with `Group-ID: red` or `Group-ID: blue`, and a publisher's token must carry `"plan": "pro"` or `"plan": "enterprise"`, or an array holding one of them.

Both directives are repeatable, and every binding that applies to a request must hold. Two `require_claim_header` directives on the same claim and header, or two `require_claim_value` directives on the same claim, are a startup error. Values must be non-empty and unique. Claim names, values and header names are literals, never replaced at runtime: any `{…}` sequence in them is a startup error, a runtime placeholder such as `{env.VAR}` and a braced GUID alike. To take one from the environment in a Caddyfile, write `"{$VAR}"` (quoted) or `{$VAR:default}`. Caddy substitutes `{$VAR}` as text before splitting the line into arguments, so an unset, unquoted variable vanishes and shifts the arguments: `require_claim_value {$P} pro enterprise` would bind the claim `pro` to the value `enterprise`. Quoted, an unset variable leaves an empty argument, and the hub refuses to start. In Caddy JSON, write the literal.

| Sub-directive                      | Description                                                                                                                                                                                                                          | Default  |
| ---------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | -------- |
| `match auto\|member\|exact`        | Which claim shapes are accepted. `auto` takes either: a string must equal the header or one of the values, an array must contain it. `member` accepts only an array, `exact` only a string. A claim of the other shape is malformed. | `auto`   |
| `on_missing reject\|allow`         | Whether a missing header or claim rejects the request or skips the binding. `allow` skips only genuine absence: a malformed header or claim still rejects.                                                                           | `reject` |
| `roles all\|subscriber\|publisher` | Which requests the binding applies to.                                                                                                                                                                                               | `all`    |
| `count_subscribers`                | `require_claim_header` only. Count connected subscribers per matched header value in `mercure_subscribers_by_binding_value`. At most one binding may set it, and it must apply to subscribers.                                       | off      |

A binding must have a key to verify against: a binding that applies to subscribers needs a `subscriber` verifier, and one that applies to publishers a `publisher` verifier, or the hub refuses to start. A binding cannot name a claim that never holds a string or a string array (`authorization_details`, `mercure`, `https://mercure.rocks/`, `exp`, `nbf`, `iat`), and cannot bind the `*` header name, which would make the [CORS](#cors) preflight allow every request header. Claim values are compared case-sensitively; header names are matched case-insensitively.

A bound claim that is `null`, an empty string or an empty array counts as missing: `403` under `on_missing reject`, skipped under `allow`. The exception is a shape `match` forbids: an empty string under `match member` and an empty array under `match exact` are malformed (`401`).

Bindings run only on a request whose token the hub has accepted. A request without a token is answered as without bindings: an anonymous subscriber skips every binding, and the hub warns at startup when a binding that applies to subscribers is combined with `anonymous`. A rejected request is answered as follows:

| Failure                                                                                                                                                                         | Status                     |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------- |
| The bound header is missing (with `on_missing reject`), empty or repeated                                                                                                       | `400`                      |
| The bound claim is present but unusable: a number, an object, a boolean, an array holding a non-string or an empty string or more than 1000 entries, or a shape `match` forbids | `401` `invalid_token`      |
| The bound claim is missing (with `on_missing reject`), or holds neither the header value nor any of the values                                                                  | `403` `insufficient_scope` |
| No token, or an invalid token                                                                                                                                                   | `401`, as without bindings |

The checks run in three tiers across every binding that applies: first every bound header, then every bound claim's type, then presence and comparison. A request is answered by the first tier that fails, so the status never depends on the order the bindings are configured in.

A bound header must arrive exactly once. A request repeating it is answered with `400`. A proxy that folds the repeats into one comma-separated line turns them into a single value, which is compared literally and answered with `403` unless the claim holds that exact string.

`Host` cannot be bound: over HTTP/1.1 Go moves it out of the request headers, and over HTTP/2 and HTTP/3 a client may send its own value, so a binding on it would never mean anything.

With any binding configured, the playground stops working for the requests it applies to: its minted token carries no custom claims, so under `on_missing reject` a binding on a custom claim rejects its requests, with `400` while the header a `require_claim_header` binding requires is missing, and with `403` otherwise (the header is present, or only a `require_claim_value` binding applies). The debugger UI sends only its access token (the `Authorization` header or the cookie) unless you set Settings > Advanced Options > Extra Request Header (a name and a value): give it the header a `require_claim_header` binding requires (for example `Group-ID` and `red` above) and a token that carries the claim. The browser's native `EventSource` cannot set request headers either: subscribers that must send a bound header need a `fetch`-based SSE client. When `cors_origins` is set, bound headers are allowed in the [CORS](#cors) preflight.

Rejections are logged at info level and counted in `mercure_claim_header_rejected_total` or `mercure_claim_value_rejected_total`; see the [metrics reference](../production/metrics.md#claim-bindings) for labels and reasons.

`count_subscribers` counts only subscribers whose binding matched; see [`mercure_subscribers_by_binding_value`](../production/metrics.md#mercure_subscribers_by_binding_value). All hubs in one Caddy configuration share these counts, so set `count_subscribers` on one hub only.

In Caddy JSON, the bindings are the handler's `require_claim_headers` (`claim`, `header`, `match`, `on_missing`, `roles`, `count_subscribers`) and `require_claim_values` (`claim`, `values`, `match`, `on_missing`, `roles`) arrays.

## JWT validation via JWKS

When tokens are minted by an external IdP (Keycloak, Cognito, Auth0):

```caddyfile
mercure {
  issuer https://idp.example.com {
    authorization_server
    publisher {
      jwks_uri https://idp.example.com/.well-known/jwks.json
    }
    subscriber {
      jwks_uri https://idp.example.com/.well-known/jwks.json
    }
  }
}
```

The hub fetches and caches the keys, validates each token's `kid` against them, and rotates automatically when the IdP rotates. Token issuance stays with the IdP; the hub only verifies.

The first fetch happens when the configuration loads, once per verifier: a `publisher` and a `subscriber` fetch separately even when they share a URL, and the verifiers fetch one after another. The configuration is rejected rather than started without keys if the URL is unreachable, answers with a status other than 200, returns an invalid key set or one holding no key the hub could decode, or gives no answer within 10 seconds; with several verifiers, a load can wait up to 10 seconds for each. A set the hub could decode may still hold no key able to verify a token (for example only `X25519` keys, or only `oct` keys under the default asymmetric allowlist); that is not detected at load.

The hub then refreshes the key set every 5 minutes, each refresh also bounded by 10 seconds:

- A failed refresh (unreachable, non-200, a body that is not JSON) is logged at error level, and the last good set stays in use.
- A refresh that gets HTTP 200 with a JSON body holding no key, such as `{}` or an error object, silently replaces the keys with nothing: no log line is written, and every token is rejected until a later refresh brings keys back.
- A token with an unknown `kid` triggers an early refetch, at most once every 5 minutes. It runs while handling that request, and can hold it for up to 1 minute.

A key the IdP removes therefore stays accepted until the next successful refresh: 5 minutes plus the fetch time, or longer while refreshes fail.

A JWK Set URL must not carry credentials: no user information (`https://user:password@…`) and no token in the query. The URL appears in startup errors, in admin API responses and in refresh-failure logs. A JWK Set holds public keys: serve it without credentials, or restrict who can reach it at the network level.

Because the key set is fetched while the configuration loads, two setups are not supported:

- Serving the JWK Set from the same Caddy configuration as the hub. Caddy provisions every module before any listener starts, so at startup the fetch finds nothing to answer it. It can appear to work on a reload, because the previous configuration's listener still answers, and then fail on the next cold start. Serve the JWK Set from something other than this Caddy configuration.
- Validating the configuration (`caddy validate`) without network access to the JWK Set URL: validation fetches the key set too.

`jwks_uri` also accepts `file://` URLs, read once at provision time, for keys mounted as files. The supported form is `file:///absolute/path/to/jwks.json`, naming a regular local file, with the host empty or `localhost` (`file://localhost/absolute/path`). A relative `file:` URL (`file:jwks.json`, `file://./jwks.json`) fails the configuration load. A file holding no key the hub could decode is rejected the same way, and so is a file containing any key of an unsupported type. Rotating file-based keys requires `caddy reload --force`: a plain reload of an unchanged configuration is skipped, so the file is not read again. The deprecated `publisher_jwks_url` and `subscriber_jwks_url` directives load their URL the same way. Append algorithms to pin the allowlist (e.g. `jwks_uri <url> RS256 ES256`); it defaults to the asymmetric algorithms.

## OAuth 2.0 protected resource metadata

When the hub validates tokens, it serves [protected resource metadata](../concepts/discovery.md) (RFC 9728) at `/.well-known/oauth-protected-resource/.well-known/mercure`. Advertise the authorization servers that issue tokens so clients can discover where to obtain one:

```caddyfile
mercure {
  resource_identifier https://hub.example.com/.well-known/mercure
  issuer https://auth.example.com {
    authorization_server
    publisher {
      jwks_uri https://auth.example.com/jwks
    }
    subscriber {
      jwks_uri https://auth.example.com/jwks
    }
  }
}
```

## Keeping tokens out of logs

Modern clients must send tokens in a header or cookie. The hub accepts the old `authorization` query parameter only in [compatibility mode](../UPGRADE.md#compatibility-mode). Redact it from access logs while migrating legacy clients:

```caddyfile
log {
  format filter {
    fields {
      request>uri query {
        replace authorization REDACTED
      }
    }
  }
}
```

## RSA / ECDSA keys

```console
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:4096 -out publisher.key
openssl pkey -in publisher.key -pubout -out publisher.key.pub
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:4096 -out subscriber.key
openssl pkey -in subscriber.key -pubout -out subscriber.key.pub
```

Start the hub with the public key for verification and the algorithm:

```console
MERCURE_PUBLISHER_JWT_KEY="$(cat publisher.key.pub)" \
MERCURE_PUBLISHER_JWT_ALG=RS256 \
MERCURE_SUBSCRIBER_JWT_KEY="$(cat subscriber.key.pub)" \
MERCURE_SUBSCRIBER_JWT_ALG=RS256 \
./mercure run --config Caddyfile
```

## Mercure hub health check endpoints

The Caddy admin API (default `localhost:2019`) exposes:

| Endpoint                           | Description                                                 |
| ---------------------------------- | ----------------------------------------------------------- |
| `GET /mercure/health/ready`        | `200` if all transports can serve traffic, `503` otherwise. |
| `GET /mercure/health/live`         | `200` if all transports are fundamentally operational.      |
| `GET /mercure/health/{name}/ready` | Per-hub readiness (when running multiple).                  |
| `GET /mercure/health/{name}/live`  | Per-hub liveness.                                           |

The endpoints bind to `localhost` for security. Probes from outside the container should use `kubectl exec` or `docker exec`, or the `/mercure/health/*` path that the bundled Caddyfiles proxy on the public listener (see [Health monitoring](../production/health-monitoring.md)). Binding the admin API to `0.0.0.0:2019` works but exposes `/stop` and `/load` to the pod network. Restrict access if you use that configuration.

## Mercure hub performance tuning

Tune these settings using measurements from your workload:

- `dispatch_timeout`: too low and slow subscribers get cut off; too high and a stuck dispatch ties up resources. The 5s default is a reasonable starting point.
- `write_timeout`: controls how often each subscriber rotates its connection in steady state. Higher values mean fewer reconnects but worse drain pacing on shutdown. See [Rolling updates](../production/rolling-updates.md).
- `topic_matcher_cache` and `subscriber_list_cache_size`: increase if your hub has many distinct matchers and you see CPU spent in matcher evaluation. Decrease if memory is tight.
- File descriptors: each TCP connection consumes one; HTTP/2 streams can share a connection. `ulimit -n 100000` on the host (or the equivalent in your orchestrator) for high-fanout hubs.

[Load testing](../production/load-testing.md) and [Debugging](../production/debugging.md) cover the rest.

## Mercure hub configuration reload

Use `caddy reload --config /etc/caddy/Caddyfile` to apply configuration changes. Active subscriptions may drain and reconnect; see [Rolling updates](../production/rolling-updates.md#graceful-mercure-hub-configuration-reloads).

On Unix, you can also reload the configuration file with `SIGUSR1`. Set `MERCURE_PID` to the hub process ID:

```console
kill -USR1 "$MERCURE_PID"
```

This works when the hub was started with `run` and a configuration file, without `--resume`. Switching to API-based configuration can disable signal reloads; see [Caddy's signal rules](https://caddyserver.com/docs/command-line#signals).

## Mercure hub runtime introspection

The Caddy admin API also exposes:

- `/config/`: the current effective config (JSON).
- `/metrics`: Prometheus metrics (when `metrics` is in `GLOBAL_OPTIONS`). The bundled Caddyfiles also serve it on a plain-HTTP `:9091` listener; see [Health monitoring](../production/health-monitoring.md#prometheus-metrics).
- `/debug/pprof/`: Go profiler endpoints on the admin API. See [Debugging](../production/debugging.md).
