# JWT Fixtures

> **WARNING: DEVELOPMENT ONLY** — All keys and tokens in this directory are
> mock credentials for local development. They are intentionally public.
> **NEVER** use these keys in production environments.

## Keys

### HS256

Secret: `!ChangeThisMercureHubJWTSecretKey!`

### RS256

| File | Format | Usage |
|------|--------|-------|
| `RS256.key` | PKCS#1 PEM | Private key (signing) |
| `RS256.key.pub` | SPKI PEM | Public key (verification) |
| `rs256_private.jwk.json` | JWK | Private key |
| `rs256_public.jwk.json` | JWK | Public key |
| `rs256.jwks.json` | JWKS | Key set for an `issuer` block's `jwks_uri` (`publisher` / `subscriber`) |

kid: `d7538996-f6d0-4ba4-831d-be962ea9a7e5`

## Tokens

All tokens are Mercure 1.0 access tokens (RFC 9068 JWTs with an RFC 9396
`authorization_details` claim; see `docs/concepts/authorization.md`) and expire
in 2125 (`exp: 4906803676`).

Each token has a `.ndjson` (decoded header + payload) and `.jwt` (signed token string).

| Token | Algorithm | Publish | Subscribe |
|-------|-----------|---------|-----------|
| `demo_admin_rs256` | RS256 | `*` | `*` |
| `demo_rs256` | RS256 | `*` | limited topics |
| `demo_hs256` | HS256 | `*` | limited topics |

The limited tokens can subscribe to `https://example.com/my-private-topic`, to
book topics matching the URL Pattern `*://*:*/demo/books/:id.jsonld` (any
scheme, host and port), and to the
subscriptions API and subscription events under
`/.well-known/mercure/subscriptions`.

Every token carries `iss: https://localhost` and
`aud: https://localhost/.well-known/mercure`. They are accepted by a hub
running `local.Caddyfile` (trusted issuer `https://localhost`) that the client
reaches at `https://localhost`: the hub derives the expected audience from the
URL of each request, so another host or port needs tokens with a matching
`aud`, or a hub with `resource_identifier` pinned to the value above.

## Hosted Mocks

Gist: <https://gist.github.com/lzrf0cuz/e3002e3a7b561f546b5c331e52f729c0>
