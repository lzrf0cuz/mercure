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
| `rs256.jwks.json` | JWKS | Key set for `publisher_jwks_url` / `subscriber_jwks_url` |

kid: `d7538996-f6d0-4ba4-831d-be962ea9a7e5`

## Tokens

All tokens expire in 2125 (`exp: 4906803676`).

Each token has a `.ndjson` (decoded header + payload) and `.jwt` (signed token string).

| Token | Algorithm | Publish | Subscribe |
|-------|-----------|---------|-----------|
| `demo_admin_rs256` | RS256 | `*` | `*` |
| `demo_rs256` | RS256 | `*` | limited topics |
| `demo_hs256` | HS256 | `*` | limited topics |

## Hosted Mocks

Gist: <https://gist.github.com/lzrf0cuz/e3002e3a7b561f546b5c331e52f729c0>
