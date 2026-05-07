# Demo fixtures — DEVELOPMENT ONLY

The keys and tokens in this directory are **publicly distributed test
material** bundled with the hub binary for the embedded demo UI. They are
**not secrets** and **must not be used in production**.

This directory is served at `/.well-known/mercure/ui/fixtures/*` only when
the hub is started with `demo: true` (which also enables the UI). With
`ui: true` alone the fixtures path is blocked at the router and returns a
404 response. The Go `//go:embed public` directive in `demo.go` includes
the entire `public/` tree, so the files ship in the binary either way —
the gate is enforced in the HTTP router.

## Inventory

| File | Purpose |
|---|---|
| `jwks.json` | Public JWKs the demo UI advertises for RS256 token verification. |
| `public-jwk.json` | The same public key as `jwks.json` in single-key form. |
| `public-key.pem` | The public key in PEM form for `pem`-flow demos. |
| `private-jwk.json` | RSA private key. **Demo-only — well-known in this repo.** |
| `tokens.json` | Pre-minted demo JWTs (HS256 + RS256). |

All RS256 entries share `kid: d7538996-f6d0-4ba4-831d-be962ea9a7e5`. The
HS256 secret is `!ChangeThisMercureHubJWTSecretKey!` (the upstream demo
default).

## See also: `fixtures/jwt/`

The root-level [`fixtures/jwt/`](../../fixtures/jwt/) directory is the
canonical source of the demo keypair — same `kid`, same modulus. It
holds the PEM keypair (`RS256.key` private, `RS256.key.pub` public),
pre-minted demo JWTs (`*.jwt`, `*.ndjson`), the canonical JWKS
(`rs256.jwks.json`), and is the regeneration target if the demo key is
ever rotated. This `public/fixtures/` directory is the subset that
ships embedded in the binary for the UI; `fixtures/jwt/` has the full
demo set (PEM + JWKS + pre-minted tokens) committed as published demo
material — these are not secrets.

## Usage

Subscribers / publishers verifying tokens issued by these fixtures can
point their JWKS URL at either location:

```text
# Live, served by a hub running with demo: true
https://<hub>/.well-known/mercure/ui/fixtures/jwks.json

# Or use the hosted gist mock (see fixtures/jwt/README.md)
https://gist.githubusercontent.com/lzrf0cuz/e3002e3a7b561f546b5c331e52f729c0/raw/mercure-mock-jwks.json
```

Pre-minted demo JWTs (with payload + signed string) live in
`fixtures/jwt/demo_*.{ndjson,jwt}` — copy `.jwt` content into an
`Authorization: Bearer …` header, or paste the `.ndjson` payload into
[jwt.io](https://jwt.io/) to inspect.

## Rotation

If the demo keypair ever needs rotation, update **all** of:

1. `fixtures/jwt/RS256.key{,.pub}` (the PEM private + public)
2. `fixtures/jwt/rs256_{public,private}.jwk.json` + `rs256.jwks.json`
3. `public/fixtures/{jwks,public-jwk,private-jwk}.json` + `public-key.pem`
4. `fixtures/jwt/demo_*.{jwt,ndjson}` — re-mint with the new private key
5. `public/fixtures/tokens.json` — re-mint the embedded UI's RS256 + HS256
   demo tokens with the new key/secret (the UI fetches this file directly
   at `/.well-known/mercure/ui/fixtures/tokens.json`)
6. The hosted gist mock (see `fixtures/jwt/README.md` Hosted Mocks)
7. `kid` should be a fresh UUID; update every reference above

If the HS256 demo secret is also being rotated (separate from the RS256
keypair), update:

- `MERCURE_PUBLISHER_JWT_KEY` / `MERCURE_SUBSCRIBER_JWT_KEY` in `.env.dev`
- The `UI_HS256_SECRET` constant in `public/app.js` (the embedded UI uses
  this to mint and validate HS256 demo flows client-side)
- The HS256 token in `public/fixtures/tokens.json` and
  `fixtures/jwt/demo_hs256.{jwt,ndjson}`

There is no Taskfile target for this yet — it's manual work because
demo-key rotation should never be casual. If a regen task is ever
warranted, wire it through `task fixtures:jwt:regen` and update both
READMEs.

## Production posture

- Fixtures only ship over HTTP when `demo: true` is set. With `ui: true`
  alone the UI loads but the `/fixtures/*` path returns 404, so anyone
  poking at the hub cannot fetch the demo private JWK or HS256 secret.
- Set `ui: false` (or do not configure the `ui` Caddyfile directive) on
  production hubs to also remove the embedded debug UI itself.
- The HS256 secret hard-coded in the UI (`!ChangeThisMercureHubJWTSecretKey!`)
  matches the upstream demo default. Production hubs must use a different
  publisher/subscriber JWT secret.

If you are about to put `demo: true` on a hub that is internet-reachable,
disable the demo and stand up a separate, authenticated
admin/observability path instead.

## Notes for security review

- **`private-jwk.json` is committed deliberately** as published demo
  material — the paired public key (`public-jwk.json`, `jwks.json`,
  `public-key.pem`) advertises the same modulus/exponent. Treating this
  file as a leak is a false positive.
- **`tokens.json` JWTs use a ~100-year `exp`** so the embedded UI
  doesn't have to mint new tokens on every demo session. Intentional
  for demo convenience; not a recommendation for production tokens.
- **`public-key.pem` is the only `.pem` allowlisted** in `.gitignore`
  (the global rule is `*.pem`); other PEMs under `public/fixtures/`
  remain ignored.
