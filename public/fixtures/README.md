# Playground fixtures

Published test keys for the debugger UI. They are served at
`/.well-known/mercure/debug/fixtures/` only when the hub's `playground`
setting is enabled. With `debugger` alone the router returns 404 for this
subtree, including encoded paths, traversal and ASCII case variants. Under a
`dev_ui` build on a case-insensitive filesystem, Unicode-folded spellings may
still resolve; the keys are published anyway. Production builds embed the
files through `public_fs_embed.go`; `dev_ui` builds read `public/` from disk.

## Inventory

| File | Purpose |
|---|---|
| `jwks.json` | Published RSA verification key in JWKS form. |
| `public-jwk.json` | The same verification key as a single JWK. |
| `public-key.pem` | The same public key in PEM form. |
| `private-jwk.json` | Published test signing key for the UI's copy tools. |

The UI gets its access token from the `playground-token` endpoint, which
signs for the hub's current resource identifier. The hub serves it only when
it can mint: the Caddy module does so with a static HMAC key, and an app
embedding the hub library must also pass `WithPlaygroundTokenFunc`. When the
endpoint answers 404 the UI says the hub provided no token and asks for a
pasted one; the 404 does not say why.

The fixture keys are for manual test-token examples. They do not affect
`playground-token`, which signs with the hub's own key. Outside playground
mode the UI disables the copy tools and replaces their steps with a note. The
HS256 example secret in `public/app.js` is also published test material.

## Rotation

The source keys live under `fixtures/jwt/`. When changing them, update
`public/fixtures/{jwks,public-jwk,private-jwk}.json` and
`public/fixtures/public-key.pem` together, and any inline PEM example in
`compose.yaml`.

## Use

The copy tools, available in playground mode, inspect and create test
tokens. A Mercure 1.0 token needs the hub's resource identifier as its
audience and the current authorization claims. The Hub Key toggle in
Settings selects key and configuration help only; it does not turn an old
0.x token into a 1.0 token.
