# Contributing

## License and Copyright Attribution

When you open a Pull Request to the project, you agree to license your code under the [GNU AFFERO GENERAL PUBLIC LICENSE](LICENSE)
and to transfer the copyright on the submitted code to [Kévin Dunglas](https://dunglas.fr).

Be sure to have the right to do that (if you are a professional, ask your company)!

If you include code from another project, please mention it in the Pull Request description and credit the original author.

## Commit Messages

The commit message must follow the [Conventional Commits specification](https://www.conventionalcommits.org/).
The following types are allowed:

- `fix`: bugfix
- `feat`: new feature
- `docs`: change in the documentation
- `spec`: spec change
- `test`: test-related change
- `perf`: performance optimization
- `ci`: CI-related change

Examples:

    fix: Fix something

    feat: Introduce X

    feat!: Introduce Y, BC break

    docs: Add docs for X

    spec: Z disambiguation

## Setup

Prerequisites:

- Go, at the version named in `go.mod` (1.27 at the time of writing).
- Docker, for the real-server tests, the image build and the Compose-based dev loop. `task setup` does not install it.

On macOS with Homebrew, run `task setup`. It installs the tools listed in the `Brewfile`, then `dashboard-linter` (pinned release, no Homebrew formula) and the `ui/` npm dependencies. It also creates the local dev env files (`.env.dev`, `.env.dev.redis`) from the committed `.env.dev*.example` files, without overwriting existing ones. For production, copy `.env.prod.example` to `.env.prod` and fill it in by hand. Real secrets go in `.env.local`, which is not tracked. It is safe to run again.

On other platforms, install the tools `task ci` uses with your package manager: Go, [Task](https://taskfile.dev/installation), golangci-lint, govulncheck, rumdl, shellcheck, shfmt, Node.js with npm, and Biome. `task setup` prints the same list with install hints when Homebrew is absent, then installs `dashboard-linter` and the npm dependencies and creates the env files.

Run `task doctor` to check the toolchain, Docker and the local files, and `task ci` to run the checks a pull request must pass.

## Hub

Clone the project and make your changes:

    git clone https://github.com/dunglas/mercure
    cd mercure

To run the test suite:

    go test -v -timeout 30s github.com/dunglas/mercure

To test the Caddy module with HS256 (shared secret):

    cd caddy/mercure
    MERCURE_EXTRA_DIRECTIVES='playground' \
    MERCURE_PUBLISHER_JWT_KEY='!ChangeThisMercureHubJWTSecretKey!' \
    MERCURE_SUBSCRIBER_JWT_KEY='!ChangeThisMercureHubJWTSecretKey!' \
    go run -tags nobadger,nomysql,nopgx main.go run --config ../../local.Caddyfile

Or with RS256 (asymmetric RSA keys from `fixtures/jwt/`):

    cd caddy/mercure
    MERCURE_EXTRA_DIRECTIVES='debugger' \
    MERCURE_PUBLISHER_JWT_KEY="$(cat ../../fixtures/jwt/RS256.key.pub)" \
    MERCURE_PUBLISHER_JWT_ALG=RS256 \
    MERCURE_SUBSCRIBER_JWT_KEY="$(cat ../../fixtures/jwt/RS256.key.pub)" \
    MERCURE_SUBSCRIBER_JWT_ALG=RS256 \
    go run -tags nobadger,nomysql,nopgx main.go run --config ../../local.Caddyfile

Both commands run `local.Caddyfile`, which trusts the issuer `https://localhost`
(override with `MERCURE_TRUSTED_ISSUERS`) and verifies access tokens with the
keys above. The tokens in `fixtures/jwt/` (`demo_hs256.jwt`, `demo_rs256.jwt`,
`demo_admin_rs256.jwt`) are minted for it: they carry `iss: https://localhost`
and `aud: https://localhost/.well-known/mercure`, the audience the hub derives
from the URL the client used. A hub reached on another host or port derives a
different audience and rejects them.

Go to `https://localhost`. The HS256 variant (`playground`) prefills a token. The RS256 variant (`debugger`) does not: paste `fixtures/jwt/demo_rs256.jwt` into the JWT field. Its subscribe grant does not cover the default example topic (`https://localhost/books/1`), so for private updates on it use `fixtures/jwt/demo_admin_rs256.jwt`, which may subscribe to every topic.

When you send a PR, make sure that:

- You add valid test cases.
- Tests are green.
- You make a PR on the related documentation.
- You make the PR on the same branch you based your changes on. If you see commits
  that you did not make in your PR, you're doing it wrong.

### Debugger UI Dependencies

The debugger UI loads nothing from third-party origins. Its npm dependencies are declared in `ui/package.json` and bundled into `public/vendor/`, which is committed because the hub embeds it. Run the scripts from `ui/`:

- `npm ci && npm run vendor` regenerates `public/vendor/` from `package-lock.json`.
- `npm run upgrade` bumps every dependency to its latest version published at least a week ago, then regenerates `public/vendor/`. Nothing runs it automatically: run it yourself and commit the result.
- `npm run check` fails when `public/vendor/` doesn't match the lockfile or a dependency is outdated. The dependency check needs network access. `release.sh` runs it.

`task ui:check` fails when `public/vendor/` doesn't match the lockfile, and `task ci` runs it. `task ui:outdated` runs the dependency check alone.

### Configuring Visual Studio Code

A configuration for Visual Studio Code is provided in the `.vscode/` directory of the repository.
It is automatically loaded by Visual Studio Code.

### Finding Deadlocks

To debug potential deadlocks:

1. Install `go-deadlock`: `./tests/use-go-deadlock.sh`
2. Run the tests in race mode: `go test -race ./... -v`, with the `deprecated_transport` build tag added to the repository's build tags (for example through `GOFLAGS`). The script's timer-pool patch is in `transport_deprecated.go`, which only builds with that tag, and without the patch go-deadlock crashes the tests under `testing/synctest`.
3. To stress-test the app, run the load test (see `docs/production/load-testing.md`)
4. Be sure to remove `go-deadlock` before committing

### Avoiding silent failures

Do not fold an `err != nil` check into the same boolean expression as a
log-level gate. The compound short-circuits when the level is disabled,
skipping any control flow inside the `if` body, including `return`.

    // Wrong — at the default INFO log level the log-level gate (Debug) is
    // false, the body is skipped, and the caller gets a misleading
    // "success" return:
    if _, err := w.Write(b); err != nil && h.logger.Enabled(ctx, slog.LevelDebug) {
        h.logger.LogAttrs(ctx, slog.LevelDebug, "Write failed", slog.Any("error", err))
        return false
    }

    // Right — control flow always fires on error; only the log line is gated:
    if _, err := w.Write(b); err != nil {
        if h.logger.Enabled(ctx, slog.LevelDebug) {
            h.logger.LogAttrs(ctx, slog.LevelDebug, "Write failed", slog.Any("error", err))
        }
        return false
    }

## Spec

The spec is written in Markdown, compatible with [Mmark](https://mmark.miek.nl/).
It is then converted in [the "xml2rfc" Version 3 Vocabulary](https://tools.ietf.org/html/rfc7991).

To contribute to the protocol itself:

- Make your changes
- [Download Mmark](https://github.com/mmarkdown/mmark/releases)
- [Download `xml2rfc` using pip](https://pypi.org/project/xml2rfc/): `pip install xml2rfc`
- Generate the XML file: `mmark spec/mercure.md > spec/mercure.xml`
- Validate the generated XML file and generate the text file: `xml2rfc --text --v3 spec/mercure.xml`
- Remove non-ASCII characters from the generated `mercure.txt` file (example: K**é**vin)
- If appropriate, be sure to update the reference implementation accordingly
