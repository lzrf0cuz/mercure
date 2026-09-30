# Mercure

Mercure hub (Go, Caddy module) based on [dunglas/mercure](https://github.com/dunglas/mercure),
with an optional Redis/Valkey Streams transport in `redistransport/`.

See [CONTRIBUTING.md](CONTRIBUTING.md) for the contribution rules and the
documents under `docs/` for configuration and operation.

## Modules

The repository holds four Go modules, each with its own `go.mod`.

| Path | Module | Purpose |
|---|---|---|
| `/` | `github.com/dunglas/mercure` | The hub library |
| `/caddy` | `github.com/dunglas/mercure/caddy` | The Caddy module and the `mercure` binary (`caddy/mercure/`) |
| `/redistransport` | `github.com/lzrf0cuz/mercure/redistransport` | Redis/Valkey Streams transport (AGPL-3.0) |
| `/redistransport/caddy` | `github.com/lzrf0cuz/mercure/redistransport/caddy` | Caddy directives for the Redis transport |

`caddy/go.mod` replaces the other three modules with local paths. When
dependencies change, run `go mod tidy` in all four modules.

The repository root is a library package. There is no `cmd/` directory: the
`main` package is `caddy/mercure/main.go`.

## Build tags

Build and test the hub root and `caddy/` with these tags:

```text
nobadger,nomysql,nopgx
```

`GO_BUILD_TAGS` in `Taskfile.yml` holds the same list. The `redistransport/`
modules do not need them. Prefer the `task` targets below, which pass the tags
for you. With plain `go`:

```console
go test -tags "nobadger,nomysql,nopgx" ./...
```

## Build, test and lint

Run `task --list-all` for every target. Targets prefixed `rt:` run in
`redistransport/`.

| Goal | Command |
|---|---|
| Build the binary | `task go:build` |
| Hub and Caddy module tests (race, coverage) | `task go:test` |
| Redis transport tests (miniredis) | `task rt:test`, or `task rt:test:all` with the Caddy module |
| Redis, Valkey and Cluster tests (needs Docker) | `task rt:test:real:matrix` |
| Format Go code | `task go:fmt`, `task rt:fmt` |
| Lint Go code | `task go:lint`, `task rt:lint:all` |
| Lint Markdown, shell scripts, dashboards | `task md:lint`, `task sh:lint`, `task dashboard:lint` |
| Check the debug UI sources | `task ui:check` |
| Validate a Caddyfile | `task caddy:validate CADDYFILE=path/to/Caddyfile` |

The debug UI in `public/` is embedded in the binary. The `dev_ui` build tag
serves it from disk instead. `task go:test:dev` and `task go:lint:dev` cover
that variant.

## Checks before sending a change

Three gates build on each other.

- `task ci`: formatting (Go and Caddyfile), vet, lint, tests (race), build
  check, build-tag consistency, UI, Markdown, shell and dashboard checks across
  all four modules. It is deterministic. It does not look up new package
  versions or advisories.
- `task verify`: `task ci`, plus `govulncheck`, the npm freshness check
  (`task ui:outdated`) and race stress (`-race -count=5`). The vulnerability
  and freshness checks need network access, so their result changes over time.
- `task verify:full`: `task verify`, plus the real-server test matrix, the
  Docker image build and the Playwright conformance tests.

Run `task ci` before opening a pull request.

## Development loop

| Goal | Command |
|---|---|
| Hub and debug UI in Docker (Bolt transport) | `task dev` |
| Native hub with Redis, rebuilt on save | `task dev:redis` |
| Same, from an empty database and Redis volume | `task dev:redis:fresh` |
| Same, plus Prometheus and Grafana | `task dev:full` |
| Run the hub natively without Redis | `task go:run` |
| Install the toolchain (macOS: Homebrew; prints the list elsewhere) and create the local env files | `task setup` |
| Create only the local env files | `task env:init` |
| Check the local toolchain | `task doctor` |

`task setup` copies the committed `.env.dev.example` and
`.env.dev.redis.example` to `.env.dev` and `.env.dev.redis`. It never
overwrites an existing file. The copies are not tracked. For production, copy
`.env.prod.example` to `.env.prod` yourself and fill it in.

Put real secrets in `.env.local`, which is not tracked. `task dev:redis` and
the tasks that call it load `.env.local` ahead of `.env.dev`. `task dev` runs
the hub in Docker and reads only the compose env file (`.env.dev`, or the file
named by `ENV_FILE`), so `.env.local` does not reach it.

## Code conventions

- The Go version is the one in `go.mod`. Use `range` over integers, `any`,
  `log/slog` for logging and `errors.Is` / `errors.As` for error checks.
- Code is formatted with `golangci-lint fmt` and must pass `golangci-lint`
  with no reported issues.
- Never fold an `err != nil` check into the same condition as a log-level
  check. See "Avoiding silent failures" in CONTRIBUTING.md.
- Add tests with every behaviour change. Update the matching page under `docs/`.
- Commit messages follow Conventional Commits.
- Markdown follows the rules in `.rumdl.toml`.
