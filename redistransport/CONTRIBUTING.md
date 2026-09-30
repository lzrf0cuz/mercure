# Contributing to the Redis Transport

## License

This transport is licensed under the [GNU Affero General Public License v3.0](LICENSE).
By contributing, you agree to license your code under the same terms.

## DCO sign-off

Every commit must carry a `Signed-off-by:` trailer affirming the
[Developer Certificate of Origin v1.1](https://developercertificate.org/).
Use `git commit -s` to add the trailer automatically:

```text
Signed-off-by: Your Name <your.email@example.com>
```

The name + email must match the commit author. Anonymous and pseudonymous
sign-offs are not accepted. PRs whose commits lack the trailer cannot
be merged.

## Prerequisites

- **Go 1.27+**
- **Docker** with Compose v2
- **[Task](https://taskfile.dev)** (go-task, not Taskwarrior; both install a
  `task` binary)
- **[golangci-lint](https://golangci-lint.run)**
- **[govulncheck](https://pkg.go.dev/golang.org/x/vuln/cmd/govulncheck)** (used by `task rt:vuln:all`)
- **[rumdl](https://github.com/rvben/rumdl)** (Markdown linter)

All commands below are invoked from the **repo root** (the top-level `mercure/`
directory, not `redistransport/`).
The transport's tasks are namespaced `rt:*` via the root `Taskfile.yml`
(e.g. `task rt:test`, `task rt:redis:up`). Run them from the repo root:
invoked from `redistransport/`, they miss the `MERCURE_*`, `REDIS_*` and
`SERVER_NAME` variables.

`task doctor` (from the repo root) probes Docker + Go + env files +
go.sum + TLS cert before you start. `task setup` creates the local env files
(`.env.dev`, `.env.dev.redis`) from the committed `.env*.example` files.

## Commit messages

[Conventional Commits](https://www.conventionalcommits.org/): `fix:`,
`feat:`, `docs:`, `test:`, `perf:`, `ci:`, `refactor:`.

```text
fix: handle NOGROUP error during PEL drain
feat: add dispatch sharding for high fan-out
docs: add Valkey setup instructions to README
```

## Architecture

| File | Responsibility |
|---|---|
| `redis.go` | Core transport — startup, Dispatch, AddSubscriber, Close, XREADGROUP listener, history replay, health check, TTL cleanup |
| `shard.go` | Sharded dispatch — per-shard SubscriberLists, shard workers, fan-out, sync.Pool |
| `presence.go` | Presence heartbeat, MGET-batched read, summary mode, zombie GC, `GetSubscribers` |
| `lua.go` | Lua scripts, stream-ID helpers, UUIDv7 → stream-ID conversion |
| `topicindex.go` | Per-shard exact-topic index that matches updates to subscribers |
| `tracer.go` | OpenTelemetry history-replay span and parent-span tagging |
| `codec.go` | Codec aliases of mercure core |
| `options.go` | `WithX` option functions + `validateOptions` / `warnSuspiciousOptions` |
| `metrics.go` | Prometheus metric definitions and `record*` helpers |
| `poolmetrics.go` | Read-through Prometheus collector for go-redis pool stats |
| `version.go` | Build provenance — `Version()`, `LookupBuildInfo()` |
| `caddy/redis.go` | Caddy module wrapper and Caddyfile parser |

## Testing

- Unit tests use [miniredis](https://github.com/alicebob/miniredis) — no
  real server required. Run with `task rt:test`.
- `real_redis`-tagged tests cover behaviour miniredis cannot emulate
  (INFO server version check, real `XTRIM MINID` semantics, zombie GC,
  concurrent-churn stress, multi-hub invariants). Run with
  `task rt:test:real:redis` / `task rt:test:real:multihub:redis` (Valkey
  variants symmetric).
- Override the server address via `REDIS_ADDR` (default `localhost:6379`).
- Always use `t.Parallel()` where safe; `t.Helper()` in test helpers;
  `t.Cleanup()` for resource teardown.

## Grafana dashboard JSON edits

The bundled dashboard is the source of truth (`editable: false` —
operators cannot save edits back from the Grafana UI; fork via "Save
As" if you want to experiment locally). Two lint gates guard it:

1. **`task rt:lint:dashboard`** — Grafana Labs'
   [`dashboard-linter`](https://github.com/grafana/dashboard-linter)
   (run with `--strict`) enforces the standard best-practice surface:
   templated datasource, templated `job` and `instance` variables,
   `$__rate_interval` on counter rates, panel title+description, etc.
   Per-panel exclusions for the `redis_exporter` panels live in
   `grafana/dashboards/.lint` with inline rationale (those panels
   legitimately scope to a separate scrape job).

   Run `task setup` from the repository root to install it.

2. **`task rt:test`**: `dashboard_lint_test.go` checks what `dashboard-linter`
   does not: every aggregator keeps the `backend_type` and `instance` labels
   its legend uses; the `job`, `instance` and `backend_type` variables exist
   and default to `All`; every `mercure_redis_*` query filters on
   `backend_type=~"$backend_type"`; every panel named in `.lint` exists; and
   every legend containing `{{instance}}` starts with it, followed by end of
   line, a space or `|`.

The tests also enforce minimum legend counts, so removing legends cannot make
them pass vacuously. When a panel change moves the `{{instance}}` legend
count, update the exact counts in `TestInstanceLegendCountSentinel` and the
`legendFloor` values in `instanceDashboards()` (both in
`dashboard_lint_test.go`).

### Dev tracing workflow

Traces (OTel via Jaeger) are **opt-in**, not part of the default dev loop:

```sh
task dev:full:tracing       # rt:redis:up + rt:metrics:up:redis + rt:tracing:up:redis + dev:redis TRACING=true
```

`dev:full:tracing` brings up the Jaeger sidecar under the `mercure-redis`
compose project (alongside Prometheus + Grafana) so the Grafana trace
dashboard's Jaeger datasource resolves `http://jaeger:16686` over the
shared project network. View traces either at the **Jaeger UI**
(`http://localhost:16686`) or the **Grafana "Mercure Redis Transport —
Tracing" dashboard** (separate from the always-on metrics dashboard, so
the latter never shows dead trace panels when tracing is off).

**Teardown is `task rt:tracing:down:redis`**, not `task tracing:down` —
the latter is the root-stack pair for the bolt/standalone hub's Jaeger.
The two stacks share `container_name: jaeger` and host ports
`16686/4317/4318` (daemon-global), so they're **mutually exclusive at
runtime**; running both fails with a name/port collision. Use the
matching teardown for whichever stack you brought up:

```sh
task rt:tracing:down:redis  # if you ran dev:full:tracing
task rt:tracing:down:valkey # if you ran rt:tracing:up:valkey
task tracing:down           # if you ran the root tracing:up
```

To capture the hub's stdout+stderr alongside the terminal stream during
a tracing run (handy when investigating an issue across both the
Jaeger traces and the hub's own logs), pass `LOG_FILE=path` as a
go-task variable:

```sh
task dev:full:tracing LOG_FILE=/tmp/mercure.log
```

The native `wgo`-supervised hub isn't a Docker container, so its
stdout doesn't survive in `docker logs` — `LOG_FILE` is the Taskfile-
provided way to read its output after the terminal closes. Off by
default.

`rt:lint:dashboard` skips the trace dashboard; `TestTracingDashboardStructure`
in `dashboard_lint_test.go` checks that every panel and target that runs a
query uses the provisioned Jaeger datasource.

### Per-instance vs fleet-aggregate panel design

Pick treatment by panel intent:

| Intent | Aggregator | Legend starts with `{{instance}}` |
|---|---|---|
| Timeseries (per-pod over time) | `sum by (instance, …)` | yes (prefix `{{instance}} \| `) |
| Stat alarm / worst-case / binary | `min(…)` / `max(…)` / `sum(…)` | no |
| Multi-row stat (one row per pod) | `sum by (instance, …)` + `values: true` | yes (prefix `{{instance}} \| `) |
| Histogram percentile | `sum by (backend_type, le)` / `sum by (le)` | no |
| Piechart / range-share / correlation | fleet aggregate | no |
| `redis_exporter` row (`{job="redis"}`) | unchanged | no (the `instance` label here is the Redis target, not the hub) |

`TestDashboardInstanceLegendPrefixShape` enforces only `HasPrefix("{{instance}}")`
— the `{{instance}} <token>` shape (e.g. `{{instance}} alloc` on Go-runtime
panels) and the `{{instance}} | <token>` shape (e.g. `{{instance}} | dispatches/sec`
on transport panels) both pass. Prefer ` | ` for new panels with structured
suffixes; the bare-space form survives for Go-runtime panels where the
suffix is a single label.

Histograms stay fleet-aggregate because per-instance `histogram_quantile`
returns NaN on low-traffic replicas — the series silently disappears.
Operators get per-replica percentile views by filtering `$instance` to
one replica.

If you edit the JSON directly in a fork of the dashboard and re-export
into `transport.json`, **inspect the diff before committing**. Both
lint gates will catch common drift, but a clean lint isn't the same as
a clean semantic review — watch specifically for:

- accidental panel `id` renumber (breaks operator bookmarks linking to
  specific panels)
- hardcoded datasource UID instead of `${datasource}` template
- hardcoded `$job` value (e.g. `job="redis"` instead of `job=~"$job"`)
  on a hub-side panel — the `.lint` exemption is only for the
  `redis_exporter` panels enumerated in that file
- by-clause that drops `backend_type` even though the legend still
  references `{{backend_type}}`

## Pull-request checklist

- [ ] Every commit carries `Signed-off-by:` (DCO; use `git commit -s`)
- [ ] `task rt:ci` passes (fmt:check + lint:all + vet:all + test:all + build:all)
- [ ] `task rt:vuln:all` reports zero findings
- [ ] `task rt:test:real:redis` passes if the change touches real-server
      behaviour (INFO parse, XTRIM, presence MGET, zombie GC, etc.)
- [ ] New functionality has tests (miniredis preferred; `real_redis`-tagged
      only for what miniredis can't emulate)
- [ ] `CHANGELOG.md` updated (entries go into the section of the next
      release)
- [ ] `task md:lint` clean (Markdown changes follow `.rumdl.toml`)
