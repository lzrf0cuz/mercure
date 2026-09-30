# Security policy

## Reporting a vulnerability

**Do not open a public GitHub issue or pull request for security
issues.** Public disclosure before a fix is available puts users at risk.

Use **GitHub's private vulnerability reporting** instead:

1. Go to <https://github.com/lzrf0cuz/mercure/security/advisories/new>
   (or click the **Report a vulnerability** button under the **Security**
   tab on the repository).
2. Describe the issue: affected versions, reproduction steps, and the
   impact you've observed or expect.
3. Submit. Only repository maintainers will see the report; GitHub keeps
   the discussion in a private channel until an advisory is published.

**Response targets** (non-contractual; volunteer-maintained fork):

- **Acknowledge receipt**: within **3 business days**.
- **Initial assessment** (severity, scope, affected versions): within
  **7 business days**.
- **Coordinated-disclosure window**: by default 90 days from initial
  assessment, shortened for in-the-wild exploitation and extended on
  request for complex remediation. We'll discuss the window in the
  private advisory thread.

If you've never used GitHub Security Advisories, the GitHub docs walk
through the reporter side here:
<https://docs.github.com/en/code-security/security-advisories/guidance-on-reporting-and-writing-information-about-vulnerabilities/privately-reporting-a-security-vulnerability>.

## Scope

This policy covers the `redistransport` Go module and its `caddy/`
submodule in this repository. Issues in upstream dependencies should go
to their respective maintainers:

| Component | Report to |
|-----------|-----------|
| Mercure hub (`github.com/dunglas/mercure`) | <https://github.com/dunglas/mercure/security> |
| Caddy server | <https://github.com/caddyserver/caddy/security> |
| go-redis (`github.com/redis/go-redis`) | <https://github.com/redis/go-redis/security> |
| Redis server | <https://redis.io/docs/latest/operate/oss_and_stack/management/security/> |
| Valkey server | <https://github.com/valkey-io/valkey/security> |

If you're uncertain which project owns the issue, file with us anyway
and we'll forward it.

## Supported versions

This module is pre-1.0, with a single supported line. Security
fixes target the latest tagged release on the `fork/main` branch.
Backports to older tags are not provided. Operators are expected to
upgrade to the latest release to receive fixes.

## Disclosure

Once a fix is available, we publish a GitHub Security Advisory with a
CVE (where appropriate), credit reporters who wish to be named, and
release a patch version. The advisory describes the issue, affected
versions, and the upgrade path.

## Supply chain

The build injects the version at link time via `-ldflags -X` into
`redistransport.version`. The transport surfaces the version, the VCS
revision and the Go version via `LookupBuildInfo()` and the
`mercure_redis_build_info` Prometheus gauge (labels: `version`, `revision`,
`go_version`).

The `revision` label carries the first 7 characters of the `vcs.revision`
build setting. It is empty for some build modes, such as `go run` and
`-buildvcs=false` builds. Use it for **rollout identification** (which build
is running where), not for cryptographic verification.

Release tags (`redistransport/vX.Y.Z`) and their commits are SSH-signed.
Verify a tag with `git tag -v`. The project publishes no binaries, signed
archives or SBOM. Operators needing strong verification should build from a
tagged source checkout and pin by full commit SHA at the build host.

`task rt:vuln:all` runs `govulncheck` across this module and its caddy
submodule. Run it before a dependency bump; `task release:check` runs it.

### What to report as a supply-chain issue

The vulnerability-reporting channel above also accepts supply-chain
incidents that the standard `govulncheck` flow does not catch:

- **Compromised dependency** — a direct or transitive dependency with
  credible evidence of malicious code, even if no advisory is published yet.
- **Module substitution or typosquatting** — a fake module that imitates
  this module's name.
- **Poisoned release artifact** — a tagged release whose embedded
  `revision` does not match the corresponding tag commit, or whose
  binary contents diverge from a clean rebuild.
- **Leaked maintainer credentials** — release-signing keys, GitHub
  tokens, or registry credentials with publish access to this module.

If you're unsure whether an issue fits, file it anyway. We would rather
triage and forward than miss one.
