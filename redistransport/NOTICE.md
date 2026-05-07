# NOTICE

This directory contains a Redis/Valkey transport plugin for the Mercure hub
(<https://github.com/dunglas/mercure>). It implements the Transport interface defined by
that project.

## License

This module is licensed under the GNU Affero General Public License, version 3
(AGPL-3.0-or-later). See [LICENSE](./LICENSE) for the full text. Contributions are
accepted under the same license via Developer Certificate of Origin sign-off
(see [CONTRIBUTING.md](./CONTRIBUTING.md)).

## Attribution

The upstream Mercure project and its authors retain copyright over the Transport
interface and related upstream code. Contributions to this module are © their respective
authors and licensed under AGPL-3.0-or-later.

## Third-party dependencies

This module's direct dependencies appear below; their licenses are
preserved with each project. Transitive dependencies are tracked in
`go.sum` and inherit their upstream notices.

| Package | License | Purpose |
|---|---|---|
| [github.com/dunglas/mercure](https://github.com/dunglas/mercure) | AGPL-3.0 | Upstream hub — Transport interface, codec types |
| [github.com/redis/go-redis/v9](https://github.com/redis/go-redis) | BSD-2-Clause | Redis/Valkey client (Cluster, Sentinel, single-node) |
| [github.com/gofrs/uuid/v5](https://github.com/gofrs/uuid) | MIT | UUIDv7 generation for event IDs |
| [github.com/cespare/xxhash/v2](https://github.com/cespare/xxhash) | MIT | Fast non-cryptographic hash for shard assignment |
| [github.com/prometheus/client_golang](https://github.com/prometheus/client_golang) | Apache-2.0 | Prometheus metrics collectors |
| [github.com/prometheus/client_model](https://github.com/prometheus/client_model) | Apache-2.0 | Prometheus metrics protobuf types (paired with client_golang) |
| [golang.org/x/time](https://pkg.go.dev/golang.org/x/time) | BSD-3-Clause | Rate-limiting (token bucket) for publishers and subscribers |
| [github.com/alicebob/miniredis/v2](https://github.com/alicebob/miniredis) | MIT | In-process Redis fake (test-only) |
| [go.uber.org/goleak](https://github.com/uber-go/goleak) | MIT | Goroutine-leak detection (test-only) |
| [github.com/stretchr/testify](https://github.com/stretchr/testify) | MIT | Test assertions (test-only) |

The `caddy/` submodule additionally links
[caddyserver/caddy/v2](https://github.com/caddyserver/caddy) (Apache-2.0)
for the Caddyfile parser + module-host integration.

## Security

See [SECURITY.md](./SECURITY.md) for the vulnerability disclosure process.
