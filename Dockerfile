# syntax=docker/dockerfile:latest

# Define the build image.
ARG BUILD_IMAGE=golang:1.26.2-alpine

# Define the base image. Pinned to the same Caddy 2.x patch the caddy/
# submodule's go.mod targets (`github.com/caddyserver/caddy/v2 v2.11.4`)
# so the runtime base matches the linked Caddy version.
ARG BASE_IMAGE=caddy:2.11.4-alpine

# Define the version. SVC_VERSION has NO default: a silent fallback would let a
# bare build (or a compose build that interpolated an unset var to empty) stamp
# a mislabeled artifact version. The Taskfile always passes it; bare builds must
# too. UPSTREAM_VERSION/RT_VERSION keep a pinned default (build-target metadata,
# not the artifact's own identity).
ARG SVC_VERSION
ARG UPSTREAM_VERSION=v0.24.2
# In-tree redistransport module version. Bumped via `task version:bump:rt:*`
# (independent from SVC_VERSION because the transport is a separate Go
# module with its own release cadence).
ARG RT_VERSION=v0.0.1

# Set the build image.
FROM ${BUILD_IMAGE} AS build

# Re-declare global ARGs for use in this stage.
ARG SVC_VERSION
ARG UPSTREAM_VERSION
ARG RT_VERSION
ARG BASE_IMAGE

# Version guard: refuse to stamp empty version metadata (failure modes noted
# above). The Taskfile always exports these, so only a bare/empty-arg build trips it.
RUN { [ -n "$SVC_VERSION" ] && [ -n "$UPSTREAM_VERSION" ] && [ -n "$RT_VERSION" ]; } \
    || { echo "version guard: SVC_VERSION ('$SVC_VERSION'), UPSTREAM_VERSION ('$UPSTREAM_VERSION'), and RT_VERSION ('$RT_VERSION') must be non-empty build-args — all three are -X stamped into the binary, and an empty one would silently mislabel provenance (the Taskfile sets them)" >&2; exit 1; }

# Set the environment variables.
ENV SVC=mercure SVC_BASE_LOC=/go/src/mercure SVC_CADDY_LOC=caddy SVC_LOC=caddy/mercure SVC_BIN_LOC=/go/bin

# Set the working directory.
WORKDIR ${SVC_BASE_LOC}

# Copy go.mod and go.sum files first for better caching.
# caddy/go.mod has `replace` directives pointing to ../redistransport and
# ../redistransport/caddy — those modules' go.mod files must be present for
# `go mod download` to resolve the replace targets.
COPY go.mod go.sum ./
COPY ${SVC_CADDY_LOC}/go.mod ${SVC_CADDY_LOC}/go.sum ./${SVC_CADDY_LOC}/
COPY redistransport/go.mod redistransport/go.sum ./redistransport/
COPY redistransport/caddy/go.mod redistransport/caddy/go.sum ./redistransport/caddy/

# Pin guards: fail the build if the build/base images fall out of lock-step with
# the modules, so an image pin can't silently drift. Go: the build image's
# major.minor must equal go.mod's. Caddy: the base image's caddy tag must equal
# the caddy/v2 version caddy/go.mod links. Unparseable inputs (registry-prefixed
# base image, a reshaped require line) fail loud rather than pass silently.
RUN need="$(awk '$1=="go"{print $2; exit}' go.mod | cut -d. -f1,2)"; \
    have="$(go version | awk '{print $3}' | sed 's/^go//' | cut -d. -f1,2)"; \
    { [ -n "$need" ] && [ -n "$have" ]; } || { echo "Go pin guard: unparseable go.mod ('$need') or go version ('$have')" >&2; exit 1; }; \
    [ "$need" = "$have" ] || { echo "Go pin drift: go.mod is $need, BUILD_IMAGE has $have" >&2; exit 1; }; \
    want="$(awk '{for (i=1; i<NF; i++) if ($i=="github.com/caddyserver/caddy/v2" && $(i+1) ~ /^v[0-9]/) {print $(i+1); exit}}' ${SVC_CADDY_LOC}/go.mod | sed 's/^v//')"; \
    base="$(printf '%s\n' "${BASE_IMAGE}" | sed -n 's#.*caddy:v\{0,1\}\([0-9][0-9.]*\).*#\1#p')"; \
    { [ -n "$want" ] && [ -n "$base" ]; } || { echo "Caddy pin guard: unparseable caddy/go.mod ('$want') or BASE_IMAGE ('$base' from '${BASE_IMAGE}')" >&2; exit 1; }; \
    [ "$want" = "$base" ] || { echo "Caddy pin drift: caddy/go.mod is v$want, BASE_IMAGE is $base (${BASE_IMAGE})" >&2; exit 1; }

# Set the working directory.
WORKDIR ${SVC_CADDY_LOC}

# Download the Go dependencies.
RUN go mod download && go mod verify

# Go back to base directory and copy the rest of the source code.
WORKDIR ${SVC_BASE_LOC}
COPY ./ ./

# Set the working directory to the caddy binary entry point.
WORKDIR ${SVC_BASE_LOC}/${SVC_LOC}

# Build the binary. `-buildvcs=false` is explicit: the build image has no git, so
# the default `auto` already omits VCS stamping — this documents that `.git/`
# (excluded by `.dockerignore`) is genuinely unused, and stays deterministic.
RUN CGO_ENABLED=0 go build -buildvcs=false \
    -ldflags="\
      -X 'github.com/caddyserver/caddy/v2.CustomVersion=Mercure ${SVC_VERSION} (upstream ${UPSTREAM_VERSION}) Caddy' \
      -X 'github.com/caddyserver/caddy/v2/modules/caddyhttp.ServerHeader=Mercure ${SVC_VERSION} (upstream ${UPSTREAM_VERSION})' \
      -X 'github.com/dunglas/mercure/common.version=${SVC_VERSION}' \
      -X 'github.com/dunglas/mercure/common.upstreamVersion=${UPSTREAM_VERSION}' \
      -X 'github.com/lzrf0cuz/mercure/redistransport.version=${RT_VERSION}'" \
    -tags="deprecated_server,deprecated_transport,nobadger,nomysql,nopgx" -o ${SVC_BIN_LOC}/${SVC}

# Set the base image.
FROM ${BASE_IMAGE}

# Set the environment variables.
ENV SVC=mercure SVC_LOC=/usr/bin/mercure SVC_BIN_LOC=/go/bin CADDY_CONFIG_FILE_LOC=/etc/caddy HOME=/home/caddy

# OCI labels for upstream traceability.
ARG SVC_VERSION
ARG UPSTREAM_VERSION
LABEL org.opencontainers.image.title="Mercure Hub (lzrf0cuz fork)" \
      org.opencontainers.image.version="${SVC_VERSION}" \
      org.opencontainers.image.source="https://github.com/dunglas/mercure" \
      org.opencontainers.image.base.version="${UPSTREAM_VERSION}" \
      org.opencontainers.image.description="Real-time SSE notification service based on Mercure protocol" \
      dev.orbstack.icon="https://mercure.rocks/favicon-32x32.png"

# Install nss-tools for certificate management.
RUN apk add --no-cache nss-tools

# Create home directory and initialize NSS databases as root and set proper permissions.
RUN mkdir -p /etc/ssl/nssdb /home/caddy/.pki/nssdb && \
    certutil -N -d /etc/ssl/nssdb --empty-password && \
    certutil -N -d /home/caddy/.pki/nssdb --empty-password && \
    chmod -R 755 /etc/ssl/nssdb /home/caddy/.pki

# Copy the binary from the build stage.
COPY --from=build ${SVC_BIN_LOC}/${SVC} ${SVC_LOC}

# Copy Caddyfile configurations bundled with the image:
# - Caddyfile: Production (direct TLS, no fronting load balancer) — default CMD target
# - local.Caddyfile: Local development (mkcert TLS, debug UI)
# - dev.Caddyfile: Upstream permissive dev config (anonymous, demo, cors *).
#   Shipped to keep the image compatible with the upstream Helm chart's default
#   args (`/etc/caddy/dev.Caddyfile`) and the install/traefik docs. CMD does NOT
#   default to it — operator must explicitly opt in.
# Downstream deploys behind a load balancer layer their own Caddyfiles on top
# of this image via `COPY --from=source`.
COPY Caddyfile ${CADDY_CONFIG_FILE_LOC}/Caddyfile
COPY local.Caddyfile ${CADDY_CONFIG_FILE_LOC}/local.Caddyfile
COPY dev.Caddyfile ${CADDY_CONFIG_FILE_LOC}/dev.Caddyfile

# Expose ports for all Caddyfile configurations:
# - 80: HTTP redirect (Caddyfile, local) or h2c (when a downstream reverse-proxy config enables it)
# - 443: HTTPS (Caddyfile, local.Caddyfile)
# - 9091: Metrics endpoint (all configs)
# Admin API (:2019) is intentionally NOT in EXPOSE — it's localhost-only
# inside the container, and the HEALTHCHECK reaches it via in-container
# loopback. compose.yaml may host-bind 2019 to 127.0.0.1 for dev curl
# convenience, but that's a compose-layer choice, not an image contract.
EXPOSE 80 443 9091

# Transport-aware health via Caddy admin API (localhost-only inside container).
# The admin API serves /mercure/health/{ready,live} via the mercure_health admin module.
# Requires admin to be enabled in the Caddyfile (all included configs leave it at default).
# Uses 127.0.0.1 explicitly because `admin localhost:2019` binds IPv4 loopback only,
# and Alpine's `localhost` resolution prefers IPv6 (::1) — which would Connection-refuse.
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=5 \
    CMD wget -q --spider http://127.0.0.1:2019/mercure/health/ready

# Set working directory to root so ./data paths resolve to /data (volume mount)
WORKDIR /

# Default: production config (override via compose.yaml command or -c flag)
CMD ["mercure", "run", "-c", "/etc/caddy/Caddyfile"]
