# syntax=docker/dockerfile:latest

# Define the build image.
ARG BUILD_IMAGE=golang:1.27.3-alpine

# Define the base image. Pinned to the Caddy patch caddy/go.mod requires, so
# the runtime base matches the linked Caddy version.
ARG BASE_IMAGE=caddy:2.11.8-alpine

# Define the version. SVC_VERSION has no default, so a bare or unset-variable
# build fails instead of stamping a wrong version; the Taskfile passes it.
# UPSTREAM_VERSION and RT_VERSION keep pinned defaults.
ARG SVC_VERSION
ARG UPSTREAM_VERSION=v1.1.6
# In-tree redistransport module version. Bumped via `task version:bump:rt:*`
# (independent from SVC_VERSION because the transport is a separate Go
# module with its own release cadence).
ARG RT_VERSION=v0.1.0

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
    || { echo "version guard: SVC_VERSION ('$SVC_VERSION'), UPSTREAM_VERSION ('$UPSTREAM_VERSION') and RT_VERSION ('$RT_VERSION') must be non-empty build args (the Taskfile sets them)" >&2; exit 1; }

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
# the modules, so an image pin can't silently drift. Go: the build image's Go
# must have the same major.minor as the highest `go` directive across the four
# go.mod files, and be at least that version (a missing patch counts as 0).
# Caddy: the base image's caddy tag must equal the caddy/v2 version caddy/go.mod
# links. Unparseable inputs (a missing or reshaped go directive or go version, a
# registry-prefixed base image, a reshaped require line) fail loud rather than
# pass silently.
RUN need="$(awk 'FNR == 1 { seen = 0 } $1 == "go" && !seen { seen = 1; print $2 }' \
            go.mod ${SVC_CADDY_LOC}/go.mod redistransport/go.mod redistransport/caddy/go.mod \
        | awk 'function newer(a, b,  x, y, i) { split(a, x, "."); split(b, y, "."); for (i = 1; i <= 3; i++) if (x[i] + 0 != y[i] + 0) return x[i] + 0 > y[i] + 0; return 0 } \
               !/^[0-9]+[.][0-9]+([.][0-9]+)?$/ { bad = 1 } \
               NR == 1 || newer($0, max) { max = $0 } \
               END { if (!bad && NR == 4) print max }')"; \
    have="$(go version | awk '{print $3}' | sed 's/^go//')"; \
    { [ -n "$need" ] && printf '%s\n' "$have" | grep -Eq '^[0-9]+[.][0-9]+([.][0-9]+)?$'; } \
      || { echo "Go pin guard: unparseable go directive in a go.mod ('$need') or go version ('$have')" >&2; exit 1; }; \
    awk -v need="$need" -v have="$have" 'BEGIN { split(need, n, "."); split(have, h, "."); exit !(n[1] + 0 == h[1] + 0 && n[2] + 0 == h[2] + 0 && h[3] + 0 >= n[3] + 0) }' \
      || { echo "Go pin drift: the go.mod files need go $need or a later patch of the same minor; BUILD_IMAGE has $have" >&2; exit 1; }; \
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

# Build the binary. `-buildvcs=false`: .git/ is excluded by .dockerignore.
#
# `mercure.version` / `mercure.upstreamVersion` feed the mercure_version_info metric.
# `redistransport.version` stamps the Redis transport, which caddy/mercure/main.go links in.
RUN CGO_ENABLED=0 go build -buildvcs=false \
    -ldflags="\
      -X 'github.com/caddyserver/caddy/v2.CustomVersion=Mercure ${SVC_VERSION} (upstream ${UPSTREAM_VERSION}) Caddy' \
      -X 'github.com/caddyserver/caddy/v2.CustomBinaryName=mercure' \
      -X 'github.com/dunglas/mercure.version=${SVC_VERSION}' \
      -X 'github.com/dunglas/mercure.upstreamVersion=${UPSTREAM_VERSION}' \
      -X 'github.com/caddyserver/caddy/v2/modules/caddyhttp.ServerHeader=Mercure ${SVC_VERSION} (upstream ${UPSTREAM_VERSION})' \
      -X 'github.com/lzrf0cuz/mercure/redistransport.version=${RT_VERSION}'" \
    -tags="nobadger,nomysql,nopgx" -o ${SVC_BIN_LOC}/${SVC}

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
      org.opencontainers.image.description="Real-time SSE notification service based on Mercure protocol"

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
# - local.Caddyfile: Local development (Caddy internal TLS, debug UI)
COPY Caddyfile ${CADDY_CONFIG_FILE_LOC}/Caddyfile
COPY local.Caddyfile ${CADDY_CONFIG_FILE_LOC}/local.Caddyfile

# Expose ports for all Caddyfile configurations:
# - 80: HTTP redirect (Caddyfile, local) or h2c (when a downstream reverse-proxy config enables it)
# - 443: HTTPS (Caddyfile, local.Caddyfile)
# - 9091: Metrics endpoint (all configs)
# The caddy base image already EXPOSEs 2019 (admin); `admin localhost:2019`
# keeps it loopback-only inside the container.
EXPOSE 80 443 9091

# Transport-aware liveness via Caddy admin API (localhost-only inside container).
# The admin API serves /mercure/health/{ready,live} via the mercure_health admin module.
# Probes /live, not /ready; see docs/production/health-monitoring.md.
# Requires the admin API on port 2019 (the bundled configs keep it there).
# Uses 127.0.0.1, not localhost: admin binds IPv4 loopback and Alpine
# resolves localhost to ::1 first.
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=5 \
    CMD wget -q --spider http://127.0.0.1:2019/mercure/health/live

# Set working directory to root so ./data paths resolve to /data (volume mount)
WORKDIR /

# Default: production config (override via compose.yaml command or -c flag)
CMD ["mercure", "run", "-c", "/etc/caddy/Caddyfile"]
