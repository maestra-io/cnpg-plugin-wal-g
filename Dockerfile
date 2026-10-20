# syntax = docker/dockerfile:1.16

# WAL-G Build section
# Need to build from sources, because binary distributions do not include necessary base OS libs
# Go 1.27: wal-g master's go.mod requires go >= 1.27.2 (the image sets GOTOOLCHAIN=local).
FROM docker.io/golang:1.27-trixie AS walg-builder
ARG TARGETOS
ARG TARGETARCH
ARG WALG_REPO=https://github.com/wal-g/wal-g
# Pinned upstream master commit, not a release tag: the newest tag (v3.0.9)
# still vendors google.golang.org/grpc v1.83.0 (CVE-2026-84445/84304) and
# golang.org/x/net v0.58.0. v3.0.9-185-gebd2e83f vendors grpc v1.84.0,
# x/crypto v0.57.0, x/net v0.60.0, pgx/v5 v5.11.0 (trivy: 0 CRITICAL/HIGH).
# Move back to a tag (set WALG_COMMIT to the tag's commit) once one ships.
ARG WALG_COMMIT=ebd2e83f8e39b5997c7694616af14f68f05a3223
# Stamped into `wal-g --version` (the Makefile reads it from the environment).
ARG WALG_VERSION=v3.0.9-185-gebd2e83f

# build arguments for wal-g
ARG USE_BROTLI=1
ARG USE_LIBSODIUM=1
ARG USE_LZO=1

# Install lib dependencies
RUN apt update && apt install -y libbrotli-dev liblzo2-dev libsodium-dev curl cmake git
# Fetch exactly WALG_COMMIT (GitHub serves a reachable commit by SHA; `git clone --branch` takes refs only)
RUN git init -q $(go env GOPATH)/src/github.com/wal-g/wal-g && \
    cd $(go env GOPATH)/src/github.com/wal-g/wal-g && \
    git remote add origin ${WALG_REPO} && \
    git fetch -q --depth 1 origin ${WALG_COMMIT} && \
    git checkout -q FETCH_HEAD

# Preparing && building necessary dependencies
RUN cd $(go env GOPATH)/src/github.com/wal-g/wal-g && \
    export GOOS=${TARGETOS} && \
    export GOARCH=${TARGETARCH} && \
    make deps

# Build wal-g for postgresql (GOEXPERIMENT comes from wal-g's Makefile).
# brotli, libsodium and lzo are linked statically (link_brotli.sh,
# link_libsodium.sh, go_deps' lzo patch): ldd must list glibc only, which is
# why the runtime image needs none of the -dev libraries' runtime packages.
RUN cd $(go env GOPATH)/src/github.com/wal-g/wal-g && \
    export GOOS=${TARGETOS} && \
    export GOARCH=${TARGETARCH} && \
    make pg_build && make pg_install && /wal-g --version && ldd /wal-g

# Controller Build section
#
FROM docker.io/golang:1.26-trixie AS controller-builder
ARG TARGETOS
ARG TARGETARCH
ARG GIT_TAG=v0.0.0-dev
ARG GIT_COMMIT=f07cc65b11df5506d85f8b143c64884c1dcf24a4
COPY go.mod go.sum /cnpg-plugin-wal-g/
WORKDIR /cnpg-plugin-wal-g
ENV GOOS=$TARGETOS GOARCH=$TARGETARCH
RUN go mod download
COPY . /cnpg-plugin-wal-g/
RUN make build && ./output/cnpg-plugin-wal-g version

# Runtime image section
#
# Primary runtime image (using trixie-slim due to wal-g dynamic linked C dependencies)
FROM docker.io/debian:trixie-slim AS runtime
LABEL org.opencontainers.image.source=https://github.com/wal-g/cnpg-plugin-wal-g
LABEL org.opencontainers.image.description="CloudNativePG WAL-G Backup Plugin"
LABEL org.opencontainers.image.licenses="Apache-2.0"

# ca-certificates: TLS to S3/R2/GCS. procps: `ps` for the operator runbooks
# (kubectl exec -c plugin-wal-g -- ps). No curl: neither the plugin (it only
# execs wal-g) nor anything exec'd into the sidecar uses it, and libcurl's
# dependency tree carried HIGH CVEs. --no-install-recommends keeps psmisc,
# bash-completion & co. out; upgrade picks up Debian security fixes released
# after the base image was built.
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y --no-install-recommends ca-certificates procps && \
    apt-get clean && rm -rf /var/lib/apt/lists/*
RUN groupadd -o -g 26 postgres && useradd -ms /bin/bash -u 26 -g postgres postgres
WORKDIR /home/postgres
COPY --from=controller-builder --chmod=0755 /cnpg-plugin-wal-g/output/cnpg-plugin-wal-g /usr/local/bin/cnpg-plugin-wal-g
COPY --from=walg-builder /wal-g /usr/local/bin/wal-g
# Fail the image build if wal-g misses a shared library in this stage.
RUN /usr/local/bin/wal-g --version
# Using same user as postgres user in postgresql instances to be able to manage files in PGDATA
USER 26:26

ENTRYPOINT ["/usr/local/bin/cnpg-plugin-wal-g"]
