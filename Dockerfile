# syntax = docker/dockerfile:1.16

# WAL-G Build section
# Need to build from sources, because binary distributions do not include necessary base OS libs
# Go 1.27: wal-g master's go.mod requires go >= 1.27.2 and the image sets
# GOTOOLCHAIN=local, so pin a patch tag: a stale cached golang:1.27-trixie
# (1.27.0/1.27.1) would fail `make deps`.
# Base images, apt and Go modules come through nora (packages.maestra.io, 3-day hold,
# issues-maestra#1649). Token = BuildKit secret PRIVATE_PACKAGES_TOKEN (see the Makefile).
FROM packages.maestra.io/docker-hub/library/golang:1.27.3-trixie@sha256:af1bfc2614ebb1dbd31af514b78740fd678850e76498762b425ecee4c5c6ea80 AS walg-builder
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
RUN --mount=type=secret,id=PRIVATE_PACKAGES_TOKEN,required=true --mount=type=bind,from=public.ecr.aws/g5f9s8a4/nora-apt:1.0.0@sha256:cd5fcec38f8b7c4fdd283d41f49ac656751d4deed168111f89f44976a43a8178,target=/nora-apt,rw \
    . /nora-apt/env.sh && apt-get update && apt-get install -y libbrotli-dev liblzo2-dev libsodium-dev curl cmake git
# Fetch exactly WALG_COMMIT (GitHub serves a reachable commit by SHA; `git clone --branch` takes refs only)
RUN git init -q $(go env GOPATH)/src/github.com/wal-g/wal-g && \
    cd $(go env GOPATH)/src/github.com/wal-g/wal-g && \
    git remote add origin ${WALG_REPO} && \
    git fetch -q --depth 1 origin ${WALG_COMMIT} && \
    git checkout -q FETCH_HEAD

# Preparing && building necessary dependencies
# go get: x/net >= 0.62.0 for CVE-2026-78661; WALG_COMMIT pins v0.60.0 (wal-g master
# has 0.62.0). Drop it when WALG_COMMIT moves past that.
RUN --mount=type=secret,id=PRIVATE_PACKAGES_TOKEN,env=NORA_TOKEN,required=true \
    cd $(go env GOPATH)/src/github.com/wal-g/wal-g && \
    export GOPROXY="https://token:${NORA_TOKEN}@packages.maestra.io/go" && \
    export GOOS=${TARGETOS} && \
    export GOARCH=${TARGETARCH} && \
    go get golang.org/x/net@v0.62.0 && \
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
# go.mod requires go >= 1.26.4 (cloudnative-pg 1.28.4); pinned for the same reason.
FROM packages.maestra.io/docker-hub/library/golang:1.26.10-trixie@sha256:fd9a91f0ece5664556f4851d06dce5e5b505a71166dffd385a03ba9d200b6983 AS controller-builder
ARG TARGETOS
ARG TARGETARCH
ARG GIT_TAG=v0.0.0-dev
ARG GIT_COMMIT=f07cc65b11df5506d85f8b143c64884c1dcf24a4
COPY go.mod go.sum /cnpg-plugin-wal-g/
WORKDIR /cnpg-plugin-wal-g
ENV GOOS=$TARGETOS GOARCH=$TARGETARCH
RUN --mount=type=secret,id=PRIVATE_PACKAGES_TOKEN,env=NORA_TOKEN,required=true \
    export GOPROXY="https://token:${NORA_TOKEN}@packages.maestra.io/go" && go mod download
COPY . /cnpg-plugin-wal-g/
# make build go-installs controller-gen: modules through nora too.
RUN --mount=type=secret,id=PRIVATE_PACKAGES_TOKEN,env=NORA_TOKEN,required=true \
    export GOPROXY="https://token:${NORA_TOKEN}@packages.maestra.io/go" && make build && ./output/cnpg-plugin-wal-g version

# Runtime image section
#
# Primary runtime image (using trixie-slim due to wal-g dynamic linked C dependencies)
FROM packages.maestra.io/docker-hub/library/debian:trixie-slim@sha256:a72b4433de1cf7d6bae0c4cee74d0547f58601a643b3375f070c51600d63b6b2 AS runtime
LABEL org.opencontainers.image.source=https://github.com/wal-g/cnpg-plugin-wal-g
LABEL org.opencontainers.image.description="CloudNativePG WAL-G Backup Plugin"
LABEL org.opencontainers.image.licenses="Apache-2.0"

# ca-certificates: TLS to S3/R2/GCS. procps: `ps` for the operator runbooks
# (kubectl exec -c plugin-wal-g -- ps). No curl: neither the plugin (it only
# execs wal-g) nor anything exec'd into the sidecar uses it, and libcurl's
# dependency tree carried HIGH CVEs. --no-install-recommends keeps psmisc,
# bash-completion & co. out; upgrade picks up Debian security fixes released
# after the base image was built.
RUN --mount=type=secret,id=PRIVATE_PACKAGES_TOKEN,required=true --mount=type=bind,from=public.ecr.aws/g5f9s8a4/nora-apt:1.0.0@sha256:cd5fcec38f8b7c4fdd283d41f49ac656751d4deed168111f89f44976a43a8178,target=/nora-apt,rw \
    . /nora-apt/env.sh && apt-get update && \
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
