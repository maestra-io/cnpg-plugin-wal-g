# syntax = docker/dockerfile:1.16

# WAL-G Build section
# Need to build from sources, because binary distributions do not include necessary base OS libs
FROM packages.maestra.io/docker-hub/library/golang:1.26-trixie@sha256:fd9a91f0ece5664556f4851d06dce5e5b505a71166dffd385a03ba9d200b6983 AS walg-builder
ARG TARGETOS
ARG TARGETARCH
ARG WALG_VERSION=v3.0.8
ARG WALG_REPO=https://github.com/wal-g/wal-g

# build arguments for wal-g
ARG USE_BROTLI=1
ARG USE_LIBSODIUM=1
ARG USE_LZO=1

# apt, Go modules: through nora (packages.maestra.io, 3-day hold, issues-maestra#1649); token =
# BuildKit secret PRIVATE_PACKAGES_TOKEN (docker build --secret id=PRIVATE_PACKAGES_TOKEN,env=...).
# Install lib dependencies
RUN --mount=type=secret,id=PRIVATE_PACKAGES_TOKEN,required=true --mount=type=bind,from=public.ecr.aws/g5f9s8a4/nora-apt:1.0.0@sha256:cd5fcec38f8b7c4fdd283d41f49ac656751d4deed168111f89f44976a43a8178,target=/nora-apt,rw \
    . /nora-apt/env.sh && apt-get update && apt-get install -y libbrotli-dev liblzo2-dev libsodium-dev curl cmake git
# Clone wal-g sources
RUN git clone --depth 1 --branch ${WALG_VERSION} ${WALG_REPO} $(go env GOPATH)/src/github.com/wal-g/wal-g

# Preparing && building necessary dependencies
RUN --mount=type=secret,id=PRIVATE_PACKAGES_TOKEN,env=NORA_TOKEN,required=true \
    cd $(go env GOPATH)/src/github.com/wal-g/wal-g && \
    export GOPROXY="https://token:${NORA_TOKEN}@packages.maestra.io/go" && \
    export GOOS=${TARGETOS} && \
    export GOARCH=${TARGETARCH} && \
    make deps

# Build wal-g for postgresql
RUN cd $(go env GOPATH)/src/github.com/wal-g/wal-g && \
    export GOOS=${TARGETOS} && \
    export GOARCH=${TARGETARCH} && \
    export GOEXPERIMENT=jsonv2 && \
    make pg_build && make pg_install && /wal-g --version

# Controller Build section
#
FROM packages.maestra.io/docker-hub/library/golang:1.26-trixie@sha256:fd9a91f0ece5664556f4851d06dce5e5b505a71166dffd385a03ba9d200b6983 AS controller-builder
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

RUN --mount=type=secret,id=PRIVATE_PACKAGES_TOKEN,required=true --mount=type=bind,from=public.ecr.aws/g5f9s8a4/nora-apt:1.0.0@sha256:cd5fcec38f8b7c4fdd283d41f49ac656751d4deed168111f89f44976a43a8178,target=/nora-apt,rw \
    . /nora-apt/env.sh && apt-get update && apt-get install -y ca-certificates curl procps && apt-get clean && rm -rf /var/lib/apt/lists/*
RUN groupadd -o -g 26 postgres && useradd -ms /bin/bash -u 26 -g postgres postgres
WORKDIR /home/postgres
COPY --from=controller-builder --chmod=0755 /cnpg-plugin-wal-g/output/cnpg-plugin-wal-g /usr/local/bin/cnpg-plugin-wal-g
COPY --from=walg-builder /wal-g /usr/local/bin/wal-g
# Using same user as postgres user in postgresql instances to be able to manage files in PGDATA
USER 26:26

ENTRYPOINT ["/usr/local/bin/cnpg-plugin-wal-g"]
