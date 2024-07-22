# syntax = docker/dockerfile:1.6
FROM rust:bookworm AS builder

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

ARG DEBIAN_FRONTEND="noninteractive"
ARG DEBCONF_NOWARNINGS="yes"
ARG DEBCONF_TERSE="yes"
ARG LANG="C.UTF-8"

WORKDIR /app

RUN apt-get update --yes \
  && apt-get install --yes --no-install-recommends \
  ca-certificates \
  curl \
  unzip \
  && apt-get autoremove --yes \
  && apt-get clean --yes \
  && rm -rf /var/lib/apt/lists/*

# Install rundexer
RUN set -x && \
  curl \
  --fail \
  --silent \
  --location \
  --show-error \
  --url https://rindexer.xyz/install.sh | \
  sed 's/curl -sSf -L "$RESOURCES_URL" -o "$RINDEXER_DIR\/resources.zip"/curl -sSf -L "$RESOURCES_URL" -o "$RINDEXER_DIR\/resources.zip" || echo "Failed to download resources.zip"/' | \
  sed 's/unzip -o "$RINDEXER_DIR\/resources.zip"/[ -f "$RINDEXER_DIR\/resources.zip" ] \&\& unzip -o "$RINDEXER_DIR\/resources.zip" || echo "Skipping unzip, resources.zip not found"/' | \
  sed 's/rm "$RINDEXER_DIR\/resources.zip"/rm -f "$RINDEXER_DIR\/resources.zip"/' | \
  bash

FROM debian:bookworm-slim AS runtime

ARG DEBIAN_FRONTEND="noninteractive"
ARG DEBCONF_NOWARNINGS="yes"
ARG DEBCONF_TERSE="yes"
ARG LANG="C.UTF-8"

# where rindexer.yaml and a JSON abi file are located
# make sure the project path is in .dockerignore
ARG PROJECT_PATH="./rindexer"

ENV PATH="/root/.rindexer/bin:${PATH}"

WORKDIR /app

RUN apt-get update --yes \
  && apt-get install --yes --no-install-recommends \
  ca-certificates \
  bash \
  sudo \
  openssl \
  && apt-get autoremove --yes \
  && apt-get clean --yes \
  && rm -rf /var/lib/apt/lists/*

# Copy rindexer from builder stage
COPY --from=builder /root/.rindexer /root/.rindexer
COPY ${PROJECT_PATH} ${PROJECT_PATH}

COPY ./scripts/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT [ "/entrypoint.sh" ]
