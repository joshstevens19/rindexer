# Stage 1: Get the pre-built binary from GitHub Release
# We don't need clux/muslrust here anymore, just a base image with curl/wget/gh CLI
FROM --platform=linux/amd64 alpine/git:latest as downloader
# You might need a specific curl/gh cli image if alpine/git doesn't have what's needed.
# Or just a basic alpine and install curl/gh. Let's try alpine/git first.

# Arguments to pass the version dynamically from the GitHub Actions workflow
ARG RELEASE_VERSION

WORKDIR /tmp/downloads

# Download the specific Linux binary from your GitHub Release
# Using curl as it's generally available and works well with GitHub Releases
# You will need to determine the exact filename for the Linux binary from your releases
# Based on your changelog, it's typically: rindexer_linux-amd64.tar.gz
# We'll use the 'gh' CLI for simplicity and robustness.
RUN apk add --no-cache curl unzip # Ensure curl and unzip are available
RUN wget -qO- https://cli.github.com/packages/githubcli-archive-keyring.gpg | tee /etc/apt/trusted.gpg.d/githubcli.gpg > /dev/null \
    && apt-get update \
    && apt-get install -y gh # Install GitHub CLI

RUN gh release download "v${RELEASE_VERSION}" \
    --repo joshstevens19/rindexer \
    --pattern "rindexer_linux-amd64.tar.gz" \
    --dir . \
    --skip-verify # Adjust pattern based on exact filename in release assets
    # For private repos: add --token "${{ env.GH_TOKEN }}"
    # For public repos, secrets.GITHUB_TOKEN should suffice if the context is correct.

# Extract the binary
RUN tar -xzvf rindexer_linux-amd64.tar.gz
# The binary inside the tar.gz is usually named `rindexer_cli` or `rindexer`
# Adjust if the name inside the tar.gz is different.
RUN mv rindexer_cli /usr/local/bin/rindexer # or `mv rindexer /usr/local/bin/rindexer`

# --- Optional: If you need rindexer-graphql-linux ---
FROM --platform=linux/amd64 node:lts-bookworm as node-builder
RUN apt update && apt install -y ca-certificates
WORKDIR /app
COPY . .
RUN cd /app/graphql && npm i && npm run build-linux
# --- End Optional ---


# Final image (runtime)
FROM --platform=linux/amd64 debian:bookworm-slim
RUN apt update \
  && apt install -y libssl-dev libc-dev libstdc++6 ca-certificates lsof curl git \
  && apt-get autoremove --yes \
  && apt-get clean --yes \
  && rm -rf /var/lib/apt/lists/*

# Install Foundryup if needed in the final image
# This depends on whether your 'rindexer' binary itself needs foundryup at runtime,
# or if foundryup was only for compilation/testing. If it's for runtime, keep it.
RUN curl -L https://foundry.paradigm.xyz | bash
RUN /root/.foundry/bin/foundryup

# Copy rindexer-graphql-linux if it was generated
COPY --from=node-builder /app/core/resources/rindexer-graphql-linux /app/resources/rindexer-graphql-linux

# Copy the pre-built rindexer binary from the downloader stage
COPY --from=downloader /usr/local/bin/rindexer /app/rindexer
COPY --from=downloader /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

ENTRYPOINT ["/app/rindexer"]