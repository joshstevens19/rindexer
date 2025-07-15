# Stage 1: Get the pre-built binary from GitHub Release
FROM --platform=linux/amd64 alpine/git:latest as downloader

WORKDIR /tmp/downloads

# Install curl and unzip for downloading and extracting
RUN apk add --no-cache curl unzip

# Download and install GitHub CLI (gh) static binary for Alpine Linux
RUN GH_CLI_VERSION=$(curl -s "https://api.github.com/repos/cli/cli/releases/latest" | grep '"tag_name":' | sed -E 's/.*"v([^"]+)".*/\1/') \
    && curl -sSL "https://github.com/cli/cli/releases/download/v${GH_CLI_VERSION}/gh_${GH_CLI_VERSION}_linux_amd64.tar.gz" -o gh_cli.tar.gz \
    && tar -xvzf gh_cli.tar.gz \
    && mv gh_${GH_CLI_VERSION}_linux_amd64/bin/gh /usr/local/bin/gh \
    && rm -rf gh_cli.tar.gz gh_${GH_CLI_VERSION}_linux_amd64

# Argument to pass the target release version from the GitHub Actions workflow
ARG RELEASE_VERSION

# Download the specific Linux binary from your GitHub Release
# Removed: --skip-verify
RUN gh release download "v${RELEASE_VERSION}" \
    --repo joshstevens19/rindexer \
    --pattern "rindexer_linux-amd64.tar.gz" \
    --dir .


# Extract the binary from the downloaded tar.gz
RUN tar -xzvf rindexer_linux-amd64.tar.gz
# The binary inside the tar.gz is typically named `rindexer_cli`. Adjust if it's just `rindexer`.
RUN mv rindexer_cli /usr/local/bin/rindexer


# Stage 2: Build Node.js assets (if your project requires rindexer-graphql-linux)
FROM --platform=linux/amd64 node:lts-bookworm as node-builder
RUN apt update && apt install -y ca-certificates
WORKDIR /app
COPY . .
RUN cd /app/graphql && npm i && npm run build-linux


# Stage 3: Final image for runtime
FROM --platform=linux/amd64 debian:bookworm-slim
RUN apt update \
  && apt install -y libssl-dev libc-dev libstdc++6 ca-certificates lsof curl git \
  && apt-get autoremove --yes \
  && apt-get clean --yes \
  && rm -rf /var/lib/apt/lists/*

# Install Foundryup if your application binary (rindexer) requires it at runtime
RUN curl -L https://foundry.paradigm.xyz | bash
RUN /root/.foundry/bin/foundryup

# Copy rindexer-graphql-linux from the node-builder stage
COPY --from=node-builder /app/core/resources/rindexer-graphql-linux /app/resources/rindexer-graphql-linux

# Copy the pre-built rindexer binary from the downloader stage
COPY --from=downloader /usr/local/bin/rindexer /app/rindexer
# Copy CA certificates if needed for trust store (downloader stage might have them)
COPY --from=downloader /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

ENTRYPOINT ["/app/rindexer"]