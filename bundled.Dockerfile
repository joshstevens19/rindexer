# Arguments passed from the GitHub Actions workflow
ARG RELEASE_VERSION
ARG IS_RELEASE_BUILD

# Stage 1: Binary Preparation (conditionally compiles OR downloads the rindexer binary)
# Start with a base image that supports Rust compilation tools (Debian-based for apt/gh CLI)
FROM --platform=linux/amd64 debian:bookworm-slim AS binary_preparer

# Install build essentials, git, curl, and gh CLI prerequisites
RUN apt update \
    && apt install -y --no-install-recommends \
        build-essential \
        curl \
        git \
        libssl-dev \
        pkg-config \
        tar \
        xz-utils \
        unzip \
    && rm -rf /var/lib/apt/lists/*

# Install Rust toolchain via rustup (crucial for compilation)
# Use a specific stable version that is compatible with your project's dependencies
# (e.g., 1.88.0, as your logs indicated earlier)
# As of current date (July 15, 2025), rustc 1.88.0 stable is current/upcoming.
# If `rustup.rs` fails, you might need to find a specific dated `rust:` image or use `clux/muslrust` again.
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain 1.88.0-x86_64-unknown-linux-musl --profile minimal
ENV PATH="/root/.cargo/bin:${PATH}"
ENV CARGO_NET_GIT_FETCH_WITH_CLI=true

# Install gh CLI for downloading release assets
RUN wget -qO- https://cli.github.com/packages/githubcli-archive-keyring.gpg | tee /etc/apt/trusted.gpg.d/githubcli.gpg > /dev/null \
    && apt update \
    && apt install -y gh \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app/rindexer_build

# Copy source code into the build context
COPY . .

# Conditional logic to either download a pre-built binary or compile from source
# The result will always be moved to /usr/local/bin/rindexer
RUN if [ "$IS_RELEASE_BUILD" = "true" ]; then \
        echo "IS_RELEASE_BUILD is 'true'. Attempting to download rindexer binary for release v${RELEASE_VERSION}..."; \
        gh release download "v${RELEASE_VERSION}" \
            --repo joshstevens19/rindexer \
            --pattern "rindexer_linux-amd64.tar.gz" \
            --dir . \
        && tar -xzvf rindexer_linux-amd64.tar.gz \
        && mv rindexer_cli /usr/local/bin/rindexer; \
        echo "Binary downloaded and extracted to /usr/local/bin/rindexer."; \
    else \
        echo "IS_RELEASE_BUILD is 'false'. Compiling rindexer from source..."; \
        # Compile the Rust project
        RUSTFLAGS='-C target-cpu=x86-64-v2' cargo build --release --target x86_64-unknown-linux-musl --features jemalloc; \
        # Move the compiled binary to the final location for this stage
        mv /app/rindexer_build/target/x86_64-unknown-linux-musl/release/rindexer_cli /usr/local/bin/rindexer; \
        echo "Binary compiled and moved to /usr/local/bin/rindexer."; \
    fi


# Stage 2: Build Node.js assets (if your project requires rindexer-graphql-linux)
FROM --platform=linux/amd64 node:lts-bookworm as node-builder
RUN apt update && apt install -y ca-certificates
WORKDIR /app
COPY . .
RUN cd /app/graphql && npm i && npm run build-linux


# Stage 3: Final image for runtime (minimal base image)
FROM --platform=linux/amd64 debian:bookworm-slim

# Install runtime dependencies for rindexer (e.g., libssl-dev, curl, git if needed by binary)
RUN apt update \
  && apt install -y --no-install-recommends libssl-dev libc-dev libstdc++6 ca-certificates lsof curl git \
  && apt-get autoremove --yes \
  && apt-get clean --yes \
  && rm -rf /var/lib/apt/lists/*

# Install Foundryup if your application binary (rindexer) requires it at runtime
# (This adds significant size; remove if only needed for build/testing.)
RUN curl -L https://foundry.paradigm.xyz | bash
RUN /root/.foundry/bin/foundryup

# Copy rindexer-graphql-linux from the node-builder stage
COPY --from=node-builder /app/core/resources/rindexer-graphql-linux /app/resources/rindexer-graphql-linux

# Copy the prepared rindexer binary from the 'binary_preparer' stage
COPY --from=binary_preparer /usr/local/bin/rindexer /app/rindexer

ENTRYPOINT ["/app/rindexer"]