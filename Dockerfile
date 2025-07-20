FROM --platform=linux/amd64 rust:1.88.0-bookworm as builder
ENV CARGO_NET_GIT_FETCH_WITH_CLI=true

# Install OpenSSL dev libraries and clang for bindgen
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    clang \
    libclang-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .

# Build for standard Linux (glibc) instead of musl
RUN RUSTFLAGS='-C target-cpu=x86-64-v2' cargo build --release --features jemalloc,reth --workspace --exclude rindexer_rust_playground

FROM --platform=linux/amd64 debian:bookworm-slim
RUN apt-get update && apt-get install -y \
    libssl3 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/rindexer_cli /app/rindexer
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

ENTRYPOINT ["/app/rindexer"]