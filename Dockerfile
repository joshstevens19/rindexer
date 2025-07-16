FROM --platform=linux/amd64 clux/muslrust:1.88.0-stable-2025-07-07 as builder
ENV CARGO_NET_GIT_FETCH_WITH_CLI=true

WORKDIR /app
COPY . .
RUN rustup target add x86_64-unknown-linux-musl

# Install necessary tools including wget and make
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    musl-dev \
    musl-tools \
    wget \
    make \
    perl \
    && rm -rf /var/lib/apt/lists/*

# Try using the existing OpenSSL with correct paths for musl
ENV OPENSSL_STATIC=1
ENV OPENSSL_DIR=/usr
ENV OPENSSL_LIB_DIR=/usr/lib/x86_64-linux-gnu
ENV OPENSSL_INCLUDE_DIR=/usr/include
ENV PKG_CONFIG_ALLOW_CROSS=1

# Exclude the playground that uses edition 2024
RUN RUSTFLAGS='-C target-cpu=x86-64-v2' cargo build --release --target x86_64-unknown-linux-musl --features jemalloc --workspace --exclude rindexer_rust_playground

FROM --platform=linux/amd64 scratch
COPY --from=builder /app/target/x86_64-unknown-linux-musl/release/rindexer_cli /app/rindexer
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

ENTRYPOINT ["/app/rindexer"]