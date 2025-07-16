FROM --platform=linux/amd64 clux/muslrust:1.88.0-stable-2025-07-07 as builder
ENV CARGO_NET_GIT_FETCH_WITH_CLI=true

WORKDIR /app
COPY . .
RUN rustup target add x86_64-unknown-linux-musl

# Install musl-specific OpenSSL development libraries
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    musl-dev \
    musl-tools \
    && rm -rf /var/lib/apt/lists/*

# Install musl OpenSSL specifically
RUN wget -q https://www.openssl.org/source/openssl-1.1.1w.tar.gz && \
    tar -xzf openssl-1.1.1w.tar.gz && \
    cd openssl-1.1.1w && \
    CC=musl-gcc ./Configure linux-x86_64 --prefix=/usr/local/musl --openssldir=/usr/local/musl no-shared && \
    make -j$(nproc) && \
    make install && \
    cd .. && \
    rm -rf openssl-1.1.1w openssl-1.1.1w.tar.gz

ENV OPENSSL_STATIC=1
ENV OPENSSL_DIR=/usr/local/musl
ENV PKG_CONFIG_ALLOW_CROSS=1

# Exclude the playground that uses edition 2024
RUN RUSTFLAGS='-C target-cpu=x86-64-v2' cargo build --release --target x86_64-unknown-linux-musl --features jemalloc --workspace --exclude rindexer_rust_playground

FROM --platform=linux/amd64 scratch
COPY --from=builder /app/target/x86_64-unknown-linux-musl/release/rindexer_cli /app/rindexer
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

ENTRYPOINT ["/app/rindexer"]