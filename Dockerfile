FROM --platform=linux/amd64 clux/muslrust:1.88.0-stable-2025-07-07 as builder
ENV CARGO_NET_GIT_FETCH_WITH_CLI=true

WORKDIR /app
COPY . .
RUN rustup target add x86_64-unknown-linux-musl

RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

ENV OPENSSL_STATIC=1
ENV PKG_CONFIG_ALLOW_CROSS=1
ENV PKG_CONFIG_PATH=/usr/lib/x86_64-linux-musl/pkgconfig:/usr/local/musl/lib/pkgconfig
ENV PKG_CONFIG_SYSROOT_DIR=/usr/x86_64-linux-musl:/usr/local/musl
ENV OPENSSL_DIR=/usr/x86_64-linux-musl/usr:/usr/local/musl/usr

RUN RUSTFLAGS='-C target-cpu=x86-64-v2' cargo build --release --target x86_64-unknown-linux-musl --features jemalloc

FROM --platform=linux/amd64 scratch
COPY --from=builder /app/target/x86_64-unknown-linux-musl/release/rindexer_cli /app/rindexer
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

ENTRYPOINT ["/app/rindexer"]
