FROM --platform=linux/amd64 alpine:3.18 as builder

# Install Rust and build dependencies
RUN apk add --no-cache \
    rust \
    cargo \
    musl-dev \
    openssl-dev \
    openssl-libs-static \
    pkgconfig \
    git

ENV CARGO_NET_GIT_FETCH_WITH_CLI=true
ENV OPENSSL_STATIC=1
ENV OPENSSL_DIR=/usr

WORKDIR /app
COPY . .

RUN RUSTFLAGS='-C target-cpu=x86-64-v2' cargo build --release --target x86_64-unknown-linux-musl --features jemalloc

FROM --platform=linux/amd64 scratch
COPY --from=builder /app/target/x86_64-unknown-linux-musl/release/rindexer_cli /app/rindexer
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

ENTRYPOINT ["/app/rindexer"]