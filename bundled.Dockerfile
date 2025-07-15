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

FROM --platform=linux/amd64 node:lts-bookworm as node-builder
RUN apt update && apt install -y ca-certificates
WORKDIR /app
COPY . .
RUN cd /app/graphql && npm i && npm run build-linux

FROM --platform=linux/amd64 debian:bookworm-slim
RUN apt update \
  && apt install -y libssl-dev libc-dev libstdc++6 ca-certificates lsof curl git \
  && apt-get autoremove --yes \
  && apt-get clean --yes \
  && rm -rf /var/lib/apt/lists/*

RUN curl -L https://foundry.paradigm.xyz | bash
RUN /root/.foundry/bin/foundryup

COPY --from=node-builder /app/core/resources/rindexer-graphql-linux /app/resources/rindexer-graphql-linux
COPY --from=builder /app/target/x86_64-unknown-linux-musl/release/rindexer_cli /app/rindexer
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

ENTRYPOINT ["/app/rindexer"]