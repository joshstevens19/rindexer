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

FROM --platform=linux/amd64 node:lts-bookworm as node-builder
RUN apt update && apt install -y ca-certificates
WORKDIR /app
COPY . .
RUN cd /app/graphql && npm i && npm run build-linux

FROM --platform=linux/amd64 debian:bookworm-slim
RUN apt update \
  && apt install -y libssl3 libc-dev libstdc++6 ca-certificates lsof curl git \
  && apt-get autoremove --yes \
  && apt-get clean --yes \
  && rm -rf /var/lib/apt/lists/*

RUN curl -L https://foundry.paradigm.xyz | bash
RUN /root/.foundry/bin/foundryup

COPY --from=node-builder /app/core/resources/rindexer-graphql-linux /app/resources/rindexer-graphql-linux
COPY --from=builder /app/target/release/rindexer_cli /app/rindexer
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

ENTRYPOINT ["/app/rindexer"]