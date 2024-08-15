FROM --platform=linux/amd64 rust:1.79-slim-bookworm as builder
ENV CARGO_NET_GIT_FETCH_WITH_CLI=true
RUN apt update && apt install -y libssl-dev pkg-config build-essential

WORKDIR /app
COPY . .
RUN RUSTFLAGS='-C target-cpu=native' cargo build --release --features jemalloc

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
COPY --from=builder /app/target/release/rindexer_cli /app/rindexer

ENTRYPOINT ["/app/rindexer"]
