FROM --platform=linux/amd64 rust:1.79-slim-bookworm as builder
ENV CARGO_NET_GIT_FETCH_WITH_CLI=true
RUN apt update && apt install -y libssl-dev binutils libc-dev libstdc++6 pkg-config

WORKDIR /app
COPY . .
RUN cargo build --release
RUN strip /app/target/release/rindexer_cli

FROM --platform=linux/amd64 node:lts-bookworm as node-builder
RUN apt update && apt install -y ca-certificates
WORKDIR /app
COPY . .
RUN cd /app/graphql && npm i && npm run build-linux

FROM --platform=linux/amd64 debian:bookworm-slim
RUN apt update \
  && apt install -y libssl-dev libc-dev libstdc++6 ca-certificates lsof \
  && apt-get autoremove --yes \
  && apt-get clean --yes \
  && rm -rf /var/lib/apt/lists/*

COPY --from=node-builder /app/core/resources/rindexer-graphql-linux /app/resources/rindexer-graphql-linux
COPY --from=builder /app/target/release/rindexer_cli /app/rindexer

ENTRYPOINT ["/app/rindexer"]
