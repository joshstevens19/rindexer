FROM --platform=linux/amd64 clux/muslrust:stable as builder
ENV CARGO_NET_GIT_FETCH_WITH_CLI=true
RUN mkdir /app
WORKDIR /app
COPY . .
RUN rustup target add x86_64-unknown-linux-musl
RUN RUSTFLAGS='-C target-cpu=native' cargo build --release --target x86_64-unknown-linux-musl --features jemalloc

FROM --platform=linux/amd64 scratch
COPY --from=builder /app/target/x86_64-unknown-linux-musl/release/rindexer_cli /app/rindexer
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

ENTRYPOINT ["/app/rindexer"]
