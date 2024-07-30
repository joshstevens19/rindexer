# syntax = docker/dockerfile:1.6
FROM ghcr.io/joshstevens19/rindexer:latest AS rindexer

ARG PROJECT_PATH="./example-app"

WORKDIR /usr/src/app

COPY ${PROJECT_PATH} /usr/src/app/rindexer-app

ENV PORT=3001

EXPOSE ${PORT}

ENTRYPOINT ["/app/rindexer"]
CMD ["start", "-p", "./rindexer-app", "all", "--port", "${PORT}"]
