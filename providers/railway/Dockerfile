# syntax = docker/dockerfile:1.6
FROM ghcr.io/joshstevens19/rindexer-bundled:latest AS rindexer

ARG PROJECT_PATH="./example-app"

WORKDIR /usr/src/app

COPY ${PROJECT_PATH} /usr/src/app/rindexer-app

ARG PORT="3001"
ENV PORT=${PORT}

EXPOSE ${PORT}

ENTRYPOINT ["/app/rindexer"]
CMD ["start", "-p", "./rindexer-app", "all"]
