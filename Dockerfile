# syntax=docker/dockerfile:1
#
# One Dockerfile, two targets:
#   docker build --target controller -t mini-object-store-controller .
#   docker build --target worker     -t mini-object-store-worker .
#
# The SQLite driver (modernc.org/sqlite) is pure Go, so both binaries are built
# with CGO_ENABLED=0 as static executables with no libc or libsqlite3 dependency.

ARG GO_VERSION=1.27
ARG ALPINE_VERSION=3.22

# ---- build -----------------------------------------------------------------
FROM golang:${GO_VERSION}-alpine AS build
WORKDIR /src
ENV CGO_ENABLED=0 GOFLAGS=-trimpath

# Download modules in their own layer so source edits do not re-download them.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download && go mod verify

COPY cmd ./cmd
COPY internal ./internal
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -ldflags="-s -w" -o /out/controller ./cmd/controller && \
    go build -ldflags="-s -w" -o /out/worker ./cmd/worker

# ---- shared runtime ----------------------------------------------------------
# Alpine (not distroless) so healthchecks and demonstrations can use the
# busybox wget, ls, sha256sum and dd that ship with it.
FROM alpine:${ALPINE_VERSION} AS runtime
RUN addgroup -S -g 10001 app && \
    adduser -S -D -H -u 10001 -G app app && \
    mkdir -p /data && chown app:app /data && chmod 0750 /data
# An empty named volume mounted at /data inherits this ownership.
WORKDIR /data
USER app:app

# ---- controller --------------------------------------------------------------
FROM runtime AS controller
COPY --from=build /out/controller /usr/local/bin/controller
ENV CONTROLLER_ADDR=:8080 \
    METADATA_BACKEND=sqlite \
    METADATA_DB_PATH=/data/metadata.db
EXPOSE 8080
HEALTHCHECK --interval=5s --timeout=3s --start-period=10s --retries=3 \
    CMD wget -q -O /dev/null http://127.0.0.1:8080/internal/workers || exit 1
# Exec form: the binary is PID 1 and receives SIGTERM from `docker stop`.
ENTRYPOINT ["/usr/local/bin/controller"]

# ---- worker ------------------------------------------------------------------
FROM runtime AS worker
COPY --from=build /out/worker /usr/local/bin/worker
ENV WORKER_ADDR=:8081 \
    WORKER_DATA_DIR=/data/chunks
EXPOSE 8081
# Shell form so $WORKER_ID expands; the inventory route only answers for the
# worker's own ID, so this also catches a misconfigured ID.
HEALTHCHECK --interval=5s --timeout=3s --start-period=10s --retries=3 \
    CMD wget -q -O /dev/null "http://127.0.0.1:8081/internal/workers/${WORKER_ID}/chunks" || exit 1
ENTRYPOINT ["/usr/local/bin/worker"]
