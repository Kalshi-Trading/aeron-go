# Multi-stage build of the Go e2e test clients (e2e/cmd/*). Build context is the repository root:
#   docker build -f docker/go-clients.Dockerfile -t aeron-go-e2e .
# Override the base images to use a registry mirror when Docker Hub rate limits, e.g.
#   GO_IMAGE=mirror.gcr.io/library/golang:1.21-bookworm RUNTIME_IMAGE=mirror.gcr.io/library/debian:bookworm-slim
ARG GO_IMAGE=golang:1.21-bookworm
ARG RUNTIME_IMAGE=debian:bookworm-slim
FROM ${GO_IMAGE} AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/ ./e2e/cmd/...

FROM ${RUNTIME_IMAGE}
COPY --from=build /out/ /usr/local/bin/
ENV AERON_DIR=/dev/shm/aeron-compose
