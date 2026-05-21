# syntax=docker/dockerfile:1.7
# Build stage
FROM golang:1.22-alpine AS builder

WORKDIR /src
RUN apk add --no-cache git ca-certificates

COPY go.mod go.sum* ./
RUN go mod download

COPY . .

ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev

RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH:-amd64} \
    go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" \
    -o /out/sidecar ./cmd/sidecar
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH:-amd64} \
    go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" \
    -o /out/meshctl ./cmd/meshctl

# Runtime stage
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /out/sidecar /usr/local/bin/sidecar
COPY --from=builder /out/meshctl /usr/local/bin/meshctl

USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/sidecar"]
