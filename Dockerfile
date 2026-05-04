############################
# STEP 0 build arguments
############################
ARG GO_VERSION=1.26
ARG BASE_VARIANT=trixie

############################
# STEP 1 build the binary
############################
FROM golang:${GO_VERSION}-${BASE_VARIANT} AS builder

LABEL org.opencontainers.image.title="bergrebase"
LABEL org.opencontainers.image.source="https://github.com/fairtier/bergrebase"
LABEL org.opencontainers.image.licenses="Apache-2.0"

WORKDIR /app

ENV GOOS=linux \
    GOARCH=amd64 \
    CGO_ENABLED=0 \
    GOAMD64=v3

COPY go.mod go.sum* ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -o /out/bergrb ./cmd/bergrb

############################
# STEP 2 distroless static
############################
FROM gcr.io/distroless/static:nonroot

COPY --from=builder /out/bergrb /bin/bergrb

USER nonroot:nonroot

ENTRYPOINT ["/bin/bergrb"]
