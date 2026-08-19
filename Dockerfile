# syntax=docker/dockerfile:1

FROM --platform=$BUILDPLATFORM golang:1.25-bookworm AS build

ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev

WORKDIR /src

COPY go.mod go.sum ./
COPY third_party ./third_party
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" \
    -o /out/lightningbnb ./cmd/lightningbnb

FROM debian:bookworm-slim

ARG VERSION=dev
LABEL org.opencontainers.image.title="LightningBNB" \
      org.opencontainers.image.description="Resumable TCP bridge over Bluetooth Low Energy" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.source="https://github.com/Sygmei/LightningBNB"

COPY --from=build /out/lightningbnb /usr/local/bin/lightningbnb

ENTRYPOINT ["/usr/local/bin/lightningbnb"]
CMD ["--help"]
