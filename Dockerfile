# syntax=docker/dockerfile:1
FROM --platform=$BUILDPLATFORM golang:1.26.4-bookworm AS build
ARG TARGETOS=linux
ARG TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /out/cnpg-connect-plugin ./cmd/cnpg-connect-plugin

FROM scratch
LABEL org.opencontainers.image.source="https://github.com/nakiner/cnpg-connect-plugin"
LABEL org.opencontainers.image.description="CloudNativePG topology discovery plugin"
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/cnpg-connect-plugin /cnpg-connect-plugin
USER 65532:65532
EXPOSE 9090 8080 8081
ENTRYPOINT ["/cnpg-connect-plugin"]
