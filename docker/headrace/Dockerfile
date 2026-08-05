# Build headrace as a static musl binary and ship it on Alpine.
#
# OTLP support is always compiled in (the binary enables headrace-core's `otlp`
# feature). The dependency tree is pure Rust, so the musl build needs no extra system
# libraries, and headrace speaks plain gRPC, so no TLS libraries are needed at runtime.
#
# Build context is the repo root: `docker build -f docker/headrace/Dockerfile .`
FROM rust:1-alpine AS build
RUN apk add --no-cache musl-dev
WORKDIR /src
COPY . .
RUN cargo build --release --locked -p headrace

FROM alpine:3.20
RUN apk add --no-cache ca-certificates \
 && adduser -D -H -u 65532 nonroot
COPY --from=build /src/target/release/headrace /usr/local/bin/headrace
USER nonroot
# OTLP/gRPC receiver.
EXPOSE 4317
ENTRYPOINT ["/usr/local/bin/headrace"]
