# Build headrace as a fully static binary and ship it on scratch: no base image, no
# runtime dependencies. The static build uses the musl target - the standard, DNS-safe
# way to statically link a Rust binary (static glibc would break hostname resolution) -
# but nothing musl remains at runtime; the binary is self-contained.
#
# OTLP support is always compiled in (the binary enables headrace-core's `otlp`
# feature). The dep tree is pure Rust and headrace speaks plain gRPC, so no system
# libraries or CA certificates are needed.
#
# Build context is the repo root: `docker build -f docker/headrace/Dockerfile .`
FROM rust:1-alpine AS build
RUN apk add --no-cache musl-dev
WORKDIR /src
COPY . .
RUN cargo build --release --locked -p headrace

FROM scratch
COPY --from=build /src/target/release/headrace /headrace
# No /etc/passwd on scratch; run as a numeric non-root uid.
USER 65532:65532
# OTLP/gRPC receiver.
EXPOSE 4317
ENTRYPOINT ["/headrace"]
