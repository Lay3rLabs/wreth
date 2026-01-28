# syntax=docker.io/docker/dockerfile:1.7-labs

FROM lukemathwalker/cargo-chef:latest-rust-1 AS chef
WORKDIR /app

LABEL org.opencontainers.image.source=https://github.com/paradigmxyz/reth
LABEL org.opencontainers.image.licenses="MIT OR Apache-2.0"

# Install system dependencies
RUN apt-get update && apt-get install -y libclang-dev pkg-config protobuf-compiler

# Builds a cargo-chef plan
FROM chef AS planner
COPY --exclude=.git --exclude=dist . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json

# Build profile, release by default
ARG BUILD_PROFILE=release
ENV BUILD_PROFILE=$BUILD_PROFILE

# Extra Cargo flags
ARG RUSTFLAGS=""
ENV RUSTFLAGS="$RUSTFLAGS"

# Extra Cargo features
ARG FEATURES=""
ENV FEATURES=$FEATURES

# Builds dependencies
RUN cargo chef cook --profile $BUILD_PROFILE --features "$FEATURES" --recipe-path recipe.json

# Build application
COPY --exclude=dist . .
RUN cargo build --profile $BUILD_PROFILE --features "$FEATURES" --locked -p wreth

# ARG is not resolved in COPY so we have to hack around it by copying the
# binary to a temporary location
RUN cp /app/target/$BUILD_PROFILE/wreth /app/wreth

# Use Ubuntu as the release image
FROM ubuntu AS runtime
WORKDIR /app

# Copy wreth over from the build stage
COPY --from=builder /app/wreth /usr/local/bin

# Copy licenses
COPY LICENSE-* ./

# Ports:
# 30303     - P2P TCP/UDP
# 8545      - HTTP RPC
# 8546      - WebSocket RPC
# 9001      - Metrics
# 10000     - ExEx gRPC
EXPOSE 30303 30303/udp 9001 8545 8546 10000
ENTRYPOINT ["/usr/local/bin/wreth"]
