# build image
FROM rust:latest as builder

RUN apt update && apt install -y build-essential gcc-x86-64-linux-gnu clang llvm

# Install mold
ENV MOLD_VERSION=1.11.0
RUN set -eux; \
    curl --fail --location "https://github.com/rui314/mold/releases/download/v${MOLD_VERSION}/mold-${MOLD_VERSION}-x86_64-linux.tar.gz" --output /tmp/mold.tar.gz; \
    tar --directory "/usr/local" -xzvf "/tmp/mold.tar.gz" --strip-components 1; \
    rm /tmp/mold.tar.gz; \
    mold --version;

RUN set -eux; \
    curl --fail --location "https://github.com/koute/not-perf/releases/download/0.1.1/not-perf-x86_64-unknown-linux-gnu.tgz" --output /tmp/nperf.tar.gz; \
    tar --directory "/usr/local/bin" -xzvf "/tmp/nperf.tar.gz"; \
    rm /tmp/nperf.tar.gz; \
    nperf --version;

WORKDIR /usr/src/app
COPY . .
# Will build and cache the binary and dependent crates in release mode
RUN --mount=type=cache,target=/usr/local/cargo,from=rust:latest,source=/usr/local/cargo \
    --mount=type=cache,target=target \
    cargo build --release && mv target/release/corrosion ./

# Runtime image
FROM debian:bookworm-slim

COPY --from=builder /usr/local/bin/nperf /usr/local/bin/nperf

# Run as "corrosion" user
RUN useradd -ms /bin/bash corrosion

COPY examples/fly/entrypoint.sh /entrypoint

USER corrosion
WORKDIR /app

COPY examples/fly/config.toml /app/config.toml
COPY examples/fly/schemas /app/schemas

# Get compiled binaries from builder's cargo install directory
COPY --from=builder /usr/src/app/corrosion /app/corrosion

ENTRYPOINT ["/entrypoint"]
# Run the app
CMD ["/app/corrosion", "agent", "-c", "/app/config.toml"]