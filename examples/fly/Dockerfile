# build image
FROM rust:bookworm as builder

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
RUN --mount=type=cache,target=/usr/local/cargo,from=rust:bookworm,source=/usr/local/cargo \
    --mount=type=cache,target=target \
    cargo build --release && mv target/release/corrosion ./

# Runtime image
FROM debian:bookworm-slim

RUN apt update && apt install -y sqlite3 watch ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/bin/nperf /usr/src/app/corrosion /usr/local/bin/

# Create "corrosion" user
RUN useradd -ms /bin/bash corrosion

COPY examples/fly/entrypoint.sh /entrypoint.sh

# COPY examples/fly/corrosion-files/* /etc/corrosion/

COPY examples/fly/config.toml /etc/corrosion/config.toml
COPY examples/fly/schemas /etc/corrosion/schemas
COPY examples/fly/templates /etc/corrosion/templates

ENTRYPOINT ["/entrypoint.sh"]
# Run the app
CMD ["corrosion", "agent"]
