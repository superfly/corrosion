# build image
FROM rust:bookworm AS builder

RUN apt update && apt install -y build-essential gcc-x86-64-linux-gnu clang llvm unzip

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

ENV CONSUL_VERSION="1.14.3"

RUN set -eux; \
    curl --silent --remote-name https://releases.hashicorp.com/consul/${CONSUL_VERSION}/consul_${CONSUL_VERSION}_linux_amd64.zip; \
    unzip consul_${CONSUL_VERSION}_linux_amd64.zip; \
    mv consul /usr/local/bin/consul; \
    rm consul_${CONSUL_VERSION}_linux_amd64.zip; \
    consul --version;

WORKDIR /usr/src/app
COPY . .

COPY .antithesis/libvoidstar.so /usr/lib/antithesis/lib/libvoidstar.so
ENV LD_LIBRARY_PATH=/usr/lib/antithesis/lib

# Will build and cache the binary and dependent crates in release mode
RUN --mount=type=cache,target=/usr/local/cargo,from=rust:bookworm,source=/usr/local/cargo \
    --mount=type=cache,target=target \
    cargo build --release && mv target/release/corrosion ./ && mv target/release/rust-load-generator ./

# Runtime image
FROM debian:bookworm-slim

RUN apt update && apt install -y sqlite3 sqlite3-tools watch socat iproute2 gettext-base ca-certificates supervisor && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/bin/nperf /usr/local/bin/consul /usr/src/app/corrosion /usr/src/app/rust-load-generator /usr/local/bin/

# Create "corrosion" user
RUN useradd -ms /bin/bash corrosion

ENV LD_LIBRARY_PATH=/usr/lib/antithesis/lib
COPY .antithesis/libvoidstar.so /usr/lib/antithesis/lib/libvoidstar.so

RUN mkdir -p /symbols
COPY --from=builder /usr/src/app/corrosion /symbols/corrosion
COPY --from=builder /usr/src/app/rust-load-generator /symbols/rust-load-generator

COPY .antithesis/corrosion/entrypoint.sh /entrypoint.sh
COPY .antithesis/corrosion/config.toml.tpl /etc/corrosion/config.toml.tpl
COPY .antithesis/corrosion/schemas /etc/corrosion/schemas
COPY .antithesis/corrosion/templates /etc/corrosion/templates

COPY .antithesis/corrosion/test-templates/* /opt/antithesis/test/v1/basic_test/

# Create supervisor configuration directory
RUN mkdir -p /etc/supervisor/conf.d
COPY .antithesis/corrosion/supervisord.conf /etc/supervisor/conf.d/supervisord.conf

ENTRYPOINT ["/entrypoint.sh"]
# Run the app
# CMD ["corrosion", "agent"]
CMD ["/usr/bin/supervisord", "-n", "-c", "/etc/supervisor/conf.d/supervisord.conf"]
