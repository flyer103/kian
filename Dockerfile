# syntax=docker/dockerfile:1

# Kian is one process plus one sqlite file. This image builds the two binaries
# and nothing else: no hosted service, no external database.
#
#   docker build -t kian .
#   docker run --rm -p 7433:7433 \
#     -v "$PWD/kian.yaml:/etc/kian/kian.yaml:ro" \
#     -v kian-data:/data kian
#
# The config must listen on 0.0.0.0 inside the container (the quickstart's
# 127.0.0.1 is the host-loopback default, not a container bind).

FROM rust:1-bookworm AS build
WORKDIR /src
ENV CARGO_INCREMENTAL=0
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN cargo build --release --locked -p kian-cli -p kiand

FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/kian /usr/local/bin/kian
COPY --from=build /src/target/release/kiand /usr/local/bin/kiand

# Everything durable lives in this one directory: kian.db (+ WAL) and, if you
# turn them on, the archive and worktree roots. Mount it.
WORKDIR /data
ENV RUST_LOG=info
EXPOSE 7433
ENTRYPOINT ["kian"]
CMD ["serve", "--config", "/etc/kian/kian.yaml"]
