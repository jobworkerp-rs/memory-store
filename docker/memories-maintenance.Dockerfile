FROM rust:1.91-bookworm AS build

RUN apt-get update && apt-get install -y --no-install-recommends \
        protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace
COPY . .
RUN cargo build --release -p grpc-admin --bin memories-maintenance

FROM debian:bookworm-slim

COPY --from=build /workspace/target/release/memories-maintenance /usr/local/bin/memories-maintenance
ENTRYPOINT ["/usr/local/bin/memories-maintenance"]
