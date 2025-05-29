FROM rust:1.87.0 AS build

# cmake is for snmalloc
RUN apt-get update && apt-get install -y protobuf-compiler cmake
COPY . .
RUN cargo build -p nozzle --release -v && \
    cp ./target/release/nozzle /bin/nozzle

# As a convenience, we also provide datafusion-cli
RUN cargo install datafusion-cli

FROM debian:bookworm-slim AS nozzle
RUN apt-get update && apt-get install -y ca-certificates && apt-get clean
COPY --from=build /bin/nozzle /bin/
COPY --from=build /usr/local/cargo/bin/datafusion-cli /bin/
CMD ["/bin/nozzle"]
