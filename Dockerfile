FROM rust:latest AS build

# cmake is for snmalloc
RUN apt-get update && apt-get install -y protobuf-compiler cmake
COPY . .
RUN cargo build -p nozzle --release -v && \
    cp ./target/release/nozzle /bin/nozzle

FROM debian:bookworm-slim AS nozzle
RUN apt-get update && apt-get install -y ca-certificates && apt-get clean
COPY --from=build /bin/nozzle /bin/
CMD ["/bin/nozzle"]
