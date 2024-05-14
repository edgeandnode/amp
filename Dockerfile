FROM rust:latest AS build
RUN apt-get update && apt-get install -y protobuf-compiler
COPY . .
RUN cargo build --release --workspace && \
    cp ./target/release/dump /bin/dump && \
    cp ./target/release/server /bin/server

FROM debian:bookworm-slim AS server
RUN apt-get update && apt-get install -y ca-certificates && apt-get clean
COPY --from=build /bin/server /bin/
CMD ["/bin/server"]

FROM debian:bookworm-slim AS dump
RUN apt-get update && apt-get install -y ca-certificates && apt-get clean
COPY --from=build /bin/dump /bin/
CMD ["/bin/dump"]

