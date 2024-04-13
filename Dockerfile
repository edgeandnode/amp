FROM rust:latest AS build
RUN apt-get update && apt-get install -y protobuf-compiler
COPY . .
RUN cargo build --release -p dump && \
    cp ./target/release/dump /bin/dump

FROM debian:bookworm-slim AS final
RUN apt-get update && apt-get install -y ca-certificates && apt-get clean
COPY --from=build /bin/dump /bin/
CMD ["/bin/dump"]
