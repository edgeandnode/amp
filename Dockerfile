FROM rust:latest AS build

# Install host build dependencies.
RUN apt-get update && apt-get install -y protobuf-compiler

COPY . .

RUN cargo build --release -p dump && \
    cp ./target/release/dump /bin/dump


FROM rust:slim AS final

COPY --from=build /bin/dump /bin/

CMD ["/bin/dump"]
