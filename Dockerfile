#===============================================================================
# BUILD STAGE
#===============================================================================

FROM rust:1.89.0 AS build
# Install cmake (snmalloc) and protoc and datafusion-cli (convenience) and sqlx-cli (migration management).
RUN apt-get update && \
    apt-get install -y protobuf-compiler cmake && \
    cargo install sqlx-cli && \
    cargo install datafusion-cli
# Copy the workspace. See `.dockerignore` for the ignore list.
COPY . .
# Build the nozzle binary.
RUN cargo build -p nozzle --release

#===============================================================================
# RUNTIME STAGE
#===============================================================================

FROM debian:trixie-slim AS nozzle
# Install runtime dependencies.
RUN apt-get update && \
    apt-get install -y ca-certificates && \
    apt-get clean
# Copy the binaries to the runtime image.
COPY --from=build ./target/release/nozzle /usr/local/cargo/bin/datafusion-cli /usr/local/cargo/bin/sqlx /bin/
ENTRYPOINT ["nozzle"]
