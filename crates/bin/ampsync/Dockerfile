# Build stage
FROM rustlang/rust:nightly-slim AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    build-essential \
    curl \
    python3 \
    cmake \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy workspace files
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY tests ./tests

# Build the ampsync binary
RUN cargo build --release -p ampsync

# Runtime stage - minimal like nozzle
FROM debian:trixie-slim

# Install runtime dependencies
RUN apt-get update && \
    apt-get install -y ca-certificates && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy binary from builder
COPY --from=builder /app/target/release/ampsync /ampsync
RUN chmod +x /ampsync

ENTRYPOINT ["/ampsync"]
