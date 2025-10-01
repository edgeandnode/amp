# Build stage
FROM rustlang/rust:nightly-slim AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    build-essential \
    git \
    curl \
    cmake \
    && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /app

# Copy workspace files
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY tests ./tests

# Build the ampsync binary with release optimizations
RUN cargo build --release -p ampsync

# Runtime stage - use minimal base image
FROM debian:bookworm-slim

# Install runtime dependencies only
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user for security
RUN useradd -m -u 1001 ampsync

# Copy binary from builder
COPY --from=builder /app/target/release/ampsync /usr/local/bin/ampsync

# Set ownership
RUN chown ampsync:ampsync /usr/local/bin/ampsync

# Switch to non-root user
USER ampsync

# Set working directory
WORKDIR /home/ampsync

# Default command
ENTRYPOINT ["/usr/local/bin/ampsync"]