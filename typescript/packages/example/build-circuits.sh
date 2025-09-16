#!/bin/bash

set -e

echo "Building circuits with automated setup..."

# Generate random entropy for ceremony
ENTROPY=$(openssl rand -hex 32)
echo "Using entropy: $ENTROPY"

# Function to build a circuit
build() {
    local name=$1
    echo
    echo "Building $name circuit ..."

    # Compile circuit
    echo "Compiling $name ..."
    pnpm run compile:$name

    # Setup initial proving key
    echo "Setting up proving key for $name ..."
    pnpm run setup:$name

    # Contribute with automated entropy
    echo "Contributing to ceremony for $name ..."
    echo "$ENTROPY-$name" | pnpm run contribute:$name

    echo "$name circuit built successfully"
}

# Build circuits
if [ "$1" = "shot" ] || [ "$1" = "" ]; then
    build "shot"
fi

if [ "$1" = "board" ] || [ "$1" = "" ]; then
    build "board"
fi
