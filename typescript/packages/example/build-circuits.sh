#!/bin/bash

set -e

echo "Building circuits ..."

# Generate random entropy for ceremony
ENTROPY=$(openssl rand -hex 32)
echo "Using entropy $ENTROPY"

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

    # Export Solidity verifier contract
    echo "Exporting Solidity verifier for $name ..."
    pnpm run export:$name

    echo "$name circuit built successfully with verifier contract exported"
}

# Build circuits
if [ "$1" = "impact" ] || [ "$1" = "" ]; then
    build "impact"
fi

if [ "$1" = "board" ] || [ "$1" = "" ]; then
    build "board"
fi
