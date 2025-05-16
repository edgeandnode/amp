# Default target - displays available commands and their descriptions
default:
    @just --list

# Format all Rust code
#
# Requires nightly toolchain for unstable features.
# See `rustfmt.toml` for details.
fmt:
    cargo +nightly fmt --all

# Run all tests (unit and integration)
test:
    cargo test --workspace

# Run unit tests
test-unit:
    cargo test --workspace --exclude tests -- --nocapture

# Run integration tests
test-it:
    cargo test --package tests -- --nocapture

# Clean build artifacts and temporary files
clean:
    cargo clean

# Install Git hooks.
#
# See `.github/pre-commit-config.yaml` for the pre-commit configuration.
install-git-hooks:
    #!/usr/bin/env bash
    set -e # Exit on error

    # Check if pre-commit is installed
    if ! command -v "pre-commit" &> /dev/null; then
        >&2 echo "=============================================================="
        >&2 echo "Required command 'pre-commit' not available ❌"
        >&2 echo ""
        >&2 echo "Please install pre-commit using your preferred package manager"
        >&2 echo "  pip install pre-commit"
        >&2 echo "  pacman -S pre-commit"
        >&2 echo "  apt-get install pre-commit"
        >&2 echo "  brew install pre-commit"
        >&2 echo "=============================================================="
        exit 1
    fi

    # Install the pre-commit hooks
    pre-commit install --config .github/pre-commit-config.yaml

# Remove Git hooks
#
# See `.github/pre-commit-config.yaml` for the pre-commit configuration.
remove-git-hooks:
    #!/usr/bin/env bash
    set -e # Exit on error

    # Check if pre-commit is installed
    if ! command -v "pre-commit" &> /dev/null; then
        >&2 echo "=============================================================="
        >&2 echo "Required command 'pre-commit' not available ❌"
        >&2 echo ""
        >&2 echo "Please install pre-commit using your preferred package manager"
        >&2 echo "  pip install pre-commit"
        >&2 echo "  pacman -S pre-commit"
        >&2 echo "  apt-get install pre-commit"
        >&2 echo "  brew install pre-commit"
        >&2 echo "=============================================================="
        exit 1
    fi

    # Remove the pre-commit hooks
    pre-commit uninstall --config .github/pre-commit-config.yaml
