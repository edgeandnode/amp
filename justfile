# Display available commands and their descriptions (default target)
default:
    @just --list


## Workspace management

# Clean workspace (cargo clean)
clean:
    cargo clean


## Code formatting
# NOTE: The formatting commands below use the RUSTFMT environment variable to
#  explicitly specify rustup's nightly rustfmt binary. This ensures nightly
#  formatting features work correctly in both standard and Nix environments,
#  preventing "unstable features only available in nightly" warnings that occur
#  when Nix's stable rustfmt takes PATH precedenShogce over rustup's nightly version.

# Format Rust code (cargo fmt)
fmt:
    RUSTFMT="$(rustup which --toolchain nightly rustfmt)" cargo fmt --all

# Check Rust code format (cargo fmt --check)
fmt-check:
    RUSTFMT="$(rustup which --toolchain nightly rustfmt)" cargo fmt --all -- --check


## Linting

# Check Rust code (cargo check)
check *EXTRA_FLAGS:
    cargo check {{EXTRA_FLAGS}}

# Check all Rust code (cargo check --tests)
check-all *EXTRA_FLAGS:
    cargo check --tests {{EXTRA_FLAGS}}

# Lint Rust code (cargo clippy)
clippy *EXTRA_FLAGS:
    cargo clippy {{EXTRA_FLAGS}}

# Lint all Rust code (cargo clippy --tests)
clippy-all *EXTRA_FLAGS:
    cargo clippy --tests {{EXTRA_FLAGS}}


## Testing

# Run all tests (unit and integration)
test *EXTRA_FLAGS:
    #!/usr/bin/env bash
    set -e # Exit on error

    if command -v "cargo-nextest" &> /dev/null; then
        cargo nextest run {{EXTRA_FLAGS}} --workspace
    else
        >&2 echo "================================================================="
        >&2 echo "WARNING: cargo-nextest not found - using 'cargo test' fallback ⚠️"
        >&2 echo ""
        >&2 echo "For faster test execution, consider installing cargo-nextest:"
        >&2 echo "  cargo install --locked cargo-nextest@^0.9"
        >&2 echo "================================================================="
        sleep 1 # Give the user a moment to read the warning
        cargo test {{EXTRA_FLAGS}} --workspace
    fi

# Run unit tests
test-unit *EXTRA_FLAGS:
    #!/usr/bin/env bash
    set -e # Exit on error

    if command -v "cargo-nextest" &> /dev/null; then
        cargo nextest run {{EXTRA_FLAGS}} --workspace --exclude tests 
    else
        >&2 echo "================================================================="
        >&2 echo "WARNING: cargo-nextest not found - using 'cargo test' fallback ⚠️"
        >&2 echo ""
        >&2 echo "For faster test execution, consider installing cargo-nextest:"
        >&2 echo "  cargo install --locked cargo-nextest@^0.9"
        >&2 echo "================================================================="
        sleep 1 # Give the user a moment to read the warning
        cargo test {{EXTRA_FLAGS}} --workspace --exclude tests -- --nocapture
    fi

# Run integration tests
test-it *EXTRA_FLAGS:
    #!/usr/bin/env bash
    set -e # Exit on error

    if command -v "cargo-nextest" &> /dev/null; then
        cargo nextest run {{EXTRA_FLAGS}} --package tests
    else
        >&2 echo "================================================================="
        >&2 echo "WARNING: cargo-nextest not found - using 'cargo test' fallback ⚠️"
        >&2 echo ""
        >&2 echo "For faster test execution, consider installing cargo-nextest:"
        >&2 echo "  cargo install --locked cargo-nextest@^0.9"
        >&2 echo "================================================================="
        sleep 1 # Give the user a moment to read the warning
        cargo test {{EXTRA_FLAGS}} --package tests -- --nocapture
    fi

# Run only tests without external dependencies
test-local *EXTRA_FLAGS:
    #!/usr/bin/env bash
    set -e # Exit on error

    if command -v "cargo-nextest" &> /dev/null; then
        cargo nextest run --profile local {{EXTRA_FLAGS}} --workspace
    else
        >&2 echo "================================================="
        >&2 echo "ERROR: This command requires 'cargo-nextest' ❌"
        >&2 echo ""
        >&2 echo "Please install cargo-nextest to use this command:"
        >&2 echo "  cargo install --locked cargo-nextest@^0.9"
        >&2 echo "================================================="
        exit 1
    fi


## Misc

# Install Git hooks
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
