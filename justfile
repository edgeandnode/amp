# Display available commands and their descriptions (default target)
default:
    @just --list


## Workspace management

alias clean := cargo-clean

# Clean cargo workspace (cargo clean)
cargo-clean:
    cargo clean

# PNPM install (pnpm install)
[working-directory: 'typescript']
pnpm-install:
    pnpm install


## Code formatting and linting

alias fmt := fmt-rs
alias fmt-check := fmt-rs-check

# Format specific files (Rust or TypeScript based on extension)
fmt-file FILE:
    #!/usr/bin/env bash
    case "{{FILE}}" in
        *.rs) just fmt-rs-file "{{FILE}}" ;;
        *.ts|*.tsx) just fmt-ts-file "{{FILE}}" ;;
    esac

# Format Rust code (cargo fmt --all)
fmt-rs:
    cargo +nightly fmt --all

# Check Rust code format (cargo fmt --check)
fmt-rs-check:
    cargo +nightly fmt --all -- --check

# Format specific Rust file (cargo fmt <file>)
fmt-rs-file FILE:
    cargo +nightly fmt -- {{FILE}}

# Format TypeScript code (pnpm format)
[working-directory: 'typescript']
fmt-ts:
    pnpm format

# Check TypeScript code format (pnpm lint)
[working-directory: 'typescript']
fmt-ts-check:
    pnpm lint

# Format specific TypeScript file (pnpm lint --fix <file>)
[working-directory: 'typescript']
fmt-ts-file FILE:
    pnpm lint --fix {{replace_regex(FILE, '^(.*/)?typescript/', '')}}


## Check

alias check := check-rs

# Check Rust and TypeScript code
check-all: check-rs check-ts

# Check Rust code (cargo check --all-targets)
check-rs *EXTRA_FLAGS:
    cargo check --all-targets {{EXTRA_FLAGS}}

# Check specific crate with tests (cargo check -p <crate> --all-targets)
check-crate CRATE *EXTRA_FLAGS:
    cargo check --package {{CRATE}} --all-targets {{EXTRA_FLAGS}}

# Lint Rust code (cargo clippy --all-targets)
clippy *EXTRA_FLAGS:
    cargo clippy --all-targets {{EXTRA_FLAGS}}

# Lint specific crate (cargo clippy -p <crate> --all-targets)
clippy-crate CRATE *EXTRA_FLAGS:
    cargo clippy --package {{CRATE}} --all-targets {{EXTRA_FLAGS}}

# Check typescript code (pnpm check)
[working-directory: 'typescript']
check-ts:
    pnpm check


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


## Codegen

alias codegen := gen

# Run all codegen tasks
gen: gen-substreams-datasets-proto gen-firehose-datasets-proto

# Generate Substreams protobuf bindings (cargo build ... --features=gen-proto)
gen-substreams-datasets-proto:
    cargo build -p substreams-datasets --features=gen-proto

# Generate Firehose protobuf bindings (cargo build ... --features=gen-proto)
gen-firehose-datasets-proto:
    cargo build -p firehose-datasets --features=gen-proto


## Misc

PRECOMMIT_CONFIG := ".github/pre-commit-config.yaml"
PRECOMMIT_DEFAULT_HOOKS := "pre-commit pre-push"

# Install Git hooks
install-git-hooks HOOKS=PRECOMMIT_DEFAULT_HOOKS:
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

    # Install all Git hooks (see PRECOMMIT_HOOKS for default hooks)
    pre-commit install --config {{PRECOMMIT_CONFIG}} {{replace_regex(HOOKS, "\\s*([a-z-]+)\\s*", "--hook-type $1 ")}}

# Remove Git hooks
remove-git-hooks HOOKS=PRECOMMIT_DEFAULT_HOOKS:
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

    # Remove all Git hooks (see PRECOMMIT_HOOKS for default hooks)
    pre-commit uninstall --config {{PRECOMMIT_CONFIG}} {{replace_regex(HOOKS, "\\s*([a-z-]+)\\s*", "--hook-type $1 ")}}
