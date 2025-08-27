# Display available commands and their descriptions (default target)
default:
    @just --list

# Format all Rust code (cargo fmt)
fmt:
    cargo fmt --all

# Check Rust code format (cargo fmt --check)
fmt-check:
    cargo fmt --all -- --check

# Check Rust code (cargo check)
check *EXTRA_FLAGS:
    cargo check {{EXTRA_FLAGS}}

# Run all tests (unit and integration)
test *EXTRA_FLAGS:
    #!/usr/bin/env bash
    set -e # Exit on error

    if command -v "cargo-nextest" &> /dev/null; then
        cargo nextest run {{EXTRA_FLAGS}} --workspace
    else
        cargo test {{EXTRA_FLAGS}} --workspace
    fi

# Run unit tests
test-unit *EXTRA_FLAGS:
    #!/usr/bin/env bash
    set -e # Exit on error

    if command -v "cargo-nextest" &> /dev/null; then
        cargo nextest run {{EXTRA_FLAGS}} --workspace --exclude tests 
    else
        cargo test {{EXTRA_FLAGS}} --workspace --exclude tests -- --nocapture
    fi

# Run integration tests
test-it *EXTRA_FLAGS:
    #!/usr/bin/env bash
    set -e # Exit on error

    if command -v "cargo-nextest" &> /dev/null; then
        cargo nextest run {{EXTRA_FLAGS}} --package tests
    else
        cargo test {{EXTRA_FLAGS}} --package tests -- --nocapture
    fi

# Run only tests without external dependencies
test-local *EXTRA_FLAGS:
    #!/usr/bin/env bash
    set -e # Exit on error

    if command -v "cargo-nextest" &> /dev/null; then
        cargo nextest run --profile local {{EXTRA_FLAGS}} --workspace
    else
        echo "This command requires cargo-nextest to filter tests"
        exit 1
    fi

# Clean workspace (cargo clean)
clean:
    cargo clean

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

# nozzle-registry commands for running gel database commands, such as migrations

## create a gel migration for any nozzle-registry schema changes
nozzle_registry__migration_create:
    docker compose exec nozzle_registry_db gel --tls-security=insecure --user=nozzle_registry_adm --password -P 5656 migration create
## runs any unapplied migrations
## note: the docker image will run all migrations on startup
nozzle_registry__migrate:
    docker compose exec nozzle_registry_db gel --tls-security=insecure --user=nozzle_registry_adm --password -P 5656 migration apply
## Renders a logs of the applied migrations on the gel instance
nozzle_registry__migrate_log:
    docker compose exec nozzle_registry_db gel --tls-security=insecure --user=nozzle_registry_adm --password -P 5656 migration log --from-db
## connects the nozzle-registry gel sql instance to the running docker compose gel instance
nozzle_registry__gel_cli:
    docker compose exec nozzle_registry_db gel --tls-security=insecure --user=nozzle_registry_adm --password -P 5656