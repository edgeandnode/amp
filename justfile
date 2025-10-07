# Display available commands and their descriptions (default target)
default:
    @just --list

# Support both docker and podman
docker := if `command -v podman >/dev/null 2>&1; echo $?` == "0" { "podman" } else { "docker" }

## Workspace management

alias clean := cargo-clean

# Clean cargo workspace (cargo clean)
cargo-clean:
    cargo clean

# PNPM install (pnpm install)
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
fmt-ts:
    pnpm format

# Check TypeScript code format (pnpm lint)
fmt-ts-check:
    pnpm lint

# Format specific TypeScript file (pnpm lint --fix <file>)
fmt-ts-file FILE:
    #!/usr/bin/env bash
    file="{{FILE}}"; [[ "$file" =~ ^(/|typescript/) ]] || file="typescript/$file"; pnpm lint --fix "$file"


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
gen: \
    gen-common-dataset-manifest-schema \
    gen-derived-dataset-manifest-schema \
    gen-evm-rpc-dataset-manifest-schema \
    gen-firehose-dataset-manifest-schema \
    gen-substreams-dataset-manifest-schema \
    gen-admin-api-openapi-spec
# TODO: Uncomment to enable protobuf bindings generation
#    gen-substreams-datasets-proto \
#    gen-firehose-datasets-proto \

### JSON Schema generation

SCHEMAS_DIR := "docs/dataset-def-schemas"

# Generate the common dataset manifest JSON schema (RUSTFLAGS="--cfg gen_schema" cargo build)
gen-common-dataset-manifest-schema DEST_DIR=SCHEMAS_DIR:
    RUSTFLAGS="--cfg gen_schema" cargo build -p datasets-common-gen
    @mkdir -p {{DEST_DIR}}
    @cp -f $(ls -t target/debug/build/datasets-common-gen-*/out/schema.json | head -1) {{DEST_DIR}}/common.spec.json
    @echo "Schema generated and copied to {{DEST_DIR}}/common.spec.json"

# Generate the common derived dataset manifest JSON schema (RUSTFLAGS="--cfg gen_schema" cargo build)
gen-derived-dataset-manifest-schema DEST_DIR=SCHEMAS_DIR:
    RUSTFLAGS="--cfg gen_schema" cargo build -p datasets-derived-gen
    @mkdir -p {{DEST_DIR}}
    @cp -f $(ls -t target/debug/build/datasets-derived-gen-*/out/schema.json | head -1) {{DEST_DIR}}/derived.spec.json
    @echo "Schema generated and copied to {{DEST_DIR}}/derived.spec.json"

# Generate the EVM RPC dataset definition JSON schema (RUSTFLAGS="--cfg gen_schema" cargo build)
gen-evm-rpc-dataset-manifest-schema DEST_DIR=SCHEMAS_DIR:
    RUSTFLAGS="--cfg gen_schema" cargo build -p datasets-evm-rpc-gen
    @mkdir -p {{DEST_DIR}}
    @cp -f $(ls -t target/debug/build/datasets-evm-rpc-gen-*/out/schema.json | head -1) {{DEST_DIR}}/evm-rpc.spec.json
    @echo "Schema generated and copied to {{DEST_DIR}}/evm-rpc.spec.json"

# Generate the Firehose dataset definition JSON schema (RUSTFLAGS="--cfg gen_schema" cargo build)
gen-firehose-dataset-manifest-schema DEST_DIR=SCHEMAS_DIR:
    RUSTFLAGS="--cfg gen_schema" cargo build -p datasets-firehose-gen
    @mkdir -p {{DEST_DIR}}
    @cp -f $(ls -t target/debug/build/datasets-firehose-gen-*/out/schema.json | head -1) {{DEST_DIR}}/firehose.spec.json
    @echo "Schema generated and copied to {{DEST_DIR}}/firehose.spec.json"

# Generate the Substreams dataset definition JSON schema (RUSTFLAGS="--cfg gen_schema" cargo build)
gen-substreams-dataset-manifest-schema DEST_DIR=SCHEMAS_DIR:
    RUSTFLAGS="--cfg gen_schema" cargo build -p datasets-substreams-gen
    @mkdir -p {{DEST_DIR}}
    @cp -f $(ls -t target/debug/build/datasets-substreams-gen-*/out/schema.json | head -1) {{DEST_DIR}}/substreams.spec.json
    @echo "Schema generated and copied to {{DEST_DIR}}/substreams.spec.json"

### OpenAPI specification generation

OPENAPI_SCHEMAS_DIR := "docs/openapi-specs"

# Generate the admin API OpenAPI specification
gen-admin-api-openapi-spec DEST_DIR=OPENAPI_SCHEMAS_DIR:
    RUSTFLAGS="--cfg gen_openapi_spec" cargo build -p admin-api-gen
    @mkdir -p {{DEST_DIR}}
    @cp -f $(ls -t target/debug/build/admin-api-gen-*/out/openapi.spec.json | head -1) {{DEST_DIR}}/admin.spec.json
    @echo "Schema generated and copied to {{DEST_DIR}}/admin.spec.json"

### Protobuf bindings generation

# Generate Substreams protobuf bindings (RUSTFLAGS="--cfg gen_proto" cargo build)
gen-substreams-datasets-proto:
    RUSTFLAGS="--cfg gen_proto" cargo build -p substreams-datasets

# Generate Firehose protobuf bindings (RUSTFLAGS="--cfg gen_proto" cargo build)
gen-firehose-datasets-proto:
    RUSTFLAGS="--cfg gen_proto" cargo build -p firehose-datasets


## Build and Containerize

# Build nozzle for current platform and create a Docker image
containerize TAG="nozzle:local":
    #!/usr/bin/env bash
    set -e # Exit on error

    # Build nozzle for release
    echo "Building nozzle binary..."
    cargo build --release -p nozzle

    # Create temporary directory
    TMPDIR=$(mktemp --directory)
    trap "rm -rf $TMPDIR" EXIT

    # Detect current architecture
    ARCH=$(uname -m)
    if [ "$ARCH" = "x86_64" ]; then
        DOCKER_ARCH="amd64"
    elif [ "$ARCH" = "arm64" ] || [ "$ARCH" = "aarch64" ]; then
        DOCKER_ARCH="arm64"
    else
        echo "Unsupported architecture: $ARCH"
        exit 1
    fi

    # Copy binary to tmpdir
    cp target/release/nozzle $TMPDIR/nozzle-linux-$DOCKER_ARCH
    chmod +x $TMPDIR/nozzle-linux-$DOCKER_ARCH

    # Create Dockerfile in tmpdir
    printf '%s\n' \
        'FROM debian:trixie-slim' \
        'ARG TARGETARCH' \
        '' \
        '# Install runtime dependencies' \
        'RUN apt-get update && \' \
        '    apt-get install -y ca-certificates && \' \
        '    apt-get clean && \' \
        '    rm -rf /var/lib/apt/lists/*' \
        '' \
        '# Copy the binary' \
        'COPY nozzle-linux-$TARGETARCH /nozzle' \
        'RUN chmod +x /nozzle' \
        'ENTRYPOINT ["/nozzle"]' \
        > $TMPDIR/Dockerfile

    # Build Docker image
    echo "Building Docker image..."
    cd $TMPDIR && {{docker}} build --build-arg TARGETARCH=$DOCKER_ARCH -t {{TAG}} .

    echo "Docker image built successfully: {{TAG}}"


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
