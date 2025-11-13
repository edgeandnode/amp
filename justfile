# Display available commands and their descriptions (default target)
default:
    @just --list


## Workspace management

alias clean := cargo-clean

# Clean cargo workspace (cargo clean)
[group: 'workspace']
cargo-clean:
    cargo clean

# PNPM install (pnpm install)
[group: 'workspace']
pnpm-install:
    pnpm install


## Code formatting and linting

alias fmt := fmt-rs
alias fmt-check := fmt-rs-check

# Format specific files (Rust or TypeScript based on extension)
[group: 'format']
fmt-file FILE:
    #!/usr/bin/env bash
    case "{{FILE}}" in
        *.rs) just fmt-rs-file "{{FILE}}" ;;
        *.ts|*.tsx) just fmt-ts-file "{{FILE}}" ;;
    esac

# Format Rust code (cargo fmt --all)
[group: 'format']
fmt-rs:
    cargo +nightly fmt --all

# Check Rust code format (cargo fmt --check)
[group: 'format']
fmt-rs-check:
    cargo +nightly fmt --all -- --check

# Format specific Rust file (cargo fmt <file>)
[group: 'format']
fmt-rs-file FILE:
    cargo +nightly fmt -- {{FILE}}

# Format TypeScript code (pnpm format)
[group: 'format']
fmt-ts:
    pnpm format

# Check TypeScript code format (pnpm lint)
[group: 'format']
fmt-ts-check:
    pnpm lint

# Format specific TypeScript file (pnpm lint --fix <file>)
[group: 'format']
fmt-ts-file FILE:
    #!/usr/bin/env bash
    file="{{FILE}}"; [[ "$file" =~ ^(/|typescript/) ]] || file="typescript/$file"; pnpm lint --fix "$file"


## Check

alias check := check-rs

# Check Rust and TypeScript code
[group: 'check']
check-all: check-rs check-ts

# Check Rust code (cargo check --all-targets)
[group: 'check']
check-rs *EXTRA_FLAGS:
    cargo check --all-targets {{EXTRA_FLAGS}}

# Check specific crate with tests (cargo check -p <crate> --all-targets)
[group: 'check']
check-crate CRATE *EXTRA_FLAGS:
    cargo check --package {{CRATE}} --all-targets {{EXTRA_FLAGS}}

# Lint Rust code (cargo clippy --all-targets)
[group: 'check']
clippy *EXTRA_FLAGS:
    cargo clippy --all-targets {{EXTRA_FLAGS}}

# Lint specific crate (cargo clippy -p <crate> --all-targets --no-deps)
[group: 'check']
clippy-crate CRATE *EXTRA_FLAGS:
    cargo clippy --package {{CRATE}} --all-targets --no-deps {{EXTRA_FLAGS}}

# Check typescript code (pnpm check)
[group: 'check']
check-ts:
    pnpm check


## Testing

# Run all tests (unit and integration)
[group: 'test']
test *EXTRA_FLAGS:
    #!/usr/bin/env bash
    set -e # Exit on error

    if command -v "cargo-nextest" &> /dev/null; then
        cargo nextest run {{EXTRA_FLAGS}} --workspace --all-features
    else
        >&2 echo "================================================================="
        >&2 echo "WARNING: cargo-nextest not found - using 'cargo test' fallback ⚠️"
        >&2 echo ""
        >&2 echo "For faster test execution, consider installing cargo-nextest:"
        >&2 echo "  cargo install --locked cargo-nextest@^0.9"
        >&2 echo "================================================================="
        sleep 1 # Give the user a moment to read the warning
        cargo test {{EXTRA_FLAGS}} --workspace --all-features
    fi

# Run unit tests
[group: 'test']
test-unit *EXTRA_FLAGS:
    #!/usr/bin/env bash
    set -e # Exit on error

    if command -v "cargo-nextest" &> /dev/null; then
        cargo nextest run {{EXTRA_FLAGS}} --workspace --exclude tests --exclude ampup --all-features
    else
        >&2 echo "================================================================="
        >&2 echo "WARNING: cargo-nextest not found - using 'cargo test' fallback ⚠️"
        >&2 echo ""
        >&2 echo "For faster test execution, consider installing cargo-nextest:"
        >&2 echo "  cargo install --locked cargo-nextest@^0.9"
        >&2 echo "================================================================="
        sleep 1 # Give the user a moment to read the warning
        cargo test {{EXTRA_FLAGS}} --workspace --exclude tests --exclude ampup --all-features -- --nocapture
    fi

# Run integration tests
[group: 'test']
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
[group: 'test']
test-local *EXTRA_FLAGS:
    #!/usr/bin/env bash
    set -e # Exit on error

    if command -v "cargo-nextest" &> /dev/null; then
        cargo nextest run --profile local {{EXTRA_FLAGS}} --workspace --all-features
    else
        >&2 echo "================================================="
        >&2 echo "ERROR: This command requires 'cargo-nextest' ❌"
        >&2 echo ""
        >&2 echo "Please install cargo-nextest to use this command:"
        >&2 echo "  cargo install --locked cargo-nextest@^0.9"
        >&2 echo "================================================="
        exit 1
    fi

# Run ampup tests
[group: 'test']
test-ampup *EXTRA_FLAGS:
    #!/usr/bin/env bash
    set -e # Exit on error

    if command -v "cargo-nextest" &> /dev/null; then
        cargo nextest run {{EXTRA_FLAGS}} --package ampup
    else
        >&2 echo "================================================================="
        >&2 echo "WARNING: cargo-nextest not found - using 'cargo test' fallback ⚠️"
        >&2 echo ""
        >&2 echo "For faster test execution, consider installing cargo-nextest:"
        >&2 echo "  cargo install --locked cargo-nextest@^0.9"
        >&2 echo "================================================================="
        sleep 1 # Give the user a moment to read the warning
        cargo test {{EXTRA_FLAGS}} --package ampup --test it_ampup -- --nocapture
    fi


## Codegen

alias codegen := gen

GEN_MANIFEST_SCHEMAS_OUTDIR := "docs/manifest-schemas"
GEN_OPENAPI_SCHEMAS_OUTDIR := "docs/openapi-specs"
GEN_TABLE_SCHEMAS_OUTDIR := "docs/schemas"

# Run all codegen tasks
[group: 'codegen']
gen:
    RUSTFLAGS="--cfg gen_schema_manifest --cfg gen_schema_tables --cfg gen_openapi_spec" cargo check --workspace
    @mkdir -p {{GEN_MANIFEST_SCHEMAS_OUTDIR}} {{GEN_OPENAPI_SCHEMAS_OUTDIR}} {{GEN_TABLE_SCHEMAS_OUTDIR}}
    @cp -f $(ls -t target/debug/build/datasets-derived-gen-*/out/schema.json | head -1) {{GEN_MANIFEST_SCHEMAS_OUTDIR}}/derived.spec.json
    @cp -f $(ls -t target/debug/build/datasets-eth-beacon-gen-*/out/schema.json | head -1) {{GEN_MANIFEST_SCHEMAS_OUTDIR}}/eth-beacon.spec.json
    @cp -f $(ls -t target/debug/build/datasets-evm-rpc-gen-*/out/schema.json | head -1) {{GEN_MANIFEST_SCHEMAS_OUTDIR}}/evm-rpc.spec.json
    @cp -f $(ls -t target/debug/build/datasets-firehose-gen-*/out/schema.json | head -1) {{GEN_MANIFEST_SCHEMAS_OUTDIR}}/firehose.spec.json
    @cp -f $(ls -t target/debug/build/datasets-eth-beacon-gen-*/out/tables.md | head -1) {{GEN_TABLE_SCHEMAS_OUTDIR}}/eth-beacon.md
    @cp -f $(ls -t target/debug/build/datasets-evm-rpc-gen-*/out/tables.md | head -1) {{GEN_TABLE_SCHEMAS_OUTDIR}}/evm-rpc.md
    @cp -f $(ls -t target/debug/build/datasets-firehose-gen-*/out/tables.md | head -1) {{GEN_TABLE_SCHEMAS_OUTDIR}}/firehose-evm.md
    @cp -f $(ls -t target/debug/build/admin-api-gen-*/out/openapi.spec.json | head -1) {{GEN_OPENAPI_SCHEMAS_OUTDIR}}/admin.spec.json
    @echo "Schemas generated and copied:"
    @echo "  {{GEN_MANIFEST_SCHEMAS_OUTDIR}}/derived.spec.json"
    @echo "  {{GEN_MANIFEST_SCHEMAS_OUTDIR}}/eth-beacon.spec.json"
    @echo "  {{GEN_MANIFEST_SCHEMAS_OUTDIR}}/evm-rpc.spec.json"
    @echo "  {{GEN_MANIFEST_SCHEMAS_OUTDIR}}/firehose.spec.json"
    @echo "  {{GEN_TABLE_SCHEMAS_OUTDIR}}/eth-beacon.md"
    @echo "  {{GEN_TABLE_SCHEMAS_OUTDIR}}/evm-rpc.md"
    @echo "  {{GEN_TABLE_SCHEMAS_OUTDIR}}/firehose-evm.md"
    @echo "  {{GEN_OPENAPI_SCHEMAS_OUTDIR}}/admin.spec.json"

### JSON Schema generation

# Generate the common derived dataset manifest JSON schema (RUSTFLAGS="--cfg gen_schema_manifest" cargo build)
[group: 'codegen']
gen-derived-dataset-manifest-schema DEST_DIR=GEN_MANIFEST_SCHEMAS_OUTDIR:
    RUSTFLAGS="--cfg gen_schema_manifest" cargo check -p datasets-derived-gen
    @mkdir -p {{DEST_DIR}}
    @cp -f $(ls -t target/debug/build/datasets-derived-gen-*/out/schema.json | head -1) {{DEST_DIR}}/derived.spec.json
    @echo "Schema generated and copied to {{DEST_DIR}}/derived.spec.json"

# Generate the ETH Beacon dataset definition JSON schema (RUSTFLAGS="--cfg gen_schema_manifest" cargo build)
[group: 'codegen']
gen-eth-beacon-dataset-manifest-schema DEST_DIR=GEN_MANIFEST_SCHEMAS_OUTDIR:
    RUSTFLAGS="--cfg gen_schema_manifest" cargo check -p datasets-eth-beacon-gen
    @mkdir -p {{DEST_DIR}}
    @cp -f $(ls -t target/debug/build/datasets-eth-beacon-gen-*/out/schema.json | head -1) {{DEST_DIR}}/eth-beacon.spec.json
    @echo "Schema generated and copied to {{DEST_DIR}}/eth-beacon.spec.json"

# Generate the EVM RPC dataset definition JSON schema (RUSTFLAGS="--cfg gen_schema_manifest" cargo build)
[group: 'codegen']
gen-evm-rpc-dataset-manifest-schema DEST_DIR=GEN_MANIFEST_SCHEMAS_OUTDIR:
    RUSTFLAGS="--cfg gen_schema_manifest" cargo check -p datasets-evm-rpc-gen
    @mkdir -p {{DEST_DIR}}
    @cp -f $(ls -t target/debug/build/datasets-evm-rpc-gen-*/out/schema.json | head -1) {{DEST_DIR}}/evm-rpc.spec.json
    @echo "Schema generated and copied to {{DEST_DIR}}/evm-rpc.spec.json"

# Generate the Firehose dataset definition JSON schema (RUSTFLAGS="--cfg gen_schema_manifest" cargo build)
[group: 'codegen']
gen-firehose-dataset-manifest-schema DEST_DIR=GEN_MANIFEST_SCHEMAS_OUTDIR:
    RUSTFLAGS="--cfg gen_schema_manifest" cargo check -p datasets-firehose-gen
    @mkdir -p {{DEST_DIR}}
    @cp -f $(ls -t target/debug/build/datasets-firehose-gen-*/out/schema.json | head -1) {{DEST_DIR}}/firehose.spec.json
    @echo "Schema generated and copied to {{DEST_DIR}}/firehose.spec.json"

### Table Schema markdown generation

# Generate the ETH Beacon table schema markdown (RUSTFLAGS="--cfg gen_schema_tables" cargo build)
[group: 'codegen']
gen-eth-beacon-tables-schema DEST_DIR=GEN_TABLE_SCHEMAS_OUTDIR:
    RUSTFLAGS="--cfg gen_schema_tables" cargo check -p datasets-eth-beacon-gen
    @mkdir -p {{DEST_DIR}}
    @cp -f $(ls -t target/debug/build/datasets-eth-beacon-gen-*/out/tables.md | head -1) {{DEST_DIR}}/eth-beacon.md
    @echo "Table schema markdown generated and copied to {{DEST_DIR}}/eth-beacon.md"

# Generate the EVM RPC table schema markdown (RUSTFLAGS="--cfg gen_schema_tables" cargo build)
[group: 'codegen']
gen-evm-rpc-tables-schema DEST_DIR=GEN_TABLE_SCHEMAS_OUTDIR:
    RUSTFLAGS="--cfg gen_schema_tables" cargo check -p datasets-evm-rpc-gen
    @mkdir -p {{DEST_DIR}}
    @cp -f $(ls -t target/debug/build/datasets-evm-rpc-gen-*/out/tables.md | head -1) {{DEST_DIR}}/evm-rpc.md
    @echo "Table schema markdown generated and copied to {{DEST_DIR}}/evm-rpc.md"

# Generate the Firehose table schema markdown (RUSTFLAGS="--cfg gen_schema_tables" cargo build)
[group: 'codegen']
gen-firehose-tables-schema DEST_DIR=GEN_TABLE_SCHEMAS_OUTDIR:
    RUSTFLAGS="--cfg gen_schema_tables" cargo check -p datasets-firehose-gen
    @mkdir -p {{DEST_DIR}}
    @cp -f $(ls -t target/debug/build/datasets-firehose-gen-*/out/tables.md | head -1) {{DEST_DIR}}/firehose-evm.md
    @echo "Table schema markdown generated and copied to {{DEST_DIR}}/firehose-evm.md"

### OpenAPI specification generation

# Generate the admin API OpenAPI specification
[group: 'codegen']
gen-admin-api-openapi-spec DEST_DIR=GEN_OPENAPI_SCHEMAS_OUTDIR:
    RUSTFLAGS="--cfg gen_openapi_spec" cargo check -p admin-api-gen
    @mkdir -p {{DEST_DIR}}
    @cp -f $(ls -t target/debug/build/admin-api-gen-*/out/openapi.spec.json | head -1) {{DEST_DIR}}/admin.spec.json
    @echo "Schema generated and copied to {{DEST_DIR}}/admin.spec.json"

### Protobuf bindings generation

# Generate Firehose protobuf bindings (RUSTFLAGS="--cfg gen_proto" cargo build)
[group: 'codegen']
gen-firehose-datasets-proto:
    RUSTFLAGS="--cfg gen_proto" cargo check -p firehose-datasets


## Misc

PRECOMMIT_CONFIG := ".github/pre-commit-config.yaml"
PRECOMMIT_DEFAULT_HOOKS := "pre-commit pre-push"

# Install Git hooks
[group: 'misc']
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
[group: 'misc']
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

