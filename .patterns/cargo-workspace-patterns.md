📦 Cargo Workspace Patterns (Rust)
==================================

## 🎯 OVERVIEW

Comprehensive workspace management strategies for Rust multi-crate projects, with emphasis on proper dependency management, cross-crate interactions, and workspace-level operations in the Amp project.

## 🚨 CRITICAL WORKSPACE REQUIREMENTS
**🤖 AI AGENTS: These rules are NON-NEGOTIABLE. Follow them exactly.**

### Mandatory Workspace Operations
- **Workspace root execution**: ALL `cargo` commands MUST be issued from the root of the workspace (where the workspace `Cargo.toml` file lives)
- **Immediate formatting**: `just fmt-rs` for entire workspace or `just fmt-file <rust_file>.rs` for individual files
- **Compilation validation**: `just check-rs` and `just check-crate <crate-name>` MUST pass

### Mandatory Cargo.toml Ordering
- **Workspace members**: Root `Cargo.toml` `members` array MUST be ordered alphabetically
- **Dependencies**: All `Cargo.toml` dependencies sections MUST be ordered alphabetically
- **Rationale**: Ensures consistent merge conflict resolution and maintainable dependency management

## 📁 Workspace structure

The Amp workspace follows a hierarchical organization under the `crates/` directory with four main categories: `core`, `bin`, `extractors`, and `services`. Each category serves a specific architectural purpose and has distinct dependency patterns.

### 🏗️ Top-Level Crate Categories

#### 📦 `crates/core/` - Foundation Libraries
**Purpose**: Shared functionality and domain logic used across the entire workspace.

**What belongs here:**
- **`common`**: Shared utilities, error types, configuration, EVM functions
- **`metadata-db`**: PostgreSQL database operations and worker coordination  
- **`dataset-store`**: Dataset management, manifest parsing, SQL dataset support
- **`js-runtime`**: JavaScript UDF execution runtime
- **`dump`**: Core data extraction logic and parallel processing
- **`monitoring`**: Observability, metrics, and telemetry

**Dependency Rules:**
- ✅ Core crates can depend on other core crates
- ❌ Core crates NEVER depend on `bin`, `extractors`, or `services`
- ✅ Should be the most stable and well-tested crates

#### 🚀 `crates/bin/` - Executable Applications
**Purpose**: Main entry points and command-line applications.

**What belongs here:**
- **`ampd`**: Main CLI application with subcommands (`dump`, `server`, `worker`)
- **`generate-manifest`**: Utility for creating dataset manifest files

**Dependency Rules:**
- ✅ Can depend on `core`, `extractors`, and `services` crates
- ❌ Other categories should NOT depend on `bin` crates
- ✅ Entry points that orchestrate functionality from other layers

#### 🔌 `crates/extractors/` - Data Source Connectors
**Purpose**: Specialized connectors for different blockchain data sources.

**What belongs here:**
- **`evm-rpc`**: Ethereum JSON-RPC data extraction (blocks, transactions, logs)
- **`firehose`**: StreamingFast Firehose protocol implementation  
- **`substreams`**: Substreams package processing and entity extraction

**Dependency Rules:**
- ✅ Can depend on `core` crates for shared functionality
- ❌ Should NOT depend on other `extractors` (maintain independence)
- ❌ Should NOT depend on `services` or `bin` crates
- ✅ Each extractor should be self-contained for its data source

#### 🌐 `crates/services/` - Server Components
**Purpose**: HTTP/gRPC services and server-side functionality.

**What belongs here:**
- **`server`**: Arrow Flight gRPC server and JSON Lines HTTP server
- **`admin-api`**: Administrative HTTP API for management
- **`http-common`**: Shared HTTP utilities and middleware

**Dependency Rules:**
- ✅ Can depend on `core` crates for business logic
- ✅ `http-common` can be shared among other service crates
- ❌ Should NOT depend on `extractors` or `bin` crates directly
- ✅ Services orchestrate core functionality for external interfaces

#### 📋 Additional Workspace Components

**`crates/client/`**: Client libraries for connecting to Amp services
- Language bindings and SDK functionality
- Can depend on `core` for shared data structures

**`tests/`**: Workspace-level integration tests
- End-to-end testing across multiple crates
- Cross-crate integration scenarios

## 🛠️ Cargo workspace management

### 🚨 MANDATORY WORKSPACE MANAGEMENT RULES
**🤖 AI AGENTS: These rules are NON-NEGOTIABLE. Follow them exactly.**

#### Rule #1: ALWAYS Execute Commands from Workspace Root
- ✅ **REQUIRED**: All `cargo` commands MUST be executed from the PROJECT ROOT DIRECTORY
- ❌ **FORBIDDEN**: Never `cd` into crate directories to run cargo commands
- ✅ **REQUIRED**: Always use `--package <crate-name>` (or `-p <crate-name>`) to target specific crates

#### Rule #2: NEVER Manually Edit Cargo.toml Dependencies
- ❌ **FORBIDDEN**: Manually editing `[dependencies]`, `[dev-dependencies]`, or `[build-dependencies]` sections
- ✅ **REQUIRED**: Always use `cargo add` and `cargo remove` commands from workspace root

### 🔧 Dependency management commands

#### Adding Dependencies

```bash
# Add runtime dependency to specific crate (from workspace root)
cargo add <dependency-name> --package <crate-name>

# Add dependency with features (from workspace root)
cargo add <dependency-name> --package <crate-name> --features <feature1>,<feature2>

# Add dependency with no default features (minimal features - RECOMMENDED)
cargo add <dependency-name> --package <crate-name> --no-default-features

# Add dependency with no default features and specific features (from workspace root)
cargo add <dependency-name> --package <crate-name> --no-default-features --features <feature1>,<feature2>

# Add development dependency (from workspace root)
cargo add <dependency-name> --package <crate-name> --dev

# Add build dependency (from workspace root)
cargo add <dependency-name> --package <crate-name> --build

# Add workspace-level dependency (from workspace root)
cargo add <dependency-name> --workspace

# Add local workspace crate dependency (from workspace root)
cargo add <local-crate-name> --package <target-crate-name> --path crates/<category>/<local-crate-name>

# Add optional dependency (from workspace root)
cargo add <dependency-name> --package <crate-name> --optional
```

#### Real Examples 

```bash
# Add tokio to metadata-db crate (from workspace root)
cargo add tokio --package metadata-db

# Add serde with minimal features to common crate (RECOMMENDED)
cargo add serde --package common --no-default-features --features derive

# Add sqlx with only required features (minimal approach)
cargo add sqlx --package metadata-db --no-default-features --features postgres,runtime-tokio-rustls,migrate

# Add reqwest with minimal features for HTTP client
cargo add reqwest --package evm-rpc --no-default-features --features json,rustls-tls

# Add test dependency (from workspace root)
cargo add tempfile --package metadata-db --dev

# Add build-time dependency for protocol buffers (from workspace root)
cargo add prost-build --package firehose --build

# Add workspace dependency available to all crates (from workspace root)
cargo add anyhow --workspace

# Add internal crate dependency (from workspace root)
cargo add common --package metadata-db --path crates/core/common

# Add optional Redis support with minimal features (from workspace root)
cargo add redis --package metadata-db --optional --no-default-features --features tokio-comp
```

#### Removing Dependencies

```bash
# Remove runtime dependency (from workspace root)
cargo remove <dependency-name> --package <crate-name>

# Remove development dependency (from workspace root)
cargo remove <dependency-name> --package <crate-name> --dev

# Remove build dependency (from workspace root)
cargo remove <dependency-name> --package <crate-name> --build
```

### 🏗️ Creating new crates 

#### 📋 New Crate Creation Procedure

**Step 1: Planning Questions**

Before creating any new crate, answer these questions:

1. **🎯 What is the primary purpose of this crate?**
   - What specific functionality will it provide?
   - Is it shared library code, a data connector, a service, or an executable?

2. **📂 Which workspace category does it belong in?**
   - `core/`: Shared functionality used across the workspace?
   - `extractors/`: Data source connector for blockchain data?
   - `services/`: HTTP/gRPC server or network service?
   - `bin/`: Executable application or CLI tool?

3. **🏷️ What should the crate name be?**
   - Does the name clearly describe its function?
   - Follow kebab-case naming (e.g., `data-validator`, `metrics-api`)
   - **💡 RECOMMENDED**: Consider namespace patterns: `<namespace>-<specific>` (e.g., `dataset-common`, `dataset-derived`)
     + Namespace is more generic than the specific name part
     + Groups related crates under a common prefix
   - Is it specific enough to avoid confusion with other crates?

4. **🔗 What are its expected dependencies?**
   - Will it depend only on `core` crates (recommended)?
   - Does it need to depend on other categories (check dependency rules)?
   - Will other crates depend on this one?

**Step 2: Create the Crate (from workspace root)**

```bash
# For library crates
cargo new --lib crates/<category>/<crate-directory> --name <crate-name>

# For binary crates  
cargo new --bin crates/<category>/<crate-directory> --name <binary-name>
```

**Step 3: Add to Workspace**

```bash
# Add the new crate path to root Cargo.toml members array (alphabetically ordered)
# Edit Cargo.toml to add: "crates/<category>/<crate-directory>"
```

**Step 4: Validate Setup**

```bash
# Check that the new crate compiles
just check-crate <crate-name>
```

#### Command Format
```bash
# Create library crate (from workspace root)
cargo new --lib <destination-path> --name <crate-name>

# Create binary crate (from workspace root)  
cargo new --bin <destination-path> --name <binary-name>
```

#### Location-Specific Examples

**Core Crates:**
```bash
# New core library (from workspace root)
cargo new --lib crates/core/analytics --name analytics

# Cache core crate (from workspace root)
cargo new --lib crates/core/cache --name cache
```

**Extractor Crates:**
```bash
# New blockchain extractor (from workspace root)
cargo new --lib crates/extractors/polygon --name polygon-extractor

# Cosmos blockchain extractor (from workspace root)
cargo new --lib crates/extractors/cosmos --name cosmos-extractor
```

**Service Crates:**
```bash
# New HTTP API service (from workspace root)
cargo new --lib crates/services/metrics-api --name metrics-api

# WebSocket service (from workspace root)
cargo new --lib crates/services/websocket --name websocket-server
```

**Binary Crates:**
```bash
# New CLI tool (from workspace root)
cargo new --bin crates/bin/validator --name data-validator

# Background worker (from workspace root)
cargo new --bin crates/bin/worker --name background-worker
```

## 📑 Crate manifest (Cargo.toml)

### 📋 Cargo.toml Section Ordering

The cargo manifest (`Cargo.toml`) for each crate MUST follow this exact section ordering:

```toml
[package]
name = "crate-name"
version = "0.1.0"
edition = "2021"
# ... other package metadata

[features]  # OPTIONAL - only include if crate needs features
# See detailed features section below for requirements
default = ["basic-logging"]
# ... other features in alphabetical order

[dependencies]  # OPTIONAL
# Runtime dependencies in alphabetical order
anyhow = "1.0"
serde = { version = "1.0", features = ["derive"] }
tokio = { version = "1.0", features = ["full"] }

[dev-dependencies]  # OPTIONAL
# Development/test dependencies in alphabetical order
tempfile = "3.0"
tokio-test = "0.4"

[build-dependencies]  # OPTIONAL
# Build-time dependencies in alphabetical order
prost-build = "0.12"
```

#### 🚨 MANDATORY SECTION ORDERING
**🤖 AI AGENTS: These rules are NON-NEGOTIABLE. Follow them exactly.**

Sections MUST appear in this exact order:

1. **`[package]`** - Crate metadata (name, version, edition, etc.)
2. **`[features]`** - Feature flags and their dependencies (see detailed requirements below)
3. **`[dependencies]`** - Runtime dependencies (alphabetically ordered)
4. **`[dev-dependencies]`** - Development/test dependencies (alphabetically ordered) 
5. **`[build-dependencies]`** - Build-time dependencies (alphabetically ordered)

#### Section Requirements

- **All sections are optional** except `[package]`
- **Dependencies within each section MUST be alphabetically ordered**
- **No other sections** should be mixed between these core sections
- **Consistent formatting** with proper spacing between sections

### 🏷️ Crate `[features]` section

#### 🚨 MANDATORY FEATURE REQUIREMENTS

**🚨 IMPORTANT: Features sections are OPTIONAL. Do NOT add a `[features]` section if the crate doesn't already have one. The `default` feature is implicit and optional when empty.**

When a `[features]` section exists, follow these rules:
- **Alphabetical ordering**: All features in `[features]` section MUST be ordered alphabetically
- **🚨 EXCEPTION**: The `default` feature MUST be listed FIRST - THIS IS THE ONLY EXCEPTION to alphabetical ordering
- **Documentation**: Each feature MUST have a `#` comment above it explaining its purpose
- **Naming convention**: Feature names MUST use kebab-case (e.g., `postgres-support`, `admin-api`)

#### 📋 Feature Naming Conventions

Feature names MUST use kebab-case with lowercase letters and hyphens only for consistency.

**✅ RECOMMENDED FEATURE NAMES**
- `postgres-support` (database type + purpose)
- `admin-api` (component + interface type)
- `tls-support` (protocol + purpose)
- `metrics-collection` (function + action)
- `redis-cache` (technology + purpose)
- `json-serialization` (format + function)

**⚠️ LESS IDEAL FEATURE NAMES** (but not violations)
- `postgres` (too abbreviated, unclear scope)
- `admin` (too vague, what kind of admin?)
- `tls` (unclear what TLS functionality)
- `metrics` (unclear if collection, export, or both)
- `cache` (unclear which caching technology)
- `json` (unclear if parsing, serialization, or both)

#### 📝 Feature documentation format

Each feature must follow this exact format:

```toml
[features]
# Default features (enabled by default)
default = ["basic-logging"]
# Enable admin API endpoints and management interface
admin-api = ["dep:axum", "dep:tower", "http-common/admin"]
# Basic logging functionality with structured output
basic-logging = ["dep:tracing", "dep:tracing-subscriber"]
# PostgreSQL database support with connection pooling
postgres-support = ["dep:sqlx/postgres", "dep:sqlx/runtime-tokio-rustls"]
# Redis caching support with async client
redis-cache = ["dep:redis", "dep:tokio"]
```

**✅ Correct `[features]` section examples**

```toml
[features]
# Default features that are always enabled (unless default-features is set to false)
default = ["basic-logging", "json-support"]
# Enable comprehensive admin API with authentication
admin-api = ["dep:axum", "dep:tower-http", "dep:serde_json"]
# Basic structured logging with console output
basic-logging = ["dep:tracing", "dep:tracing-subscriber/fmt"]
# Database migration support and utilities
database-migrations = ["dep:sqlx/migrate", "postgres-support"]
# JSON serialization and deserialization support
json-support = ["dep:serde", "dep:serde_json"]
# Metrics collection and Prometheus export
metrics-collection = ["dep:metrics", "dep:metrics-prometheus"]
# PostgreSQL database with connection pooling
postgres-support = ["dep:sqlx/postgres", "dep:sqlx/runtime-tokio-rustls"]
# Redis caching with tokio async support
redis-cache = ["dep:redis", "dep:tokio"]
# TLS support for secure connections
tls-support = ["dep:rustls", "dep:tokio-rustls"]
```

**❌ Incorrect `[features]` section examples**

```toml
[features]
# ❌ WRONG - Features not alphabetically ordered and `default` feature should be first.
redis-cache = ["dep:redis"]
basic-logging = ["dep:tracing"]  # Should come before redis-cache
postgres-support = ["dep:sqlx"]  # Should come before redis-cache
default = ["basic-logging"]  # `default` should be first

# ❌ WRONG - Missing documentation comments
admin-api = ["dep:axum"]  # No comment explaining what this feature does

# ❌ WRONG - Incorrect naming (not kebab-case)
postgresSupport = ["dep:sqlx"]    # Should be "postgres-support"
REDIS_CACHE = ["dep:redis"]       # Should be "redis-cache" 
admin_API = ["dep:axum"]          # Should be "admin-api"
postgres_support = ["dep:sqlx"]   # Should be "postgres-support" (no underscores)
metrics_collection = ["dep:metrics"]  # Should be "metrics-collection" (no underscores)
json_serialization = ["dep:serde"]   # Should be "json-serialization" (no underscores)

# ❌ WRONG - Vague or unhelpful comments
# stuff
basic-features = ["dep:serde"]    # Comment doesn't explain purpose

# enable db
db = ["dep:sqlx"]                 # Too abbreviated, unclear what it enables
```
