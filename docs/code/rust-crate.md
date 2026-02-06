---
name: "rust-crate"
description: "Cargo.toml section ordering, feature flag rules, kebab-case naming. Load when editing Cargo.toml or adding features to a crate"
type: arch
scope: "global"
---

# Rust Crate Manifest Patterns

**MANDATORY for ALL `Cargo.toml` files in the Amp project**

## Table of Contents

1. [Cargo.toml Section Ordering](#1-cargotoml-section-ordering)
2. [Features Section Rules](#2-features-section-rules)
3. [Checklist](#checklist)

## 1. Cargo.toml Section Ordering

The cargo manifest (`Cargo.toml`) for each crate MUST follow this exact section ordering:

```toml
[package]
name = "crate-name"
version = "0.1.0"
edition = "2021"
# ... other package metadata

[features]  # OPTIONAL - only include if crate needs features
# See features section below for requirements
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

### Mandatory Section Order

Sections MUST appear in this exact order:

1. **`[package]`** - Crate metadata (name, version, edition, etc.)
2. **`[features]`** - Feature flags and their dependencies
3. **`[dependencies]`** - Runtime dependencies (alphabetically ordered)
4. **`[dev-dependencies]`** - Development/test dependencies (alphabetically ordered)
5. **`[build-dependencies]`** - Build-time dependencies (alphabetically ordered)

### Section Requirements

- **All sections are optional** except `[package]`
- **Dependencies within each section MUST be alphabetically ordered**
- **No other sections** should be mixed between these core sections
- **Consistent formatting** with proper spacing between sections

## 2. Features Section Rules

**Features sections are OPTIONAL. Do NOT add a `[features]` section if the crate doesn't already have one. The `default` feature is implicit and optional when empty.**

When a `[features]` section exists, follow these rules:

### Ordering

- **Alphabetical ordering**: All features MUST be ordered alphabetically
- **Exception**: The `default` feature MUST be listed FIRST - this is the only exception to alphabetical ordering

### Naming Convention

Feature names MUST use kebab-case (lowercase letters and hyphens only).

**Recommended feature names** (descriptive):

- `postgres-support` (database type + purpose)
- `admin-api` (component + interface type)
- `tls-support` (protocol + purpose)
- `metrics-collection` (function + action)
- `redis-cache` (technology + purpose)
- `json-serialization` (format + function)

**Less ideal feature names** (too abbreviated):

- `postgres` (unclear scope)
- `admin` (too vague)
- `tls` (unclear functionality)
- `metrics` (unclear if collection, export, or both)
- `cache` (unclear caching technology)
- `json` (unclear if parsing, serialization, or both)

### Documentation

Each feature MUST have a `#` comment above it explaining its purpose.

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

### Incorrect Examples

```toml
[features]
# WRONG - Features not alphabetically ordered and `default` feature should be first.
redis-cache = ["dep:redis"]
basic-logging = ["dep:tracing"]  # Should come before redis-cache
postgres-support = ["dep:sqlx"]  # Should come before redis-cache
default = ["basic-logging"]  # `default` should be first

# WRONG - Missing documentation comments
admin-api = ["dep:axum"]  # No comment explaining what this feature does

# WRONG - Incorrect naming (not kebab-case)
postgresSupport = ["dep:sqlx"]    # Should be "postgres-support"
REDIS_CACHE = ["dep:redis"]       # Should be "redis-cache"
admin_API = ["dep:axum"]          # Should be "admin-api"
postgres_support = ["dep:sqlx"]   # Should be "postgres-support" (no underscores)
metrics_collection = ["dep:metrics"]  # Should be "metrics-collection" (no underscores)
json_serialization = ["dep:serde"]   # Should be "json-serialization" (no underscores)

# WRONG - Vague or unhelpful comments
# stuff
basic-features = ["dep:serde"]    # Comment doesn't explain purpose

# enable db
db = ["dep:sqlx"]                 # Too abbreviated, unclear what it enables
```

## References

- [rust-workspace](rust-workspace.md) - Related: Workspace organization

## Checklist

Before committing Cargo.toml changes, verify:

- [ ] Sections appear in the correct order: `[package]` → `[features]` → `[dependencies]` → `[dev-dependencies]` → `[build-dependencies]`
- [ ] All dependencies within each section are alphabetically ordered
- [ ] Features use kebab-case naming
- [ ] `default` feature is listed first (if present)
- [ ] All remaining features are alphabetically ordered
- [ ] Every feature has a descriptive `#` comment above it
- [ ] No `[features]` section added unnecessarily
