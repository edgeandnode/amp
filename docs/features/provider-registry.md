---
name: "providers-registry"
description: "Provider configuration storage, CRUD operations, and caching. Load when working with provider management, RPC/Firehose endpoints, or blockchain data source configuration"
type: component
components: "crate:amp-providers-registry,crate:object-store,crate:datasets-derived,crate:eth-beacon-datasets,crate:evm-rpc-datasets,crate:firehose-datasets,crate:solana-datasets"
---

# Providers Registry

## Summary

The Providers Registry manages external data source provider configurations (RPC endpoints, Firehose endpoints, etc.) that datasets connect to for blockchain data extraction. It handles storage, retrieval, caching, and CRUD operations for provider configurations stored as TOML files. The registry is injected as a dependency into services that need provider access, following the Inversion of Control principle.

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Architecture](#architecture)
3. [Implementation](#implementation)
4. [References](#references)

## Key Concepts

- **Provider Configuration**: A TOML file defining connection details for a blockchain data source, including required fields (`name`, `kind`, `network`) and provider-specific fields (URLs, tokens, rate limits)
- **ProvidersRegistry**: The main struct that manages provider configurations with lazy-loaded, write-through caching
- **DatasetKind**: Enum representing supported provider types (`evm-rpc`, `firehose`, `solana`, `eth-beacon`, `derived`)
- **Write-Through Cache**: Cache strategy where writes update both the underlying store and in-memory cache synchronously

## Architecture

### Responsibility Boundaries

The Providers Registry follows strict separation of concerns:

**Registry IS responsible for:**
- Storing and retrieving provider configurations (TOML files)
- CRUD operations: register, get, list, delete
- Caching configurations in memory (lazy-loaded, write-through)
- Loading configurations from disk into cache
- Parsing and serializing provider configuration files
- Validating provider configuration structure (required fields: `name`, `kind`, `network`)

**Registry is NOT responsible for:**
- Managing relationships between datasets and providers
- Deciding which provider to use for a given dataset (handled by `DatasetStore`)
- Loading or interpreting dataset manifests
- Dataset revision management

### Dependency Flow

```
┌─────────────────┐     ┌───────────────────┐     ┌─────────────────┐
│   admin-api     │────▶│ ProvidersRegistry │◀────│  DatasetStore   │
│   (CRUD APIs)   │     │    (storage)      │     │ (selection)     │
└─────────────────┘     └───────────────────┘     └─────────────────┘
                                 │
                                 ▼
                        ┌───────────────────┐
                        │   Object Store    │
                        │  (TOML files)     │
                        └───────────────────┘
```

## Implementation

### Source Files

- `crates/core/providers-registry/src/lib.rs` - `ProvidersRegistry` struct and CRUD operations
- `crates/core/providers-registry/src/dataset_kind.rs` - `DatasetKind` enum for provider types
- `crates/core/providers-registry/src/tests/` - Unit tests for caching and CRUD operations

### Caching Behavior

- **Lazy Loading**: Cache populated on first read when empty
- **Write-Through**: `register()` and `delete()` update both store and cache
- **No Expiration**: Cache never expires; external changes require process restart
- **Thread-Safe**: Uses `Arc<RwLock<BTreeMap>>` for concurrent access

## References

- [provider-config](provider-config.md) - Related: Provider configuration file format and validation
