---
name: "providers-registry"
description: "Provider configuration storage, CRUD operations, caching, and blockchain client creation. Load when working with provider management, RPC/Firehose endpoints, or blockchain data source configuration"
type: component
components: "crate:amp-providers-registry,crate:object-store,crate:datasets-derived,crate:evm-rpc-datasets,crate:firehose-datasets,crate:solana-datasets"
---

# Providers Registry

## Summary

The Providers Registry manages external data source provider configurations (RPC endpoints, Firehose endpoints, etc.) that datasets connect to for blockchain data extraction. It handles storage, retrieval, caching, CRUD operations, and blockchain client creation from provider configurations stored as TOML files. The registry is injected as a dependency into services that need provider access, following the Inversion of Control principle.

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Terminology](#terminology)
3. [Architecture](#architecture)
4. [Implementation](#implementation)
5. [References](#references)

## Key Concepts

- **Provider Configuration**: Connection details for a blockchain data source stored as TOML
- **Upsert Semantics**: Registration overwrites existing providers; deletion is idempotent
- **Write-Through Cache**: Writes update both object store and in-memory cache synchronously

## Terminology

This document uses two key terms that describe different parts of the data pipeline:

- **Provider**: An external service or endpoint that supplies blockchain data (e.g., a Firehose gRPC server, an EVM JSON-RPC node, a Solana RPC endpoint). The registry stores *configurations* for these external services.

- **Client**: Internal code that connects to a provider and consumes its data. Clients are created by the registry from provider configurations. Examples include `BlockStreamClient` (for extracting blocks) and underlying protocol-specific clients like `JsonRpcClient` or `firehose_datasets::Client`.

> [!NOTE]
> **EVM RPC Provider naming**: The `EvmRpcProvider` type follows [Alloy's naming convention](https://docs.rs/alloy-provider/latest/alloy_provider/) where "provider" refers to the RPC interface abstraction. In this codebase, it functions as a client that consumes data from an external EVM RPC endpoint.

**Relationship**: Provider configurations define *where* to connect; clients implement *how* to connect and extract data.

## Architecture

### Responsibility Boundaries

The Providers Registry is a storage layer for provider configurations and a factory for blockchain clients.

**Registry IS responsible for:**

- Provider configuration storage, caching, and CRUD operations
- Configuration parsing, validation, and environment variable substitution
- Blockchain client creation from provider configurations
- Provider selection by kind and network

**Registry is NOT responsible for:**

- Dataset-provider relationships
- Dataset manifest interpretation
- Revision management

### Client Types (Consumers)

The registry creates clients that consume data from external provider endpoints. Two distinct client types serve different purposes:

1. **Block Stream Clients**: Used for extracting raw blockchain data (blocks, transactions, logs) from data sources like Firehose, EVM RPC, or Solana providers. These clients stream blocks for dataset population.

2. **EVM RPC Providers**: Used for executing `eth_call` operations against EVM chains. These providers handle read-only contract interactions and state queries. Only available for EVM chains.

### Configuration Cache Strategy

Provider configurations are cached in memory using a lazy-loaded, write-through strategy. 
This avoids repeated object store reads for frequently accessed configurations, ensures writes remain consistent without complex invalidation logic, and provides robustness against object store connection issues once configurations are loaded.

**Read Path (Lazy Loading)**
1. On first read request, check if cache is empty
2. If empty, enumerate all configuration files from object store
3. Parse each file into provider configuration and populate cache atomically
4. Return data from cache (subsequent reads skip object store)

**Write Path (Write-Through)**
1. Registration: Write TOML to object store first, then update cache
2. Deletion: Remove from object store first (idempotent), then remove from cache

### Provider Selection Strategy

When multiple providers exist for the same kind and network, the registry uses random selection to distribute load. 
This prevents any single provider from becoming a bottleneck and provides natural load balancing without additional coordination infrastructure.

### Environment Variable Substitution

Provider configurations support `${VAR}` syntax for referencing environment variables. 
Substitution occurs at load time before parsing, allowing secrets like API tokens to be injected from the environment rather than stored in configuration files.


## Implementation

### Caching Behavior

- **Structure**: Shared map with read-write locking
- **Thread-safe**: Uses `parking_lot::RwLock` for concurrent access
- **Lazy Loading**: Cache populated on first read when empty
- **Write-Through**: Registration and deletion update both object store and cache
- **Upsert Semantics**: Registration overwrites existing providers with the same name; deletion is idempotent (succeeds even if provider doesn't exist)
- **No Expiration**: Cache persists until process restart; external store changes not reflected
- **Graceful degradation**: Invalid files are logged and skipped during load

### Source Files

- `crates/core/providers-registry/src/lib.rs` - `ProvidersRegistry` facade with CRUD and client creation
- `crates/core/providers-registry/src/client.rs` - `BlockStreamClient` enum and `create()` function
- `crates/core/providers-registry/src/config.rs` - `ProviderConfig` struct and parsing
- `crates/core/providers-registry/src/store.rs` - `ProviderConfigsStore` with integrated caching
- `crates/core/providers-registry/src/envsubs.rs` - Environment variable substitution
- `crates/core/providers-registry/src/tests/` - Unit tests

## References

- [provider-config](provider-config.md) - Related: Provider configuration file format and validation
