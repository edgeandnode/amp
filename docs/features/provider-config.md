---
name: "provider-config"
description: "Provider configuration format, TOML storage, and environment variable substitution. Load when asking about provider config files, secrets, or provider storage"
components: "crate:dataset-store"
---

# Provider Configuration

## Summary

Provider configurations are stored as TOML files and support environment variable substitution for secrets. The configuration system provides a write-through cache for performance and handles conversion between TOML (storage) and JSON (API) formats automatically.

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Architecture](#architecture)
3. [Configuration Format](#configuration-format)
4. [Environment Variable Substitution](#environment-variable-substitution)
5. [Usage](#usage)
6. [Storage and Caching](#storage-and-caching)
7. [Implementation](#implementation)
8. [References](#references)

## Key Concepts

- **TOML Storage**: Providers are persisted as individual `.toml` files
- **Environment Substitution**: `${VAR_NAME}` syntax for runtime secret injection
- **Write-Through Cache**: In-memory cache updated on every write operation
- **Format Conversion**: Automatic TOML ↔ JSON conversion for API interactions

## Architecture

The provider configuration system is part of the dataset store layer and handles the complete lifecycle of provider configurations:

```
Registration (API/CLI) → JSON → TOML Conversion → File Storage → Cache
                                                        ↓
Dataset Resolution ← Environment Substitution ← Cache Lookup
```

### Storage Layer

- **File-based persistence**: Each provider stored as `{name}.toml` in configured directory
- **Lazy-loaded cache**: Configurations loaded into memory on first access
- **Write-through updates**: Cache synchronized immediately on create/delete operations

### Format Bridge

- **API layer**: Accepts/returns JSON for HTTP compatibility
- **Storage layer**: Persists TOML for human-readability and type safety
- **Bidirectional conversion**: Automatic translation between formats

## Configuration Format

All providers share common fields with provider-specific extensions:

| Field | Required | Description |
|-------|----------|-------------|
| `kind` | Yes | Provider type: `evm-rpc`, `firehose`, `solana` |
| `network` | Yes | Blockchain network identifier |
| Additional | Varies | Provider-specific fields (url, token, etc.) |

### Example Structure

```toml
# Common fields
kind = "evm-rpc"
network = "mainnet"

# Provider-specific fields
url = "${RPC_ETH_MAINNET_URL}"
concurrent_request_limit = 1024
```

## Environment Variable Substitution

Provider configurations support `${VAR_NAME}` syntax for secrets:

```toml
# These are replaced at runtime with environment variable values
url = "${RPC_ETH_MAINNET_URL}"
token = "${API_TOKEN}"
```

### Substitution Rules

- Variables are substituted when provider is loaded, not when stored
- Missing variables cause provider resolution to fail gracefully
- Substitution applies to string values only

### Security Best Practice

Never store sensitive values directly in provider configs:

```toml
# ✅ Good - use environment variables
url = "${RPC_URL}"
token = "${API_TOKEN}"

# ❌ Bad - hardcoded secrets
url = "https://eth-mainnet.example.com/v2/abc123"
token = "sk-secret-key-here"
```

## Usage

### Storing Providers

Providers are stored as TOML files in the configured directory:

```toml
# File: /etc/amp/providers/my-mainnet-rpc.toml
kind = "evm-rpc"
network = "mainnet"
url = "${RPC_ETH_MAINNET_URL}"
concurrent_request_limit = 1024
```

### Environment Variable Substitution

Configure secrets via environment variables:

```bash
# Set environment variable
export RPC_ETH_MAINNET_URL="https://eth-mainnet.g.alchemy.com/v2/YOUR_KEY"

# Provider config references it
# url = "${RPC_ETH_MAINNET_URL}"
# Resolved at runtime to: https://eth-mainnet.g.alchemy.com/v2/YOUR_KEY
```

## Storage and Caching

### File Storage

- Location: Configured provider directory (one file per provider)
- Format: TOML with provider name derived from filename
- Naming: `{provider_name}.toml`

### Caching Behavior

| Behavior | Description |
|----------|-------------|
| **Lazy Loading** | Cache populated on first access |
| **Write-Through** | Register/delete updates both store and cache |
| **No Expiration** | Cache persists for process lifetime |
| **No Hot Reload** | External file changes require process restart |

## Implementation

### Source Files

- `crates/core/dataset-store/src/providers.rs` - ProviderConfigsStore implementation
- `crates/core/dataset-store/src/lib.rs` - Provider resolution (`find_provider`)

### Data Structure

```rust
pub struct ProviderConfig {
    pub name: String,
    pub kind: DatasetKind,
    pub network: String,
    pub rest: toml::Table,  // Provider-specific fields
}
```

## References

- [provider](provider.md) - Base: Provider system overview
- [admin-providers](admin-providers.md) - Related: Provider management API
