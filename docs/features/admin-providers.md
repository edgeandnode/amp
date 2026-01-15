---
name: "admin-providers"
description: "Provider registration, listing, and deletion via Admin API and CLI. Load when asking about registering providers, listing providers, or managing provider configurations"
components: "service:admin-api,crate:admin-client,app:ampctl"
---

# Provider Management

## Summary

Provider management enables operators to register, list, inspect, and delete provider configurations through the Admin API and CLI. Providers must be registered before datasets can use them for data extraction.

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Architecture](#architecture)
3. [Usage](#usage)
4. [API Reference](#api-reference)
5. [Implementation](#implementation)
6. [References](#references)

## Key Concepts

- **Provider Registration**: Adding a new provider configuration to the system
- **Provider Info**: API representation of a provider (name, kind, network, config)
- **Idempotent Delete**: Deletion returns 204 even if provider doesn't exist

## Architecture

Provider management is accessed through two interfaces:

| Interface | Description |
|-----------|-------------|
| CLI | [ampctl](app-ampctl.md) provider commands |
| API | RESTful Admin API on port 1610 |

### Management Flow

```
Operator → ampctl/API → Admin API → Dataset Store → TOML files
```

## Usage

**Register a provider:**

Add a new blockchain data source to enable dataset extraction.

```bash
# EVM RPC provider
ampctl provider register my-mainnet-rpc \
  --kind evm-rpc \
  --network mainnet \
  --url '${RPC_ETH_MAINNET_URL}'

# Firehose provider
ampctl provider register my-firehose \
  --kind firehose \
  --network mainnet \
  --url '${FIREHOSE_URL}' \
  --token '${FIREHOSE_TOKEN}'

# Anvil (local development - IPC)
ampctl provider register anvil-ipc \
  --kind evm-rpc \
  --network anvil \
  --url 'ipc:///tmp/anvil.ipc'

# Anvil (local development - HTTP)
ampctl provider register anvil-local \
  --kind evm-rpc \
  --network anvil \
  --url 'http://localhost:8545'
```

**List providers:**

View all registered providers and their configurations.

```bash
ampctl provider list
ampctl provider ls          # alias
ampctl provider list --json # JSON output
```

**Note**: Listing displays raw configuration with `${VAR_NAME}` placeholders, not interpolated secrets.

**Inspect a provider:**

Check details of a specific provider configuration.

```bash
ampctl provider inspect my-mainnet-rpc
ampctl provider get my-mainnet-rpc  # alias
```

**Delete a provider:**

Remove a provider that's no longer needed.

```bash
ampctl provider remove my-mainnet-rpc
ampctl provider rm my-mainnet-rpc  # alias
```

**Direct API access:**

Programmatically manage providers without the CLI.

```bash
# List
curl http://localhost:1610/providers

# Get
curl http://localhost:1610/providers/my-mainnet-rpc

# Create
curl -X POST http://localhost:1610/providers \
  -H "Content-Type: application/json" \
  -d '{"name":"my-rpc","kind":"evm-rpc","network":"mainnet","url":"${RPC_URL}"}'

# Delete
curl -X DELETE http://localhost:1610/providers/my-mainnet-rpc
```

## API Reference

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/providers` | GET | List all providers |
| `/providers` | POST | Register new provider |
| `/providers/{name}` | GET | Get provider by name |
| `/providers/{name}` | DELETE | Delete provider |

### Error Codes

| Code | Status | Description |
|------|--------|-------------|
| `INVALID_REQUEST_BODY` | 400 | Malformed JSON |
| `DATA_CONVERSION_ERROR` | 400 | JSON→TOML failed |
| `INVALID_PROVIDER_NAME` | 400 | Invalid name format |
| `PROVIDER_NOT_FOUND` | 404 | Provider doesn't exist |
| `PROVIDER_CONFLICT` | 409 | Name already exists |
| `STORE_ERROR` | 500 | Storage failed |

## Implementation

### Source Files

- `crates/services/admin-api/src/handlers/providers/` - API handlers
- `crates/clients/admin/src/providers.rs` - Rust client
- `crates/bin/ampctl/src/cmd/provider/` - CLI commands

## References

- [admin](admin.md) - Base: Administration overview
- [provider](provider.md) - Dependency: Provider system
- [app-ampctl](app-ampctl.md) - Related: CLI tool
- [app-ampd-controller](app-ampd-controller.md) - Related: Admin API server
