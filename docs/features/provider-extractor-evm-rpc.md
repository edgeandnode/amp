---
name: "provider-extractor-evm-rpc"
description: "EVM JSON-RPC provider configuration for Ethereum and compatible chains. Load when asking about RPC providers, EVM endpoints, rate limiting, or batch requests"
type: feature
components: "crate:evm-rpc,crate:common"
---

# JSON-RPC Provider

## Summary

The JSON-RPC provider enables data extraction from EVM-compatible blockchains via standard JSON-RPC endpoints. It supports HTTP, WebSocket, and IPC connections with configurable rate limiting, request batching, and concurrent request management.

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Architecture](#architecture)
3. [Configuration](#configuration)
4. [Connection Types](#connection-types)
5. [Usage](#usage)
6. [Implementation](#implementation)
7. [Limitations](#limitations)
8. [References](#references)

## Key Concepts

- **JSON-RPC**: Standard Ethereum JSON-RPC API for blockchain data access
- **Batching**: Combining multiple RPC calls into a single HTTP request
- **Rate Limiting**: Throttling requests to avoid endpoint limits
- **Receipt Fetching**: Strategy for obtaining transaction receipts

## Architecture

The JSON-RPC provider integrates into Amp's extraction pipeline as a data source for EVM-compatible blockchains:

```
Dataset Job → Provider Resolution → JSON-RPC Client → RPC Endpoint
                                           ↓
                                   Rate Limiter
                                           ↓
                                   Batch Processor
                                           ↓
                            Block Stream (blocks, txs, logs)
```

### Connection Management

- **Auto-detection**: URL scheme determines connection type (HTTP/WS/IPC)
- **Concurrency control**: Semaphore limits parallel requests
- **Rate limiting**: Token bucket algorithm enforces per-minute quotas

### Data Extraction Flow

1. Job requests block range from dataset manifest
2. Provider resolution finds matching `evm-rpc` provider for network
3. Client streams blocks using batched or unbatched strategy
4. Receipt fetching uses bulk or per-tx strategy based on config
5. Data materialized as Parquet files (blocks, transactions, logs tables)

## Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `kind` | string | - | Must be `"evm-rpc"` |
| `network` | string | - | Network identifier (mainnet, base, polygon, etc.) |
| `url` | string | - | RPC endpoint URL (http/https/ws/wss/ipc) |
| `concurrent_request_limit` | number | 1024 | Max concurrent requests |
| `rpc_batch_size` | number | 0 | Requests per batch (0 = disabled) |
| `rate_limit_per_minute` | number | none | Rate limit in requests/minute |
| `fetch_receipts_per_tx` | boolean | false | Use per-tx receipt fetching |

### Minimal Configuration

```toml
kind = "evm-rpc"
network = "mainnet"
url = "${RPC_ETH_MAINNET_URL}"
```

### Full Configuration

```toml
kind = "evm-rpc"
network = "mainnet"
url = "${RPC_ETH_MAINNET_URL}"
concurrent_request_limit = 512
rpc_batch_size = 100
rate_limit_per_minute = 1000
fetch_receipts_per_tx = false
```

## Connection Types

The provider auto-detects connection type from URL scheme:

| Scheme | Type | Use Case |
|--------|------|----------|
| `http://`, `https://` | HTTP | Standard RPC endpoints |
| `ws://`, `wss://` | WebSocket | Persistent connections |
| `ipc://` | IPC Socket | Local node connections |

### Examples

```toml
# HTTP endpoint
url = "https://eth-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}"

# WebSocket endpoint
url = "wss://eth-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}"

# Local IPC socket
url = "ipc:///home/user/.ethereum/geth.ipc"
```

## Usage

### Receipt Fetching Strategies

**Bulk receipts** (default, `fetch_receipts_per_tx = false`):
- Uses `eth_getBlockReceipts` for all receipts at once
- Faster but requires RPC support

**Per-transaction receipts** (`fetch_receipts_per_tx = true`):
- Uses `eth_getTransactionReceipt` for each transaction
- Slower but more compatible with all endpoints

### Rate Limiting

Configure rate limiting for endpoints with quotas:

```toml
# 10 requests per second
rate_limit_per_minute = 600
```

### Batching

Enable batching to reduce HTTP overhead:

```toml
# Batch up to 100 RPC calls per request
rpc_batch_size = 100
```

## Implementation

### Extracted Tables

| Table | Description |
|-------|-------------|
| `blocks` | Block headers |
| `transactions` | Transaction data |
| `logs` | Event logs |

### Source Files

- `crates/extractors/evm-rpc/src/lib.rs` - ProviderConfig and client factory
- `crates/extractors/evm-rpc/src/client.rs` - JsonRpcClient with streaming
- `crates/core/common/src/evm/provider.rs` - Low-level provider construction

### Sample Configuration File

`docs/providers/evm-rpc.sample.toml`

## Limitations

- IPC connections only work with local nodes
- `eth_getBlockReceipts` not supported by all endpoints
- Rate limiting applies per provider instance, not globally

## References

- [provider](provider.md) - Base: Provider system overview
- [provider-config](provider-config.md) - Related: Configuration format
