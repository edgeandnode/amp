---
name: "provider-extractor-firehose"
description: "StreamingFast Firehose provider for high-throughput blockchain streaming. Load when asking about Firehose, gRPC streaming, or StreamingFast"
components: "crate:firehose"
---

# Firehose Provider

## Summary

The Firehose provider enables high-throughput blockchain data extraction via StreamingFast's Firehose protocol. It uses gRPC streaming for efficient data transfer and supports optional bearer token authentication.

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Architecture](#architecture)
3. [Configuration](#configuration)
4. [Usage](#usage)
5. [Implementation](#implementation)
6. [References](#references)

## Key Concepts

- **Firehose**: StreamingFast's high-performance blockchain data streaming protocol
- **gRPC Streaming**: Efficient binary protocol for continuous data delivery
- **Bearer Token**: Optional authentication for protected endpoints

## Architecture

The Firehose provider integrates into Amp's extraction pipeline as a high-throughput data source for blockchain data:

```
Dataset Job → Provider Resolution → Firehose Client → gRPC Stream → Firehose Endpoint
                                            ↓
                                    TLS + Compression
                                            ↓
                                    Block Stream (with traces)
                                            ↓
                        Parquet Tables (blocks, txs, calls, logs)
```

### Streaming Model

- **Server-side streaming**: Firehose pushes blocks continuously via gRPC
- **Auto-retry**: 5-second backoff on stream errors for resilience
- **Compression**: Gzip compression reduces network bandwidth
- **Large payloads**: Supports up to 100 MiB messages for call traces

### Data Extraction Flow

1. Job requests block range from dataset manifest
2. Provider resolution finds matching `firehose` provider for network
3. Client establishes gRPC stream with authentication (if configured)
4. Firehose streams blocks with full transaction traces
5. Data materialized as Parquet files (blocks, transactions, calls, logs tables)

## Configuration

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `kind` | string | Yes | Must be `"firehose"` |
| `network` | string | Yes | Network identifier (mainnet, base, etc.) |
| `url` | string | Yes | Firehose gRPC endpoint URL |
| `token` | string | No | Bearer token for authentication |

### Minimal Configuration

```toml
kind = "firehose"
network = "mainnet"
url = "${FIREHOSE_ETH_MAINNET_URL}"
```

### With Authentication

```toml
kind = "firehose"
network = "mainnet"
url = "${FIREHOSE_ETH_MAINNET_URL}"
token = "${FIREHOSE_ETH_MAINNET_TOKEN}"
```

## Usage

### Supported Networks

Any EVM-compatible network with a Firehose endpoint:

```toml
# Ethereum mainnet
network = "mainnet"

# Base L2
network = "base"

# Polygon
network = "polygon"
```

### Authentication

When a token is provided, it's sent as a bearer token in the `authorization` header:

```
authorization: bearer <token>
```

## Implementation

### Connection Features

| Feature | Description |
|---------|-------------|
| **Gzip Compression** | Both send and receive compressed |
| **Large Messages** | Up to 100 MiB message size |
| **Auto Retry** | 5-second backoff on stream errors |
| **TLS** | Native TLS with system roots |

### Extracted Tables

| Table | Description |
|-------|-------------|
| `blocks` | Block header information |
| `transactions` | Transaction data |
| `calls` | Internal call traces |
| `logs` | Event logs |

### Source Files

- `crates/extractors/firehose/src/client.rs` - Firehose gRPC client
- `crates/extractors/firehose/src/dataset.rs` - ProviderConfig struct

### Sample Configuration File

`docs/providers/firehose.sample.toml`

## References

- [provider](provider.md) - Base: Provider system overview
- [provider-config](provider-config.md) - Related: Configuration format
