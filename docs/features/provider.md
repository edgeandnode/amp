---
name: "provider"
description: "Provider system overview and concepts. Load when asking about providers, data sources, or blockchain connections"
type: meta
components: "crate:dataset-store,crate:common"
---

# Providers

## Summary

Providers are external data source configurations that enable datasets to connect to blockchain networks. They abstract connection details from dataset definitions, allowing reusable, shareable configurations across multiple datasets. The provider system supports multiple blockchain protocols including EVM JSON-RPC, StreamingFast Firehose, and Solana.

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Architecture](#architecture)
3. [Provider Types](#provider-types)

## Key Concepts

- **Provider**: A named configuration representing a connection to a blockchain data source
- **Provider Kind**: The protocol type (evm-rpc, firehose, solana)
- **Network**: The blockchain network identifier (mainnet, goerli, base, etc.)
- **Provider Resolution**: Automatic matching of datasets to compatible providers by kind and network

## Architecture

Providers decouple dataset definitions from concrete data source configurations:

```
Dataset Manifest → Provider Resolution → Provider Config → Blockchain Connection
     (kind, network)     (find match)       (credentials)      (data extraction)
```

### Benefits

| Benefit | Description |
|---------|-------------|
| **Reusability** | Multiple datasets share the same provider |
| **Flexibility** | Switch endpoints without modifying datasets |
| **Load Balancing** | Random selection among matching providers |
| **Security** | Credentials isolated from dataset definitions |

### Resolution Flow

1. Dataset requests provider by `(kind, network)` tuple
2. System finds all matching providers
3. Providers are shuffled for load balancing
4. Each is tried with environment variable substitution
5. First successful connection is used

## Provider Types

| Kind | Protocol | Use Case |
|------|----------|----------|
| `evm-rpc` | JSON-RPC | EVM-compatible chains via HTTP/WS/IPC |
| `firehose` | gRPC | High-throughput streaming from StreamingFast |
| `solana` | JSON-RPC + CAR | Solana blockchain with Old Faithful archive |
