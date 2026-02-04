# Solana Dataset Extractor

A high-performance extractor for Solana blockchain data, designed to work with the [Old Faithful](https://docs.old-faithful.net/) archive and Solana JSON-RPC providers.

## Overview

The Solana extractor implements a two-stage data extraction pipeline:

1. **Historical Data**: Downloads and processes epoch-based CAR files from the Old Faithful archive
2. **Recent Data**: Fetches the latest slots via JSON-RPC once the historical data is exhausted

This hybrid approach ensures efficient historical backfills while maintaining low-latency access to new blocks. The `use_archive` configuration controls how these two stages interact (see [Provider Config](#provider-config)).

## Architecture

### Components

- **`SolanaExtractor`**: Main extractor implementing the `BlockStreamer` trait
- **`SolanaRpcClient`**: Handles HTTP requests to the Solana RPC endpoint, with optional rate limiting and metrics
- **`of1_client`**: Manages Old Faithful CAR file downloads with resume support, retry logic, and lifecycle tracking

### Data Flow

```
Old Faithful Archive (CAR files) ──┐
                                   ├──> SolanaExtractor ──> Block Processing ──> Parquet Tables
Solana JSON-RPC ───────────────────┘
```

### Archive Usage Modes

The `use_archive` setting controls how the extractor sources block data:

- **`always`** (default): Always use Old Faithful CAR files for block data
- **`never`**: RPC-only mode, no archive downloads
- **`auto`**: Use RPC for recent slots (last ~10k), archive for historical data

### Tables

The extractor produces the following tables:

| Table | Description |
|-------|-------------|
| `block_headers` | Block-level metadata (slot, parent slot, blockhash, timestamp, etc.) |
| `block_rewards` | Per-block rewards (pubkey, lamports, post balance, reward type, commission) |
| `transactions` | Transaction data (signatures, status, fees, compute units, etc.) |
| `messages` | Transaction messages (accounts, recent blockhash, address table lookups) |
| `instructions` | Both top-level and inner instructions with program IDs and data |

### Slot vs. Block Number Handling

**Important**: Solana uses "slots" as time intervals for block production, but not every slot produces a block. Some slots may be skipped due to network issues or validator performance.

This extractor treats Solana slots as block numbers for compatibility with the `BlockStreamer` infrastructure.

## Provider Config

```toml
kind = "solana"
network = "mainnet"
rpc_provider_url = "https://api.mainnet-beta.solana.com"
of1_car_directory = "path/to/local/car/files"
```

**Configuration Options**:

| Field | Required | Description |
|-------|----------|-------------|
| `rpc_provider_url` | Yes | Solana RPC HTTP endpoint |
| `of1_car_directory` | Yes | Local directory for caching Old Faithful CAR files |
| `max_rpc_calls_per_second` | No | Rate limit for RPC calls |
| `keep_of1_car_files` | No | Whether to retain downloaded CAR files after use (default: `false`) |
| `use_archive` | No | Archive usage mode: `always` (default), `never`, or `auto` |
| `start_block` | No | Starting slot number for extraction (set in the manifest) |
| `finalized_blocks_only` | No | Whether to only extract finalized blocks (set in the manifest) |

## Old Faithful Archive

The extractor downloads epoch-based CAR (Content Addressable aRchive) files from the Old Faithful service:

- **Archive URL**: `https://files.old-faithful.net`
- **File Format**: `epoch-<epoch_number>.car`
- **Epoch Size**: 432,000 slots per epoch (~2 days at 400ms slot time)

CAR files are automatically downloaded on-demand and cached locally. Downloads support resumption on failure and exponential backoff retries. When `keep_of1_car_files` is `false`, files are deleted once no longer needed.

### Warning

Due to the large size of Solana CAR files, ensure sufficient disk space is available in the specified `of1_car_directory`.

## Utilities

### `solana-compare`

A companion example (`examples/solana_compare.rs`) that compares block data from Old Faithful CAR files against the RPC endpoint for the same epoch. Useful for validating data consistency between the two sources.

## JSON Schema Generation

JSON schemas for Solana dataset manifests can be generated using the companion `solana-gen` crate:

```bash
just gen-solana-dataset-manifest-schema
```

This generates a JSON schema from the `Manifest` struct and copies it to `docs/manifest-schemas/solana.spec.json`.

## Related Resources

- [Old Faithful Documentation](https://docs.old-faithful.net/)
- [Solana RPC API](https://docs.solana.com/api)
- [Yellowstone Faithful CAR Parser](https://github.com/lamports-dev/yellowstone-faithful-car-parser)
