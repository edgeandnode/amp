---
name: "provider-extractor-solana"
description: "Solana blockchain provider with Old Faithful archive support. Load when asking about Solana providers, Old Faithful, or CAR files"
type: feature
components: "crate:solana"
---

# Solana Provider

## Summary

The Solana provider enables data extraction from the Solana blockchain using a two-stage approach: historical data from Old Faithful CAR archive files and real-time data from JSON-RPC endpoints. It handles Solana's slot-based architecture and supports configurable rate limiting.

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Configuration](#configuration)
3. [Architecture](#architecture)
4. [Usage](#usage)
5. [Implementation](#implementation)
6. [Limitations](#limitations)
7. [References](#references)

## Key Concepts

- **Slot**: Solana's time interval unit (~400ms); not every slot produces a block
- **Old Faithful**: Archive of historical Solana data in CAR file format
- **CAR File**: Content-addressable archive file containing epoch data
- **Epoch**: ~432,000 slots (~2 days) of Solana data

## Configuration

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `kind` | string | Yes | Must be `"solana"` |
| `network` | string | Yes | Network identifier (mainnet, devnet) |
| `rpc_provider_url` | string | Yes | Solana RPC HTTP endpoint |
| `of1_car_directory` | string | Yes | Local directory for CAR file cache |
| `max_rpc_calls_per_second` | number | No | Rate limit for RPC calls |
| `keep_of1_car_files` | boolean | No | Retain CAR files after processing (default: false) |

### Example Configuration

```toml
kind = "solana"
network = "mainnet"
rpc_provider_url = "${SOLANA_MAINNET_HTTP_URL}"
of1_car_directory = "${SOLANA_OF1_CAR_DIRECTORY}"
max_rpc_calls_per_second = 50
keep_of1_car_files = false
```

## Architecture

### Two-Stage Data Extraction

```
Historical Data                    Real-time Data
     ↓                                  ↓
Old Faithful Archive → CAR files → RPC endpoint
     ↓                                  ↓
  Download epoch CAR              getBlock calls
     ↓                                  ↓
  Process locally                 Stream blocks
     ↓                                  ↓
  Delete CAR (optional)           Continuous sync
```

### Data Sources

| Stage | Source | Data |
|-------|--------|------|
| Historical | Old Faithful (`files.old-faithful.net`) | Archived epoch CAR files |
| Real-time | Solana RPC | Live blocks via JSON-RPC |

## Usage

### Required Environment Variables

```bash
export SOLANA_MAINNET_HTTP_URL="https://api.mainnet-beta.solana.com"
export SOLANA_OF1_CAR_DIRECTORY="/data/solana/car"
```

### CAR File Management

By default, CAR files are deleted after processing to save disk space:

```toml
# Delete CAR files after processing (default)
keep_of1_car_files = false

# Retain CAR files for debugging or reprocessing
keep_of1_car_files = true
```

### Rate Limiting

For public RPC endpoints, configure rate limiting:

```toml
# 50 requests per second
max_rpc_calls_per_second = 50
```

## Implementation

### Extracted Tables

| Table | Key Fields |
|-------|------------|
| `block_headers` | slot, parent_slot, block_hash, block_height, block_time |
| `transactions` | slot, tx_index, signatures, status, fee, balances |
| `messages` | slot, tx_index, message fields |
| `instructions` | slot, tx_index, program_id_index, accounts, data |

### Slot Handling

- Solana uses slots rather than sequential block numbers
- Skipped slots (no block produced) yield empty rows
- Maintains slot continuity in output data

### Source Files

- `crates/extractors/solana/src/lib.rs` - ProviderConfig and factory
- `crates/extractors/solana/src/extractor.rs` - SolanaExtractor implementation
- `crates/extractors/solana/src/rpc_client.rs` - RPC client with rate limiting
- `crates/extractors/solana/src/of1_client.rs` - Old Faithful CAR client

## Limitations

- CAR files can be very large; ensure sufficient disk space
- Only HTTP/HTTPS RPC URLs supported (no WebSocket)
- Uses finalized commitment level (not configurable)
- Missing blocks yield empty rows, not errors

## References

- [provider](provider.md) - Base: Provider system overview
- [provider-config](provider-config.md) - Related: Configuration format
