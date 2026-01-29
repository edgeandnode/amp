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
| `use_archive` | string | No | Archive usage mode: `"auto"`, `"always"`, or `"never"` (default: `"always"`) |
| `max_rpc_calls_per_second` | number | No | Rate limit for RPC calls |
| `keep_of1_car_files` | boolean | No | Retain CAR files after processing (default: false) |

### Example Configuration

```toml
kind = "solana"
network = "mainnet"
rpc_provider_url = "${SOLANA_MAINNET_HTTP_URL}"
of1_car_directory = "${SOLANA_OF1_CAR_DIRECTORY}"

# Archive mode: "always" (default), "auto", or "never"
# - "always": Always use archive, even for recent data
# - "auto": RPC for recent slots (last 10k), archive for historical
# - "never": Never use archive, RPC-only mode
use_archive = "always"

max_rpc_calls_per_second = 50
keep_of1_car_files = false
```

## Architecture

### Two-Stage Data Extraction

The extractor supports three archive modes controlled by the `use_archive` configuration:

- **`"always"`** (default): Always use archive mode, even for recent slots. Downloads epoch CAR files (~745GB each).
- **`"auto"`**: Smart selection based on slot age. Uses RPC-only mode when `start_slot > current_slot - 10,000` (recent slots within ~83 minutes on mainnet), and archive mode for historical data.
- **`"never"`**: Always use RPC-only mode. Best for demos and recent data extraction.

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
| Historical | Old Faithful (`files.old-faithful.net`) | Archived epoch CAR files (~745GB) |
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
- Skipped slots (no block produced) do not produce any rows, creating gaps in the block number sequence
- Chain integrity is maintained through hash-based validation where each block's `prev_hash` must match the previous block's hash

### Source Files

- `crates/extractors/solana/src/lib.rs` - ProviderConfig and factory
- `crates/extractors/solana/src/extractor.rs` - SolanaExtractor implementation
- `crates/extractors/solana/src/rpc_client.rs` - RPC client with rate limiting
- `crates/extractors/solana/src/of1_client.rs` - Old Faithful CAR client

## Limitations

- CAR files are ~745GB per epoch; download takes 10+ hours on typical connections
- Archive mode can be controlled via `use_archive` config (`"auto"`, `"always"`, `"never"`)
- Only HTTP/HTTPS RPC URLs supported (no WebSocket)
- Uses finalized commitment level (not configurable)

## References

- [provider](provider.md) - Base: Provider system overview
- [provider-config](provider-config.md) - Related: Configuration format
