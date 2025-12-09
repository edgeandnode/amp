# Solana Dataset Extractor

A high-performance extractor for Solana blockchain data, designed to work with the [Old Faithful](https://docs.old-faithful.net/) archive and Solana RPC subscriptions.

## Overview

The Solana extractor implements a two-stage data extraction pipeline:

1. **Historical Data**: Downloads and processes epoch-based CAR files from the Old Faithful archive
2. **Real-time Data**: Subscribes to new blocks via Solana RPC PubSub (WebSocket)

This hybrid approach ensures efficient historical backfills while maintaining low-latency access to new blocks.

## Architecture

### Components

- **`SolanaExtractor`**: Main extractor implementing the `BlockStreamer` trait
- **`SolanaRpcClient`**: Handles HTTP requests to the Solana RPC endpoint
- **Subscription Task**: Background task that listens for new block notifications and populates the ring buffer
- **`SolanaSlotRingBuffer`**: Fixed-size ring buffer for storing recent slots from subscriptions

### Data Flow

```
Old Faithful Archive (CAR files) ──┐
                                   ├──> SolanaExtractor ──> Block Processing ──> Parquet Tables
WebSocket Subscription ────────────┘          │
         │                                    │
         └──> Ring Buffer ────────────────────┘
```

### Slot vs. Block Number Handling

**Important**: Solana uses "slots" as time intervals for block production, but not every slot produces a block. Some slots may be skipped due to network issues or validator performance.

This extractor treats Solana slots as block numbers for compatibility with the `BlockStreamer` infrastructure. **Skipped slots are handled by yielding empty rows**, ensuring the sequence of block numbers remains continuous and queries work correctly.

## Provider Config

```toml
kind = "solana"
name = "solana-mainnet"
network = "mainnet"
http_url = "https://api.mainnet-beta.solana.com"
ws_url = "wss://api.mainnet-beta.solana.com"
of1_car_directory = "path/to/local/car/files"
```

**Key Configuration Options**:

- `http_url`: Solana RPC HTTP endpoint for historical data
- `ws_url`: Solana RPC WebSocket endpoint for real-time subscriptions
- `of1_car_directory`: Local directory for caching Old Faithful CAR files
- `start_block`: Starting slot number for extraction
- `finalized_blocks_only`: Whether to only extract finalized blocks

## Old Faithful Archive

The extractor downloads epoch-based CAR (Content Addressable aRchive) files from the Old Faithful service:

- **Archive URL**: `https://files.old-faithful.net`
- **File Format**: `epoch-<epoch_number>.car`
- **Epoch Size**: 432,000 slots per epoch (≈2 days at 400ms slot time)

CAR files are automatically downloaded on-demand and cached locally for future use.

### Warning

Due to the large size of Solana CAR files, ensure sufficient disk space is available in the specified `of1_car_directory`.

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
