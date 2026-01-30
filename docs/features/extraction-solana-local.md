---
name: "extraction-solana-local"
description: "Solana local test node via Surfpool/LiteSVM for integration testing. Load when working with Solana test fixtures, Surfpool, LiteSVM, or local Solana extraction tests"
type: "feature"
status: "development"
components: "crate:solana-datasets,crate:tests"
---

# Solana Local Test Node

## Summary

The Solana local test node enables in-process Solana integration testing using Surfpool (a JSON-RPC wrapper around LiteSVM) as a child process. This mirrors the existing Anvil fixture pattern used for Ethereum testing. The Surfpool fixture spawns a local Solana node with automatic port allocation, provides RPC operations for test data generation, and integrates with `TestCtxBuilder` for full E2E pipeline validation of Solana data extraction.

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Architecture](#architecture)
3. [Configuration](#configuration)
4. [Usage](#usage)
5. [Implementation](#implementation)
6. [Limitations](#limitations)
7. [References](#references)

## Key Concepts

- **Surfpool**: A CLI tool that wraps LiteSVM and exposes a standard Solana JSON-RPC HTTP endpoint. Acts as "Solana's Anvil" for local testing.
- **LiteSVM**: An in-process Solana test validator library. Runs inside Surfpool without requiring an external `solana-test-validator` binary.
- **Surfpool Fixture**: The `Surfpool` struct in the test infrastructure that manages the Surfpool child process lifecycle, provides RPC operations, and generates provider configuration.
- **Localnet**: The network identifier (`"localnet"`) used by the local test node, distinguishing it from mainnet/devnet configurations.

## Architecture

### Test Pipeline Flow

The Surfpool fixture integrates into the Amp test pipeline as the Solana RPC source, analogous to how Anvil serves as the Ethereum RPC source:

```
Test Code (Rust)
    │
    ├── Spawn Surfpool CLI ──→ surfpool start --ci --port <PORT> --offline
    │       └── LiteSVM (in-process inside Surfpool)
    │
    ├── Generate test data ──→ SOL transfers via JSON-RPC
    │
    ├── Register provider ───→ sol_rpc (kind=solana, network=localnet, use_archive=never)
    │
    ├── Deploy extraction ───→ Amp Worker reads blocks via JSON-RPC from Surfpool
    │
    └── Query & assert ──────→ Verify all 4 Solana tables contain extracted data
```

### Process Lifecycle

1. `TestCtxBuilder::with_surfpool()` sets the builder flag
2. During `build()`, `Surfpool::new()` allocates a free port via `TcpListener::bind("127.0.0.1:0")`
3. Surfpool CLI spawns as a child process with `--ci` (headless), `--offline` (no remote RPC), `--port <port>`
4. `wait_for_ready()` polls `getSlot` with 200ms retry intervals using `backon::ConstantBuilder`
5. Provider config is generated dynamically with the assigned port and registered as `"sol_rpc"`
6. On `Drop`, the child process is killed and waited on

## Configuration

### Surfpool CLI Flags

| Flag | Purpose |
|------|---------|
| `--ci` | Headless mode, no interactive UI |
| `--port <port>` | Bind to specific port (fixture pre-allocates a free port) |
| `--offline` | No remote RPC fallback, fully local operation |

### Generated Provider Config

The fixture generates this TOML configuration dynamically:

```toml
kind = "solana"
network = "localnet"
rpc_provider_url = "http://127.0.0.1:<PORT>"
of1_car_directory = "/tmp/amp-test-of1"
use_archive = "never"
```

- `network = "localnet"` distinguishes from mainnet configurations
- `use_archive = "never"` disables Old Faithful archive access (not available locally)
- `of1_car_directory` is a dummy path; the directory is never accessed when archive is disabled

### Solana Local Manifest

The test uses `tests/config/manifests/solana_local.json`, which defines the same 4 tables as the mainnet manifest with `network = "localnet"`:

| Table | Description |
|-------|-------------|
| `block_headers` | Slot, parent_slot, block_hash, block_height, block_time |
| `transactions` | Slot, tx_index, signatures, status, fee, balances |
| `messages` | Slot, tx_index, message fields |
| `instructions` | Slot, tx_index, program_id_index, accounts, data |

## Usage

### Basic Test Pattern

```rust
use crate::testlib::ctx::TestCtxBuilder;

#[tokio::test(flavor = "multi_thread")]
async fn test_solana_extraction() {
    // 1. Build test context with Surfpool fixture
    let ctx = TestCtxBuilder::new("solana_local")
        .with_dataset_manifest("solana_local")
        .with_surfpool()
        .build()
        .await
        .unwrap();

    // 2. Generate on-chain activity
    let surfpool = ctx.surfpool();
    let sender = surfpool.fund_new_keypair().unwrap();
    let recipient = solana_sdk::pubkey::Pubkey::new_unique();
    surfpool.send_sol_transfer(&sender, &recipient, 100_000).unwrap();

    // 3. Deploy extraction and query results
    let ampctl = ctx.new_ampctl();
    // ... deploy job, wait, query tables, assert
}
```

### Surfpool Fixture API

```rust
// Lifecycle
Surfpool::new() -> Result<Self>         // Spawn Surfpool child process
surfpool.wait_for_ready(timeout).await  // Poll until RPC responds
// (Drop kills child process automatically)

// RPC operations
surfpool.rpc_url() -> String            // http://127.0.0.1:<port>
surfpool.port() -> u16                  // Assigned port number
surfpool.rpc_client() -> &RpcClient     // Underlying Solana RPC client
surfpool.latest_slot() -> Result<u64>   // Current slot number

// Test data generation
surfpool.fund_new_keypair() -> Result<Keypair>           // Airdrop 1 SOL to new keypair
surfpool.airdrop(to, lamports) -> Result<Signature>      // Request SOL from faucet
surfpool.send_sol_transfer(from, to, lamports) -> Result<Signature> // Transfer SOL

// Configuration
surfpool.new_provider_config() -> String // TOML for provider registration
```

### Prerequisites

The `surfpool` binary must be installed and available in `PATH`:

```bash
# macOS
brew install txtx/taps/surfpool

# From source
cargo install surfpool
```

## Implementation

### Source Files

- `tests/src/testlib/fixtures/surfpool.rs` - Surfpool fixture: process lifecycle, RPC operations, port allocation, provider config generation
- `tests/src/testlib/ctx.rs` - `TestCtxBuilder` integration: `with_surfpool()` builder method, build phase, provider registration as `"sol_rpc"`, `TestCtx` accessor
- `tests/src/testlib/mod.rs` - Module registration for the Surfpool fixture
- `tests/src/tests/it_solana_local.rs` - E2E integration test: SOL transfers, extraction job, query assertions on all 4 tables
- `tests/config/manifests/solana_local.json` - Solana localnet dataset manifest (4 tables, `network = "localnet"`)

### Dependencies

The test crate has these direct Solana dependencies (not re-exported from `solana-datasets`):

| Crate | Version | Purpose |
|-------|---------|---------|
| `solana-sdk` | 3.0.0 | Keypair, transaction construction, pubkey types |
| `solana-rpc-client` | 3.1.8 | RPC client for Surfpool communication |
| `solana-commitment-config` | 3.1.0 | Commitment config for RPC client |
| `solana-system-transaction` | 3.0.0 | SOL transfer transaction construction |

## Limitations

- Surfpool CLI binary must be installed separately; tests fail with a descriptive error if not found
- Small TOCTOU window in port allocation (bind port 0, release, pass to Surfpool)
- Surfpool `getBlock` response format compatibility with the Amp extractor's expected `UiConfirmedBlock` structure is validated through the E2E test rather than a standalone format check
- No custom Solana program support yet; tests use System Program SOL transfers only
- CI environments require a Surfpool installation step

## References

- [provider-extractor-solana](provider-extractor-solana.md) - Related: Solana extraction provider configuration and architecture
- [provider-config](provider-config.md) - Related: Provider configuration format
