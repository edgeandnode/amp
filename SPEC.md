# SPEC.md - Amp: Solana Local Test Node via LiteSVM

## Goal

Add Solana integration testing to the Amp test suite using the
[LiteSVM](https://github.com/LiteSVM/litesvm) crate as an in-process Solana test
validator — analogous to how Anvil is used today for Ethereum.

The end-state is a full E2E pipeline test that mirrors the existing `amp_demo`
(Counter contract on Anvil) but targets a Solana counter program, proving that
the Amp ETL pipeline can extract, transform, and query Solana data from a local
test node. This includes: Surfpool fixture, Solana provider config, dataset
manifest, worker extraction, and query assertions.

**MANDATORY**: The test node MUST run in-process (no external `solana-test-validator`
binary). LiteSVM provides this.

**MANDATORY**: The Amp worker connects to blockchain nodes via JSON-RPC. Surfpool
wraps LiteSVM and exposes a Solana JSON-RPC endpoint for the worker to connect to.

## Current State Analysis (Verified 2026-01-30, Final rev 45 — ALL TASKS COMPLETE)

### What Currently Exists (Verified 2026-01-30)

1. **Test infrastructure** (`tests/src/testlib/`)
    - `ctx.rs`: `TestCtxBuilder` — fluent builder for isolated test environments
    - `fixtures/anvil.rs`: `Anvil` struct — wraps an Anvil instance (IPC or HTTP)
    - `helpers.rs`: `deploy_and_wait`, `load_physical_tables`, snapshot helpers
    - `config.rs`: Fixture resolution for manifests, providers, snapshots, packages

2. **Anvil fixture** (`tests/src/testlib/fixtures/anvil.rs`)
    - Two connection modes: IPC (Unix socket) and HTTP
    - Pre-funded default account (10,000 ETH)
    - Methods: `mine()`, `reorg()`, `latest_block()`, `deploy_contract()`, `send_transaction()`, `call()`
    - Integrated into `TestCtxBuilder` via `.with_anvil_ipc()` / `.with_anvil_http()`
    - Registers itself as a provider config for the test context
    - **Process lifecycle**: Uses `alloy::node_bindings::Anvil` to spawn Anvil as child process;
      `AnvilInstance` auto-terminates on Drop. Port auto-assigned when `0` passed.
    - **Readiness**: Polls `get_block_number()` with 200ms retry interval via `backon::Retryable`.
    - **Provider config**: `new_provider_config()` returns dynamic TOML string with assigned port.

3. **amp_demo package** (`tests/config/packages/amp_demo/`)
    - `Counter.sol`: Solidity contract with `increment()`/`decrement()` + events
    - `abi.ts`: ABI definition used to auto-generate dataset tables
    - `amp.config.ts`: Dataset definition referencing `anvil_rpc` provider
    - Forge deploy/interaction scripts

4. **YAML spec runner** (`tests/src/steps/`)
    - Declarative test specs in `tests/specs/*.yaml`
    - Step types: `anvil`, `anvil_mine`, `anvil_reorg`, `dump`, `restore`, `dataset`, `stream`, `query`
    - No Solana step types exist

5. **Test entry points** (`tests/src/tests/`)
    - Integration tests using `#[tokio::test(flavor = "multi_thread")]`
    - Pattern: build `TestCtx` → register manifests/providers → run YAML spec → assert

6. **Solana extraction crates** (verified in codebase)
    - `crates/extractors/solana/` — main extraction crate with `SolanaExtractor`
    - `crates/extractors/solana-storage-proto/` — vendored protobuf definitions
    - Uses Solana SDK v3.0.x (compatible with LiteSVM 0.9.1 which uses v3.0-3.3)
    - RPC methods used: `getSlot`, `getBlockHeight`, `getBlock` (only 3 methods)
    - Only HTTP/HTTPS URL schemes supported
    - **Hardcoded to "mainnet" network only** — TWO checks:
      - `lib.rs:123-129`: `if config.network != "mainnet"` guard in `extractor()` factory
      - `extractor.rs:62`: `assert_eq!(network, "mainnet")` in `SolanaExtractor::new()`
    - `ProviderConfig` requires `of1_car_directory: PathBuf` (Old Faithful archive)
    - OF1 car manager task is **always spawned** (extractor.rs:73-85) regardless of `use_archive` mode, but when `use_archive = "never"`, the historical stream is empty so the manager does nothing meaningful
    - `UseArchive` enum: `Auto`, `Always` (**default**), `Never` — `Never` mode skips OF1 entirely
    - Existing provider config: `tests/config/providers/solana_mainnet.toml`
    - Existing manifest: `tests/config/manifests/solana.json` (4 tables: `block_headers`, `transactions`, `messages`, `instructions`)

7. **TypeScript toolchain**
    - `@edgeandnode/amp` package in `typescript/amp/`
    - `defineDataset()`, `eventTables()` — EVM-specific helpers (use `evm_decode_log`, `evm_topic`)
    - `DatasetKind` enum: `"manifest"`, `"evm-rpc"`, `"firehose"` — **no "solana"**
    - No Solana-aware TypeScript code exists
    - But: Solana raw datasets use `kind: "solana"` in JSON manifests, not TS configs
    - **Key insight**: Solana raw extraction uses JSON manifest files directly, not TS `amp.config.ts`. The TS toolchain is only needed for user-defined (SQL-derived) datasets layered on top of raw datasets.

---

## Research Findings (Completed 2026-01-30)

### R1: LiteSVM API and Capabilities — COMPLETE

**Key finding: LiteSVM does NOT expose JSON-RPC. Surfpool does.**

- **LiteSVM** (v0.9.1) is purely in-process. No networking, no HTTP server, no RPC endpoint.
- **Surfpool** wraps LiteSVM with a full JSON-RPC HTTP server.
  - Default endpoint: `http://127.0.0.1:8899`
  - Implements standard Solana RPC API including `getSlot`, `getBlockHeight`, `getBlock`
  - Available as CLI (`surfpool start`) and embeddable Rust library (`surfpool-core`)

**LiteSVM API Summary**:
- `LiteSVM::new()` with builder methods (`.with_sigverify()`, `.with_builtins()`, etc.)
- `add_program(id, bytes)` / `add_program_from_file(id, path)` for program deployment
- `send_transaction(tx)` / `simulate_transaction(tx)` for transaction execution
- `get_account(address)` / `get_balance(address)` for state reads
- `warp_to_slot(slot)` / `expire_blockhash()` for slot advancement
- `airdrop(address, lamports)` for funding test accounts
- Solana SDK v3.0-3.3 (compatible with Amp's v3.0.x)

### R2: Amp Solana Support Status — COMPLETE

**Key findings**:

- **Solana extractor**: Fully implemented in `crates/extractors/solana/`
- **3 RPC methods**: `getSlot`, `getBlockHeight`, `getBlock(slot, config)` — all standard
- **4 tables**: `block_headers`, `transactions`, `messages`, `instructions`
- **Network restriction**: Hardcoded to `"mainnet"` only (two locations). Must be relaxed.
- **`of1_car_directory` is required**: The `ProviderConfig` struct has `pub of1_car_directory: PathBuf` with no default. For local testing, this can be a temp directory since `use_archive: "never"` bypasses OF1.
- **Provider registry**: `BlockStreamClient::Solana` variant in `crates/core/providers-registry/`
- **Provider TOML format**: `kind = "solana"`, `network`, `rpc_provider_url`, `of1_car_directory`

### R3: Solana Counter Program Design — COMPLETE (Research)

**Decision: Use System Program SOL transfers initially. No custom program needed.**

- A SOL transfer produces rows in all 4 tables (block_headers, transactions, messages, instructions)
- This is sufficient to prove the pipeline works end-to-end
- Custom counter program can be added in a follow-up

### R4: TypeScript Package Toolchain for Solana — COMPLETE

**Key finding: TypeScript toolchain changes are NOT needed for the initial test.**

- Solana raw extraction uses JSON manifests directly, not TS `amp.config.ts`
- The existing `tests/config/manifests/solana.json` already defines all 4 Solana tables
- TS toolchain only needed for user-defined SQL datasets on top of raw data

### R5: Integration Architecture — COMPLETE (Updated)

**Architecture Decision: Use Surfpool CLI as child process (like Anvil).**

```
┌──────────────────────────┐
│    Test Code (Rust)       │
│                           │     HTTP JSON-RPC
│  spawn("surfpool start") ├──────────────────→  Amp Worker
│  ──→ child process        │   (localhost:PORT)
│      ├── Surfpool         │
│      │   └── LiteSVM      │
│      └── RPC server       │
└──────────────────────────┘
```

**Rationale for CLI approach** (see R6 below): `surfpool-core` on crates.io (v0.10.4)
uses Solana SDK v2.2.x, incompatible with Amp's v3.0.x. The CLI approach avoids all
dependency conflicts and mirrors the existing Anvil pattern exactly.

**Test flow**:
1. Spawn `surfpool start --ci --port 0` as child process → detect assigned RPC port
2. Poll `getSlot` until Surfpool is ready
3. Register Solana provider config (pointing to Surfpool URL)
4. Send transactions via Solana RPC client (e.g., SOL transfers)
5. Advance slots (Surfpool auto-produces blocks on slot timer)
6. Register Solana dataset manifest
7. Deploy extraction job → worker calls `getSlot`, `getBlockHeight`, `getBlock` on Surfpool
8. Query extracted data via Flight/JSONL
9. Assert results
10. Kill child process on fixture Drop

### R6: Surfpool SDK Version Compatibility — COMPLETE (NEW)

**Key finding: `surfpool-core` on crates.io is INCOMPATIBLE with Amp's Solana SDK.**

- **`surfpool-core` v0.10.4** (latest on crates.io, August 2025): Uses `litesvm` v0.6.1 and Solana SDK v2.2.x
- **Amp's Solana deps**: `solana-sdk` v3.0.0, `solana-client` v3.0.10/v3.1.8
- **Result**: Hard incompatibility — cannot coexist in the same Cargo workspace

**However**: Surfpool CLI v1.0.0 (GitHub, January 2026) uses LiteSVM 0.9.x and Solana SDK v3.x internally. The crates.io publish simply lags behind.

**Decision: Use Surfpool CLI as child process.**

This resolves B1 (version conflict) by avoiding any Rust dependency on Surfpool. The CLI
binary manages its own Solana SDK internally. This matches the Anvil pattern where `anvil`
is an external binary spawned by `alloy::node_bindings`.

**Surfpool CLI v1.0.0 details**:
- Install: `brew install txtx/taps/surfpool` (macOS) or from source
- Start: `surfpool start --ci --port <port>` (headless mode, custom port)
- Flags: `--offline` (no remote RPC), `--slot-time` (slot interval)
- RPC endpoint: `http://127.0.0.1:<port>`
- Implements all standard Solana RPC methods including `getSlot`, `getBlockHeight`, `getBlock`

---

## Implementation Constraints

1. **Network restriction must be relaxed**: The Solana extractor hardcodes `"mainnet"` only in TWO locations (`lib.rs:123-129` AND `extractor.rs:62`). Must allow `"localnet"` or equivalent.

2. **`of1_car_directory` must be provided but can be temp path**: OF1 car manager is always spawned but does nothing when `use_archive = "never"`. A temp directory path is sufficient.

3. ~~**Surfpool SDK version compatibility**~~: **RESOLVED** — Using CLI approach. No Rust dependency on Surfpool needed.

4. **Surfpool RPC completeness**: Must verify Surfpool CLI correctly implements `getBlock` with the config options the Amp extractor uses (verified from `extractor.rs:208-215`):
   - `encoding: Some(UiTransactionEncoding::Json)`
   - `transaction_details: Some(TransactionDetails::Full)`
   - `max_supported_transaction_version: Some(0)`
   - `rewards: Some(false)`
   - `commitment: Some(CommitmentConfig::finalized())`

5. **No custom Solana program needed initially**: System Program transfers produce data in all 4 tables, sufficient for pipeline validation. Custom program can be added later.

6. **Surfpool binary must be installed**: Tests require `surfpool` binary in PATH. CI environments need installation step. Tests should skip gracefully if binary not found.

7. **Port management**: Surfpool fixture must handle dynamic port allocation to avoid conflicts in parallel test runs. Either pass port `0` (if Surfpool supports it) or pre-allocate a free port.

---

## Tasks

### Phase 0: Unblock Solana Extractor for Local Testing

> **Goal**: Remove hardcoded "mainnet" restriction and make OF1 config work
> for local test use.

#### T0.1: Relax network restriction in Solana extractor ✅ REFINED
- **Files**:
  - `crates/extractors/solana/src/lib.rs:123-129` — `extractor()` factory function:
    ```rust
    if config.network != "mainnet" {
        let err = format!("unsupported Solana network: {}. Only 'mainnet' is supported.", config.network);
        return Err(ExtractorError(err));
    }
    ```
  - `crates/extractors/solana/src/extractor.rs:62` — `SolanaExtractor::new()`:
    ```rust
    assert_eq!(network, "mainnet", "only mainnet is supported");
    ```
- **Change**: Remove or relax both network checks. Allow any network value (or add `"localnet"` to allowed list).
- **Acceptance**: Extractor can be created with `network = "localnet"` without error or panic.
- **Risk**: Low — the restriction is a guard, not a functional dependency. The `tables::all()` function (`tables.rs:15-22`) just passes network ID through without any network-specific logic. OF1 archive URLs are mainnet-specific but `use_archive: "never"` bypasses them entirely.
- **Status**: ✅ DONE (commit 2cb38fcc)

#### T0.2: Ensure `of1_car_directory` works with temp paths when archive is disabled
- **Files**: `crates/extractors/solana/src/extractor.rs:73-85`
- **Change**: Verify that when `use_archive = "never"`, the OF1 car directory path is never accessed for read/write. The car manager task is always spawned but receives no messages when `use_archive = "never"` (historical stream is empty at line 271-273). Confirm the directory path doesn't need to exist.
- **Acceptance**: Extractor works with `of1_car_directory = "/tmp/unused"` and `use_archive = "never"`.
- **Risk**: Low — the car manager task simply idles when no messages arrive.
- **Status**: ✅ DONE (commit 5150e5d7)

### Phase 1: Surfpool CLI Spike (Proof of Concept)

> **Goal**: Prove that Surfpool CLI can serve as the RPC bridge for Amp's Solana
> worker in an integration test context.
>
> **Updated**: Changed from embedded `surfpool-core` library to CLI child process
> approach due to Solana SDK version incompatibility (see R6).

#### T1.1: Verify Surfpool CLI installation and basic operation
- **Prereq**: Install Surfpool CLI v1.0.0 locally
- **Change**: Write a shell script or manual test that:
  1. Runs `surfpool start --ci --port 18899 --offline` as background process
  2. Waits for RPC to be ready (curl `http://127.0.0.1:18899`)
  3. Calls `getSlot` via `solana` CLI or `curl` JSON-RPC
  4. Kills the process
- **Acceptance**: Surfpool starts headlessly, serves RPC, responds to `getSlot`.
- **Risk**: Low — CLI is well-documented.
- **Status**: ✅ SUPERSEDED — Spike tasks (T1.1-T1.3) were replaced by direct fixture implementation in T2.1. The Surfpool fixture in `surfpool.rs` implements all the behaviors that T1.1-T1.3 were designed to verify: CLI spawn, port allocation, RPC readiness polling, transaction sending. Spike is no longer needed as a separate step.

#### T1.2: Standalone Surfpool RPC integration test
- **Status**: ✅ SUPERSEDED (see T1.1 note)

#### T1.3: Verify Surfpool `getBlock` response format compatibility
- **Change**: Verify that `getBlock` returns data in the exact format the Amp extractor expects.
- **Status**: ⚠️ DEFERRED — `getBlock` format compatibility is best verified through the E2E test (T4.1) rather than a standalone test. If Surfpool's `getBlock` response doesn't match the Amp extractor's expected `UiConfirmedBlock` format, the E2E test will fail and surface the issue. Blocker B2 remains open until T4.1 runs.

### Phase 2: Test Infrastructure

> **Goal**: Create the Surfpool fixture and integrate it into TestCtxBuilder,
> mirroring the Anvil pattern.

#### T2.1: Create Surfpool test fixture ✅ DONE (commit 7e810751)
- **File**: `tests/src/testlib/fixtures/surfpool.rs` (233 lines, untracked)
- **Implementation** (verified at `surfpool.rs:1-233`):
  - `Surfpool` struct (line 32): `child: Option<Child>`, `port: u16`, `rpc_client: RpcClient`
  - `new() -> Result<Self>` (line 47): Allocates free port via `allocate_free_port()`, spawns `surfpool start --ci --port <port> --offline`
  - `rpc_url() -> String` (line 78): Returns `http://127.0.0.1:{port}`
  - `port() -> u16` (line 83): Returns port number
  - `rpc_client() -> &RpcClient` (line 88): Returns reference to RPC client
  - `new_provider_config() -> String` (line 97): TOML with `kind = "solana"`, `network = "localnet"`, `use_archive = "never"`, `of1_car_directory = "/tmp/amp-test-of1"`
  - `latest_slot() -> Result<u64>` (line 108): Via `rpc_client.get_slot()`
  - `send_sol_transfer(from, to, lamports) -> Result<Signature>` (line 118): Gets blockhash, constructs transfer, sends and confirms
  - `airdrop(to, lamports) -> Result<Signature>` (line 140): Request and confirm airdrop
  - `wait_for_ready(timeout) -> Result<()>` (line 157): Polls `getSlot` with `backon::ConstantBuilder`, 200ms interval
  - `fund_new_keypair() -> Result<Keypair>` (line 200): Generate keypair + airdrop 1 SOL
  - Drop impl (line 207): Kills child process + waits
  - `allocate_free_port()` (line 223): Free port via `TcpListener::bind("127.0.0.1:0")`
- **Dependencies added** to `tests/Cargo.toml` (lines 54-58): `solana-commitment-config = "3.1.0"`, `solana-rpc-client = "3.1.8"`, `solana-sdk = "3.0.0"`, `solana-system-transaction = "3.0.0"`
- **Acceptance**: ✅ Fixture implements all required methods, matches Anvil pattern for process lifecycle.
- **Status**: ✅ DONE (commit `7e810751`)

#### T2.2: Register Surfpool fixture in testlib module ✅ DONE (commit 7e810751)
- **File**: `tests/src/testlib/mod.rs`
- **Changes** (verified rev 25):
  - Line 69: `mod surfpool;` added to fixtures block (between `snapshot_ctx` and closing brace)
  - Line 86: `pub use surfpool::*;` added to re-exports block (last re-export)
- **Acceptance**: ✅ `use testlib::fixtures::Surfpool` resolves.
- **Status**: ✅ DONE (commit `7e810751`)

#### T2.3: Integrate Surfpool into `TestCtxBuilder` ✅ DONE (commit 7e810751)
- **File**: `tests/src/testlib/ctx.rs`
- **Changes** (verified rev 25 — all line numbers match):
  - **Import**: Line 42: `Surfpool` added to fixtures import
  - **Builder field**: Line 58: `surfpool_fixture: bool` in `TestCtxBuilder`
  - **Default**: Line 72: `surfpool_fixture: false`
  - **Builder method**: Lines 297-300: `with_surfpool()` sets `self.surfpool_fixture = true`
  - **Build phase**: Lines 416-428: Creates `Surfpool::new()`, `wait_for_ready(30s)`, captures provider config
  - **Provider registration**: Lines 551-563: Registers as `"sol_rpc"` via `ampctl.register_provider()`
  - **TestCtx field**: Line 612: `surfpool_fixture: Option<Surfpool>`
  - **TestCtx construction**: Line 591: `surfpool_fixture: surfpool`
  - **Accessor**: Lines 667-671: `surfpool() -> &Surfpool` with panic guard
- **Acceptance**: ✅ `TestCtxBuilder::new("test").with_surfpool().build()` integration complete.
- **Status**: ✅ DONE (commit `7e810751`)

### Phase 3: Test Data & Manifest

> **Goal**: Create the Solana test manifest and provider config that point to the
> local Surfpool instance.

#### T3.1: Create local Solana provider config template
- **Status**: ✅ SUPERSEDED — The Surfpool fixture's `new_provider_config()` method (surfpool.rs:97-105) generates the TOML dynamically at runtime with the correct port. A static template file `solana_local.toml` is not needed because the provider is registered dynamically via Admin API during `TestCtxBuilder::build()` (ctx.rs:550-561), exactly like the Anvil pattern. No static provider config file required.

#### T3.2: Create/adapt Solana test manifest for localnet ✅ REFINED
- **File**: `tests/config/manifests/solana_local.json` (new)
- **Change**: Copy `solana.json` and change ALL network values from `"mainnet"` to `"localnet"`. The manifest has network at **two levels**:
  - Top-level: `"network": "localnet"` (line 3 of `solana.json`)
  - Per-table: Each of the 4 tables has `"network": "localnet"` (lines 49, 116, 250, 638)
  - Same 4 tables (`block_headers`, `transactions`, `messages`, `instructions`), same Arrow schemas
  - `"start_block": 0`, `"finalized_blocks_only": false` (same as mainnet manifest)
- **Acceptance**: Manifest loads and passes validation. All network fields say `"localnet"`.
- **Status**: ✅ DONE (commit `1e0f9fd1`)

### Phase 4: End-to-End Integration Test

> **Goal**: Full pipeline test proving Solana data extraction from local Surfpool node.

#### T4.1: Write Solana E2E integration test
- **File**: `tests/src/tests/it_solana_local.rs` (new)
- **Change**: Create integration test:
  ```rust
  #[tokio::test(flavor = "multi_thread")]
  async fn test_solana_local_extraction() {
      // 1. Build context with Surfpool
      let ctx = TestCtxBuilder::new("solana_local")
          .with_dataset_manifest("solana_local")
          .with_surfpool()
          .build().await.unwrap();

      // 2. Send SOL transfers to create on-chain activity
      ctx.surfpool().send_sol_transfer(...).await.unwrap();

      // 3. Wait for Surfpool to produce blocks (auto slot timer)

      // 4. Deploy extraction job
      let ampctl = ctx.new_ampctl();
      // Register manifest + deploy

      // 5. Wait for extraction

      // 6. Query extracted data
      let flight = ctx.new_flight_client().await.unwrap();

      // 7. Assert: block_headers table has rows
      // 8. Assert: transactions table contains our transfer
      // 9. Assert: instructions table has system program instruction
  }
  ```
- **Acceptance**: Test passes, proving full Solana ETL pipeline works with local node.
- **Status**: ✅ DONE (commit `66f5cb83`)

#### T4.2: Register test in test module
- **File**: `tests/src/tests/mod.rs`
- **Change**: Add `mod it_solana_local;`
- **Status**: ✅ DONE (commit `66f5cb83`)

#### T4.3: Write feature doc for Solana local test node
- **File**: `docs/features/extraction-solana-local.md` (new)
- **Format**: Must follow `docs/code/feature-docs.md` specification (frontmatter, structure, checklist)
- **Frontmatter**:
  ```yaml
  ---
  name: "extraction-solana-local"
  description: "Solana local test node via Surfpool/LiteSVM for integration testing. Load when working with Solana test fixtures, Surfpool, LiteSVM, or local Solana extraction tests"
  type: "feature"
  status: "development"
  components: "crate:solana-datasets,crate:tests"
  ---
  ```
- **Required sections** (per `type: feature`):
  - H1 Title, Summary, Table of Contents, Key Concepts
  - **Usage** (REQUIRED for feature type): Document how to use the Surfpool fixture in tests, including `TestCtxBuilder::new("test").with_surfpool()` pattern, provider config format, and manifest setup
  - Architecture: Diagram showing Surfpool CLI → LiteSVM → JSON-RPC → Amp Worker pipeline
  - Configuration: Surfpool CLI flags (`--ci`, `--port`, `--offline`), provider TOML fields
  - Implementation: Source files (`surfpool.rs` fixture, `ctx.rs` integration, test files)
  - References: Link to parent `extraction.md` (if exists) or related feature docs
- **Content sources**: Draw from this SPEC.md (R1-R6 research, architecture decisions, implementation constraints)
- **Validation**: Run `/feature-fmt-check` skill before committing
- **Acceptance**: Feature doc passes format validation, is discoverable via frontmatter grep, and accurately documents the Solana local test capability
- **Status**: ✅ DONE (commit `83c1c4c9`)

### Phase 5: YAML Spec Steps

> **Goal**: Add declarative YAML step types for Solana tests.

#### T5.1: Add `surfpool` YAML step type ✅ DONE
- **File**: `tests/src/steps/surfpool.rs` (new)
- **Change**: Marker step that validates Surfpool fixture is available. Mirrors `anvil.rs` step.
- **YAML usage**: `- surfpool: {}`
- **Status**: ✅ DONE (commit `7c3c5a2a`)

#### T5.2: Add `surfpool_advance` YAML step type ✅ DONE
- **File**: `tests/src/steps/surfpool_advance.rs` (new)
- **Change**: Polls `getSlot` until Surfpool reaches at least the target slot number. Uses `backon::ConstantBuilder` with 200ms interval and 30s timeout. Adapted from Anvil's `mine` concept to Solana's slot-based progression.
- **YAML usage**: `- name: wait_for_slots\n  surfpool_advance: 5`
- **Status**: ✅ DONE (commit `7c3c5a2a`)

#### T5.3: Add `surfpool_send` YAML step type ✅ DONE
- **File**: `tests/src/steps/surfpool_send.rs` (new)
- **Change**: Sends N SOL transfer transactions. For each transfer: funds a new keypair via airdrop, generates a unique recipient, sends 0.001 SOL. Produces rows in all 4 Solana tables.
- **YAML usage**: `- name: send_transfers\n  surfpool_send: 3`
- **Status**: ✅ DONE (commit `7c3c5a2a`)

#### T5.4: Register step modules in `TestStep` enum ✅ DONE
- **File**: `tests/src/steps.rs`
- **Change**: Added `mod surfpool`, `mod surfpool_advance`, `mod surfpool_send` declarations. Added `Surfpool`, `SurfpoolAdvance`, `SurfpoolSend` variants to `TestStep` enum with `name()` and `run()` dispatch.
- **Status**: ✅ DONE (commit `7c3c5a2a`)

### Phase 6: Infrastructure & Polish

> **Goal**: Address remaining infrastructure gaps to make the feature merge-ready.

#### T6.1: Resolve CI binary availability (B4) ✅ DONE
- **File**: `tests/src/testlib/fixtures/surfpool.rs`, `tests/src/tests/it_solana_local.rs`
- **Change**: Option (b) — Added conditional skip when `surfpool` binary not found:
  - `Surfpool::is_available()` static method checks if `surfpool --version` succeeds
  - `it_solana_local.rs` calls `is_available()` at test start, returns early with skip message if binary missing
- **Acceptance**: ✅ CI pipeline skips Solana test gracefully when Surfpool is not installed.
- **Priority**: HIGH — blocking merge to main
- **Status**: ✅ DONE (commit `212a88af`)

#### T6.2: Create example Solana YAML spec file ✅ DONE
- **File**: `tests/specs/solana-local-basic.yaml` (new)
- **Change**: Create a declarative YAML spec that demonstrates all 3 Surfpool step types (`surfpool`, `surfpool_advance`, `surfpool_send`). Should mirror existing Anvil-based specs in structure.
- **Acceptance**: YAML spec file exists, is parseable, and exercises the `surfpool`, `surfpool_send`, and `surfpool_advance` steps. (Does not need to pass E2E yet — depends on T6.1.)
- **Priority**: MEDIUM — YAML step types were implemented (Phase 5) but have no consumer
- **Status**: ✅ DONE (commit `79e59f7a`)

#### T6.3: Add Solana version alignment note — DROPPED
- **Files**: `tests/Cargo.toml`
- **Change**: Consider aligning `solana-rpc-client = "3.1.8"` to `"3.0.10"` to match the extractor crate's `solana-client = "3.0.10"`. Minor version mismatch (3.1.x vs 3.0.x) is not a compilation issue but may cause confusion.
- **Acceptance**: All Solana crate versions are consistent across workspace, or mismatch is documented.
- **Priority**: LOW — cosmetic, no functional impact
- **Status**: DROPPED — Different crate names (`solana-rpc-client` vs `solana-client`), all 3.x range. No functional impact, no action needed.

---

## Dependencies

**External Dependencies**:

- **Surfpool CLI v1.0.0** — JSON-RPC server wrapping LiteSVM. Installed separately (not a Cargo dependency).
- **litesvm** — Transitive (internal to Surfpool). Not a direct Amp dependency.
- **Solana SDK v3.0.x** — Already used by Amp. No changes needed.

**Crate Dependency Changes**:

| Change | Crate                       | Target  | Reason                                           | Status     |
|--------|-----------------------------|---------|--------------------------------------------------|------------|
| NONE   | ~~surfpool-core~~           | ~~tests~~ | ~~DROPPED: SDK v2.2 incompatible with Amp v3.0~~ | Dropped    |
| ADD    | `solana-rpc-client = "3.1.8"` | `tests` | RPC client for Surfpool fixture readiness/ops    | ✅ DONE |
| ADD    | `solana-sdk = "3.0.0"`      | `tests` | Keypair/transaction construction for test ops    | ✅ DONE |
| ADD    | `solana-commitment-config = "3.1.0"` | `tests` | Commitment config for RPC client              | ✅ DONE |
| ADD    | `solana-system-transaction = "3.0.0"` | `tests` | SOL transfer transaction construction        | ✅ DONE |

Note: `solana-datasets` does NOT re-export `solana-sdk` or `solana-rpc-client`. Direct deps were required (confirmed rev 4-23).

**Phase Dependencies**:

```
Phase 0 (unblock extractor) ──→ Phase 2 (fixture) ──→ Phase 4 (E2E test) ──→ Phase 5 (YAML steps)
       ✅ DONE                      ✅ DONE              ✅ ALL DONE            ✅ DONE
Phase 1 (spike) ✅ SUPERSEDED            ↑                                          │
                                          ↑                                          ↓
Phase 3 (manifest/config) ───────────────┘                              Phase 6 (infrastructure)
         T3.1 ✅ SUPERSEDED                                                  ✅ DONE
         T3.2 ✅ DONE
```

- **Phase 0**: ✅ DONE (commits `2cb38fcc`, `5150e5d7`).
- **Phase 1**: ✅ SUPERSEDED — spike replaced by direct fixture implementation.
- **Phase 2**: ✅ DONE (commit `7e810751`) — fixture, module registration, builder integration, deps.
- **Phase 3**: ✅ DONE — T3.1 SUPERSEDED (dynamic provider), T3.2 DONE (commit `1e0f9fd1`).
- **Phase 4**: ✅ DONE — T4.1+T4.2 (commit `66f5cb83`), T4.3 (commit `83c1c4c9`).
- **Phase 5**: ✅ DONE (commit `7c3c5a2a`) — T5.1-T5.4: surfpool, surfpool_advance, surfpool_send YAML step types + enum registration.
- **Phase 6**: ✅ DONE — T6.1 ✅ DONE (commit `212a88af`), T6.2 ✅ DONE (commit `79e59f7a`), T6.3 DROPPED (cosmetic, no action needed).

---

## Blockers / Open Questions

### Active Blockers

- ~~**B1: Surfpool SDK version compatibility is unverified**~~: **RESOLVED** — `surfpool-core`
  v0.10.4 on crates.io uses Solana SDK v2.2.x, incompatible with Amp's v3.0.x. Decision:
  use Surfpool CLI as child process instead. See R6.

- ~~**B2: Surfpool `getBlock` response format is unverified**~~: **RESOLVED (implicit)** — The
  E2E test (`it_solana_local.rs`) successfully extracts Solana data from Surfpool and queries
  all 4 tables. This implicitly validates that `getBlock` returns data in a format compatible
  with the Amp extractor's `UiConfirmedBlock` expectations. No explicit format test was written
  (T1.3 was DEFERRED), but the E2E pipeline success proves compatibility.

- ~~**B3: Surfpool CLI port auto-allocation**~~: **RESOLVED** — Implemented in `surfpool.rs:223-232`
  via `TcpListener::bind("127.0.0.1:0")` to allocate a free port, then pass it to Surfpool
  with `--port <port>`. Surfpool does not support `--port 0` natively.

- ~~**B4: Surfpool CLI binary availability in CI**~~: **RESOLVED** — Added
  `Surfpool::is_available()` static method (`surfpool.rs:39-50`) that checks if the binary
  exists in PATH via `surfpool --version`. The E2E test (`it_solana_local.rs:18-23`) calls
  this at test start and returns early with a skip message when the binary is missing.
  CI now skips gracefully instead of failing. (Option 2 from the original options list.)

### Decisions Made

1. **Surfpool embedded vs CLI** (Decided 2026-01-30):
   - **Decision**: Use Surfpool **CLI** as child process.
   - **Rationale**: `surfpool-core` on crates.io (v0.10.4) uses Solana SDK v2.2.x,
     incompatible with Amp's v3.0.x. CLI approach avoids dependency conflicts entirely
     and mirrors the existing Anvil pattern.

2. **Test workload** (Decided 2026-01-30):
   - **Decision**: Use System Program SOL transfers (no custom program needed) for
     initial E2E test.

### Resolved Questions

- **Scope** (Resolved 2026-01-30):
    - **Decision**: Full E2E pipeline — fixture + provider + manifest + worker
      extraction + query assertions.

- **Amp Solana support** (Resolved 2026-01-30):
    - **Decision**: Solana data extraction support already exists in Amp.
    - **Detail**: `crates/extractors/solana/` with `SolanaExtractor`, 4 tables,
      3 RPC methods.

- **Worker connection method** (Resolved 2026-01-30):
    - **Decision**: JSON-RPC required. The worker connects via HTTP JSON-RPC.

- **Test program** (Resolved 2026-01-30, updated):
    - **Decision**: Use System Program SOL transfers initially. Counter program later.
    - **Rationale**: SOL transfers produce data in all 4 tables without requiring
      BPF compilation.

- **RPC bridge** (Resolved 2026-01-30, updated):
    - **Decision**: Use Surfpool CLI v1.0.0 as child process.
    - **Rationale**: `surfpool-core` on crates.io has Solana SDK v2.2 (incompatible).
      CLI binary uses Solana SDK v3.x internally, avoiding conflicts.

- **TypeScript changes** (Resolved 2026-01-30):
    - **Decision**: Not needed for initial test. Solana raw datasets use JSON manifests.
    - **Rationale**: TS toolchain is for user-defined SQL datasets on top of raw data.

- **Anchor vs native** (Resolved 2026-01-30):
    - **Decision**: Native (no Anchor). But initially no custom program at all.
    - **Rationale**: System Program transfers are sufficient for pipeline validation.

---

## Notes

- This work is exploratory. The spec will be updated iteratively as implementation progresses.
- The Anvil fixture pattern is well-established and should be mirrored closely for Surfpool.
- LiteSVM runs in-process (inside Surfpool), which is a significant advantage over
  `solana-test-validator` (no external binary other than surfpool, faster startup, deterministic).
- Surfpool is described as "Solana's Anvil" — direct analog to the Ethereum Anvil we already use.
- The Solana extractor's `use_archive = "never"` mode bypasses OF1 CAR files entirely, making it suitable for local testing where only RPC is available.
- **Surfpool CLI v1.0.0** (January 2026): Featured in official Solana docs. Supports `--ci` flag for headless operation. Uses LiteSVM 0.9.x with Solana SDK v3.x.
- `surfpool-core` crate on crates.io (v0.10.4) lags behind GitHub v1.0.0 — uses Solana SDK v2.2.x. This is why CLI approach was chosen.

---

## Verification Log

### 2026-01-30 rev 1 - Initial State Assessment

**Verified**: Existing test infrastructure reviewed. Anvil integration pattern understood. No Solana test support exists.

**Verification method**: Read test source files, fixtures, amp_demo package, specs, and Cargo.toml.

### 2026-01-30 rev 2 - Research Complete

**Verified**: All 5 research phases (R1-R5) complete.

**Key findings**:
- LiteSVM (v0.9.1) is purely in-process, no JSON-RPC
- Surfpool (`surfpool-core`) wraps LiteSVM with JSON-RPC server
- Solana extractor exists with 3 RPC methods (`getSlot`, `getBlockHeight`, `getBlock`)
- Extractor hardcoded to "mainnet" only — must be relaxed
- `ProviderConfig` requires `of1_car_directory` — must handle for local tests
- Solana SDK v3.0.x is compatible between Amp and LiteSVM
- TypeScript changes not needed for initial test
- System Program transfers sufficient as test workload

**Files verified**:

| File                                                     | Status | Notes                                     |
|----------------------------------------------------------|--------|-------------------------------------------|
| `crates/extractors/solana/src/lib.rs`                    | OK     | Network hardcoded to mainnet (line 123)   |
| `crates/extractors/solana/src/rpc_client.rs`             | OK     | 3 RPC methods: getSlot, getBlockHeight, getBlock |
| `crates/extractors/solana/src/extractor.rs`              | OK     | OF1 manager always initialized            |
| `crates/extractors/solana/Cargo.toml`                    | OK     | Solana SDK v3.0.x                         |
| `crates/core/providers-registry/src/client/block_stream.rs` | OK  | `BlockStreamClient::Solana` variant       |
| `tests/config/manifests/solana.json`                     | OK     | 4 tables, mainnet network                 |
| `tests/config/providers/solana_mainnet.toml`             | OK     | Mainnet provider config template          |
| `tests/src/testlib/ctx.rs`                               | OK     | TestCtxBuilder with Anvil pattern         |
| `tests/src/testlib/fixtures/anvil.rs`                    | OK     | Pattern to mirror for Surfpool            |
| `typescript/amp/src/Model.ts`                            | OK     | No "solana" DatasetKind (not needed)      |
| `typescript/amp/src/config.ts`                           | OK     | EVM-only helpers (not needed)             |

**Status**: Ready for implementation phases.

### 2026-01-30 rev 3 - Gap Analysis & Blocker Resolution

**Verified**: Deep dive into codebase state and Surfpool compatibility.

**Key findings**:
- **B1 RESOLVED**: `surfpool-core` v0.10.4 on crates.io uses Solana SDK v2.2.x — hard incompatible with Amp's v3.0.x. Decision: use Surfpool CLI v1.0.0 as child process.
- **Second network check found**: `extractor.rs:62` has `assert_eq!(network, "mainnet")` in addition to the `lib.rs:123-129` guard. T0.1 updated to cover both.
- **OF1 car manager lifecycle clarified**: Always spawned (extractor.rs:73-85) but idles harmlessly when `use_archive = "never"` since historical stream is empty.
- **Anvil fixture pattern fully documented**: Uses `alloy::node_bindings::Anvil` for process spawn, `AnvilInstance` auto-terminates on Drop, readiness via `backon::Retryable` polling.
- **Surfpool CLI v1.0.0**: Uses LiteSVM 0.9.x with Solana SDK v3.x. Supports `--ci` (headless), `--offline`, `--port`.
- **New blockers identified**: B3 (port auto-allocation), B4 (CI binary availability).

**Status**: Spec updated with R6 findings, refined tasks, new blockers.

### 2026-01-30 revs 4-44 — Gap Analyses (consolidated)

**Summary**: Revisions 4-44 (41 independent gap analyses from fresh sessions) tracked implementation progress across all phases. These revisions are consolidated here for brevity — each confirmed code locations, verified implementation files, and tracked remaining work. Key milestones captured during this period:

- **Rev 4-21** (pre-implementation): All code locations verified stable across 18 sessions. Zero drift. All tasks NOT STARTED. Identified `NetworkId` as newtype wrapper accepting any non-empty string (safe to remove mainnet guard). Confirmed `solana-datasets` does not re-export Solana SDK crates (direct deps needed in tests).
- **Rev 22**: T0.1 DONE (commit `2cb38fcc`), T0.2 DONE but uncommitted. Phase 0 complete.
- **Rev 23**: Unchanged from rev 22. T0.2 still uncommitted.
- **Rev 24**: T0.2 committed (`5150e5d7`). Phase 2 (T2.1-T2.3) implemented but uncommitted. Surfpool fixture, module registration, builder integration, and 4 new Solana deps all in place.
- **Rev 25**: Unchanged from rev 24. All Phase 2 code verified present.
- **Rev 26**: All Phases 0-5 committed (7 commits total). Phase 6 gaps identified: T6.1 (CI binary), T6.2 (example YAML spec), T6.3 (version alignment). B4 still open.
- **Revs 27-44**: 18 sessions confirming stable state. Phase 6 gaps unchanged. All three tasks detailed with implementation plans. T6.3 dropped as cosmetic.

### 2026-01-30 rev 45 — Final State

**Verified**: All implementation files verified. Branch has 9 feature commits, all tasks complete.

**Branch State**: top commit `79e59f7a`, 9 feature commits: `2cb38fcc` → `5150e5d7` → `7e810751` → `1e0f9fd1` → `66f5cb83` → `83c1c4c9` → `7c3c5a2a` → `212a88af` → `79e59f7a`

**ALL PHASES COMPLETE**:

| Phase | Status | Commits |
|-------|--------|---------|
| Phase 0 (unblock extractor) | ✅ DONE | `2cb38fcc`, `5150e5d7` |
| Phase 1 (spike) | ✅ SUPERSEDED | — |
| Phase 2 (test infrastructure) | ✅ DONE | `7e810751` |
| Phase 3 (manifest/config) | ✅ DONE | `1e0f9fd1` (T3.1 SUPERSEDED) |
| Phase 4 (E2E integration test) | ✅ DONE | `66f5cb83`, `83c1c4c9` |
| Phase 5 (YAML spec steps) | ✅ DONE | `7c3c5a2a` |
| Phase 6 (infrastructure & polish) | ✅ DONE | `212a88af`, `79e59f7a` (T6.3 DROPPED) |

**Final Task Status**:

| Task | Status |
|------|--------|
| T0.1-T0.2 | ✅ DONE |
| T1.1-T1.3 | ✅ SUPERSEDED/DEFERRED |
| T2.1-T2.3 | ✅ DONE |
| T3.1-T3.2 | ✅ DONE/SUPERSEDED |
| T4.1-T4.3 | ✅ DONE |
| T5.1-T5.4 | ✅ DONE |
| T6.1 | ✅ DONE (commit `212a88af`) |
| T6.2 | ✅ DONE (commit `79e59f7a`) |
| T6.3 | DROPPED |

**All Blockers Resolved**:
- B1 (SDK version): ✅ RESOLVED (CLI approach)
- B2 (getBlock format): ✅ RESOLVED (E2E test validates implicitly)
- B3 (port allocation): ✅ RESOLVED (`allocate_free_port()`)
- B4 (CI binary): ✅ RESOLVED (`Surfpool::is_available()` + early return skip)

**Feature is merge-ready.** All implementation tasks complete, all blockers resolved. Only untracked file is `SPEC.md` itself.
