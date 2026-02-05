---
name: "extractors"
description: "Data extraction crate patterns: BlockStreamer trait, Dataset trait, provider config, row building. Load when working on extractor crates in crates/extractors/"
type: arch
scope: "global"
---

# Extractor Patterns

**MANDATORY for ALL extractor crates in `crates/extractors/`**

## PURPOSE

This document establishes the common architecture for blockchain data extraction crates. All extractors follow the same structural patterns to ensure consistency across data sources (EVM RPC, Firehose, Solana).

## TABLE OF CONTENTS

1. [Core Traits](#core-traits)
2. [Dataset Kind Pattern](#dataset-kind-pattern)
3. [Manifest Structure](#manifest-structure)
4. [Provider Config Pattern](#provider-config-pattern)
5. [Factory Function Pattern](#factory-function-pattern)
6. [Table Definition and Row Building](#table-definition-and-row-building)
7. [Metrics Registry Pattern](#metrics-registry-pattern)
8. [Error Handling](#error-handling)
9. [Source File Layout](#source-file-layout)
10. [Checklist](#checklist)
11. [Rationale](#rationale)

## CORE TRAITS

### BlockStreamer Trait

Defined in `crates/core/datasets-raw/src/client.rs`. Every extractor client must implement this trait.

```rust
pub trait BlockStreamer: Clone + 'static {
    fn block_stream(
        self,
        start: BlockNum,
        end: BlockNum,
    ) -> impl Future<Output = impl Stream<Item = Result<Rows, BlockStreamError>> + Send> + Send;

    fn latest_block(
        &mut self,
        finalized: bool,
    ) -> impl Future<Output = Result<Option<BlockNum>, LatestBlockError>> + Send;

    fn wait_for_cleanup(self) -> impl Future<Output = Result<(), CleanupError>> + Send;

    fn provider_name(&self) -> &str;
}
```

**Methods:**
- `block_stream()` — async stream of `Rows` from start to end block (inclusive)
- `latest_block()` — fetches the latest (optionally finalized) block number
- `wait_for_cleanup()` — releases background tasks, connections, and resources
- `provider_name()` — returns the provider instance name

**Error types** are all `Box<dyn std::error::Error + Sync + Send + 'static>`.

**Implementations:**
- `evm_rpc_datasets::JsonRpcClient`
- `solana_datasets::SolanaExtractor`
- `firehose_datasets::Client`

### Dataset Trait

Defined in `crates/core/datasets-common/src/dataset.rs`. Each extractor provides a concrete `Dataset` struct that implements this trait.

```rust
pub trait Dataset: DowncastSync {
    fn reference(&self) -> &HashReference;
    fn kind(&self) -> DatasetKindStr;
    fn tables(&self) -> &[Table];
    fn start_block(&self) -> Option<BlockNum>;
    fn finalized_blocks_only(&self) -> bool;
}
```

Supports downcasting via `downcast_rs` (`is::<T>()`, `downcast_ref::<T>()`, `downcast_arc::<T>()`).

## DATASET KIND PATTERN

Each extractor defines a zero-sized type representing its dataset kind. The pattern is identical across all extractors.

```rust
const DATASET_KIND: &str = "evm-rpc";  // Canonical string identifier

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct EvmRpcDatasetKind;

impl EvmRpcDatasetKind {
    #[inline]
    pub const fn as_str(self) -> &'static str {
        DATASET_KIND
    }
}
```

**Required trait implementations:**
- `FromStr` with custom error type (e.g., `EvmRpcDatasetKindError`)
- `Display` — returns the const string
- `Serialize` / `Deserialize` — round-trips through the const string
- `From<Kind> for DatasetKindStr` — conversion to common type
- `PartialEq` impls for `str`, `&str`, `String`, `DatasetKindStr`

**Error type:**
```rust
#[derive(Debug, thiserror::Error)]
#[error("invalid dataset kind: {}, expected: {}", .0, DATASET_KIND)]
pub struct EvmRpcDatasetKindError(String);
```

**Existing kinds:**
| Crate | Type | Const |
|---|---|---|
| evm-rpc | `EvmRpcDatasetKind` | `"evm-rpc"` |
| solana | `SolanaDatasetKind` | `"solana"` |
| firehose | `FirehoseDatasetKind` | `"firehose"` |

## MANIFEST STRUCTURE

Each extractor defines a `Manifest` struct with common fields:

```rust
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct Manifest {
    pub kind: EvmRpcDatasetKind,          // Dataset kind (zero-sized type)
    pub network: NetworkId,               // e.g., "mainnet", "anvil"
    #[serde(default)]
    pub start_block: BlockNum,            // Defaults to 0
    #[serde(default)]
    pub finalized_blocks_only: bool,      // Defaults to false
    pub tables: BTreeMap<String, Table>,  // Table name -> definition
}
```

**Common fields across all extractors:**

| Field | Type | Default | Description |
|---|---|---|---|
| `kind` | Zero-sized type | required | Dataset kind identifier |
| `network` | `NetworkId` | required | Target blockchain network |
| `start_block` | `BlockNum` | `0` | First block to extract |
| `finalized_blocks_only` | `bool` | `false` | Wait for finalization |
| `tables` | `BTreeMap<String, Table>` | required | Table definitions |

## PROVIDER CONFIG PATTERN

Each extractor defines a `ProviderConfig` with common base fields plus extractor-specific options:

```rust
#[serde_as]
#[derive(Debug, serde::Deserialize)]
pub struct ProviderConfig {
    // Common fields (all extractors)
    pub name: String,
    pub kind: EvmRpcDatasetKind,
    pub network: NetworkId,
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub url: Url,

    // Extractor-specific fields
    pub concurrent_request_limit: Option<u16>,
    #[serde(default)]
    pub rpc_batch_size: usize,
    pub rate_limit_per_minute: Option<NonZeroU32>,
}
```

**Common fields:**

| Field | Type | Description |
|---|---|---|
| `name` | `String` | Provider instance name |
| `kind` | Zero-sized type | Must match extractor kind |
| `network` | `NetworkId` | Target network |
| `url` | `Url` or `String` | Endpoint URL |

**Extractor-specific fields (examples):**
- EVM-RPC: `concurrent_request_limit`, `rpc_batch_size`, `rate_limit_per_minute`, `fetch_receipts_per_tx`
- Solana: `rpc_provider_url`, `of1_car_directory`, `keep_of1_car_files`, `use_archive`
- Firehose: `token` (authentication)

## FACTORY FUNCTION PATTERN

Each extractor exposes a factory function that creates a client/extractor from config:

```rust
// ✅ EVM-RPC pattern — async, returns client
pub async fn client(
    config: ProviderConfig,
    meter: Option<&monitoring::telemetry::metrics::Meter>,
) -> Result<JsonRpcClient, ProviderError> {
    // Validate config, select transport, create client
}

// ✅ Solana pattern — sync, returns extractor
pub fn extractor(
    config: ProviderConfig,
    meter: Option<&monitoring::telemetry::metrics::Meter>,
) -> Result<SolanaExtractor, ExtractorError> {
    // Validate network, create extractor
}
```

**Convention:**
- Takes `ProviderConfig` + optional `&Meter` for metrics
- Returns the `BlockStreamer` implementor or an error
- Validates configuration (URL scheme, network)
- Creates `MetricsRegistry` if meter is provided

Each extractor also provides a `dataset()` function to convert a manifest into a `Dataset` trait object:

```rust
pub fn dataset(reference: HashReference, manifest: Manifest) -> Dataset {
    Dataset {
        reference,
        kind: manifest.kind,
        start_block: Some(manifest.start_block),
        finalized_blocks_only: manifest.finalized_blocks_only,
        tables: tables::all(&manifest.network),
    }
}
```

## TABLE DEFINITION AND ROW BUILDING

### Tables Module

Each extractor has a `tables/` module with an `all()` function returning all table definitions:

```rust
// tables/mod.rs
pub fn all(network: &NetworkId) -> Vec<datasets_common::dataset::Table> {
    vec![
        blocks::table(network.clone()),
        transactions::table(network.clone()),
        logs::table(network.clone()),
    ]
}
```

### Individual Table Definition

Each table file exports a `table()` function and a const `TABLE_NAME`:

```rust
pub const TABLE_NAME: &str = "transactions";

pub fn table(network: NetworkId) -> Table {
    let name = TABLE_NAME.parse().expect("table name is valid");
    Table::new(
        name,
        SCHEMA.clone(),
        network,
        vec!["block_num".to_string(), "timestamp".to_string()],
    )
}
```

### Row Builder Pattern

Each table defines a row builder with typed Arrow array builders:

```rust
pub(crate) struct TransactionRowsBuilder {
    special_block_num: UInt64Builder,
    block_hash: Bytes32ArrayBuilder,
    // ... one builder per column
}

impl TransactionRowsBuilder {
    pub(crate) fn with_capacity(count: usize, total_input_size: usize) -> Self {
        // Pre-allocate all builders
    }

    pub(crate) fn append(&mut self, tx: &Transaction) {
        // Append values from source data
    }

    pub(crate) fn build(self, range: BlockRange) -> Result<TableRows, TableRowError> {
        // Finalize columns into Arrow RecordBatch via TableRows
    }
}
```

**Output types (from `datasets-raw`):**

- `Rows` — container for multiple `TableRows`, all sharing the same `BlockRange`
- `TableRows` — single table's data as an Arrow `RecordBatch` with block range metadata

## METRICS REGISTRY PATTERN

Each extractor defines a `MetricsRegistry` struct:

```rust
#[derive(Debug, Clone)]
pub struct MetricsRegistry {
    pub rpc_requests: telemetry::metrics::Counter,
    pub rpc_request_duration: telemetry::metrics::Histogram<f64>,
    pub rpc_errors: telemetry::metrics::Counter,
    // ... extractor-specific metrics
}

impl MetricsRegistry {
    pub fn new(meter: &telemetry::metrics::Meter) -> Self {
        Self {
            rpc_requests: telemetry::metrics::Counter::new(
                meter,
                "evm_rpc_requests_total",
                "Total number of EVM RPC requests",
            ),
            // ... initialize all metrics
        }
    }

    pub(crate) fn record_single_request(&self, duration_millis: f64, provider: &str, network: &NetworkId, method: &str) {
        let kv_pairs = [
            telemetry::metrics::KeyValue::new("provider", provider.to_string()),
            telemetry::metrics::KeyValue::new("network", network.to_string()),
            telemetry::metrics::KeyValue::new("method", method.to_string()),
        ];
        self.rpc_requests.inc_with_kvs(&kv_pairs);
        self.rpc_request_duration.record_with_kvs(duration_millis, &kv_pairs);
    }
}
```

**Conventions:**
- `Counter` for total counts (requests, errors)
- `Histogram<f64>` for durations (milliseconds)
- `Histogram<u64>` for sizes (bytes, batch counts)
- `new()` takes `&Meter`, creates all instruments
- Record methods are `pub(crate)`, use `provider` + `network` as standard key-value pairs
- Method names follow `record_*()` convention

## ERROR HANDLING

Extractors use `thiserror` with domain-specific error categories:

```rust
// ✅ Transport/connection errors
#[derive(thiserror::Error, Debug)]
#[error("provider error: {0}")]
pub struct ProviderError(#[source] pub alloy::transports::TransportError);

// ✅ Data conversion errors
#[derive(Debug, thiserror::Error)]
pub enum RpcToRowsError {
    #[error("mismatched tx and receipt count for block {block_num}")]
    TxReceiptCountMismatch { block_num: u64, tx_count: usize, receipt_count: usize },

    #[error("row conversion failed")]
    ToRow(#[source] ToRowError),

    #[error("table build failed")]
    TableRow(#[source] TableRowError),
}

// ✅ Protocol-level errors (firehose)
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("HTTP/2 connection error")]
    Connection(#[source] tonic::transport::Error),

    #[error("gRPC call error")]
    Call(#[source] tonic::Status),
}
```

**Common error categories:**
- **Transport** — connection failures, network errors
- **Conversion** — data transformation failures (RPC response to rows)
- **Protocol** — protocol-specific errors (gRPC, protobuf)
- **Configuration** — invalid provider config, unsupported network

All errors use `#[source]` for chain support and follow the project-wide `errors-reporting` patterns.

## SOURCE FILE LAYOUT

Every extractor crate follows this file structure:

```
crates/extractors/<name>/
├── src/
│   ├── lib.rs              # Exports, ProviderConfig, Manifest, factory functions
│   ├── dataset.rs           # Dataset trait implementation
│   ├── dataset_kind.rs      # Zero-sized kind type + trait impls
│   ├── error.rs             # Error types (thiserror)
│   ├── metrics.rs           # MetricsRegistry
│   ├── client.rs            # BlockStreamer implementation (or extractor.rs)
│   └── tables/
│       ├── mod.rs           # all() function
│       ├── transactions.rs  # Table definition + row builder
│       └── ...              # One file per table
├── gen/                     # Code generation (optional)
│   ├── build.rs
│   └── src/lib.rs
└── Cargo.toml
```

**Key conventions:**
- `lib.rs` is the main entry point, exports all public types
- `dataset_kind.rs` is always a separate file for the zero-sized type
- `tables/mod.rs` always has an `all()` function
- `client.rs` contains the `BlockStreamer` impl (Solana uses `extractor.rs`)
- Generated code (protobuf, etc.) lives in `gen/` or `proto/`

## CHECKLIST

Before committing extractor code, verify:

### Core Traits
- [ ] `BlockStreamer` trait implemented with all four methods
- [ ] `Dataset` trait implemented with all five methods
- [ ] Factory function accepts `ProviderConfig` + optional `&Meter`

### Dataset Kind
- [ ] Zero-sized type with `const DATASET_KIND: &str`
- [ ] `FromStr`, `Display`, `Serialize`, `Deserialize` implemented
- [ ] `PartialEq` impls for `str`, `&str`, `String`, `DatasetKindStr`
- [ ] Custom error type for invalid kind strings

### Tables
- [ ] `tables::all()` returns all table definitions
- [ ] Each table has `TABLE_NAME` const and `table()` function
- [ ] Row builders use `with_capacity()` for pre-allocation
- [ ] `build()` produces `TableRows` with correct schema

### Metrics
- [ ] `MetricsRegistry::new()` takes `&Meter`
- [ ] Record methods use `provider` and `network` key-value pairs
- [ ] Counters for totals, histograms for durations and sizes

### Error Handling
- [ ] All errors use `thiserror`
- [ ] `#[source]` used for error chaining
- [ ] Domain-specific error categories (transport, conversion, protocol)

### File Structure
- [ ] Follows standard source file layout
- [ ] `lib.rs` exports key types and factory functions
- [ ] `dataset_kind.rs` is a separate file

## RATIONALE

These patterns ensure:

1. **Uniform interface** — all extractors implement the same traits, enabling the `BlockStreamClient` enum in `providers-registry` to dispatch to any extractor
2. **Consistent discovery** — identical dataset kind pattern makes manifest parsing predictable
3. **Observable systems** — standardized metrics registry enables unified monitoring dashboards
4. **Extensibility** — adding a new extractor means following the established file layout and implementing the core traits
5. **Type safety** — zero-sized dataset kinds prevent mixing datasets of different types at compile time

## References

- [errors-reporting](errors-reporting.md) - Foundation: Error type design patterns
- [services](services.md) - Related: Service crate structure (extractors follow a similar two-phase pattern)
