# Implementation Plan: Failed Transactions and Failed Transaction Logs Tables

## Overview

This document outlines the implementation plan for adding two new tables to the evm-rpc extractor:
1. `failed_transactions` - stores transactions that failed during execution (status = false)
2. `failed_transaction_logs` - stores logs emitted by failed transactions before they reverted

## Current State Analysis

### Key Findings

**Problem**: Failed transactions and their logs are currently discarded completely.

**Location**: `crates/extractors/evm-rpc/src/client.rs`

**Line 595-606**: Failed transactions are filtered out
```rust
let tx_receipt_pairs = block
    .transactions
    .clone()
    .into_transactions()
    .zip(receipts)
    .filter(|(_, receipt)| {
        // Filter out failed transactions.
        match receipt.inner.inner.inner.receipt.status {
            alloy::consensus::Eip658Value::Eip658(status) => status,
            alloy::consensus::Eip658Value::PostState(fixed_bytes) => !fixed_bytes.is_zero(),
        }
    });
```

**Line 612-628**: Only successful transaction-receipt pairs are processed
- Logs are extracted only from successful transactions (line 623-626)
- Failed transactions never have their logs extracted
- Both failed transactions and their logs are lost

**Result**: 
- `transactions` table contains only successful transactions (status = true)
- `logs` table contains only logs from successful transactions
- Failed transactions and their logs are completely absent from the dataset

---

## Implementation Strategy

### Architecture Decision

**Approach**: Separate tables for failed data (not a single combined table)

**Rationale**:
1. **Query Performance**: Users can query only what they need (success vs failure)
2. **Schema Clarity**: Clear separation of concerns
3. **Backward Compatibility**: Existing `transactions` and `logs` tables unchanged
4. **Analytics Flexibility**: Easy to UNION tables when full history needed

**Alternative Considered**: Add a `status` column and keep everything in existing tables
- **Rejected because**: Breaking change to existing queries and schema

---

## Detailed Implementation Plan

### Step 1: Create `failed_transactions.rs` Table Module

**File**: `crates/extractors/evm-rpc/src/tables/failed_transactions.rs`

**Size**: ~310 lines (based on `transactions.rs` template)

**Structure**:
```rust
use std::sync::{Arc, LazyLock};
use common::{
    BYTES32_TYPE, BoxError, Bytes32, Bytes32ArrayBuilder, 
    EVM_ADDRESS_TYPE as ADDRESS_TYPE, EvmAddress as Address, 
    EvmAddressArrayBuilder, EvmCurrency, EvmCurrencyArrayBuilder, 
    RawTableRows, SPECIAL_BLOCK_NUM, Table, Timestamp, TimestampArrayBuilder,
    arrow::{
        array::{ArrayRef, BinaryBuilder, BooleanBuilder, Int32Builder, 
                UInt32Builder, UInt64Builder},
        datatypes::{DataType, Field, Schema, SchemaRef},
    },
    metadata::segments::BlockRange,
};

static SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| Arc::new(schema()));

pub fn table(network: String) -> Table {
    let name = TABLE_NAME.parse().expect("table name is valid");
    Table::new(
        name,
        SCHEMA.clone(),
        network,
        vec!["block_num".to_string(), "timestamp".to_string()],
    )
    .expect("table creation succeeds")
}

pub const TABLE_NAME: &str = "failed_transactions";

fn schema() -> Schema {
    let special_block_num = Field::new(SPECIAL_BLOCK_NUM, DataType::UInt64, false);
    let block_hash = Field::new("block_hash", BYTES32_TYPE, false);
    let block_num = Field::new("block_num", DataType::UInt64, false);
    let timestamp = Field::new("timestamp", common::timestamp_type(), false);
    let tx_index = Field::new("tx_index", DataType::UInt32, false);
    let tx_hash = Field::new("tx_hash", BYTES32_TYPE, false);
    let to = Field::new("to", ADDRESS_TYPE, true);
    let nonce = Field::new("nonce", DataType::UInt64, false);
    let gas_price = Field::new("gas_price", EVM_CURRENCY_TYPE, true);
    let gas_limit = Field::new("gas_limit", DataType::UInt64, false);
    let value = Field::new("value", EVM_CURRENCY_TYPE, false);
    let input = Field::new("input", DataType::Binary, false);
    let v = Field::new("v", DataType::Binary, false);
    let r = Field::new("r", DataType::Binary, false);
    let s = Field::new("s", DataType::Binary, false);
    let gas_used = Field::new("gas_used", DataType::UInt64, false);
    let r#type = Field::new("type", DataType::Int32, false);
    let max_fee_per_gas = Field::new("max_fee_per_gas", EVM_CURRENCY_TYPE, true);
    let max_priority_fee_per_gas = Field::new("max_priority_fee_per_gas", EVM_CURRENCY_TYPE, true);
    let max_fee_per_blob_gas = Field::new("max_fee_per_blob_gas", EVM_CURRENCY_TYPE, true);
    let from = Field::new("from", ADDRESS_TYPE, false);
    let status = Field::new("status", DataType::Boolean, false);

    Schema::new(vec![
        special_block_num, block_hash, block_num, timestamp,
        tx_index, tx_hash, to, nonce,
        gas_price, gas_limit, value, input,
        v, r, s, gas_used, r#type,
        max_fee_per_gas, max_priority_fee_per_gas, max_fee_per_blob_gas,
        from, status,
    ])
}

#[derive(Debug, Default)]
pub(crate) struct FailedTransaction {
    pub(crate) block_hash: Bytes32,
    pub(crate) block_num: u64,
    pub(crate) timestamp: Timestamp,
    pub(crate) tx_index: u32,
    pub(crate) tx_hash: Bytes32,
    pub(crate) to: Option<Address>,
    pub(crate) nonce: u64,
    pub(crate) gas_price: Option<EvmCurrency>,
    pub(crate) gas_limit: u64,
    pub(crate) value: EvmCurrency,
    pub(crate) input: Vec<u8>,
    pub(crate) v: Vec<u8>,
    pub(crate) r: Vec<u8>,
    pub(crate) s: Vec<u8>,
    pub(crate) receipt_cumulative_gas_used: u64,
    pub(crate) r#type: i32,
    pub(crate) max_fee_per_gas: EvmCurrency,
    pub(crate) max_priority_fee_per_gas: Option<EvmCurrency>,
    pub(crate) max_fee_per_blob_gas: Option<EvmCurrency>,
    pub(crate) from: Address,
    pub(crate) status: bool,
}

pub(crate) struct FailedTransactionRowsBuilder {
    special_block_num: UInt64Builder,
    block_hash: Bytes32ArrayBuilder,
    block_num: UInt64Builder,
    timestamp: TimestampArrayBuilder,
    tx_index: UInt32Builder,
    tx_hash: Bytes32ArrayBuilder,
    to: EvmAddressArrayBuilder,
    nonce: UInt64Builder,
    gas_price: EvmCurrencyArrayBuilder,
    gas_limit: UInt64Builder,
    value: EvmCurrencyArrayBuilder,
    input: BinaryBuilder,
    v: BinaryBuilder,
    r: BinaryBuilder,
    s: BinaryBuilder,
    gas_used: UInt64Builder,
    r#type: Int32Builder,
    max_fee_per_gas: EvmCurrencyArrayBuilder,
    max_priority_fee_per_gas: EvmCurrencyArrayBuilder,
    max_fee_per_blob_gas: EvmCurrencyArrayBuilder,
    from: EvmAddressArrayBuilder,
    status: BooleanBuilder,
}

impl FailedTransactionRowsBuilder {
    pub(crate) fn with_capacity(
        count: usize,
        total_input_size: usize,
        total_v_size: usize,
        total_r_size: usize,
        total_s_size: usize,
    ) -> Self {
        Self {
            special_block_num: UInt64Builder::with_capacity(count),
            block_hash: Bytes32ArrayBuilder::with_capacity(count),
            block_num: UInt64Builder::with_capacity(count),
            timestamp: TimestampArrayBuilder::with_capacity(count),
            tx_index: UInt32Builder::with_capacity(count),
            tx_hash: Bytes32ArrayBuilder::with_capacity(count),
            to: EvmAddressArrayBuilder::with_capacity(count),
            nonce: UInt64Builder::with_capacity(count),
            gas_price: EvmCurrencyArrayBuilder::with_capacity(count),
            gas_limit: UInt64Builder::with_capacity(count),
            value: EvmCurrencyArrayBuilder::with_capacity(count),
            input: BinaryBuilder::with_capacity(count, total_input_size),
            v: BinaryBuilder::with_capacity(count, total_v_size),
            r: BinaryBuilder::with_capacity(count, total_r_size),
            s: BinaryBuilder::with_capacity(count, total_s_size),
            gas_used: UInt64Builder::with_capacity(count),
            r#type: Int32Builder::with_capacity(count),
            max_fee_per_gas: EvmCurrencyArrayBuilder::with_capacity(count),
            max_priority_fee_per_gas: EvmCurrencyArrayBuilder::with_capacity(count),
            max_fee_per_blob_gas: EvmCurrencyArrayBuilder::with_capacity(count),
            from: EvmAddressArrayBuilder::with_capacity(count),
            status: BooleanBuilder::with_capacity(count),
        }
    }

    pub(crate) fn append(&mut self, tx: &FailedTransaction) {
        // Implementation identical to TransactionRowsBuilder::append
        // (omitted for brevity - see transactions.rs lines 180-229)
    }

    pub(crate) fn build(self, range: BlockRange) -> Result<RawTableRows, BoxError> {
        // Implementation identical to TransactionRowsBuilder::build
        // (omitted for brevity - see transactions.rs lines 231-283)
    }
}

#[test]
fn default_to_arrow() {
    //* Given
    let tx = FailedTransaction::default();
    
    //* When
    let rows = {
        let mut builder = FailedTransactionRowsBuilder::with_capacity(
            1,
            tx.input.len(),
            tx.v.len(),
            tx.r.len(),
            tx.s.len(),
        );
        builder.append(&tx);
        builder
            .build(BlockRange {
                numbers: tx.block_num..=tx.block_num,
                network: "test_network".to_string(),
                hash: tx.block_hash.into(),
                prev_hash: None,
            })
            .unwrap()
    };
    
    //* Then
    assert_eq!(rows.rows.num_columns(), 22);
    assert_eq!(rows.rows.num_rows(), 1);
}
```

**Schema Fields** (22 total):
- `special_block_num`: u64 (non-nullable)
- `block_hash`: Bytes32 (non-nullable)
- `block_num`: u64 (non-nullable)
- `timestamp`: Timestamp (non-nullable)
- `tx_index`: u32 (non-nullable)
- `tx_hash`: Bytes32 (non-nullable)
- `to`: Address (nullable - contract creation txs have no `to`)
- `nonce`: u64 (non-nullable)
- `gas_price`: EvmCurrency (nullable - EIP-1559 txs may not have this)
- `gas_limit`: u64 (non-nullable)
- `value`: EvmCurrency (non-nullable)
- `input`: Binary (non-nullable)
- `v`: Binary (non-nullable)
- `r`: Binary (non-nullable)
- `s`: Binary (non-nullable)
- `gas_used`: u64 (non-nullable)
- `type`: i32 (non-nullable)
- `max_fee_per_gas`: EvmCurrency (nullable)
- `max_priority_fee_per_gas`: EvmCurrency (nullable)
- `max_fee_per_blob_gas`: EvmCurrency (nullable)
- `from`: Address (non-nullable)
- `status`: bool (non-nullable - always false for this table)

---

### Step 2: Create `failed_transaction_logs.rs` Table Module

**File**: `crates/extractors/evm-rpc/src/tables/failed_transaction_logs.rs`

**Size**: ~210 lines (based on `common::evm::tables::logs` template)

**Structure**:
```rust
use std::sync::{Arc, LazyLock};
use common::{
    BYTES32_TYPE, BoxError, Bytes32, Bytes32ArrayBuilder,
    EVM_ADDRESS_TYPE as ADDRESS_TYPE, EvmAddress as Address, 
    EvmAddressArrayBuilder, RawTableRows, SPECIAL_BLOCK_NUM, 
    Table, Timestamp, TimestampArrayBuilder,
    arrow::{
        array::{ArrayRef, BinaryBuilder, UInt32Builder, UInt64Builder},
        datatypes::{DataType, Field, Schema, SchemaRef},
    },
    metadata::segments::BlockRange,
};

static SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| Arc::new(schema()));

pub fn table(network: String) -> Table {
    let name = TABLE_NAME.parse().expect("table name is valid");
    Table::new(
        name,
        SCHEMA.clone(),
        network,
        vec!["block_num".to_string(), "timestamp".to_string()],
    )
    .expect("table creation succeeds")
}

pub const TABLE_NAME: &str = "failed_transaction_logs";

fn schema() -> Schema {
    let special_block_num = Field::new(SPECIAL_BLOCK_NUM, DataType::UInt64, false);
    let block_hash = Field::new("block_hash", BYTES32_TYPE, false);
    let block_num = Field::new("block_num", DataType::UInt64, false);
    let timestamp = Field::new("timestamp", common::timestamp_type(), false);
    let tx_hash = Field::new("tx_hash", BYTES32_TYPE, false);
    let tx_index = Field::new("tx_index", DataType::UInt32, false);
    let log_index = Field::new("log_index", DataType::UInt32, false);
    let address = Field::new("address", ADDRESS_TYPE, false);
    let topic0 = Field::new("topic0", BYTES32_TYPE, true);
    let topic1 = Field::new("topic1", BYTES32_TYPE, true);
    let topic2 = Field::new("topic2", BYTES32_TYPE, true);
    let topic3 = Field::new("topic3", BYTES32_TYPE, true);
    let data = Field::new("data", DataType::Binary, false);

    Schema::new(vec![
        special_block_num, block_hash, block_num, timestamp,
        tx_hash, tx_index, log_index,
        address, topic0, topic1, topic2, topic3, data,
    ])
}

#[derive(Debug, Default)]
pub(crate) struct FailedTransactionLog {
    pub(crate) block_hash: Bytes32,
    pub(crate) block_num: u64,
    pub(crate) timestamp: Timestamp,
    pub(crate) tx_index: u32,
    pub(crate) tx_hash: Bytes32,
    pub(crate) log_index: u32,
    pub(crate) address: Address,
    pub(crate) topic0: Option<Bytes32>,
    pub(crate) topic1: Option<Bytes32>,
    pub(crate) topic2: Option<Bytes32>,
    pub(crate) topic3: Option<Bytes32>,
    pub(crate) data: Vec<u8>,
}

pub(crate) struct FailedTransactionLogRowsBuilder {
    special_block_num: UInt64Builder,
    block_hash: Bytes32ArrayBuilder,
    block_num: UInt64Builder,
    timestamp: TimestampArrayBuilder,
    tx_index: UInt32Builder,
    tx_hash: Bytes32ArrayBuilder,
    address: EvmAddressArrayBuilder,
    topic0: Bytes32ArrayBuilder,
    topic1: Bytes32ArrayBuilder,
    topic2: Bytes32ArrayBuilder,
    topic3: Bytes32ArrayBuilder,
    data: BinaryBuilder,
    log_index: UInt32Builder,
}

impl FailedTransactionLogRowsBuilder {
    pub(crate) fn with_capacity(count: usize, total_data_size: usize) -> Self {
        Self {
            special_block_num: UInt64Builder::with_capacity(count),
            block_hash: Bytes32ArrayBuilder::with_capacity(count),
            block_num: UInt64Builder::with_capacity(count),
            timestamp: TimestampArrayBuilder::with_capacity(count),
            tx_index: UInt32Builder::with_capacity(count),
            tx_hash: Bytes32ArrayBuilder::with_capacity(count),
            address: EvmAddressArrayBuilder::with_capacity(count),
            topic0: Bytes32ArrayBuilder::with_capacity(count),
            topic1: Bytes32ArrayBuilder::with_capacity(count),
            topic2: Bytes32ArrayBuilder::with_capacity(count),
            topic3: Bytes32ArrayBuilder::with_capacity(count),
            data: BinaryBuilder::with_capacity(count, total_data_size),
            log_index: UInt32Builder::with_capacity(count),
        }
    }

    pub(crate) fn append(&mut self, log: &FailedTransactionLog) {
        // Implementation identical to LogRowsBuilder::append
        // (omitted for brevity - see common::evm::tables::logs lines 119-148)
    }

    pub(crate) fn build(self, range: BlockRange) -> Result<RawTableRows, BoxError> {
        // Implementation identical to LogRowsBuilder::build
        // (omitted for brevity - see common::evm::tables::logs lines 150-184)
    }
}

#[test]
fn default_to_arrow() {
    //* Given
    let log = FailedTransactionLog::default();
    
    //* When
    let rows = {
        let mut builder = FailedTransactionLogRowsBuilder::with_capacity(1, log.data.len());
        builder.append(&log);
        builder
            .build(BlockRange {
                numbers: log.block_num..=log.block_num,
                network: "test_network".to_string(),
                hash: log.block_hash.into(),
                prev_hash: None,
            })
            .unwrap()
    };
    
    //* Then
    assert_eq!(rows.rows.num_columns(), 13);
    assert_eq!(rows.rows.num_rows(), 1);
}
```

**Schema Fields** (13 total):
- `special_block_num`: u64 (non-nullable)
- `block_hash`: Bytes32 (non-nullable)
- `block_num`: u64 (non-nullable)
- `timestamp`: Timestamp (non-nullable)
- `tx_hash`: Bytes32 (non-nullable)
- `tx_index`: u32 (non-nullable)
- `log_index`: u32 (non-nullable)
- `address`: Address (non-nullable - contract that emitted the log)
- `topic0`: Bytes32 (nullable - event signature)
- `topic1`: Bytes32 (nullable - indexed parameter 1)
- `topic2`: Bytes32 (nullable - indexed parameter 2)
- `topic3`: Bytes32 (nullable - indexed parameter 3)
- `data`: Binary (non-nullable - non-indexed parameters)

**Important Note**: These are logs emitted during the execution of a failed transaction. Even though the transaction failed and its state changes were reverted, the logs were still emitted and are available in the receipt. They are valuable for debugging WHY a transaction failed.

---

### Step 3: Update `tables/mod.rs`

**File**: `crates/extractors/evm-rpc/src/tables/mod.rs`

**Changes**:

```diff
 pub mod transactions;
+pub mod failed_transactions;
+pub mod failed_transaction_logs;

 pub fn all(network: &str) -> Vec<common::Table> {
     vec![
         common::evm::tables::blocks::table(network.to_string()),
         transactions::table(network.to_string()),
+        failed_transactions::table(network.to_string()),
         common::evm::tables::logs::table(network.to_string()),
+        failed_transaction_logs::table(network.to_string()),
     ]
 }
```

**Result**: The `all()` function now returns 5 tables instead of 3:
1. `blocks` (existing)
2. `transactions` (existing - successful only)
3. `failed_transactions` (new)
4. `logs` (existing - from successful transactions only)
5. `failed_transaction_logs` (new)

---

### Step 4: Update `client.rs::rpc_to_rows()` Function

**File**: `crates/extractors/evm-rpc/src/client.rs`

**Function**: `rpc_to_rows()` (starts at line ~580)

**Changes Required**:

#### 4.1: Add imports
```diff
 use crate::tables::transactions::{Transaction, TransactionRowsBuilder};
+use crate::tables::failed_transactions::{FailedTransaction, FailedTransactionRowsBuilder};
+use crate::tables::failed_transaction_logs::{FailedTransactionLog, FailedTransactionLogRowsBuilder};
```

#### 4.2: Replace filter with partition (lines ~595-606)

**Before**:
```rust
let tx_receipt_pairs = block
    .transactions
    .clone()
    .into_transactions()
    .zip(receipts)
    .filter(|(_, receipt)| {
        // Filter out failed transactions.
        match receipt.inner.inner.inner.receipt.status {
            alloy::consensus::Eip658Value::Eip658(status) => status,
            alloy::consensus::Eip658Value::PostState(fixed_bytes) => !fixed_bytes.is_zero(),
        }
    });
```

**After**:
```rust
// Split into successful and failed transactions
let (successful_pairs, failed_pairs): (Vec<_>, Vec<_>) = block
    .transactions
    .clone()
    .into_transactions()
    .zip(receipts)
    .partition(|(_, receipt)| {
        // Partition based on transaction status
        match receipt.inner.inner.inner.receipt.status {
            alloy::consensus::Eip658Value::Eip658(status) => status,
            alloy::consensus::Eip658Value::PostState(fixed_bytes) => !fixed_bytes.is_zero(),
        }
    });
```

#### 4.3: Process successful transactions (lines ~608-628)

**Keep existing logic**, but modify to iterate over `successful_pairs` instead of `tx_receipt_pairs`:

```rust
let header = rpc_header_to_row(block.header.clone())?;
let mut logs = Vec::new();
let mut transactions = Vec::new();

for (idx, (tx, mut receipt)) in successful_pairs.into_iter().enumerate() {
    if tx.tx_hash() != receipt.transaction_hash {
        let err = format!(
            "mismatched tx and receipt hash for block {}: tx {}, receipt {}",
            header.block_num,
            tx.tx_hash().encode_hex(),
            receipt.transaction_hash.encode_hex()
        );
        return Err(err.into());
    }
    // Move the logs out of the nested structure.
    let receipt_logs = std::mem::take(&mut receipt.inner.inner.inner.receipt.logs);
    for log in receipt_logs {
        logs.push(rpc_log_to_row(log, header.timestamp)?);
    }
    transactions.push(rpc_transaction_to_row(&header, tx, receipt, idx)?);
}
```

#### 4.4: Process failed transactions (NEW - insert after successful processing)

```rust
// Process failed transactions and their logs
let mut failed_logs = Vec::new();
let mut failed_transactions = Vec::new();

for (idx, (tx, mut receipt)) in failed_pairs.into_iter().enumerate() {
    if tx.tx_hash() != receipt.transaction_hash {
        let err = format!(
            "mismatched tx and receipt hash for block {}: tx {}, receipt {}",
            header.block_num,
            tx.tx_hash().encode_hex(),
            receipt.transaction_hash.encode_hex()
        );
        return Err(err.into());
    }
    // Move the logs out of the nested structure.
    let receipt_logs = std::mem::take(&mut receipt.inner.inner.inner.receipt.logs);
    for log in receipt_logs {
        failed_logs.push(rpc_log_to_row(log, header.timestamp)?);
    }
    failed_transactions.push(rpc_transaction_to_row(&header, tx, receipt, idx)?);
}
```

#### 4.5: Build all table rows (lines ~630-675)

**Keep existing table building logic** for `header_row`, `logs_row`, and `transactions_row`.

**Add failed transactions row builder** (insert after `transactions_row`):

```rust
let failed_transactions_row = {
    let total_input_size: usize = failed_transactions.iter().map(|t| t.input.len()).sum();
    let total_v_size: usize = failed_transactions.iter().map(|tx| tx.v.len()).sum();
    let total_r_size: usize = failed_transactions.iter().map(|tx| tx.r.len()).sum();
    let total_s_size: usize = failed_transactions.iter().map(|tx| tx.s.len()).sum();

    let mut builder = FailedTransactionRowsBuilder::with_capacity(
        failed_transactions.len(),
        total_input_size,
        total_v_size,
        total_r_size,
        total_s_size,
    );
    for tx in failed_transactions {
        builder.append(&tx);
    }
    builder.build(block.clone())?
};
```

**Add failed logs row builder** (insert after `failed_transactions_row`):

```rust
let failed_logs_row = {
    let total_data_size = failed_logs.iter().map(|log| log.data.len()).sum();
    let mut builder = FailedTransactionLogRowsBuilder::with_capacity(
        failed_logs.len(),
        total_data_size,
    );
    for log in failed_logs {
        builder.append(&log);
    }
    builder.build(block.clone())?
};
```

#### 4.6: Update return statement (line ~671)

**Before**:
```rust
Ok(RawDatasetRows::new(vec![
    header_row,
    logs_row,
    transactions_row,
]))
```

**After**:
```rust
Ok(RawDatasetRows::new(vec![
    header_row,
    logs_row,
    transactions_row,
    failed_transactions_row,
    failed_logs_row,
]))
```

---

### Step 5: Type Compatibility Considerations

**Important**: The existing helper functions can be reused:
- `rpc_log_to_row()` returns `Result<logs::Log, ToRowError>`
- `rpc_transaction_to_row()` returns `Result<Transaction, ToRowError>`

**Solution**: Since the structs are identical, we can either:
1. **Type alias**: `type FailedTransaction = Transaction;` (simplest)
2. **Copy structs**: Duplicate but maintains clarity
3. **Shared module**: Extract common struct to shared location

**Recommendation**: Use **type aliases** for simplicity:

```rust
// In failed_transactions.rs
pub(crate) use super::transactions::Transaction as FailedTransaction;
pub(crate) use super::transactions::TransactionRowsBuilder as FailedTransactionRowsBuilder;

// In failed_transaction_logs.rs  
pub(crate) use common::evm::tables::logs::Log as FailedTransactionLog;
pub(crate) use common::evm::tables::logs::LogRowsBuilder as FailedTransactionLogRowsBuilder;
```

**Benefit**: No code duplication, reuse existing tested implementations.

**Alternative**: If we want stronger type separation, duplicate the structs but keep identical fields.

---

### Step 6: Testing Strategy

#### Unit Tests

**Test 1**: Schema validation for `failed_transactions`
```rust
#[test]
fn default_to_arrow() {
    //* Given
    let tx = FailedTransaction::default();
    
    //* When
    let rows = {
        let mut builder = FailedTransactionRowsBuilder::with_capacity(
            1, tx.input.len(), tx.v.len(), tx.r.len(), tx.s.len()
        );
        builder.append(&tx);
        builder.build(BlockRange {
            numbers: tx.block_num..=tx.block_num,
            network: "test_network".to_string(),
            hash: tx.block_hash.into(),
            prev_hash: None,
        }).unwrap()
    };
    
    //* Then
    assert_eq!(rows.rows.num_columns(), 22);
    assert_eq!(rows.rows.num_rows(), 1);
}
```

**Test 2**: Schema validation for `failed_transaction_logs`
```rust
#[test]
fn default_to_arrow() {
    //* Given
    let log = FailedTransactionLog::default();
    
    //* When
    let rows = {
        let mut builder = FailedTransactionLogRowsBuilder::with_capacity(1, log.data.len());
        builder.append(&log);
        builder.build(BlockRange {
            numbers: log.block_num..=log.block_num,
            network: "test_network".to_string(),
            hash: log.block_hash.into(),
            prev_hash: None,
        }).unwrap()
    };
    
    //* Then
    assert_eq!(rows.rows.num_columns(), 13);
    assert_eq!(rows.rows.num_rows(), 1);
}
```

#### Integration Testing

**Manual Test**: Run dump command and verify tables are created
```bash
cargo run --release -p ampd -- dump --dataset eth_rpc -e 1000 -j 1
```

**Validation**:
1. Check that `failed_transactions` table exists in output
2. Check that `failed_transaction_logs` table exists in output
3. Verify row counts: `transactions.count() + failed_transactions.count() == total_tx_count`
4. Query a known failed transaction and verify it appears in `failed_transactions`
5. Verify logs from failed transactions appear in `failed_transaction_logs`

---

### Step 7: Validation Workflow (MANDATORY)

Following `.patterns/README.md` and `AGENTS.md` exactly:

```bash
# Step 1: Format all modified files (MANDATORY after EVERY edit)
just fmt-file crates/extractors/evm-rpc/src/tables/failed_transactions.rs
just fmt-file crates/extractors/evm-rpc/src/tables/failed_transaction_logs.rs
just fmt-file crates/extractors/evm-rpc/src/tables/mod.rs
just fmt-file crates/extractors/evm-rpc/src/client.rs

# Step 2: Validate formatting (must show no changes)
just fmt-rs-check

# Step 3: Check compilation (MUST PASS with zero errors)
just check-crate evm-rpc

# Step 4: Run clippy (MUST PASS with zero warnings)
just clippy-crate evm-rpc

# Step 5: Run tests (MUST PASS - all tests green)
just test-local
```

**Critical**: Each step MUST pass before proceeding to the next. Zero tolerance for:
- Compilation errors
- Clippy warnings
- Test failures
- Formatting violations

---

## Implementation Checklist

### Pre-Implementation
- [ ] Review this plan with team
- [ ] Confirm type alias vs struct duplication approach
- [ ] Verify no conflicts with existing schema migrations

### Implementation Phase

#### Part 1: Table Modules
- [ ] Create `failed_transactions.rs` (~310 lines)
  - [ ] Define `TABLE_NAME` constant
  - [ ] Implement `schema()` function (22 fields)
  - [ ] Define `FailedTransaction` struct
  - [ ] Implement `FailedTransactionRowsBuilder`
  - [ ] Add `with_capacity()` constructor
  - [ ] Implement `append()` method
  - [ ] Implement `build()` method
  - [ ] Add `default_to_arrow()` test
  - [ ] Format: `just fmt-file crates/extractors/evm-rpc/src/tables/failed_transactions.rs`

- [ ] Create `failed_transaction_logs.rs` (~210 lines)
  - [ ] Define `TABLE_NAME` constant
  - [ ] Implement `schema()` function (13 fields)
  - [ ] Define `FailedTransactionLog` struct
  - [ ] Implement `FailedTransactionLogRowsBuilder`
  - [ ] Add `with_capacity()` constructor
  - [ ] Implement `append()` method
  - [ ] Implement `build()` method
  - [ ] Add `default_to_arrow()` test
  - [ ] Format: `just fmt-file crates/extractors/evm-rpc/src/tables/failed_transaction_logs.rs`

#### Part 2: Registration
- [ ] Update `tables/mod.rs`
  - [ ] Add `pub mod failed_transactions;`
  - [ ] Add `pub mod failed_transaction_logs;`
  - [ ] Register `failed_transactions::table()` in `all()`
  - [ ] Register `failed_transaction_logs::table()` in `all()`
  - [ ] Format: `just fmt-file crates/extractors/evm-rpc/src/tables/mod.rs`

#### Part 3: Client Logic
- [ ] Update `client.rs`
  - [ ] Add imports for new table modules
  - [ ] Replace `.filter()` with `.partition()` (line ~595)
  - [ ] Keep successful transaction processing loop
  - [ ] Add failed transaction processing loop
  - [ ] Add failed transaction row builder
  - [ ] Add failed logs row builder
  - [ ] Update return statement to include 5 tables
  - [ ] Format: `just fmt-file crates/extractors/evm-rpc/src/client.rs`

### Validation Phase
- [ ] **Format Check**: `just fmt-rs-check` (must show no changes)
- [ ] **Compilation Check**: `just check-crate evm-rpc` (must pass - zero errors)
- [ ] **Lint Check**: `just clippy-crate evm-rpc` (must pass - zero warnings)
- [ ] **Unit Tests**: `just test-local` (must pass - all tests green)

### Post-Implementation
- [ ] Manual integration test with dump command
- [ ] Verify both new tables appear in output
- [ ] Query failed transaction from known test data
- [ ] Update documentation (if needed)
- [ ] Create PR with detailed description

---

## Benefits and Impact

### Data Completeness
- **Before**: Failed transactions and logs completely lost
- **After**: Complete blockchain history preserved

### Query Flexibility
```sql
-- Get only successful transactions
SELECT * FROM transactions;

-- Get only failed transactions  
SELECT * FROM failed_transactions;

-- Get ALL transactions (success + failure)
SELECT * FROM transactions
UNION ALL
SELECT * FROM failed_transactions;

-- Get logs from failed transactions
SELECT * FROM failed_transaction_logs
WHERE tx_hash = '0x...';

-- Analyze failure rates
SELECT 
    COUNT(*) AS total_txs,
    (SELECT COUNT(*) FROM transactions) AS successful,
    (SELECT COUNT(*) FROM failed_transactions) AS failed,
    (SELECT COUNT(*) FROM failed_transactions) * 100.0 / COUNT(*) AS failure_rate
FROM (
    SELECT * FROM transactions
    UNION ALL
    SELECT * FROM failed_transactions
);
```

### Debugging and Analytics
1. **Smart Contract Debugging**: View logs from failed transactions to understand revert reasons
2. **Gas Analysis**: Analyze gas consumption of failed vs successful transactions
3. **Error Patterns**: Identify common failure causes by analyzing failed transaction logs
4. **Network Health**: Monitor failure rates over time
5. **MEV Analysis**: Track failed transactions in MEV bundles

### Backward Compatibility
- **Existing Queries**: Unchanged - still work exactly as before
- **Existing Tables**: Schema unchanged - no breaking changes
- **Opt-in Usage**: Teams can ignore new tables if not needed

---

## Alternative Approaches Considered

### Alternative 1: Single Transactions Table with Status Column
**Approach**: Keep all transactions in one table, add `status: bool` column

**Pros**:
- Single table to query
- Simpler schema

**Cons**:
- Breaking change to existing queries
- All queries must filter by status
- Larger table scans for success-only queries
- **REJECTED**

### Alternative 2: Separate Database Schema
**Approach**: Create `failed_` schema with mirror tables

**Pros**:
- Clear namespace separation
- No table naming conflicts

**Cons**:
- More complex to query (cross-schema joins)
- Overly complex for this use case
- **REJECTED**

### Alternative 3: Combined Failed Data Table
**Approach**: Single `failed_transaction_data` table with both tx and log data

**Pros**:
- Fewer tables

**Cons**:
- Denormalized schema
- Difficult to query logs independently
- Violates single responsibility principle
- **REJECTED**

### Selected Approach: Separate Tables (Current Plan)
**Rationale**:
- Clean separation of concerns
- Backward compatible
- Optimal query performance
- Consistent with existing schema design
- **SELECTED** ✓

---

## Risks and Mitigations

### Risk 1: Increased Storage Requirements
**Impact**: Two new tables will increase storage usage

**Mitigation**:
- Failed transactions are typically <5% of total transactions
- Parquet compression will minimize overhead
- Users can choose to exclude these tables if storage is constrained

**Estimated Impact**: +5-10% storage increase

### Risk 2: Processing Performance
**Impact**: Processing both success and failure paths may slow down extraction

**Mitigation**:
- `.partition()` is O(n) - same as `.filter()`
- Most blocks have few or no failed transactions
- Parallel processing (existing) still applies

**Estimated Impact**: <5% performance decrease (negligible)

### Risk 3: Schema Migration Complexity
**Impact**: Existing datasets don't have these tables

**Mitigation**:
- New tables are additive (not breaking)
- Existing queries continue to work
- Re-extraction not required (but recommended for completeness)

**Estimated Impact**: Low - no forced migrations

### Risk 4: Type Confusion Between Success/Failure Types
**Impact**: Developers might accidentally mix types

**Mitigation**:
- Clear naming: `FailedTransaction` vs `Transaction`
- Type system enforces separation
- Tests validate correct table routing

**Estimated Impact**: Low - caught at compile time

---

## Success Criteria

### Must Have (Blocking)
- [ ] Both new tables exist and are registered
- [ ] Failed transactions appear in `failed_transactions` table
- [ ] Failed transaction logs appear in `failed_transaction_logs` table
- [ ] All compilation checks pass (zero errors)
- [ ] All clippy checks pass (zero warnings)
- [ ] All unit tests pass
- [ ] Schema tests validate column count and types
- [ ] Existing `transactions` and `logs` tables unchanged

### Should Have (Important)
- [ ] Manual integration test confirms data accuracy
- [ ] Documentation updated with new table schemas
- [ ] Example queries provided for common use cases
- [ ] Performance benchmarks show <5% impact

### Nice to Have (Optional)
- [ ] Dashboard metrics for failed transaction rates
- [ ] Automated schema validation tests
- [ ] Example analytics queries for failed transactions

---

## Timeline Estimate

**Total Estimated Time**: 4-6 hours

| Task | Estimated Time | Notes |
|------|----------------|-------|
| Create `failed_transactions.rs` | 1.5 hours | Copy, modify, test |
| Create `failed_transaction_logs.rs` | 1 hour | Copy, modify, test |
| Update `tables/mod.rs` | 15 minutes | Simple registration |
| Update `client.rs` | 1.5 hours | Partition logic, builders |
| Format, check, clippy | 30 minutes | Run validation tools |
| Unit testing | 30 minutes | Schema validation tests |
| Integration testing | 45 minutes | Manual dump and validation |
| Documentation | 30 minutes | Update schemas, examples |

**Dependencies**:
- No external dependencies
- No schema migrations required
- No breaking changes

**Parallelization**: Steps 1-2 can be done in parallel (create both table modules simultaneously)

---

## Appendix

### A. Type Alias Implementation (Recommended)

If using type aliases to avoid duplication:

**File**: `crates/extractors/evm-rpc/src/tables/failed_transactions.rs`
```rust
// Reuse existing Transaction type
pub(crate) use super::transactions::{Transaction, TransactionRowsBuilder};

// Wrapper struct for failed transactions
#[derive(Debug, Default)]
pub(crate) struct FailedTransaction(pub(crate) Transaction);

// Or simply use type alias
pub(crate) type FailedTransaction = Transaction;
pub(crate) type FailedTransactionRowsBuilder = TransactionRowsBuilder;

// Change table name
pub const TABLE_NAME: &str = "failed_transactions";
```

**Benefit**: Zero code duplication, maximum code reuse.

**Tradeoff**: Less type safety (can accidentally pass wrong type).

### B. Full Struct Implementation (Alternative)

If full type separation is desired, duplicate all structs but maintain identical fields.

**Benefit**: Stronger type safety, clearer intent.

**Tradeoff**: Code duplication, more maintenance.

### C. Code References

**Similar patterns in codebase**:
- `firehose/src/evm/tables/transactions.rs` (similar transaction table)
- `common/src/evm/tables/logs.rs` (log table pattern)
- `firehose/src/evm/tables/calls.rs` (another table example)

**Helper functions to reuse**:
- `client.rs::rpc_log_to_row()` (line ~710)
- `client.rs::rpc_transaction_to_row()` (line ~740)
- `client.rs::rpc_header_to_row()` (line ~678)

---

## Questions and Answers

**Q: Why separate tables instead of a status column?**
A: Backward compatibility and query performance. Existing queries don't need to change, and users querying only successful transactions don't pay the cost of filtering.

**Q: Do failed transactions actually emit logs?**
A: Yes! Logs are emitted during execution. When a transaction fails, the state changes are reverted but logs are still available in the receipt. They're crucial for debugging.

**Q: What about reorgs? Do these tables handle them?**
A: Yes, the same BlockRange mechanism handles reorgs for all tables consistently. No special handling needed.

**Q: Can we add failed transactions later without breaking existing datasets?**
A: Yes! The tables are additive. Existing datasets continue to work. Re-extraction is optional.

**Q: What's the failure rate typically?**
A: On Ethereum mainnet, roughly 3-5% of transactions fail. The percentage varies by network and time period.

**Q: Should we add an index on `status` field?**
A: Not needed - the tables are already separated by status. Querying `failed_transactions` implicitly means `status = false`.

---

## Document History

- **Version 1.0** (2025-11-09): Initial implementation plan
- **Author**: AI Assistant (Claude)
- **Reviewers**: [To be filled]
- **Status**: Draft - Awaiting Review

---

## Approval

- [ ] Technical Lead: __________________ Date: __________
- [ ] Team Review: __________________ Date: __________
- [ ] Ready for Implementation: Yes / No

