pub mod arrow_helpers;
pub mod attestation;
pub mod catalog;
pub mod config;
pub mod evm;
pub mod manifest;
pub mod meta_tables;
pub mod multirange;
pub mod query_context;
pub mod store;
pub mod stream_helpers;
pub mod tracing_helpers;

use std::{
    future::Future,
    time::{Duration, SystemTime},
};

use arrow::{array::FixedSizeBinaryArray, datatypes::DataType};
pub use arrow_helpers::*;
pub use catalog::logical::*;
use datafusion::arrow::{
    array::{ArrayRef, AsArray as _, RecordBatch},
    datatypes::{TimeUnit, UInt64Type, DECIMAL128_MAX_PRECISION},
    error::ArrowError,
};
pub use datafusion::{arrow, parquet};
pub use query_context::QueryContext;
use serde::{Deserialize, Serialize};
pub use store::Store;
use tokio::sync::mpsc;

pub type BoxError = Box<dyn std::error::Error + Sync + Send + 'static>;
pub type BoxResult<T> = Result<T, BoxError>;

/// The block number column name.
pub const BLOCK_NUM: &str = "block_num";

pub type BlockNum = u64;
pub type Bytes32 = [u8; 32];
pub type EvmAddress = [u8; 20];
pub type EvmCurrency = i128; // Payment amount in the EVM. Used for gas or value transfers.

pub const BYTES32_TYPE: DataType = DataType::FixedSizeBinary(32);
pub type Bytes32ArrayType = FixedSizeBinaryArray;

pub const EVM_ADDRESS_TYPE: DataType = DataType::FixedSizeBinary(20);
pub type EvmAddressArrayType = FixedSizeBinaryArray;

/// Payment amount in the EVM. Used for gas or value transfers.
pub const EVM_CURRENCY_TYPE: DataType = DataType::Decimal128(DECIMAL128_MAX_PRECISION, 0);

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize)]
pub struct Timestamp(pub Duration);

impl Timestamp {
    pub fn now() -> Self {
        Timestamp(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap(),
        )
    }
}

// Note: We choose a 'nanosecond' precision for the timestamp, even though many blockchains expose
// only 'second' precision for block timestamps. A couple justifications for 'nanosecond':
// 1. We found that the `date_bin` scalar function in DataFusion expects a nanosecond precision
//    origin parameter. If we patch DataFusion to fix this, we can revisit this decision.
// 2. There is no performance hit for choosing higher precision, it's all i64 at the Arrow level.
// 3. It's the most precise timestamp, so it's maximally future-compatible with data sources that use
//    more precise timestamps.
pub fn timestamp_type() -> DataType {
    let timezone = Some("+00:00".into());
    DataType::Timestamp(TimeUnit::Nanosecond, timezone)
}

/// Remember to call `.with_timezone_utc()` after creating a Timestamp array.
pub(crate) type TimestampArrayType = arrow::array::TimestampNanosecondArray;

/// A non-empty record batch associated with a single block of chain data, for populating raw
/// datasets.
pub struct RawTableRows {
    pub table: Table,
    pub rows: RecordBatch,
    pub block: BlockNum,
}

impl RawTableRows {
    pub fn new(table: Table, columns: Vec<ArrayRef>) -> Result<Self, BoxError> {
        let schema = table.schema.clone();
        let rows = RecordBatch::try_new(schema, columns)?;
        let block = Self::block(&table, &rows)?;
        Ok(RawTableRows { table, rows, block })
    }

    fn block(table: &Table, rows: &RecordBatch) -> Result<BlockNum, BoxError> {
        use arrow::compute::kernels::aggregate::{max, min};

        if rows.num_rows() == 0 {
            return Err(format!("empty table: {}", table.name).into());
        }

        let block_nums = rows
            .column_by_name(BLOCK_NUM)
            .ok_or_else(|| format!("missing block_num column in table: {}", table.name))?;
        let block_nums = block_nums
            .as_primitive_opt::<UInt64Type>()
            .ok_or("block_num column is not uint64")?;

        // Unwrap: We are not empty.
        let start = min(block_nums).unwrap();
        let end = max(block_nums).unwrap();

        if start != end {
            return Err(format!(
                "table {} contains mismatching block_num: {} != {}",
                table.name, start, end
            )
            .into());
        };

        Ok(start)
    }
}

pub struct RawDatasetRows(Vec<RawTableRows>);

impl RawDatasetRows {
    pub fn new(rows: Vec<RawTableRows>) -> Self {
        assert!(!rows.is_empty());
        assert!(rows.iter().skip(1).all(|r| &r.block == &rows[0].block));
        Self(rows)
    }

    pub fn block(&self) -> BlockNum {
        self.0[0].block
    }
}

impl IntoIterator for RawDatasetRows {
    type Item = RawTableRows;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

pub trait BlockStreamer: Clone + 'static {
    fn block_stream(
        self,
        start: BlockNum,
        end: BlockNum,
        tx: mpsc::Sender<RawDatasetRows>,
    ) -> impl Future<Output = Result<(), BoxError>> + Send;

    fn latest_block(
        &mut self,
        finalized: bool,
    ) -> impl Future<Output = Result<BlockNum, BoxError>> + Send;
}
