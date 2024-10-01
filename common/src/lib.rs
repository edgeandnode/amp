pub mod arrow_helpers;
pub mod attestation;
pub mod catalog;
pub mod config;
pub mod evm;
pub mod meta_tables;
pub mod multirange;
pub mod query_context;
pub mod store;
pub mod tracing;

pub use arrow_helpers::*;
pub use catalog::logical::*;
pub use datafusion::arrow;
pub use datafusion::parquet;
pub use query_context::QueryContext;
pub use store::Store;

use std::future::Future;
use std::time::Duration;
use std::time::SystemTime;

use arrow::array::FixedSizeBinaryArray;
use arrow::datatypes::DataType;
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::{TimeUnit, DECIMAL128_MAX_PRECISION};
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::{
    array::{AsArray as _, RecordBatch},
    datatypes::UInt64Type,
};
use tokio::sync::mpsc;

pub type BoxError = Box<dyn std::error::Error + Sync + Send + 'static>;

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

#[derive(Clone, Copy, Debug, Default)]
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

pub struct TableRows {
    pub table: Table,
    pub rows: RecordBatch,
}

impl TableRows {
    pub fn new(table: Table, columns: Vec<ArrayRef>) -> Result<Self, ArrowError> {
        let schema = table.schema.clone();
        Ok(TableRows {
            table,
            rows: RecordBatch::try_new(schema, columns)?,
        })
    }

    pub fn is_empty(&self) -> bool {
        self.rows.num_rows() == 0
    }

    /// Returns the block number if all rows have the same block number.
    ///
    /// # Errors
    /// - If the table is empty.
    /// - If any table is missing the block_num column.
    /// - If the block_num column values are not all the same.
    pub fn block_num(&self) -> Result<u64, BoxError> {
        use arrow::compute::kernels::aggregate::max;
        use arrow::compute::kernels::aggregate::min;

        if self.is_empty() {
            return Err(format!("empty table: {}", self.table.name).into());
        }

        let block_nums = self
            .rows
            .column_by_name(BLOCK_NUM)
            .ok_or_else(|| format!("missing block_num column in table: {}", self.table.name))?;
        let block_nums = block_nums
            .as_primitive_opt::<UInt64Type>()
            .ok_or("block_num column is not uint64")?;

        // Unwrap: We are not empty.
        let min = min(block_nums).unwrap();
        let max = max(block_nums).unwrap();

        if min != max {
            return Err(format!(
                "table {} contains mismatching block_num: {} != {}",
                self.table.name, min, max
            )
            .into());
        };

        Ok(min)
    }
}

pub struct DatasetRows(pub Vec<TableRows>);

impl DatasetRows {
    pub fn is_empty(&self) -> bool {
        let mut empty = true;
        for table_rows in &self.0 {
            if !table_rows.is_empty() {
                empty = false;
                break;
            }
        }
        empty
    }

    /// Returns the block number if all tables have the same block number.
    ///
    /// # Errors
    /// - If the dataset is empty.
    /// - If any table is missing the block_num column.
    /// - If the block_num column values are not all the same.
    pub fn block_num(&self) -> Result<u64, BoxError> {
        if self.is_empty() {
            return Err("empty dataset".into());
        }

        let mut block_nums = vec![];
        for table_rows in &self.0 {
            if table_rows.is_empty() {
                continue;
            }
            let block_num = table_rows.block_num()?;
            block_nums.push(block_num);
        }

        // Unwrap: We are not empty.
        let min = block_nums.iter().min().unwrap();
        let max = block_nums.iter().max().unwrap();

        if min != max {
            return Err(
                format!("dataset contains mismatching block_num: {} != {}", min, max).into(),
            );
        }

        Ok(*min)
    }
}

impl IntoIterator for DatasetRows {
    type Item = TableRows;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

pub trait BlockStreamer: Clone + 'static {
    fn block_stream(
        self,
        start_block: u64,
        end_block: u64,
        tx: mpsc::Sender<DatasetRows>,
    ) -> impl Future<Output = Result<(), BoxError>> + Send;

    fn recent_final_block_num(&mut self)
        -> impl Future<Output = Result<BlockNum, BoxError>> + Send;
}
