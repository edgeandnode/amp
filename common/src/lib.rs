pub mod arrow_helpers;
pub mod dataset_context;
pub mod multirange;

pub use datafusion::arrow;
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::error::ArrowError;
pub use datafusion::parquet;

use std::future::Future;
use std::time::Duration;

use anyhow::Context as _;
use arrow::datatypes::DataType;
use datafusion::arrow::datatypes::{SchemaRef, TimeUnit, DECIMAL128_MAX_PRECISION};
use datafusion::arrow::{
    array::{AsArray as _, RecordBatch},
    datatypes::UInt64Type,
};
use tokio::sync::mpsc;

/// The block number column name.
pub const BLOCK_NUM: &str = "block_num";

pub type BlockNum = u64;
pub type Bytes32 = [u8; 32];
pub type Bytes = Box<[u8]>;
pub type EvmAddress = [u8; 20];
pub type EvmCurrency = i128; // Payment amount in the EVM. Used for gas or value transfers.

pub const BYTES32_TYPE: DataType = DataType::FixedSizeBinary(32);
pub type Bytes32ArrayType = arrow::array::FixedSizeBinaryArray;

pub const EVM_ADDRESS_TYPE: DataType = DataType::FixedSizeBinary(20);
pub type EvmAddressArrayType = arrow::array::FixedSizeBinaryArray;

pub const EVM_CURRENCY_TYPE: DataType = DataType::Decimal128(DECIMAL128_MAX_PRECISION, 0);
pub type EvmCurrencyArrayType = arrow::array::Decimal128Array;

#[derive(Clone, Copy, Debug, Default)]
pub struct Timestamp(pub Duration);

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
pub type TimestampArrayType = arrow::array::TimestampNanosecondArray;

/// Identifies a dataset and its data schema.
#[derive(Clone, Debug)]
pub struct DataSet {
    pub name: String,
    pub network: String,
    pub data_schema: DataSchema,
}

impl DataSet {
    pub fn tables(&self) -> &[Table] {
        &self.data_schema.tables
    }
}

#[derive(Clone, Debug)]
pub struct DataSchema {
    pub tables: Vec<Table>,
}

#[derive(Clone, Hash, PartialEq, Eq, Debug)]
pub struct Table {
    pub name: String,
    pub schema: SchemaRef,
}

pub struct TableRows {
    pub table: Table,
    pub rows: RecordBatch,
}

impl TableRows {
    // Useful for tests.
    pub fn try_new(table: Table, columns: Vec<ArrayRef>) -> Result<Self, ArrowError> {
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
    fn unify_block_num(&self) -> Result<u64, anyhow::Error> {
        use arrow::compute::kernels::aggregate::max;
        use arrow::compute::kernels::aggregate::min;

        if self.is_empty() {
            return Err(anyhow::anyhow!("empty table: {}", self.table.name));
        }

        let block_nums = self
            .rows
            .column_by_name(BLOCK_NUM)
            .with_context(|| format!("missing block_num column in table: {}", self.table.name))?;
        let block_nums = block_nums
            .as_primitive_opt::<UInt64Type>()
            .context("block_num column is not uint64")?;

        // Unwrap: We are not empty.
        let min = min(block_nums).unwrap();
        let max = max(block_nums).unwrap();

        anyhow::ensure!(
            min == max,
            "table {} contains mismatching block_num: {} != {}",
            self.table.name,
            min,
            max
        );

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
    pub fn block_num(&self) -> Result<u64, anyhow::Error> {
        if self.is_empty() {
            return Err(anyhow::anyhow!("empty dataset"));
        }

        let mut block_nums = vec![];
        for table_rows in &self.0 {
            if table_rows.is_empty() {
                continue;
            }
            let block_num = table_rows.unify_block_num()?;
            block_nums.push(block_num);
        }

        // Unwrap: We are not empty.
        let min = block_nums.iter().min().unwrap();
        let max = block_nums.iter().max().unwrap();

        anyhow::ensure!(
            min == max,
            "dataset contains mismatching block_num: {} != {}",
            min,
            max
        );

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
    ) -> impl Future<Output = Result<(), anyhow::Error>> + Send;
}
