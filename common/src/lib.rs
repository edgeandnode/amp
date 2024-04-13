pub mod arrow_helpers;
pub mod dataset_context;
pub mod multirange;

pub use datafusion::arrow;
pub use datafusion::parquet;

use std::time::Duration;

use arrow::datatypes::DataType;
use arrow_helpers::TableRow;
use datafusion::arrow::datatypes::{SchemaRef, TimeUnit, DECIMAL128_MAX_PRECISION};

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
    pub rows: Vec<Box<dyn TableRow>>,
}
