pub mod arrow_helpers;
pub mod attestation;
pub mod catalog;
pub mod config;
pub mod evm;
pub mod js_udf;
pub mod manifest;
pub mod metadata;
pub mod notification_multiplexer;
pub mod plan_visitors;
pub mod query_context;
pub mod sql_visitors;
pub mod store;
pub mod stream_helpers;
pub mod streaming_query;
pub mod tracing_helpers;

use std::{
    future::Future,
    ops::RangeInclusive,
    time::{Duration, SystemTime},
};

use arrow::{array::FixedSizeBinaryArray, datatypes::DataType};
pub use arrow_helpers::*;
pub use catalog::logical::*;
use datafusion::arrow::{
    array::{ArrayRef, AsArray as _, RecordBatch},
    datatypes::{DECIMAL128_MAX_PRECISION, TimeUnit, UInt64Type},
    error::ArrowError,
};
pub use datafusion::{arrow, parquet};
use futures::Stream;
use metadata::segments::BlockRange;
pub use query_context::QueryContext;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
pub use store::Store;

pub type BoxError = Box<dyn std::error::Error + Sync + Send + 'static>;
pub type BoxResult<T> = Result<T, BoxError>;

/// The block number column name.
pub const BLOCK_NUM: &str = "block_num";

/// Special column name for block numbers. These are implicitly selected when doing streaming
/// queries, and in some other cases.
pub const SPECIAL_BLOCK_NUM: &str = "_block_num";

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

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
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

/// A record batch associated with a single block of chain data, for populating raw datasets.
pub struct RawTableRows {
    pub table: Table,
    pub rows: RecordBatch,
    pub range: BlockRange,
}

impl RawTableRows {
    pub fn new(table: Table, range: BlockRange, columns: Vec<ArrayRef>) -> Result<Self, BoxError> {
        let schema = table.schema().clone();
        let rows = RecordBatch::try_new(schema, columns)?;
        Self::check_invariants(&range, &rows)
            .map_err(|err| format!("malformed table {}: {}", table.name(), err))?;
        Ok(RawTableRows { table, rows, range })
    }

    pub fn block_num(&self) -> BlockNum {
        self.range.start()
    }

    fn check_invariants(range: &BlockRange, rows: &RecordBatch) -> Result<(), BoxError> {
        if range.start() != range.end() {
            return Err("block range must contain a single block number".into());
        }
        if rows.num_rows() == 0 {
            return Ok(());
        }

        let block_nums = rows
            .column_by_name(BLOCK_NUM)
            .ok_or("missing block_num column")?;
        let block_nums = block_nums
            .as_primitive_opt::<UInt64Type>()
            .ok_or("block_num column is not uint64")?;

        // Unwrap: `rows` is not empty.
        let start = arrow::compute::kernels::aggregate::min(block_nums).unwrap();
        let end = arrow::compute::kernels::aggregate::max(block_nums).unwrap();
        if start != range.start() {
            return Err(format!("contains unexpected block_num: {}", start).into());
        };
        if end != range.start() {
            return Err(format!("contains unexpected block_num: {}", end).into());
        };

        Ok(())
    }
}

pub struct RawDatasetRows(Vec<RawTableRows>);

impl RawDatasetRows {
    pub fn new(rows: Vec<RawTableRows>) -> Self {
        assert!(!rows.is_empty());
        assert!(rows.iter().skip(1).all(|r| &r.range == &rows[0].range));
        Self(rows)
    }

    pub fn block_num(&self) -> BlockNum {
        self.0[0].block_num()
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
    ) -> impl Future<Output = impl Stream<Item = Result<RawDatasetRows, BoxError>> + Send> + Send;

    fn latest_block(
        &mut self,
        finalized: bool,
    ) -> impl Future<Output = Result<BlockNum, BoxError>> + Send;
}

pub enum DatasetValue {
    Toml(toml::Value),
    Json(serde_json::Value),
}

pub fn block_range_intersection(
    a: RangeInclusive<BlockNum>,
    b: RangeInclusive<BlockNum>,
) -> Option<RangeInclusive<BlockNum>> {
    let start = BlockNum::max(*a.start(), *b.start());
    let end = BlockNum::min(*a.end(), *b.end());
    if start <= end {
        Some(start..=end)
    } else {
        None
    }
}

/// Wrapper to implement [`JsonSchema`] for [`datafusion::arrow::datatypes::DataType`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DataTypeJsonSchema(pub datafusion::arrow::datatypes::DataType);

impl JsonSchema for DataTypeJsonSchema {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "DataType".into()
    }

    fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        let mut schema = String::json_schema(generator);
        schema.as_object_mut().unwrap().insert(
            "description".to_string(),
            serde_json::json!("Arrow data type, e.g. `Int32`, `Utf8`, etc."),
        );
        schema
    }
}
