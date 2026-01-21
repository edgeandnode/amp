pub mod arrow_helpers;
pub mod catalog;
pub mod evm;
pub mod incrementalizer;
pub mod memory_pool;
pub mod metadata;
pub mod plan_visitors;
pub mod planning_context;
pub mod query_context;
pub mod sql;
pub mod sql_str;
pub mod stream_helpers;
pub mod utils;

use std::{
    ops::RangeInclusive,
    time::{Duration, SystemTime},
};

use arrow::{array::FixedSizeBinaryArray, datatypes::DataType};
pub use arrow_helpers::*;
pub use catalog::logical::*;
use datafusion::arrow::{
    datatypes::{DECIMAL128_MAX_PRECISION, TimeUnit},
    error::ArrowError,
};
pub use datafusion::{arrow, parquet};
pub use datasets_common::{BlockNum, SPECIAL_BLOCK_NUM, block_range::BlockRange};
pub use metadata::segments::{ResumeWatermark, Watermark};
pub use planning_context::{DetachedLogicalPlan, PlanningContext};
pub use query_context::{Error as QueryError, QueryContext};
use serde::{Deserialize, Serialize};

pub type BoxError = Box<dyn std::error::Error + Sync + Send + 'static>;
pub type BoxResult<T> = Result<T, BoxError>;
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
