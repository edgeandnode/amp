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

use std::{
    ops::RangeInclusive,
    time::{Duration, SystemTime},
};

use arrow::{array::FixedSizeBinaryArray, datatypes::DataType};
pub use catalog::logical::*;
pub use datafusion::{arrow, parquet};
pub use datasets_common::{BlockNum, SPECIAL_BLOCK_NUM, block_range::BlockRange};
pub use metadata::segments::{ResumeWatermark, Watermark};
pub use planning_context::{DetachedLogicalPlan, PlanningContext};
pub use query_context::{Error as QueryError, QueryContext};
use serde::{Deserialize, Serialize};

pub type BoxError = Box<dyn std::error::Error + Sync + Send + 'static>;
pub type BoxResult<T> = Result<T, BoxError>;

pub const BYTES32_TYPE: DataType = DataType::FixedSizeBinary(32);
pub type Bytes32ArrayType = FixedSizeBinaryArray;
pub const EVM_ADDRESS_TYPE: DataType = DataType::FixedSizeBinary(20);

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
