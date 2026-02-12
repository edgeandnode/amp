use std::{
    ops::RangeInclusive,
    time::{Duration, SystemTime},
};

pub use ::datasets_derived::sql_str;
use arrow::{array::FixedSizeBinaryArray, datatypes::DataType};
pub use datafusion::{arrow, parquet};
pub use datasets_common::{block_num::BlockNum, block_range::BlockRange, end_block::EndBlock};

pub mod catalog;
pub mod context;
pub mod dataset_store;
pub mod datasets_derived;
pub mod evm;
pub mod incrementalizer;
pub mod memory_pool;
pub mod metadata;
pub mod plan_visitors;
pub mod sql;
pub mod stream_helpers;
pub mod streaming_query;

pub use self::{
    catalog::logical::{LogicalCatalog, LogicalTable},
    metadata::segments::{ResumeWatermark, Watermark},
};

pub const BYTES32_TYPE: DataType = DataType::FixedSizeBinary(32);
pub type Bytes32ArrayType = FixedSizeBinaryArray;
pub const EVM_ADDRESS_TYPE: DataType = DataType::FixedSizeBinary(20);

#[derive(Debug, Clone, Copy, Default, serde::Deserialize, serde::Serialize)]
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
