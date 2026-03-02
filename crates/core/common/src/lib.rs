use std::time::{Duration, SystemTime};

pub use ::datasets_derived::sql_str;
use arrow::{array::FixedSizeBinaryArray, datatypes::DataType};
pub use datafusion::{arrow, parquet};
pub use datasets_common::{block_num::BlockNum, block_range::BlockRange, end_block::EndBlock};

pub mod block_num;
pub mod catalog;
pub mod context;
pub mod cursor;
pub mod dataset_store;
pub mod datasets_derived;
pub mod detached_logical_plan;
pub mod evm;
pub mod exec_env;
pub mod func_catalog;
pub mod incrementalizer;
pub mod memory_pool;
pub mod metadata;
pub mod physical_table;
pub mod plan_table;
pub mod plan_visitors;
pub mod sql;
pub mod stream_helpers;
pub mod streaming_query;

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
