pub use ::datasets_derived::sql_str;
use arrow::{array::FixedSizeBinaryArray, datatypes::DataType};
pub use datafusion::{arrow, parquet};
pub use datasets_common::{block_num::BlockNum, block_range::BlockRange, end_block::EndBlock};

pub mod amp_catalog_provider;
pub mod catalog;
pub mod context;
pub mod cursor;
pub mod dataset_schema_provider;
pub mod datasets_cache;
pub mod detached_logical_plan;
pub mod exec_env;
pub mod func_catalog;
pub mod incrementalizer;
pub mod memory_pool;
pub mod metadata;
pub mod physical_table;
pub mod plan_table;
pub mod plan_visitors;
pub mod retryable;
pub mod self_schema_provider;
pub mod sql;
pub mod streaming_query;
pub mod udfs;

pub const BYTES32_TYPE: DataType = DataType::FixedSizeBinary(32);
pub type Bytes32ArrayType = FixedSizeBinaryArray;
pub const EVM_ADDRESS_TYPE: DataType = DataType::FixedSizeBinary(20);
