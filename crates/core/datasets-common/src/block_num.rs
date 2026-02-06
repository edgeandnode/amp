//! Block number types for dataset management.

/// Type alias for block numbers used throughout the dataset system.
pub type BlockNum = u64;

/// Reserved column name for block numbers in dataset tables.
///
/// This constant defines the reserved column name (`"_block_num"`) used to identify
/// the block number column in all dataset tables. This standardized column is essential
/// for partitioning, range queries, and incremental processing.
pub const RESERVED_BLOCK_NUM_COLUMN_NAME: &str = "_block_num";
