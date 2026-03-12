//! Keeps track of block ranges that have already been scanned. One use of this is an optimization to
//! skip scanned ranges when resuming a write process.
//!
//! These ranges could not be perfectly inferred from tables themselves, because tables can be sparse
//! and not have data for all block numbers. In which case we don't know if a block was never
//! scanned, or if it was scanned and is empty.
//!
//! # Consistency
//! We need to be mindful of consistency. The non-atomic write process is:
//! ```ignore
//! write_real_data();
//! write_metadata_to_db();
//! ```
//! Creating the possibility of orphaned data files if the process is interrupted between the two
//! writes or if the first operation succeeds and the second one errors. An orphaned file means a
//! data file for which the metadata DB does not contain a corresponding range. One way we ensure
//! consistency is by detecting and deleting orphaned files when starting up a write process.
//!
//! See also: metadata-consistency

use amp_data_store::file_name::FileName;
use datasets_common::{block_num::BlockNum, block_range::BlockRange};

use crate::timestamp::Timestamp;

/// A watermark represents a monotonically increasing block number in materialized parquet segments.
pub type Watermark = BlockNum;

/// Key used to store [`ParquetMeta`] JSON in the Parquet file's key-value metadata.
pub const PARQUET_METADATA_KEY: &str = "nozzle_metadata";
/// Key used to store the list of parent file IDs in the Parquet key-value metadata.
pub const PARENT_FILE_ID_METADATA_KEY: &str = "parent_file_ids";
/// Key used to store the compaction generation number in the Parquet key-value metadata.
pub const GENERATION_METADATA_KEY: &str = "generation";

/// File metadata stored in the metadata DB and the KV metadata of the corresponding parquet file.
/// Modifying the serialization of this struct may break compatibility with existing parquet files
/// that have been dumped.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ParquetMeta {
    /// The table name this file belongs to.
    pub table: String,
    /// The object store filename for this Parquet segment.
    pub filename: FileName,
    /// When this file was created.
    pub created_at: Timestamp,
    /// Block ranges covered by this file (currently exactly one entry).
    pub ranges: Vec<BlockRange>,
    /// For single-network segments, this is `None` as the watermark equals the end block number.
    /// For multi-network segments, this represents the cumulative watermark at the end of
    /// this segment.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub watermark: Option<Watermark>,
}
