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

use serde::{Deserialize, Serialize};

use crate::Timestamp;

pub const PARQUET_METADATA_KEY: &'static str = "nozzle_metadata";

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ParquetMeta {
    pub table: String,
    pub range_start: u64,
    pub range_end: u64,
    pub filename: String,
    pub created_at: Timestamp,
}
