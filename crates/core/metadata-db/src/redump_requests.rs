//! Redump request management
//!
//! This module provides operations for managing redump requests, which are
//! used to trigger re-extraction of specific block ranges for corrupted segments.
//!
//! ## Database Tables
//!
//! - **redump_requests**: Stores pending requests for block range re-extraction
//!
//! ## Workflow
//!
//! 1. Operator creates a redump request via `ampctl dataset redump`
//! 2. Active dump job checks for pending requests in its main loop
//! 3. When found, the dump job expands the range to segment boundaries
//! 4. After successful extraction, the request is deleted

mod request_id;
pub(crate) mod sql;

pub use self::{request_id::RequestId, sql::RedumpRequest};
use crate::{DatasetName, DatasetNamespace, Error, Executor, ManifestHash};

/// Insert a new redump request
///
/// Creates a request for re-extracting blocks in the given range.
/// Returns the request ID on success.
///
/// Returns an error if a request with the same dataset and block range already exists
/// (unique constraint violation).
#[tracing::instrument(skip(exe), err)]
pub async fn insert<'c, E>(
    exe: E,
    namespace: impl Into<DatasetNamespace<'_>> + std::fmt::Debug,
    name: impl Into<DatasetName<'_>> + std::fmt::Debug,
    manifest_hash: impl Into<ManifestHash<'_>> + std::fmt::Debug,
    start_block: u64,
    end_block: u64,
) -> Result<RequestId, Error>
where
    E: Executor<'c>,
{
    sql::insert(
        exe,
        namespace.into(),
        name.into(),
        manifest_hash.into(),
        start_block as i64,
        end_block as i64,
    )
    .await
    .map_err(Error::Database)
}

/// Get all pending redump requests for a dataset
///
/// Returns requests ordered by creation time (oldest first).
/// The active dump job calls this to check for pending work.
#[tracing::instrument(skip(exe), err)]
pub async fn get_pending_for_dataset<'c, E>(
    exe: E,
    namespace: impl Into<DatasetNamespace<'_>> + std::fmt::Debug,
    name: impl Into<DatasetName<'_>> + std::fmt::Debug,
    manifest_hash: impl Into<ManifestHash<'_>> + std::fmt::Debug,
) -> Result<Vec<RedumpRequest>, Error>
where
    E: Executor<'c>,
{
    sql::get_pending_for_dataset(exe, namespace.into(), name.into(), manifest_hash.into())
        .await
        .map_err(Error::Database)
}

/// Delete a redump request by ID
///
/// Called after successful re-dump to remove the processed request.
/// Returns true if the request was found and deleted.
#[tracing::instrument(skip(exe), err)]
pub async fn delete<'c, E>(exe: E, id: RequestId) -> Result<bool, Error>
where
    E: Executor<'c>,
{
    sql::delete_by_id(exe, id).await.map_err(Error::Database)
}

/// Delete multiple redump requests by their IDs
///
/// Called after successful re-dump to remove all processed requests in batch.
/// Returns the number of requests deleted.
#[tracing::instrument(skip(exe), err)]
pub async fn delete_batch<'c, E>(exe: E, ids: &[RequestId]) -> Result<u64, Error>
where
    E: Executor<'c>,
{
    sql::delete_by_ids(exe, ids).await.map_err(Error::Database)
}
