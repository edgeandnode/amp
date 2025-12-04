use std::collections::BTreeSet;

use common::{catalog::physical::PhysicalTable, query_context::Error as QueryError};
use futures::TryStreamExt as _;
use metadata_db::LocationId;
use object_store::ObjectMeta;

/// Validates consistency between metadata database and object store for a table
///
/// This function performs a consistency check to ensure that all files registered
/// in the metadata database exist in the object store.
///
/// ## Consistency Checks Performed
///
/// **Missing Registered Files Detection**
/// - Verifies all metadata DB entries have corresponding files in object store
/// - **Action on failure**: Returns [`ConsistencyError::MissingRegisteredFile`]
/// - Indicates data corruption requiring manual intervention
///
/// ## Note on Orphaned Files
///
/// Files that exist in the object store but are not registered in the metadata DB
/// are intentionally NOT deleted by this function. These files may be pending
/// garbage collection via the `gc_manifest` table. The garbage collector handles
/// cleanup of such files after their expiration period.
pub async fn consistency_check(table: &PhysicalTable) -> Result<(), ConsistencyError> {
    // See also: metadata-consistency

    let location_id = table.location_id();

    let files = table
        .files()
        .await
        .map_err(|err| ConsistencyError::GetTableFiles {
            location_id,
            source: QueryError::DatasetError(err),
        })?;

    let registered_files: BTreeSet<String> = files.into_iter().map(|m| m.file_name).collect();

    let store = table.object_store();
    let path = table.path();

    let stored_files: BTreeSet<String> = store
        .list(Some(table.path()))
        .try_collect::<Vec<ObjectMeta>>()
        .await
        .map_err(|err| ConsistencyError::ListObjectStore {
            location_id,
            source: err,
        })?
        .into_iter()
        .filter_map(|object| object.location.filename().map(|s| s.to_string()))
        .collect();

    // Check for files in the metadata DB that do not exist in the store.
    for filename in registered_files {
        if !stored_files.contains(&filename) {
            return Err(ConsistencyError::MissingRegisteredFile {
                location_id,
                filename,
                path: path.to_string(),
            });
        }
    }

    Ok(())
}

/// Errors that occur during consistency checks
///
/// This error type is used by `consistency_check()` to report issues
/// found when validating consistency between metadata DB and object store.
#[derive(Debug, thiserror::Error)]
pub enum ConsistencyError {
    /// Failed to retrieve file list from metadata database
    ///
    /// This occurs when the metadata database cannot be queried to get the list
    /// of files registered for a table. This typically indicates:
    /// - Database connection issues
    /// - Corrupted metadata entries
    /// - Missing table metadata
    ///
    /// The consistency check cannot proceed without this information.
    #[error("Failed to get file list from metadata database for table location {location_id}")]
    GetTableFiles {
        location_id: LocationId,
        #[source]
        source: QueryError,
    },

    /// Failed to list files in object store
    ///
    /// This occurs when the object store cannot list the files in the table's
    /// storage location. Common causes:
    /// - Object store connectivity issues
    /// - Invalid or inaccessible storage path
    /// - Permission/authentication failures
    /// - Object store service unavailable
    ///
    /// The consistency check cannot proceed without knowing which files exist.
    #[error("Failed to list files in object store for table location {location_id}")]
    ListObjectStore {
        location_id: LocationId,
        #[source]
        source: object_store::Error,
    },

    /// Registered file missing from object store (data corruption)
    ///
    /// This occurs when a file is registered in the metadata database but does
    /// not exist in the object store. This indicates data loss or corruption:
    /// - File was accidentally deleted from object store
    /// - Metadata DB entry exists but file was never written
    /// - Storage backend failure corrupted/lost the file
    /// - Inconsistent state after failed operations
    ///
    /// This is a CRITICAL error indicating data corruption. The table cannot
    /// be queried reliably as expected data is missing. Manual intervention
    /// is required:
    /// 1. Restore file from backup if available
    /// 2. Remove metadata entry if file is unrecoverable
    /// 3. Re-dump affected block ranges to regenerate missing data
    ///
    /// The operation is NOT safe to retry automatically.
    #[error(
        "File '{path}/{filename}' registered in metadata DB but missing from object store for table location {location_id}"
    )]
    MissingRegisteredFile {
        location_id: LocationId,
        filename: String,
        path: String,
    },
}
