use std::collections::{BTreeMap, BTreeSet};

use common::{catalog::physical::PhysicalTable, query_context::Error as QueryError};
use futures::TryStreamExt as _;
use metadata_db::LocationId;
use object_store::ObjectMeta;

/// Validates consistency between metadata database and object store for a table
///
/// This function performs a comprehensive consistency check to ensure that the metadata
/// database and object store are in sync. It detects and attempts to repair consistency
/// issues where possible, or returns errors for corruption that requires manual intervention.
///
/// ## Consistency Checks Performed
///
/// 1. **Orphaned Files Detection**
///    - Verifies all files in object store are registered in metadata DB
///    - **Action on failure**: Automatically deletes orphaned files to restore consistency
///    - Orphaned files typically result from failed dump operations
///
/// 2. **Missing Registered Files Detection**
///    - Verifies all metadata DB entries have corresponding files in object store
///    - **Action on failure**: Returns [`ConsistencyError::MissingRegisteredFile`]
///    - Indicates data corruption requiring manual intervention
///
/// ## Side Effects
///
/// ⚠️ **Warning**: This function has side effects - it deletes orphaned files from object store.
/// These deletions are logged at `WARN` level before execution.
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

    let stored_files: BTreeMap<String, ObjectMeta> = store
        .list(Some(table.path()))
        .try_collect::<Vec<ObjectMeta>>()
        .await
        .map_err(|err| ConsistencyError::ListObjectStore {
            location_id,
            source: err,
        })?
        .into_iter()
        .filter_map(|object| Some((object.location.filename()?.to_string(), object)))
        .collect();

    // TODO: Move the orphaned files deletion logic out of this check function. This side effect
    //  should be handled somewhere else (e.g., by the garbage collector).
    for (filename, object_meta) in &stored_files {
        if !registered_files.contains(filename) {
            // This file was written by a dump job, but it is not present in the metadata DB,
            // so it is an orphaned file. Delete it.
            tracing::warn!("Deleting orphaned file: {}", object_meta.location);
            store.delete(&object_meta.location).await.map_err(|err| {
                ConsistencyError::DeleteOrphanedFile {
                    location_id,
                    filename: filename.clone(),
                    source: err,
                }
            })?;
        }
    }

    // Check for files in the metadata DB that do not exist in the store.
    for filename in registered_files {
        if !stored_files.contains_key(&filename) {
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

    /// Failed to delete orphaned file from object store
    ///
    /// This occurs when attempting to clean up a file that exists in object store
    /// but is not registered in metadata DB. The file is considered orphaned
    /// (likely from a failed dump operation) and should be deleted to restore
    /// consistency.
    ///
    /// Possible causes:
    /// - Object store connectivity issues during delete
    /// - File already deleted by concurrent process
    /// - Permission/authentication issues
    /// - Object store service unavailable
    ///
    /// This is a critical error - orphaned files indicate incomplete operations
    /// and should be cleaned up to prevent storage bloat.
    #[error(
        "Failed to delete orphaned file '{filename}' from object store for table location {location_id}"
    )]
    DeleteOrphanedFile {
        location_id: LocationId,
        filename: String,
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
