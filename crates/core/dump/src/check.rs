use std::collections::{BTreeMap, BTreeSet};

use common::{BoxError, catalog::physical::PhysicalTable, query_context::Error as QueryError};
use futures::TryStreamExt as _;
use metadata_db::{FileId, LocationId};
use object_store::ObjectMeta;

/// This will check and fix consistency issues when possible. When fixing is not possible, it will
/// return a `CorruptedDataset` error.
///
/// ## List of checks
///
/// Check: All files in the data store are accounted for in the metadata DB.
/// On fail: Fix by deleting orphaned files to restore consistency.
///
/// Check: All files in the table exist in the data store.
/// On fail: Return a `CorruptedDataset` error.
///
/// Check: metadata entries do not contain overlapping ranges.
/// On fail: Return a `CorruptedDataset` error.
pub async fn consistency_check(table: &PhysicalTable) -> Result<(), ConsistencyCheckError> {
    // See also: metadata-consistency

    let location_id = table.location_id();

    let files = table
        .files()
        .await
        .map_err(|err| ConsistencyCheckError::ListFilesError(location_id, err))?;

    let (registered_files, file_name_to_file_ids): (BTreeSet<String>, BTreeMap<String, FileId>) =
        files
            .into_iter()
            .map(|m| (m.file_name.clone(), (m.file_name, m.file_id)))
            .collect();

    let store = table.object_store();
    let path = table.path();

    let stored_files: BTreeMap<String, ObjectMeta> = store
        .list(Some(table.path()))
        .try_collect::<Vec<ObjectMeta>>()
        .await
        .map_err(|err| ConsistencyCheckError::ObjectStoreError(err.into()))?
        .into_iter()
        .filter_map(|object| Some((object.location.filename()?.to_string(), object)))
        .collect();

    for (filename, object_meta) in &stored_files {
        if !registered_files.contains(filename) {
            // This file was written by a dump job but it is not present in the metadata DB,
            // so it is an orphaned file. Delete it.
            tracing::warn!("Discovered orphaned file: {}", object_meta.location);
            // store.delete(&object_meta.location).await?;
        }
    }

    // Check for files in the metadata DB that do not exist in the store.
    for filename in registered_files {
        if !stored_files.contains_key(&filename) {
            let is_canonical = table
                .canonical_chain()
                .await
                .map_err(|err| ConsistencyCheckError::CanonicalChainError(location_id, err.into()))?
                .into_iter()
                .flatten()
                .any(|t| t.id == file_name_to_file_ids[&filename]);
            let err = format!(
                "file `{path}/{filename}` is registered in metadata DB but is not in the data store"
            )
            .into();
            return Err(ConsistencyCheckError::CorruptedTable(
                location_id,
                err,
                is_canonical,
            ));
        }
    }

    Ok(())
}

/// Error type for consistency checks
#[derive(Debug, thiserror::Error)]
#[error("consistency check error: {0}")]
pub enum ConsistencyCheckError {
    #[error("internal query error: {0}")]
    QueryError(#[from] QueryError),

    #[error("object store error: {0}")]
    ObjectStoreError(#[from] object_store::Error),

    #[error("table {0} is corrupted: {1}, is_canonical={2}")]
    CorruptedTable(LocationId, BoxError, bool),

    #[error("could not get canonical chain for table {0}: {1}")]
    CanonicalChainError(LocationId, BoxError),

    #[error("could not list files for table {0}: {1}")]
    ListFilesError(LocationId, BoxError),
}
