//! Physical table revision database operations
//!
//! This module provides a type-safe API for managing physical table revisions (locations)
//! in the metadata database. Each revision represents a specific storage path for a
//! dataset table, with optional writer job tracking and active/inactive status.

pub mod location_id;
pub(crate) mod path;
pub(crate) mod sql;

pub use self::{
    location_id::LocationId,
    path::{Path as TablePath, PathOwned as TablePathOwned},
};
use crate::{
    DatasetName, DatasetNamespace,
    db::Executor,
    error::Error,
    jobs::{Job, JobId},
    manifests::ManifestHash,
    physical_table::name::Name as TableName,
};

/// Idempotently create a physical table revision record.
///
/// Inserts a new record into `physical_table_revisions` with the given path and metadata.
/// If a revision with the same path already exists, returns its existing ID without
/// performing any updates.
///
/// This is a low-level operation that only creates the revision record. It does NOT
/// create or modify `physical_tables` entries, nor does it activate the revision.
#[tracing::instrument(skip(exe), err)]
pub async fn register<'c, E>(
    exe: E,
    dataset_namespace: impl Into<DatasetNamespace<'_>> + std::fmt::Debug + serde::Serialize,
    dataset_name: impl Into<DatasetName<'_>> + std::fmt::Debug + serde::Serialize,
    manifest_hash: impl Into<ManifestHash<'_>> + std::fmt::Debug + serde::Serialize,
    table_name: impl Into<TableName<'_>> + std::fmt::Debug + serde::Serialize,
    path: impl Into<TablePath<'_>> + std::fmt::Debug,
) -> Result<LocationId, Error>
where
    E: Executor<'c>,
{
    let metadata = serde_json::json!({
        "dataset_namespace": dataset_namespace,
        "dataset_name": dataset_name,
        "manifest_hash": manifest_hash,
        "table_name": table_name,
    });
    sql::insert(exe, path.into(), metadata)
        .await
        .map_err(Error::Database)
}

/// Get a physical table revision by its location ID
///
/// Returns `None` if no revision exists with the given location ID.
#[tracing::instrument(skip(exe), err)]
pub async fn get_by_location_id<'c, E>(
    exe: E,
    location_id: impl Into<LocationId> + std::fmt::Debug,
) -> Result<Option<PhysicalTableRevision>, Error>
where
    E: Executor<'c>,
{
    sql::get_by_location_id(exe, location_id.into())
        .await
        .map_err(Error::Database)
}

/// Get an active physical table location with full writer job details
#[tracing::instrument(skip(exe), err)]
pub async fn get_by_location_id_with_details<'c, E>(
    exe: E,
    id: impl Into<LocationId> + std::fmt::Debug,
) -> Result<Option<LocationWithDetails>, Error>
where
    E: Executor<'c>,
{
    sql::get_by_location_id_with_details(exe, id.into())
        .await
        .map_err(Error::Database)
}

/// Look up a location ID by its storage path
///
/// If multiple locations exist with the same path (which shouldn't happen in normal operation),
/// this returns the first match found.
#[tracing::instrument(skip(exe), err)]
pub async fn path_to_id<'c, E>(
    exe: E,
    path: impl Into<TablePath<'_>> + std::fmt::Debug,
) -> Result<Option<LocationId>, Error>
where
    E: Executor<'c>,
{
    sql::path_to_id(exe, path.into())
        .await
        .map_err(Error::Database)
}

/// Get the currently active physical table location for a given table
///
/// Each table can have multiple locations, but only one should be marked as active.
/// This function returns the active location for querying.
#[tracing::instrument(skip(exe), err)]
pub async fn get_active<'c, E>(
    exe: E,
    manifest_hash: impl Into<ManifestHash<'_>> + std::fmt::Debug,
    table_name: impl Into<TableName<'_>> + std::fmt::Debug,
) -> Result<Option<PhysicalTableRevision>, Error>
where
    E: Executor<'c>,
{
    sql::get_active(exe, manifest_hash.into(), table_name.into())
        .await
        .map_err(Error::Database)
}

/// Assign a job as the writer for multiple locations
///
/// This updates the `writer` field for all specified locations, establishing
/// a relationship between the job and the physical table locations it created.
#[tracing::instrument(skip(exe), err)]
pub async fn assign_job_writer<'c, E>(
    exe: E,
    locations: &[LocationId],
    job_id: impl Into<JobId> + std::fmt::Debug,
) -> Result<(), Error>
where
    E: Executor<'c>,
{
    sql::assign_job_writer(exe, locations, job_id.into())
        .await
        .map_err(Error::Database)
}

/// Delete a physical table location by its ID
///
/// This will also delete all associated file_metadata entries due to CASCADE constraints.
///
/// # Cascade Effects
///
/// Deleting a location will also delete:
/// - All file_metadata entries associated with this location
#[tracing::instrument(skip(exe), err)]
pub async fn delete_by_id<'c, E>(
    exe: E,
    id: impl Into<LocationId> + std::fmt::Debug,
) -> Result<bool, Error>
where
    E: Executor<'c>,
{
    sql::delete_by_id(exe, id.into())
        .await
        .map_err(Error::Database)
}

/// List physical table locations with cursor-based pagination
///
/// This function provides an ergonomic interface for paginated listing that automatically
/// handles first page vs subsequent page logic based on the cursor parameter.
#[tracing::instrument(skip(exe), err)]
pub async fn list<'c, E>(
    exe: E,
    limit: i64,
    last_id: Option<impl Into<LocationId> + std::fmt::Debug>,
) -> Result<Vec<PhysicalTableRevision>, Error>
where
    E: Executor<'c>,
{
    match last_id {
        None => sql::list_first_page(exe, limit).await,
        Some(id) => sql::list_next_page(exe, limit, id.into()).await,
    }
    .map_err(Error::Database)
}

/// List all physical table revisions with an optional active status filter
///
/// When `active` is `None`, returns all revisions. When `Some(true)` or `Some(false)`,
/// returns only revisions matching that active status.
#[tracing::instrument(skip(exe), err)]
pub async fn list_all<'c, E>(
    exe: E,
    active: Option<bool>,
    limit: i64,
) -> Result<Vec<PhysicalTableRevision>, Error>
where
    E: Executor<'c>,
{
    sql::list_all(exe, active, limit)
        .await
        .map_err(Error::Database)
}

/// A specific storage revision (location) of a physical table
///
/// Each revision has its own storage path and an optional writer job that
/// populates it.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct PhysicalTableRevision {
    /// Unique identifier for this revision (location ID)
    pub id: LocationId,
    /// Relative path to the storage location
    pub path: TablePathOwned,
    /// Whether this revision is currently active
    pub active: bool,
    /// Writer job responsible for populating this revision, if one exists
    pub writer: Option<JobId>,
    /// Metadata about the revision
    pub metadata: sqlx::types::Json<RevisionMetadata>,
}

/// Metadata stored as JSONB on each physical table revision.
///
/// Captures the dataset context under which the revision was created,
/// enabling lookups without joining back to `physical_tables`.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct RevisionMetadata {
    /// Dataset namespace
    pub dataset_namespace: String,
    /// Dataset name
    pub dataset_name: String,
    /// Manifest hash
    pub manifest_hash: String,
    /// Table name
    #[serde(default)] // TODO: remove this once all revisions have a table_name
    pub table_name: String,
}

/// A physical table combined with its active revision and writer job details
#[derive(Debug, Clone)]
pub struct LocationWithDetails {
    /// The active revision of the physical table
    pub revision: PhysicalTableRevision,
    /// Writer job (if one exists)
    pub writer: Option<Job>,
}

impl LocationWithDetails {
    /// Get the unique identifier for the location
    pub fn id(&self) -> LocationId {
        self.revision.id
    }

    /// Check if this location is currently active for queries
    pub fn active(&self) -> bool {
        self.revision.active
    }
}

/// In-tree integration tests
#[cfg(test)]
mod tests {
    mod it_crud;
    mod it_pagination;
}
