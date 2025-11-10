//! Physical table database operations
//!
//! This module provides a type-safe API for managing physical table locations in the metadata database.
//! Physical tables represent actual storage locations (e.g., Parquet files) for dataset tables.

use url::Url;

pub mod events;
mod location_id;
mod name;
pub(crate) mod sql;

pub use self::{
    location_id::{LocationId, LocationIdFromStrError, LocationIdI64ConvError, LocationIdU64Error},
    name::{Name as TableName, NameOwned as TableNameOwned},
};
use crate::{
    DatasetName, DatasetNameOwned, DatasetNamespace, DatasetNamespaceOwned, ManifestHashOwned,
    db::Executor,
    error::Error,
    jobs::{Job, JobId},
    manifests::ManifestHash,
};

/// Register a new physical table location in the database
///
/// This operation is idempotent - if a location with the same URL already exists,
/// its manifest_hash will be updated and the existing location ID will be returned.
#[expect(clippy::too_many_arguments)]
#[tracing::instrument(skip(exe), err)]
pub async fn register<'c, E>(
    exe: E,
    table_id: TableId<'_>,
    dataset_namespace: impl Into<DatasetNamespace<'_>> + std::fmt::Debug,
    dataset_name: impl Into<DatasetName<'_>> + std::fmt::Debug,
    bucket: Option<&str>,
    path: &str,
    url: &Url,
    active: bool,
) -> Result<LocationId, Error>
where
    E: Executor<'c>,
{
    sql::insert(
        exe,
        table_id.manifest_hash,
        table_id.table,
        dataset_namespace.into(),
        dataset_name.into(),
        bucket,
        path,
        url,
        active,
    )
    .await
    .map_err(Error::DbError)
}

/// Get a physical table location by its ID
#[tracing::instrument(skip(exe), err)]
pub async fn get_by_id<'c, E>(
    exe: E,
    id: impl Into<LocationId> + std::fmt::Debug,
) -> Result<Option<PhysicalTable>, Error>
where
    E: Executor<'c>,
{
    sql::get_by_id(exe, id.into()).await.map_err(Error::DbError)
}

/// Get a physical table location with full writer job details
#[tracing::instrument(skip(exe), err)]
pub async fn get_by_id_with_details<'c, E>(
    exe: E,
    id: impl Into<LocationId> + std::fmt::Debug,
) -> Result<Option<LocationWithDetails>, Error>
where
    E: Executor<'c>,
{
    sql::get_by_id_with_details(exe, id.into())
        .await
        .map_err(Error::DbError)
}

/// Look up a location ID by its storage URL
///
/// If multiple locations exist with the same URL (which shouldn't happen in normal operation),
/// this returns the first match found.
#[tracing::instrument(skip(exe), err)]
pub async fn url_to_id<'c, E>(exe: E, url: &Url) -> Result<Option<LocationId>, Error>
where
    E: Executor<'c>,
{
    sql::url_to_id(exe, url).await.map_err(Error::DbError)
}

/// Get the currently active physical table location for a given table
///
/// Each table can have multiple locations, but only one should be marked as active.
/// This function returns the active location for querying.
#[tracing::instrument(skip(exe), err)]
pub async fn get_active_physical_table<'c, E>(
    exe: E,
    table_id: TableId<'_>,
) -> Result<Option<PhysicalTable>, Error>
where
    E: Executor<'c>,
{
    sql::get_active_physical_table(exe, table_id.manifest_hash, table_id.table)
        .await
        .map_err(Error::DbError)
}

/// Mark all active locations for a table as inactive
///
/// This is typically used before marking a new location as active, ensuring
/// only one location per table is active at a time.
///
/// # Transaction Boundaries
///
/// This operation should typically be performed within a transaction along with
/// `mark_active_by_id()` to ensure atomicity when switching active locations.
#[tracing::instrument(skip(exe), err)]
pub async fn mark_inactive_by_table_id<'c, E>(exe: E, table_id: TableId<'_>) -> Result<(), Error>
where
    E: Executor<'c>,
{
    sql::mark_inactive_by_table_id(exe, table_id.manifest_hash, table_id.table)
        .await
        .map_err(Error::DbError)
}

/// Mark a specific location as active
///
/// This does not automatically deactivate other locations. Use `mark_inactive_by_table_id()`
/// first within a transaction to ensure only one location is active.
///
/// # Transaction Boundaries
///
/// This operation should typically be performed within a transaction along with
/// `mark_inactive_by_table_id()` to ensure atomicity when switching active locations.
#[tracing::instrument(skip(exe), err)]
pub async fn mark_active_by_id<'c, E>(
    exe: E,
    table_id: TableId<'_>,
    location_id: impl Into<LocationId> + std::fmt::Debug,
) -> Result<(), Error>
where
    E: Executor<'c>,
{
    sql::mark_active_by_id(
        exe,
        table_id.manifest_hash,
        table_id.table,
        location_id.into(),
    )
    .await
    .map_err(Error::DbError)
}

/// Get all physical table locations that were written by a specific job
#[tracing::instrument(skip(exe), err)]
pub async fn get_by_job_id<'c, E>(
    exe: E,
    job_id: impl Into<JobId> + std::fmt::Debug,
) -> Result<Vec<PhysicalTable>, Error>
where
    E: Executor<'c>,
{
    sql::get_by_job_id(exe, job_id.into())
        .await
        .map_err(Error::DbError)
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
        .map_err(Error::DbError)
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
        .map_err(Error::DbError)
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
) -> Result<Vec<PhysicalTable>, Error>
where
    E: Executor<'c>,
{
    match last_id {
        None => sql::list_first_page(exe, limit).await,
        Some(id) => sql::list_next_page(exe, limit, id.into()).await,
    }
    .map_err(Error::DbError)
}

/// Listen for location change notifications
///
/// Creates a new PostgreSQL LISTEN connection to receive notifications when
/// location data changes in the database. This enables real-time cache
/// invalidation and data refresh.
#[tracing::instrument(skip(metadata_db), err)]
pub async fn listen_for_location_change_notif(
    metadata_db: &crate::MetadataDb,
) -> Result<events::LocationNotifListener, Error> {
    events::listen_url(&metadata_db.url)
        .await
        .map_err(|err| Error::LocationNotificationSendError(events::LocationNotifSendError(err)))
}

/// Send a location change notification
///
/// Sends a notification to all listeners that a location has changed.
/// This is used to trigger cache invalidation and data refresh.
#[tracing::instrument(skip(exe), err)]
pub async fn send_location_change_notif<'c, E>(
    exe: E,
    location_id: impl Into<LocationId> + std::fmt::Debug,
) -> Result<(), Error>
where
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    events::notify(exe, location_id.into())
        .await
        .map_err(|err| Error::DbError(err.0))
}

/// Logical tables are identified by the tuple: `(manifest_hash, table)`. For each logical table, there
/// is at most one active physical_table entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableId<'a> {
    pub manifest_hash: ManifestHash<'a>,
    pub table: TableName<'a>,
}

impl<'a> TableId<'a> {
    /// Create a new TableId with flexible parameter types
    pub fn new(
        manifest_hash: impl Into<ManifestHash<'a>>,
        table: impl Into<TableName<'a>>,
    ) -> Self {
        Self {
            manifest_hash: manifest_hash.into(),
            table: table.into(),
        }
    }
}

/// Basic location information from the database
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct PhysicalTable {
    /// Unique identifier for the location
    pub id: LocationId,
    /// Manifest hash identifying the dataset version
    pub manifest_hash: ManifestHashOwned,

    // Labels for the dataset name under which this location was created
    pub dataset_namespace: DatasetNamespaceOwned,
    pub dataset_name: DatasetNameOwned,

    /// Name of the table within the dataset
    pub table_name: TableNameOwned,
    /// Full URL to the storage location
    #[sqlx(try_from = "&'a str")]
    pub url: Url,
    /// Whether this location is currently active for queries
    pub active: bool,
    /// Writer job ID (if one exists)
    pub writer: Option<JobId>,
}

/// Location information with detailed writer job information
#[derive(Debug, Clone)]
pub struct LocationWithDetails {
    pub table: PhysicalTable,

    /// Writer job (if one exists)
    pub writer: Option<Job>,
}

impl LocationWithDetails {
    /// Get the unique identifier for the location
    pub fn id(&self) -> LocationId {
        self.table.id
    }

    /// Get the storage URL for this location
    pub fn url(&self) -> &Url {
        &self.table.url
    }

    /// Check if this location is currently active for queries
    pub fn active(&self) -> bool {
        self.table.active
    }
}

/// In-tree integration tests
#[cfg(test)]
mod tests {
    mod it_crud;
    mod it_pagination;
}
