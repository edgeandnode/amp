//! Physical table database operations
//!
//! This module provides a type-safe API for managing physical table locations in the metadata database.
//! Physical tables represent actual storage locations (e.g., Parquet files) for dataset tables.

pub mod events;
mod location_id;
mod name;
mod path;
pub(crate) mod sql;

use sqlx::{Postgres, error::BoxDynError};

pub use self::{
    location_id::{LocationId, LocationIdFromStrError, LocationIdI64ConvError, LocationIdU64Error},
    name::{Name as TableName, NameOwned as TableNameOwned},
    path::{Path as TablePath, PathOwned as TablePathOwned},
};
use crate::{
    DatasetName, DatasetNameOwned, DatasetNamespace, DatasetNamespaceOwned, JobStatus,
    ManifestHashOwned,
    db::Executor,
    error::Error,
    jobs::{Job, JobId},
    manifests::ManifestHash,
};

/// Register a new physical table location in the database
///
/// This operation is idempotent - if a location with the same path already exists,
/// its manifest_hash will be updated and the existing location ID will be returned.
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
    sql::insert(
        exe,
        dataset_namespace.into(),
        dataset_name.into(),
        manifest_hash.into(),
        table_name.into(),
        path.into(),
        metadata,
    )
    .await
    .map_err(Error::Database)
}

/// Get an active physical table location by its ID.
///
/// # Errors
///
/// - `GetActiveByLocationIdError::NotFound` if no revision exists with this location ID
/// - `GetActiveByLocationIdError::Inactive` if the revision exists but is not currently active
#[tracing::instrument(skip(exe), err)]
pub async fn get_active_by_location_id<'c, E>(
    exe: E,
    id: impl Into<LocationId> + std::fmt::Debug,
) -> Result<PhysicalTable, Error>
where
    E: Executor<'c>,
{
    let row = sql::get_active_by_location_id(exe, id.into())
        .await
        .map_err(Error::Database)?;

    match row {
        None => Err(Error::GetActiveByLocationId(
            GetActiveByLocationIdError::NotFound,
        )),
        Some(None) => Err(Error::GetActiveByLocationId(
            GetActiveByLocationIdError::Inactive,
        )),
        Some(Some(table)) => Ok(table),
    }
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
pub async fn mark_inactive_by_table_name<'c, E>(
    exe: E,
    dataset_namespace: impl Into<DatasetNamespace<'_>> + std::fmt::Debug,
    dataset_name: impl Into<DatasetName<'_>> + std::fmt::Debug,
    manifest_hash: impl Into<ManifestHash<'_>> + std::fmt::Debug,
    table_name: impl Into<TableName<'_>> + std::fmt::Debug,
) -> Result<bool, Error>
where
    E: Executor<'c>,
{
    sql::mark_inactive_by_table_name(
        exe,
        dataset_namespace.into(),
        dataset_name.into(),
        manifest_hash.into(),
        table_name.into(),
    )
    .await
    .map_err(Error::Database)
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
    location_id: impl Into<LocationId> + std::fmt::Debug,
    dataset_namespace: impl Into<DatasetNamespace<'_>> + std::fmt::Debug,
    dataset_name: impl Into<DatasetName<'_>> + std::fmt::Debug,
    manifest_hash: impl Into<ManifestHash<'_>> + std::fmt::Debug,
    table_name: impl Into<TableName<'_>> + std::fmt::Debug,
) -> Result<bool, Error>
where
    E: Executor<'c>,
{
    sql::mark_active_by_id(
        exe,
        dataset_namespace.into(),
        dataset_name.into(),
        manifest_hash.into(),
        table_name.into(),
        location_id.into(),
    )
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
        .map_err(|err| Error::LocationNotificationSend(events::LocationNotifSendError(err)))
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
        .map_err(|err| Error::Database(err.0))
}

/// Unique identifier for a logical physical table entity in the database.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PhysicalTableId(i64);

impl TryFrom<i64> for PhysicalTableId {
    type Error = PhysicalTableIdI64ConvError;

    /// Attempts to convert an `i64` to a [`PhysicalTableId`] with validation.
    ///
    /// # Errors
    ///
    /// - `PhysicalTableId::NonPositive` if the value is zero or negative
    fn try_from(value: i64) -> Result<Self, Self::Error> {
        if value <= 0 {
            Err(PhysicalTableIdI64ConvError::NonPositive(value))
        } else {
            Ok(Self(value))
        }
    }
}

/// Errors that can occur when converting from `i64` to [`PhysicalTableId`].
#[derive(Debug, thiserror::Error)]
pub enum PhysicalTableIdI64ConvError {
    /// The provided value is zero or negative, but [`PhysicalTableId`] requires positive values.
    #[error("PhysicalTableId must be positive, got: {0}")]
    NonPositive(i64),
}

impl<'r> sqlx::Decode<'r, Postgres> for PhysicalTableId {
    fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, BoxDynError> {
        let id = <i64 as sqlx::Decode<Postgres>>::decode(value)?;
        id.try_into().map_err(|err| Box::new(err) as BoxDynError)
    }
}

impl sqlx::Type<Postgres> for PhysicalTableId {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <i64 as sqlx::Type<Postgres>>::type_info()
    }
}

impl sqlx::postgres::PgHasArrayType for PhysicalTableId {
    fn array_type_info() -> sqlx::postgres::PgTypeInfo {
        <i64 as sqlx::postgres::PgHasArrayType>::array_type_info()
    }
}

/// Logical physical table entity from the database
///
/// Represents a physical table with its dataset metadata and an optional
/// pointer to its currently active revision.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct PhysicalTable {
    /// Unique identifier for the physical table
    pub id: PhysicalTableId,
    /// Manifest hash identifying the dataset version
    pub manifest_hash: ManifestHashOwned,

    /// Labels for the dataset name under which this location was created
    pub dataset_namespace: DatasetNamespaceOwned,
    pub dataset_name: DatasetNameOwned,

    /// Name of the table within the dataset
    pub table_name: TableNameOwned,
    /// Location ID of the currently active revision, if one exists
    pub active_revision_id: Option<LocationId>,
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

/// Writer info for a table
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct TableWriterInfo {
    /// Name of the table within the dataset
    pub table_name: TableNameOwned,
    /// ID of the writer job (if one exists)
    pub job_id: Option<JobId>,
    /// Status of the writer job (if one exists)
    pub job_status: Option<JobStatus>,
}

/// Info about a table associated with a writer
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct WriterTableInfo {
    /// Name of the table within the dataset
    pub table_name: TableNameOwned,
    /// Manifest hash identifying the dataset version
    pub manifest_hash: ManifestHashOwned,
    /// Dataset namespace
    pub dataset_namespace: DatasetNamespaceOwned,
    /// Dataset name
    pub dataset_name: DatasetNameOwned,
}

/// Get active tables with writer info for a dataset
///
/// Returns a list of active tables for the given dataset manifest hash,
/// along with their writer job information.
#[tracing::instrument(skip(exe), err)]
pub async fn get_active_tables_with_writer_info<'c, E>(
    exe: E,
    manifest_hash: impl Into<ManifestHash<'_>> + std::fmt::Debug,
) -> Result<Vec<TableWriterInfo>, Error>
where
    E: Executor<'c>,
{
    sql::get_active_tables_with_writer_info(exe, manifest_hash.into())
        .await
        .map_err(Error::Database)
}

/// Get tables associated with a specific writer
///
/// Returns a list of active tables where the specified writer is assigned,
/// along with metadata about each table including dataset information.
#[tracing::instrument(skip(exe), err)]
pub async fn get_tables_by_writer<'c, E>(
    exe: E,
    writer_id: impl Into<JobId> + std::fmt::Debug,
) -> Result<Vec<WriterTableInfo>, Error>
where
    E: Executor<'c>,
{
    sql::get_tables_by_writer(exe, writer_id.into())
        .await
        .map_err(Error::Database)
}

/// Error type for looking up an active physical table by location ID
///
/// This error is returned when `get_active_by_location_id` cannot return
/// a valid active physical table for the given location ID.
#[derive(Debug, thiserror::Error)]
pub enum GetActiveByLocationIdError {
    /// The location ID does not exist in the database
    ///
    /// This occurs when the provided location ID has no corresponding row
    /// in the `physical_table_revisions` table. The ID may have never existed
    /// or the revision may have been deleted.
    #[error("Location not found")]
    NotFound,

    /// The location exists but is not currently active
    ///
    /// This occurs when the revision exists in `physical_table_revisions` but
    /// no `physical_tables` row references it as the `active_revision_id`.
    /// The revision may have been superseded by a newer revision or explicitly
    /// deactivated.
    #[error("Location exists but is inactive")]
    Inactive,
}

/// In-tree integration tests
#[cfg(test)]
mod tests {
    mod it_crud;
    mod it_pagination;
}
