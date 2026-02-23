//! Physical table database operations
//!
//! This module provides a type-safe API for managing physical table locations in the metadata database.
//! Physical tables represent actual storage locations (e.g., Parquet files) for dataset tables.

pub mod events;

pub(crate) mod name;
pub(crate) mod sql;

use sqlx::{Postgres, error::BoxDynError};

pub use self::name::{Name as TableName, NameOwned as TableNameOwned};
use crate::{
    datasets::{DatasetName, DatasetNameOwned, DatasetNamespace, DatasetNamespaceOwned},
    db::Executor,
    error::Error,
    jobs::{JobId, JobStatus},
    manifests::{ManifestHash, ManifestHashOwned},
    physical_table_revision::location_id::LocationId,
};

/// Idempotently upsert a physical table record in the database.
///
/// Inserts into `physical_tables` only. If a record with the same
/// (namespace, name, manifest_hash, table_name) already exists, touches
/// `updated_at` and returns the existing ID.
#[tracing::instrument(skip(exe), err)]
pub async fn register<'c, E>(
    exe: E,
    dataset_namespace: impl Into<DatasetNamespace<'_>> + std::fmt::Debug,
    dataset_name: impl Into<DatasetName<'_>> + std::fmt::Debug,
    manifest_hash: impl Into<ManifestHash<'_>> + std::fmt::Debug,
    table_name: impl Into<TableName<'_>> + std::fmt::Debug,
) -> Result<PhysicalTableId, Error>
where
    E: Executor<'c>,
{
    sql::insert(
        exe,
        dataset_namespace.into(),
        dataset_name.into(),
        manifest_hash.into(),
        table_name.into(),
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

/// Mark all active physical tables for a table as inactive
///
/// This is typically used before marking a new physical table as active, ensuring
/// only one physical table per table is active at a time.
///
/// # Transaction Boundaries
///
/// This operation should typically be performed within a transaction along with
/// `mark_active_by_id()` to ensure atomicity when switching active physical tables.
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

/// Mark a specific physical table as active
///
/// This does not automatically deactivate other physical tables. Use `mark_inactive_by_table_name()`
/// first within a transaction to ensure only one physical table is active.
///
/// # Transaction Boundaries
///
/// This operation should typically be performed within a transaction along with
/// `mark_inactive_by_table_name()` to ensure atomicity when switching active physical tables.
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
}
