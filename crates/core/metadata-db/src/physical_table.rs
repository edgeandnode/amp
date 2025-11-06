//! Physical table database operations

use sqlx::{Executor, Postgres, types::JsonValue};
use url::Url;

pub use self::{
    location_id::{LocationId, LocationIdFromStrError, LocationIdI64ConvError, LocationIdU64Error},
    pagination::{list_first_page, list_next_page},
};
use crate::{
    JobStatus, TableId,
    jobs::{Job, JobId},
    workers::WorkerNodeIdOwned,
};

pub mod events;
mod location_id;
mod pagination;

/// Insert a physical table location into the database and return its ID (idempotent operation)
#[allow(clippy::too_many_arguments)]
pub async fn insert<'c, E>(
    exe: E,
    table: TableId<'_>,
    dataset_namespace: &str,
    dataset_name: &str,
    bucket: Option<&str>,
    path: &str,
    url: &Url,
    active: bool,
) -> Result<LocationId, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    // Upsert with RETURNING id - the no-op update ensures RETURNING works for both insert and conflict cases
    let query = indoc::indoc! {"
        INSERT INTO physical_tables(manifest_hash, table_name, dataset_namespace, dataset_name, bucket, path, url, active)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (url) DO UPDATE SET manifest_hash = EXCLUDED.manifest_hash
        RETURNING id
    "};

    let id: LocationId = sqlx::query_scalar(query)
        .bind(table.manifest_hash)
        .bind(table.table)
        .bind(dataset_namespace)
        .bind(dataset_name)
        .bind(bucket)
        .bind(path)
        .bind(url.as_str())
        .bind(active)
        .fetch_one(exe)
        .await?;
    Ok(id)
}

/// Get a location by its ID
pub async fn get_by_id<'c, E>(exe: E, id: LocationId) -> Result<Option<PhysicalTable>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = "SELECT * FROM physical_tables WHERE id = $1";

    sqlx::query_as(query).bind(id).fetch_optional(exe).await
}

/// Get location ID by URL only, returns first match if multiple exist
pub async fn url_to_id<'c, E>(exe: E, url: &Url) -> Result<Option<LocationId>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = "SELECT id FROM physical_tables WHERE url = $1 LIMIT 1";

    let id: Option<LocationId> = sqlx::query_scalar(query)
        .bind(url.as_str())
        .fetch_optional(exe)
        .await?;
    Ok(id)
}

/// Get a location by its ID
pub async fn get_by_id_with_details<'c, E>(
    exe: E,
    id: LocationId,
) -> Result<Option<LocationWithDetails>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {"
        SELECT
            -- Location fields
            l.id,
            l.manifest_hash,
            l.table_name,
            l.url,
            l.active,
            l.dataset_namespace,
            l.dataset_name,

            -- Writer job fields (optional)
            j.id          AS writer_job_id,
            j.node_id     AS writer_job_node_id,
            j.status      AS writer_job_status,
            j.descriptor  AS writer_job_descriptor,
            j.created_at  AS writer_job_created_at,
            j.updated_at  AS writer_job_updated_at
        FROM physical_tables l
        LEFT JOIN jobs j ON l.writer = j.id
        WHERE l.id = $1
    "};

    // Internal row structure to match the query result
    #[derive(sqlx::FromRow)]
    struct Row {
        id: LocationId,
        manifest_hash: crate::manifests::ManifestHash,
        table_name: String,
        #[sqlx(try_from = "&'a str")]
        url: Url,
        active: bool,
        dataset_namespace: String,
        dataset_name: String,
        writer_job_id: Option<JobId>,
        writer_job_node_id: Option<WorkerNodeIdOwned>,
        writer_job_status: Option<JobStatus>,
        writer_job_descriptor: Option<JsonValue>,
        writer_job_created_at: Option<sqlx::types::chrono::DateTime<sqlx::types::chrono::Utc>>,
        writer_job_updated_at: Option<sqlx::types::chrono::DateTime<sqlx::types::chrono::Utc>>,
    }

    let Some(row) = sqlx::query_as::<_, Row>(query)
        .bind(id)
        .fetch_optional(exe)
        .await?
    else {
        return Ok(None);
    };

    // Construct the writer job if all fields are present
    let writer = match (
        row.writer_job_id,
        row.writer_job_node_id,
        row.writer_job_status,
        row.writer_job_descriptor,
        row.writer_job_created_at,
        row.writer_job_updated_at,
    ) {
        (Some(id), Some(node_id), Some(status), Some(desc), Some(created_at), Some(updated_at)) => {
            Some(Job {
                id,
                node_id,
                status,
                desc,
                created_at,
                updated_at,
            })
        }
        _ => None,
    };

    let location = PhysicalTable {
        id: row.id,
        manifest_hash: row.manifest_hash,
        dataset_namespace: row.dataset_namespace,
        dataset_name: row.dataset_name,
        table_name: row.table_name,
        url: row.url,
        active: row.active,
        writer: writer.as_ref().map(|j| j.id),
    };

    Ok(Some(LocationWithDetails { location, writer }))
}

/// Get the active physical table for a table
#[tracing::instrument(skip(exe), err)]
pub async fn get_active_physical_table<'c, E>(
    exe: E,
    table: TableId<'_>,
) -> Result<Option<PhysicalTable>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {"
        SELECT *
        FROM physical_tables
        WHERE manifest_hash = $1 AND table_name = $2 AND active
    "};

    let table = sqlx::query_as(query)
        .bind(table.manifest_hash)
        .bind(table.table)
        .fetch_optional(exe)
        .await?;

    Ok(table)
}

/// Deactivate all active locations for a specific table
#[tracing::instrument(skip(exe), err)]
pub async fn mark_inactive_by_table_id<'c, E>(exe: E, table: TableId<'_>) -> Result<(), sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {"
        UPDATE physical_tables
        SET active = false
        WHERE manifest_hash = $1 AND table_name = $2 AND active
    "};

    sqlx::query(query)
        .bind(table.manifest_hash)
        .bind(table.table)
        .execute(exe)
        .await?;
    Ok(())
}

/// Activate a specific location by URL (does not deactivate others)
#[tracing::instrument(skip(exe), err)]
pub async fn mark_active_by_id<'c, E>(
    exe: E,
    table_id: TableId<'_>,
    location_id: &LocationId,
) -> Result<(), sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {"
        UPDATE physical_tables
        SET active = true
        WHERE id = $1 AND manifest_hash = $2 AND table_name = $3
    "};

    sqlx::query(query)
        .bind(location_id)
        .bind(table_id.manifest_hash)
        .bind(table_id.table)
        .execute(exe)
        .await?;
    Ok(())
}

/// Get all locations that were written by a specific job
#[tracing::instrument(skip(exe), err)]
pub async fn get_by_job_id<'c, E>(exe: E, job_id: JobId) -> Result<Vec<PhysicalTable>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {"
        SELECT id, manifest_hash, dataset_namespace, dataset_name, table_name, url, active, writer
        FROM physical_tables
        WHERE writer = $1
    "};

    let locations = sqlx::query_as(query).bind(job_id).fetch_all(exe).await?;
    Ok(locations)
}

/// Assign a job as the writer for multiple locations
#[tracing::instrument(skip(exe), err)]
pub async fn assign_job_writer<'c, E>(
    exe: E,
    locations: &[LocationId],
    job_id: JobId,
) -> Result<(), sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {"
        UPDATE physical_tables
        SET writer = $1
        WHERE id = ANY($2)
    "};

    sqlx::query(query)
        .bind(job_id)
        .bind(locations)
        .execute(exe)
        .await?;
    Ok(())
}

/// Delete a location by its ID
///
/// This will also delete all associated file_metadata entries due to CASCADE.
/// Returns true if the location was deleted, false if it didn't exist.
#[tracing::instrument(skip(exe), err)]
pub async fn delete_by_id<'c, E>(exe: E, id: LocationId) -> Result<bool, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {"
        DELETE FROM physical_tables
        WHERE id = $1
    "};

    let result = sqlx::query(query).bind(id).execute(exe).await?;

    Ok(result.rows_affected() > 0)
}

/// Basic location information from the database
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct PhysicalTable {
    /// Unique identifier for the location
    pub id: LocationId,
    /// Manifest hash identifying the dataset version
    pub manifest_hash: crate::manifests::ManifestHash,

    // Labels for the dataset name under which this location was created
    pub dataset_namespace: String,
    pub dataset_name: String,

    /// Name of the table within the dataset
    pub table_name: String,
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
    pub location: PhysicalTable,

    /// Writer job (if one exists)
    pub writer: Option<Job>,
}

impl LocationWithDetails {
    /// Get the unique identifier for the location
    pub fn id(&self) -> LocationId {
        self.location.id
    }

    /// Get the storage URL for this location
    pub fn url(&self) -> &Url {
        &self.location.url
    }

    /// Check if this location is currently active for queries
    pub fn active(&self) -> bool {
        self.location.active
    }
}

/// In-tree integration tests
#[cfg(test)]
mod tests {
    mod it_crud;
    mod it_pagination;
}
