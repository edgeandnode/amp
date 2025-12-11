//! SQL wrapper functions for physical table operations
//!
//! This module contains the internal SQL layer for physical table database operations.
//! Functions in this module are generic over `sqlx::Executor` and return `sqlx::Error`.
//! The public API layer (in `physical_table.rs`) wraps these functions with the custom
//! `Executor` trait and converts errors to `metadata_db::Error`.

use sqlx::{Executor, Postgres, types::JsonValue};

use super::{
    LocationId, LocationWithDetails, PhysicalTable,
    name::{Name, NameOwned},
    url::{Url, UrlOwned},
};
use crate::{
    DatasetName, DatasetNameOwned, DatasetNamespace, DatasetNamespaceOwned, JobStatus,
    ManifestHashOwned,
    jobs::{Job, JobId},
    manifests::ManifestHash,
    workers::WorkerNodeIdOwned,
};

/// Insert a physical table location into the database and return its ID (idempotent operation)
pub async fn insert<'c, E>(
    exe: E,
    dataset_namespace: DatasetNamespace<'_>,
    dataset_name: DatasetName<'_>,
    manifest_hash: ManifestHash<'_>,
    table_name: Name<'_>,
    url: Url<'_>,
    active: bool,
) -> Result<LocationId, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    // Upsert with RETURNING id - the no-op update ensures RETURNING works for both insert and conflict cases
    let query = indoc::indoc! {"
        INSERT INTO physical_tables(manifest_hash, table_name, dataset_namespace, dataset_name, url, active)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (url) DO UPDATE SET manifest_hash = EXCLUDED.manifest_hash
        RETURNING id
    "};

    let id: LocationId = sqlx::query_scalar(query)
        .bind(manifest_hash)
        .bind(table_name)
        .bind(dataset_namespace)
        .bind(dataset_name)
        .bind(url)
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
pub async fn url_to_id<'c, E>(exe: E, url: Url<'_>) -> Result<Option<LocationId>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = "SELECT id FROM physical_tables WHERE url = $1 LIMIT 1";

    let id: Option<LocationId> = sqlx::query_scalar(query)
        .bind(url)
        .fetch_optional(exe)
        .await?;
    Ok(id)
}

/// Get a location by its ID with full writer job details
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
        manifest_hash: ManifestHashOwned,
        table_name: NameOwned,
        url: UrlOwned,
        active: bool,
        dataset_namespace: DatasetNamespaceOwned,
        dataset_name: DatasetNameOwned,
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

    let table = PhysicalTable {
        id: row.id,
        manifest_hash: row.manifest_hash,
        dataset_namespace: row.dataset_namespace,
        dataset_name: row.dataset_name,
        table_name: row.table_name,
        url: row.url,
        active: row.active,
        writer: writer.as_ref().map(|j| j.id),
    };

    Ok(Some(LocationWithDetails { table, writer }))
}

/// Get the active physical table for a table
pub async fn get_active_physical_table<'c, E>(
    exe: E,
    manifest_hash: ManifestHash<'_>,
    table_name: Name<'_>,
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
        .bind(manifest_hash)
        .bind(table_name)
        .fetch_optional(exe)
        .await?;

    Ok(table)
}

/// Deactivate all active locations for a specific table
pub async fn mark_inactive_by_table_id<'c, E>(
    exe: E,
    manifest_hash: ManifestHash<'_>,
    table_name: Name<'_>,
) -> Result<(), sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {"
        UPDATE physical_tables
        SET active = false
        WHERE manifest_hash = $1 AND table_name = $2 AND active
    "};

    sqlx::query(query)
        .bind(manifest_hash)
        .bind(table_name)
        .execute(exe)
        .await?;
    Ok(())
}

/// Activate a specific location by ID (does not deactivate others)
pub async fn mark_active_by_id<'c, E>(
    exe: E,
    manifest_hash: ManifestHash<'_>,
    table_name: Name<'_>,
    location_id: LocationId,
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
        .bind(manifest_hash)
        .bind(table_name)
        .execute(exe)
        .await?;
    Ok(())
}

/// Assign a job as the writer for multiple locations
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

/// List the first page of physical tables
///
/// Returns a paginated list of physical tables ordered by ID in descending order (newest first).
/// This function is used to fetch the initial page when no cursor is available.
pub async fn list_first_page<'c, E>(exe: E, limit: i64) -> Result<Vec<PhysicalTable>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT
            id,
            manifest_hash,
            dataset_namespace,
            dataset_name,
            table_name,
            url,
            active,
            writer
        FROM physical_tables
        ORDER BY id DESC
        LIMIT $1
    "#};

    let res = sqlx::query_as(query).bind(limit).fetch_all(exe).await?;
    Ok(res)
}

/// List subsequent pages of physical tables using cursor-based pagination
///
/// Returns a paginated list of physical tables with IDs less than the provided cursor,
/// ordered by ID in descending order (newest first). This implements cursor-based
/// pagination for efficient traversal of large physical table lists.
pub async fn list_next_page<'c, E>(
    exe: E,
    limit: i64,
    last_id: LocationId,
) -> Result<Vec<PhysicalTable>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT
            id,
            manifest_hash,
            dataset_namespace,
            dataset_name,
            table_name,
            url,
            active,
            writer
        FROM physical_tables
        WHERE id < $2
        ORDER BY id DESC
        LIMIT $1
    "#};

    let res = sqlx::query_as(query)
        .bind(limit)
        .bind(last_id)
        .fetch_all(exe)
        .await?;
    Ok(res)
}
