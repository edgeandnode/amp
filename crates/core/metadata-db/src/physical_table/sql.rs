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
    path::{Path, PathOwned},
};
use crate::{
    DatasetName, DatasetNameOwned, DatasetNamespace, DatasetNamespaceOwned, JobStatus,
    ManifestHashOwned,
    jobs::{Job, JobId},
    manifests::ManifestHash,
    workers::WorkerNodeIdOwned,
};

/// Insert a physical table revision into the database and return its ID (idempotent operation)
///
/// This operation:
/// 1. Upserts the physical_table (meta) record
/// 2. Inserts the revision record
pub async fn insert<'c, E>(
    exe: E,
    dataset_namespace: DatasetNamespace<'_>,
    dataset_name: DatasetName<'_>,
    manifest_hash: ManifestHash<'_>,
    table_name: Name<'_>,
    path: Path<'_>,
    metadata: JsonValue,
) -> Result<LocationId, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    // Upsert with RETURNING id - the no-op update ensures RETURNING works for both insert and conflict cases
    let query = indoc::indoc! {"
        WITH pt AS (
            INSERT INTO physical_tables (manifest_hash, table_name, dataset_namespace, dataset_name)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (dataset_namespace, dataset_name, manifest_hash, table_name) DO UPDATE SET updated_at = now()
            RETURNING id
        )
        INSERT INTO physical_table_revisions (path, metadata)
        SELECT $5, $6
        FROM pt
        ON CONFLICT (path) DO UPDATE SET updated_at = now()
        RETURNING id
    "};

    let id: LocationId = sqlx::query_scalar(query)
        .bind(manifest_hash)
        .bind(table_name)
        .bind(dataset_namespace)
        .bind(dataset_name)
        .bind(path)
        .bind(metadata)
        .fetch_one(exe)
        .await?;
    Ok(id)
}

/// Get a revision by its ID (Only returns active revision)
pub async fn get_by_id<'c, E>(exe: E, id: LocationId) -> Result<Option<PhysicalTable>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {"
        SELECT
            ptr.id,
            pt.manifest_hash,
            pt.dataset_namespace,
            pt.dataset_name,
            pt.table_name,
            ptr.path,
            (pt.active_revision_id = ptr.id) AS active,
            ptr.writer
        FROM physical_table_revisions ptr
        JOIN physical_tables pt ON pt.active_revision_id = ptr.id
        WHERE ptr.id = $1
    "};

    sqlx::query_as(query).bind(id).fetch_optional(exe).await
}

/// Get revision ID by path only, returns first match if multiple exist
pub async fn path_to_id<'c, E>(exe: E, path: Path<'_>) -> Result<Option<LocationId>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = "SELECT id FROM physical_table_revisions WHERE path = $1 LIMIT 1";

    let id: Option<LocationId> = sqlx::query_scalar(query)
        .bind(path)
        .fetch_optional(exe)
        .await?;
    Ok(id)
}

/// Get a revision by its ID with full writer job details (Only returns active revision)
pub async fn get_by_id_with_details<'c, E>(
    exe: E,
    id: LocationId,
) -> Result<Option<LocationWithDetails>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {"
        SELECT
            -- Revision fields
            ptr.id,
            pt.manifest_hash,
            pt.table_name,
            ptr.path,
            (pt.active_revision_id = ptr.id) AS active,
            pt.dataset_namespace,
            pt.dataset_name,

            -- Writer job fields (optional)
            j.id          AS writer_job_id,
            j.node_id     AS writer_job_node_id,
            j.status      AS writer_job_status,
            j.descriptor  AS writer_job_descriptor,
            j.created_at  AS writer_job_created_at,
            j.updated_at  AS writer_job_updated_at
        FROM physical_table_revisions ptr
        JOIN physical_tables pt ON pt.active_revision_id = ptr.id
        LEFT JOIN jobs j ON ptr.writer = j.id
        WHERE ptr.id = $1
    "};

    // Internal row structure to match the query result
    #[derive(sqlx::FromRow)]
    struct Row {
        id: LocationId,
        manifest_hash: ManifestHashOwned,
        table_name: NameOwned,
        path: PathOwned,
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
        path: row.path,
        active: row.active,
        writer: writer.as_ref().map(|j| j.id),
    };

    Ok(Some(LocationWithDetails { table, writer }))
}

/// Get the active revision for a physical table
pub async fn get_active_physical_table<'c, E>(
    exe: E,
    manifest_hash: ManifestHash<'_>,
    table_name: Name<'_>,
) -> Result<Option<PhysicalTable>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {"
        SELECT
            ptr.id,
            pt.manifest_hash,
            pt.dataset_namespace,
            pt.dataset_name,
            pt.table_name,
            ptr.path,
            true AS active,
            ptr.writer
        FROM physical_tables pt
        JOIN physical_table_revisions ptr ON ptr.id = pt.active_revision_id
        WHERE pt.manifest_hash = $1 AND pt.table_name = $2
    "};

    sqlx::query_as(query)
        .bind(manifest_hash)
        .bind(table_name)
        .fetch_optional(exe)
        .await
}

/// Deactivate all revisions for a specific table (set active_revision_id to NULL)
pub async fn mark_inactive_by_table_id<'c, E>(
    exe: E,
    dataset_namespace: DatasetNamespace<'_>,
    dataset_name: DatasetName<'_>,
    manifest_hash: ManifestHash<'_>,
    table_name: Name<'_>,
) -> Result<(), sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {"
        UPDATE physical_tables
        SET active_revision_id = NULL, updated_at = now()
        WHERE dataset_namespace = $1 AND dataset_name = $2 AND manifest_hash = $3 AND table_name = $4
    "};

    sqlx::query(query)
        .bind(dataset_namespace)
        .bind(dataset_name)
        .bind(manifest_hash)
        .bind(table_name)
        .execute(exe)
        .await?;
    Ok(())
}

/// Activate a specific location by ID (does not deactivate others)
pub async fn mark_active_by_id<'c, E>(
    exe: E,
    dataset_namespace: DatasetNamespace<'_>,
    dataset_name: DatasetName<'_>,
    manifest_hash: ManifestHash<'_>,
    table_name: Name<'_>,
    location_id: LocationId,
) -> Result<(), sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {"
        UPDATE physical_tables
        SET active_revision_id = $1, updated_at = now()
        WHERE dataset_namespace = $2 AND dataset_name = $3 AND manifest_hash = $4 AND table_name = $5
    "};

    sqlx::query(query)
        .bind(location_id)
        .bind(dataset_namespace)
        .bind(dataset_name)
        .bind(manifest_hash)
        .bind(table_name)
        .execute(exe)
        .await?;
    Ok(())
}

/// Assign a job as the writer for multiple revisions
pub async fn assign_job_writer<'c, E>(
    exe: E,
    locations: &[LocationId],
    job_id: JobId,
) -> Result<(), sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {"
        UPDATE physical_table_revisions
        SET writer = $1, updated_at = now()
        WHERE id = ANY($2)
    "};

    sqlx::query(query)
        .bind(job_id)
        .bind(locations)
        .execute(exe)
        .await?;
    Ok(())
}

/// Delete a revision by its ID
///
/// This will also delete all associated file_metadata entries due to CASCADE.
/// Returns true if the revision was deleted, false if it didn't exist.
pub async fn delete_by_id<'c, E>(exe: E, id: LocationId) -> Result<bool, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {"
        DELETE FROM physical_table_revisions
        WHERE id = $1
    "};

    let result = sqlx::query(query).bind(id).execute(exe).await?;

    Ok(result.rows_affected() > 0)
}

/// List the first page of active physical table revisions
///
/// Returns a paginated list of revisions ordered by ID in descending order (newest first).
/// This function is used to fetch the initial page when no cursor is available.
pub async fn list_first_page<'c, E>(exe: E, limit: i64) -> Result<Vec<PhysicalTable>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT
            ptr.id,
            pt.manifest_hash,
            pt.dataset_namespace,
            pt.dataset_name,
            pt.table_name,
            ptr.path,
            (pt.active_revision_id = ptr.id) AS active,
            ptr.writer
        FROM physical_table_revisions ptr
        JOIN physical_tables pt ON pt.active_revision_id = ptr.id
        ORDER BY ptr.id DESC
        LIMIT $1
    "#};

    sqlx::query_as(query).bind(limit).fetch_all(exe).await
}

/// List subsequent pages of active physical table revisions using cursor-based pagination
///
/// Returns a paginated list of revisions with IDs less than the provided cursor,
/// ordered by ID in descending order (newest first). This implements cursor-based
/// pagination for efficient traversal of large revision lists.
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
            ptr.id,
            pt.manifest_hash,
            pt.dataset_namespace,
            pt.dataset_name,
            pt.table_name,
            ptr.path,
            (pt.active_revision_id = ptr.id) AS active,
            ptr.writer
        FROM physical_table_revisions ptr
        JOIN physical_tables pt ON pt.active_revision_id = ptr.id
        WHERE ptr.id < $2
        ORDER BY ptr.id DESC
        LIMIT $1
    "#};

    sqlx::query_as(query)
        .bind(limit)
        .bind(last_id)
        .fetch_all(exe)
        .await
}

/// Query active tables and their writer info for a dataset
pub async fn get_active_tables_with_writer_info<'c, E>(
    exe: E,
    manifest_hash: ManifestHash<'_>,
) -> Result<Vec<super::TableWriterInfo>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT
            pt.table_name,
            ptr.writer AS job_id,
            j.status AS job_status
        FROM physical_tables pt
        JOIN physical_table_revisions ptr ON ptr.id = pt.active_revision_id
        LEFT JOIN jobs j ON ptr.writer = j.id
        WHERE pt.manifest_hash = $1
        ORDER BY pt.table_name
    "#};

    sqlx::query_as(query)
        .bind(manifest_hash)
        .fetch_all(exe)
        .await
}

/// Query tables associated with a specific writer (Only returns active revision)
pub async fn get_tables_by_writer<'c, E>(
    exe: E,
    writer_id: JobId,
) -> Result<Vec<super::WriterTableInfo>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT
            pt.table_name,
            pt.manifest_hash,
            pt.dataset_namespace,
            pt.dataset_name
        FROM physical_table_revisions ptr
        JOIN physical_tables pt ON pt.active_revision_id = ptr.id
        WHERE ptr.writer = $1
        ORDER BY pt.table_name
    "#};

    sqlx::query_as(query).bind(writer_id).fetch_all(exe).await
}
