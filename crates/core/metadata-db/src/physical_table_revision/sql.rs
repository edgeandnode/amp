//! SQL queries for physical table revision operations
//!
//! The public API layer (in `physical_table_revision.rs`) wraps these functions with the custom
//! `Executor` trait and converts errors to `metadata_db::Error`.

use sqlx::{Executor, Postgres, types::JsonValue};

use super::{
    LocationWithDetails, PhysicalTableRevision, RevisionMetadata, RevisionMetadataOwned,
    location_id::LocationId,
    path::{Path, PathOwned},
};
use crate::{
    JobStatus,
    jobs::{Job, JobId},
    manifests::ManifestHash,
    physical_table::name::Name,
    workers::WorkerNodeIdOwned,
};

/// Idempotently create a physical table revision record.
///
/// Uses `INSERT ... ON CONFLICT (path) DO NOTHING` with a CTE fallback
/// to always return the `id` without performing any updates on conflict.
pub async fn insert<'c, E>(
    exe: E,
    path: Path<'_>,
    metadata: RevisionMetadata<'_>,
) -> Result<LocationId, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {"
        WITH ins AS (
            INSERT INTO physical_table_revisions (path, metadata)
            VALUES ($1, $2)
            ON CONFLICT (path) DO NOTHING
            RETURNING id
        )
        SELECT COALESCE(
            (SELECT id FROM ins),
            (SELECT id FROM physical_table_revisions WHERE path = $1)
        ) AS id
    "};

    let id: LocationId = sqlx::query_scalar(query)
        .bind(path)
        .bind(metadata)
        .fetch_one(exe)
        .await?;
    Ok(id)
}

/// Get a physical table revision by its location ID
///
/// Returns `None` if no revision exists with the given location ID.
pub async fn get_by_location_id<'c, E>(
    exe: E,
    id: LocationId,
) -> Result<Option<PhysicalTableRevision>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {"
        SELECT
            ptr.id,
            ptr.path,
            EXISTS (
                SELECT 1
                FROM physical_tables
                WHERE active_revision_id = ptr.id
            ) AS active,
            ptr.writer,
            ptr.metadata
        FROM physical_table_revisions ptr
        WHERE ptr.id = $1;
    "};

    sqlx::query_as(query).bind(id).fetch_optional(exe).await
}

/// Get the active revision for a physical table
pub async fn get_active<'c, E>(
    exe: E,
    manifest_hash: ManifestHash<'_>,
    table_name: Name<'_>,
) -> Result<Option<PhysicalTableRevision>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {"
        SELECT
            ptr.id,
            ptr.path,
            true AS active,
            ptr.writer,
            ptr.metadata
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

/// Get a revision by location ID with full writer job details
pub async fn get_by_location_id_with_details<'c, E>(
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
            ptr.path,
            EXISTS(SELECT 1 FROM physical_tables WHERE active_revision_id = ptr.id) AS active,
            ptr.metadata,

            -- Writer job fields (optional)
            j.id          AS writer_job_id,
            j.node_id     AS writer_job_node_id,
            j.status      AS writer_job_status,
            j.descriptor  AS writer_job_descriptor,
            j.created_at  AS writer_job_created_at,
            j.updated_at  AS writer_job_updated_at
        FROM physical_table_revisions ptr
        LEFT JOIN jobs j ON ptr.writer = j.id
        WHERE ptr.id = $1
    "};

    // Internal row structure to match the query result
    #[derive(sqlx::FromRow)]
    struct Row {
        id: LocationId,
        active: bool,
        metadata: RevisionMetadataOwned,
        path: PathOwned,
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

    let revision = PhysicalTableRevision {
        id: row.id,
        path: row.path,
        active: row.active,
        writer: writer.as_ref().map(|j| j.id),
        metadata: row.metadata,
    };

    Ok(Some(LocationWithDetails { revision, writer }))
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

/// List the first page of physical table revisions
///
/// Returns a paginated list of revisions ordered by ID in descending order (newest first).
/// This function is used to fetch the initial page when no cursor is available.
pub async fn list_first_page<'c, E>(
    exe: E,
    limit: i64,
) -> Result<Vec<PhysicalTableRevision>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT
            ptr.id,
            ptr.path,
            EXISTS(SELECT 1 FROM physical_tables WHERE active_revision_id = ptr.id) AS active,
            ptr.writer,
            ptr.metadata
        FROM physical_table_revisions ptr
        ORDER BY ptr.id DESC
        LIMIT $1
    "#};

    sqlx::query_as(query).bind(limit).fetch_all(exe).await
}

/// List subsequent pages of physical table revisions using cursor-based pagination
///
/// Returns a paginated list of revisions with IDs less than the provided cursor,
/// ordered by ID in descending order (newest first). This implements cursor-based
/// pagination for efficient traversal of large revision lists.
pub async fn list_next_page<'c, E>(
    exe: E,
    limit: i64,
    last_id: LocationId,
) -> Result<Vec<PhysicalTableRevision>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT
            ptr.id,
            ptr.path,
            EXISTS(SELECT 1 FROM physical_tables WHERE active_revision_id = ptr.id) AS active,
            ptr.writer,
            ptr.metadata
        FROM physical_table_revisions ptr
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

/// List all physical table revisions with an optional active status filter
///
/// Returns all revisions ordered by ID in descending order (newest first).
/// When `active` is `None`, all revisions are returned. When `Some(true)` or
/// `Some(false)`, only revisions matching that active status are returned.
pub async fn list_all<'c, E>(
    exe: E,
    active: Option<bool>,
    limit: i64,
) -> Result<Vec<PhysicalTableRevision>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT
            ptr.id,
            ptr.path,
            pt.active_revision_id IS NOT NULL AS active,
            ptr.writer,
            ptr.metadata
        FROM physical_table_revisions AS ptr
        LEFT JOIN physical_tables pt ON pt.active_revision_id = ptr.id
        WHERE (
            $1::boolean IS NULL
            OR (pt.active_revision_id IS NOT NULL) = $1
        )
        ORDER BY ptr.id DESC
        LIMIT $2
    "#};

    sqlx::query_as(query)
        .bind(active)
        .bind(limit)
        .fetch_all(exe)
        .await
}
