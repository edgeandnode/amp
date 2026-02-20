//! Internal SQL operations for redump request management

use sqlx::{
    Executor, Postgres,
    types::chrono::{DateTime, Utc},
};

use super::request_id::RequestId;
use crate::{DatasetName, DatasetNamespace, ManifestHash};

/// A pending redump request from the database
#[derive(Debug, Clone)]
pub struct RedumpRequest {
    pub id: RequestId,
    pub dataset_namespace: String,
    pub dataset_name: String,
    pub manifest_hash: String,
    pub start_block: i64,
    pub end_block: i64,
    pub created_at: DateTime<Utc>,
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for RedumpRequest {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> Result<Self, sqlx::Error> {
        use sqlx::Row;
        Ok(Self {
            id: row.try_get("id")?,
            dataset_namespace: row.try_get("dataset_namespace")?,
            dataset_name: row.try_get("dataset_name")?,
            manifest_hash: row.try_get("manifest_hash")?,
            start_block: row.try_get("start_block")?,
            end_block: row.try_get("end_block")?,
            created_at: row.try_get("created_at")?,
        })
    }
}

/// Insert a new redump request
///
/// Returns the new request ID. Returns a unique constraint violation error
/// if a request with the same dataset and block range already exists.
pub async fn insert<'c, E>(
    exe: E,
    namespace: DatasetNamespace<'_>,
    name: DatasetName<'_>,
    manifest_hash: ManifestHash<'_>,
    start_block: i64,
    end_block: i64,
) -> Result<RequestId, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        INSERT INTO redump_requests (dataset_namespace, dataset_name, manifest_hash, start_block, end_block)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING id
    "#};

    sqlx::query_scalar(query)
        .bind(namespace)
        .bind(name)
        .bind(manifest_hash)
        .bind(start_block)
        .bind(end_block)
        .fetch_one(exe)
        .await
}

/// Get all pending redump requests for a dataset
///
/// Returns requests ordered by creation time (oldest first).
pub async fn get_pending_for_dataset<'c, E>(
    exe: E,
    namespace: DatasetNamespace<'_>,
    name: DatasetName<'_>,
    manifest_hash: ManifestHash<'_>,
) -> Result<Vec<RedumpRequest>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT id, dataset_namespace, dataset_name, manifest_hash, start_block, end_block, created_at
        FROM redump_requests
        WHERE dataset_namespace = $1 AND dataset_name = $2 AND manifest_hash = $3
        ORDER BY created_at ASC
    "#};

    sqlx::query_as(query)
        .bind(namespace)
        .bind(name)
        .bind(manifest_hash)
        .fetch_all(exe)
        .await
}

/// Delete a redump request by ID
///
/// Called after successful re-dump to remove the processed request.
/// Returns true if the request was found and deleted, false if not found.
pub async fn delete_by_id<'c, E>(exe: E, id: RequestId) -> Result<bool, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = "DELETE FROM redump_requests WHERE id = $1";

    let result = sqlx::query(query).bind(id).execute(exe).await?;
    Ok(result.rows_affected() > 0)
}

/// Delete multiple redump requests by their IDs
///
/// Called after successful re-dump to remove all processed requests in batch.
/// Returns the number of requests deleted.
pub async fn delete_by_ids<'c, E>(exe: E, ids: &[RequestId]) -> Result<u64, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    if ids.is_empty() {
        return Ok(0);
    }

    let query = "DELETE FROM redump_requests WHERE id = ANY($1)";

    let result = sqlx::query(query).bind(ids).execute(exe).await?;
    Ok(result.rows_affected())
}
