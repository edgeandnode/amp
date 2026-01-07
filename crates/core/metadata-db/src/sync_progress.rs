//! Sync progress tracking for datasets
//!
//! This module provides functionality for querying the sync progress of dataset tables,
//! including current block numbers, job status, and file statistics.

use crate::{JobStatus, ManifestHash, jobs::JobId, physical_table::TableNameOwned};

/// Get sync progress for all tables in a dataset
///
/// Returns sync progress information for each active table in the dataset,
/// including the current synced block range, job status, and file statistics.
#[tracing::instrument(skip(exe), err)]
pub async fn get_by_manifest_hash<'c, E>(
    exe: E,
    manifest_hash: impl Into<ManifestHash<'_>> + std::fmt::Debug,
) -> Result<Vec<TableSyncProgress>, crate::Error>
where
    E: crate::Executor<'c>,
{
    sql::get_by_manifest_hash(exe, manifest_hash.into())
        .await
        .map_err(crate::Error::Database)
}

/// Sync progress information for a single table
#[derive(Debug, Clone)]
pub struct TableSyncProgress {
    /// Name of the table within the dataset
    pub table_name: TableNameOwned,
    /// Highest block number that has been synced
    pub current_block: Option<i64>,
    /// Lowest block number that has been synced
    pub start_block: Option<i64>,
    /// ID of the writer job (if one exists)
    pub job_id: Option<JobId>,
    /// Status of the writer job (if one exists)
    pub job_status: Option<JobStatus>,
    /// Number of Parquet files written for this table
    pub files_count: i64,
    /// Total size of all Parquet files in bytes
    pub total_size_bytes: i64,
}

pub(crate) mod sql {
    use sqlx::{Executor, Postgres};

    use super::TableSyncProgress;
    use crate::{JobStatus, ManifestHash, jobs::JobId, physical_table::TableNameOwned};

    /// Query sync progress for all active tables in a dataset
    pub async fn get_by_manifest_hash<'c, E>(
        exe: E,
        manifest_hash: ManifestHash<'_>,
    ) -> Result<Vec<TableSyncProgress>, sqlx::Error>
    where
        E: Executor<'c, Database = Postgres>,
    {
        let query = indoc::indoc! {r#"
            SELECT
                pt.table_name,
                pt.writer AS job_id,
                j.status AS job_status,
                COUNT(fm.id) AS files_count,
                COALESCE(SUM(fm.object_size), 0)::bigint AS total_size_bytes,
                MAX((fm.metadata->'ranges'->0->'numbers'->>'end')::bigint) AS current_block,
                MIN((fm.metadata->'ranges'->0->'numbers'->>'start')::bigint) AS start_block
            FROM physical_tables pt
            LEFT JOIN file_metadata fm ON fm.location_id = pt.id
            LEFT JOIN jobs j ON pt.writer = j.id
            WHERE pt.manifest_hash = $1 AND pt.active = true
            GROUP BY pt.table_name, pt.writer, j.status
            ORDER BY pt.table_name
        "#};

        #[derive(sqlx::FromRow)]
        struct Row {
            table_name: TableNameOwned,
            job_id: Option<JobId>,
            job_status: Option<JobStatus>,
            files_count: i64,
            total_size_bytes: i64,
            current_block: Option<i64>,
            start_block: Option<i64>,
        }

        let rows: Vec<Row> = sqlx::query_as(query)
            .bind(manifest_hash)
            .fetch_all(exe)
            .await?;

        Ok(rows
            .into_iter()
            .map(|row| TableSyncProgress {
                table_name: row.table_name,
                current_block: row.current_block,
                start_block: row.start_block,
                job_id: row.job_id,
                job_status: row.job_status,
                files_count: row.files_count,
                total_size_bytes: row.total_size_bytes,
            })
            .collect())
    }
}
