//! Progress tracking for datasets
//!
//! This module provides functionality for querying the progress of dataset tables,
//! including current block numbers, job status, and file statistics.

use crate::{JobStatus, ManifestHash, jobs::JobId, physical_table::TableNameOwned};

/// Get active tables with writer info for a dataset
///
/// Returns a list of active tables for the given dataset manifest hash,
/// along with their writer job information.
#[tracing::instrument(skip(exe), err)]
pub async fn get_active_tables_with_writer_info<'c, E>(
    exe: E,
    manifest_hash: impl Into<ManifestHash<'_>> + std::fmt::Debug,
) -> Result<Vec<TableWriterInfo>, crate::Error>
where
    E: crate::Executor<'c>,
{
    sql::get_active_tables_with_writer_info(exe, manifest_hash.into())
        .await
        .map_err(crate::Error::Database)
}

/// Writer info for a table
#[derive(Debug, Clone)]
pub struct TableWriterInfo {
    /// Name of the table within the dataset
    pub table_name: TableNameOwned,
    /// ID of the writer job (if one exists)
    pub job_id: Option<JobId>,
    /// Status of the writer job (if one exists)
    pub job_status: Option<JobStatus>,
}

pub(crate) mod sql {
    use sqlx::{Executor, Postgres};

    use super::TableWriterInfo;
    use crate::{JobStatus, ManifestHash, jobs::JobId, physical_table::TableNameOwned};

    /// Query active tables and their writer info for a dataset
    pub async fn get_active_tables_with_writer_info<'c, E>(
        exe: E,
        manifest_hash: ManifestHash<'_>,
    ) -> Result<Vec<TableWriterInfo>, sqlx::Error>
    where
        E: Executor<'c, Database = Postgres>,
    {
        let query = indoc::indoc! {r#"
            SELECT
                pt.table_name,
                pt.writer AS job_id,
                j.status AS job_status
            FROM physical_tables pt
            LEFT JOIN jobs j ON pt.writer = j.id
            WHERE pt.manifest_hash = $1 AND pt.active = true
            ORDER BY pt.table_name
        "#};

        #[derive(sqlx::FromRow)]
        struct Row {
            table_name: TableNameOwned,
            job_id: Option<JobId>,
            job_status: Option<JobStatus>,
        }

        let rows: Vec<Row> = sqlx::query_as(query)
            .bind(manifest_hash)
            .fetch_all(exe)
            .await?;

        Ok(rows
            .into_iter()
            .map(|row| TableWriterInfo {
                table_name: row.table_name,
                job_id: row.job_id,
                job_status: row.job_status,
            })
            .collect())
    }
}
