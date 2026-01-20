//! Progress tracking for datasets
//!
//! This module provides functionality for querying the progress of dataset tables,
//! including current block numbers, job status, and file statistics.

use crate::{
    DatasetNameOwned, DatasetNamespaceOwned, JobStatus, ManifestHash, ManifestHashOwned,
    jobs::JobId, physical_table::TableNameOwned,
};

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

/// Get tables written by a specific job
///
/// Returns a list of active tables where the specified job is the writer,
/// along with metadata about each table including dataset information.
#[tracing::instrument(skip(exe), err)]
pub async fn get_tables_written_by_job<'c, E>(
    exe: E,
    job_id: impl Into<JobId> + std::fmt::Debug,
) -> Result<Vec<JobTableInfo>, crate::Error>
where
    E: crate::Executor<'c>,
{
    sql::get_tables_written_by_job(exe, job_id.into())
        .await
        .map_err(crate::Error::Database)
}

/// Info about a table written by a job
#[derive(Debug, Clone)]
pub struct JobTableInfo {
    /// Name of the table within the dataset
    pub table_name: TableNameOwned,
    /// Manifest hash identifying the dataset version
    pub manifest_hash: ManifestHashOwned,
    /// Dataset namespace
    pub dataset_namespace: DatasetNamespaceOwned,
    /// Dataset name
    pub dataset_name: DatasetNameOwned,
    /// Status of the writer job
    pub job_status: JobStatus,
}

pub(crate) mod sql {
    use sqlx::{Executor, Postgres};

    use super::{JobTableInfo, TableWriterInfo};
    use crate::{
        DatasetNameOwned, DatasetNamespaceOwned, JobStatus, ManifestHash, ManifestHashOwned,
        jobs::JobId, physical_table::TableNameOwned,
    };

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

    /// Query tables written by a specific job
    pub async fn get_tables_written_by_job<'c, E>(
        exe: E,
        job_id: JobId,
    ) -> Result<Vec<JobTableInfo>, sqlx::Error>
    where
        E: Executor<'c, Database = Postgres>,
    {
        let query = indoc::indoc! {r#"
            SELECT
                pt.table_name,
                pt.manifest_hash,
                pt.dataset_namespace,
                pt.dataset_name,
                j.status AS job_status
            FROM physical_tables pt
            INNER JOIN jobs j ON pt.writer = j.id
            WHERE pt.writer = $1 AND pt.active = true
            ORDER BY pt.table_name
        "#};

        #[derive(sqlx::FromRow)]
        struct Row {
            table_name: TableNameOwned,
            manifest_hash: ManifestHashOwned,
            dataset_namespace: DatasetNamespaceOwned,
            dataset_name: DatasetNameOwned,
            job_status: JobStatus,
        }

        let rows: Vec<Row> = sqlx::query_as(query).bind(job_id).fetch_all(exe).await?;

        Ok(rows
            .into_iter()
            .map(|row| JobTableInfo {
                table_name: row.table_name,
                manifest_hash: row.manifest_hash,
                dataset_namespace: row.dataset_namespace,
                dataset_name: row.dataset_name,
                job_status: row.job_status,
            })
            .collect())
    }
}
