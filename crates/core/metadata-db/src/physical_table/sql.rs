//! SQL wrapper functions for physical table operations
//!
//! This module contains the internal SQL layer for physical table database operations.
//! Functions in this module are generic over `sqlx::Executor` and return `sqlx::Error`.
//! The public API layer (in `physical_table.rs`) wraps these functions with the custom
//! `Executor` trait and converts errors to `metadata_db::Error`.

use sqlx::{Executor, Postgres};

use super::{LocationId, PhysicalTable, PhysicalTableId, name::Name};
use crate::{
    DatasetName, DatasetNameOwned, DatasetNamespace, DatasetNamespaceOwned, ManifestHashOwned,
    jobs::JobId, manifests::ManifestHash, physical_table::TableNameOwned,
};

/// Idempotently upsert a physical table record and return its ID.
///
/// Inserts into `physical_tables` only. On conflict (same namespace, name,
/// manifest_hash, table_name), touches `updated_at` and returns the existing ID.
pub async fn insert<'c, E>(
    exe: E,
    dataset_namespace: DatasetNamespace<'_>,
    dataset_name: DatasetName<'_>,
    manifest_hash: ManifestHash<'_>,
    table_name: Name<'_>,
) -> Result<PhysicalTableId, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {"
        INSERT INTO physical_tables (manifest_hash, table_name, dataset_namespace, dataset_name)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (dataset_namespace, dataset_name, manifest_hash, table_name) DO UPDATE SET updated_at = now()
        RETURNING id
    "};

    let id: PhysicalTableId = sqlx::query_scalar(query)
        .bind(manifest_hash)
        .bind(table_name)
        .bind(dataset_namespace)
        .bind(dataset_name)
        .fetch_one(exe)
        .await?;
    Ok(id)
}

/// Get a physical table revision by location ID with active status
///
/// - Returns `None` if the location ID does not exist
/// - Returns `Some((false, None))` if the location ID exists but is not active
/// - Returns `Some((true, PhysicalTable))` if the location ID exists and is active
pub async fn get_active_by_location_id<'c, E>(
    exe: E,
    id: LocationId,
) -> Result<Option<Option<PhysicalTable>>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {"
        SELECT
            pt.id IS NOT NULL AS active,
            pt.id,
            pt.manifest_hash,
            pt.dataset_namespace,
            pt.dataset_name,
            pt.table_name,
            pt.active_revision_id
        FROM physical_table_revisions ptr
        LEFT JOIN physical_tables pt ON pt.active_revision_id = ptr.id
        WHERE ptr.id = $1
    "};

    #[derive(sqlx::FromRow)]
    struct Row {
        active: bool,
        id: Option<PhysicalTableId>,
        manifest_hash: Option<ManifestHashOwned>,
        dataset_namespace: Option<DatasetNamespaceOwned>,
        dataset_name: Option<DatasetNameOwned>,
        table_name: Option<TableNameOwned>,
        active_revision_id: Option<LocationId>,
    }

    let Some(row) = sqlx::query_as::<_, Row>(query)
        .bind(id)
        .fetch_optional(exe)
        .await?
    else {
        // No revision found for the given location ID
        return Ok(None);
    };

    // Active revision found for the given location ID
    if row.active {
        let err = || sqlx::Error::Protocol("unexpected NULL columns for active revision".into());
        return Ok(Some(Some(PhysicalTable {
            id: row.id.ok_or_else(&err)?,
            manifest_hash: row.manifest_hash.ok_or_else(&err)?,
            dataset_namespace: row.dataset_namespace.ok_or_else(&err)?,
            dataset_name: row.dataset_name.ok_or_else(&err)?,
            table_name: row.table_name.ok_or_else(&err)?,
            active_revision_id: row.active_revision_id,
        })));
    }

    // No active revision found for the given location ID
    Ok(Some(None))
}

/// Deactivate all revisions for a specific table (set active_revision_id to NULL)
pub async fn mark_inactive_by_table_name<'c, E>(
    exe: E,
    dataset_namespace: DatasetNamespace<'_>,
    dataset_name: DatasetName<'_>,
    manifest_hash: ManifestHash<'_>,
    table_name: Name<'_>,
) -> Result<bool, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {"
        UPDATE physical_tables
        SET
            active_revision_id = NULL,
            updated_at = now()
        WHERE
            dataset_namespace = $1
            AND dataset_name = $2
            AND manifest_hash = $3
            AND table_name = $4;
    "};

    let result = sqlx::query(query)
        .bind(dataset_namespace)
        .bind(dataset_name)
        .bind(manifest_hash)
        .bind(table_name)
        .execute(exe)
        .await?;
    Ok(result.rows_affected() > 0)
}

/// Activate a specific location by ID (does not deactivate others)
pub async fn mark_active_by_id<'c, E>(
    exe: E,
    dataset_namespace: DatasetNamespace<'_>,
    dataset_name: DatasetName<'_>,
    manifest_hash: ManifestHash<'_>,
    table_name: Name<'_>,
    location_id: LocationId,
) -> Result<bool, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {"
        UPDATE physical_tables
        SET active_revision_id = $1,
            updated_at = now()
        WHERE dataset_namespace = $2
        AND dataset_name      = $3
        AND manifest_hash     = $4
        AND table_name        = $5;
    "};

    let result = sqlx::query(query)
        .bind(location_id)
        .bind(dataset_namespace)
        .bind(dataset_name)
        .bind(manifest_hash)
        .bind(table_name)
        .execute(exe)
        .await?;
    Ok(result.rows_affected() > 0)
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
