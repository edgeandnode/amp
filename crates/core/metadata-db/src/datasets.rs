//! Dataset registry resource management
//!
//! This module provides database operations for the `registry` table,
//! which stores dataset registration information including namespace, name,
//! version, and manifest data.

use futures::stream::Stream;
use sqlx::{Executor, Postgres};

mod name;
mod namespace;
mod pagination;
mod version;

pub use self::{
    name::{Name, NameOwned},
    namespace::{Namespace, NamespaceOwned},
    pagination::{
        list_first_page, list_next_page, list_versions_by_name_first_page,
        list_versions_by_name_next_page,
    },
    version::{Version, VersionOwned},
};

/// Insert a new dataset registry entry
///
/// The combination of dataset name and version must be unique.
#[tracing::instrument(skip(exe), err)]
pub async fn insert<'c, E>(
    exe: E,
    namespace: Namespace<'_>,
    name: Name<'_>,
    version: Version<'_>,
    manifest_path: &str,
) -> Result<(), sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        INSERT INTO registry (dataset, version, manifest, owner)
        VALUES ($1, $2, $3, $4)
    "#};

    sqlx::query(query)
        .bind(name)
        .bind(version)
        .bind(manifest_path)
        .bind(namespace)
        .execute(exe)
        .await?;

    Ok(())
}

/// Get complete dataset information by name and version
pub async fn get_by_name_and_version_with_details<'c, E>(
    exe: E,
    name: Name<'_>,
    version: Version<'_>,
) -> Result<Option<DatasetWithDetails>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT
            owner,
            dataset,
            version,
            manifest
        FROM registry
        WHERE dataset = $1 AND version = $2
    "#};

    let result = sqlx::query_as(query)
        .bind(name)
        .bind(version)
        .fetch_optional(exe)
        .await?;

    Ok(result)
}

/// Check if a dataset exists for the given name and version
pub async fn exists_by_name_and_version<'c, E>(
    exe: E,
    name: Name<'_>,
    version: Version<'_>,
) -> Result<bool, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = "SELECT COUNT(*) FROM registry WHERE dataset = $1 AND version = $2";

    let result: i64 = sqlx::query_scalar(query)
        .bind(name)
        .bind(version)
        .fetch_one(exe)
        .await?;

    Ok(result > 0)
}

/// Get manifest path by name and version
pub async fn get_manifest_path_by_name_and_version<'c, E>(
    exe: E,
    name: Name<'_>,
    version: Version<'_>,
) -> Result<Option<String>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = "SELECT manifest FROM registry WHERE dataset = $1 AND version = $2";

    let result = sqlx::query_scalar(query)
        .bind(name)
        .bind(version)
        .fetch_optional(exe)
        .await?;

    Ok(result)
}

/// Get the latest version for a dataset
pub async fn get_latest_version_by_name_with_details<'c, E>(
    exe: E,
    name: Name<'_>,
) -> Result<Option<DatasetWithDetails>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT
            owner,
            dataset,
            version,
            manifest
        FROM registry
        WHERE dataset = $1
        ORDER BY version DESC
        LIMIT 1
    "#};

    let result = sqlx::query_as(query).bind(name).fetch_optional(exe).await?;

    Ok(result)
}

/// Stream all datasets from the registry
///
/// Returns a stream of all dataset records with basic information (namespace, name, version),
/// ordered by dataset name first, then by version.
pub fn stream<'e, E>(executor: E) -> impl Stream<Item = Result<Dataset, sqlx::Error>> + 'e
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres> + 'e,
{
    let query = indoc::indoc! {r#"
        SELECT
            owner,
            dataset,
            version
        FROM registry
        ORDER BY dataset ASC, version ASC
    "#};

    sqlx::query_as(query).fetch(executor)
}

/// Dataset registry entry with basic information
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Dataset {
    /// Dataset namespace identifier
    #[sqlx(rename = "owner")]
    pub namespace: String,
    /// Dataset name
    #[sqlx(rename = "dataset")]
    pub name: NameOwned,
    /// Dataset version
    pub version: VersionOwned,
}

/// Dataset registry entry representing a dataset registration
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct DatasetWithDetails {
    /// Dataset namespace identifier
    #[sqlx(rename = "owner")]
    pub namespace: String,
    /// Dataset name
    #[sqlx(rename = "dataset")]
    pub name: NameOwned,
    /// Dataset version
    pub version: VersionOwned,
    /// Dataset manifest content
    #[sqlx(rename = "manifest")]
    pub manifest_path: String,
}

#[cfg(test)]
mod tests {
    mod it_crud;
    mod it_pagination;
}
