//! Dataset registry resource management
//!
//! This module provides database operations for the `manifests` and `tags` tables,
//! which store dataset registration information including namespace, name,
//! version, and manifest data.

use futures::stream::Stream;
use sqlx::{Executor, Postgres};

mod hash;
mod name;
mod namespace;
mod version;

pub use self::{
    hash::{Hash, HashOwned},
    name::{Name, NameOwned},
    namespace::{Namespace, NamespaceOwned},
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
    path: &str,
    hash: Hash<'_>,
) -> Result<(), sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    // Insert into manifest_files, dataset_manifests, and tags tables using CTE
    // Hash is provided by the caller (computed from manifest content)
    let query = indoc::indoc! {r#"
        WITH manifest_insert AS (
          INSERT INTO manifest_files (hash, path)
          VALUES ($5, $4)
          ON CONFLICT (hash) DO NOTHING
          RETURNING hash
        ),
        dataset_manifest_insert AS (
          INSERT INTO dataset_manifests (namespace, name, hash)
          VALUES ($1, $2, $5)
          ON CONFLICT (namespace, name, hash) DO NOTHING
          RETURNING namespace, name, hash
        )
        INSERT INTO tags (namespace, name, version, hash)
        VALUES ($1, $2, $3, $5)
   "#};

    sqlx::query(query)
        .bind(namespace)
        .bind(name)
        .bind(version)
        .bind(path)
        .bind(hash)
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
            t.namespace,
            t.name,
            t.version,
            mf.path
        FROM tags t
        JOIN dataset_manifests dm ON t.namespace = dm.namespace AND t.name = dm.name AND t.hash = dm.hash
        JOIN manifest_files mf ON dm.hash = mf.hash
        WHERE t.name = $1 AND t.version = $2
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
    let query = "SELECT COUNT(*) FROM tags WHERE name = $1 AND version = $2";

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
    let query = indoc::indoc! {r#"
        SELECT mf.path
        FROM tags t
        JOIN dataset_manifests dm ON t.namespace = dm.namespace AND t.name = dm.name AND t.hash = dm.hash
        JOIN manifest_files mf ON dm.hash = mf.hash
        WHERE t.name = $1 AND t.version = $2
    "#};

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
            t.namespace,
            t.name,
            t.version,
            mf.path
        FROM tags t
        JOIN dataset_manifests dm ON t.namespace = dm.namespace AND t.name = dm.name AND t.hash = dm.hash
        JOIN manifest_files mf ON dm.hash = mf.hash
        WHERE t.name = $1
        ORDER BY t.version DESC
        LIMIT 1
    "#};

    let result = sqlx::query_as(query).bind(name).fetch_optional(exe).await?;

    Ok(result)
}

/// List all datasets from the registry
///
/// Returns all dataset records ordered by dataset name ASC and version DESC.
pub async fn list_all<'c, E>(exe: E) -> Result<Vec<Dataset>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT
            namespace,
            name,
            version
        FROM tags
        ORDER BY name ASC, version DESC
    "#};

    sqlx::query_as(query).fetch_all(exe).await
}

/// List all versions for a dataset
///
/// Returns all versions for the specified dataset ordered by version DESC.
pub async fn list_versions_by_name<'c, E>(
    exe: E,
    name: Name<'_>,
) -> Result<Vec<VersionOwned>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT version
        FROM tags
        WHERE name = $1
        ORDER BY version DESC
    "#};

    sqlx::query_scalar(query).bind(name).fetch_all(exe).await
}

/// Stream all datasets from the registry
///
/// Returns a stream of all dataset records with basic information (namespace, name, version),
/// ordered by dataset name first, then by version.
pub fn stream<'e, E>(executor: E) -> impl Stream<Item = Result<Dataset, sqlx::Error>> + 'e
where
    E: Executor<'e, Database = Postgres> + 'e,
{
    let query = indoc::indoc! {r#"
        SELECT
            namespace,
            name,
            version
        FROM tags
        ORDER BY name ASC, version ASC
    "#};

    sqlx::query_as(query).fetch(executor)
}

/// Dataset registry entry with basic information
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Dataset {
    /// Dataset namespace identifier
    pub namespace: String,
    /// Dataset name
    pub name: NameOwned,
    /// Dataset version
    pub version: VersionOwned,
}

/// Dataset registry entry representing a dataset registration
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct DatasetWithDetails {
    /// Dataset namespace identifier
    pub namespace: String,
    /// Dataset name
    pub name: NameOwned,
    /// Dataset version
    pub version: VersionOwned,
    /// Dataset manifest content
    #[sqlx(rename = "path")]
    pub manifest_path: String,
}
