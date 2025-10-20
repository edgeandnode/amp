//! Tag resource management
//!
//! This module provides database operations for the `tags` table,
//! which stores version tags for dataset manifests.
//!
//! ## What is a Tag?
//!
//! A "tag" is the common denomination for human-readable identifiers that point to
//! specific dataset manifests. Tags serve as stable references that can be used to
//! retrieve dataset versions without needing to know the underlying manifest hash.
//!
//! ### Tag Types
//!
//! Tags can be one of the following:
//!
//! - **Semantic Versions (semver)**: Explicit version numbers following semantic versioning
//!   (e.g., `"1.0.0"`, `"2.3.1"`, `"0.1.0-beta"`). These are immutable references to
//!   specific dataset releases.
//!
//! - **Special Tags**: Mutable tags that point to specific releases based on conventions:
//!   - `"latest"`: Points to the most recent stable release (highest semver version)
//!   - `"dev"`: Points to the latest manifest hash in time (most recently created)
//!
//! All tag types share the same underlying structure and are stored in the same `tags` table,
//! providing a unified interface for referencing dataset versions.

use futures::stream::Stream;
use sqlx::{
    Executor, Postgres,
    types::chrono::{DateTime, Utc},
};

use super::{
    hash::{Hash, HashOwned},
    name::{Name, NameOwned},
    namespace::{Namespace, NamespaceOwned},
    version::{Version, VersionOwned},
};

/// Insert a new tag
///
/// Creates a version tag that points to a specific dataset-manifest combination.
/// The combination of (namespace, name, version) must be unique.
///
/// Note: This function assumes the dataset-manifest link already exists in the
/// dataset_manifests table. Violating this constraint will result in a foreign key error.
pub async fn insert<'c, E>(
    exe: E,
    namespace: &Namespace<'_>,
    name: &Name<'_>,
    version: &Version<'_>,
    hash: &Hash<'_>,
) -> Result<(), sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        INSERT INTO tags (namespace, name, version, hash)
        VALUES ($1, $2, $3, $4)
   "#};

    sqlx::query(query)
        .bind(namespace)
        .bind(name)
        .bind(version)
        .bind(hash)
        .execute(exe)
        .await?;

    Ok(())
}

/// Get tag with manifest details by namespace, name and version
pub async fn get_by_namespace_name_and_version_with_details<'c, E>(
    exe: E,
    namespace: Namespace<'_>,
    name: Name<'_>,
    version: Version<'_>,
) -> Result<Option<Tag>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT
            t.namespace,
            t.name,
            t.version,
            t.hash,
            m.path,
            t.created_at,
            t.updated_at
        FROM tags t
        JOIN dataset_manifests dm ON t.namespace = dm.namespace AND t.name = dm.name AND t.hash = dm.hash
        JOIN manifest_files m ON dm.hash = m.hash
        WHERE t.namespace = $1 AND t.name = $2 AND t.version = $3
    "#};

    let result = sqlx::query_as(query)
        .bind(namespace)
        .bind(name)
        .bind(version)
        .fetch_optional(exe)
        .await?;

    Ok(result)
}

/// Check if a tag exists for the given namespace, name and version
pub async fn exists_by_namespace_name_and_version<'c, E>(
    exe: E,
    namespace: Namespace<'_>,
    name: Name<'_>,
    version: Version<'_>,
) -> Result<bool, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = "SELECT COUNT(*) FROM tags WHERE namespace = $1 AND name = $2 AND version = $3";

    let result: i64 = sqlx::query_scalar(query)
        .bind(namespace)
        .bind(name)
        .bind(version)
        .fetch_one(exe)
        .await?;

    Ok(result > 0)
}

/// Get manifest hash by namespace, name and version
///
/// Resolves the manifest hash for a specific dataset version without fetching
/// additional manifest details. This is more efficient than full tag queries
/// when only the hash is needed.
pub async fn get_hash_by_namespace_name_version<'c, E>(
    exe: E,
    namespace: &Namespace<'_>,
    name: &Name<'_>,
    version: &Version<'_>,
) -> Result<Option<HashOwned>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = "SELECT hash FROM tags WHERE namespace = $1 AND name = $2 AND version = $3";

    sqlx::query_scalar(query)
        .bind(namespace)
        .bind(name)
        .bind(version)
        .fetch_optional(exe)
        .await
}

/// Get the latest version tag for a dataset by namespace and name
pub async fn get_latest_by_namespace_and_name<'c, E>(
    exe: E,
    namespace: Namespace<'_>,
    name: Name<'_>,
) -> Result<Option<Tag>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT
            t.namespace,
            t.name,
            t.version,
            t.hash,
            m.path,
            t.created_at,
            t.updated_at
        FROM tags t
        JOIN dataset_manifests dm ON t.namespace = dm.namespace AND t.name = dm.name AND t.hash = dm.hash
        JOIN manifest_files m ON dm.hash = m.hash
        WHERE t.namespace = $1 AND t.name = $2
        ORDER BY t.version DESC
        LIMIT 1
    "#};

    let result = sqlx::query_as(query)
        .bind(namespace)
        .bind(name)
        .fetch_optional(exe)
        .await?;

    Ok(result)
}

/// List all tags from the registry
///
/// Returns all tag records ordered by version DESC.
pub async fn list_all<'c, E>(exe: E) -> Result<Vec<Tag>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT
            t.namespace,
            t.name,
            t.version,
            t.hash,
            m.path,
            t.created_at,
            t.updated_at
        FROM tags t
        JOIN dataset_manifests dm ON t.namespace = dm.namespace AND t.name = dm.name AND t.hash = dm.hash
        JOIN manifest_files m ON dm.hash = m.hash
        ORDER BY t.version DESC
    "#};

    sqlx::query_as(query).fetch_all(exe).await
}

/// List all versions for a dataset by namespace and name
///
/// Returns all versions for the specified dataset ordered by version DESC.
pub async fn list_by_namespace_and_name<'c, E>(
    exe: E,
    namespace: Namespace<'_>,
    name: Name<'_>,
) -> Result<Vec<VersionOwned>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT version
        FROM tags
        WHERE namespace = $1 AND name = $2
        ORDER BY version DESC
    "#};

    sqlx::query_scalar(query)
        .bind(namespace)
        .bind(name)
        .fetch_all(exe)
        .await
}

/// Stream all tags from the registry
///
/// Returns a stream of all tag records ordered by version DESC.
pub fn stream<'e, E>(executor: E) -> impl Stream<Item = Result<Tag, sqlx::Error>> + 'e
where
    E: Executor<'e, Database = Postgres> + 'e,
{
    let query = indoc::indoc! {r#"
        SELECT
            t.namespace,
            t.name,
            t.version,
            t.hash,
            m.path,
            t.created_at,
            t.updated_at
        FROM tags t
        JOIN dataset_manifests dm ON t.namespace = dm.namespace AND t.name = dm.name AND t.hash = dm.hash
        JOIN manifest_files m ON dm.hash = m.hash
        ORDER BY t.version DESC
    "#};

    sqlx::query_as(query).fetch(executor)
}

/// Tag record from the `tags` table
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Tag {
    /// Dataset namespace identifier
    pub namespace: NamespaceOwned,
    /// Dataset name
    pub name: NameOwned,
    /// Version tag
    pub version: VersionOwned,
    /// Manifest hash this tag references
    pub hash: HashOwned,
    /// File path where the manifest is stored
    #[sqlx(rename = "path")]
    pub manifest_path: String,
    /// Timestamp when the tag was created
    pub created_at: DateTime<Utc>,
    /// Timestamp when the tag was last updated
    pub updated_at: DateTime<Utc>,
}
