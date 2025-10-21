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

/// Upsert a version tag
///
/// Creates or updates a version tag that points to a specific manifest hash.
/// If the tag exists with a different hash, it is updated and `updated_at` is modified.
/// If the tag exists with the same hash, no changes are made.
pub async fn upsert_version<'c, E>(
    exe: E,
    namespace: Namespace<'_>,
    name: Name<'_>,
    version: Version<'_>,
    hash: Hash<'_>,
) -> Result<(), sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        INSERT INTO tags (namespace, name, version, hash, created_at, updated_at)
        VALUES ($1, $2, $3, $4, NOW() AT TIME ZONE 'utc', NOW() AT TIME ZONE 'utc')
        ON CONFLICT (namespace, name, version)
        DO UPDATE SET
            hash = EXCLUDED.hash,
            updated_at = NOW() AT TIME ZONE 'utc'
        WHERE tags.hash != EXCLUDED.hash
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

/// Upsert the "latest" tag
///
/// Creates or updates the "latest" tag to point to a specific manifest hash.
pub async fn upsert_latest<'c, E>(
    exe: E,
    namespace: Namespace<'_>,
    name: Name<'_>,
    hash: Hash<'_>,
) -> Result<(), sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        INSERT INTO tags (namespace, name, version, hash, created_at, updated_at)
        VALUES ($1, $2, 'latest', $3, NOW() AT TIME ZONE 'utc', NOW() AT TIME ZONE 'utc')
        ON CONFLICT (namespace, name, version)
        DO UPDATE SET
            hash = EXCLUDED.hash,
            updated_at = NOW() AT TIME ZONE 'utc'
        WHERE tags.hash != EXCLUDED.hash
   "#};

    sqlx::query(query)
        .bind(namespace)
        .bind(name)
        .bind(hash)
        .execute(exe)
        .await?;

    Ok(())
}

/// Upsert the "dev" tag
///
/// Creates or updates the "dev" tag to point to a specific manifest hash.
pub async fn upsert_dev<'c, E>(
    exe: E,
    namespace: Namespace<'_>,
    name: Name<'_>,
    hash: Hash<'_>,
) -> Result<(), sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        INSERT INTO tags (namespace, name, version, hash, created_at, updated_at)
        VALUES ($1, $2, 'dev', $3, NOW() AT TIME ZONE 'utc', NOW() AT TIME ZONE 'utc')
        ON CONFLICT (namespace, name, version)
        DO UPDATE SET
            hash = EXCLUDED.hash,
            updated_at = NOW() AT TIME ZONE 'utc'
        WHERE tags.hash != EXCLUDED.hash
   "#};

    sqlx::query(query)
        .bind(namespace)
        .bind(name)
        .bind(hash)
        .execute(exe)
        .await?;

    Ok(())
}

/// Get a tag by namespace, name, and version
pub async fn get_version<'c, E>(
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
            namespace,
            name,
            version,
            hash,
            created_at,
            updated_at
        FROM tags
        WHERE namespace = $1 AND name = $2 AND version = $3
    "#};

    let result = sqlx::query_as(query)
        .bind(namespace)
        .bind(name)
        .bind(version)
        .fetch_optional(exe)
        .await?;

    Ok(result)
}

/// Resolve the "latest" tag to its corresponding semver tag
///
/// Returns the semver tag (excluding "dev" and "latest") that points to the same hash
/// as the "latest" tag. If multiple semver tags point to the same hash, returns the
/// most recently updated one.
pub async fn get_latest<'c, E>(
    exe: E,
    namespace: Namespace<'_>,
    name: Name<'_>,
) -> Result<Option<Tag>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        WITH latest_hash AS (
            SELECT hash
            FROM tags
            WHERE namespace = $1 AND name = $2 AND version = 'latest'
        )
        SELECT
            t.namespace,
            t.name,
            t.version,
            t.hash,
            t.created_at,
            t.updated_at
        FROM tags t
        INNER JOIN latest_hash lh ON t.hash = lh.hash
        WHERE t.namespace = $1 AND t.name = $2
          AND t.version NOT IN ('dev', 'latest')
        ORDER BY t.updated_at DESC
        LIMIT 1
    "#};

    let result = sqlx::query_as(query)
        .bind(namespace)
        .bind(name)
        .fetch_optional(exe)
        .await?;

    Ok(result)
}

/// Get a tag's hash by namespace, name, and version
pub async fn get_version_hash<'c, E>(
    exe: E,
    namespace: Namespace<'_>,
    name: Name<'_>,
    version: Version<'_>,
) -> Result<Option<HashOwned>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT hash
        FROM tags
        WHERE namespace = $1 AND name = $2 AND version = $3
    "#};

    sqlx::query_scalar(query)
        .bind(namespace)
        .bind(name)
        .bind(version)
        .fetch_optional(exe)
        .await
}

/// Get the "latest" tag's hash
pub async fn get_latest_hash<'c, E>(
    exe: E,
    namespace: Namespace<'_>,
    name: Name<'_>,
) -> Result<Option<HashOwned>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT hash
        FROM tags
        WHERE namespace = $1 AND name = $2 AND version = 'latest'
    "#};

    sqlx::query_scalar(query)
        .bind(namespace)
        .bind(name)
        .fetch_optional(exe)
        .await
}

/// Get the "dev" tag's hash
pub async fn get_dev_hash<'c, E>(
    exe: E,
    namespace: Namespace<'_>,
    name: Name<'_>,
) -> Result<Option<HashOwned>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT hash
        FROM tags
        WHERE namespace = $1 AND name = $2 AND version = 'dev'
    "#};

    sqlx::query_scalar(query)
        .bind(namespace)
        .bind(name)
        .fetch_optional(exe)
        .await
}

/// List all versions for a dataset
///
/// Returns semver versions only (excludes "dev" and "latest"), ordered by version descending.
pub async fn list_versions<'c, E>(
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
          AND version NOT IN ('dev', 'latest')
        ORDER BY version DESC
    "#};

    sqlx::query_scalar(query)
        .bind(namespace)
        .bind(name)
        .fetch_all(exe)
        .await
}

/// List all tags
///
/// Returns all semver tags (excludes "dev" and "latest"), ordered by version descending.
pub async fn list_all<'c, E>(exe: E) -> Result<Vec<Tag>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT
            namespace,
            name,
            version,
            hash,
            created_at,
            updated_at
        FROM tags
        WHERE version NOT IN ('dev', 'latest')
        ORDER BY version DESC
    "#};

    sqlx::query_as(query).fetch_all(exe).await
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
    /// Timestamp when the tag was created
    pub created_at: DateTime<Utc>,
    /// Timestamp when the tag was last updated
    pub updated_at: DateTime<Utc>,
}
