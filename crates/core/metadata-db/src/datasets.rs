//! Dataset registry and manifest management
//!
//! This module provides a unified interface for managing dataset
//! registrations, manifests, and version tags. It encompasses all database
//! operations related to dataset metadata, including:
//!
//! - **Type definitions**: Dataset identification (namespace, name, version,
//!   hash)
//! - **manifest_files**: Content-addressable manifest storage indexed by
//!   SHA256 hash
//! - **manifests**: Many-to-many junction table linking datasets to manifests
//! - **tags**: Version tags (semver, "latest", "dev") pointing to
//!   dataset-manifest combinations

use std::borrow::Cow;

mod hash;
mod name;
mod namespace;
mod version;

pub mod manifest_files;
pub mod manifests;
pub mod tags;

pub use self::{
    hash::{Hash as DatasetHash, HashOwned as DatasetHashOwned},
    name::{Name as DatasetName, NameOwned as DatasetNameOwned},
    namespace::{Namespace as DatasetNamespace, NamespaceOwned as DatasetNamespaceOwned},
    tags::Tag as DatasetTag,
    version::{Version as DatasetVersion, VersionOwned as DatasetVersionOwned},
};
use crate::{db::Executor, error::Error};

/// Register manifest file in content-addressable storage
///
/// Inserts manifest hash and path into `manifest_files` table with ON
/// CONFLICT DO NOTHING. This operation is idempotent - duplicate
/// registrations are silently ignored.
#[tracing::instrument(skip(exe), err)]
pub async fn register_manifest<'c, E>(
    exe: E,
    manifest_hash: impl Into<DatasetHash<'_>> + std::fmt::Debug,
    manifest_path: impl Into<Cow<'_, str>> + std::fmt::Debug,
) -> Result<(), Error>
where
    E: Executor<'c>,
{
    manifest_files::sql::insert(exe, manifest_hash.into(), manifest_path.into())
        .await
        .map_err(Into::into)
}

/// Retrieve manifest file path by content hash
///
/// Queries `manifest_files` table for the object store path associated
/// with the given manifest hash. Returns `None` if hash not found.
#[tracing::instrument(skip(exe), err)]
pub async fn get_manifest_path<'c, E>(
    exe: E,
    manifest_hash: impl Into<DatasetHash<'_>> + std::fmt::Debug,
) -> Result<Option<String>, Error>
where
    E: Executor<'c>,
{
    manifest_files::sql::get_path_by_hash(exe, &manifest_hash.into())
        .await
        .map_err(Into::into)
}

/// Link manifest to dataset in junction table
///
/// Inserts association into `dataset_manifests` table with ON CONFLICT
/// DO NOTHING for idempotency. Both manifest and dataset must exist or
/// foreign key constraint will fail.
#[tracing::instrument(skip(exe), err)]
pub async fn link_manifest_to_dataset<'c, E>(
    exe: E,
    namespace: impl Into<DatasetNamespace<'_>> + std::fmt::Debug,
    name: impl Into<DatasetName<'_>> + std::fmt::Debug,
    manifest_hash: impl Into<DatasetHash<'_>> + std::fmt::Debug,
) -> Result<(), Error>
where
    E: Executor<'c>,
{
    manifests::sql::insert(exe, namespace.into(), name.into(), manifest_hash.into())
        .await
        .map_err(Into::into)
}

/// Register or update semantic version tag pointing to manifest
///
/// Upserts version tag (e.g., "1.0.0", "2.3.1") in `tags` table. Only
/// updates `updated_at` if hash changes (WHERE clause). Manifest must be
/// registered and linked or foreign key constraint fails.
#[tracing::instrument(skip(exe), err)]
pub async fn register_version_tag<'c, E>(
    exe: E,
    namespace: impl Into<DatasetNamespace<'_>> + std::fmt::Debug,
    name: impl Into<DatasetName<'_>> + std::fmt::Debug,
    version: impl Into<DatasetVersion<'_>> + std::fmt::Debug,
    manifest_hash: impl Into<DatasetHash<'_>> + std::fmt::Debug,
) -> Result<(), Error>
where
    E: Executor<'c>,
{
    tags::sql::upsert_version(
        exe,
        namespace.into(),
        name.into(),
        version.into(),
        manifest_hash.into(),
    )
    .await
    .map_err(Into::into)
}

/// Update "latest" special tag to point to manifest
///
/// Upserts "latest" tag in `tags` table, typically managed automatically
/// to track the highest semantic version. Only updates `updated_at` if hash
/// changes.
#[tracing::instrument(skip(exe), err)]
pub async fn set_latest_tag<'c, E>(
    exe: E,
    namespace: impl Into<DatasetNamespace<'_>> + std::fmt::Debug,
    name: impl Into<DatasetName<'_>> + std::fmt::Debug,
    manifest_hash: impl Into<DatasetHash<'_>> + std::fmt::Debug,
) -> Result<(), Error>
where
    E: Executor<'c>,
{
    tags::sql::upsert_latest(exe, namespace.into(), name.into(), manifest_hash.into())
        .await
        .map_err(Into::into)
}

/// Update "dev" special tag to point to manifest
///
/// Upserts "dev" tag in `tags` table, typically managed automatically to
/// track most recently registered manifest. Only updates `updated_at` if
/// hash changes.
#[tracing::instrument(skip(exe), err)]
pub async fn set_dev_tag<'c, E>(
    exe: E,
    namespace: impl Into<DatasetNamespace<'_>> + std::fmt::Debug,
    name: impl Into<DatasetName<'_>> + std::fmt::Debug,
    manifest_hash: impl Into<DatasetHash<'_>> + std::fmt::Debug,
) -> Result<(), Error>
where
    E: Executor<'c>,
{
    tags::sql::upsert_dev(exe, namespace.into(), name.into(), manifest_hash.into())
        .await
        .map_err(Into::into)
}

/// Retrieve version tag with complete details
///
/// Queries `tags` table for specified version tag. Returns full tag record
/// including namespace, name, version, manifest hash, and creation/update
/// timestamps, or `None` if tag doesn't exist.
#[tracing::instrument(skip(exe), err)]
pub async fn get_version_tag<'c, E>(
    exe: E,
    namespace: impl Into<DatasetNamespace<'_>> + std::fmt::Debug,
    name: impl Into<DatasetName<'_>> + std::fmt::Debug,
    version: impl Into<DatasetVersion<'_>> + std::fmt::Debug,
) -> Result<Option<DatasetTag>, Error>
where
    E: Executor<'c>,
{
    tags::sql::get_version(exe, namespace.into(), name.into(), version.into())
        .await
        .map_err(Into::into)
}

/// Resolve "latest" special tag to its corresponding semantic version tag
///
/// Uses CTE query to find the semver tag pointing to same manifest hash
/// as "latest". Returns most recently updated tag if multiple tags share
/// the hash, or `None` if "latest" doesn't exist.
///
/// **Note**: This function performs a plain `SELECT` without row locking.
/// For read-modify-write operations on the "latest" tag within a transaction,
/// use `get_latest_tag_for_update` instead to prevent race conditions.
#[tracing::instrument(skip(exe), err)]
pub async fn get_latest_tag<'c, E>(
    exe: E,
    namespace: impl Into<DatasetNamespace<'_>> + std::fmt::Debug,
    name: impl Into<DatasetName<'_>> + std::fmt::Debug,
) -> Result<Option<DatasetTag>, Error>
where
    E: Executor<'c>,
{
    tags::sql::get_latest(exe, namespace.into(), name.into())
        .await
        .map_err(Into::into)
}

/// Resolve "latest" tag with row-level locking for safe concurrent updates
///
/// This function is identical to `get_latest_tag` but acquires a row-level lock
/// using `SELECT FOR UPDATE` on the "latest" tag row. This enables safe
/// read-modify-write operations on the "latest" tag within a transaction.
#[tracing::instrument(skip(exe), err)]
pub async fn get_latest_tag_for_update<'c, E>(
    exe: E,
    namespace: impl Into<DatasetNamespace<'_>> + std::fmt::Debug,
    name: impl Into<DatasetName<'_>> + std::fmt::Debug,
) -> Result<Option<DatasetTag>, Error>
where
    E: Executor<'c>,
{
    tags::sql::get_latest_for_update(exe, namespace.into(), name.into())
        .await
        .map_err(Into::into)
}

/// Retrieve manifest hash for version tag
///
/// Queries `tags` table for hash column only. Returns `None` if tag
/// doesn't exist.
#[tracing::instrument(skip(exe), err)]
pub async fn get_version_tag_hash<'c, E>(
    exe: E,
    namespace: impl Into<DatasetNamespace<'_>> + std::fmt::Debug,
    name: impl Into<DatasetName<'_>> + std::fmt::Debug,
    version: impl Into<DatasetVersion<'_>> + std::fmt::Debug,
) -> Result<Option<DatasetHashOwned>, Error>
where
    E: Executor<'c>,
{
    tags::sql::get_version_hash(exe, namespace.into(), name.into(), version.into())
        .await
        .map_err(Into::into)
}

/// Retrieve manifest hash for "latest" special tag
///
/// Queries `tags` table for hash where version='latest'. Returns `None`
/// if "latest" tag doesn't exist.
#[tracing::instrument(skip(exe), err)]
pub async fn get_latest_tag_hash<'c, E>(
    exe: E,
    namespace: impl Into<DatasetNamespace<'_>> + std::fmt::Debug,
    name: impl Into<DatasetName<'_>> + std::fmt::Debug,
) -> Result<Option<DatasetHashOwned>, Error>
where
    E: Executor<'c>,
{
    tags::sql::get_latest_hash(exe, namespace.into(), name.into())
        .await
        .map_err(Into::into)
}

/// Retrieve manifest hash for "dev" special tag
///
/// Queries `tags` table for hash where version='dev'. Returns `None` if
/// "dev" tag doesn't exist.
#[tracing::instrument(skip(exe), err)]
pub async fn get_dev_tag_hash<'c, E>(
    exe: E,
    namespace: impl Into<DatasetNamespace<'_>> + std::fmt::Debug,
    name: impl Into<DatasetName<'_>> + std::fmt::Debug,
) -> Result<Option<DatasetHashOwned>, Error>
where
    E: Executor<'c>,
{
    tags::sql::get_dev_hash(exe, namespace.into(), name.into())
        .await
        .map_err(Into::into)
}

/// List all semantic versions for dataset
///
/// Queries `tags` table excluding special tags (NOT IN ('dev', 'latest')),
/// ordered by version DESC.
#[tracing::instrument(skip(exe))]
pub async fn list_versions<'c, E>(
    exe: E,
    namespace: impl Into<DatasetNamespace<'_>> + std::fmt::Debug,
    name: impl Into<DatasetName<'_>> + std::fmt::Debug,
) -> Result<Vec<DatasetVersionOwned>, Error>
where
    E: Executor<'c>,
{
    tags::sql::list_versions(exe, namespace.into(), name.into())
        .await
        .map_err(Into::into)
}

/// List all dataset tags from registry
///
/// Queries all semantic version tags from `tags` table (excludes "dev"
/// and "latest"), ordered by version DESC.
#[tracing::instrument(skip(exe))]
pub async fn list_all<'c, E>(exe: E) -> Result<Vec<DatasetTag>, Error>
where
    E: Executor<'c>,
{
    tags::sql::list_all(exe).await.map_err(Into::into)
}
