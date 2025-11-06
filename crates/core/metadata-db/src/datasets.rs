//! Dataset registry and version tag management
//!
//! This module provides operations for managing datasets and their metadata:
//!
//! - **Type definitions**: Dataset identification (namespace, name, version)
//! - **manifests**: Many-to-many junction table linking datasets to manifests
//! - **tags**: Version tags (semver, "latest", "dev") pointing to
//!   dataset-manifest combinations

mod name;
mod namespace;
mod version;

pub mod manifests;
pub mod tags;

pub use self::{
    name::{Name as DatasetName, NameOwned as DatasetNameOwned},
    namespace::{Namespace as DatasetNamespace, NamespaceOwned as DatasetNamespaceOwned},
    tags::Tag as DatasetTag,
    version::{Version as DatasetVersion, VersionOwned as DatasetVersionOwned},
};
use crate::{ManifestHashOwned, db::Executor, error::Error, manifests::ManifestHash};

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
    manifest_hash: impl Into<ManifestHash<'_>> + std::fmt::Debug,
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
    manifest_hash: impl Into<ManifestHash<'_>> + std::fmt::Debug,
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
    manifest_hash: impl Into<ManifestHash<'_>> + std::fmt::Debug,
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
    manifest_hash: impl Into<ManifestHash<'_>> + std::fmt::Debug,
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

/// Get "latest" tag and lock the row to prevent concurrent modifications
///
/// This function retrieves the "latest" tag while acquiring a row-level lock using
/// `SELECT FOR UPDATE`. This enables safe read-modify-write operations on the
/// "latest" tag within a transaction, preventing concurrent modifications.
#[tracing::instrument(skip(exe), err)]
pub async fn get_latest_tag_and_lock<'c, E>(
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
) -> Result<Option<ManifestHashOwned>, Error>
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
) -> Result<Option<ManifestHashOwned>, Error>
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
) -> Result<Option<ManifestHashOwned>, Error>
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

/// List all version tags with full details for a dataset
///
/// Queries semantic version tags with hash and timestamps from `tags` table
/// (excludes "dev" and "latest"), ordered by version DESC.
#[tracing::instrument(skip(exe))]
pub async fn list_version_tags<'c, E>(
    exe: E,
    namespace: impl Into<DatasetNamespace<'_>> + std::fmt::Debug,
    name: impl Into<DatasetName<'_>> + std::fmt::Debug,
) -> Result<Vec<DatasetTag>, Error>
where
    E: Executor<'c>,
{
    tags::sql::list_version_tags(exe, namespace.into(), name.into())
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

/// List all dataset tags using a specific manifest
///
/// Returns all dataset tags (namespace, name, version) that reference the given
/// manifest hash. This is useful for discovering which datasets and versions
/// are using a particular manifest.
///
/// System-managed tags ("latest" and "dev") are excluded from the results.
///
/// Returns an empty vector if no datasets use this manifest.
#[tracing::instrument(skip(exe), err)]
pub async fn list_tags_by_hash<'c, E>(
    exe: E,
    manifest_hash: impl Into<ManifestHash<'_>> + std::fmt::Debug,
) -> Result<Vec<DatasetTag>, Error>
where
    E: Executor<'c>,
{
    tags::sql::list_by_manifest_hash(exe, manifest_hash.into())
        .await
        .map_err(Into::into)
}

/// Delete a specific version tag
///
/// Removes a version tag from the `tags` table. This operation is idempotent -
/// it succeeds even if the tag doesn't exist.
#[tracing::instrument(skip(exe), err)]
pub async fn delete_version_tag<'c, E>(
    exe: E,
    namespace: impl Into<DatasetNamespace<'_>> + std::fmt::Debug,
    name: impl Into<DatasetName<'_>> + std::fmt::Debug,
    version: impl Into<DatasetVersion<'_>> + std::fmt::Debug,
) -> Result<(), Error>
where
    E: Executor<'c>,
{
    tags::sql::delete_version(exe, namespace.into(), name.into(), version.into())
        .await
        .map_err(Into::into)
}

/// Delete all tags for a dataset
///
/// Removes all version tags (including "latest" and "dev") for a dataset.
/// This operation is idempotent - it succeeds even if no tags exist.
#[tracing::instrument(skip(exe), err)]
pub async fn delete_all_tags<'c, E>(
    exe: E,
    namespace: impl Into<DatasetNamespace<'_>> + std::fmt::Debug,
    name: impl Into<DatasetName<'_>> + std::fmt::Debug,
) -> Result<(), Error>
where
    E: Executor<'c>,
{
    tags::sql::delete_all_for_dataset(exe, namespace.into(), name.into())
        .await
        .map_err(Into::into)
}

/// Delete all manifest links for a dataset
///
/// Removes all dataset-manifest associations for a dataset. Due to the database
/// foreign key constraint with ON DELETE CASCADE, this will also automatically
/// delete all associated version tags.
///
/// Returns the list of manifest hashes that were unlinked. These can be checked
/// for orphaned manifests that should be cleaned up.
///
/// This operation is idempotent - it succeeds even if no manifest links exist.
#[tracing::instrument(skip(exe), err)]
pub async fn unlink_manifests<'c, E>(
    exe: E,
    namespace: impl Into<DatasetNamespace<'_>> + std::fmt::Debug,
    name: impl Into<DatasetName<'_>> + std::fmt::Debug,
) -> Result<Vec<ManifestHashOwned>, Error>
where
    E: Executor<'c>,
{
    manifests::sql::delete_all_for_dataset(exe, namespace.into(), name.into())
        .await
        .map_err(Into::into)
}
