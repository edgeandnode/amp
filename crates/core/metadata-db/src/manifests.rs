//! Manifest registry and file management
//!
//! This module provides operations for managing manifest files:
//! - **manifest_files**: Content-addressable manifest storage indexed by SHA256 hash
//! - **Type definitions**: ManifestHash, ManifestPath
//!
//! ## Database Tables
//!
//! - **manifest_files**: Content-addressable manifest storage with hash → path mapping

mod hash;
mod path;
pub(crate) mod sql;

pub use self::{
    hash::{Hash as ManifestHash, HashOwned as ManifestHashOwned},
    path::{Path as ManifestPath, PathOwned as ManifestPathOwned},
    sql::ManifestSummary,
};
use crate::{db::Executor, error::Error};

/// Register manifest file in content-addressable storage
///
/// Inserts manifest hash and path into `manifest_files` table with ON
/// CONFLICT DO NOTHING. This operation is idempotent - duplicate
/// registrations are silently ignored.
#[tracing::instrument(skip(exe), err)]
pub async fn register<'c, E>(
    exe: E,
    hash: impl Into<ManifestHash<'_>> + std::fmt::Debug,
    path: impl Into<ManifestPath<'_>> + std::fmt::Debug,
) -> Result<(), Error>
where
    E: Executor<'c>,
{
    sql::insert(exe, hash.into(), path.into())
        .await
        .map_err(Error::DbError)
}

/// Retrieve manifest file path by content hash
///
/// Queries `manifest_files` table for the object store path associated
/// with the given manifest hash. Returns `None` if hash not found.
#[tracing::instrument(skip(exe), err)]
pub async fn get_path<'c, E>(
    exe: E,
    hash: impl Into<ManifestHash<'_>> + std::fmt::Debug,
) -> Result<Option<ManifestPathOwned>, Error>
where
    E: Executor<'c>,
{
    sql::get_path_by_hash(exe, hash.into())
        .await
        .map_err(Error::DbError)
}

/// List all orphaned manifests (manifests with no dataset links)
///
/// Queries for all manifests in the `manifest_files` table that have no corresponding
/// entries in the `dataset_manifests` table. These are manifests that were registered
/// but are no longer linked to any datasets.
///
/// Returns a vector of manifest hashes for all orphaned manifests.
#[tracing::instrument(skip(exe), err)]
pub async fn list_orphaned<'c, E>(exe: E) -> Result<Vec<ManifestHashOwned>, Error>
where
    E: Executor<'c>,
{
    sql::list_orphaned(exe).await.map_err(Error::DbError)
}

/// List all registered manifests with metadata
///
/// Queries for all manifests in the `manifest_files` table, returning:
/// - Hash (content-addressable identifier)
/// - Dataset count (number of datasets using this manifest)
///
/// Results are ordered by hash.
#[tracing::instrument(skip(exe), err)]
pub async fn list_all<'c, E>(exe: E) -> Result<Vec<ManifestSummary>, Error>
where
    E: Executor<'c>,
{
    sql::list_all(exe).await.map_err(Error::DbError)
}

/// Count dataset links and lock rows to prevent concurrent modifications
///
/// This function counts how many datasets are linked to a manifest while acquiring
/// row-level locks on all `dataset_manifests` rows using `SELECT FOR UPDATE`.
/// This prevents concurrent transactions from inserting new links during the count.
///
/// **Important**: This function must be called within an explicit transaction to be effective.
/// When called outside a transaction (in auto-commit mode), the lock is immediately released
/// after the query completes, providing no concurrency protection.
#[tracing::instrument(skip(exe), err)]
pub async fn count_dataset_links_and_lock<'c, E>(
    exe: E,
    hash: impl Into<ManifestHash<'_>> + std::fmt::Debug,
) -> Result<i64, Error>
where
    E: Executor<'c>,
{
    sql::count_dataset_links_for_update(exe, hash.into())
        .await
        .map_err(Error::DbError)
}

/// Delete a manifest from the database
///
/// This operation will CASCADE delete:
/// - All `dataset_manifests` entries linking this manifest to datasets
/// - All `tags` entries pointing to this manifest
///
/// Returns `Some(path)` if the manifest was deleted, `None` if it was not found.
#[tracing::instrument(skip(exe), err)]
pub async fn delete<'c, E>(
    exe: E,
    hash: impl Into<ManifestHash<'_>> + std::fmt::Debug,
) -> Result<Option<ManifestPathOwned>, Error>
where
    E: Executor<'c>,
{
    sql::delete(exe, hash.into()).await.map_err(Error::DbError)
}
