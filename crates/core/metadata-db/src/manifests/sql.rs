//! Internal SQL operations for manifest file management

use sqlx::{Executor, Postgres};

use super::{
    hash::{Hash, HashOwned},
    path::{Path, PathOwned},
};

/// Insert a new manifest record
///
/// Idempotent (`ON CONFLICT DO NOTHING`).
pub(crate) async fn insert<'c, E>(exe: E, hash: Hash<'_>, path: Path<'_>) -> Result<(), sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        INSERT INTO manifest_files (hash, path)
        VALUES ($1, $2)
        ON CONFLICT (hash) DO NOTHING
    "#};

    sqlx::query(query)
        .bind(hash)
        .bind(path)
        .execute(exe)
        .await?;

    Ok(())
}

/// Get manifest file path by hash
///
/// Returns `None` if not found.
pub(crate) async fn get_path_by_hash<'c, E>(
    exe: E,
    hash: Hash<'_>,
) -> Result<Option<PathOwned>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = "SELECT path FROM manifest_files WHERE hash = $1";

    sqlx::query_scalar(query)
        .bind(hash)
        .fetch_optional(exe)
        .await
}

/// Count dataset links with row-level locking
///
/// Uses `SELECT FOR UPDATE` to lock all `dataset_manifests` rows with this hash, preventing
/// concurrent link creation. Must be called within a transaction to be effective.
pub(crate) async fn count_dataset_links_for_update<'c, E>(
    exe: E,
    hash: Hash<'_>,
) -> Result<i64, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = "SELECT COUNT(*) FROM dataset_manifests WHERE hash = $1 FOR UPDATE";

    sqlx::query_scalar(query).bind(hash).fetch_one(exe).await
}

/// Delete a manifest file record
///
/// CASCADE deletes all `dataset_manifests` and `tags` entries.
/// Returns `Some(path)` if deleted, `None` if not found.
pub(crate) async fn delete<'c, E>(exe: E, hash: Hash<'_>) -> Result<Option<PathOwned>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = "DELETE FROM manifest_files WHERE hash = $1 RETURNING path";

    sqlx::query_scalar(query)
        .bind(hash)
        .fetch_optional(exe)
        .await
}

/// List all orphaned manifests (manifests with no dataset links)
///
/// Returns a vector of manifest hashes for all manifests in `manifest_files`
/// that have no corresponding entries in `dataset_manifests`.
pub(crate) async fn list_orphaned<'c, E>(exe: E) -> Result<Vec<HashOwned>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    // NOT EXISTS short-circuits on first match, avoiding full join materialization
    let query = indoc::indoc! {r#"
        SELECT hash
        FROM manifest_files mf
        WHERE NOT EXISTS (
            SELECT 1 FROM dataset_manifests dm WHERE dm.hash = mf.hash
        )
    "#};

    sqlx::query_scalar(query).fetch_all(exe).await
}
