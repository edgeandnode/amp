//! Dataset manifests resource management
//!
//! This module provides database operations for the `manifest_files` table,
//! which stores dataset manifest information indexed by content hash.

use std::borrow::Cow;

use sqlx::{Executor, Postgres};

use super::hash::Hash;

/// Insert a new manifest record
///
/// Returns `true` if inserted (didn't exist), or `false` if already existed (ON CONFLICT).
/// Enables optimistic registration: only the process returning `true` should proceed
/// with expensive operations like object store writes.
pub async fn insert<'c, E>(exe: E, hash: Hash<'_>, path: Cow<'_, str>) -> Result<bool, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        INSERT INTO manifest_files (hash, path)
        VALUES ($1, $2)
        ON CONFLICT (hash) DO NOTHING
        RETURNING 1
    "#};

    // Return constant 1 instead of hash for efficiency (4 bytes vs 64+ bytes)
    let result: Option<i32> = sqlx::query_scalar(query)
        .bind(hash)
        .bind(path)
        .fetch_optional(exe)
        .await?;

    // If RETURNING gave us a result, the INSERT succeeded (we inserted it)
    // If None, the ON CONFLICT triggered (already existed)
    Ok(result.is_some())
}

/// Get manifest file path by hash
///
/// Retrieves the file path for a manifest identified by its content hash.
/// Returns `None` if no manifest with the given hash exists.
pub async fn get_path_by_hash<'c, E>(exe: E, hash: &Hash<'_>) -> Result<Option<String>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = "SELECT path FROM manifest_files WHERE hash = $1";

    sqlx::query_scalar(query)
        .bind(hash)
        .fetch_optional(exe)
        .await
}
