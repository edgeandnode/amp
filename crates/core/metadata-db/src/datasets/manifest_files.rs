//! Dataset manifests resource management
//!
//! This module provides database operations for the `manifest_files` table,
//! which stores dataset manifest information indexed by content hash.

use sqlx::{Executor, Postgres};

use super::hash::Hash;

/// Insert a new manifest record
///
/// Inserts a manifest identified by its hash with the corresponding file path.
/// Uses ON CONFLICT DO NOTHING to make the operation idempotent - if a manifest
/// with the same hash already exists, no error is raised.
pub async fn insert<'c, E>(exe: E, hash: &Hash<'_>, path: &str) -> Result<(), sqlx::Error>
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
