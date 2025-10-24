//! Dataset manifests resource management
//!
//! This module provides database operations for the `manifest_files` table,
//! which stores dataset manifest information indexed by content hash.

use std::borrow::Cow;

use sqlx::{Executor, Postgres};

use super::hash::Hash;

/// Internal SQL operations for manifest file management
///
/// This module is private to the crate. External users should use the
/// public API in the parent `datasets` module instead.
pub(crate) mod sql {
    use super::*;

    /// Insert a new manifest record
    ///
    /// This operation is idempotent - uses ON CONFLICT DO NOTHING to silently ignore
    /// duplicate registrations. Both new insertions and existing records succeed.
    pub async fn insert<'c, E>(
        exe: E,
        hash: Hash<'_>,
        path: Cow<'_, str>,
    ) -> Result<(), sqlx::Error>
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
    /// Retrieves the file path for a manifest identified by its content hash.
    /// Returns `None` if no manifest with the given hash exists.
    pub async fn get_path_by_hash<'c, E>(
        exe: E,
        hash: &Hash<'_>,
    ) -> Result<Option<String>, sqlx::Error>
    where
        E: Executor<'c, Database = Postgres>,
    {
        let query = "SELECT path FROM manifest_files WHERE hash = $1";

        sqlx::query_scalar(query)
            .bind(hash)
            .fetch_optional(exe)
            .await
    }
}
