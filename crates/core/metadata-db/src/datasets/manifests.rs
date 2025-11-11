//! Dataset-manifest junction table resource management
//!
//! This module provides database operations for the `dataset_manifests` junction table,
//! which implements the many-to-many relationship between datasets and manifests.
//! One manifest can belong to multiple datasets, and one dataset can have multiple manifests.

use sqlx::{Executor, Postgres};

use super::{name::Name, namespace::Namespace};
use crate::manifests::{ManifestHash, ManifestHashOwned};

/// Internal SQL operations for dataset-manifest junction table
///
/// This module is private to the crate. External users should use the
/// public API in the parent `datasets` module instead.
pub(crate) mod sql {
    use super::*;

    /// Link a manifest to a dataset
    ///
    /// Creates a dataset-manifest association. This operation is idempotent - if the link
    /// already exists, no error is raised.
    ///
    /// Note: This function assumes both the dataset and manifest already exist in their
    /// respective tables. Violating this constraint will result in a foreign key error.
    pub async fn insert<'c, E>(
        exe: E,
        namespace: Namespace<'_>,
        name: Name<'_>,
        hash: ManifestHash<'_>,
    ) -> Result<(), sqlx::Error>
    where
        E: Executor<'c, Database = Postgres>,
    {
        let query = indoc::indoc! {r#"
            INSERT INTO dataset_manifests (namespace, name, hash)
            VALUES ($1, $2, $3)
            ON CONFLICT (namespace, name, hash) DO NOTHING
        "#};

        sqlx::query(query)
            .bind(namespace)
            .bind(name)
            .bind(hash)
            .execute(exe)
            .await?;

        Ok(())
    }

    /// Check if a manifest is linked to a specific dataset
    ///
    /// Returns `true` if the manifest hash is linked to the dataset identified by
    /// namespace and name, `false` otherwise.
    ///
    /// This performs an efficient EXISTS query that short-circuits as soon as
    /// a matching row is found, rather than scanning all rows.
    pub async fn exists<'c, E>(
        exe: E,
        namespace: Namespace<'_>,
        name: Name<'_>,
        hash: ManifestHash<'_>,
    ) -> Result<bool, sqlx::Error>
    where
        E: Executor<'c, Database = Postgres>,
    {
        let query = indoc::indoc! {r#"
            SELECT EXISTS(
                SELECT 1 FROM dataset_manifests
                WHERE namespace = $1 AND name = $2 AND hash = $3
            )
        "#};

        sqlx::query_scalar(query)
            .bind(namespace)
            .bind(name)
            .bind(hash)
            .fetch_one(exe)
            .await
    }

    /// Delete all manifest links for a dataset
    ///
    /// Removes all dataset-manifest associations for a given dataset. This operation is idempotent
    /// and will cascade delete all associated tags due to the foreign key constraint.
    ///
    /// Returns the list of manifest hashes that were unlinked from the dataset. This can be used
    /// to check for and clean up orphaned manifests.
    ///
    /// This effectively removes all versions of a dataset from the system.
    pub async fn delete_all_for_dataset<'c, E>(
        exe: E,
        namespace: Namespace<'_>,
        name: Name<'_>,
    ) -> Result<Vec<ManifestHashOwned>, sqlx::Error>
    where
        E: Executor<'c, Database = Postgres>,
    {
        let query =
            "DELETE FROM dataset_manifests WHERE namespace = $1 AND name = $2 RETURNING hash";

        sqlx::query_scalar(query)
            .bind(namespace)
            .bind(name)
            .fetch_all(exe)
            .await
    }
}
