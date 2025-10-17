//! Dataset-manifest junction table resource management
//!
//! This module provides database operations for the `dataset_manifests` junction table,
//! which implements the many-to-many relationship between datasets and manifests.
//! One manifest can belong to multiple datasets, and one dataset can have multiple manifests.

use sqlx::{Executor, Postgres};

use super::{hash::Hash, name::Name, namespace::Namespace};

/// Link a manifest to a dataset
///
/// Creates a dataset-manifest association. This operation is idempotent - if the link
/// already exists, no error is raised.
///
/// Note: This function assumes both the dataset and manifest already exist in their
/// respective tables. Violating this constraint will result in a foreign key error.
pub async fn insert<'c, E>(
    exe: E,
    namespace: &Namespace<'_>,
    name: &Name<'_>,
    hash: &Hash<'_>,
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
