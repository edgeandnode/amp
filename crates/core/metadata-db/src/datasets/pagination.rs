//! Pagination functions for dataset listing

use sqlx::{Executor, Postgres};

use super::{
    Dataset,
    name::Name,
    version::{Version, VersionOwned},
};

/// List the first page of datasets
///
/// Returns a paginated list of datasets ordered by dataset name ASC and version DESC (newest first within each dataset).
/// This function is used to fetch the initial page when no cursor is available.
pub async fn list_first_page<'c, E>(exe: E, limit: i64) -> Result<Vec<Dataset>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT
            owner,
            dataset,
            version
        FROM registry
        ORDER BY dataset ASC, version DESC
        LIMIT $1
    "#};

    let res = sqlx::query_as(query).bind(limit).fetch_all(exe).await?;
    Ok(res)
}

/// List subsequent pages of datasets using cursor-based pagination
///
/// Returns a paginated list of datasets with names and versions lexicographically after the provided cursor,
/// ordered by dataset name ASC and version DESC (newest first within each dataset). This implements cursor-based
/// pagination for efficient traversal of large dataset lists.
pub async fn list_next_page<'c, E>(
    exe: E,
    limit: i64,
    (last_dataset, last_version): (Name<'_>, Version<'_>),
) -> Result<Vec<Dataset>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT
            owner,
            dataset,
            version
        FROM registry
        WHERE dataset > $2 OR (dataset = $2 AND version < $3)
        ORDER BY dataset ASC, version DESC
        LIMIT $1
    "#};

    let res = sqlx::query_as(query)
        .bind(limit)
        .bind(last_dataset)
        .bind(last_version)
        .fetch_all(exe)
        .await?;
    Ok(res)
}

/// List the first page of datasets for a given dataset name
///
/// Returns a paginated list of dataset versions ordered by version in descending order (newest first).
/// This function is used to fetch the initial page when no cursor is available.
pub async fn list_versions_by_name_first_page<'c, E>(
    exe: E,
    name: Name<'_>,
    limit: i64,
) -> Result<Vec<VersionOwned>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT version
        FROM registry
        WHERE dataset = $1
        ORDER BY version DESC
        LIMIT $2
    "#};

    let res = sqlx::query_scalar(query)
        .bind(name)
        .bind(limit)
        .fetch_all(exe)
        .await?;
    Ok(res)
}

/// List subsequent pages of datasets for a given dataset name using cursor-based pagination
///
/// Returns a paginated list of dataset versions with versions lexicographically before the provided cursor,
/// ordered by version in descending order (newest first). This implements cursor-based
/// pagination for efficient traversal of large dataset version lists.
pub async fn list_versions_by_name_next_page<'c, E>(
    exe: E,
    name: Name<'_>,
    limit: i64,
    last_version: Version<'_>,
) -> Result<Vec<VersionOwned>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT version
        FROM registry
        WHERE dataset = $1 AND version < $3
        ORDER BY version DESC
        LIMIT $2
    "#};

    let res = sqlx::query_scalar(query)
        .bind(name)
        .bind(limit)
        .bind(last_version)
        .fetch_all(exe)
        .await?;
    Ok(res)
}
