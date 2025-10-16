//! Pagination functions for dataset listing

use sqlx::{Executor, Postgres};

use super::{
    Dataset,
    name::Name,
    version_tag::{VersionTag, VersionTagOwned},
};

/// List the first page of datasets
///
/// Returns a paginated list of datasets ordered by dataset name ASC and version DESC (lexicographical ordering).
/// This function is used to fetch the initial page when no cursor is available.
pub async fn list_first_page<'c, E>(exe: E, limit: i64) -> Result<Vec<Dataset>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT
            namespace,
            name,
            version
        FROM tags
        ORDER BY name ASC, version DESC
        LIMIT $1
    "#};

    let res = sqlx::query_as(query).bind(limit).fetch_all(exe).await?;
    Ok(res)
}

/// List subsequent pages of datasets using cursor-based pagination
///
/// Returns a paginated list of datasets with names and versions lexicographically after the provided cursor,
/// ordered by dataset name ASC and version DESC (lexicographical ordering). This implements cursor-based
/// pagination for efficient traversal of large dataset lists.
pub async fn list_next_page<'c, E>(
    exe: E,
    limit: i64,
    (last_dataset, last_version): (Name<'_>, VersionTag<'_>),
) -> Result<Vec<Dataset>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT
            namespace,
            name,
            version
        FROM tags
        WHERE name > $2 OR (name = $2 AND version < $3)
        ORDER BY name ASC, version DESC
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
/// Returns a paginated list of dataset versions ordered by version in descending lexicographical order.
/// This function is used to fetch the initial page when no cursor is available.
pub async fn list_versions_by_name_first_page<'c, E>(
    exe: E,
    name: Name<'_>,
    limit: i64,
) -> Result<Vec<VersionTagOwned>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT version
        FROM tags
        WHERE name = $1
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
/// ordered by version in descending lexicographical order. This implements cursor-based
/// pagination for efficient traversal of large dataset version lists.
pub async fn list_versions_by_name_next_page<'c, E>(
    exe: E,
    name: Name<'_>,
    limit: i64,
    last_version: VersionTag<'_>,
) -> Result<Vec<VersionTagOwned>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT version
        FROM tags
        WHERE name = $1 AND version < $3
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
