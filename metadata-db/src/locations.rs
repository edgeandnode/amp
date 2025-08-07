//! Location-related database operations

use sqlx::{Executor, Postgres};
use url::Url;

use crate::{TableId, workers::jobs::JobId};

/// Unique identifier for location records tracking dataset table storage locations (non-negative).
pub type LocationId = i64;

/// List all locations from the database ordered by dataset, table, and creation date
pub async fn list_all<'c, E>(exe: E) -> Result<Vec<Location>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {"
        SELECT 
            id,
            dataset,
            dataset_version,
            tbl,
            url,
            active
        FROM locations
        ORDER BY dataset, tbl, created_at DESC
    "};

    let locations = sqlx::query_as(query).fetch_all(exe).await?;
    Ok(locations)
}

/// List all active locations ordered by dataset and table
pub async fn list_all_active<'c, E>(exe: E) -> Result<Vec<Location>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {"
        SELECT 
            id,
            dataset,
            dataset_version,
            tbl,
            url,
            active
        FROM locations
        WHERE active = true
        ORDER BY dataset, tbl
    "};

    let locations = sqlx::query_as(query).fetch_all(exe).await?;
    Ok(locations)
}

/// List all locations for a specific dataset ordered by table and creation date
pub async fn list_by_dataset<'c, E>(exe: E, dataset: &str) -> Result<Vec<Location>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {"
        SELECT 
            id,
            dataset,
            dataset_version,
            tbl,
            url,
            active
        FROM locations
        WHERE dataset = $1
        ORDER BY tbl, created_at DESC
    "};

    let locations = sqlx::query_as(query).bind(dataset).fetch_all(exe).await?;
    Ok(locations)
}

/// Insert a location into the database and return its ID (idempotent operation)
#[tracing::instrument(skip(exe), err)]
pub async fn insert<'c, E>(
    exe: E,
    table: TableId<'_>,
    bucket: Option<&str>,
    path: &str,
    url: &Url,
    active: bool,
) -> Result<LocationId, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let dataset_version = table.dataset_version.unwrap_or("");

    // Upsert with RETURNING id - the no-op update ensures RETURNING works for both insert and conflict cases
    let query = indoc::indoc! {"
        INSERT INTO locations (dataset, dataset_version, tbl, bucket, path, url, active)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (dataset, dataset_version, tbl, url) DO UPDATE SET dataset = EXCLUDED.dataset
        RETURNING id
    "};

    let location_id: LocationId = sqlx::query_scalar(query)
        .bind(table.dataset)
        .bind(dataset_version)
        .bind(table.table)
        .bind(bucket)
        .bind(path)
        .bind(url.to_string())
        .bind(active)
        .fetch_one(exe)
        .await?;
    Ok(location_id)
}

/// Get location ID by table identifier and URL
#[tracing::instrument(skip(exe), err)]
pub async fn get_by_id<'c, E>(
    exe: E,
    table: TableId<'_>,
    url: &Url,
) -> Result<LocationId, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let dataset_version = table.dataset_version.unwrap_or("");

    let query = indoc::indoc! {"
        SELECT id
        FROM locations
        WHERE dataset = $1 AND dataset_version = $2 AND tbl = $3 AND url = $4
    "};

    let location_id: LocationId = sqlx::query_scalar(query)
        .bind(table.dataset)
        .bind(dataset_version)
        .bind(table.table)
        .bind(url.to_string())
        .fetch_one(exe)
        .await?;
    Ok(location_id)
}

/// Get location ID by URL only, returns first match if multiple exist
pub async fn url_to_location_id<'c, E>(exe: E, url: &Url) -> Result<Option<LocationId>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {"
        SELECT id
        FROM locations
        WHERE url = $1
        LIMIT 1
    "};

    let location_id: Option<LocationId> = sqlx::query_scalar(query)
        .bind(url.to_string())
        .fetch_optional(exe)
        .await?;
    Ok(location_id)
}

/// Get all active locations for a table
#[tracing::instrument(skip(exe), err)]
pub async fn get_active_by_table_id<'c, E>(
    exe: E,
    table: TableId<'_>,
) -> Result<Vec<(String, LocationId)>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {"
        SELECT url, id
        FROM locations
        WHERE dataset = $1 AND dataset_version = $2 AND tbl = $3 AND active
    "};

    let tuples: Vec<(String, LocationId)> = sqlx::query_as(query)
        .bind(table.dataset)
        .bind(table.dataset_version.unwrap_or(""))
        .bind(table.table)
        .fetch_all(exe)
        .await?;

    Ok(tuples)
}

/// Deactivate all active locations for a specific table
#[tracing::instrument(skip(exe), err)]
pub async fn mark_inactive_by_table_id<'c, E>(exe: E, table: TableId<'_>) -> Result<(), sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let dataset_version = table.dataset_version.unwrap_or("");

    let query = indoc::indoc! {"
        UPDATE locations
        SET active = false
        WHERE dataset = $1 AND dataset_version = $2 AND tbl = $3 AND active
    "};

    sqlx::query(query)
        .bind(table.dataset)
        .bind(dataset_version)
        .bind(table.table)
        .execute(exe)
        .await?;
    Ok(())
}

/// Activate a specific location by URL (does not deactivate others)
#[tracing::instrument(skip(exe), err)]
pub async fn mark_active_by_url<'c, E>(
    exe: E,
    table: TableId<'_>,
    location_url: &Url,
) -> Result<(), sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let dataset_version = table.dataset_version.unwrap_or("");

    let query = indoc::indoc! {"
        UPDATE locations
        SET active = true
        WHERE dataset = $1 AND dataset_version = $2 AND tbl = $3 AND url = $4
    "};

    sqlx::query(query)
        .bind(table.dataset)
        .bind(dataset_version)
        .bind(table.table)
        .bind(location_url.as_str())
        .execute(exe)
        .await?;
    Ok(())
}

/// Get all locations that were written by a specific job
#[tracing::instrument(skip(exe), err)]
pub async fn get_by_job_id<'c, E>(exe: E, job_id: JobId) -> Result<Vec<Location>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {"
        SELECT id, dataset, dataset_version, tbl, url, active
        FROM locations
        WHERE writer = $1
    "};

    let locations = sqlx::query_as(query).bind(job_id).fetch_all(exe).await?;
    Ok(locations)
}

/// Assign a job as the writer for multiple locations
#[tracing::instrument(skip(exe), err)]
pub async fn assign_job_writer<'c, E>(
    exe: E,
    locations: &[LocationId],
    job_id: JobId,
) -> Result<(), sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {"
        UPDATE locations 
        SET writer = $1 
        WHERE id = ANY($2)
    "};

    sqlx::query(query)
        .bind(job_id)
        .bind(locations)
        .execute(exe)
        .await?;
    Ok(())
}

/// Basic location information from the database
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Location {
    /// Unique identifier for the location
    pub id: LocationId,
    /// Name of the dataset this location belongs to
    pub dataset: String,
    /// Version of the dataset (empty string if unversioned)
    pub dataset_version: String,
    /// Name of the table within the dataset
    pub tbl: String,
    /// Full URL to the storage location
    #[sqlx(try_from = "&'a str")]
    pub url: Url,
    /// Whether this location is currently active for queries
    pub active: bool,
}
