//! Metadata DB worker node tracking

use std::time::Duration;

use sqlx::{Executor, Postgres};

use super::WorkerNodeId;

/// Registers a worker.
///
/// If the worker already exists, its `last_heartbeat` column is updated.
#[tracing::instrument(skip(exe), err)]
pub async fn register_worker<'c, E>(exe: E, id: &WorkerNodeId) -> Result<(), sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        INSERT INTO workers (node_id, last_heartbeat)
        VALUES ($1, timezone('UTC', now()))
        ON CONFLICT (node_id) DO UPDATE SET last_heartbeat = timezone('UTC', now())
    "#};
    sqlx::query(query).bind(id).execute(exe).await?;
    Ok(())
}

/// Updates the `last_heartbeat` column for a given worker
#[tracing::instrument(skip(exe), err)]
pub async fn update_heartbeat<'c, E>(exe: E, id: &WorkerNodeId) -> Result<(), sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        UPDATE workers 
        SET last_heartbeat = timezone('UTC', now())
        WHERE node_id = $1
    "#};
    sqlx::query(query).bind(id).execute(exe).await?;
    Ok(())
}

/// Returns a list of active workers.
///
/// A worker is active if its `last_heartbeat` column is within the given active `interval`.
#[tracing::instrument(skip(exe), err)]
pub async fn get_active_workers<'c, E>(
    exe: E,
    interval: Duration,
) -> Result<Vec<WorkerNodeId>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT node_id 
        FROM workers 
        WHERE last_heartbeat > timezone('UTC', now()) - $1
    "#};
    sqlx::query_scalar(query)
        .bind(interval)
        .fetch_all(exe)
        .await
}
