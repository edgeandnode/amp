use std::time::Duration;

use sqlx::{Executor, Postgres};

use super::{
    Worker, WorkerInfo,
    node_id::{NodeId, NodeIdOwned},
};

/// Registers a worker.
///
/// If the worker already exists, its `heartbeat_at`, `info`, and `registered_at` columns are updated.
/// The `created_at` column is set only on the initial insert.
#[tracing::instrument(skip(exe), err)]
pub async fn register<'c, E>(
    exe: E,
    id: NodeId<'_>,
    info: WorkerInfo<'_>,
) -> Result<(), sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        INSERT INTO workers (node_id, info, created_at, registered_at, heartbeat_at)
        VALUES ($1, $2::jsonb, timezone('UTC', now()), timezone('UTC', now()), timezone('UTC', now()))
        ON CONFLICT (node_id) DO UPDATE SET
            info = EXCLUDED.info,
            registered_at = timezone('UTC', now()),
            heartbeat_at = timezone('UTC', now())
    "#};
    sqlx::query(query).bind(id).bind(info).execute(exe).await?;
    Ok(())
}

/// Updates the `heartbeat_at` column for a given worker
#[tracing::instrument(skip(exe), err)]
pub async fn update_heartbeat<'c, E>(exe: E, id: NodeId<'_>) -> Result<(), sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        UPDATE workers
        SET heartbeat_at = timezone('UTC', now())
        WHERE node_id = $1
    "#};
    sqlx::query(query).bind(id).execute(exe).await?;
    Ok(())
}

/// Returns a worker by its node ID.
///
/// Returns `None` if no worker with the given node_id exists.
#[tracing::instrument(skip(exe), err)]
pub async fn get_by_id<'c, E>(exe: E, id: NodeId<'_>) -> Result<Option<Worker>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT node_id, info, created_at, registered_at, heartbeat_at
        FROM workers
        WHERE node_id = $1
    "#};
    sqlx::query_as(query).bind(id).fetch_optional(exe).await
}

/// Returns a list of all workers.
///
/// Returns all workers in the database with their complete information including
/// node_id, info, created_at, registered_at, and heartbeat_at.
#[tracing::instrument(skip(exe), err)]
pub async fn list<'c, E>(exe: E) -> Result<Vec<Worker>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT node_id, info, created_at, registered_at, heartbeat_at
        FROM workers
        ORDER BY id DESC
    "#};
    sqlx::query_as(query).fetch_all(exe).await
}

/// Returns a list of active workers.
///
/// A worker is active if its `heartbeat_at` timestamp is within the given active `interval`.
#[tracing::instrument(skip(exe), err)]
pub async fn list_active<'c, E>(exe: E, interval: Duration) -> Result<Vec<NodeIdOwned>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT node_id
        FROM workers
        WHERE heartbeat_at > timezone('UTC', now()) - $1
    "#};
    sqlx::query_scalar(query)
        .bind(interval)
        .fetch_all(exe)
        .await
}
