use std::time::Duration;

use sqlx::{
    Executor, Postgres,
    types::chrono::{DateTime, Utc},
};

pub mod events;
mod node_id;

pub use self::node_id::{NodeId, NodeIdOwned};

/// Registers a worker.
///
/// If the worker already exists, its `last_heartbeat` column is updated.
#[tracing::instrument(skip(exe), err)]
pub async fn register<'c, E>(exe: E, id: NodeId<'_>) -> Result<(), sqlx::Error>
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
pub async fn update_heartbeat<'c, E>(exe: E, id: NodeId<'_>) -> Result<(), sqlx::Error>
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

/// Returns a list of all workers.
///
/// Returns all workers in the database with their complete information including
/// id, node_id, and last_heartbeat timestamp.
#[tracing::instrument(skip(exe), err)]
pub async fn list<'c, E>(exe: E) -> Result<Vec<Worker>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT node_id, last_heartbeat
        FROM workers
        ORDER BY id DESC
    "#};
    sqlx::query_as(query).fetch_all(exe).await
}

/// Returns a list of active workers.
///
/// A worker is active if its `last_heartbeat` column is within the given active `interval`.
#[tracing::instrument(skip(exe), err)]
pub async fn list_active<'c, E>(exe: E, interval: Duration) -> Result<Vec<NodeIdOwned>, sqlx::Error>
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

/// Represents a worker node in the metadata database.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Worker {
    /// ID of the worker node
    pub node_id: NodeIdOwned,
    /// Last heartbeat timestamp
    pub last_heartbeat: DateTime<Utc>,
}

/// In-tree integration tests
#[cfg(test)]
mod tests {
    mod it_events;
    mod it_heartbeat;
}
