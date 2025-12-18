use std::time::Duration;

use sqlx::types::chrono::{DateTime, Utc};

pub mod events;
mod info;
mod node_id;
pub(crate) mod sql;

pub use self::{
    info::{WorkerInfo, WorkerInfoOwned},
    node_id::{NodeId as WorkerNodeId, NodeIdOwned as WorkerNodeIdOwned},
};
use crate::{MetadataDb, db::Executor, error::Error, workers::events::NotifListener};

/// Register a worker in the metadata database
///
/// Inserts worker into `workers` table with ON CONFLICT UPDATE for idempotency.
/// If worker exists, updates `info`, `registered_at`, and `heartbeat_at` fields.
/// The `created_at` field is only set on initial insert.
#[tracing::instrument(skip(exe), err)]
pub async fn register<'c, E>(
    exe: E,
    node_id: impl Into<WorkerNodeId<'c>> + std::fmt::Debug,
    info: impl Into<WorkerInfo<'c>> + std::fmt::Debug,
) -> Result<(), Error>
where
    E: Executor<'c>,
{
    sql::register(exe, node_id.into(), info.into())
        .await
        .map_err(Error::Database)
}

/// Get worker by node ID
///
/// Returns worker information from `workers` table. Returns `None` if
/// no worker with the given node_id exists.
#[tracing::instrument(skip(exe), err)]
pub async fn get_by_id<'c, E>(
    exe: E,
    node_id: impl Into<WorkerNodeId<'c>> + std::fmt::Debug,
) -> Result<Option<Worker>, Error>
where
    E: Executor<'c>,
{
    sql::get_by_id(exe, node_id.into())
        .await
        .map_err(Error::Database)
}

/// List all workers
///
/// Returns all workers in the database with their complete information including
/// node_id, info, created_at, registered_at, and heartbeat_at.
#[tracing::instrument(skip(exe), err)]
pub async fn list<'c, E>(exe: E) -> Result<Vec<Worker>, Error>
where
    E: Executor<'c>,
{
    sql::list(exe).await.map_err(Error::Database)
}

/// List active workers
///
/// Returns node IDs of workers whose `heartbeat_at` timestamp is within the
/// given `interval` from the current time. Workers outside this interval
/// are considered inactive.
#[tracing::instrument(skip(exe), err)]
pub async fn list_active<'c, E>(exe: E, interval: Duration) -> Result<Vec<WorkerNodeIdOwned>, Error>
where
    E: Executor<'c>,
{
    sql::list_active(exe, interval)
        .await
        .map_err(Error::Database)
}

/// Locks a PG advisory lock on the given worker node ID.
///
/// Returns whether a lock on the given node ID was successfully acquired.
/// The lock is held for as long as the connection stays open.
#[tracing::instrument(skip(exe), err)]
pub async fn lock_node_id<'c, E>(
    exe: E,
    node_id: impl Into<WorkerNodeId<'_>> + std::fmt::Debug,
) -> Result<bool, Error>
where
    E: Executor<'c>,
{
    sql::lock_node_id(exe, node_id.into())
        .await
        .map_err(Error::Database)
}

/// Updates the `heartbeat_at` column for a given worker.
#[tracing::instrument(skip(exe), err)]
pub async fn update_heartbeat<'c, E>(
    exe: E,
    node_id: impl Into<WorkerNodeId<'_>> + std::fmt::Debug,
) -> Result<(), Error>
where
    E: Executor<'c>,
{
    sql::update_heartbeat(exe, node_id.into())
        .await
        .map_err(Error::Database)
}

/// Listen to the job actions notification channel for job notifications.
///
/// Establishes a listener that will only yield notifications targeted to the specified `node_id`.
///
/// # Delivery Guarantees
/// - Notifications sent before the `LISTEN` command is issued will not be delivered.
/// - Notifications may be lost during automatic retry of a closed DB connection.
#[tracing::instrument(skip(metadata_db), err)]
pub async fn listen_for_job_notif(
    metadata_db: &MetadataDb,
    node_id: impl Into<WorkerNodeIdOwned> + std::fmt::Debug,
) -> Result<NotifListener, Error> {
    events::listen_url(&metadata_db.url, node_id.into())
        .await
        .map_err(|err| Error::JobNotificationRecv(events::NotifRecvError::Database(err)))
}

/// Send a job notification to a worker.
///
/// This function sends a notification with a custom payload to the specified worker node.
/// The payload must implement `serde::Serialize` and will be serialized to JSON.
///
/// Typically called after successful job state transitions (e.g., after scheduling a job
/// or requesting a job stop) to notify the worker of the change.
#[tracing::instrument(skip(exe, payload), err)]
pub async fn send_job_notif<'c, E, T>(
    exe: E,
    node_id: impl Into<WorkerNodeIdOwned> + std::fmt::Debug,
    payload: &T,
) -> Result<(), Error>
where
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    T: serde::Serialize,
{
    events::notify(exe, node_id.into(), payload)
        .await
        .map_err(Error::JobNotificationSend)
}

/// Represents a worker node in the metadata database.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Worker {
    /// ID of the worker node
    pub node_id: WorkerNodeIdOwned,
    /// Worker metadata (build info, system info, etc.)
    pub info: WorkerInfoOwned,
    /// Timestamp when the worker was first registered
    pub created_at: DateTime<Utc>,
    /// Timestamp when the worker was last registered (updated on every re-registration)
    pub registered_at: DateTime<Utc>,
    /// Last heartbeat timestamp (updated periodically by the worker)
    pub heartbeat_at: DateTime<Utc>,
}

/// In-tree integration tests
#[cfg(test)]
mod tests {
    mod it_events;
    mod it_heartbeat;
}
