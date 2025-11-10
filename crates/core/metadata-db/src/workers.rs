use std::time::Duration;

use futures::future::BoxFuture;
use sqlx::types::chrono::{DateTime, Utc};
use tokio::time::MissedTickBehavior;

pub mod events;
mod info;
mod node_id;
pub(crate) mod sql;

pub use self::{
    info::{WorkerInfo, WorkerInfoOwned},
    node_id::{NodeId as WorkerNodeId, NodeIdOwned as WorkerNodeIdOwned},
};
use crate::{
    MetadataDb,
    db::{Connection, Executor},
    error::Error,
    workers::events::NotifListener,
};

/// Frequency on which to send a heartbeat.
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);

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
        .map_err(Error::DbError)
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
        .map_err(Error::DbError)
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
    sql::list(exe).await.map_err(Error::DbError)
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
        .map_err(Error::DbError)
}

/// Establish a dedicated connection to the metadata DB, and return a future that loops
/// forever, updating the worker's heartbeat in the dedicated DB connection.
///
/// If the initial connection fails, an error is returned.
pub async fn heartbeat_loop(
    metadata_db: &MetadataDb,
    node_id: impl Into<WorkerNodeIdOwned>,
) -> Result<BoxFuture<'static, Result<(), Error>>, Error> {
    let mut conn = Connection::connect(&metadata_db.url).await?;

    let node_id = node_id.into();
    let fut = async move {
        let mut interval = tokio::time::interval(HEARTBEAT_INTERVAL);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            interval.tick().await;
            sql::update_heartbeat(&mut conn, node_id.clone())
                .await
                .map_err(Error::DbError)?;
        }
    };

    Ok(Box::pin(fut))
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
        .map_err(|err| {
            Error::JobNotificationRecvError(crate::workers::events::NotifRecvError::DbError(err))
        })
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
        .map_err(Error::JobNotificationSendError)
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
