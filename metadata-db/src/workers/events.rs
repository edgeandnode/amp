//! Worker node queue actions channel
//!
//! This module provides a channel for worker nodes to send and receive notifications
//! via Postgres's `LISTEN`/`NOTIFY` mechanism. This allows the worker nodes to be notified
//! when a new job is available reducing the latency of the job scheduling.

use futures::stream::{Stream, TryStreamExt as _};
use sqlx::{postgres::PgListener, Postgres};

use super::{jobs::JobId, node_id::WorkerNodeId};

/// The worker actions PostgreSQL notification channel name
const WORKER_ACTIONS_CHANNEL: &str = "worker_actions";

/// Sends a notification with the given payload to the worker actions channel using the provided executor.
///
/// # Delivery Guarantees
///
/// - Notifications sent before the `LISTEN` command is issued will not be delivered.
/// - Notifications may be lost during automatic retry of a closed DB connection.
#[tracing::instrument(skip_all, err)]
pub async fn notify<'c, E>(exe: E, payload: JobNotification) -> Result<(), JobNotifSendError>
where
    E: sqlx::Executor<'c, Database = Postgres>,
{
    let payload_str =
        serde_json::to_string(&payload).map_err(JobNotifSendError::SerializationFailed)?;

    let query = format!(r#"SELECT pg_notify('{}', $1)"#, WORKER_ACTIONS_CHANNEL);
    sqlx::query(&query)
        .bind(&payload_str)
        .execute(exe)
        .await
        .map_err(JobNotifSendError::DbError)?;
    Ok(())
}

/// An error that can occur when sending a notification to the worker actions channel
#[derive(Debug, thiserror::Error)]
pub enum JobNotifSendError {
    /// The notification payload serialization failed
    #[error("payload serialization failed: {0}")]
    SerializationFailed(#[source] serde_json::Error),

    /// An error occurred while sending the notification
    #[error(transparent)]
    DbError(sqlx::Error),
}

/// Establishes a new [`Listener`] by connecting to the specified URL.
///
/// See [`PgListener`] for more details.
#[tracing::instrument(skip_all, err)]
pub async fn listen_url(url: &str) -> Result<JobNotifListener, sqlx::Error> {
    JobNotifListener::connect(url).await
}

/// A listener for notifications on the Metadata DB worker actions channel.
///
/// # Delivery Guarantees
///
/// - Notifications sent before the `LISTEN` command is issued will not be delivered.
/// - Notifications may be lost during automatic retry of a closed DB connection.
pub struct JobNotifListener(PgListener);

impl JobNotifListener {
    /// Connects to the worker actions channel using `LISTEN`
    async fn connect(url: &str) -> Result<Self, sqlx::Error> {
        let mut listener = PgListener::connect(url).await?;
        listener.listen(WORKER_ACTIONS_CHANNEL).await?;
        Ok(Self(listener))
    }

    /// Consumes the [`Listener`] and returns the inner [`PgListener`].
    pub fn into_inner(self) -> PgListener {
        self.0
    }

    /// Converts the listener into a stream of [`Notification`]s.
    ///
    /// # Error cases
    ///
    /// This stream will generally not return an error, except on failure to estabilish the intial
    /// connection, because connection errors are retried.
    pub fn into_stream(self) -> impl Stream<Item = Result<JobNotification, JobNotifRecvError>> {
        self.0
            .into_stream()
            .map_err(JobNotifRecvError::DbError)
            .and_then(|notif| async move {
                serde_json::from_str(notif.payload())
                    .map_err(JobNotifRecvError::DeserializationFailed)
            })
    }
}

/// An error that can occur when listening for worker actions
#[derive(Debug, thiserror::Error)]
pub enum JobNotifRecvError {
    /// An error occurred while receiving the notification
    #[error(transparent)]
    DbError(sqlx::Error),

    /// The notification payload deserialization failed
    #[error("payload deserialization failed: {0}")]
    DeserializationFailed(#[source] serde_json::Error),
}

/// The payload of a worker action notification
#[derive(serde::Serialize, serde::Deserialize)]
pub struct JobNotification {
    pub node_id: WorkerNodeId,
    pub job_id: JobId,
    pub action: JobNotifAction,
}

impl JobNotification {
    /// Create a new start action
    pub fn start(node_id: WorkerNodeId, job_id: JobId) -> Self {
        Self {
            node_id,
            job_id,
            action: JobNotifAction::Start,
        }
    }

    /// Create a new stop action
    pub fn stop(node_id: WorkerNodeId, job_id: JobId) -> Self {
        Self {
            node_id,
            job_id,
            action: JobNotifAction::Stop,
        }
    }
}

impl std::fmt::Debug for JobNotification {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&serde_json::to_string(self).expect("failed to serialize notification"))
    }
}

/// These actions coordinate the jobs state and the write lock on the output table locations.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub enum JobNotifAction {
    /// Start the job
    ///
    /// Fetch the job descriptor from the Metdata DB job queue and start the job.
    Start,

    /// Stop the job
    ///
    /// Stop the job: mark the job as stopped and release the locations by deleting
    /// the row from the `jobs` table. // TODO: Review the job deletion event
    Stop,
}

impl JobNotifAction {
    /// Returns the string representation of the action
    pub fn as_str(&self) -> &str {
        match self {
            Self::Start => "START",
            Self::Stop => "STOP",
        }
    }
}

impl std::fmt::Display for JobNotifAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for JobNotifAction {
    type Err = Box<dyn std::error::Error + Send + Sync>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            s if s.eq_ignore_ascii_case("START") => Ok(Self::Start),
            s if s.eq_ignore_ascii_case("STOP") => Ok(Self::Stop),
            _ => Err(format!("Invalid action variant: {s}").into()),
        }
    }
}

impl serde::Serialize for JobNotifAction {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> serde::Deserialize<'de> for JobNotifAction {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s: &str = serde::Deserialize::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}
