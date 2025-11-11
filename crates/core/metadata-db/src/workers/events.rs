//! Job notifications channel
//!
//! This module provides a channel for job notifications via Postgres's `LISTEN`/`NOTIFY`
//! mechanism. This allows workers to be notified when job actions are required,
//! reducing the latency of job scheduling and coordination.

use futures::stream::{Stream, TryStreamExt as _};
use serde_json::value::RawValue as JsonRawValue;
use sqlx::{Postgres, postgres::PgListener};

use super::node_id::NodeIdOwned;

/// The job actions PostgreSQL notification channel name
const JOB_ACTIONS_CHANNEL: &str = "job_actions";

/// Sends a notification with the given payload to the job actions channel using the provided executor.
///
/// # Delivery Guarantees
///
/// - Notifications sent before the `LISTEN` command is issued will not be delivered.
/// - Notifications may be lost during automatic retry of a closed DB connection.
#[tracing::instrument(skip_all, err)]
pub async fn notify<'c, E, T>(
    exe: E,
    node_id: NodeIdOwned,
    payload: &T,
) -> Result<(), NotifSendError>
where
    E: sqlx::Executor<'c, Database = Postgres>,
    T: serde::Serialize,
{
    let notif = serde_json::to_string(&Notification { node_id, payload })
        .map_err(NotifSendError::SerializationFailed)?;

    let query = format!(r#"SELECT pg_notify('{}', $1)"#, JOB_ACTIONS_CHANNEL);
    sqlx::query(&query)
        .bind(notif)
        .execute(exe)
        .await
        .map_err(NotifSendError::Database)?;
    Ok(())
}

/// An error that can occur when sending a notification to the job actions channel
#[derive(Debug, thiserror::Error)]
pub enum NotifSendError {
    /// The notification payload serialization failed
    #[error("payload serialization failed: {0}")]
    SerializationFailed(#[source] serde_json::Error),

    /// An error occurred while sending the notification
    #[error(transparent)]
    Database(sqlx::Error),
}

/// Establishes a new [`Listener`] by connecting to the specified URL.
///
/// The listener will only yield notifications targeted to the specified `node_id`.
///
/// See [`PgListener`] for more details.
#[tracing::instrument(skip_all, err)]
pub async fn listen_url(url: &str, node_id: NodeIdOwned) -> Result<NotifListener, sqlx::Error> {
    NotifListener::connect(url, node_id).await
}

/// A listener for notifications on the Metadata DB job actions channel.
///
/// The listener filters notifications to only those targeted to its configured `node_id`.
///
/// # Delivery Guarantees
///
/// - Notifications sent before the `LISTEN` command is issued will not be delivered.
/// - Notifications may be lost during automatic retry of a closed DB connection.
pub struct NotifListener {
    listener: PgListener,
    node_id: NodeIdOwned,
}

impl NotifListener {
    /// Connects to the job actions channel using `LISTEN`
    async fn connect(url: &str, node_id: NodeIdOwned) -> Result<Self, sqlx::Error> {
        let mut listener = PgListener::connect(url).await?;
        listener.listen(JOB_ACTIONS_CHANNEL).await?;
        Ok(Self { listener, node_id })
    }

    /// Converts the listener into a stream of notification payloads.
    ///
    /// Only yields notifications where the target node_id matches this listener's node_id.
    /// Notifications for other nodes are silently filtered out.
    ///
    /// # Error cases
    ///
    /// This stream will generally not return an error, except on failure to establish the initial
    /// connection, because connection errors are retried.
    pub fn into_stream<T>(self) -> impl Stream<Item = Result<T, NotifRecvError>>
    where
        T: serde::de::DeserializeOwned,
    {
        let node_id = self.node_id;
        self.listener
            .into_stream()
            .map_err(NotifRecvError::Database)
            .try_filter_map(move |notif| {
                let node_id = node_id.clone();
                async move {
                    // Deserialize the envelope first and defer the actual payload deserialization
                    // until we know it's for us
                    let envelope: Notification<&JsonRawValue> =
                        serde_json::from_str(notif.payload())
                            .unwrap_or_else(|err| unreachable!("malformed notification: {err}"));

                    // Filter out notifications not targeted to this node
                    if envelope.node_id != node_id {
                        return Ok(None);
                    }

                    let payload = serde_json::from_str(envelope.payload.get())
                        .map_err(NotifRecvError::DeserializationFailed)?;

                    Ok(Some(payload))
                }
            })
    }
}

/// An error that can occur when listening for notifications
#[derive(Debug, thiserror::Error)]
pub enum NotifRecvError {
    /// An error occurred while receiving the notification
    #[error(transparent)]
    Database(sqlx::Error),

    /// The notification payload deserialization failed
    #[error("payload deserialization failed: {0}")]
    DeserializationFailed(#[source] serde_json::Error),
}

/// A notification envelope containing the target worker node ID and the payload.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct Notification<T> {
    #[serde(
        serialize_with = "serialize_node_id",
        deserialize_with = "deserialize_node_id"
    )]
    node_id: NodeIdOwned,
    payload: T,
}

fn serialize_node_id<S>(node_id: &NodeIdOwned, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serde::Serialize::serialize(node_id.as_str(), serializer)
}

fn deserialize_node_id<'de, D>(deserializer: D) -> Result<NodeIdOwned, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = serde::Deserialize::deserialize(deserializer)?;
    // SAFETY: Deserialized values are trusted to uphold invariants; typically from database or internal communication.
    Ok(NodeIdOwned::from_owned_unchecked(s))
}
