//! Location change notifications channel
//!
//! This module provides a channel for location change notifications via Postgres's `LISTEN`/`NOTIFY`
//! mechanism. This allows components to be notified when location data changes,
//! enabling real-time cache invalidation and data refresh.

use futures::stream::{Stream, TryStreamExt as _};
use sqlx::{Postgres, postgres::PgListener};

use crate::physical_table::LocationId;

/// The location change actions PostgreSQL notification channel name
const LOCATION_CHANGE_CHANNEL: &str = "change-tracking";

/// Sends a notification with the given location ID to the location change channel using the provided executor.
///
/// # Delivery Guarantees
///
/// - Notifications sent before the `LISTEN` command is issued will not be delivered.
/// - Notifications may be lost during automatic retry of a closed DB connection.
#[tracing::instrument(skip_all, err)]
pub async fn notify<'c, E>(exe: E, location_id: LocationId) -> Result<(), LocationNotifSendError>
where
    E: sqlx::Executor<'c, Database = Postgres>,
{
    let query = format!(r#"SELECT pg_notify('{}', $1)"#, LOCATION_CHANGE_CHANNEL);

    sqlx::query(&query)
        .bind(location_id.to_string())
        .execute(exe)
        .await
        .map_err(LocationNotifSendError)?;
    Ok(())
}

/// An error that can occur when sending a notification to the location change channel
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct LocationNotifSendError(pub sqlx::Error);

/// Establishes a new [`LocationNotifListener`] by connecting to the specified URL.
///
/// See [`PgListener`] for more details.
#[tracing::instrument(skip_all, err)]
pub async fn listen_url(url: &str) -> Result<LocationNotifListener, sqlx::Error> {
    LocationNotifListener::connect(url).await
}

/// A listener for notifications on the Metadata DB location change channel.
///
/// # Delivery Guarantees
///
/// - Notifications sent before the `LISTEN` command is issued will not be delivered.
/// - Notifications may be lost during automatic retry of a closed DB connection.
pub struct LocationNotifListener(PgListener);

impl LocationNotifListener {
    /// Connects to the location change channel using `LISTEN`
    async fn connect(url: &str) -> Result<Self, sqlx::Error> {
        let mut listener = PgListener::connect(url).await?;
        listener.listen(LOCATION_CHANGE_CHANNEL).await?;
        Ok(Self(listener))
    }

    /// Converts the listener into a stream of [`LocationNotification`]s.
    ///
    /// # Error cases
    ///
    /// This stream will generally not return an error, except on failure to estabilish the intial
    /// connection, because connection errors are retried.
    pub fn into_stream(
        self,
    ) -> impl Stream<Item = Result<LocationNotification, LocationNotifRecvError>> {
        self.0
            .into_stream()
            .map_err(LocationNotifRecvError::DbError)
            .and_then(|notif| async move {
                let location_id: LocationId = notif.payload().parse().map_err(|err| {
                    LocationNotifRecvError::PayloadParsingFailed {
                        payload: notif.payload().to_string(),
                        source: Box::new(err),
                    }
                })?;
                Ok(LocationNotification(location_id))
            })
    }
}

/// An error that can occur when listening for location change actions
#[derive(Debug, thiserror::Error)]
pub enum LocationNotifRecvError {
    /// The notification payload parsing failed
    #[error("Invalid LocationId '{payload}': {source}")]
    PayloadParsingFailed {
        payload: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// An error occurred while receiving the notification
    #[error(transparent)]
    DbError(sqlx::Error),
}

/// A location change notification containing the LocationId
#[derive(Debug, Clone)]
pub struct LocationNotification(pub LocationId);
