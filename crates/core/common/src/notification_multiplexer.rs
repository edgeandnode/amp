use std::{collections::HashMap, sync::Arc};

use backon::{ExponentialBuilder, Retryable};
use metadata_db::{LocationId, LocationNotification, MetadataDb};
use tokio::sync::{Mutex, watch};
use tokio_stream::StreamExt;
use tokio_util::task::AbortOnDropHandle;
use tracing::instrument;

use crate::BoxError;

struct NotificationMultiplexer {
    metadata_db: MetadataDb,
    watchers: Arc<Mutex<HashMap<LocationId, watch::Sender<()>>>>,
}

pub struct NotificationMultiplexerHandle {
    watchers: Arc<Mutex<HashMap<LocationId, watch::Sender<()>>>>,
    _join_handle: AbortOnDropHandle<()>,
}

/// Spawns a new notification multiplexer and returns a handle to it.
///
/// This creates a background task that maintains a single PostgreSQL LISTEN connection
/// and multiplexes notifications to multiple tokio::watch channels based on location_id.
pub fn spawn(metadata_db: MetadataDb) -> NotificationMultiplexerHandle {
    let watchers = Arc::new(Mutex::new(HashMap::new()));

    let multiplexer = NotificationMultiplexer {
        metadata_db: metadata_db.clone(),
        watchers: watchers.clone(),
    };

    let join_handle = AbortOnDropHandle::new(tokio::spawn(multiplexer.execute_with_retry()));

    NotificationMultiplexerHandle {
        watchers,
        _join_handle: join_handle,
    }
}

impl NotificationMultiplexerHandle {
    #[instrument(skip(self))]
    pub async fn subscribe(&self, location_id: LocationId) -> watch::Receiver<()> {
        let mut watchers = self.watchers.lock().await;

        if let Some(sender) = watchers.get(&location_id) {
            return sender.subscribe();
        }

        let (sender, receiver) = watch::channel(());
        watchers.insert(location_id, sender);
        tracing::trace!("Created new watcher for location_id: {}", location_id);

        receiver
    }
}

impl NotificationMultiplexer {
    async fn execute_with_retry(self) {
        let metadata_db = self.metadata_db.clone();
        let watchers = self.watchers.clone();

        let _ = (|| async {
            let multiplexer = NotificationMultiplexer {
                metadata_db: metadata_db.clone(),
                watchers: watchers.clone(),
            };
            multiplexer.execute().await
        })
        .retry(retry_policy().without_max_times())
        .notify(|err, dur| {
            tracing::warn!(
                error = %err,
                "NotificationMultiplexer execute failed. Retrying in {:.1}s",
                dur.as_secs_f32()
            );
        })
        .await;

        // The execution will only return with an error and errors are retried forever
        unreachable!()
    }

    #[instrument(skip(self))]
    async fn execute(self) -> Result<(), BoxError> {
        // Establish connection
        let listener = self.metadata_db.listen_for_location_notifications().await?;
        let mut stream = std::pin::pin!(listener.into_stream());

        tracing::debug!("Connected to notification channel: change-tracking");

        while let Some(notification_result) = stream.next().await {
            let location_id = match notification_result {
                Ok(LocationNotification(location_id)) => location_id,
                Err(err) => {
                    tracing::error!(error = ?err, "Failed to parse location notification. Continuing.");
                    continue;
                }
            };

            let watchers_guard = self.watchers.lock().await;
            let Some(sender) = watchers_guard.get(&location_id) else {
                tracing::trace!("No watcher registered for location_id: {}", location_id);
                continue;
            };

            match sender.send(()) {
                Ok(_) => tracing::trace!("Notified watchers for location_id: {}", location_id),
                Err(_) => tracing::trace!("No receivers for location_id: {}", location_id),
            }
        }

        // Stream ended, which typically means connection was closed
        Err("Listen connection closed for channel: change-tracking".into())
    }
}

/// A retry policy for notification multiplexer operations.
///
/// The retry policy is an exponential backoff with:
/// - jitter: false
/// - factor: 2
/// - min_delay: 1s
/// - max_delay: 60s
/// - max_times: 3
#[inline]
fn retry_policy() -> ExponentialBuilder {
    ExponentialBuilder::default()
}
