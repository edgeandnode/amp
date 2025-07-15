use std::{collections::HashMap, sync::Arc};

use backon::{ExponentialBuilder, Retryable};
use metadata_db::{LocationId, MetadataDb};
use tokio::sync::{Mutex, watch};
use tokio_stream::StreamExt;
use tokio_util::task::AbortOnDropHandle;
use tracing::{debug, instrument, trace, warn};

use crate::BoxError;

const CHANGE_TRACKING_CHANNEL: &str = "change-tracking";

struct NotificationMultiplexer {
    metadata_db: MetadataDb,
    watchers: Arc<Mutex<HashMap<LocationId, watch::Sender<()>>>>,
}

pub struct NotificationMultiplexerHandle {
    watchers: Arc<Mutex<HashMap<LocationId, watch::Sender<()>>>>,
    _join_handle: AbortOnDropHandle<()>,
}

impl NotificationMultiplexerHandle {
    pub fn spawn(metadata_db: MetadataDb) -> Self {
        let watchers = Arc::new(Mutex::new(HashMap::new()));

        let multiplexer = NotificationMultiplexer {
            metadata_db: metadata_db.clone(),
            watchers: watchers.clone(),
        };
        
        let join_handle = AbortOnDropHandle::new(tokio::spawn(
            multiplexer.execute_with_retry()
        ));

        NotificationMultiplexerHandle {
            watchers,
            _join_handle: join_handle,
        }
    }


    #[instrument(skip(self))]
    pub async fn subscribe(&self, location_id: LocationId) -> watch::Receiver<()> {
        let mut watchers = self.watchers.lock().await;

        if let Some(sender) = watchers.get(&location_id) {
            return sender.subscribe();
        }

        let (sender, receiver) = watch::channel(());
        watchers.insert(location_id, sender);
        trace!("Created new watcher for location_id: {}", location_id);

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
            warn!(
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
        let mut stream = self.metadata_db.listen(CHANGE_TRACKING_CHANNEL).await?;

        debug!(
            "Connected to notification channel: {}",
            CHANGE_TRACKING_CHANNEL
        );

        while let Some(notification_result) = stream.next().await {
            let notification = notification_result?;

            let payload = notification.payload();
            match payload.parse::<LocationId>() {
                Ok(location_id) => {
                    let watchers_guard = self.watchers.lock().await;
                    let Some(sender) = watchers_guard.get(&location_id) else {
                        trace!("No watcher registered for location_id: {}", location_id);
                        continue;
                    };

                    match sender.send(()) {
                        Ok(_) => trace!("Notified watchers for location_id: {}", location_id),
                        Err(_) => trace!("No receivers for location_id: {}", location_id),
                    }
                }
                Err(e) => {
                    warn!(
                        "Failed to parse location_id from payload '{}': {}",
                        payload, e
                    );
                }
            }
        }

        // Stream ended, which typically means connection was closed
        Err(format!(
            "Listen connection closed for channel: {}",
            CHANGE_TRACKING_CHANNEL
        )
        .into())
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
