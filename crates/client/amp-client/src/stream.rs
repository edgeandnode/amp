//! Main stream implementation

use std::{
    fmt::Debug,
    future::{Future, IntoFuture},
    ops::RangeInclusive,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use async_stream::try_stream;
use common::{
    arrow::array::RecordBatch,
    metadata::segments::{BlockRange, ResumeWatermark},
};
use futures::{Stream as FuturesStream, StreamExt, stream::BoxStream};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::{
    client::{AmpClient, ResponseBatch},
    error::Error,
    state::{Action, CommitHandle, StateManager, StateManagerHandle},
    store::{InMemoryStateStore, StateStore},
};

/// Watermark checkpoint with checkpoint id tracking.
///
/// Represents a durable checkpoint in the stream.
/// - `ranges`: Block ranges confirmed complete at this checkpoint
/// - `checkpoint`: Highest id number committed at this checkpoint
///
/// The ranges can be used to construct a ResumeWatermark for sending over the wire
/// whilst the checkpoint id is used for local state tracking.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct WatermarkCheckpoint {
    /// Block ranges confirmed complete at this checkpoint
    pub ranges: Vec<BlockRange>,
    /// Highest id number committed at this checkpoint
    pub checkpoint: u64,
}

impl WatermarkCheckpoint {
    /// Create a new watermark checkpoint.
    pub fn new(ranges: Vec<BlockRange>, checkpoint: u64) -> Self {
        Self { ranges, checkpoint }
    }

    /// Convert to ResumeWatermark for sending over the wire.
    pub fn to_resume_watermark(&self) -> ResumeWatermark {
        ResumeWatermark::from_ranges(&self.ranges)
    }
}

/// Events emitted by the stateful stream.
#[derive(Debug, Clone)]
pub enum Event {
    /// Watermark checkpoint.
    Watermark {
        id: u64,
        ranges: Vec<BlockRange>,
        /// Prune batches with ids less than this value, or None if no pruning needed
        cutoff: Option<u64>,
    },

    /// New data to process.
    Data {
        id: u64,
        batch: RecordBatch,
        ranges: Vec<BlockRange>,
    },

    /// Blockchain reorg detected.
    Reorg {
        id: u64,
        /// Block ranges at the new checkpoint after reorg
        ranges: Vec<BlockRange>,
        /// Range of ids to invalidate (inclusive), or None if no id affected
        invalidation: Option<RangeInclusive<u64>>,
    },

    /// Rewind required after stream restart.
    ///
    /// Emitted when reconnecting after a crash/disconnect and we detect uncommitted batches
    /// beyond the last watermark. Consumer must delete all data with the invalidated ids.
    ///
    /// The stream will then replay from the watermark with fresh transaction ids.
    Rewind {
        id: u64,
        /// Block ranges at the checkpoint to rewind to
        ranges: Vec<BlockRange>,
        /// Range of ids to invalidate (inclusive), or None if no id affected
        invalidation: Option<RangeInclusive<u64>>,
    },
}

/// Builder for configuring and creating a streaming query.
///
/// Provides a fluent API for configuring all aspects of a stream including:
/// - Initial state (watermark + ranges for resumption)
/// - Buffer retention for deduplication and reorg filtering
///
/// # Example
///
/// ```rust,ignore
/// // Simple usage - just await the builder
/// let stream = client.stream("SELECT * FROM eth.logs SETTINGS stream = true").await?;
///
/// // With configuration
/// let stream = client.stream("SELECT * FROM eth.logs SETTINGS stream = true")
///     .with_state_store(my_store)  // Use persistent state store
///     .await?;
/// ```
pub struct StreamBuilder {
    client: AmpClient,
    sql: String,
    store: Option<Box<dyn StateStore>>,
    retention: u64,
}

impl StreamBuilder {
    /// Create a new StreamBuilder.
    ///
    /// # Arguments
    /// - `client`: Amp client
    /// - `sql`: SQL query string (should include `SETTINGS stream = true`)
    pub(crate) fn new(client: AmpClient, sql: impl Into<String>) -> Self {
        Self {
            client,
            sql: sql.into(),
            store: None,
            retention: 128,
        }
    }

    /// Provide a custom state store implementation.
    ///
    /// By default, `InMemoryStateStore` is used (suitable for testing/prototyping).
    /// For production use, provide a persistent store.
    ///
    /// # Example
    /// ```rust,ignore
    /// let stream = client.stream("SELECT * FROM eth.logs SETTINGS stream = true")
    ///     .with_state_store(LmdbStore::new("state.db"))
    ///     .await?;
    /// ```
    pub fn with_state_store(mut self, store: impl StateStore + 'static) -> Self {
        self.store = Some(Box::new(store));
        self
    }

    /// Set the retention window in blocks for the buffered batches.
    ///
    /// This determines how far back in history we can detect reorgs.
    ///
    /// # Example
    /// ```rust,ignore
    /// let stream = client.stream("SELECT * FROM eth.logs SETTINGS stream = true")
    ///     .with_retention_blocks(128)
    ///     .await?;
    /// ```
    pub fn with_retention_blocks(mut self, blocks: u64) -> Self {
        self.retention = blocks;
        self
    }
}

impl IntoFuture for StreamBuilder {
    type Output = Result<Stream, Error>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send>>;

    fn into_future(mut self) -> Self::IntoFuture {
        Box::pin(async move {
            let store = self.store.unwrap_or(Box::new(InMemoryStateStore::new()));
            let manager = StateManager::new(store, self.retention);

            // Get watermark checkpoint and resume watermark for reconnection
            let watermark = manager
                .watermark()
                .as_ref()
                .map(|w| w.to_resume_watermark());
            let stream = self
                .client
                .request(&self.sql, watermark.as_ref())
                .await?
                .boxed();

            // Create stateful stream (adds reorg detection and state tracking)
            Stream::create(stream, Arc::new(Mutex::new(manager))).await
        })
    }
}

/// Stateful streaming query with state tracking, reorg detection, and exactly-once semantics.
///
/// Provides:
/// - Reorg detection by comparing incoming block ranges against previous ranges
/// - State tracking (consumed batches buffer for reorg id range computation)
/// - Rewind detection on reconnection (uncommitted batches beyond last watermark)
/// - Buffer retention/pruning based on block ranges
/// - Commit handles for atomic state changes with exactly-once semantics
///
/// On stream creation, automatically loads persisted state and emits a Rewind event if
/// necessary. This provides robust crash recovery. Just create a new stream with
/// the same state store and it will resume from the last watermark.
///
/// # Usage
///
/// ## Low-Level: Manual Commit
/// ```rust,ignore
/// let mut stream = client.stream("SELECT * FROM eth.logs SETTINGS stream = true")
///     .with_state_store(LmdbStore::new("state.db"))
///     .await?;
///
/// while let Some(result) = stream.next().await {
///     let (event, commit) = result?;
///     match event {
///         Event::Data { batch, id, .. } => {
///             save(batch, id).await?;
///             commit.await?;
///         }
///         Event::Watermark { ranges, cutoff, .. } => {
///             checkpoint(ranges).await?;
///             prune_data_before(cutoff).await?;
///             commit.await?;
///         }
///         Event::Reorg { invalidation, .. } => {
///             rollback(invalidation).await?;
///             commit.await?;
///         }
///         Event::Rewind { invalidation, .. } => {
///             rollback(invalidation).await?;
///             commit.await?;
///         }
///     }
/// }
/// ```
///
/// ## High-Level: Auto-Commit
/// ```rust,ignore
/// client.stream("SELECT * FROM eth.logs SETTINGS stream = true")
///     .with_state_store(LmdbStore::new("state.db"))
///     .await?
///     .for_each(|event| async move {
///         match event {
///             Event::Data { batch, id, .. } => {
///                 save(batch, id).await?;
///             }
///             Event::Rewind { invalidation, .. } => {
///                 rollback(invalidation).await?;
///             }
///             Event::Reorg { invalidation, .. } => {
///                 rollback(invalidation).await?;
///             }
///             _ => {}
///         }
///         Ok(())
///     })
///     .await?;
/// ```
pub struct Stream {
    stream: BoxStream<'static, Result<(Event, CommitHandle), Error>>,
}

impl Stream {
    pub(crate) async fn create(
        mut responses: BoxStream<'static, Result<ResponseBatch, Error>>,
        state: Arc<Mutex<StateManager>>,
    ) -> Result<Self, Error> {
        let stream = try_stream! {
            // Check if rewind is needed on startup and get initial ranges
            let (rewind, mut previous) = {
                let mgr = state.lock().await;
                let checkpoint = mgr.watermark().as_ref().map(|w| w.checkpoint).unwrap_or(0);
                let next = mgr.peek();
                let rewind = next > checkpoint + 1;
                let ranges = mgr.watermark()
                    .as_ref()
                    .map(|w| w.ranges.clone())
                    .unwrap_or_default();
                (rewind, ranges)
            };

            if rewind {
                yield state.execute(Action::Rewind).await?;
            }

            while let Some(response) = responses.next().await {
                let batch = response?;
                let reorg = batch.metadata.ranges.iter().any(|current| {
                    previous
                        .iter()
                        .find(|r| r.network == current.network)
                        .is_some_and(|previous| {
                            (current != previous) && (current.start() <= previous.end())
                        })
                });

                if reorg {
                    let action = Action::Reorg {
                        previous: previous.clone(),
                        incoming: batch.metadata.ranges.clone(),
                    };

                    yield state.execute(action).await?;
                }

                if batch.metadata.ranges_complete {
                    let action = Action::Watermark {
                        ranges: batch.metadata.ranges.clone(),
                    };

                    yield state.execute(action).await?;
                } else {
                    let action = Action::Data {
                        batch: batch.data,
                        ranges: batch.metadata.ranges.clone(),
                    };

                    yield state.execute(action).await?;
                }

                previous = batch.metadata.ranges.clone();
            }
        }
        .boxed();

        Ok(Self { stream })
    }

    /// High-level consumer: auto-commit after callback succeeds.
    ///
    /// # Semantics
    /// - Callback is called with simplified event (no commit handle)
    /// - If callback succeeds, event is automatically committed
    /// - If callback fails, stream stops and pending batch remains
    /// - On restart, uncommitted batch triggers Rewind event
    ///
    /// # Example
    /// ```rust,ignore
    /// stream.for_each(|event| async move {
    ///     match event {
    ///         Event::Data { batch, id, .. } => {
    ///             save(batch, id).await?;
    ///         }
    ///         Event::Rewind { invalidation, .. } => {
    ///             rollback(invalidation).await?;
    ///         }
    ///         _ => {}
    ///     }
    ///     Ok(())
    /// }).await?;
    /// ```
    pub async fn for_each<F, Fut>(mut self, mut f: F) -> Result<(), Error>
    where
        F: FnMut(Event) -> Fut,
        Fut: Future<Output = Result<(), Error>>,
    {
        while let Some(result) = self.next().await {
            let (event, commit) = result?;
            f(event).await?;
            commit.await?;
        }
        Ok(())
    }
}

impl FuturesStream for Stream {
    type Item = Result<(Event, CommitHandle), Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.stream).poll_next(cx)
    }
}
