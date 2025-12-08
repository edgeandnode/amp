//! CDC (Change Data Capture) streaming with at-least-once delivery semantics.
//!
//! # Example
//!
//! ```rust,ignore
//! use amp_client::{AmpClient, CdcEvent, InMemoryStateStore, InMemoryBatchStore};
//! use futures::StreamExt;
//!
//! let client = AmpClient::from_endpoint("http://localhost:1602").await?;
//!
//! // Create CDC stream with separate state and batch stores
//! let state_store = InMemoryStateStore::new();
//! let batch_store = InMemoryBatchStore::new();
//! let mut stream = client
//!     .stream("SELECT * FROM eth.logs WHERE address = '0x...'")
//!     .cdc(state_store, batch_store, 128)
//!     .await?;
//!
//! while let Some(result) = stream.next().await {
//!     let (event, commit) = result?;
//!
//!     match event {
//!         CdcEvent::Insert { batch, .. } => {
//!             for row in batch.rows() {
//!                 // Process the row
//!             }
//!             commit.await?;
//!         }
//!         CdcEvent::Delete { mut batches, .. } => {
//!             while let Some(result) = batches.next().await {
//!                 let (id, batch) = result?;
//!                 for row in batch.rows() {
//!                     // Process the row
//!                 }
//!             }
//!             commit.await?;
//!         }
//!     }
//! }
//! ```

use std::{
    future::{Future, IntoFuture},
    pin::Pin,
    sync::Arc,
};

use arrow::{array::RecordBatch, datatypes::SchemaRef};
use async_stream::try_stream;
use futures::{Stream, StreamExt, stream::BoxStream};
use tokio::sync::Mutex;

use crate::{
    BlockNum,
    client::{AmpClient, HasSchema},
    error::Error,
    store::{BatchStore, StateStore},
    transactional::{
        CommitHandle, TransactionEvent, TransactionId, TransactionalStream,
        TransactionalStreamBuilder,
    },
};

/// Iterator over delete batches that loads them lazily.
///
/// This iterator is returned for Undo events and allows consumers to
/// process delete batches one at a time without loading them all into memory.
pub struct DeleteBatchIterator {
    store: Arc<Mutex<Box<dyn BatchStore>>>,
    ids: Vec<TransactionId>,
    current: usize,
}

impl DeleteBatchIterator {
    pub(crate) fn new(store: Arc<Mutex<Box<dyn BatchStore>>>, ids: Vec<TransactionId>) -> Self {
        Self {
            store,
            ids,
            current: 0,
        }
    }

    /// Get the next delete batch, loading it lazily from storage.
    pub async fn next(&mut self) -> Option<Result<(TransactionId, RecordBatch), Error>> {
        while self.current < self.ids.len() {
            let id = self.ids[self.current];
            self.current += 1;

            let store = self.store.lock().await;
            match store.load(id).await {
                Ok(Some(batch)) => return Some(Ok((id, batch))),
                Ok(None) => continue, // Skip missing batches (watermarks)
                Err(e) => return Some(Err(e)),
            }
        }
        None
    }
}

/// CDC events emitted by the CDC stream.
///
/// Simplified interface for forwarding consumers - only Insert and Delete events.
/// Watermarks are handled internally by the system.
pub enum CdcEvent {
    /// Insert event
    Insert {
        /// Transaction ID uniquely identifying this data
        id: TransactionId,
        /// Data batch to forward
        batch: RecordBatch,
    },

    /// Delete event
    ///
    /// Emitted during rewind (crash recovery) or reorg (chain reorganization).
    /// The iterator lazily loads batches as they are consumed, avoiding memory issues.
    ///
    /// # Usage
    /// ```rust,ignore
    /// match event {
    ///     CdcEvent::Delete { mut batches, .. } => {
    ///         while let Some(result) = batches.next().await {
    ///             let (id, batch) = result?;
    ///             forward_delete(id, batch).await?;
    ///         }
    ///     }
    ///     _ => {}
    /// }
    /// ```
    Delete {
        /// Transaction ID uniquely identifying this operation
        id: TransactionId,
        /// Iterator over batches to delete (loads lazily)
        batches: DeleteBatchIterator,
    },
}

/// CDC stream with at-least-once delivery semantics.
///
/// Wraps a transactional stream and maintains a separate BatchStore to store batch content.
/// This enables generating Delete events with original data for forwarding consumers.
///
/// # Example
///
/// ```rust,ignore
/// let state_store = InMemoryStateStore::new();
/// let batch_store = InMemoryBatchStore::new();
/// let mut stream = client
///     .stream("SELECT * FROM eth.logs")
///     .cdc(state_store, batch_store, 128)
///     .await?;
///
/// while let Some(result) = stream.next().await {
///     let (event, commit) = result?;
///
///     match event {
///         CdcEvent::Insert { id, batch } => {
///             forward_insert(id, batch).await?;
///             commit.await?;
///         }
///         CdcEvent::Delete { mut batches } => {
///             // Process all delete batches before committing
///             while let Some(result) = batches.next().await {
///                 let (id, batch) = result?;
///                 forward_delete(id, batch).await?;
///             }
///             commit.await?;
///         }
///     }
/// }
/// ```
pub struct CdcStream {
    inner: BoxStream<'static, Result<(CdcEvent, CommitHandle), Error>>,
    schema: SchemaRef,
}

impl CdcStream {
    /// Create a CDC stream by wrapping a transactional stream with batch storage.
    ///
    /// # Arguments
    /// - `inner`: The transactional stream to wrap
    /// - `batch_store`: BatchStore for batch content persistence
    ///
    /// # Example
    /// ```ignore
    /// // Production usage:
    /// let stream = CdcStream::create(builder.await?, batch_store)?;
    ///
    /// // Test usage:
    /// let mock_stream = create_mock_transactional_stream();
    /// let stream = CdcStream::create(mock_stream, batch_store)?;
    /// ```
    pub(crate) fn create(
        inner: TransactionalStream,
        batch_store: Box<dyn BatchStore>,
    ) -> Result<Self, Error> {
        let schema = inner.schema();
        let mut inner = inner;
        let store = Arc::new(Mutex::new(batch_store));
        let stream = try_stream! {
            while let Some(result) = inner.next().await {
                let (event, commit) = result?;

                match event {
                    TransactionEvent::Data { id, batch, .. } => {
                        // Storing batches before emitting guarantees they are available for
                        // future Delete events
                        let mut store = store.lock().await;
                        store.append(batch.clone(), id).await?;
                        // Emit single Insert event with commit handle
                        yield (CdcEvent::Insert { id, batch }, commit);
                    }

                    TransactionEvent::Undo { id, invalidate, .. } => {
                        // Get all batch IDs in the invalidation range (lightweight operation)
                        let ids = {
                            let store = store.lock().await;
                            store.seek(invalidate.clone()).await?
                        };

                        if !ids.is_empty() {
                            // Create an iterator that will lazily load batches
                            let batches = DeleteBatchIterator::new(store.clone(), ids);
                            // Consumer processes all batches via the iterator, then commits
                            yield (CdcEvent::Delete { id, batches }, commit);
                        }
                    }

                    TransactionEvent::Watermark { prune, .. } => {
                        // Handle batch pruning when watermark moves retention window
                        if let Some(cutoff) = prune {
                            // Best-effort pruning (non-fatal if it fails)
                            let result = {
                                let mut store = store.lock().await;
                                store.prune(cutoff).await
                            };

                            if let Err(e) = result {
                                tracing::warn!("Batch pruning failed (will retry on next watermark): {}", e);
                            }
                        }
                    }
                }
            }
        }
        .boxed();

        Ok(Self {
            inner: stream,
            schema,
        })
    }
}

impl HasSchema for CdcStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for CdcStream {
    type Item = Result<(CdcEvent, CommitHandle), Error>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }
}

/// Builder for creating a CDC stream.
///
/// Created by calling `.cdc()` on a `StreamBuilder`.
pub struct CdcStreamBuilder {
    client: AmpClient,
    sql: String,
    state_store: Box<dyn StateStore>,
    batch_store: Box<dyn BatchStore>,
    retention: BlockNum,
}

impl CdcStreamBuilder {
    /// Create a new CDC stream builder.
    ///
    /// # Arguments
    /// - `client`: Amp client
    /// - `sql`: SQL query string
    /// - `state_store`: StateStore for watermark persistence
    /// - `batch_store`: BatchStore for batch content persistence
    /// - `retention`: Retention window in blocks
    pub(crate) fn new(
        client: AmpClient,
        sql: impl Into<String>,
        state_store: Box<dyn StateStore>,
        batch_store: Box<dyn BatchStore>,
        retention: BlockNum,
    ) -> Self {
        Self {
            client,
            sql: sql.into(),
            state_store,
            batch_store,
            retention,
        }
    }
}

impl IntoFuture for CdcStreamBuilder {
    type Output = Result<CdcStream, Error>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            // Create a transactional stream builder with the state store
            let transactional_builder = TransactionalStreamBuilder::new(
                self.client,
                self.sql,
                self.state_store,
                self.retention,
            );

            // Create the CDC stream by wrapping the transactional stream with batch storage
            CdcStream::create(transactional_builder.await?, self.batch_store)
        })
    }
}
