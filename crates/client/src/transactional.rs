//! Transactional stream implementation with state management
//! for durable streaming with exactly-once semantics.

use std::{
    collections::{HashMap, VecDeque},
    fmt::{Debug, Formatter},
    future::{Future, IntoFuture},
    ops::RangeInclusive,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use alloy::primitives::BlockNumber;
use arrow::{array::RecordBatch, datatypes::SchemaRef};
use async_stream::try_stream;
use futures::{Stream as FuturesStream, StreamExt, stream::BoxStream};
use tokio::sync::Mutex;

use crate::{
    BlockRange, ResumeWatermark,
    client::{AmpClient, HasSchema, InvalidationRange, ProtocolMessage, ProtocolStream},
    error::{Error, ReorgError},
    store::StateStore,
};

pub type TransactionId = u64;

#[derive(Debug, Clone)]
pub enum Cause {
    Reorg(Vec<InvalidationRange>),
    Rewind,
}

/// Transaction events emitted by the transactional stream.
#[derive(Debug, Clone)]
pub enum TransactionEvent {
    /// New data to process.
    Data {
        /// Transaction id of the data event
        id: TransactionId,
        /// Data batch
        batch: RecordBatch,
        /// Block ranges covered by the data batch
        ranges: Vec<BlockRange>,
    },

    /// Consumer must delete all data with the invalidated ids.
    Undo {
        /// Transaction id of the undo event
        id: TransactionId,
        // Cause of the undo event (rewind after restart or chain reorg)
        cause: Cause,
        /// Range of ids to invalidate (inclusive)
        invalidate: RangeInclusive<TransactionId>,
    },

    /// Watermark with transaction id.
    Watermark {
        /// Transaction id of the watermark event
        id: TransactionId,
        /// Block ranges confirmed complete at this watermark
        ranges: Vec<BlockRange>,
        /// Last transaction id pruned at this watermark, or None if no pruning occurred.
        prune: Option<TransactionId>,
    },
}

/// Actions that can be executed on the state machine.
///
/// Each action represents a high-level streaming operation that gets translated
/// into state transitions and event emissions by the state machine.
#[derive(Debug)]
pub(crate) enum Action {
    /// Process a reorg (includes previous ranges for invalidation computation)
    Message(ProtocolMessage),
    /// Process a rewind on reconnection
    Rewind,
}

/// A pending commit that hasn't been persisted to the store yet.
///
/// When `execute()` is called, it updates in-memory state immediately (needed for
/// subsequent actions) and creates a `Commit` to track what needs to be
/// persisted. The actual persistence happens when `CommitHandle::commit()` is called.
#[derive(Clone)]
struct PendingCommit {
    ranges: Vec<BlockRange>,
    /// Pre-computed pruning point based on buffer state at watermark creation time
    prune: Option<TransactionId>,
}

/// Compressed representation of multiple pending commits.
///
/// When committing multiple pending commits at once, we compress them into a single
/// atomic state update to avoid redundant store operations and ensure atomicity.
#[derive(Debug)]
pub struct Commit {
    /// Watermarks to add (oldest to newest)
    pub insert: Vec<(TransactionId, Vec<BlockRange>)>,
    /// Last transaction id to prune (inclusive).
    pub prune: Option<TransactionId>,
}

impl Commit {
    /// Compress a list of pending commits into a single atomic update.
    ///
    /// Takes the maximum pruning point from all pending commits (since pruning is cumulative).
    fn new(events: Vec<(TransactionId, PendingCommit)>) -> Self {
        let mut compressed = Self {
            insert: Vec::new(),
            prune: None,
        };

        // Collect watermarks and find maximum prune point
        for (id, event) in events {
            compressed.insert.push((id, event.ranges.clone()));

            // Take the maximum prune point (most recent pruning)
            if let Some(event_prune) = event.prune {
                compressed.prune = Some(
                    compressed
                        .prune
                        .map(|existing| existing.max(event_prune))
                        .unwrap_or(event_prune),
                );
            }
        }

        // Remove any watermarks that were pruned
        if let Some(prune) = compressed.prune {
            compressed.insert.retain(|(id, _)| *id > prune);
        }

        compressed
    }

    fn is_empty(&self) -> bool {
        self.insert.is_empty() && self.prune.is_none()
    }
}

/// Handle for committing state changes.
///
/// Commits are idempotent. Calling `commit()` multiple times is safe.
///
/// Uncommitted transactions are handled by rewind on restart.
pub struct CommitHandle {
    actor: StateActor,
    id: TransactionId,
}

impl CommitHandle {
    /// Create a new commit handle.
    pub(crate) fn new(actor: StateActor, id: TransactionId) -> Self {
        Self { actor, id }
    }

    /// Commit all pending changes up to and including this transaction id.
    ///
    /// Safe to call multiple times. Subsequent calls are no-ops if already committed.
    ///
    /// Compresses multiple pending commits into a single atomic state update.
    pub async fn commit(&self) -> Result<(), Error> {
        self.actor.commit(self.id).await
    }
}

impl Debug for CommitHandle {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommitHandle")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

impl IntoFuture for CommitHandle {
    type Output = Result<(), Error>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move { self.commit().await })
    }
}

/// Internal manager for stream state operations.
///
/// Wraps the user-provided state store for crash recovery and resumption.
///
/// The manager maintains an in-memory copy of the stream state that serves as
/// the source of truth. State is loaded once from the persistent store on
/// creation (rehydration). All subsequent operations update the in-memory
/// state immediately but do not persist changes to the store until the commit
/// handle is invoked by the user.
pub(crate) struct StateContainer {
    /// Persistent state store
    store: Box<dyn StateStore>,
    /// Retention window in blocks
    retention: BlockNumber,
    /// Next transaction id to be assigned
    next: TransactionId,
    /// Buffer of watermarks and undo events (oldest to newest)
    buffer: VecDeque<(TransactionId, Vec<BlockRange>)>,
    /// Pending commits (oldest to newest)
    uncommitted: VecDeque<(TransactionId, PendingCommit)>,
}

impl Debug for StateContainer {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StateContainer")
            .field("next", &self.next)
            .field("retention", &self.retention)
            .field("buffer", &self.buffer.len())
            .field("uncommitted", &self.uncommitted.len())
            .finish_non_exhaustive()
    }
}

/// Find the recovery point watermark for a reorg.
///
/// Walks backwards through watermarks to find the last unaffected watermark.
/// This watermark represents the safe recovery point - everything after it
/// needs to be invalidated.
///
/// # Arguments
/// - `buffer`: Watermark buffer (oldest to newest)
/// - `invalidation`: Invalidation ranges from the reorg (network -> block range)
///
/// # Returns
/// - `Some((id, ranges))`: Last unaffected watermark (recovery point)
/// - `None`: No recovery point found (all watermarks affected or buffer empty)
fn find_recovery_point(
    buffer: &VecDeque<(TransactionId, Vec<BlockRange>)>,
    invalidation: &[InvalidationRange],
) -> Option<(TransactionId, Vec<BlockRange>)> {
    if buffer.is_empty() {
        return None;
    }

    // Build reorg points map: network -> first invalid block
    let points: HashMap<String, BlockNumber> = invalidation
        .iter()
        .map(|inv| (inv.network.clone(), *inv.numbers.start()))
        .collect();

    // Walk backwards through watermarks (newest to oldest)
    for (id, ranges) in buffer.iter().rev() {
        // Check if any network in this watermark is affected
        let affected = ranges.iter().any(|range| {
            points
                .get(&range.network)
                .map(|point| range.start() >= *point)
                .unwrap_or(false)
        });

        if !affected {
            // Found last unaffected watermark
            return Some((*id, ranges.clone()));
        }
    }

    // All watermarks are affected
    None
}

/// Compute the last transaction ID that should be pruned based on retention window.
///
/// Walks through watermarks from oldest to newest and identifies the last one outside
/// the retention window (all networks before cutoff).
///
/// # Arguments
/// - `buffer`: Watermark buffer (oldest to newest)
/// - `retention`: Retention window in blocks
///
/// # Returns
/// - `Some(end)` - Last transaction ID to prune (all IDs <= this are removed)
/// - `None` if no pruning needed
fn find_pruning_point(
    buffer: &VecDeque<(TransactionId, Vec<BlockRange>)>,
    retention: BlockNumber,
) -> Option<TransactionId> {
    // Get latest ranges from buffer
    let (_, ranges) = buffer.back()?;

    // Calculate cutoff block for each network
    let cutoffs: HashMap<String, BlockNumber> = ranges
        .iter()
        .map(|range| {
            let cutoff = range.start().saturating_sub(retention);
            (range.network.clone(), cutoff)
        })
        .collect();

    if cutoffs.is_empty() {
        return None;
    }

    let mut last = None;

    // Walk from front, checking which watermarks are outside retention (skip last watermark)
    for (id, ranges) in buffer.iter().take(buffer.len() - 1) {
        // Check if this watermark is outside the retention window
        let outside = ranges.iter().all(|range| {
            let cutoff = cutoffs.get(&range.network).unwrap();
            range.end() < *cutoff
        });

        if outside {
            last = Some(*id);
        } else {
            break; // First watermark within retention
        }
    }

    last
}

impl StateContainer {
    /// Create a new state container with the given store and retention window.
    ///
    /// Loads initial state from the persistent store (rehydration) and maintains
    /// it in-memory for efficient access. After initialization, state is only
    /// read from memory, not from the persistent store.
    ///
    /// # Arguments
    /// * `store` - Persistent state store implementation
    /// * `retention` - How many blocks to keep in buffer
    pub(crate) async fn new(
        store: Box<dyn StateStore>,
        retention: BlockNumber,
    ) -> Result<Self, Error> {
        let state = store.load().await?;

        Ok(Self {
            store,
            retention,
            buffer: state.buffer,
            next: state.next,
            uncommitted: VecDeque::new(),
        })
    }

    /// Get the last watermark (derived from buffer.back())
    pub(crate) fn watermark(&self) -> Option<(TransactionId, Vec<BlockRange>)> {
        self.buffer.back().map(|(id, ranges)| (*id, ranges.clone()))
    }

    /// Get the next transaction id to be assigned.
    pub(crate) fn peek(&self) -> TransactionId {
        self.next
    }
}

/// Actor for managing stream state.
///
/// Wraps the state container in Arc<Mutex<>> for concurrent access.
/// Provides the execute method for processing actions and generating events.
#[derive(Clone)]
pub(crate) struct StateActor {
    state: Arc<Mutex<StateContainer>>,
}

impl StateActor {
    /// Create a new state actor with the given store and retention window.
    pub(crate) async fn new(
        store: Box<dyn StateStore>,
        retention: BlockNumber,
    ) -> Result<Self, Error> {
        let state = StateContainer::new(store, retention).await?;

        Ok(Self {
            state: Arc::new(Mutex::new(state)),
        })
    }

    /// Get the last watermark (transaction ID and ranges).
    pub(crate) async fn watermark(&self) -> Option<(TransactionId, Vec<BlockRange>)> {
        let state = self.state.lock().await;
        state.watermark()
    }

    /// Get the next transaction id to be assigned.
    pub(crate) async fn peek(&self) -> TransactionId {
        let state = self.state.lock().await;
        state.peek()
    }

    /// Execute an action on the state actor and return the corresponding event and commit handle.
    ///
    /// The commit handle can be awaited to commit the state changes to the store.
    pub(crate) async fn execute(
        &self,
        action: Action,
    ) -> Result<(TransactionEvent, CommitHandle), Error> {
        // Acquire lock and execute action
        let mut mgr = self.state.lock().await;

        // Pre-allocate monotonically increasing identifier
        let id: TransactionId = mgr.next;
        mgr.next += 1;
        let next = mgr.next;
        mgr.store.advance(next).await?;

        // Execute action, update in-memory state, and derive event
        let event = match action {
            Action::Rewind => {
                // Compute invalidation range based on buffer state
                let invalidate = if mgr.buffer.is_empty() {
                    // Empty buffer (early crash before any watermark): invalidate from the beginning
                    TransactionId::MIN..=(id.saturating_sub(1))
                } else {
                    // Normal rewind: invalidate after last watermark
                    let last = mgr.buffer.back().map(|(id, _)| *id).unwrap();
                    (last + 1)..=(id.saturating_sub(1))
                };

                TransactionEvent::Undo {
                    id,
                    cause: Cause::Rewind,
                    invalidate,
                }
            }

            Action::Message(message) => {
                match message {
                    ProtocolMessage::Data { batch, ranges } => {
                        TransactionEvent::Data { id, batch, ranges }
                    }

                    ProtocolMessage::Watermark { ranges } => {
                        // Add watermark to buffer
                        mgr.buffer.push_back((id, ranges.clone()));

                        // Compute pruning point based on current buffer state
                        let prune = find_pruning_point(&mgr.buffer, mgr.retention);

                        // Record pending commit with pre-computed pruning
                        mgr.uncommitted.push_back((
                            id,
                            PendingCommit {
                                ranges: ranges.clone(),
                                prune,
                            },
                        ));

                        TransactionEvent::Watermark { id, ranges, prune }
                    }

                    ProtocolMessage::Reorg { invalidation, .. } => {
                        // Find recovery point (last unaffected watermark)
                        let recovery = find_recovery_point(&mgr.buffer, &invalidation);
                        let invalidate = match recovery {
                            None => {
                                if mgr.buffer.is_empty() {
                                    // If the buffer is empty, we can invalidate everything up to before the
                                    // current event. This is a hypothetical scenario that should never happen
                                    // in practice.
                                    Ok(TransactionId::MIN..=id.saturating_sub(1))
                                } else {
                                    // No recovery point with a non-empty buffer means all buffered watermarks
                                    // are affected by the reorg. This is not recoverable.
                                    Err(Error::Reorg(ReorgError::Unrecoverable))
                                }
                            }
                            Some((tx, ref ranges)) => {
                                // Check each range for partial reorg. A partial reorg is a reorg for which the
                                // recovery point does not line up perfectly with the start of the invalidation
                                // range. This is not recoverable and requires a reconnect using the recovery
                                // point as a starting point. If we were to ignore this, we would end up with
                                // a data gap because we are telling the consumer to invalidate data that will
                                // afterwards not be replayed.
                                //
                                // Note: Only check networks that are in the invalidation list. Other networks
                                // at the recovery point are unaffected and should be ignored.
                                for range in ranges {
                                    let inv =
                                        invalidation.iter().find(|i| i.network == range.network);
                                    if let Some(inv) = inv {
                                        let point = inv.numbers.start();
                                        if range.start() < *point && *point <= range.end() {
                                            return Err(Error::Reorg(ReorgError::Partial));
                                        }
                                    }
                                }

                                Ok(tx + 1..=id.saturating_sub(1))
                            }
                        }?;

                        // Immediately truncate buffer (both in-memory and persisted) to recovery point
                        let tx = recovery.map(|(tx, _)| tx).unwrap_or(TransactionId::MIN);
                        let truncate = tx + 1; // One past the recovery point
                        mgr.buffer.retain(|(id, _)| *id < truncate);
                        mgr.store.truncate(truncate).await?;

                        // Clear uncommitted watermarks after recovery point
                        mgr.uncommitted.retain(|(id, _)| *id < truncate);

                        TransactionEvent::Undo {
                            id,
                            cause: Cause::Reorg(invalidation),
                            invalidate,
                        }
                    }
                }
            }
        };

        Ok((event, CommitHandle::new(self.clone(), id)))
    }

    /// Commit all pending changes up to and including this transaction id.
    pub(crate) async fn commit(&self, id: TransactionId) -> Result<(), Error> {
        let mut mgr = self.state.lock().await;

        // Find position where IDs become > id (all before this are <= id)
        let pos = mgr
            .uncommitted
            .iter()
            .position(|(current, _)| *current > id)
            .unwrap_or(mgr.uncommitted.len());

        if pos == 0 {
            return Ok(()); // Nothing to commit
        }

        // Collect commits [0..pos]
        let pending: Vec<(TransactionId, PendingCommit)> = mgr
            .uncommitted
            .iter()
            .take(pos)
            .map(|(id, pending)| (*id, pending.clone()))
            .collect();

        // Compress and persist (uses pre-computed pruning from pending commits)
        let compressed = Commit::new(pending);
        if !compressed.is_empty() {
            // Apply pruning to in-memory buffer
            if let Some(prune) = compressed.prune {
                mgr.buffer.retain(|(id, _)| *id > prune);
            }

            mgr.store.commit(compressed).await?;
        }

        // Remove committed from front
        mgr.uncommitted.drain(0..pos);

        Ok(())
    }
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
/// let stream = client.stream("SELECT * FROM eth.logs").await?;
///
/// // With configuration
/// let stream = client.stream("SELECT * FROM eth.logs")
///     .transactional(store, 128)  // Use persistent state store
///     .await?;
/// ```
pub struct TransactionalStreamBuilder {
    client: AmpClient,
    sql: String,
    store: Box<dyn StateStore>,
    retention: BlockNumber,
}

impl TransactionalStreamBuilder {
    /// Create a new TransactionalStreamBuilder.
    ///
    /// # Arguments
    /// - `client`: Amp client
    /// - `sql`: SQL query string
    /// - `store`: State store for persistence
    /// - `retention`: Retention window in blocks
    pub(crate) fn new(
        client: AmpClient,
        sql: impl Into<String>,
        store: Box<dyn StateStore>,
        retention: BlockNumber,
    ) -> Self {
        Self {
            client,
            sql: sql.into(),
            store,
            retention,
        }
    }
}

impl IntoFuture for TransactionalStreamBuilder {
    type Output = Result<TransactionalStream, Error>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send>>;

    fn into_future(mut self) -> Self::IntoFuture {
        Box::pin(async move {
            TransactionalStream::create(
                self.store,
                self.retention,
                |resume: Option<ResumeWatermark>| async move {
                    self.client.request(&self.sql, resume.as_ref(), true).await
                },
            )
            .await
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
/// let mut stream = client.stream("SELECT * FROM eth.logs")
///     .transactional(LmdbStore::new("state.db"), 128)
///     .await?;
///
/// while let Some(result) = stream.next().await {
///     let (event, commit) = result?;
///     match event {
///         TransactionEvent::Data { batch, id, .. } => {
///             save(batch, id).await?;
///             commit.await?;
///         }
///         TransactionEvent::Undo { invalidate, .. } => {
///             rollback(invalidate).await?;
///             commit.await?;
///         }
///         _ => {}
///     }
/// }
/// ```
///
/// ## High-Level: Auto-Commit
/// ```rust,ignore
/// client.stream("SELECT * FROM eth.logs")
///     .transactional(LmdbStore::new("state.db"), 128)
///     .await?
///     .for_each(|event| async move {
///         match event {
///             TransactionEvent::Data { batch, id, .. } => {
///                 save(batch, id).await?;
///             }
///             TransactionEvent::Undo { invalidate, .. } => {
///                 rollback(invalidate).await?;
///             }
///             _ => {}
///         }
///         Ok(())
///     })
///     .await?;
/// ```
pub struct TransactionalStream {
    stream: BoxStream<'static, Result<(TransactionEvent, CommitHandle), Error>>,
    schema: SchemaRef,
}

impl TransactionalStream {
    /// Create a transactional stream from a generic stream-producing function.
    ///
    /// This is the reusable constructor that handles:
    /// - StateActor setup with provided store and retention
    /// - Watermark and previous ranges extraction
    /// - Rewind detection on startup
    /// - Protocol stream wrapping (reorg detection)
    /// - Final transactional stream creation
    ///
    /// # Arguments
    /// - `store`: State store for persistence
    /// - `retention`: Retention window in blocks
    /// - `connect`: Async function that produces a raw stream, given an optional resume watermark
    pub(crate) async fn create<F, Fut>(
        store: Box<dyn StateStore>,
        retention: BlockNumber,
        connect: F,
    ) -> Result<Self, Error>
    where
        F: FnOnce(Option<ResumeWatermark>) -> Fut,
        Fut: Future<Output = Result<crate::client::RawStream, Error>>,
    {
        let actor = StateActor::new(store, retention).await?;

        let watermark = actor.watermark().await; // Derived from buffer.back()
        let next = actor.peek().await;

        // Unified rewind detection (handles both empty buffer and normal rewind)
        let rewind = match &watermark {
            Some((id, _)) => next > id + 1,
            None => next > 0,
        };

        let previous = watermark
            .as_ref()
            .map(|(_, ranges)| ranges.clone())
            .unwrap_or_default();
        let resume = watermark.map(|(_, ranges)| ResumeWatermark::from_ranges(&ranges));

        let raw = connect(resume).await?;
        let schema = raw.schema();
        let mut protocol = ProtocolStream::new(raw.boxed(), previous, schema.clone());

        let stream = try_stream! {
            // Emit rewind if needed (StateManager determines invalidation range)
            if rewind {
                yield actor.execute(Action::Rewind).await?;
            }

            // Process protocol messages (already validated by ProtocolStream)
            while let Some(message) = protocol.next().await {
                yield actor.execute(Action::Message(message?)).await?;
            }
        }
        .boxed();

        Ok(Self { stream, schema })
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
    ///         TransactionEvent::Data { batch, id, .. } => {
    ///             save(batch, id).await?;
    ///         }
    ///         TransactionEvent::Undo { invalidate, .. } => {
    ///             rollback(invalidate).await?;
    ///         }
    ///         _ => {}
    ///     }
    ///     Ok(())
    /// }).await?;
    /// ```
    pub async fn for_each<F, Fut>(mut self, mut f: F) -> Result<(), Error>
    where
        F: FnMut(TransactionEvent) -> Fut,
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

impl HasSchema for TransactionalStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl FuturesStream for TransactionalStream {
    type Item = Result<(TransactionEvent, CommitHandle), Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.stream).poll_next(cx)
    }
}
