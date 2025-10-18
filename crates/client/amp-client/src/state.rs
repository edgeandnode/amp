//! State management for durable streaming

use std::{
    collections::{BTreeMap, HashMap},
    fmt::Debug,
    future::{Future, IntoFuture},
    ops::RangeInclusive,
    pin::Pin,
    sync::Arc,
};

use common::{arrow::array::RecordBatch, metadata::segments::BlockRange};
use tokio::sync::Mutex;

use crate::{
    error::Error,
    store::StateStore,
    stream::{Event, WatermarkCheckpoint},
};

/// Actions that can be executed on the state machine.
///
/// Each action represents a high-level streaming operation that gets translated
/// into state transitions and event emissions by the state machine.
#[derive(Debug)]
pub enum Action {
    /// Process a data batch
    Data {
        batch: RecordBatch,
        ranges: Vec<BlockRange>,
    },
    /// Process a watermark checkpoint
    Watermark { ranges: Vec<BlockRange> },
    /// Process a reorg (includes previous ranges for invalidation computation)
    Reorg {
        previous: Vec<BlockRange>,
        incoming: Vec<BlockRange>,
    },
    /// Process a rewind on reconnection
    Rewind,
}

/// A pending commit that hasn't been persisted to the store yet.
///
/// When `execute()` is called, it updates in-memory state immediately (needed for
/// subsequent actions) and creates a `Commit` to track what needs to be
/// persisted. The actual persistence happens when `CommitHandle::commit()` is called.
#[derive(Debug, Clone)]
enum Commit {
    /// Data batch pending commit
    Data { ranges: Vec<BlockRange> },
    /// Watermark checkpoint pending commit
    Watermark {
        ranges: Vec<BlockRange>,
        cutoff: Option<u64>,
    },
    /// Reorg checkpoint pending commit
    Reorg {
        ranges: Vec<BlockRange>,
        invalidation: Option<RangeInclusive<u64>>,
    },
    /// Rewind checkpoint pending commit
    Rewind {
        ranges: Vec<BlockRange>,
        invalidation: Option<RangeInclusive<u64>>,
    },
}

/// Compressed representation of multiple pending commits.
///
/// When committing multiple transactions at once, we compress them into a single
/// atomic state update to avoid redundant store operations and ensure atomicity.
#[derive(Debug, Default)]
pub struct CompressedCommit {
    /// Data batches to add (id -> ranges)
    pub insert: BTreeMap<u64, Vec<BlockRange>>,
    /// Invalidation ranges (from reorgs/rewinds)
    pub delete: Vec<RangeInclusive<u64>>,
    /// Final watermark checkpoint
    pub watermark: Option<WatermarkCheckpoint>,
}

impl CompressedCommit {
    /// Compress a sequence of pending commits into a single atomic update.
    fn new(commits: Vec<(u64, Commit)>) -> Self {
        let mut compressed = Self::default();
        let mut prune = u64::MIN;

        for (id, commit) in commits {
            match commit {
                Commit::Data { ranges } => {
                    compressed.insert.insert(id, ranges);
                }
                Commit::Watermark { ranges, cutoff } => {
                    // Update watermark (last one wins)
                    compressed.watermark = Some(WatermarkCheckpoint::new(ranges, id));
                    if let Some(c) = cutoff {
                        prune = prune.max(c);
                    }
                }
                Commit::Reorg {
                    ranges,
                    invalidation,
                } => {
                    // Update watermark (last one wins)
                    compressed.watermark = Some(WatermarkCheckpoint::new(ranges, id));
                    // Add invalidation range
                    if let Some(range) = invalidation {
                        compressed.delete.push(range);
                    }
                }
                Commit::Rewind {
                    ranges,
                    invalidation,
                } => {
                    // Update watermark (last one wins)
                    compressed.watermark = Some(WatermarkCheckpoint::new(ranges, id));
                    // Add invalidation range
                    if let Some(range) = invalidation {
                        compressed.delete.push(range);
                    }
                }
            }
        }

        // Add pruning range if any
        if prune > u64::MIN {
            compressed.delete.push(0..=prune - 1);
        }

        // Merge delete ranges
        compressed.delete = merge_ranges(compressed.delete);
        compressed
            .insert
            .retain(|id, _| !compressed.delete.iter().any(|range| range.contains(id)));
        compressed
    }

    fn is_empty(&self) -> bool {
        self.insert.is_empty() && self.delete.is_empty() && self.watermark.is_none()
    }
}

/// Merge overlapping or adjacent ranges into minimal set.
///
/// Example: [0..=2, 1..=5, 7..=9] -> [0..=5, 7..=9]
fn merge_ranges(mut ranges: Vec<RangeInclusive<u64>>) -> Vec<RangeInclusive<u64>> {
    if ranges.is_empty() {
        return ranges;
    }

    // Sort by start position
    ranges.sort_by_key(|r| *r.start());

    let mut merged = Vec::new();
    let mut current = ranges[0].clone();

    for range in ranges.into_iter().skip(1) {
        if *range.start() <= current.end() + 1 {
            // Overlapping or adjacent - extend current
            current = *current.start()..=(*current.end().max(range.end()));
        } else {
            // Gap - push current and start new range
            merged.push(current);
            current = range;
        }
    }

    merged.push(current);
    merged
}

/// Handle for committing state changes.
///
/// Commits are idempotent - calling `commit()` multiple times is safe.
/// Uncommitted transactions are handled by rewind on restart.
pub struct CommitHandle {
    manager: Arc<Mutex<StateManager>>,
    id: u64,
}

impl CommitHandle {
    /// Create a new commit handle.
    pub(crate) fn new(manager: Arc<Mutex<StateManager>>, id: u64) -> Self {
        Self { manager, id }
    }

    /// Commit all pending changes up to and including this transaction id.
    ///
    /// Safe to call multiple times - subsequent calls are no-ops if already committed.
    /// Compresses multiple pending commits into a single atomic state update.
    pub async fn commit(&self) -> Result<(), Error> {
        let mut mgr = self.manager.lock().await;

        // Collect all pending commits up to this id
        let pending: Vec<_> = mgr
            .uncommitted
            .range(..=self.id)
            .map(|(id, pc)| (*id, pc.clone()))
            .collect();

        // Early return if nothing to commit
        if pending.is_empty() {
            return Ok(());
        }

        // Compress pending commits into single atomic update
        let compressed = CompressedCommit::new(pending.clone());
        if !compressed.is_empty() {
            mgr.store.commit(compressed).await?;
        }

        // Remove committed transactions from uncommitted map
        for (id, _) in pending {
            mgr.uncommitted.remove(&id);
        }

        Ok(())
    }
}

impl std::fmt::Debug for CommitHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
/// The manager maintains an in-memory copy of the stream state that serves as the source of truth.
/// State is loaded once from the persistent store on creation (rehydration), and all subsequent
/// operations update the in-memory state and persist changes to the store.
///
/// This design ensures:
/// - Efficient access to state (no repeated reads from persistent store)
/// - Single source of truth (in-memory state)
/// - Durability (all changes written to persistent store)
pub(crate) struct StateManager {
    /// The state store to persist the state to for crash recovery
    store: Box<dyn StateStore>,
    /// Last committed watermark checkpoint (ranges + sequence)
    watermark: Option<WatermarkCheckpoint>,
    /// Consumed batches buffer (for computing invalidated sequences on reorg)
    buffer: BTreeMap<u64, Vec<BlockRange>>,
    /// Next sequence number to assign to events (monotonically increasing, never resets)
    next: u64,
    /// Retention window in blocks (for pruning consumed_batches buffer)
    retention: u64,
    /// Pending commits awaiting persistence
    uncommitted: BTreeMap<u64, Commit>,
}

impl Debug for StateManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StateManager")
            .field("next", &self.next)
            .field("watermark", &self.watermark)
            .field("retention", &self.retention)
            .field("buffer", &self.buffer.len())
            .field("uncommitted", &self.uncommitted.len())
            .finish_non_exhaustive()
    }
}

impl StateManager {
    /// Create a new state manager with the given store and retention window.
    ///
    /// Loads initial state from the persistent store (rehydration) and maintains
    /// it in-memory for efficient access. After initialization, state is only
    /// read from memory, not from the persistent store.
    ///
    /// # Arguments
    /// * `store` - Persistent state store implementation
    /// * `retention` - How many blocks to keep in buffer
    pub(crate) fn new(store: Box<dyn StateStore>, retention: u64) -> Self {
        let state = futures::executor::block_on(async {
            store.load().await.expect("failed to load initial state")
        });

        Self {
            store,
            retention,
            watermark: state.watermark,
            buffer: state.buffer,
            next: state.next,
            uncommitted: BTreeMap::new(),
        }
    }

    /// Get watermark checkpoint (from in-memory state).
    pub(crate) fn watermark(&self) -> &Option<WatermarkCheckpoint> {
        &self.watermark
    }

    /// Get the next sequence number to be assigned.
    pub(crate) fn peek(&self) -> u64 {
        self.next
    }

    /// Compute which sequences are invalidated by comparing prev ranges to incoming ranges.
    ///
    /// When a reorg is detected, we need to invalidate all batches that covered blocks
    /// from the reorg point onwards (on the affected networks).
    ///
    /// Returns a range of sequences to invalidate (min..=max), or None if no sequences affected.
    pub(crate) fn invalidation_range(
        &self,
        previous: &[BlockRange],
        incoming: &[BlockRange],
    ) -> Option<RangeInclusive<u64>> {
        let mut min = u64::MAX;
        let mut max = u64::MIN;

        // Traverse backwards from newest to oldest (early exit when we find unaffected batch)
        for (id, ranges) in self.buffer.iter().rev() {
            // Check if this batch is affected by the reorg
            if ranges.iter().any(|range| {
                // Find the corresponding ranges in previous and incoming
                let previous = previous
                    .iter()
                    .find(|current| current.network == range.network);
                let incoming = incoming
                    .iter()
                    .find(|current| current.network == range.network);

                if let (Some(previous), Some(incoming)) = (previous, incoming) {
                    // Reorg detected if ranges differ and overlap
                    previous != incoming
                        && incoming.start() <= previous.end()
                        && range.end() >= incoming.start()
                } else {
                    false
                }
            }) {
                // This batch is affected by the reorg
                min = min.min(*id);
                max = max.max(*id);
            } else {
                // Found first unaffected batch, all older batches are also unaffected
                break;
            }
        }

        let range = min..=max;
        if range.is_empty() { None } else { Some(range) }
    }

    /// Compute the cutoff id based on the configured retention window.
    ///
    /// Returns the lowest id that should be retained in the buffer.
    ///
    /// All entries below this id can be safely pruned.
    pub(crate) fn cutoff(&self, ranges: &[BlockRange]) -> Option<u64> {
        // If buffer is empty or no watermark checkpoint, nothing to prune
        if self.buffer.is_empty() {
            return None;
        }

        // Build end block map
        let end: HashMap<String, u64> = ranges
            .iter()
            .map(|range| (range.network.clone(), *range.numbers.end()))
            .collect();

        // Find first batch that should be retained (walk from oldest to newest)
        for (id, ranges) in &self.buffer {
            if ranges.iter().any(|range| {
                // All messages in a stream are expected to carry the same networks in their block ranges.
                let end = end
                    .get(&range.network)
                    .expect("watermark not found for network");

                // The batch is within the retention window if the end block is greater than the
                // calculated cutoff for this network.
                let cutoff = end.saturating_sub(self.retention);
                *range.numbers.end() >= cutoff
            }) {
                // This is the first batch to keep, prune everything before it
                return Some(*id);
            }
        }

        // All batches are outside retention window, prune everything
        Some(self.next)
    }
}

/// Extension trait providing ergonomic method syntax for state machine execution.
///
/// This trait is automatically implemented for `Arc<Mutex<StateManager>>`,
/// enabling clean method call syntax like `state.execute(action)`.
#[async_trait::async_trait]
pub trait StateManagerHandle {
    /// Execute an action on the state machine.
    ///
    /// This is the core state machine method that:
    /// 1. Blocks until previous commit is done to prevent out of order execution
    /// 2. Pre-allocates incremental identifiers to guarantee uniqueness
    /// 3. Updates in-memory state optimistically
    async fn execute(&self, action: Action) -> Result<(Event, CommitHandle), Error>;
}

#[async_trait::async_trait]
impl StateManagerHandle for Arc<Mutex<StateManager>> {
    async fn execute(&self, action: Action) -> Result<(Event, CommitHandle), Error> {
        // Acquire lock and execute action
        let mut mgr = self.lock().await;

        // Pre-allocate monotonically increasing identifier
        let id: u64 = mgr.next;
        mgr.next += 1;
        let next = mgr.next;
        mgr.store.advance(next).await?;

        // Execute action, update in-memory state, and create pending commit
        let (event, uncommitted) = match action {
            Action::Data { batch, ranges } => {
                // Update in-memory buffer immediately (needed for subsequent actions)
                mgr.buffer.insert(id, ranges.clone());

                // Pending commit for later persistence
                let commit = Commit::Data {
                    ranges: ranges.clone(),
                };

                // Event to be emitted
                let event = Event::Data { id, batch, ranges };

                (event, commit)
            }

            Action::Watermark { ranges } => {
                // Compute cutoff and prune buffer immediately
                let cutoff = mgr.cutoff(&ranges);
                if let Some(c) = cutoff {
                    mgr.buffer = mgr.buffer.split_off(&c);
                }

                // Update watermark immediately
                let checkpoint = WatermarkCheckpoint::new(ranges.clone(), id);
                mgr.watermark = Some(checkpoint);

                // Pending commit for later persistence
                let commit = Commit::Watermark {
                    ranges: ranges.clone(),
                    cutoff,
                };

                // Event to be emitted
                let event = Event::Watermark { id, ranges, cutoff };

                (event, commit)
            }

            Action::Reorg { previous, incoming } => {
                // Compute invalidation
                let invalidation = mgr.invalidation_range(&previous, &incoming);

                // Remove invalidated batches from buffer immediately
                if let Some(ref range) = invalidation {
                    mgr.buffer.retain(|id, _| !range.contains(id));
                }

                // Update watermark immediately
                let checkpoint = WatermarkCheckpoint::new(incoming.clone(), id);
                mgr.watermark = Some(checkpoint);

                // Pending commit for later persistence
                let commit = Commit::Reorg {
                    ranges: incoming.clone(),
                    invalidation: invalidation.clone(),
                };

                // Event to be emitted
                let event = Event::Reorg {
                    id,
                    ranges: incoming,
                    invalidation,
                };

                (event, commit)
            }

            Action::Rewind => {
                // Compute invalidation from gap
                let checkpoint = mgr.watermark.as_ref().map(|w| w.checkpoint).unwrap_or(0);
                let invalidation = if mgr.next > checkpoint + 1 {
                    Some((checkpoint + 1)..=(mgr.next - 1))
                } else {
                    None
                };

                // Remove invalidated batches from buffer immediately
                if let Some(ref range) = invalidation {
                    mgr.buffer.retain(|id, _| !range.contains(id));
                }

                // Get watermark ranges (or empty if no watermark)
                let ranges = mgr
                    .watermark
                    .as_ref()
                    .map(|w| w.ranges.clone())
                    .unwrap_or_default();

                // Update watermark immediately
                let checkpoint = WatermarkCheckpoint::new(ranges.clone(), id);
                mgr.watermark = Some(checkpoint);

                // Pending commit for later persistence
                let commit = Commit::Rewind {
                    ranges: ranges.clone(),
                    invalidation: invalidation.clone(),
                };

                // Event to be emitted
                let event = Event::Rewind {
                    id,
                    ranges,
                    invalidation,
                };

                (event, commit)
            }
        };

        // Record pending commit for later persistence
        mgr.uncommitted.insert(id, uncommitted);

        let commit = CommitHandle::new(self.clone(), id);
        Ok((event, commit))
    }
}
