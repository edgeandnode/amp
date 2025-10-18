# Amp Client Streaming Guide

This document explains how to use the amp-client streaming API and documents the design decisions made.

## Table of Contents
1. [Quick Start](#quick-start)
2. [Core Concepts](#core-concepts)
3. [Design Decisions](#design-decisions)
4. [Event Handling Guide](#event-handling-guide)
5. [State Store Implementation](#state-store-implementation)
6. [Guarantees & Implications](#guarantees--implications)

---

## Quick Start

### Basic Streaming Query

```rust
use amp_client::{AmpClient, Event};
use futures::StreamExt;

let client = AmpClient::from_endpoint("http://localhost:1602").await?;

// Create a durable stream with automatic crash recovery
let mut stream = client
    .stream("SELECT * FROM eth.logs SETTINGS stream = true")
    .with_state_store(my_persistent_store)
    .await?;

while let Some(result) = stream.next().await {
    let (event, commit) = result?;

    match event {
        Event::Data { id, batch, .. } => {
            // Save data with its sequence ID
            save_to_database(id, batch).await?;
            commit.await?;  // Must commit after successful save
        }

        Event::Watermark { id, ranges, cutoff, .. } => {
            // Save checkpoint for resumption
            save_checkpoint(id, ranges).await?;

            // Optionally prune old data
            if let Some(cutoff) = cutoff {
                delete_data_before(cutoff).await?;
            }
            commit.await?;
        }

        Event::Reorg { invalidation, .. } => {
            // Blockchain reorganized - delete affected data
            if let Some(range) = invalidation {
                delete_sequences(range).await?;
            }
            commit.await?;
        }

        Event::Rewind { invalidation, .. } => {
            // Restart after crash - delete uncommitted data
            if let Some(range) = invalidation {
                delete_sequences(range).await?;
            }
            commit.await?;
        }
    }
}
```

### The Golden Rule

**Always commit after successfully processing an event.** If you don't commit, the stream will block forever.

---

## Core Concepts

### Sequence IDs

Every data batch and watermark gets a unique **sequence ID** (a monotonically increasing `u64`). These IDs:

- **Never reset**, even after restart
- **Never decrease**, ensuring temporal ordering
- **Uniquely identify** batches in your database
- Enable **precise invalidation** during reorgs and restarts

**Why this matters:** When a reorg happens, we tell you exactly which sequence IDs to delete. When you restart after a crash, we tell you which uncommitted IDs to roll back.

### Watermarks

A **watermark** is a checkpoint that says "all data up to this point is complete and durable." It contains:

- Block ranges (what blocks are covered)
- A sequence ID (highest committed ID at this point)

**Why this matters:** If your process crashes, you resume from the last watermark. Everything between the watermark and where you crashed is replayed or invalidated via a `Rewind` event.

### Commit Handles

Every event comes with a **commit handle** that you can `.await`. This handle:

- **Batches multiple pending commits** into a single compressed update
- **Atomically persists** all changes to your StateStore
- **Merges overlapping cleanup ranges** for efficient deletion
- **Can be called multiple times** (idempotent - subsequent calls are no-ops)

### Crash Recovery

On startup, the stream automatically:

1. Loads the last watermark from your StateStore
2. Checks if there are uncommitted batches (sequence gap)
3. Emits a `Rewind` event to delete uncommitted data
4. Resumes streaming from the watermark

**Why this matters:** You get exactly-once semantics automatically. No data duplication, no manual cleanup needed.

---

## Design Decisions

This section explains **why** the streaming architecture is designed this way.

### Decision 1: Serial Processing with Batch Commits

**What:** Only one event can be in flight at a time. The next `execute()` acquires the StateManager mutex. Commits can batch multiple pending transactions.

**Why:**
- **Prevents race conditions:** State updates happen in strict serial order
- **Guarantees monotonicity:** Sequence IDs can't get out of order
- **Simplifies reasoning:** No concurrent state mutations to worry about
- **Efficient batching:** Multiple commits compress into single store operation

**Implementation:** Uses `Arc<Mutex<StateManager>>` for serialization. `execute()` acquires lock, updates state, releases lock. `commit()` acquires lock, compresses pending commits, persists atomically.

**Trade-off:** Lower throughput (no parallelism) but vastly simpler correctness guarantees. For blockchain streaming where batches are typically large, the bottleneck is I/O not CPU anyway.

### Decision 2: Idempotent Commit Handles

**What:** Events return a `CommitHandle` that can be `.await`ed multiple times. The compiler warns via `#[must_use]` if unused.

**Why:**
- **Compile-time safety:** Can't forget to commit (compile warning)
- **Idempotent commits:** Safe to retry on transient errors
- **Batch optimization:** Commits all pending transactions ≤ ID
- **Ergonomic API:** `commit.await?` reads naturally

**Implementation:** `CommitHandle` implements `IntoFuture` so you can directly `.await` it. Uses `&self` instead of `self` for idempotence. No Drop impl needed - uncommitted transactions handled by rewind on restart.

### Decision 3: Monotonic Sequence IDs

**What:** Sequence IDs never reset, never decrease, even across restarts.

**Why:**
- **Unique identification:** Each batch has a globally unique ID
- **Reorg safety:** Can invalidate specific ranges without confusion
- **Restart safety:** Gap detection identifies uncommitted batches
- **Temporal ordering:** Higher ID = happened later, always

**Implementation:** `StateManager` pre-allocates ID, increments counter, persists to store **before** returning event. Even if event processing fails, ID is burned (never reused).

**Trade-off:** IDs grow forever (u64 is sufficient for trillions of batches). Alternative would be to reset on watermark, but this complicates reorg invalidation logic significantly.

### Decision 4: Optimistic State Updates with Compressed Commits

**What:** State is updated in-memory **first**, then multiple pending commits are compressed and persisted as a single atomic operation.

**Why:**
- **Low latency:** Event generation is fast (no I/O)
- **Efficient batching:** Multiple commits merge into one store operation
- **Atomic persistence:** All changes applied together
- **Reorg computation:** Can walk in-memory buffer to compute invalidations
- **Range merging:** Cleanup ranges are automatically merged (e.g., `[0..=5, 10..=15]`)

**Implementation:**
- `StateManager` holds `watermark`, `buffer`, and `uncommitted` in memory
- `execute()` updates in-memory state immediately and records a `PendingCommit`
- `commit()` collects all pending commits ≤ ID, compresses them into `CompressedCommit`, persists atomically
- Compression separates pruning (from cutoff) and invalidation (from reorgs), then merges ranges

**Trade-off:** If crash happens between optimistic update and commit, the in-memory state is lost. On restart, a fresh StateManager loads from the store, detects the gap, and emits a Rewind event to clean up. This is correct behavior - uncommitted work is automatically rolled back.

**Compression benefits:**
- Multiple data batches → single batch insert operation
- Multiple cleanup ranges → merged minimal set (e.g., `[0..=5, 7..=9]` instead of 15 individual IDs)
- Last watermark wins (earlier watermarks discarded)

### Decision 5: Compressed Atomic Commits

**What:** `StateStore::commit()` takes a single `CompressedCommit` containing all batches to insert, ranges to delete, and the watermark.

**Why:**
- **True atomicity:** All changes happen together in one transaction
- **Efficient batching:** Multiple operations compressed into minimal set
- **Simpler API:** One method with clear contract
- **Range merging:** Overlapping/adjacent cleanup ranges automatically merged
- **Consistency guarantee:** Impossible to have partial state updates

**Implementation:**
- `CompressedCommit::new()` compresses pending commits:
  - Separates pruning (from cutoff) and invalidation (from reorgs)
  - Merges overlapping ranges (e.g., `[0..=2, 1..=5]` → `[0..=5]`)
  - Removes batches that will be deleted
- `StateStore::commit()` applies all changes atomically (e.g., single LMDB transaction)

**Example compression:**
```rust
// Input: 5 commits
// - Data(1), Data(2), Watermark(cutoff=10), Reorg(invalidate=15..=20), Data(25)
// Compressed output:
// - insert: {25 → ranges}  // 1,2 pruned by cutoff
// - delete: [0..=9, 15..=20]  // merged ranges
// - watermark: checkpoint at reorg ID
```

### Decision 6: Rewind Event on Reconnection

**What:** On startup, if `next_sequence > watermark.sequence + 1`, emit a `Rewind` event with the gap range.

**Why:**
- **Exactly-once semantics:** Uncommitted batches are explicitly invalidated
- **Automatic recovery:** Consumer doesn't need to manually detect gaps
- **Consistent with reorgs:** Same invalidation pattern as `Reorg` events
- **Simple for consumers:** Just handle the event, no special restart logic

**Implementation:** `Stream::create()` checks for gap before entering main loop. If gap exists, emits `Rewind` with `invalidation = (watermark_seq + 1)..=(next_seq - 1)`.

**Trade-off:** First event might be a `Rewind` instead of `Data`. Consumers must handle this, but it's the same pattern as `Reorg` anyway.

---

## Event Handling Guide

### Event::Data

**When:** New blockchain data is available.

**Fields:**
- `id: u64` - Unique sequence ID for this batch
- `batch: RecordBatch` - Arrow data (logs, transactions, etc.)
- `ranges: Vec<BlockRange>` - Block ranges covered by this batch

**What to do:**
1. Save `batch` to your database, tagged with `id`
2. Commit the state change

```rust
Event::Data { id, batch, ranges } => {
    db.insert(id, batch).await?;
    commit.await?;
}
```

**Important:** You **must** store the `id` with the data. During reorgs and rewinds, you'll be asked to delete data by ID range.

### Event::Watermark

**When:** Server signals that all data up to this point is complete.

**Fields:**
- `id: u64` - Sequence ID of this watermark
- `ranges: Vec<BlockRange>` - Block ranges confirmed complete
- `cutoff: Option<u64>` - Recommended ID below which data can be pruned

**What to do:**
1. Save the watermark (for crash recovery)
2. Optionally prune old data if `cutoff` is provided
3. Commit the state change

```rust
Event::Watermark { id, ranges, cutoff } => {
    // Save checkpoint
    db.save_checkpoint(id, ranges).await?;

    // Optionally free up space
    if let Some(cutoff_id) = cutoff {
        db.delete_batches_before(cutoff_id).await?;
    }

    commit.await?;
}
```

**Important:** Pruning is optional but recommended. The `cutoff` ID is computed based on the retention window (default: 128 blocks). Data below the cutoff is outside the reorg detection window.

### Event::Reorg

**When:** Blockchain reorganization detected (blocks changed on a network).

**Fields:**
- `id: u64` - Sequence ID of this reorg checkpoint
- `ranges: Vec<BlockRange>` - New block ranges after reorg
- `invalidation: Option<RangeInclusive<u64>>` - Sequence IDs to delete

**What to do:**
1. Delete all data with sequence IDs in the invalidation range
2. Commit the state change
3. New data will arrive for the reorg'd blocks

```rust
Event::Reorg { id, ranges, invalidation } => {
    if let Some(range) = invalidation {
        db.delete_sequences(range).await?;
    }
    commit.await?;
}
```

**Important:** The invalidation range is computed by walking backward through the buffer to find all batches affected by the reorg. This is why you must store sequence IDs with your data.

### Event::Rewind

**When:** Process restarted and uncommitted batches detected.

**Fields:**
- `id: u64` - Sequence ID of rewind checkpoint (same as last watermark)
- `ranges: Vec<BlockRange>` - Block ranges to resume from
- `invalidation: Option<RangeInclusive<u64>>` - Uncommitted sequence IDs

**What to do:**
1. Delete all uncommitted data (same as Reorg)
2. Commit the state change
3. Stream will replay from the last watermark

```rust
Event::Rewind { id, ranges, invalidation } => {
    if let Some(range) = invalidation {
        db.delete_sequences(range).await?;
    }
    commit.await?;
}
```

**Important:** This event only appears once on startup if needed. Handle it the same way as `Reorg`.

### Error Handling

If processing an event fails:

```rust
while let Some(result) = stream.next().await {
    let (event, commit) = result?;

    match event {
        Event::Data { id, batch, .. } => {
            // If this fails, commit is NOT called
            save_data(id, batch).await?;

            // Only commit on success
            commit.await?;
        }
        // ... other events
    }
}
```

**What happens:** The commit handle is dropped without committing, so:
- The uncommitted transaction remains in the pending map
- Next `execute()` can still proceed (mutex-based, no blocking)
- Your process should exit and restart
- On restart, a `Rewind` event cleans up all uncommitted batches

**Retrying on transient errors:**

```rust
// Commits are idempotent - safe to retry
loop {
    match commit.commit().await {
        Ok(()) => break,
        Err(e) if e.is_retryable() => {
            warn!("Transient error, retrying: {}", e);
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        Err(e) => return Err(e),
    }
}
```

This design ensures that **partial failures don't corrupt your database**. Either a batch is fully processed and committed, or it's not processed at all.

---

## State Store Implementation

### When to Use InMemoryStateStore

**Use for:**
- Development and testing
- Prototypes
- Scenarios where crash recovery is not needed
- Short-lived processes

**Don't use for:**
- Production deployments
- Long-running processes
- Systems that must survive restarts

### When to Use Persistent Store

**Use LMDB or similar persistent store for:**
- Production deployments
- Systems that require crash recovery
- Long-running indexers
- Mission-critical data pipelines

### Implementing Custom StateStore

The `StateStore` trait has 3 methods to implement:

```rust
#[async_trait::async_trait]
pub trait StateStore: Send + Sync {
    /// Pre-allocate and persist the next transaction ID
    async fn advance(&mut self, next: u64) -> Result<(), Error>;

    /// Persist a compressed commit
    async fn commit(&mut self, commit: CompressedCommit) -> Result<(), Error>;

    /// Load state on startup
    async fn load(&self) -> Result<StreamState, Error>;
}

pub struct CompressedCommit {
    /// Data batches to insert (id → ranges)
    pub insert: BTreeMap<u64, Vec<BlockRange>>,
    /// Batch IDs to delete (merged ranges)
    pub delete: Vec<RangeInclusive<u64>>,
    /// Final watermark checkpoint
    pub watermark: Option<WatermarkCheckpoint>,
}
```

**Critical requirement:** `commit()` must be **atomic**. If you're using LMDB, do all operations in a single write transaction. If you're using SQL, wrap in a transaction.

**Why atomicity matters:** If the process crashes partway through a commit, the buffer will be inconsistent with the watermark on restart. This breaks reorg detection and rewind computation.

**Example atomic implementation:**

```rust
async fn commit(&mut self, commit: CompressedCommit) -> Result<(), Error> {
    let mut txn = self.env.write_txn()?;

    // Insert new batches
    for (id, ranges) in commit.insert {
        self.batches_db.put(&mut txn, &id, &serialize(&ranges)?)?;
    }

    // Delete batches in all ranges
    for range in commit.delete {
        for id in range {
            self.batches_db.delete(&mut txn, &id)?;
        }
    }

    // Update watermark if present
    if let Some(checkpoint) = commit.watermark {
        self.watermark_db.put(&mut txn, &b"watermark", &serialize(&checkpoint)?)?;
    }

    // Commit atomically
    txn.commit()?;
    Ok(())
}
```

### Buffer Retention

The stream maintains a buffer of recent batches (sequence ID → block ranges) for reorg detection. This buffer:

- Is pruned on watermarks based on retention window (default: 128 blocks)
- Grows if watermarks are infrequent
- Is stored in your StateStore (must fit in memory/storage)

**Tuning retention:**

```rust
let stream = client
    .stream("SELECT * FROM eth.logs SETTINGS stream = true")
    .with_retention_blocks(256)  // Detect reorgs up to 256 blocks deep
    .await?;
```

Larger retention = higher memory usage but deeper reorg detection. For Ethereum mainnet, 128 blocks (~25 minutes) is typically sufficient.

---

## Guarantees & Implications

### Exactly-Once Processing

**Guarantee:** Each data batch is processed exactly once, even across crashes and reconnections.

**How it's achieved:**
1. Sequence IDs uniquely identify each batch
2. Watermarks checkpoint progress
3. Rewind events clean up uncommitted batches on restart
4. Commit handles prevent pipeline from advancing until state is durable

**Implications:**
- You can safely restart your process anytime
- No need to manually track "last processed block"
- No duplicate data in your database
- No missing data (gaps are detected and replayed)

### Crash Recovery

**Scenario 1: Crash before commit**

```rust
Event::Data { id: 42, batch, .. } => {
    save_data(42, batch).await?;
    // CRASH HERE - commit not called
}
```

**What happens:**
- Process restarts
- State store still has watermark at ID 41
- Next sequence is 43 (ID 42 was pre-allocated)
- `Rewind` event emitted with `invalidation = 42..=42`
- You delete data with ID 42
- Stream replays batch 42 with a new ID (43)

**Scenario 2: Crash after commit**

```rust
Event::Data { id: 42, batch, .. } => {
    save_data(42, batch).await?;
    commit.await?;  // Success!
    // CRASH HERE
}
```

**What happens:**
- Process restarts
- State store has watermark at ID 41, buffer contains ID 42
- Next sequence is 43
- No rewind needed (gap is only 1, which is normal)
- Stream continues from ID 43

**Scenario 3: Crash during watermark**

```rust
Event::Watermark { id: 50, .. } => {
    save_checkpoint(50).await?;
    // CRASH HERE - commit not called
}
```

**What happens:**
- Process restarts
- State store still has old watermark (say ID 45)
- Next sequence is 51 (ID 50 was pre-allocated)
- `Rewind` event emitted with `invalidation = 46..=50`
- You delete data with IDs 46-50
- Stream replays from ID 45 with fresh IDs

### Reorg Handling

**Guarantee:** When a blockchain reorganizes, you're told exactly which data to delete.

**How it's achieved:**
1. Stream detects reorg by comparing incoming ranges to previous ranges
2. `StateManager` walks backward through buffer to find affected batches
3. `Reorg` event emitted with invalidation range
4. Consumer deletes the specified sequence IDs
5. Stream replays the reorg'd blocks with new sequence IDs

**Detection window:** You can detect reorgs within the retention window (default: 128 blocks). Deeper reorgs are not detected.

**Implications:**
- You must store sequence IDs with your data (for deletion)
- You must handle reorgs promptly (commit the cleanup)
- You can tune retention vs memory usage trade-off
- Reorgs beyond the window are not detected (acceptable for most chains)

### Performance Implications

**Fast path (common case):**
- Event generation: ~microseconds (in-memory state update)
- Commit: ~milliseconds (single write to state store)
- Throughput: limited by consumer processing + state store I/O

**Slow path (rare cases):**
- Reorg detection: ~microseconds (buffer walk is in-memory)
- Rewind on startup: ~milliseconds (state load + gap detection)

**Memory usage:**
- Buffer size: ~(retention_blocks × avg_batches_per_block × block_range_size)
- For 128 blocks × 2 batches/block × 100 bytes/range ≈ 25 KB
- Negligible compared to Arrow batch data

**Disk usage (persistent store):**
- Watermark: ~1 KB
- Buffer: same as memory usage
- Next sequence: 8 bytes
- Total: ~30 KB for typical workload

**Bottlenecks:**
- Consumer processing time (batch I/O)
- State store persistence (commit I/O)
- NOT the streaming library itself (minimal overhead)

### Safety Guarantees

**Compile-time guarantees (enforced by type system):**
- Must call `.await` on commit handles (`#[must_use]`)
- Stream is `Send + Sync` (safe to move between threads)
- Events are `Clone` but commits are single-use (consume handle)

**Runtime guarantees (enforced by implementation):**
- Pipeline depth = 1 (serial processing)
- Monotonic sequence IDs (never reset or decrease)
- Atomic store operations (consistency on crash)
- Gap detection (no silent data loss)

**Failure modes:**
- Commit I/O error → pipeline blocks → process should exit
- Network error → stream ends → consumer can retry with same state store
- Reorg beyond retention → not detected → potentially corrupt data & state → user should set sufficient retention window
- Consumer error → uncommitted → rewind on restart cleans up

**NOT guaranteed:**
- Reorg detection beyond retention window
- Protection against consumer bugs (garbage in, garbage out)
