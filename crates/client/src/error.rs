//! Error types for the Amp client

use common::BlockNum;

// ============================================================================
// Top-Level Error
// ============================================================================

/// Top-level error type for Amp client operations
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// gRPC transport error
    ///
    /// This occurs when the underlying gRPC transport fails, typically due to:
    /// - Connection failures
    /// - Network timeouts
    /// - DNS resolution failures
    #[error("gRPC transport error: {0}")]
    Transport(#[source] tonic::transport::Error),

    /// gRPC status error returned by the server
    ///
    /// This occurs when the server returns an error status code, indicating:
    /// - Invalid queries
    /// - Server-side failures
    /// - Authentication/authorization failures
    #[error("gRPC status error: {0}")]
    Status(#[source] Box<tonic::Status>),

    /// Protocol-level error
    ///
    /// This occurs when the server violates protocol requirements, such as
    /// missing required fields in responses.
    #[error("protocol error: {0}")]
    Protocol(#[source] ProtocolError),

    /// Protocol invariant validation error
    ///
    /// This occurs when the server sends data that violates streaming protocol
    /// invariants, such as duplicate networks or non-consecutive blocks.
    #[error("validation error: {0}")]
    Validation(#[source] ValidationError),

    /// Reorg handling error
    ///
    /// This occurs when a blockchain reorg cannot be handled within the current
    /// stream and requires reconnection or manual intervention.
    #[error("reorg error: {0}")]
    Reorg(#[source] ReorgError),

    /// State store persistence error
    ///
    /// This occurs when reading or writing stream state fails, typically due to
    /// database or filesystem issues.
    #[error("state store error: {0}")]
    StateStore(#[source] StateStoreError),

    /// Batch serialization/deserialization error
    ///
    /// This occurs when converting RecordBatches to/from Arrow IPC format fails.
    #[error("serialization error: {0}")]
    Serialization(#[source] SerializationError),

    /// Arrow data decoding error
    ///
    /// This occurs when decoding Arrow Flight data fails, typically due to:
    /// - Corrupted data in transit
    /// - Schema mismatches
    /// - Invalid Arrow IPC format
    #[error("Arrow error: {0}")]
    Arrow(#[source] common::arrow::error::ArrowError),

    /// JSON deserialization error
    ///
    /// This occurs when deserializing JSON metadata fails, typically when parsing
    /// the app_metadata field from FlightData.
    #[error("JSON deserialization error: {0}")]
    Json(#[source] serde_json::Error),
}

impl From<tonic::Status> for Error {
    fn from(value: tonic::Status) -> Self {
        Self::Status(Box::new(value))
    }
}

// ============================================================================
// Protocol Errors
// ============================================================================

/// Protocol-level errors
#[derive(thiserror::Error, Debug)]
pub enum ProtocolError {
    /// FlightInfo missing ticket
    ///
    /// This occurs when the server's FlightInfo response doesn't include a
    /// ticket, which is required to fetch query results.
    #[error("FlightInfo missing ticket")]
    MissingFlightTicket,
}

// ============================================================================
// Validation Errors
// ============================================================================

/// Protocol invariant validation errors
///
/// These errors occur when the server sends data that violates streaming
/// protocol invariants. All validation errors indicate server misbehavior.
#[derive(thiserror::Error, Debug)]
pub enum ValidationError {
    /// Duplicate network detected in a single batch
    ///
    /// This occurs when a batch contains multiple BlockRanges for the same network,
    /// violating the protocol invariant that each batch must contain at most one
    /// BlockRange per network.
    #[error("duplicate network '{network}' in batch")]
    DuplicateNetwork { network: String },

    /// Network count changed between batches
    ///
    /// This occurs when the number of networks changes between consecutive batches,
    /// violating the protocol invariant that the set of networks must remain stable
    /// throughout the stream.
    #[error("network count changed: expected {expected}, got {actual}")]
    NetworkCountChanged { expected: usize, actual: usize },

    /// Unexpected network appeared in batch
    ///
    /// This occurs when a new network appears in a batch that wasn't present in
    /// previous batches, violating the protocol invariant that the set of networks
    /// must remain stable throughout the stream.
    #[error("network set changed: unexpected network '{network}'")]
    UnexpectedNetwork { network: String },

    /// Non-consecutive blocks detected for a network
    ///
    /// This occurs when block ranges are not consecutive across batches for a network,
    /// violating the protocol invariant that ranges must be adjacent (unless a reorg
    /// is detected).
    ///
    /// For example:
    /// - Previous batch ended at block 100
    /// - Current batch starts at block 105 (gap: blocks 101-104 missing)
    #[error(
        "non-consecutive blocks for network '{network}': previous ended at {prev_end}, incoming starts at {incoming_start} (expected {expected_start} or matching range)"
    )]
    NonConsecutiveBlocks {
        network: String,
        prev_end: BlockNum,
        incoming_start: BlockNum,
        expected_start: BlockNum,
    },
}

// ============================================================================
// Reorg Errors
// ============================================================================

/// Reorg handling errors
///
/// These errors occur when blockchain reorgs cannot be handled cleanly within
/// the current stream and require reconnection or manual intervention.
#[derive(thiserror::Error, Debug)]
pub enum ReorgError {
    /// Partial reorg detected (reorg point inside watermark range)
    ///
    /// This occurs when a reorg affects blocks within a watermarked range, making it
    /// impossible to cleanly invalidate data. The client must reconnect using the
    /// recovery point as the starting point.
    ///
    /// This is not recoverable within the current stream and requires reconnection.
    #[error("partial reorg detected")]
    Partial,

    /// Unrecoverable reorg (all buffered watermarks affected)
    ///
    /// This occurs when a reorg affects all buffered watermarks, leaving no safe
    /// recovery point. The client must reconnect from the beginning or a known
    /// good checkpoint.
    ///
    /// This is a catastrophic error indicating the reorg extends beyond the
    /// retention window.
    #[error("unrecoverable reorg")]
    Unrecoverable,
}

// ============================================================================
// Serialization Errors
// ============================================================================

/// Batch serialization and deserialization errors
///
/// These errors occur when converting RecordBatches to/from Arrow IPC format.
#[derive(thiserror::Error, Debug)]
pub enum SerializationError {
    /// Failed to create Arrow IPC writer for batch serialization
    ///
    /// This occurs when initializing a StreamWriter fails, typically due to:
    /// - Invalid schema
    /// - Memory allocation failures
    #[error("failed to create batch writer: {0}")]
    WriterCreation(#[source] common::arrow::error::ArrowError),

    /// Failed to write batch to Arrow IPC format
    ///
    /// This occurs when writing a RecordBatch to the IPC stream fails, typically due to:
    /// - Memory allocation failures
    /// - Schema incompatibilities
    #[error("failed to write batch: {0}")]
    BatchWrite(#[source] common::arrow::error::ArrowError),

    /// Failed to finalize Arrow IPC writer
    ///
    /// This occurs when finishing the StreamWriter fails, typically due to:
    /// - I/O errors
    /// - Incomplete writes
    #[error("failed to finish batch writer: {0}")]
    WriterFinish(#[source] common::arrow::error::ArrowError),

    /// Failed to create Arrow IPC reader for batch deserialization
    ///
    /// This occurs when initializing a StreamReader fails, typically due to:
    /// - Corrupted IPC data
    /// - Invalid Arrow format
    #[error("failed to create batch reader: {0}")]
    ReaderCreation(#[source] common::arrow::error::ArrowError),

    /// Empty batch stream encountered
    ///
    /// This occurs when attempting to read a batch from an IPC stream that
    /// contains no data, which should not happen for valid serialized batches.
    #[error("empty batch stream")]
    EmptyBatchStream,

    /// Failed to read batch from Arrow IPC format
    ///
    /// This occurs when reading a RecordBatch from the IPC stream fails, typically due to:
    /// - Corrupted data
    /// - Schema mismatches
    #[error("failed to read batch: {0}")]
    BatchRead(#[source] common::arrow::error::ArrowError),
}

// ============================================================================
// State Store Errors
// ============================================================================

/// State store persistence errors
///
/// These errors occur when reading or writing stream state to persistent storage.
#[derive(thiserror::Error, Debug)]
pub enum StateStoreError {
    /// LMDB storage error
    ///
    /// This occurs when LMDB operations fail during state persistence.
    #[cfg(feature = "lmdb")]
    #[error("LMDB error: {0}")]
    Lmdb(#[source] LmdbError),

    /// PostgreSQL storage error
    ///
    /// This occurs when PostgreSQL operations fail during state persistence.
    #[cfg(feature = "postgres")]
    #[error("PostgreSQL error: {0}")]
    Postgres(#[source] PostgresError),

    /// Batch serialization error during storage
    ///
    /// This occurs when serializing batches for storage fails.
    #[error("serialization error: {0}")]
    Serialization(#[source] SerializationError),
}

/// LMDB-specific storage errors
#[cfg(feature = "lmdb")]
#[derive(thiserror::Error, Debug)]
pub enum LmdbError {
    /// Failed to create LMDB directory
    ///
    /// This occurs when the filesystem operation to create the LMDB storage
    /// directory fails, typically due to:
    /// - Permission issues
    /// - Disk full
    /// - Invalid path
    #[error("failed to create LMDB directory: {0}")]
    DirectoryCreation(#[source] std::io::Error),

    /// Failed to open LMDB environment
    ///
    /// This occurs when initializing the LMDB environment fails, typically due to:
    /// - Corrupted database files
    /// - Incompatible LMDB versions
    /// - Insufficient memory
    #[error("failed to open LMDB environment: {0}")]
    EnvOpen(#[source] heed::Error),

    /// Failed to create LMDB write transaction
    ///
    /// This occurs when beginning a write transaction fails, typically due to:
    /// - Database lock contention
    /// - Resource exhaustion
    #[error("failed to create LMDB write transaction: {0}")]
    WriteTransactionBegin(#[source] heed::Error),

    /// Failed to create LMDB read transaction
    ///
    /// This occurs when beginning a read transaction fails, typically due to:
    /// - Database lock contention
    /// - Resource exhaustion
    #[error("failed to create LMDB read transaction: {0}")]
    ReadTransactionBegin(#[source] heed::Error),

    /// Failed to create LMDB database
    ///
    /// This occurs when creating a named database within the LMDB environment fails,
    /// typically due to:
    /// - Maximum database count exceeded
    /// - Transaction errors
    #[error("failed to create LMDB database: {0}")]
    DatabaseCreation(#[source] heed::Error),

    /// Failed to commit LMDB transaction
    ///
    /// This occurs when committing a write transaction fails, typically due to:
    /// - Disk I/O errors
    /// - Disk full
    /// - Database corruption
    #[error("failed to commit LMDB transaction: {0}")]
    TransactionCommit(#[source] heed::Error),

    /// Failed to read state from LMDB
    ///
    /// This occurs when reading the state snapshot from LMDB fails, typically due to:
    /// - Database corruption
    /// - Transaction errors
    #[error("failed to read state from LMDB: {0}")]
    StateRead(#[source] heed::Error),

    /// Failed to write state to LMDB
    ///
    /// This occurs when writing the state snapshot to LMDB fails, typically due to:
    /// - Serialization errors
    /// - Transaction errors
    #[error("failed to write state to LMDB: {0}")]
    StateWrite(#[source] heed::Error),

    /// Failed to write batch to LMDB
    ///
    /// This occurs when storing a serialized batch in LMDB fails, typically due to:
    /// - Disk full
    /// - Transaction errors
    #[error("failed to write batch to LMDB: {0}")]
    BatchWrite(#[source] heed::Error),

    /// Failed to read batch from LMDB
    ///
    /// This occurs when loading a batch from LMDB fails, typically due to:
    /// - Missing key
    /// - Database corruption
    #[error("failed to read batch from LMDB: {0}")]
    BatchRead(#[source] heed::Error),

    /// Failed to create LMDB iterator
    ///
    /// This occurs when creating an iterator over LMDB keys fails, typically due to:
    /// - Transaction errors
    /// - Database corruption
    #[error("failed to create LMDB iterator: {0}")]
    IteratorCreation(#[source] heed::Error),

    /// Failed to iterate LMDB batches
    ///
    /// This occurs when iterating over batch keys fails, typically due to:
    /// - Database corruption
    /// - Transaction errors
    #[error("failed to iterate LMDB batches: {0}")]
    BatchIteration(#[source] heed::Error),

    /// Failed to delete batch from LMDB
    ///
    /// This occurs during pruning when deleting an old batch fails, typically due to:
    /// - Transaction errors
    /// - Database corruption
    #[error("failed to delete batch from LMDB: {0}")]
    BatchDelete(#[source] heed::Error),

    /// Invalid transaction ID key length in LMDB
    ///
    /// This occurs when deserializing a transaction ID key fails due to incorrect
    /// byte length, indicating database corruption or version mismatch.
    #[error("invalid transaction ID key length")]
    InvalidTransactionIdKeyLength,
}

/// PostgreSQL-specific storage errors
#[cfg(feature = "postgres")]
#[derive(thiserror::Error, Debug)]
pub enum PostgresError {
    /// Failed to run PostgreSQL migrations
    ///
    /// This occurs when applying database migrations fails, typically due to:
    /// - Missing migration files
    /// - Database permission issues
    /// - Schema conflicts
    #[error("failed to run PostgreSQL migrations: {0}")]
    MigrationFailed(#[source] sqlx::migrate::MigrateError),

    /// Failed to initialize PostgreSQL state
    ///
    /// This occurs when inserting the initial state row fails, typically due to:
    /// - Database connection errors
    /// - Permission issues
    #[error("failed to initialize PostgreSQL state: {0}")]
    StateInitialization(#[source] sqlx::Error),

    /// Failed to advance next transaction ID in PostgreSQL
    ///
    /// This occurs when updating the next transaction ID fails, typically due to:
    /// - Database connection errors
    /// - Transaction conflicts
    #[error("failed to advance next transaction ID in PostgreSQL: {0}")]
    AdvanceNext(#[source] sqlx::Error),

    /// Stream not found in PostgreSQL
    ///
    /// This occurs when attempting to update a stream that doesn't exist in the
    /// database, indicating the stream was deleted or never initialized.
    #[error("stream '{stream_id}' not found in PostgreSQL")]
    StreamNotFound { stream_id: String },

    /// Failed to truncate buffer in PostgreSQL
    ///
    /// This occurs during reorg handling when deleting watermarks fails, typically due to:
    /// - Database connection errors
    /// - Transaction conflicts
    #[error("failed to truncate buffer in PostgreSQL: {0}")]
    BufferTruncate(#[source] sqlx::Error),

    /// Failed to begin PostgreSQL transaction
    ///
    /// This occurs when starting a database transaction fails, typically due to:
    /// - Connection pool exhaustion
    /// - Database unavailability
    #[error("failed to begin PostgreSQL transaction: {0}")]
    TransactionBegin(#[source] sqlx::Error),

    /// Failed to commit PostgreSQL transaction
    ///
    /// This occurs when committing a transaction fails, typically due to:
    /// - Connection loss
    /// - Constraint violations
    /// - Serialization failures
    #[error("failed to commit PostgreSQL transaction: {0}")]
    TransactionCommit(#[source] sqlx::Error),

    /// Failed to delete pruned entries from PostgreSQL
    ///
    /// This occurs during watermark pruning when deleting old entries fails,
    /// typically due to:
    /// - Database connection errors
    /// - Transaction conflicts
    #[error("failed to delete pruned entries from PostgreSQL: {0}")]
    PrunedEntriesDelete(#[source] sqlx::Error),

    /// Failed to serialize block ranges for PostgreSQL
    ///
    /// This occurs when serializing BlockRanges to JSON fails, typically due to:
    /// - Memory allocation failures
    /// - Invalid data structures
    #[error("failed to serialize block ranges for PostgreSQL: {0}")]
    BlockRangesSerialization(#[source] serde_json::Error),

    /// Failed to insert buffer entry into PostgreSQL
    ///
    /// This occurs when inserting a watermark into the buffer table fails,
    /// typically due to:
    /// - Database connection errors
    /// - Constraint violations
    #[error("failed to insert buffer entry into PostgreSQL: {0}")]
    BufferEntryInsert(#[source] sqlx::Error),

    /// Failed to load next transaction ID from PostgreSQL
    ///
    /// This occurs when querying the next transaction ID fails, typically due to:
    /// - Database connection errors
    /// - Missing state row
    #[error("failed to load next transaction ID from PostgreSQL: {0}")]
    NextTransactionIdLoad(#[source] sqlx::Error),

    /// Failed to load buffer entries from PostgreSQL
    ///
    /// This occurs when querying watermark buffer entries fails, typically due to:
    /// - Database connection errors
    /// - Query timeouts
    #[error("failed to load buffer entries from PostgreSQL: {0}")]
    BufferEntriesLoad(#[source] sqlx::Error),

    /// Failed to deserialize block ranges from PostgreSQL
    ///
    /// This occurs when deserializing BlockRanges from JSON fails, typically due to:
    /// - Database corruption
    /// - Schema version mismatches
    #[error("failed to deserialize block ranges from PostgreSQL: {0}")]
    BlockRangesDeserialization(#[source] serde_json::Error),
}
