//! Error types for worker operations with improved traceability.
//!
//! This module provides fine-grained error types for each major operation,
//! enabling better error tracing and debugging without losing context.

use crate::{job::JobId, node_id::NodeId};

/// Errors that can occur during worker initialization (Phase 1).
///
/// These errors occur during the setup phase before the worker service
/// starts its main event loop. Initialization includes:
/// - Worker registration in the metadata database
/// - Establishing the heartbeat connection
/// - Setting up the job notification listener
/// - Bootstrapping scheduled jobs from the database
///
/// All initialization errors are fatal and prevent the worker from starting.
#[derive(Debug, thiserror::Error)]
pub enum InitError {
    /// Worker registration failed.
    ///
    /// This occurs during the initial registration phase when the worker attempts to
    /// register itself in the metadata database. Registration is required before the
    /// worker can accept jobs.
    ///
    /// Common causes include:
    /// - Database connection failures
    /// - Database query execution errors
    /// - Insufficient database permissions
    #[error("database error during registration")]
    Registration(#[source] metadata_db::Error),

    /// Heartbeat loop initialization failed.
    ///
    /// This occurs during the setup of the heartbeat loop, which is required to
    /// maintain the worker's active status in the metadata database.
    ///
    /// See [`HeartbeatLoopInitError`] for specific failure modes during heartbeat
    /// loop initialization.
    #[error("heartbeat loop initialization failed")]
    HeartbeatLoopInit(#[source] HeartbeatLoopInitError),

    /// Notification listener setup failed.
    ///
    /// This occurs when establishing the `PostgreSQL` LISTEN connection for receiving job
    /// notifications. Without this connection, the worker cannot receive new job assignments.
    ///
    /// Common causes include:
    /// - Database connection pool exhaustion
    /// - Network connectivity issues
    /// - `PostgreSQL` LISTEN channel subscription failures
    #[error("failed to establish listener connection")]
    NotificationSetup(#[source] metadata_db::Error),

    /// Failed to fetch scheduled jobs from the metadata database during bootstrap.
    ///
    /// This occurs when querying the metadata database for jobs in `SCHEDULED` or
    /// `RUNNING` status that are assigned to this worker. The query failure prevents
    /// the worker from recovering its previous state.
    ///
    /// Common causes include:
    /// - Database connection failures during the query
    /// - Database query execution errors
    /// - Timeout while waiting for the query results
    #[error("failed to fetch scheduled jobs")]
    BootstrapFetchScheduledJobs(#[source] metadata_db::Error),

    /// Failed to spawn a job during bootstrap.
    ///
    /// This occurs when attempting to resume a previously scheduled or running job
    /// during the bootstrap phase. The job was successfully fetched from the database
    /// but could not be spawned in the worker's job set.
    ///
    /// See [`SpawnJobError`] for specific failure modes during job spawning.
    #[error("failed to spawn job {job_id} during bootstrap")]
    BootstrapSpawnJob {
        job_id: JobId,
        #[source]
        source: SpawnJobError,
    },
}

/// Errors that can occur during heartbeat loop initialization.
///
/// The heartbeat loop maintains the worker's active status by periodically
/// updating a timestamp in the metadata database. Initialization involves:
/// - Establishing a dedicated database connection
/// - Acquiring a PostgreSQL advisory lock on the worker node ID
///
/// These errors occur during the setup phase before the heartbeat loop starts.
#[derive(Debug, thiserror::Error)]
pub enum HeartbeatLoopInitError {
    /// Failed to establish database connection for heartbeat loop.
    ///
    /// This occurs when creating a dedicated connection to the metadata database
    /// for the heartbeat loop. The heartbeat requires its own connection to avoid
    /// interfering with other database operations.
    ///
    /// Common causes include:
    /// - Database unavailable or not accepting connections
    /// - Network connectivity issues
    /// - Database authentication failures
    /// - Connection string configuration errors
    #[error("failed to establish database connection for heartbeat loop")]
    ConnectionFailed(#[source] metadata_db::Error),

    /// Failed to acquire advisory lock on worker node ID.
    ///
    /// This occurs when the PostgreSQL advisory lock acquisition fails due to a
    /// database error (not because the lock is already held). The lock is used
    /// to ensure that only one worker process can use a given node ID at a time.
    ///
    /// Common causes include:
    /// - Database connection lost after initial connection
    /// - Database query execution error
    /// - Permission issues preventing lock acquisition
    #[error("failed to acquire advisory lock on worker node ID")]
    LockAcquisitionFailed(#[source] metadata_db::Error),

    /// Worker node ID is already in use by another worker process.
    ///
    /// This occurs when attempting to acquire a PostgreSQL advisory lock on the
    /// worker node ID, but the lock is already held by another active worker process.
    /// Each worker must have a unique node ID to prevent conflicts.
    ///
    /// The advisory lock is held for as long as the worker's database connection
    /// stays open. If this error occurs, another worker with the same node ID is
    /// currently running.
    #[error("worker node ID '{0}' is already in use")]
    NodeIdInUse(NodeId),
}

/// Errors that can occur during worker runtime (Phase 2).
///
/// These errors occur during the worker's main event loop after successful
/// initialization. The main loop handles:
/// - Heartbeat task monitoring
/// - Job notification processing
/// - Job completion/failure handling
/// - Periodic state reconciliation with the metadata database
///
/// Runtime errors are typically fatal and will cause the worker to shut down.
#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    /// Heartbeat task died unexpectedly.
    ///
    /// This occurs when the background heartbeat task exits, panics, or encounters
    /// a fatal error. The heartbeat is critical for maintaining the worker's active
    /// status in the metadata database, so its failure is a fatal error.
    ///
    /// See [`HeartbeatTaskError`] for specific heartbeat failure modes.
    #[error("heartbeat task died {0}")]
    HeartbeatTaskDied(#[source] HeartbeatTaskError),

    /// Error handling a job notification.
    ///
    /// This occurs when processing a job start/stop notification received via the
    /// `PostgreSQL` NOTIFY mechanism. Notification handling involves loading job
    /// metadata and spawning or aborting jobs.
    ///
    /// See [`NotificationError`] for specific notification handling failure modes.
    #[error("notification handling error: {0}")]
    NotificationHandling(#[source] NotificationError),

    /// Error handling a job result.
    ///
    /// This occurs when processing the result of a completed, failed, or aborted job.
    /// Result handling involves updating the job's status in the metadata database.
    ///
    /// See [`JobResultError`] for specific job result handling failure modes.
    #[error("job result handling error: {0}")]
    JobResultHandling(#[source] JobResultError),

    /// Reconciliation error.
    ///
    /// This occurs during the periodic reconciliation process that synchronizes the
    /// worker's in-memory job state with the authoritative state in the metadata
    /// database. Reconciliation helps recover from missed notifications.
    ///
    /// See [`ReconcileError`] for specific reconciliation failure modes.
    #[error("reconciliation error: {0}")]
    Reconciliation(#[source] ReconcileError),
}

/// Errors from the heartbeat task.
///
/// The heartbeat task runs in the background continuously updating the worker's
/// last-seen timestamp in the metadata database to indicate the worker is alive
/// and accepting jobs.
#[derive(Debug, thiserror::Error)]
pub enum HeartbeatTaskError {
    /// Heartbeat task exited unexpectedly without error.
    ///
    /// This occurs when the heartbeat task completes successfully (returns `Ok(())`),
    /// which should never happen as the heartbeat loop is designed to run indefinitely.
    /// This indicates a logic error in the heartbeat implementation.
    #[error("heartbeat task exited unexpectedly")]
    UnexpectedExit,

    /// Heartbeat update failed.
    ///
    /// This occurs when the database update to refresh the worker's heartbeat timestamp
    /// fails. Repeated heartbeat failures will eventually mark the worker as inactive.
    ///
    /// Common causes include:
    /// - Database connection loss
    /// - Database query execution errors
    /// - Network connectivity issues
    #[error("heartbeat update failed: {0}")]
    UpdateFailed(#[source] metadata_db::Error),

    /// Heartbeat task panicked.
    ///
    /// This occurs when the heartbeat task encounters an unexpected panic, indicating
    /// a critical bug in the heartbeat implementation or a severe system issue.
    #[error("heartbeat task panicked: {0}")]
    Panicked(#[source] Box<dyn std::error::Error + Send + Sync>),
}

/// Errors that can occur when spawning a job.
///
/// Spawning a job involves:
/// 1. Updating the job status to `RUNNING` in the metadata database
/// 2. Parsing the job descriptor JSON
/// 3. Creating the job instance with all required resources
/// 4. Adding the job to the worker's job set for execution
#[derive(Debug, thiserror::Error)]
pub enum SpawnJobError {
    /// Failed to update job status to `RUNNING`.
    ///
    /// This occurs when the database update to mark the job as running fails.
    /// The job cannot be spawned without updating its status, as this would
    /// create inconsistency between the database state and the worker state.
    ///
    /// Common causes include:
    /// - Database connection failures
    /// - Database query execution errors
    /// - Job already in a terminal state (completed, failed, stopped)
    #[error("failed to update job status to RUNNING: {0}")]
    StatusUpdateFailed(#[source] metadata_db::Error),

    /// Failed to parse job descriptor.
    ///
    /// This occurs when the job descriptor JSON stored in the database cannot
    /// be deserialized into the expected `Descriptor` type. This indicates either
    /// corrupted data in the database or a schema mismatch.
    ///
    /// Common causes include:
    /// - Invalid JSON syntax in the descriptor field
    /// - Missing required fields in the descriptor
    /// - Schema version mismatch between job creation and execution
    #[error("failed to parse job descriptor: {0}")]
    DescriptorParseFailed(#[source] serde_json::Error),
}

/// Errors that can occur when aborting a job.
///
/// Aborting a job updates its status to `STOPPING` in the metadata database and
/// signals the job's execution task to gracefully shut down. The job will later
/// transition to `STOPPED` status once it completes its shutdown.
///
/// Common causes include:
/// - Database connection failures during the status update
/// - Database query execution errors
/// - Job already in a terminal state
#[derive(Debug, thiserror::Error)]
#[error("failed to update job status to STOPPING: {0}")]
pub struct AbortJobError(#[source] pub metadata_db::Error);

/// Errors that can occur during job creation.
///
/// Job creation resolves all resources needed to execute a job, including:
/// - Output locations (dataset/table pairs)
/// - Dataset manifests and configurations
/// - Physical table metadata
#[derive(Debug, thiserror::Error)]
pub enum JobCreationError {
    /// Failed to fetch output locations from the metadata database.
    ///
    /// This occurs when querying the metadata database for the job's output locations
    /// (the dataset/table pairs where the job will write its results). Each job must
    /// have at least one output location defined.
    ///
    /// Common causes include:
    /// - Database connection failures
    /// - Job ID not found in the `physical_tables` table
    /// - Database query execution errors
    #[error("failed to fetch output locations: {0}")]
    OutputLocationsFetchFailed(#[source] metadata_db::Error),

    /// Dataset not found in the dataset store.
    ///
    /// This occurs when the dataset specified in an output location does not exist
    /// in the dataset store. The dataset may have been deleted or never registered.
    #[error("dataset '{dataset}' not found")]
    DatasetNotFound { dataset: String },

    /// Failed to retrieve dataset from the dataset store.
    ///
    /// This occurs when the dataset store fails to retrieve or construct the dataset,
    /// which may be due to manifest parsing errors, missing dependencies, or other
    /// dataset-specific initialization failures.
    #[error("failed to get dataset: {0}")]
    DatasetFetchFailed(#[source] Box<dyn std::error::Error + Send + Sync>),

    /// Table not found in dataset.
    ///
    /// This occurs when the table specified in an output location does not exist in
    /// the dataset's schema. This indicates a mismatch between the job configuration
    /// and the dataset definition.
    #[error("table '{table}' not found in dataset '{dataset}'")]
    TableNotFound { table: String, dataset: String },

    /// Failed to create physical table instance.
    ///
    /// This occurs when constructing a `PhysicalTable` from the dataset table definition
    /// and metadata fails. Physical tables represent the actual Parquet files where
    /// data will be written.
    ///
    /// Common causes include:
    /// - Invalid table metadata
    /// - Incompatible table schema
    /// - Storage configuration errors
    #[error("failed to create physical table: {0}")]
    PhysicalTableCreationFailed(#[source] Box<dyn std::error::Error + Send + Sync>),
}

/// Errors that can occur when handling notifications.
///
/// Notifications are received via `PostgreSQL`'s LISTEN/NOTIFY mechanism and
/// instruct the worker to start or stop jobs. The notification stream is
/// continuously polled in the main loop.
#[derive(Debug, thiserror::Error)]
pub enum NotificationError {
    /// Failed to deserialize notification message.
    ///
    /// This occurs when a notification received via `PostgreSQL` NOTIFY cannot be
    /// deserialized into the expected `Notification` structure. This may indicate
    /// corrupted notification data or a protocol version mismatch.
    ///
    /// Common causes include:
    /// - Invalid JSON in the notification payload
    /// - Missing required fields in the notification
    /// - Schema version mismatch between sender and receiver
    #[error("failed to deserialize notification: {0}")]
    DeserializationFailed(#[source] metadata_db::WorkerNotifRecvError),

    /// Notification stream closed unexpectedly.
    ///
    /// This occurs when the `PostgreSQL` LISTEN connection is closed, preventing the
    /// worker from receiving new job notifications. This should not happen during
    /// normal operation and indicates a connection issue.
    ///
    /// Common causes include:
    /// - Database connection loss
    /// - `PostgreSQL` server restart
    /// - Network connectivity issues
    #[error("notification stream closed unexpectedly")]
    StreamClosed,

    /// Failed to handle a job start notification.
    ///
    /// This occurs when processing a `Start` action for a job. Start handling
    /// involves loading the job metadata from the database and spawning the job
    /// for execution.
    ///
    /// See [`StartActionError`] for specific start action failure modes.
    #[error("failed to handle start action for job {job_id}")]
    StartActionFailed {
        job_id: JobId,
        source: StartActionError,
    },

    /// Failed to handle a job stop notification.
    ///
    /// This occurs when processing a `Stop` action for a job. Stop handling
    /// involves updating the job status to `STOPPING` and signaling the job
    /// to gracefully shut down.
    ///
    /// See [`AbortJobError`] for specific stop action failure modes.
    #[error("failed to handle stop action for job {job_id}")]
    StopActionFailed {
        job_id: JobId,
        source: AbortJobError,
    },
}

/// Errors that can occur when handling a start action.
///
/// A start action is triggered by a job notification instructing the worker to
/// begin executing a job. This involves loading the job metadata and spawning
/// the job for execution.
#[derive(Debug, thiserror::Error)]
pub enum StartActionError {
    /// Failed to load job metadata from the database.
    ///
    /// This occurs when querying the metadata database for the job's metadata
    /// (descriptor, status, output locations, etc.) fails.
    ///
    /// Common causes include:
    /// - Database connection failures
    /// - Database query execution errors
    /// - Timeout while waiting for query results
    #[error("failed to load job from database: {0}")]
    JobLoadFailed(#[source] metadata_db::Error),

    /// Job not found in the database.
    ///
    /// This occurs when the job ID from the notification does not exist in the
    /// jobs table. This may indicate the job was deleted between notification
    /// and retrieval, or a stale notification.
    #[error("job not found in database")]
    JobNotFound,

    /// Failed to spawn the job.
    ///
    /// This occurs when the job metadata was successfully loaded but the job
    /// could not be spawned for execution.
    ///
    /// See [`SpawnJobError`] for specific job spawning failure modes.
    #[error("failed to spawn job")]
    SpawnFailed(#[source] SpawnJobError),
}

/// Errors that can occur when handling job results.
///
/// Job results are emitted when a job completes its execution, either successfully
/// or with an error. The worker must update the job's status in the metadata database
/// to reflect the outcome.
#[derive(Debug, thiserror::Error)]
pub enum JobResultError {
    /// Failed to mark job as completed.
    ///
    /// This occurs when a job finishes successfully but the database update to
    /// mark the job as `COMPLETED` fails. The job execution succeeded, but the
    /// status update failed, creating a state inconsistency.
    ///
    /// Common causes include:
    /// - Database connection failures during status update
    /// - Database query execution errors
    /// - Concurrent status updates by other processes
    #[error("failed to mark job {job_id} as COMPLETED: {source}")]
    MarkCompletedFailed {
        job_id: JobId,
        source: metadata_db::Error,
    },

    /// Failed to mark job as failed.
    ///
    /// This occurs when a job execution fails but the database update to mark
    /// the job as `FAILED` also fails. The job execution failed, but the status
    /// update failed, creating a state inconsistency.
    ///
    /// Common causes include:
    /// - Database connection failures during status update
    /// - Database query execution errors
    /// - Concurrent status updates by other processes
    #[error("failed to mark job {job_id} as FAILED: {source}")]
    MarkFailedFailed {
        job_id: JobId,
        source: metadata_db::Error,
    },

    /// Failed to mark job as stopped.
    ///
    /// This occurs when a job is aborted and finishes shutdown but the database
    /// update to mark the job as `STOPPED` fails. The job stopped execution, but
    /// the status update failed, creating a state inconsistency.
    ///
    /// Common causes include:
    /// - Database connection failures during status update
    /// - Database query execution errors
    /// - Concurrent status updates by other processes
    #[error("failed to mark job {job_id} as STOPPED: {source}")]
    MarkStoppedFailed {
        job_id: JobId,
        source: metadata_db::Error,
    },
}

/// Errors that can occur during job reconciliation.
///
/// Reconciliation is a periodic process that synchronizes the worker's in-memory
/// job state with the authoritative job state in the metadata database. This helps
/// recover from missed notifications or state inconsistencies.
///
/// The reconciliation process:
/// 1. Fetches all active jobs for this worker from the database
/// 2. Spawns jobs that should be running but aren't in the worker's job set
/// 3. Aborts jobs that should be stopped but are still running
#[derive(Debug, thiserror::Error)]
pub enum ReconcileError {
    /// Failed to fetch active jobs from the metadata database.
    ///
    /// This occurs when querying the metadata database for all jobs in `SCHEDULED`,
    /// `RUNNING`, or `STOP_REQUESTED` status that are assigned to this worker.
    ///
    /// Common causes include:
    /// - Database connection failures during the query
    /// - Database query execution errors
    /// - Timeout while waiting for query results
    #[error("failed to fetch active jobs: {0}")]
    FetchActiveJobsFailed(#[source] metadata_db::Error),

    /// Failed to spawn a job during reconciliation.
    ///
    /// This occurs when the reconciliation process identifies a job that should
    /// be running (status is `SCHEDULED` or `RUNNING` in the database) but is not
    /// present in the worker's job set, and the attempt to spawn it fails.
    ///
    /// See [`SpawnJobError`] for specific job spawning failure modes.
    #[error("failed to spawn job {job_id} during reconciliation")]
    SpawnJobFailed {
        job_id: JobId,
        source: SpawnJobError,
    },

    /// Failed to abort a job during reconciliation.
    ///
    /// This occurs when the reconciliation process identifies a job that should
    /// be stopped (status is `STOP_REQUESTED` in the database) but is still running
    /// in the worker's job set, and the attempt to abort it fails.
    ///
    /// See [`AbortJobError`] for specific job abortion failure modes.
    #[error("failed to abort job {job_id} during reconciliation")]
    AbortJobFailed {
        job_id: JobId,
        source: AbortJobError,
    },
}
