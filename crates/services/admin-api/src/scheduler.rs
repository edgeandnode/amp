//! Scheduler trait abstractions for job and worker management
//!
//! This module defines the `SchedulerJobs` and `SchedulerWorkers` traits, which provide
//! abstraction layers for scheduling/managing dataset extraction jobs and querying worker
//! information. The traits are implemented by the controller service, following the
//! dependency inversion principle.
//!
//! ## Architecture
//!
//! - **admin-api**: Defines the `SchedulerJobs` and `SchedulerWorkers` traits (abstractions)
//! - **controller**: Provides `Scheduler` (implementation)
//! - **handlers**: Depend on the traits via `Arc<dyn SchedulerJobs>` and `Arc<dyn SchedulerWorkers>`
//!
//! ## Responsibilities
//!
//! ### SchedulerJobs
//! - Job lifecycle management (schedule, stop, query, delete)
//! - Worker coordination and selection
//! - Job state validation and transitions
//! - Bulk cleanup operations for terminal jobs
//!
//! ### SchedulerWorkers
//! - Worker information queries
//! - Worker status retrieval

use std::sync::Arc;

use async_trait::async_trait;
use common::{BoxError, Dataset, catalog::JobLabels};
use datasets_common::{hash::Hash, name::Name, namespace::Namespace};
use dump::EndBlock;
use metadata_db::Worker;
use worker::{
    job::{Job, JobId, JobStatus},
    node_id::NodeId,
};

/// Combined trait for scheduler functionality
///
/// This trait combines both job management and worker query capabilities,
/// providing a unified interface for handlers that need both.
/// Any type implementing both `SchedulerJobs` and `SchedulerWorkers` automatically
/// implements this trait.
pub trait Scheduler: SchedulerJobs + SchedulerWorkers {}

/// Blanket implementation for any type that implements both traits
impl<T> Scheduler for T where T: SchedulerJobs + SchedulerWorkers {}

/// Trait for scheduling and managing dataset extraction jobs
// NOTE: Using specific wrapper methods instead of `delete_jobs_by_status<const N: usize>`
// because const generics make the trait not dyn-compatible.
#[async_trait]
pub trait SchedulerJobs: Send + Sync {
    /// Schedule a dataset synchronization job
    async fn schedule_dataset_sync_job(
        &self,
        dataset: Arc<Dataset>,
        end_block: EndBlock,
        max_writers: u16,
        job_labels: JobLabels,
        worker_id: Option<NodeId>,
    ) -> Result<JobId, ScheduleJobError>;

    /// Stop a running job
    async fn stop_job(&self, job_id: JobId) -> Result<(), StopJobError>;

    /// Get a job by its ID
    async fn get_job(&self, job_id: JobId) -> Result<Option<Job>, GetJobError>;

    /// List jobs with cursor-based pagination, optionally filtered by status
    ///
    /// If `statuses` is `None`, all jobs are returned.
    /// If `statuses` is `Some(&[])`, an empty array, all jobs are returned.
    /// If `statuses` is `Some(&[status1, status2, ...])`, only jobs with those statuses are returned.
    async fn list_jobs(
        &self,
        limit: i64,
        last_id: Option<JobId>,
        statuses: Option<&[JobStatus]>,
    ) -> Result<Vec<Job>, ListJobsError>;

    /// Delete a job if it's in a terminal state
    async fn delete_job(&self, job_id: JobId) -> Result<bool, DeleteJobError>;

    /// Delete all jobs in terminal states (Completed, Stopped, Failed)
    async fn delete_jobs_in_terminal_state(&self) -> Result<usize, DeleteJobsByStatusError>;

    /// Delete all completed jobs
    async fn delete_completed_jobs(&self) -> Result<usize, DeleteJobsByStatusError>;

    /// Delete all stopped jobs
    async fn delete_stopped_jobs(&self) -> Result<usize, DeleteJobsByStatusError>;

    /// Delete all failed jobs
    async fn delete_failed_jobs(&self) -> Result<usize, DeleteJobsByStatusError>;

    /// List all jobs for a specific dataset by namespace, name, and manifest hash
    async fn list_jobs_by_dataset(
        &self,
        namespace: &Namespace,
        name: &Name,
        hash: &Hash,
    ) -> Result<Vec<Job>, ListJobsByDatasetError>;
}

/// Errors that can occur when scheduling a dataset dump job
#[derive(Debug, thiserror::Error)]
pub enum ScheduleJobError {
    /// Failed to check for existing jobs in the database
    ///
    /// This occurs when:
    /// - Database query for existing jobs by dataset fails
    /// - Connection is lost during job lookup
    /// - Connection pool is exhausted
    #[error("failed to check existing jobs: {0}")]
    CheckExistingJobs(#[source] metadata_db::Error),

    /// Failed to list active workers from the database
    ///
    /// This occurs when:
    /// - Worker heartbeat query fails
    /// - Connection is lost during worker lookup
    /// - Worker table is inaccessible
    #[error("failed to list active workers: {0}")]
    ListActiveWorkers(#[source] metadata_db::Error),

    /// No workers available to schedule the job
    ///
    /// This occurs when:
    /// - All workers are inactive or haven't sent heartbeats recently
    /// - No workers are registered in the system
    /// - All workers are at capacity
    #[error("no workers available")]
    NoWorkersAvailable,

    /// Specified worker not found or inactive
    ///
    /// This occurs when:
    /// - The specified worker ID doesn't exist in the system
    /// - The specified worker hasn't sent heartbeats recently (inactive)
    #[error("specified worker '{0}' not found or inactive")]
    WorkerNotAvailable(NodeId),

    /// Failed to get active physical table for dataset
    ///
    /// This occurs when:
    /// - Physical table lookup query fails
    /// - Catalog metadata is corrupted or inconsistent
    /// - Database connection fails during table lookup
    #[error("failed to get physical table: {0}")]
    GetPhysicalTable(#[source] BoxError),

    /// Failed to create new physical table revision
    ///
    /// This occurs when:
    /// - Storage location allocation fails
    /// - Physical table creation in catalog fails
    /// - Insufficient storage space or permissions
    #[error("failed to create physical table: {0}")]
    CreatePhysicalTable(#[source] BoxError),

    /// Failed to serialize job descriptor to JSON
    ///
    /// This occurs when:
    /// - JobDescriptor cannot be serialized to JSON
    /// - Invalid UTF-8 characters in job parameters
    /// - Serialization buffer overflow
    #[error("failed to serialize job descriptor: {0}")]
    SerializeJobDescriptor(#[source] serde_json::Error),

    /// Failed to register job in the metadata database
    ///
    /// This occurs when:
    /// - Job insertion into database fails
    /// - Unique constraint violation on job ID
    /// - Connection is lost during job registration
    #[error("failed to register job: {0}")]
    RegisterJob(#[source] metadata_db::Error),

    /// Failed to send job notification to worker
    ///
    /// This occurs when:
    /// - PostgreSQL LISTEN/NOTIFY fails
    /// - Worker notification channel is unavailable
    /// - Connection is lost during notification
    #[error("failed to notify worker: {0}")]
    NotifyWorker(#[source] metadata_db::Error),
}

/// Errors that can occur when stopping a job
#[derive(Debug, thiserror::Error)]
pub enum StopJobError {
    /// Job not found
    ///
    /// This occurs when:
    /// - No job exists with the specified ID in the database
    /// - The job was deleted after the stop request was initiated
    #[error("job not found")]
    JobNotFound,

    /// Job is already in a terminal state (stopped, completed, failed)
    ///
    /// This occurs when:
    /// - Job has already completed successfully
    /// - Job was previously stopped by another request
    /// - Job has already failed
    #[error("job is already in terminal state: {status}")]
    JobAlreadyTerminated { status: JobStatus },

    /// Job state conflict - cannot stop from current state
    ///
    /// This occurs when:
    /// - Job is in an unexpected state that doesn't allow stopping
    /// - Concurrent state modifications created an invalid transition
    #[error("cannot stop job from current state: {current_status}")]
    StateConflict { current_status: JobStatus },

    /// Failed to begin transaction for stop operation
    ///
    /// This occurs when:
    /// - Database connection pool is exhausted
    /// - Database connection fails or is lost
    /// - Transaction initialization encounters an error
    #[error("failed to begin transaction")]
    BeginTransaction(#[source] metadata_db::Error),

    /// Failed to retrieve job information during stop operation
    ///
    /// This occurs when:
    /// - Database query fails during job lookup
    /// - Connection is lost during the transaction
    /// - Query execution encounters an error
    #[error("failed to get job")]
    GetJob(#[source] metadata_db::Error),

    /// Failed to update job status to stop-requested
    ///
    /// This occurs when:
    /// - Database update operation fails
    /// - Transaction encounters a constraint violation
    /// - Connection is lost during status update
    #[error("failed to update job status")]
    UpdateJobStatus(#[source] metadata_db::Error),

    /// Failed to commit transaction for stop operation
    ///
    /// This occurs when:
    /// - Transaction commit fails due to conflicts
    /// - Connection is lost before commit completes
    /// - Database encounters an error during commit
    #[error("failed to commit transaction")]
    CommitTransaction(#[source] metadata_db::Error),

    /// Failed to send stop notification to worker
    ///
    /// This occurs when:
    /// - Worker notification channel fails
    /// - Database notification system encounters an error
    /// - Connection is lost during notification
    #[error("failed to send stop notification")]
    SendNotification(#[source] metadata_db::Error),
}

/// Error when getting a job from the metadata database
///
/// This occurs when:
/// - Database connection fails or is lost
/// - Query execution encounters an error
/// - Connection pool is exhausted
#[derive(Debug, thiserror::Error)]
#[error("metadata database error")]
pub struct GetJobError(#[source] pub metadata_db::Error);

/// Error when listing jobs from the metadata database
///
/// This occurs when:
/// - Database connection fails or is lost
/// - Query execution encounters an error (invalid pagination cursor, etc.)
/// - Connection pool is exhausted
#[derive(Debug, thiserror::Error)]
#[error("metadata database error")]
pub struct ListJobsError(#[source] pub metadata_db::Error);

/// Error when deleting a single job from the metadata database
///
/// This occurs when:
/// - Database connection fails or is lost
/// - Delete operation encounters an error
/// - Connection pool is exhausted
#[derive(Debug, thiserror::Error)]
#[error("metadata database error")]
pub struct DeleteJobError(#[source] pub metadata_db::Error);

/// Error when deleting jobs by status from the metadata database
///
/// This occurs when:
/// - Database connection fails or is lost
/// - Bulk delete operation encounters an error
/// - Connection pool is exhausted
#[derive(Debug, thiserror::Error)]
#[error("metadata database error")]
pub struct DeleteJobsByStatusError(#[source] pub metadata_db::Error);

/// Error when listing jobs by dataset from the metadata database
///
/// This occurs when:
/// - Database connection fails or is lost
/// - Query execution encounters an error
/// - Connection pool is exhausted
#[derive(Debug, thiserror::Error)]
#[error("metadata database error")]
pub struct ListJobsByDatasetError(#[source] pub metadata_db::Error);

/// Trait for querying worker information
///
/// This trait provides methods to retrieve information about registered worker nodes
/// in the system. It abstracts the metadata database operations for worker queries,
/// allowing handlers to depend on this interface rather than direct database access.
#[async_trait]
pub trait SchedulerWorkers: Send + Sync {
    /// List all registered workers
    ///
    /// Returns all workers in the system with their complete information including
    /// node_id, build info, and timestamp information (created_at, registered_at, heartbeat_at).
    async fn list_workers(&self) -> Result<Vec<Worker>, ListWorkersError>;

    /// Get a worker by its node ID
    ///
    /// Returns worker information if a worker with the specified node_id exists,
    /// or None if no such worker is found.
    async fn get_worker_by_id(&self, id: &NodeId) -> Result<Option<Worker>, GetWorkerError>;
}

/// Error when listing workers from the metadata database
///
/// This occurs when:
/// - Database connection fails or is lost
/// - Query execution encounters an error
/// - Connection pool is exhausted
#[derive(Debug, thiserror::Error)]
#[error("metadata database error")]
pub struct ListWorkersError(#[source] pub metadata_db::Error);

/// Error when getting a worker by ID from the metadata database
///
/// This occurs when:
/// - Database connection fails or is lost
/// - Query execution encounters an error
/// - Connection pool is exhausted
#[derive(Debug, thiserror::Error)]
#[error("metadata database error")]
pub struct GetWorkerError(#[source] pub metadata_db::Error);
