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

use amp_dataset_store::dataset_kind::DatasetKind;
use async_trait::async_trait;
use datasets_common::{
    hash::Hash, hash_reference::HashReference, name::Name, namespace::Namespace,
};
use dump::EndBlock;
use metadata_db::Worker;
use worker::{
    job::{Job, JobId, JobStatus},
    node_id::{InvalidIdError, NodeId, validate_node_id},
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
        dataset_reference: HashReference,
        dataset_kind: DatasetKind,
        end_block: EndBlock,
        max_writers: u16,
        worker_id: Option<NodeSelector>,
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

    /// No active workers match the glob pattern
    ///
    /// This occurs when:
    /// - No workers are registered in the system
    /// - No workers are active (have sent heartbeats recently)
    /// - No workers match the glob pattern
    #[error("no active workers match pattern '{0}'")]
    NoMatchingWorkers(NodeIdGlob),
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

/// A glob pattern for matching worker node IDs by prefix
///
/// Matches any node ID that starts with the pattern prefix.
/// Created by parsing a string ending with `*` (e.g., `"worker-eth-*"`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeIdGlob(String);

impl serde::Serialize for NodeIdGlob {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        format!("{}*", self.0).serialize(serializer)
    }
}

impl std::str::FromStr for NodeIdGlob {
    type Err = InvalidGlobError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // If the string ends with '*', remove it and validate the prefix
        let Some(prefix) = s.strip_suffix('*') else {
            return Err(InvalidGlobError(s.to_string()));
        };
        validate_node_id(prefix)?;

        Ok(NodeIdGlob(prefix.to_string()))
    }
}

impl std::fmt::Display for NodeIdGlob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}*", self.0)
    }
}

impl NodeIdGlob {
    /// Returns true if the given string starts with this glob's prefix
    pub fn matches_str(&self, s: &str) -> bool {
        s.starts_with(&self.0)
    }
}

/// Error returned when a glob pattern is invalid.
#[derive(Debug, thiserror::Error)]
#[error("glob pattern must end with '*', got '{0}'")]
pub struct InvalidGlobError(String);

impl From<InvalidIdError> for InvalidGlobError {
    fn from(e: InvalidIdError) -> Self {
        InvalidGlobError(e.to_string())
    }
}

/// Selector for targeting worker nodes by exact ID or glob pattern
///
/// Used to specify which worker(s) should handle a job deployment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeSelector {
    /// Match a specific worker by exact node ID
    Exact(NodeId),
    /// Match workers whose IDs start with the glob prefix
    Glob(NodeIdGlob),
}

impl serde::Serialize for NodeSelector {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            NodeSelector::Exact(node_id) => node_id.serialize(serializer),
            NodeSelector::Glob(glob) => glob.serialize(serializer),
        }
    }
}

impl<'de> serde::Deserialize<'de> for NodeSelector {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let val = String::deserialize(deserializer)?;
        val.parse().map_err(serde::de::Error::custom)
    }
}

impl std::str::FromStr for NodeSelector {
    type Err = NodeSelectorParseError;

    // If the string ends with '*', parse it as a glob pattern
    // Otherwise, parse it as a node ID
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.ends_with('*') {
            let glob = NodeIdGlob::from_str(s).map_err(NodeSelectorParseError::InvalidGlob)?;
            Ok(NodeSelector::Glob(glob))
        } else {
            let node_id = s
                .parse::<NodeId>()
                .map_err(NodeSelectorParseError::InvalidNodeId)?;
            Ok(NodeSelector::Exact(node_id))
        }
    }
}

/// Errors that occur when parsing a node selector string
#[derive(Debug, thiserror::Error)]
pub enum NodeSelectorParseError {
    /// The string is not a valid node ID
    ///
    /// This occurs when the input doesn't end with `*` and fails node ID validation.
    #[error("invalid node ID")]
    InvalidNodeId(#[source] InvalidIdError),

    /// The glob pattern prefix is invalid
    ///
    /// This occurs when the input ends with `*` but the prefix portion
    /// fails node ID validation rules.
    #[error("invalid glob pattern: '{0}'")]
    InvalidGlob(#[source] InvalidGlobError),
}

#[cfg(test)]
mod tests {
    use super::*;

    mod glob_or_node_id {
        use super::*;

        #[test]
        fn parse_exact_node_id() {
            let result: NodeSelector = "worker-01".parse().unwrap();
            assert!(matches!(result, NodeSelector::Exact(_)));

            if let NodeSelector::Exact(node_id) = result {
                assert_eq!(node_id.as_str(), "worker-01");
            }
        }

        #[test]
        fn parse_glob_with_asterisk() {
            let result: NodeSelector = "worker-*".parse().unwrap();
            assert!(matches!(result, NodeSelector::Glob(_)));
        }

        #[test]
        fn parse_invalid_glob_pattern() {
            let result: Result<NodeSelector, _> = "[invalid".parse();
            assert!(result.is_err());
        }

        #[test]
        fn parse_invalid_node_id() {
            // Starts with number - invalid as NodeId, no glob chars
            let result: Result<NodeSelector, _> = "123-worker".parse();
            assert!(result.is_err());
        }

        #[test]
        fn glob_matches_node_id() {
            let glob: NodeSelector = "worker-raw-eth-*".parse().unwrap();

            if let NodeSelector::Glob(ref g) = glob {
                assert!(g.matches_str("worker-raw-eth-mainnet"));
                assert!(g.matches_str("worker-raw-eth-l2s"));
                assert!(!g.matches_str("worker-other"));
                assert!(!g.matches_str("worker-raw-eth")); // No trailing chars
            } else {
                panic!("Expected Glob variant");
            }
        }

        #[test]
        fn glob_equality() {
            let glob1: NodeSelector = "worker-*".parse().unwrap();
            let glob2: NodeSelector = "worker-*".parse().unwrap();

            assert_eq!(glob1, glob2);
        }

        #[test]
        fn deserialize_exact() {
            let result: NodeSelector = serde_json::from_str(r#""worker-01""#).unwrap();
            assert!(matches!(result, NodeSelector::Exact(_)));
        }

        #[test]
        fn deserialize_glob() {
            let result: NodeSelector = serde_json::from_str(r#""worker-*""#).unwrap();
            assert!(matches!(result, NodeSelector::Glob(_)));
        }
    }
}
