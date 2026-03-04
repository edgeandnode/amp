//! Job status projection operations for the read-optimized `jobs_status` table.
//!
//! This module provides the public API for managing the current-state projection
//! of jobs. The `jobs_status` table maintains one row per job reflecting its latest
//! state, updated transactionally alongside every event appended to `job_events`.
//!
//! State transitions are enforced via conditional updates that validate the current
//! status before applying changes, preventing invalid transitions.

use crate::{Error, db::Executor, jobs::JobId, workers::WorkerNodeId};

pub(crate) mod sql;

/// Register an initial status projection for a job
#[tracing::instrument(skip(exe), err)]
pub async fn register<'c, E>(
    exe: E,
    job_id: impl Into<JobId> + std::fmt::Debug,
    node_id: &WorkerNodeId<'_>,
    status: JobStatus,
) -> Result<(), Error>
where
    E: Executor<'c>,
{
    sql::insert(exe, job_id.into(), node_id, status)
        .await
        .map_err(Error::Database)
}

/// Update job status to StopRequested
///
/// This function will only update the job status if it's currently in a valid state
/// to be stopped (Scheduled or Running). If the job is already stopping, this is
/// considered success (idempotent behavior). If the job is in a terminal state
/// (Stopped, Completed, Failed), this returns a conflict error.
///
/// Returns `Ok(true)` if the status actually changed, `Ok(false)` if already in a
/// stop-related state (idempotent no-op), or an error if the job doesn't exist, is
/// in a terminal state, or if there's a database error.
///
/// **Note:** This function does not send notifications. The caller is responsible for
/// calling `send_job_notification` after successful status update if worker notification
/// is required.
#[tracing::instrument(skip(exe), err)]
pub async fn request_stop<'c, E>(
    exe: E,
    job_id: impl Into<JobId> + std::fmt::Debug,
) -> Result<bool, Error>
where
    E: Executor<'c>,
{
    // Try to update job status
    match sql::update_status_if_any_state(
        exe,
        job_id.into(),
        &[JobStatus::Running, JobStatus::Scheduled],
        JobStatus::StopRequested,
    )
    .await
    {
        Ok(()) => Ok(true),
        // Check if the job is already stopping (idempotent behavior)
        Err(JobStatusUpdateError::StateConflict {
            actual: JobStatus::StopRequested | JobStatus::Stopping,
            ..
        }) => Ok(false),
        Err(err) => Err(Error::JobStatusUpdate(err)),
    }
}

/// Conditionally marks a job as `RUNNING` only if it's currently `SCHEDULED`
///
/// This provides idempotent behavior - if the job is already running, completed, or failed,
/// the appropriate error will be returned indicating the state conflict.
#[tracing::instrument(skip(exe), err)]
pub async fn mark_running<'c, E>(
    exe: E,
    id: impl Into<JobId> + std::fmt::Debug,
) -> Result<(), Error>
where
    E: Executor<'c>,
{
    sql::update_status_if_any_state(exe, id.into(), &[JobStatus::Scheduled], JobStatus::Running)
        .await
        .map_err(Error::JobStatusUpdate)
}

/// Conditionally marks a job as `STOPPING` only if it's currently `STOP_REQUESTED`
///
/// This is typically used by workers to acknowledge a stop request.
#[tracing::instrument(skip(exe), err)]
pub async fn mark_stopping<'c, E>(
    exe: E,
    id: impl Into<JobId> + std::fmt::Debug,
) -> Result<(), Error>
where
    E: Executor<'c>,
{
    sql::update_status_if_any_state(
        exe,
        id.into(),
        &[JobStatus::StopRequested],
        JobStatus::Stopping,
    )
    .await
    .map_err(Error::JobStatusUpdate)
}

/// Conditionally marks a job as `STOPPED` only if it's currently `STOPPING`
///
/// This provides proper state transition from stopping to stopped.
#[tracing::instrument(skip(exe), err)]
pub async fn mark_stopped<'c, E>(
    exe: E,
    id: impl Into<JobId> + std::fmt::Debug,
) -> Result<(), Error>
where
    E: Executor<'c>,
{
    sql::update_status_if_any_state(exe, id.into(), &[JobStatus::Stopping], JobStatus::Stopped)
        .await
        .map_err(Error::JobStatusUpdate)
}

/// Conditionally marks a job as `COMPLETED` only if it's currently `RUNNING`
///
/// This ensures jobs can only be completed from a running state.
#[tracing::instrument(skip(exe), err)]
pub async fn mark_completed<'c, E>(
    exe: E,
    id: impl Into<JobId> + std::fmt::Debug,
) -> Result<(), Error>
where
    E: Executor<'c>,
{
    sql::update_status_if_any_state(exe, id.into(), &[JobStatus::Running], JobStatus::Completed)
        .await
        .map_err(Error::JobStatusUpdate)
}

/// Conditionally marks a job as `ERROR` from either `RUNNING` or `SCHEDULED` states
///
/// This is used for recoverable failures where retry attempts can be made.
///
/// Jobs can fail from either scheduled (startup failure) or running (runtime failure) states.
#[tracing::instrument(skip(exe), err)]
pub async fn mark_failed_recoverable<'c, E>(
    exe: E,
    id: impl Into<JobId> + std::fmt::Debug,
) -> Result<(), Error>
where
    E: Executor<'c>,
{
    sql::update_status_if_any_state(
        exe,
        id.into(),
        &[JobStatus::Scheduled, JobStatus::Running],
        JobStatus::Error,
    )
    .await
    .map_err(Error::JobStatusUpdate)
}

/// Conditionally marks a job as `FATAL` from either `RUNNING` or `SCHEDULED` states
///
/// This is used for unrecoverable failures where retry attempts should not be made.
///
/// Jobs can fail from either scheduled (startup failure) or running (runtime failure) states.
#[tracing::instrument(skip(exe), err)]
pub async fn mark_failed_fatal<'c, E>(
    exe: E,
    id: impl Into<JobId> + std::fmt::Debug,
) -> Result<(), Error>
where
    E: Executor<'c>,
{
    sql::update_status_if_any_state(
        exe,
        id.into(),
        &[JobStatus::Scheduled, JobStatus::Running],
        JobStatus::Fatal,
    )
    .await
    .map_err(Error::JobStatusUpdate)
}

/// Reschedule a failed job for retry
///
/// Updates the job status to SCHEDULED in the `jobs_status` projection and assigns
/// the job to the given worker node.
///
/// The caller is responsible for transaction management (commit/rollback) and for
/// inserting the corresponding event into `job_events`.
///
/// This function does not send notifications. The caller is responsible
/// for sending notifications after successful rescheduling.
#[tracing::instrument(skip(exe), err)]
pub async fn reschedule<'c, E>(
    exe: E,
    job_id: impl Into<JobId> + std::fmt::Debug,
    new_node_id: impl Into<WorkerNodeId<'_>> + std::fmt::Debug,
) -> Result<(), Error>
where
    E: Executor<'c>,
{
    let job_id = job_id.into();
    let new_node_id = new_node_id.into();

    sql::reschedule(exe, job_id, &new_node_id)
        .await
        .map_err(Error::Database)
}

/// Error type for conditional job status updates
#[derive(Debug, thiserror::Error)]
pub enum JobStatusUpdateError {
    /// The job was not found in the `jobs_status` projection
    ///
    /// This occurs when attempting to update a job that does not have a status entry.
    #[error("Job not found")]
    NotFound,

    /// The job exists but its current status does not match any of the expected states
    ///
    /// This occurs when a status transition is attempted but the job is in
    /// an unexpected state (e.g., trying to mark a completed job as running).
    #[error("Job state conflict: expected one of {expected:?}, but found {actual}")]
    StateConflict {
        expected: Vec<JobStatus>,
        actual: JobStatus,
    },

    /// A database error occurred during the status update query
    ///
    /// This occurs when the underlying SQL query fails due to connection issues,
    /// constraint violations, or other database-level errors.
    #[error("Database error during status update")]
    Database(#[source] sqlx::Error),
}

/// Job status enumeration and related implementations
///
/// Represents the current status of a job
///
/// This status is used to track the progress of a job, but it is not
/// guaranteed to be up to date. It is responsibility of the caller to
/// confirm the status of the job before proceeding.
///
/// The status is stored as a `TEXT` column in the database. If the fetched
/// status is not one of the valid values in the enum, the `UNKNOWN` status is
/// returned.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum JobStatus {
    /// Job is being scheduled.
    ///
    /// This is the initial state of a job.
    ///
    /// The scheduler has added the job to the queue, but the job has not
    /// yet been picked up by the worker node.
    #[default]
    Scheduled,

    /// Job is running
    ///
    /// The job has been picked up by the worker node and is being executed.
    Running,

    /// Job has finished successfully
    ///
    /// This is a terminal state.
    Completed,

    /// Job has stopped
    ///
    /// The worker node has stopped the job as requested by the scheduler.
    ///
    /// This is a terminal state.
    Stopped,

    /// Job has been requested to stop
    ///
    /// The scheduler has requested the job to stop. The worker will stop
    /// the job as soon as possible.
    StopRequested,

    /// Job is stopping
    ///
    /// The worker node acknowledged the stop request and will stop the job
    /// as soon as possible.
    Stopping,

    /// Job has failed with a recoverable error
    ///
    /// A recoverable error occurred while running the job. The job may succeed if retried.
    ///
    /// This is a terminal state.
    Error,

    /// Job has failed with a fatal error
    ///
    /// A fatal error occurred while running the job. The job will not succeed if retried.
    ///
    /// This is a terminal state.
    Fatal,

    /// Unknown status
    ///
    /// This is an invalid status, and should never happen. Although
    /// it is possible to happen if the worker node version is different
    /// from the version of the scheduler.
    Unknown,
}

impl JobStatus {
    /// Convert the [`JobStatus`] to a string
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Scheduled => "SCHEDULED",
            Self::Running => "RUNNING",
            Self::Completed => "COMPLETED",
            Self::Stopped => "STOPPED",
            Self::StopRequested => "STOP_REQUESTED",
            Self::Stopping => "STOPPING",
            Self::Error => "ERROR",
            Self::Fatal => "FATAL",
            Self::Unknown => "UNKNOWN",
        }
    }

    /// Returns true if the job status is terminal (cannot be changed further)
    ///
    /// Terminal states are final states where the job lifecycle has ended:
    /// - `Completed`: Job finished successfully
    /// - `Stopped`: Job was stopped by request
    /// - `Failed`: Job encountered an error and failed
    ///
    /// Non-terminal states can still transition to other states
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Stopped | Self::Error | Self::Fatal
        )
    }

    /// Returns an array of all terminal job statuses
    ///
    /// These are the statuses that represent completed job lifecycles
    /// and can be safely deleted from the system.
    pub fn terminal_statuses() -> [JobStatus; 4] {
        [Self::Completed, Self::Stopped, Self::Error, Self::Fatal]
    }

    /// Returns an array of all non-terminal (active) job statuses
    ///
    /// These are the statuses that represent jobs still in progress
    /// and should be monitored or managed.
    pub fn non_terminal_statuses() -> [JobStatus; 4] {
        [
            Self::Scheduled,
            Self::Running,
            Self::StopRequested,
            Self::Stopping,
        ]
    }
}

impl std::str::FromStr for JobStatus {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Use `eq_ignore_ascii_case` to make the comparison case-insensitive
        match s {
            s if s.eq_ignore_ascii_case("SCHEDULED") => Ok(Self::Scheduled),
            s if s.eq_ignore_ascii_case("RUNNING") => Ok(Self::Running),
            s if s.eq_ignore_ascii_case("COMPLETED") => Ok(Self::Completed),
            s if s.eq_ignore_ascii_case("STOPPED") => Ok(Self::Stopped),
            s if s.eq_ignore_ascii_case("STOP_REQUESTED") => Ok(Self::StopRequested),
            s if s.eq_ignore_ascii_case("STOPPING") => Ok(Self::Stopping),
            s if s.eq_ignore_ascii_case("ERROR") => Ok(Self::Error),
            s if s.eq_ignore_ascii_case("FATAL") => Ok(Self::Fatal),
            _ => Ok(Self::Unknown), // Default to Unknown for Infallible
        }
    }
}

impl std::fmt::Display for JobStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl sqlx::Type<sqlx::Postgres> for JobStatus {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        sqlx::postgres::PgTypeInfo::with_name("TEXT")
    }
}

impl sqlx::postgres::PgHasArrayType for JobStatus {
    fn array_type_info() -> sqlx::postgres::PgTypeInfo {
        sqlx::postgres::PgTypeInfo::with_name("TEXT[]")
    }
}

impl<'r> sqlx::Decode<'r, sqlx::Postgres> for JobStatus {
    fn decode(
        value: <sqlx::Postgres as sqlx::Database>::ValueRef<'r>,
    ) -> Result<Self, sqlx::error::BoxDynError> {
        let value: &str = sqlx::Decode::<sqlx::Postgres>::decode(value)?;
        // Since FromStr::Err is Infallible, unwrap is safe.
        Ok(value.parse().unwrap())
    }
}

impl<'q> sqlx::Encode<'q, sqlx::Postgres> for JobStatus {
    fn encode_by_ref(
        &self,
        buf: &mut <sqlx::Postgres as sqlx::Database>::ArgumentBuffer<'q>,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        sqlx::Encode::<sqlx::Postgres>::encode_by_ref(&self.as_str(), buf)
    }
}

/// In-tree integration tests
#[cfg(test)]
mod tests {
    mod it_job_status;
}
