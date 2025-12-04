//! Job queue abstraction for the Worker service.
//!
//! This module provides a `JobQueue` wrapper around `MetadataDb` that encapsulates
//! all job queue operations. The Worker service interacts with the queue through this
//! abstraction rather than directly accessing the metadata database.

use backon::{ExponentialBuilder, Retryable};
use metadata_db::{Error as MetadataDbError, MetadataDb};
use monitoring::logging;

use crate::{
    job::{Job, JobId},
    node_id::NodeId,
};

/// A job queue abstraction that wraps `MetadataDb` operations.
///
/// The `JobQueue` provides a clean interface for Worker service to interact
/// with the job queue without directly depending on metadata database implementation
/// details. All operations include automatic retry logic on connection errors.
#[derive(Clone, Debug)]
pub(crate) struct JobQueue {
    metadata_db: MetadataDb,
}

impl JobQueue {
    /// Creates a new `JobQueue` instance wrapping the provided metadata database.
    #[must_use]
    pub fn new(metadata_db: MetadataDb) -> Self {
        Self { metadata_db }
    }

    /// Gets the current retry index for a job from the job_attempts table
    ///
    /// Returns the maximum retry_index for the job, or 0 if no attempts exist.
    /// This is used when marking job attempts as completed.
    ///
    /// # Errors
    ///
    /// Returns an error if the database query fails.
    async fn get_current_retry_index(&self, job_id: JobId) -> Result<i32, MetadataDbError> {
        let attempts =
            metadata_db::job_attempts::get_attempts_for_job(&self.metadata_db, job_id).await?;

        Ok(attempts
            .iter()
            .map(|attempt| attempt.retry_index)
            .max()
            .unwrap_or(0))
    }

    /// Fetches all jobs with `Scheduled` or `Running` status for the given worker node.
    ///
    /// This is used during worker bootstrap to recover state after a restart.
    /// Includes automatic retry logic on connection errors.
    ///
    /// # Errors
    ///
    /// Returns an error if the database query fails after retries.
    pub async fn get_scheduled_jobs(&self, node_id: &NodeId) -> Result<Vec<Job>, MetadataDbError> {
        let jobs = (|| metadata_db::jobs::get_scheduled(&self.metadata_db, node_id))
            .retry(with_policy())
            .when(MetadataDbError::is_connection_error)
            .notify(|err, dur| {
                tracing::warn!(
                    node_id = %node_id,
                    error = %err, error_source = logging::error_source(&err),
                    "Connection error while getting scheduled jobs. Retrying in {:.1}s",
                    dur.as_secs_f32()
                );
            })
            .await?
            .into_iter()
            .map(Into::into)
            .collect();
        Ok(jobs)
    }

    /// Fetches all active jobs for the given worker node.
    ///
    /// Active jobs include those with `Scheduled`, `Running`, or `StopRequested` status.
    /// This is used by the worker's reconciliation loop to synchronize state with the
    /// metadata database. Includes automatic retry logic on connection errors.
    ///
    /// # Errors
    ///
    /// Returns an error if the database query fails after retries.
    pub async fn get_active_jobs(&self, node_id: &NodeId) -> Result<Vec<Job>, MetadataDbError> {
        let jobs = (|| metadata_db::jobs::get_active(&self.metadata_db, node_id))
            .retry(with_policy())
            .when(MetadataDbError::is_connection_error)
            .notify(|err, dur| {
                tracing::warn!(
                    node_id = %node_id,
                    error = %err, error_source = logging::error_source(&err),
                    "Connection error while getting active jobs. Retrying in {:.1}s",
                    dur.as_secs_f32()
                );
            })
            .await?
            .into_iter()
            .map(Into::into)
            .collect();
        Ok(jobs)
    }

    /// Fetches a job by its ID.
    ///
    /// Returns `None` if the job doesn't exist. Includes automatic retry logic
    /// on connection errors.
    ///
    /// # Errors
    ///
    /// Returns an error if the database query fails after retries.
    pub async fn get_job(&self, job_id: JobId) -> Result<Option<Job>, MetadataDbError> {
        let job = (|| metadata_db::jobs::get_by_id(&self.metadata_db, job_id))
            .retry(with_policy())
            .when(MetadataDbError::is_connection_error)
            .notify(|err, dur| {
                tracing::warn!(
                    job_id = %job_id,
                    error = %err, error_source = logging::error_source(&err),
                    "Connection error while getting job. Retrying in {:.1}s",
                    dur.as_secs_f32()
                );
            })
            .await?
            .map(Into::into);
        Ok(job)
    }

    /// Marks a job as `Running`.
    ///
    /// This is called when the worker begins executing a job. Includes automatic
    /// retry logic on connection errors.
    ///
    /// # Errors
    ///
    /// Returns an error if the database update fails after retries.
    pub async fn mark_job_running(&self, job_id: JobId) -> Result<(), MetadataDbError> {
        (|| metadata_db::jobs::mark_running(&self.metadata_db, job_id))
            .retry(with_policy())
            .when(MetadataDbError::is_connection_error)
            .notify(|err, dur| {
                tracing::warn!(
                    job_id = %job_id,
                    error = %err, error_source = logging::error_source(&err),
                    "Connection error while marking job as running. Retrying in {:.1}s",
                    dur.as_secs_f32()
                );
            })
            .await
    }

    /// Marks a job as `Stopping`.
    ///
    /// This is called when the worker begins the process of stopping a job.
    /// Includes automatic retry logic on connection errors.
    ///
    /// # Errors
    ///
    /// Returns an error if the database update fails after retries.
    pub async fn mark_job_stopping(&self, job_id: JobId) -> Result<(), MetadataDbError> {
        (|| metadata_db::jobs::mark_stopping(&self.metadata_db, job_id))
            .retry(with_policy())
            .when(MetadataDbError::is_connection_error)
            .notify(|err, dur| {
                tracing::warn!(
                    job_id = %job_id,
                    error = %err, error_source = logging::error_source(&err),
                    "Connection error while marking job as stopping. Retrying in {:.1}s",
                    dur.as_secs_f32()
                );
            })
            .await
    }

    /// Marks a job as `Stopped`.
    ///
    /// This is called when a job has been successfully stopped/aborted.
    /// Includes automatic retry logic on connection errors.
    ///
    /// # Errors
    ///
    /// Returns an error if the database update fails after retries.
    pub async fn mark_job_stopped(&self, job_id: JobId) -> Result<(), MetadataDbError> {
        // Get current retry index before marking stopped
        let retry_index = (|| async { self.get_current_retry_index(job_id).await })
            .retry(with_policy())
            .when(MetadataDbError::is_connection_error)
            .notify(|err, dur| {
                tracing::warn!(
                    job_id = %job_id,
                    error = %err, error_source = logging::error_source(&err),
                    "Connection error while getting retry index. Retrying in {:.1}s",
                    dur.as_secs_f32()
                );
            })
            .await?;

        (|| metadata_db::jobs::mark_stopped(&self.metadata_db, job_id))
            .retry(with_policy())
            .when(MetadataDbError::is_connection_error)
            .notify(|err, dur| {
                tracing::warn!(
                    job_id = %job_id,
                    error = %err, error_source = logging::error_source(&err),
                    "Connection error while marking job as stopped. Retrying in {:.1}s",
                    dur.as_secs_f32()
                );
            })
            .await?;

        // Mark attempt as completed
        metadata_db::job_attempts::mark_attempt_completed(&self.metadata_db, job_id, retry_index)
            .await?;

        Ok(())
    }

    /// Marks a job as `Completed`.
    ///
    /// This is called when a job finishes successfully. Includes automatic
    /// retry logic on connection errors.
    ///
    /// # Errors
    ///
    /// Returns an error if the database update fails after retries.
    pub async fn mark_job_completed(&self, job_id: JobId) -> Result<(), MetadataDbError> {
        // Get current retry index before marking completed
        let retry_index = (|| async { self.get_current_retry_index(job_id).await })
            .retry(with_policy())
            .when(MetadataDbError::is_connection_error)
            .notify(|err, dur| {
                tracing::warn!(
                    job_id = %job_id,
                    error = %err, error_source = logging::error_source(&err),
                    "Connection error while getting retry index. Retrying in {:.1}s",
                    dur.as_secs_f32()
                );
            })
            .await?;

        (|| metadata_db::jobs::mark_completed(&self.metadata_db, job_id))
            .retry(with_policy())
            .when(MetadataDbError::is_connection_error)
            .notify(|err, dur| {
                tracing::warn!(
                    job_id = %job_id,
                    error = %err, error_source = logging::error_source(&err),
                    "Connection error while marking job as completed. Retrying in {:.1}s",
                    dur.as_secs_f32()
                );
            })
            .await?;

        // Mark attempt as completed
        metadata_db::job_attempts::mark_attempt_completed(&self.metadata_db, job_id, retry_index)
            .await?;

        Ok(())
    }

    /// Marks a job as `Failed`.
    ///
    /// This is called when a job encounters an error during execution.
    /// Includes automatic retry logic on connection errors.
    ///
    /// # Errors
    ///
    /// Returns an error if the database update fails after retries.
    pub async fn mark_job_failed(&self, job_id: JobId) -> Result<(), MetadataDbError> {
        // Get current retry index before marking failed
        let retry_index = (|| async { self.get_current_retry_index(job_id).await })
            .retry(with_policy())
            .when(MetadataDbError::is_connection_error)
            .notify(|err, dur| {
                tracing::warn!(
                    job_id = %job_id,
                    error = %err, error_source = logging::error_source(&err),
                    "Connection error while getting retry index. Retrying in {:.1}s",
                    dur.as_secs_f32()
                );
            })
            .await?;

        (|| metadata_db::jobs::mark_failed(&self.metadata_db, job_id))
            .retry(with_policy())
            .when(MetadataDbError::is_connection_error)
            .notify(|err, dur| {
                tracing::warn!(
                    job_id = %job_id,
                    error = %err, error_source = logging::error_source(&err),
                    "Connection error while marking job as failed. Retrying in {:.1}s",
                    dur.as_secs_f32()
                );
            })
            .await?;

        // Mark attempt as completed
        metadata_db::job_attempts::mark_attempt_completed(&self.metadata_db, job_id, retry_index)
            .await?;

        Ok(())
    }
}

/// A retry policy for the worker queue operations.
///
/// The retry policy is an exponential backoff with:
/// - jitter: false
/// - factor: 2
/// - `min_delay`: 1s
/// - `max_delay`: 60s
/// - `max_times`: 3
#[inline]
fn with_policy() -> ExponentialBuilder {
    ExponentialBuilder::default()
}
