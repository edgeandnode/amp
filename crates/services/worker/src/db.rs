//! This module provides an extension trait for `MetadataDb` with retry logic.
//!
//! The extension trait adds methods for interacting with the workers and jobs tables,
//! with automatic retries on connection errors.

use backon::{ExponentialBuilder, Retryable};
use futures::future::BoxFuture;
use metadata_db::{Error as MetadataDbError, MetadataDb, WorkerNotifListener as NotifListener};
pub use metadata_db::{Job as JobMeta, JobStatus};

use crate::{JobId, NodeId, WorkerInfo};

/// Extension trait for `MetadataDb` that adds retry logic to database operations.
///
/// This trait is private to the worker crate and provides methods with automatic
/// retry logic on connection errors using an exponential backoff strategy.
///
/// All methods in this trait are retryable on connection errors.
pub(crate) trait MetadataDbRetryExt {
    /// Registers a worker in the metadata DB.
    ///
    /// This operation is idempotent. Retries on connection errors.
    async fn register_worker_with_retry(
        &self,
        node_id: &NodeId,
        info: &WorkerInfo,
    ) -> Result<(), MetadataDbError>;

    /// Establishes a connection to the metadata DB and returns a future that runs the worker
    /// heartbeat loop.
    ///
    /// If the initial connection fails, the method will retry the reconnection with an exponential
    /// backoff. Retries on connection errors.
    async fn worker_heartbeat_loop_with_retry(
        &self,
        node_id: NodeId,
    ) -> Result<BoxFuture<'static, Result<(), MetadataDbError>>, MetadataDbError>;

    /// Listens for job notifications from the metadata DB.
    ///
    /// This method establishes a connection to the metadata DB and returns a future that runs the
    /// job notification listener loop. The listener will only yield notifications targeted to the
    /// specified `node_id`. If the initial connection fails, the method will retry with an
    /// exponential backoff. Retries on connection errors.
    async fn listen_for_job_notifications_with_retry(
        &self,
        node_id: &NodeId,
    ) -> Result<NotifListener, MetadataDbError>;

    /// Fetches all jobs with [`JobStatus::Scheduled`] or [`JobStatus::Running`] status from the
    /// metadata DB.
    ///
    /// This is used by the worker to recover its state after a restart. Retries on connection
    /// errors.
    async fn get_scheduled_jobs_with_retry(
        &self,
        node_id: &NodeId,
    ) -> Result<Vec<JobMeta>, MetadataDbError>;

    /// Fetches all active jobs from the metadata DB.
    ///
    /// Active jobs are those with [`JobStatus::Scheduled`], [`JobStatus::Running`], or
    /// [`JobStatus::StopRequested`] status.
    ///
    /// This is used by the worker's reconciliation loop to synchronize its state with the metadata
    /// DB state. Retries on connection errors.
    async fn get_active_jobs_with_retry(
        &self,
        node_id: NodeId,
    ) -> Result<Vec<JobMeta>, MetadataDbError>;

    /// Fetches a job by its ID from the metadata DB.
    ///
    /// Retries on connection errors.
    async fn get_job_with_retry(&self, job_id: &JobId) -> Result<Option<JobMeta>, MetadataDbError>;

    /// Marks a job as [`JobStatus::Running`].
    ///
    /// Retries on connection errors.
    async fn mark_job_running_with_retry(&self, job_id: &JobId) -> Result<(), MetadataDbError>;

    /// Marks a job as [`JobStatus::Stopping`].
    ///
    /// Retries on connection errors.
    async fn mark_job_stopping_with_retry(&self, job_id: &JobId) -> Result<(), MetadataDbError>;

    /// Marks a job as [`JobStatus::Stopped`].
    ///
    /// Retries on connection errors.
    async fn mark_job_stopped_with_retry(&self, job_id: &JobId) -> Result<(), MetadataDbError>;

    /// Marks a job as [`JobStatus::Completed`].
    ///
    /// Retries on connection errors.
    async fn mark_job_completed_with_retry(&self, job_id: &JobId) -> Result<(), MetadataDbError>;

    /// Marks a job as [`JobStatus::Failed`].
    ///
    /// Retries on connection errors.
    async fn mark_job_failed_with_retry(&self, job_id: &JobId) -> Result<(), MetadataDbError>;
}

impl MetadataDbRetryExt for MetadataDb {
    async fn register_worker_with_retry(
        &self,
        node_id: &NodeId,
        info: &WorkerInfo,
    ) -> Result<(), MetadataDbError> {
        (|| self.register_worker(node_id, info))
            .retry(retry_policy())
            .when(MetadataDbError::is_connection_error)
            .notify(|err, dur| {
                tracing::warn!(
                    node_id = %node_id,
                    error = %err,
                    "Connection error while registering worker. Retrying in {:.1}s",
                    dur.as_secs_f32()
                );
            })
            .await
    }

    async fn worker_heartbeat_loop_with_retry(
        &self,
        node_id: NodeId,
    ) -> Result<BoxFuture<'static, Result<(), MetadataDbError>>, MetadataDbError> {
        let node_id_str = node_id.to_string();
        (move || self.worker_heartbeat_loop(node_id.clone()))
            .retry(retry_policy())
            .when(MetadataDbError::is_connection_error)
            .notify(|err, dur| {
                tracing::warn!(
                    node_id = %node_id_str,
                    error = %err,
                    "Worker heartbeat connection establishment failed. Retrying in {:.1}s",
                    dur.as_secs_f32()
                );
            })
            .await
    }

    async fn listen_for_job_notifications_with_retry(
        &self,
        node_id: &NodeId,
    ) -> Result<NotifListener, MetadataDbError> {
        (|| self.listen_for_job_notifications(node_id))
            .retry(retry_policy())
            .when(MetadataDbError::is_connection_error)
            .notify(|err, dur| {
                tracing::warn!(
                    node_id = %node_id,
                    error = %err,
                    "Failed to establish connection to listen for job notifications. Retrying in {:.1}s",
                    dur.as_secs_f32()
                );
            })
            .await
    }

    async fn get_scheduled_jobs_with_retry(
        &self,
        node_id: &NodeId,
    ) -> Result<Vec<JobMeta>, MetadataDbError> {
        (|| metadata_db::jobs::get_scheduled(self, node_id))
            .retry(retry_policy())
            .when(MetadataDbError::is_connection_error)
            .notify(|err, dur| {
                tracing::warn!(
                    node_id = %node_id,
                    error = %err,
                    "Connection error while getting scheduled jobs. Retrying in {:.1}s",
                    dur.as_secs_f32()
                );
            })
            .await
    }

    async fn get_active_jobs_with_retry(
        &self,
        node_id: NodeId,
    ) -> Result<Vec<JobMeta>, MetadataDbError> {
        (|| metadata_db::jobs::get_active(self, node_id.clone()))
            .retry(retry_policy())
            .when(MetadataDbError::is_connection_error)
            .notify(|err, dur| {
                tracing::warn!(
                    node_id = %node_id,
                    error = %err,
                    "Connection error while getting active jobs. Retrying in {:.1}s",
                    dur.as_secs_f32()
                );
            })
            .await
    }

    async fn get_job_with_retry(&self, job_id: &JobId) -> Result<Option<JobMeta>, MetadataDbError> {
        (|| metadata_db::jobs::get_by_id(self, job_id))
            .retry(retry_policy())
            .when(MetadataDbError::is_connection_error)
            .notify(|err, dur| {
                tracing::warn!(
                    job_id = %job_id,
                    error = %err,
                    "Connection error while getting job. Retrying in {:.1}s",
                    dur.as_secs_f32()
                );
            })
            .await
    }

    async fn mark_job_running_with_retry(&self, job_id: &JobId) -> Result<(), MetadataDbError> {
        (|| metadata_db::jobs::mark_running(self, job_id))
            .retry(retry_policy())
            .when(MetadataDbError::is_connection_error)
            .notify(|err, dur| {
                tracing::warn!(
                    job_id = %job_id,
                    error = %err,
                    "Connection error while marking job as running. Retrying in {:.1}s",
                    dur.as_secs_f32()
                );
            })
            .await
    }

    async fn mark_job_stopping_with_retry(&self, job_id: &JobId) -> Result<(), MetadataDbError> {
        (|| metadata_db::jobs::mark_stopping(self, job_id))
            .retry(retry_policy())
            .when(MetadataDbError::is_connection_error)
            .notify(|err, dur| {
                tracing::warn!(
                    job_id = %job_id,
                    error = %err,
                    "Connection error while marking job as stopping. Retrying in {:.1}s",
                    dur.as_secs_f32()
                );
            })
            .await
    }

    async fn mark_job_stopped_with_retry(&self, job_id: &JobId) -> Result<(), MetadataDbError> {
        (|| metadata_db::jobs::mark_stopped(self, job_id))
            .retry(retry_policy())
            .when(MetadataDbError::is_connection_error)
            .notify(|err, dur| {
                tracing::warn!(
                    job_id = %job_id,
                    error = %err,
                    "Connection error while marking job as stopped. Retrying in {:.1}s",
                    dur.as_secs_f32()
                );
            })
            .await
    }

    async fn mark_job_completed_with_retry(&self, job_id: &JobId) -> Result<(), MetadataDbError> {
        (|| metadata_db::jobs::mark_completed(self, job_id))
            .retry(retry_policy())
            .when(MetadataDbError::is_connection_error)
            .notify(|err, dur| {
                tracing::warn!(
                    job_id = %job_id,
                    error = %err,
                    "Connection error while marking job as completed. Retrying in {:.1}s",
                    dur.as_secs_f32()
                );
            })
            .await
    }

    async fn mark_job_failed_with_retry(&self, job_id: &JobId) -> Result<(), MetadataDbError> {
        (|| metadata_db::jobs::mark_failed(self, job_id))
            .retry(retry_policy())
            .when(MetadataDbError::is_connection_error)
            .notify(|err, dur| {
                tracing::warn!(
                    job_id = %job_id,
                    error = %err,
                    "Connection error while marking job as failed. Retrying in {:.1}s",
                    dur.as_secs_f32()
                );
            })
            .await
    }
}

/// A retry policy for the worker metadata DB operations.
///
/// The retry policy is an exponential backoff with:
/// - jitter: false
/// - factor: 2
/// - `min_delay`: 1s
/// - `max_delay`: 60s
/// - `max_times`: 3
#[inline]
fn retry_policy() -> ExponentialBuilder {
    ExponentialBuilder::default()
}
