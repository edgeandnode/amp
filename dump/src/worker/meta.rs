//! This module provides a `WorkerMetadataDb` struct, a newtype wrapper around `Arc<MetadataDb>`
//! that exposes methods for interacting with the workers and jobs tables in the metadata DB.
//!
//! This wrapper is intended to be used by the dump worker to abstract away the direct
//! database interactions, improving modularity and clarifying the API surface required by the
//! worker.
//!
//! All the methods in this module are retryable on connection errors.
use std::{future::Future, sync::Arc};

use backon::{ExponentialBuilder, Retryable};
use metadata_db::{Error as MetadataDbError, JobNotifListener, MetadataDb};
pub use metadata_db::{
    Job as JobMeta, JobId, JobNotifAction as NotifAction, JobNotifRecvError as NotifRecvError,
    JobStatus, WorkerNodeId,
};

/// A newtype wrapper around `Arc<MetadataDb>` that provides methods for interacting with the
/// jobs and workers tables in the database.
#[derive(Clone)]
pub struct WorkerMetadataDb(Arc<MetadataDb>);

impl WorkerMetadataDb {
    /// Creates a new `JobsMetadataDb` instance.
    pub fn new(db: Arc<MetadataDb>) -> Self {
        Self(db)
    }

    /// Registers a worker in the metadata DB.
    ///
    /// This operation is idempotent. Retries on connection errors.
    pub async fn register_worker(&self, node_id: &WorkerNodeId) -> Result<(), MetadataDbError> {
        (|| self.0.register_worker(node_id))
            .retry(retry_policy())
            .when(|err| err.is_connection_error())
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

    /// Establishes a connection to the metadata DB and returns a future that runs the worker
    /// heartbeat loop.
    ///
    /// If the initial connection fails, the method will retry the reconnection with an exponential
    /// backoff. Retries on connection errors.
    pub async fn worker_heartbeat_loop(
        &self,
        node_id: &WorkerNodeId,
    ) -> Result<impl Future<Output = Result<(), MetadataDbError>> + use<>, MetadataDbError> {
        (|| self.0.worker_heartbeat_loop(node_id.clone()))
            .retry(retry_policy())
            .when(|err| err.is_connection_error())
            .notify(|err, dur| {
                tracing::warn!(
                    node_id = %node_id,
                    error = %err,
                    "Worker heartbeat connection establishment failed. Retrying in {:.1}s",
                    dur.as_secs_f32()
                );
            })
            .await
    }

    /// Listens for job notifications from the metadata DB.
    ///
    /// This method establishes a connection to the metadata DB and returns a future that runs the
    /// job notification listener loop. If the initial connection fails, the method will retry with
    /// an exponential backoff. Retries on connection errors.
    pub async fn listen_for_job_notifications(&self) -> Result<JobNotifListener, MetadataDbError> {
        (|| self.0.listen_for_job_notifications())
            .retry(retry_policy())
            .when(|err| err.is_connection_error())
            .notify(|err, dur| {
                tracing::warn!(
                    error = %err,
                    "Failed to establish connection to listen for job notifications. Retrying in {:.1}s",
                    dur.as_secs_f32()
                );
            })
            .await
    }

    /// Fetches all jobs with [`JobStatus::Scheduled`] or [`JobStatus::Running`] status from the
    /// metadata DB.
    ///
    /// This is used by the worker to recover its state after a restart. Retries on connection
    /// errors.
    pub async fn get_scheduled_jobs_with_details(
        &self,
        node_id: &WorkerNodeId,
    ) -> Result<Vec<JobMeta>, MetadataDbError> {
        (|| self.0.get_scheduled_jobs_with_details(node_id))
            .retry(retry_policy())
            .when(|err| err.is_connection_error())
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

    /// Fetches all active jobs from the metadata DB.
    ///
    /// Active jobs are those with [`JobStatus::Scheduled`], [`JobStatus::Running`], or
    /// [`JobStatus::StopRequested`] status.
    ///
    /// This is used by the worker's reconciliation loop to synchronize its state with the metadata
    /// DB state. Retries on connection errors.
    pub async fn get_active_jobs_with_details(
        &self,
        node_id: &WorkerNodeId,
    ) -> Result<Vec<JobMeta>, MetadataDbError> {
        (|| self.0.get_active_jobs_with_details(node_id))
            .retry(retry_policy())
            .when(|err| err.is_connection_error())
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

    /// Fetches a job by its ID from the metadata DB.
    ///
    /// Retries on connection errors.
    pub async fn get_job(&self, job_id: &JobId) -> Result<Option<JobMeta>, MetadataDbError> {
        (|| self.0.get_job(job_id))
            .retry(retry_policy())
            .when(|err| err.is_connection_error())
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

    /// Marks a job as [`JobStatus::Running`].
    ///
    /// Retries on connection errors.
    pub async fn mark_job_running(&self, job_id: &JobId) -> Result<(), MetadataDbError> {
        (|| self.0.mark_job_running(job_id))
            .retry(retry_policy())
            .when(|err| err.is_connection_error())
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

    /// Marks a job as [`JobStatus::Stopping`].
    ///
    /// Retries on connection errors.
    pub async fn mark_job_stopping(&self, job_id: &JobId) -> Result<(), MetadataDbError> {
        // Mark the job as STOPPING. Retry on failure
        (|| self.0.mark_job_stopping(job_id))
            .retry(retry_policy())
            .when(|err| err.is_connection_error())
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

    /// Marks a job as [`JobStatus::Stopped`].
    ///
    /// Retries on connection errors.
    pub async fn mark_job_stopped(&self, job_id: &JobId) -> Result<(), MetadataDbError> {
        (|| self.0.mark_job_stopped(job_id))
            .retry(retry_policy())
            .when(|err| err.is_connection_error())
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

    /// Marks a job as [`JobStatus::Completed`].
    ///
    /// Retries on connection errors.
    pub async fn mark_job_completed(&self, job_id: &JobId) -> Result<(), MetadataDbError> {
        (|| self.0.mark_job_completed(job_id))
            .retry(retry_policy())
            .when(|err| err.is_connection_error())
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

    /// Marks a job as [`JobStatus::Failed`].
    ///
    /// Retries on connection errors.
    pub async fn mark_job_failed(&self, job_id: &JobId) -> Result<(), MetadataDbError> {
        (|| self.0.mark_job_failed(job_id))
            .retry(retry_policy())
            .when(|err| err.is_connection_error())
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
/// - min_delay: 1s
/// - max_delay: 60s
/// - max_times: 3
#[inline]
fn retry_policy() -> ExponentialBuilder {
    ExponentialBuilder::default()
}
