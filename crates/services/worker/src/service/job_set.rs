use std::{
    collections::{BTreeMap, HashMap},
    future::Future,
};

use amp_worker_core::{RematerializeRequest, jobs::job_id::JobId, retryable::RetryableErrorExt};
use tokio::{
    sync::mpsc,
    task::{AbortHandle, Id as TaskId, JoinError as TokioJoinError, JoinSet},
};

use super::job_impl::JobError;

/// A collection of jobs that are spawned and managed by a [`Worker`].
///
/// The [`JobSet`] is an abstraction over the [`JoinSet`] type that translates
/// the task IDs returned by the [`JoinSet::join_next_with_id`] method into job
/// IDs.
///
/// On drop, all jobs are aborted.
///
/// [`Worker`]: crate::worker::Worker
#[derive(Debug, Default)]
pub struct JobSet {
    /// The mapping of job IDs to their abort handles.
    job_id_to_handle: BTreeMap<JobId, AbortHandle>,
    /// The mapping of task IDs to their job IDs.
    // NOTE: The task ID does not implement `Ord`, this is a tokio limitation.
    task_id_to_job_id: HashMap<TaskId, JobId>,

    /// The set of jobs that are spawned and managed by the worker.
    jobs: JoinSet<Result<(), JobError>>,

    /// Channel senders for sending rematerialize requests to running jobs.
    rematerialize_senders: BTreeMap<JobId, mpsc::Sender<RematerializeRequest>>,
}

impl JobSet {
    pub(super) fn job_running(&self, job_id: &JobId) -> bool {
        self.job_id_to_handle.contains_key(job_id)
    }

    /// Create a channel for a job that will be spawned
    ///
    /// Returns a receiver for rematerialize requests that should be passed to the job future.
    /// The sender is stored internally and will be used when the job is spawned.
    pub(super) fn create_job_channel(
        &mut self,
        job_id: JobId,
    ) -> mpsc::Receiver<RematerializeRequest> {
        // Create a channel for rematerialize requests (bounded to apply backpressure)
        let (tx, rx) = mpsc::channel(100);

        // Store the sender - will be linked when spawn_with_existing_channel is called
        let old_sender = self.rematerialize_senders.insert(job_id, tx);

        debug_assert!(
            old_sender.is_none(),
            "Channel for job #{job_id} already exists"
        );

        rx
    }

    /// Spawn a job that already has a channel created via `create_job_channel`
    ///
    /// The receiver from `create_job_channel` should be passed to the job future.
    pub(super) fn spawn_with_existing_channel(
        &mut self,
        job_id: JobId,
        job_fut: impl Future<Output = Result<(), JobError>> + Send + 'static,
    ) {
        debug_assert!(
            self.rematerialize_senders.contains_key(&job_id),
            "Channel for job #{job_id} must be created before spawning"
        );

        let handle = self.jobs.spawn(job_fut);

        // Register the IDs and the handle
        let old_task_id = self.task_id_to_job_id.insert(handle.id(), job_id);
        let old_job_id = self.job_id_to_handle.insert(job_id, handle);

        debug_assert!(
            old_task_id.is_none() && old_job_id.is_none(),
            "Job #{job_id} is already tracked by the set"
        );
    }

    /// Send a rematerialize request to a running job
    ///
    /// Returns `Ok(())` if the request was sent successfully, or an error if:
    /// - The job is not running
    /// - The channel is closed (job has finished or crashed)
    /// - The channel is full (job is not processing requests)
    pub(super) async fn send_rematerialize(
        &self,
        job_id: &JobId,
        request: RematerializeRequest,
    ) -> Result<(), SendRematerializeError> {
        let sender = self
            .rematerialize_senders
            .get(job_id)
            .ok_or(SendRematerializeError::JobNotFound)?;

        sender
            .send(request)
            .await
            .map_err(|_| SendRematerializeError::ChannelClosed)
    }

    /// Abort a job by its ID
    pub fn abort(&mut self, job_id: JobId) {
        let Some(handle) = self.job_id_to_handle.get(&job_id) else {
            tracing::debug!(%job_id, "Job not found, skipping.");
            return;
        };

        handle.abort();
    }

    /// Waits until one of the tasks in the set completes and returns its output,
    /// along with the [`JobId`] of the completed job.
    ///
    /// Returns the [`JobId`] and the result of the job:
    ///
    /// - If the job returned an error, the result is an [`JoinError::Failed`]
    ///   error.
    /// - If the job was aborted, the result is an [`JoinError::Aborted`] error.
    /// - If the job panicked, the result is an [`JoinError::Panicked`] error.
    /// - Otherwise, the result is `Ok(())`.
    ///
    /// Returns `None` if the set is empty.
    pub async fn join_next_with_id(&mut self) -> Option<(JobId, Result<(), JoinError>)> {
        let result = self.jobs.join_next_with_id().await?;
        let (task_id, result) = match result {
            Ok((task_id, result)) => (task_id, Ok(result)),
            Err(err) => (err.id(), Err(err)),
        };

        // Remove the job from the set tables
        let job_id = self
            .task_id_to_job_id
            .remove(&task_id)
            .unwrap_or_else(|| panic!("Task ID {task_id} is not tracked by the set"));
        let _handle = self
            .job_id_to_handle
            .remove(&job_id)
            .unwrap_or_else(|| panic!("Job ID {job_id} is not tracked by the set"));
        let _sender = self.rematerialize_senders.remove(&job_id);

        let next = match result {
            // The job completed successfully
            Ok(Ok(())) => (job_id, Ok(())),
            // The job returned an error
            Ok(Err(err)) => {
                let join_err = if err.is_retryable() {
                    JoinError::FailedRecoverable(Box::new(err))
                } else {
                    JoinError::FailedFatal(Box::new(err))
                };
                (job_id, Err(join_err))
            }
            // The job was aborted
            Err(err) if err.is_cancelled() => (job_id, Err(JoinError::Aborted)),
            // The job panicked
            Err(err) => (job_id, Err(JoinError::Panicked(err))),
        };

        Some(next)
    }
}

/// The error type for the [`JobSet::join_next`] method
#[derive(Debug, thiserror::Error)]
pub enum JoinError {
    /// The job failed with a recoverable error
    #[error("Job failed with a recoverable error: {0}")]
    FailedRecoverable(Box<JobError>),
    /// The job failed with a fatal error
    #[error("Job failed with a fatal error: {0}")]
    FailedFatal(Box<JobError>),
    /// The job was aborted
    #[error("Job was aborted")]
    Aborted,
    /// The job panicked
    #[error("Job panicked: {0}")]
    Panicked(TokioJoinError),
}

/// Error type for sending rematerialize requests
#[derive(Debug, thiserror::Error)]
pub enum SendRematerializeError {
    /// The job is not running
    #[error("job not found")]
    JobNotFound,
    /// The channel is closed (job has finished)
    #[error("channel closed")]
    ChannelClosed,
}
