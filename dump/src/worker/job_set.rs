use std::collections::{BTreeMap, HashMap};

use common::BoxError;
use metadata_db::JobId;
use tokio::task::{AbortHandle, Id as TaskId, JoinSet};

use super::job::Job;

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
    jobs: JoinSet<Result<(), BoxError>>,
}

impl JobSet {
    /// Create a new [`JobSet`] instance
    pub fn new() -> Self {
        Default::default()
    }

    /// Returns `true` if a job with the given ID is tracked by the set
    pub fn contains(&self, job_id: &JobId) -> bool {
        self.job_id_to_handle.contains_key(job_id)
    }

    /// Spawn a new job and register it in the set
    pub fn spawn(&mut self, job_id: JobId, job: Job) {
        if self.job_id_to_handle.contains_key(&job_id) {
            tracing::warn!(%job_id, "Job already spawned, skipping.");
            return;
        }

        let handle = self.jobs.spawn(job.run());

        // Register the IDs and the handle
        let old_task_id = self.task_id_to_job_id.insert(handle.id(), job_id);
        let old_job_id = self.job_id_to_handle.insert(job_id, handle);

        debug_assert!(
            old_task_id.is_none() && old_job_id.is_none(),
            "Job #{job_id} is already tracked by the set"
        );
    }

    /// Abort a job by its ID
    pub fn abort(&mut self, job_id: &JobId) {
        let handle = match self.job_id_to_handle.get(job_id) {
            Some(handle) => handle,
            None => {
                tracing::warn!(%job_id, "Job not found, skipping.");
                return;
            }
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
            .expect(&format!("Task ID {task_id} is not tracked by the set"));
        let _handle = self
            .job_id_to_handle
            .remove(&job_id)
            .expect(&format!("Job ID {job_id} is not tracked by the set"));

        let next = match result {
            // The job completed successfully
            Ok(Ok(())) => (job_id, Ok(())),
            // The job returned an error
            Ok(Err(err)) => (job_id, Err(JoinError::Failed(err))),
            // The job was aborted
            Err(err) if err.is_cancelled() => (job_id, Err(JoinError::Aborted)),
            // The job panicked
            Err(err) => (job_id, Err(JoinError::Panicked(err.into()))),
        };

        Some(next)
    }
}

/// The error type for the [`JobSet::join_next`] method
#[derive(Debug, thiserror::Error)]
pub enum JoinError {
    /// The job failed
    #[error("Job failed: {0}")]
    Failed(BoxError),
    /// The job was aborted
    #[error("Job was aborted")]
    Aborted,
    /// The job panicked
    #[error("Job panicked: {0}")]
    Panicked(BoxError),
}
