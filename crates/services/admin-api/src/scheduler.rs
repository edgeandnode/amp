use std::sync::Arc;

use common::{BoxError, Dataset, catalog::physical::PhysicalTable, config::Config};
use datasets_common::{partial_reference::PartialReference, revision::Revision};
use dump::EndBlock;
use metadata_db::{Error as MetadataDbError, Job, JobStatus, JobStatusUpdateError, MetadataDb};
use rand::seq::IndexedRandom as _;
use worker::{JobDescriptor, JobId, JobNotification};

#[derive(Clone)]
pub struct Scheduler {
    config: Arc<Config>,
    metadata_db: MetadataDb,
}

impl Scheduler {
    pub fn new(config: Arc<Config>, metadata_db: MetadataDb) -> Self {
        Self {
            config,
            metadata_db,
        }
    }

    /// Schedule a dump for a new copy of a dataset.
    pub async fn schedule_dataset_dump(
        &self,
        dataset: Dataset,
        end_block: EndBlock,
        max_writers: u16,
    ) -> Result<JobId, ScheduleJobError> {
        // Avoid re-scheduling jobs in a scheduled or running state.
        let existing_jobs = metadata_db::jobs::get_by_dataset(
            &self.metadata_db,
            dataset.name.clone(),
            dataset.version.clone(),
        )
        .await?;
        for job in existing_jobs {
            match job.status {
                JobStatus::Scheduled | JobStatus::Running => return Ok(job.id.into()),
                JobStatus::Completed
                | JobStatus::Stopped
                | JobStatus::StopRequested
                | JobStatus::Stopping
                | JobStatus::Failed
                | JobStatus::Unknown => (),
            };
        }

        // Scheduling procedure for a new `DumpDataset` job:
        // 1. Choose a responsive node.
        // 2. Create a new location for each table.
        // 3. Register the job in the metadata db.
        // 4. Send a `Start` command through `worker_actions` for that job.
        //
        // The worker node should then receive the notification and start the dump run.

        let candidates = self.metadata_db.active_workers().await?;
        let Some(node_id) = candidates.choose(&mut rand::rng()).cloned() else {
            return Err(ScheduleJobError::NoAvailableWorkers);
        };

        let dataset_ref = PartialReference::new(
            Some(dataset.namespace.clone()),
            dataset.name.clone(),
            dataset.version.clone().map(Revision::Version),
        );

        let mut locations = Vec::new();
        for table in Arc::new(dataset).resolved_tables() {
            let physical_table =
                match PhysicalTable::get_active(&table, self.metadata_db.clone()).await? {
                    Some(physical_table) => physical_table,
                    None => {
                        let store = &self.config.data_store;
                        PhysicalTable::next_revision(&table, store, self.metadata_db.clone(), true)
                            .await?
                    }
                };
            locations.push(physical_table.location_id());
        }
        let job_desc = serde_json::to_string(&JobDescriptor::Dump {
            dataset: dataset_ref,
            end_block,
            max_writers,
        })?;
        let job_id =
            metadata_db::jobs::schedule(&self.metadata_db, &node_id, &job_desc, &locations)
                .await
                .map(Into::into)?;

        // Notify the worker about the new job
        self.metadata_db
            .send_job_notification(node_id, &JobNotification::start(job_id))
            .await?;

        Ok(job_id)
    }

    /// Stop a running job
    ///
    /// This method fetches the job and performs the stop operation within a single transaction,
    /// ensuring atomicity and preventing race conditions.
    pub async fn stop_job(&self, job_id: &JobId) -> Result<(), StopJobError> {
        // Begin a transaction to ensure atomicity
        let mut tx = self
            .metadata_db
            .begin_txn()
            .await
            .map_err(StopJobError::BeginTransaction)?;

        // Fetch the job to get its node_id and validate it exists
        let job = metadata_db::jobs::get_by_id(&mut tx, job_id)
            .await
            .map_err(StopJobError::GetJob)?
            .ok_or(StopJobError::JobNotFound)?;

        // Attempt to stop the job
        metadata_db::jobs::request_stop(&mut tx, job_id)
            .await
            .map_err(|err| match err {
                MetadataDbError::JobStatusUpdateError(JobStatusUpdateError::NotFound) => {
                    StopJobError::JobNotFound
                }
                MetadataDbError::JobStatusUpdateError(JobStatusUpdateError::StateConflict {
                    actual,
                    ..
                }) => match actual {
                    JobStatus::Stopped | JobStatus::Completed | JobStatus::Failed => {
                        StopJobError::JobAlreadyTerminated { status: actual }
                    }
                    _ => StopJobError::StateConflict {
                        current_status: actual,
                    },
                },
                other => StopJobError::UpdateJobStatus(other),
            })?;

        // Commit the transaction
        tx.commit().await.map_err(StopJobError::CommitTransaction)?;

        // Notify the worker about the stop request
        // TODO: Include into the transaction
        self.metadata_db
            .send_job_notification(job.node_id, &JobNotification::stop(*job_id))
            .await
            .map_err(StopJobError::SendNotification)?;

        Ok(())
    }

    /// Get a job by its ID
    pub async fn get_job(&self, job_id: &JobId) -> Result<Option<Job>, GetJobError> {
        metadata_db::jobs::get_by_id(&self.metadata_db, job_id)
            .await
            .map_err(GetJobError)
    }

    /// List jobs with cursor-based pagination
    pub async fn list_jobs(
        &self,
        limit: i64,
        last_job_id: Option<JobId>,
    ) -> Result<Vec<Job>, ListJobsError> {
        metadata_db::jobs::list(&self.metadata_db, limit, last_job_id)
            .await
            .map_err(ListJobsError)
    }

    /// Delete a job if it's in a terminal state
    ///
    /// Returns `true` if the job was deleted, `false` if it wasn't found or wasn't in a terminal state.
    pub async fn delete_job(&self, job_id: &JobId) -> Result<bool, DeleteJobError> {
        metadata_db::jobs::delete_if_terminal(&self.metadata_db, job_id)
            .await
            .map_err(DeleteJobError)
    }

    /// Delete all jobs matching the specified status or statuses
    ///
    /// Returns the number of jobs deleted.
    pub async fn delete_jobs_by_status<const N: usize>(
        &self,
        statuses: [JobStatus; N],
    ) -> Result<usize, DeleteJobsByStatusError> {
        metadata_db::jobs::delete_all_by_status(&self.metadata_db, statuses)
            .await
            .map_err(DeleteJobsByStatusError)
    }
}

/// Errors that can occur when scheduling a dataset dump job
#[derive(Debug, thiserror::Error)]
pub enum ScheduleJobError {
    /// Metadata database error
    #[error("metadata database error: {0}")]
    MetadataDb(#[from] metadata_db::Error),

    /// No available workers
    #[error("no available workers")]
    NoAvailableWorkers,

    /// Dataset operation error
    #[error("dataset operation error: {0}")]
    DatasetError(#[from] BoxError),

    /// JSON serialization error
    #[error("serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),
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
