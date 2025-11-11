//! Scheduler implementation for the controller service
//!
//! This module provides the concrete implementation of the `JobScheduler` trait defined in
//! `admin-api`. It handles job scheduling, management, and coordination with worker nodes.
//!
//! ## Architecture
//!
//! The scheduler follows a dependency inversion pattern:
//! - `admin-api` defines the `JobScheduler` trait (abstraction)
//! - `controller` provides `SchedulerImpl` (implementation)
//! - Admin API handlers depend on the trait, not the implementation
//!
//! ## Key Responsibilities
//!
//! - **Job Scheduling**: Select available workers and create new dataset dump jobs
//! - **Job Control**: Stop running jobs with transactional guarantees
//! - **Job Queries**: Retrieve job status and list jobs with pagination
//! - **Job Cleanup**: Delete jobs in terminal states (completed, stopped, failed)
//! - **Worker Coordination**: Send notifications to workers via PostgreSQL LISTEN/NOTIFY
//!
//! ## Implementation Details
//!
//! - Uses PostgreSQL for metadata storage and job state tracking
//! - Implements atomic job operations with database transactions
//! - Selects workers randomly from active worker pool
//! - Manages physical table locations for dataset storage

use std::{sync::Arc, time::Duration};

use admin_api::scheduler::{
    DeleteJobError, DeleteJobsByStatusError, GetJobError, GetWorkerError, ListJobsError,
    ListWorkersError, ScheduleJobError, SchedulerJobs, SchedulerWorkers, StopJobError,
};
use async_trait::async_trait;
use common::{
    Dataset,
    catalog::{JobLabels, physical::PhysicalTable},
    config::Config,
};
use dump::EndBlock;
use metadata_db::{Error as MetadataDbError, JobStatusUpdateError, MetadataDb, Worker};
use rand::seq::IndexedRandom as _;
use worker::{
    job::{Job, JobDescriptor, JobId, JobNotification, JobStatus},
    node_id::NodeId,
};

/// A worker is considered active if it has sent a heartbeat in this period
///
/// The scheduler will only schedule new jobs on workers that have sent a heartbeat within
/// this interval. Workers that haven't sent heartbeats are considered dead or unavailable.
const DEAD_WORKER_INTERVAL: Duration = Duration::from_secs(5);

/// Concrete implementation of the `JobScheduler` trait
///
/// Manages job scheduling and lifecycle operations for the controller service.
/// This implementation coordinates with worker nodes to execute dataset extraction jobs.
///
/// Thread-safe for sharing across async tasks via `Arc<dyn JobScheduler>`.
pub struct Scheduler {
    config: Arc<Config>,
    metadata_db: MetadataDb,
}

impl Scheduler {
    /// Create a new scheduler instance
    pub fn new(config: Arc<Config>, metadata_db: MetadataDb) -> Self {
        Self {
            config,
            metadata_db,
        }
    }

    /// Schedule a dataset synchronization job
    ///
    /// Checks for existing scheduled or running jobs to avoid duplicates, selects an available
    /// worker node (or uses the specified worker_id), creates physical table locations, and
    /// registers the job in the metadata database.
    async fn schedule_dataset_sync_job_impl(
        &self,
        dataset: Arc<Dataset>,
        end_block: EndBlock,
        max_writers: u16,
        job_labels: JobLabels,
        worker_id: Option<NodeId>,
    ) -> Result<JobId, ScheduleJobError> {
        // Avoid re-scheduling jobs in a scheduled or running state.
        let existing_jobs =
            metadata_db::jobs::get_by_dataset(&self.metadata_db, (&dataset.manifest_hash).into())
                .await
                .map_err(ScheduleJobError::CheckExistingJobs)?;
        for job in existing_jobs {
            match job.status.into() {
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

        let candidates = metadata_db::workers::list_active(&self.metadata_db, DEAD_WORKER_INTERVAL)
            .await
            .map_err(ScheduleJobError::ListActiveWorkers)?;

        let node_id = match worker_id {
            Some(worker_id) => {
                let worker_id_ref = metadata_db::WorkerNodeId::from(&worker_id);
                if !candidates.contains(&worker_id_ref) {
                    return Err(ScheduleJobError::WorkerNotAvailable(worker_id));
                }
                worker_id_ref.to_owned()
            }
            None => {
                // Randomly select from active workers
                let Some(node_id) = candidates.choose(&mut rand::rng()).cloned() else {
                    return Err(ScheduleJobError::NoWorkersAvailable);
                };
                node_id
            }
        };

        let mut locations = Vec::new();
        for table in dataset.resolved_tables(job_labels.dataset_reference().into()) {
            let physical_table = match PhysicalTable::get_active(&table, self.metadata_db.clone())
                .await
                .map_err(ScheduleJobError::GetPhysicalTable)?
            {
                Some(physical_table) => physical_table,
                None => {
                    let store = &self.config.data_store;
                    PhysicalTable::next_revision(
                        &table,
                        store,
                        self.metadata_db.clone(),
                        true,
                        &job_labels,
                    )
                    .await
                    .map_err(ScheduleJobError::CreatePhysicalTable)?
                }
            };
            locations.push(physical_table.location_id());
        }

        let job_desc = serde_json::to_string(&JobDescriptor::Dump {
            end_block,
            max_writers,
            dataset_namespace: job_labels.dataset_namespace.clone(),
            dataset_name: job_labels.dataset_name.clone(),
            manifest_hash: job_labels.manifest_hash.clone(),
        })
        .map_err(ScheduleJobError::SerializeJobDescriptor)?;
        let job_id =
            metadata_db::jobs::schedule(&self.metadata_db, &node_id, &job_desc, &locations)
                .await
                .map(Into::into)
                .map_err(ScheduleJobError::RegisterJob)?;

        // Notify the worker about the new job
        // TODO: Include into the transaction
        metadata_db::workers::send_job_notif(
            &self.metadata_db,
            node_id,
            &JobNotification::start(job_id),
        )
        .await
        .map_err(ScheduleJobError::NotifyWorker)?;

        Ok(job_id)
    }

    /// Stop a running job with transactional guarantees
    ///
    /// Fetches the job, validates its state, updates it to stop-requested, and notifies the
    /// worker within a single database transaction for atomicity.
    async fn stop_job_impl(&self, job_id: JobId) -> Result<(), StopJobError> {
        // Begin a transaction to ensure atomicity
        let mut tx = self
            .metadata_db
            .begin_txn()
            .await
            .map_err(StopJobError::BeginTransaction)?;

        // Fetch the job to get its node_id and validate it exists
        let job = metadata_db::jobs::get_by_id(&mut tx, &job_id)
            .await
            .map_err(StopJobError::GetJob)?
            .ok_or(StopJobError::JobNotFound)?;

        // Attempt to stop the job
        metadata_db::jobs::request_stop(&mut tx, &job_id)
            .await
            .map_err(|err| match err {
                MetadataDbError::JobStatusUpdate(JobStatusUpdateError::NotFound) => {
                    StopJobError::JobNotFound
                }
                MetadataDbError::JobStatusUpdate(JobStatusUpdateError::StateConflict {
                    actual,
                    ..
                }) => match actual.into() {
                    JobStatus::Stopped | JobStatus::Completed | JobStatus::Failed => {
                        StopJobError::JobAlreadyTerminated {
                            status: actual.into(),
                        }
                    }
                    _ => StopJobError::StateConflict {
                        current_status: actual.into(),
                    },
                },
                other => StopJobError::UpdateJobStatus(other),
            })?;

        // Notify the worker about the stop request (within the transaction)
        metadata_db::workers::send_job_notif(&mut tx, job.node_id, &JobNotification::stop(job_id))
            .await
            .map_err(StopJobError::SendNotification)?;

        // Commit the transaction
        tx.commit().await.map_err(StopJobError::CommitTransaction)?;

        Ok(())
    }
}

#[async_trait]
impl SchedulerJobs for Scheduler {
    async fn schedule_dataset_sync_job(
        &self,
        dataset: Arc<Dataset>,
        end_block: EndBlock,
        max_writers: u16,
        job_labels: JobLabels,
        worker_id: Option<NodeId>,
    ) -> Result<JobId, ScheduleJobError> {
        self.schedule_dataset_sync_job_impl(dataset, end_block, max_writers, job_labels, worker_id)
            .await
    }

    async fn stop_job(&self, job_id: JobId) -> Result<(), StopJobError> {
        self.stop_job_impl(job_id).await
    }

    async fn get_job(&self, job_id: JobId) -> Result<Option<Job>, GetJobError> {
        let job = metadata_db::jobs::get_by_id(&self.metadata_db, &job_id)
            .await
            .map_err(GetJobError)?
            .map(Into::into);
        Ok(job)
    }

    async fn list_jobs(
        &self,
        limit: i64,
        last_id: Option<JobId>,
        statuses: Option<&[JobStatus]>,
    ) -> Result<Vec<Job>, ListJobsError> {
        let statuses = statuses.map(|statuses| {
            statuses
                .iter()
                .map(|s| (*s).into())
                .collect::<Vec<metadata_db::JobStatus>>()
        });
        let jobs = metadata_db::jobs::list(&self.metadata_db, limit, last_id, statuses.as_deref())
            .await
            .map_err(ListJobsError)?
            .into_iter()
            .map(Into::into)
            .collect();
        Ok(jobs)
    }

    async fn delete_job(&self, job_id: JobId) -> Result<bool, DeleteJobError> {
        metadata_db::jobs::delete_if_terminal(&self.metadata_db, &job_id)
            .await
            .map_err(DeleteJobError)
    }

    async fn delete_jobs_in_terminal_state(&self) -> Result<usize, DeleteJobsByStatusError> {
        let status = JobStatus::terminal_statuses();
        metadata_db::jobs::delete_all_by_status(&self.metadata_db, status.map(Into::into))
            .await
            .map_err(DeleteJobsByStatusError)
    }

    async fn delete_completed_jobs(&self) -> Result<usize, DeleteJobsByStatusError> {
        metadata_db::jobs::delete_all_by_status(&self.metadata_db, [JobStatus::Completed.into()])
            .await
            .map_err(DeleteJobsByStatusError)
    }

    async fn delete_stopped_jobs(&self) -> Result<usize, DeleteJobsByStatusError> {
        metadata_db::jobs::delete_all_by_status(&self.metadata_db, [JobStatus::Stopped.into()])
            .await
            .map_err(DeleteJobsByStatusError)
    }

    async fn delete_failed_jobs(&self) -> Result<usize, DeleteJobsByStatusError> {
        metadata_db::jobs::delete_all_by_status(&self.metadata_db, [JobStatus::Failed.into()])
            .await
            .map_err(DeleteJobsByStatusError)
    }
}

#[async_trait]
impl SchedulerWorkers for Scheduler {
    async fn list_workers(&self) -> Result<Vec<Worker>, ListWorkersError> {
        metadata_db::workers::list(&self.metadata_db)
            .await
            .map_err(ListWorkersError)
    }

    async fn get_worker_by_id(&self, node_id: &NodeId) -> Result<Option<Worker>, GetWorkerError> {
        metadata_db::workers::get_by_id(&self.metadata_db, node_id)
            .await
            .map_err(GetWorkerError)
    }
}
