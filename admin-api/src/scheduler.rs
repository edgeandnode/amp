use std::sync::Arc;

use common::{BoxError, Dataset, catalog::physical::PhysicalTable, config::Config};
use dump::worker::JobDesc;
use metadata_db::{
    Error as MetadataDbError, JobId, JobStatus, JobStatusUpdateError, MetadataDb, WorkerNodeId,
};
use rand::seq::IndexedRandom as _;

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
        end_block: Option<i64>,
    ) -> Result<JobId, ScheduleJobError> {
        // Scheduling procedure for a new `DumpDataset` job:
        // 1. Choose a responsive node.
        // 2. Create a new location for each table.
        // 3. Register the job in the metadata db.
        // 4. Send a `Start` command through `worker_actions` for that job.
        //
        // The worker node should then receive the notification and start the dump run.

        let candidates = self.metadata_db.active_workers().await?;
        let Some(node_id) = candidates.choose(&mut rand::rng()) else {
            return Err(ScheduleJobError::NoAvailableWorkers);
        };

        let mut locations = Vec::new();
        let metadata_db = Arc::new(self.metadata_db.clone());
        for table in Arc::new(dataset).resolved_tables() {
            let physical_table =
                match PhysicalTable::get_active(&table, metadata_db.clone()).await? {
                    Some(physical_table) => physical_table,
                    None => {
                        let store = &self.config.data_store;
                        PhysicalTable::next_revision(&table, store, metadata_db.clone(), true, None)
                            .await?
                    }
                };
            locations.push(physical_table.location_id());
        }

        let job_desc = serde_json::to_string(&JobDesc::Dump { end_block })?;

        let job_id = self
            .metadata_db
            .schedule_job(node_id, &job_desc, &locations)
            .await?;

        Ok(job_id)
    }

    /// Stop a running job
    ///
    /// Note: This method assumes validation has already been performed by the caller.
    /// It directly delegates to the atomic MetadataDb operation.
    pub async fn stop_job(
        &self,
        job_id: &JobId,
        node_id: &WorkerNodeId,
    ) -> Result<(), StopJobError> {
        Ok(self
            .metadata_db
            .request_job_stop(job_id, node_id)
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
                other => StopJobError::MetadataDb(other),
            })?)
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
    #[error("job not found")]
    JobNotFound,

    /// Job is already in a terminal state (stopped, completed, failed)
    #[error("job is already in terminal state: {status}")]
    JobAlreadyTerminated { status: metadata_db::JobStatus },

    /// Job state conflict - cannot stop from current state
    #[error("cannot stop job from current state: {current_status}")]
    StateConflict {
        current_status: metadata_db::JobStatus,
    },

    /// General metadata database error
    #[error("metadata database error: {0}")]
    MetadataDb(metadata_db::Error),
}
