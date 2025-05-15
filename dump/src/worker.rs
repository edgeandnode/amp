use std::{collections::BTreeMap, fmt, pin::pin, sync::Arc};

use common::{config::Config, BoxError};
use futures::{TryFutureExt as _, TryStreamExt};
use metadata_db::{JobDatabaseId, MetadataDb};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
    task::JoinHandle,
};
use tracing::{debug, error, info, instrument};

use crate::job::Job;

pub const WORKER_ACTIONS_PG_CHANNEL: &str = "worker_actions";

/// These actions coordinate the jobs state and the write lock on the output table locations.
///
/// Start action:
/// - Fetch the jobs descriptor and output locations from the `metadata_db`.
/// - Start the jobs.
///
/// Stop action (currently unimplemented):
/// - Stop the jobs.
/// - Release the locations by deleting the row from the `jobs` table.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum Action {
    Start,
    Stop,
}

#[derive(Serialize, Deserialize)]
pub struct WorkerAction {
    pub node_id: String,
    pub job_id: JobDatabaseId,
    pub action: Action,
}

impl fmt::Debug for WorkerAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}

pub struct Worker {
    config: Arc<Config>,
    metadata_db: Arc<MetadataDb>,
    node_id: String,

    // To prevent start/stop race conditions, actions for a same jobs are processed sequentially.
    // Each jobs has a dedicated handler task.
    action_queue: BTreeMap<JobDatabaseId, UnboundedSender<WorkerAction>>,
}

#[derive(Error, Debug)]
pub enum WorkerError {
    #[error("heartbeat task failed: {0}")]
    HeartbeatError(BoxError),

    #[error("error listening to worker actions: {0}")]
    ListenError(sqlx::Error),

    #[error("location handler panicked")]
    HandlerPanic,

    #[error("database error: {0}")]
    DbError(#[from] metadata_db::Error),

    #[error("error loading job: {0}")]
    JobLoadError(BoxError),
}

impl Worker {
    pub fn new(config: Arc<Config>, metadata_db: Arc<MetadataDb>, node_id: String) -> Self {
        Self {
            config,
            metadata_db,
            node_id,
            action_queue: BTreeMap::new(),
        }
    }

    pub async fn run(mut self) -> Result<(), WorkerError> {
        use WorkerError::*;

        // Say hello before anything else, to make sure this worker is seen by the system as
        // registered and active.
        self.metadata_db.hello_worker(&self.node_id).await?;

        // Periodic heartbeat task.
        let db = self.metadata_db.clone();
        let id = self.node_id.clone();
        let heartbeat_task: JoinHandle<Result<(), WorkerError>> = tokio::spawn(async move {
            db.heartbeat_loop(id)
                .await
                .map_err(|e| HeartbeatError(e.into()))
        });

        // Start listening for actions.
        let action_stream = self
            .metadata_db
            .listen(WORKER_ACTIONS_PG_CHANNEL)
            .await
            .map_err(|e| ListenError(e.into()))?
            .map_err(|e| ListenError(e.into()))
            .try_filter_map(|n| async move {
                match serde_json::from_str(&n.payload()) {
                    Ok(action) => Ok(Some(action)),
                    Err(e) => {
                        // Warn the operator about invalid notifications, but don't fail the worker.
                        error!(
                            "Invalid notification, error: `{}`, payload: `{}`",
                            e,
                            n.payload()
                        );
                        Ok(None)
                    }
                }
            });

        // Spawn scheduled jobs.
        let scheduled_jobs = self.metadata_db.scheduled_jobs(&self.node_id).await?;
        for job_id in scheduled_jobs {
            let job = Job::load(job_id, self.config.clone(), self.metadata_db.clone().into())
                .await
                .map_err(JobLoadError)?;
            spawn_job(self.config.clone(), self.metadata_db.clone(), job);
        }

        let scheduler_loop = async move {
            let mut stream = pin!(action_stream);
            while let Some(action) = stream.try_next().await? {
                self.handle_action(action)?;
            }
            Ok(())
        };

        // Run forever or until a fatal error.
        tokio::select! {
            res = heartbeat_task.map_err(|e| HeartbeatError(e.into())) => res?,
            res = scheduler_loop => res,
        }
    }

    fn handle_action(&mut self, action: WorkerAction) -> Result<(), WorkerError> {
        if action.node_id != self.node_id {
            return Ok(());
        }
        let job_task = self.action_queue.entry(action.job_id).or_insert_with(|| {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            let job_handler = JobHandler::new(
                self.config.clone(),
                self.metadata_db.clone(),
                self.node_id.clone(),
                action.job_id,
                rx,
            );
            tokio::spawn(job_handler.run());
            tx
        });
        job_task.send(action).map_err(|_| WorkerError::HandlerPanic)
    }
}

struct JobHandler {
    config: Arc<Config>,
    metadata_db: Arc<MetadataDb>,
    node_id: String,
    job_id: JobDatabaseId,
    recv: UnboundedReceiver<WorkerAction>,
}

impl JobHandler {
    fn new(
        config: Arc<Config>,
        metadata_db: Arc<MetadataDb>,
        node_id: String,
        job_id: JobDatabaseId,
        recv: UnboundedReceiver<WorkerAction>,
    ) -> Self {
        Self {
            config,
            metadata_db,
            node_id,
            job_id,
            recv,
        }
    }

    async fn run(mut self) {
        while let Some(action) = self.recv.recv().await {
            assert!(action.node_id == self.node_id);
            assert!(action.job_id == self.job_id);

            if let Err(e) = self.handle_action(action.action).await {
                error!("error handling action `{:?}`: {}", action, e);
            }
        }

        // Only happens if the `Worker` is dropped.
        debug!(
            "Dropping job handler for node {} and job {}",
            self.node_id, self.job_id
        );
    }

    #[instrument(skip(self), err)]
    async fn handle_action(&self, action: Action) -> Result<(), WorkerError> {
        use WorkerError::*;

        match action {
            Action::Start => {
                let job = Job::load(self.job_id, self.config.clone(), self.metadata_db.clone())
                    .await
                    .map_err(JobLoadError)?;
                spawn_job(self.config.clone(), self.metadata_db.clone(), job);
            }

            Action::Stop => {
                // No code path currently calls this.
                unimplemented!()
            }
        }
        Ok(())
    }
}

fn spawn_job(config: Arc<Config>, metadata_db: Arc<MetadataDb>, job: Job) -> JoinHandle<()> {
    tokio::spawn(async move {
        let job_desc = job.to_string();
        match job.run(config, metadata_db).await {
            Ok(()) => {
                info!("job {} finished running", job_desc);
            }
            Err(e) => {
                error!("error running job {}: {}", job_desc, e);
            }
        }
    })
}
