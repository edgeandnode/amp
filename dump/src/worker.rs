use std::{collections::BTreeMap, pin::pin, sync::Arc};

use common::{config::Config, BoxError};
use futures::{TryFutureExt as _, TryStreamExt as _};
use metadata_db::{JobId, JobNotifAction, JobNotification, MetadataDb, WorkerNodeId};
use thiserror::Error;
use tokio::{
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
    task::JoinHandle,
};

use crate::job::Job;

pub struct Worker {
    config: Arc<Config>,
    metadata_db: Arc<MetadataDb>,
    node_id: WorkerNodeId,

    // To prevent start/stop race conditions, actions for a same jobs are processed sequentially.
    // Each jobs has a dedicated handler task.
    action_queue: BTreeMap<JobId, UnboundedSender<JobNotification>>,
}

#[derive(Error, Debug)]
pub enum WorkerError {
    #[error("heartbeat task failed: {0}")]
    HeartbeatError(BoxError),

    #[error("error listening to worker actions: {0}")]
    ListenError(BoxError),

    #[error("location handler panicked")]
    HandlerPanic,

    #[error("database error: {0}")]
    DbError(#[from] metadata_db::Error),

    #[error("error loading job: {0}")]
    JobLoadError(BoxError),
}

impl Worker {
    pub fn new(config: Arc<Config>, metadata_db: Arc<MetadataDb>, node_id: WorkerNodeId) -> Self {
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
            .listen_for_job_notifications()
            .await
            .map_err(|err| ListenError(err.into()))?
            .into_stream()
            .map_err(|err| ListenError(err.into()));

        // Spawn scheduled jobs.
        let scheduled_jobs = self.metadata_db.scheduled_jobs(&self.node_id).await?;
        for job_id in scheduled_jobs {
            let job = Job::load(
                &job_id,
                self.config.clone(),
                self.metadata_db.clone().into(),
            )
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

    fn handle_action(&mut self, action: JobNotification) -> Result<(), WorkerError> {
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
    node_id: WorkerNodeId,
    job_id: JobId,
    recv: UnboundedReceiver<JobNotification>,
}

impl JobHandler {
    fn new(
        config: Arc<Config>,
        metadata_db: Arc<MetadataDb>,
        node_id: WorkerNodeId,
        job_id: JobId,
        recv: UnboundedReceiver<JobNotification>,
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
                tracing::error!("error handling action `{:?}`: {}", action, e);
            }
        }

        // Only happens if the `Worker` is dropped.
        tracing::debug!(
            "Dropping job handler for node {} and job {}",
            self.node_id,
            self.job_id
        );
    }

    #[tracing::instrument(skip(self), err)]
    async fn handle_action(&self, action: JobNotifAction) -> Result<(), WorkerError> {
        match action {
            JobNotifAction::Start => {
                let job = Job::load(&self.job_id, self.config.clone(), self.metadata_db.clone())
                    .await
                    .map_err(WorkerError::JobLoadError)?;
                spawn_job(self.config.clone(), self.metadata_db.clone(), job);
            }

            JobNotifAction::Stop => {
                // No code path currently calls this.
                unimplemented!()
            }
            _ => {
                tracing::warn!("unhandled action: {}", action);
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
                tracing::info!("job {} finished running", job_desc);
            }
            Err(e) => {
                tracing::error!("error running job {}: {}", job_desc, e);
            }
        }
    })
}
