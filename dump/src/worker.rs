use std::{collections::BTreeMap, sync::Arc};

use common::{config::Config, BoxError};
use dataset_store::DatasetStore;
use futures::{TryFutureExt as _, TryStreamExt as _};
use metadata_db::{JobId, JobNotifAction, JobNotification, MetadataDb, WorkerNodeId};
use tokio::{
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
    task::JoinHandle,
};
use tracing::instrument;

mod job;

pub use self::job::JobDesc;
use self::job::{Job, JobCtx};

pub struct Worker {
    config: Arc<Config>,
    metadata_db: Arc<MetadataDb>,
    node_id: WorkerNodeId,

    // To prevent start/stop race conditions, actions for a same jobs are processed sequentially.
    // Each jobs has a dedicated handler task.
    action_queue: BTreeMap<JobId, UnboundedSender<JobNotification>>,
}

impl Worker {
    /// Create a new worker instance
    pub fn new(config: Arc<Config>, metadata_db: Arc<MetadataDb>, node_id: WorkerNodeId) -> Self {
        Self {
            config,
            metadata_db,
            node_id,
            action_queue: Default::default(),
        }
    }

    pub async fn run(mut self) -> Result<(), WorkerError> {
        // Say hello before anything else, to make sure this worker is seen by the system as
        // registered and active.
        self.metadata_db.hello_worker(&self.node_id).await?;

        // Periodic heartbeat task.
        let db = self.metadata_db.clone();
        let id = self.node_id.clone();
        let heartbeat_task: JoinHandle<Result<(), WorkerError>> = tokio::spawn(async move {
            db.heartbeat_loop(&id)
                .await
                .map_err(|err| WorkerError::HeartbeatError(err.into()))
        });

        // Start listening for actions.
        let action_stream = self
            .metadata_db
            .listen_for_job_notifications()
            .await
            .map_err(|err| WorkerError::ListenError(err.into()))?
            .into_stream()
            .map_err(|err| WorkerError::ListenError(err.into()));

        // Create the job context and spawn scheduled jobs
        let job_ctx = JobCtx {
            config: self.config.clone(),
            metadata_db: self.metadata_db.clone(),
            dataset_store: DatasetStore::new(self.config.clone(), self.metadata_db.clone()),
            data_store: self.config.data_store.clone(),
        };

        let scheduled_jobs = self.metadata_db.scheduled_jobs(&self.node_id).await?;
        for job_id in scheduled_jobs {
            let job = Job::load(job_ctx.clone(), &job_id)
                .await
                .map_err(WorkerError::JobLoadError)?;
            spawn_job(job);
        }

        let scheduler_loop = async move {
            let mut stream = std::pin::pin!(action_stream);
            while let Some(action) = stream.try_next().await? {
                // Ignore actions targeting other nodes
                if action.node_id != self.node_id {
                    continue;
                }

                self.handle_action(&job_ctx, action)?;
            }
            Ok(())
        };

        // Run forever or until a fatal error.
        tokio::select! {
            res = heartbeat_task.map_err(|e| WorkerError::HeartbeatError(e.into())) => res?,
            res = scheduler_loop => res,
        }
    }

    fn handle_action(
        &mut self,
        job_ctx: &JobCtx,
        action: JobNotification,
    ) -> Result<(), WorkerError> {
        // Create a new job handler if the job is not already running.
        // If the job is already running, enqueue the action.
        let job_action_sender = self.action_queue.entry(action.job_id).or_insert_with(|| {
            let (job_handler, tx) =
                JobHandler::new(job_ctx.clone(), self.node_id.clone(), action.job_id);
            tokio::spawn(job_handler.run());
            tx
        });
        job_action_sender
            .send(action)
            .map_err(|_| WorkerError::HandlerPanic)
    }
}

/// Worker errors
#[derive(Debug, thiserror::Error)]
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

struct JobHandler {
    ctx: JobCtx,
    node_id: WorkerNodeId,
    job_id: JobId,
    rx: UnboundedReceiver<JobNotification>,
}

impl JobHandler {
    fn new(
        ctx: JobCtx,
        node_id: WorkerNodeId,
        job_id: JobId,
    ) -> (Self, UnboundedSender<JobNotification>) {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<JobNotification>();
        (
            Self {
                ctx,
                node_id,
                job_id,
                rx,
            },
            tx,
        )
    }

    async fn run(mut self) {
        while let Some(action) = self.rx.recv().await {
            assert!(action.node_id == self.node_id);
            assert!(action.job_id == self.job_id);

            if let Err(err) = handle_action(&self.ctx, &self.job_id, action.action).await {
                tracing::error!("error handling action `{:?}`: {}", action, err);
            }
        }

        // Only happens if the `Worker` is dropped.
        tracing::debug!(
            "Dropping job handler for node {} and job {}",
            self.node_id,
            self.job_id
        );
    }
}

#[instrument(skip(ctx), err)]
async fn handle_action(
    ctx: &JobCtx,
    job_id: &JobId,
    action: JobNotifAction,
) -> Result<(), WorkerError> {
    match action {
        JobNotifAction::Start => {
            let job = Job::load(ctx.clone(), job_id)
                .await
                .map_err(WorkerError::JobLoadError)?;
            spawn_job(job);
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

fn spawn_job(job: Job) -> JoinHandle<()> {
    tokio::spawn(async move {
        let job_desc = job.to_string();
        match job.run().await {
            Ok(()) => {
                tracing::info!("job {} finished running", job_desc);
            }
            Err(e) => {
                tracing::error!("error running job {}: {}", job_desc, e);
            }
        }
    })
}
