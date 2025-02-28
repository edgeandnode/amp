use std::{collections::BTreeMap, fmt, pin::pin, sync::Arc};

use common::{config::Config, BoxError};
use futures::{TryFutureExt as _, TryStreamExt};
use log::{debug, error};
use metadata_db::{MetadataDb, OperatorDatabaseId};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
    task::JoinHandle,
};
use tracing::{info, instrument};

use crate::operator::Operator;

pub const WORKER_ACTIONS_PG_CHANNEL: &str = "worker_actions";

/// These actions coordinate the operator state and the write lock on the output table locations.
///
/// Start action:
/// - Fetch the operator descriptor and output locations from the `metadata_db`.
/// - Start the operator.
///
/// Stop action (currently unimplemented):
/// - Stop the operator.
/// - Release the locations by deleting the row from the `operators` table.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum Action {
    Start,
    Stop,
}

#[derive(Serialize, Deserialize)]
pub struct WorkerAction {
    pub node_id: String,
    pub operator_id: OperatorDatabaseId,
    pub action: Action,
}

impl fmt::Debug for WorkerAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}

pub struct Worker {
    config: Arc<Config>,
    metadata_db: MetadataDb,
    node_id: String,

    // To prevent start/stop race conditions, actions for a same operator are processed sequentially.
    // Each operator has a dedicated handler task.
    action_queue: BTreeMap<OperatorDatabaseId, UnboundedSender<WorkerAction>>,
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

    #[error("error loading operator: {0}")]
    OperatorLoadError(BoxError),
}

impl Worker {
    pub fn new(config: Arc<Config>, metadata_db: MetadataDb, node_id: String) -> Self {
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
        let heartbeat_task: JoinHandle<Result<(), WorkerError>> = tokio::spawn(
            self.metadata_db
                .clone()
                .heartbeat_loop(self.node_id.clone())
                .map_err(|e| HeartbeatError(e.into())),
        );

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

        // Spawn scheduled operators.
        let scheduled_operators = self.metadata_db.scheduled_operators(&self.node_id).await?;
        for operator_id in scheduled_operators {
            let operator =
                Operator::load(operator_id, self.config.clone(), self.metadata_db.clone())
                    .await
                    .map_err(OperatorLoadError)?;
            spawn_operator(self.config.clone(), self.metadata_db.clone(), operator);
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
        let operator_task = self
            .action_queue
            .entry(action.operator_id)
            .or_insert_with(|| {
                let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
                let operator_handler = OperatorHandler::new(
                    self.config.clone(),
                    self.metadata_db.clone(),
                    self.node_id.clone(),
                    action.operator_id,
                    rx,
                );
                tokio::spawn(operator_handler.run());
                tx
            });
        operator_task
            .send(action)
            .map_err(|_| WorkerError::HandlerPanic)
    }
}

struct OperatorHandler {
    config: Arc<Config>,
    metadata_db: MetadataDb,
    node_id: String,
    operator_id: OperatorDatabaseId,
    recv: UnboundedReceiver<WorkerAction>,
}

impl OperatorHandler {
    fn new(
        config: Arc<Config>,
        metadata_db: MetadataDb,
        node_id: String,
        operator_id: OperatorDatabaseId,
        recv: UnboundedReceiver<WorkerAction>,
    ) -> Self {
        Self {
            config,
            metadata_db,
            node_id,
            operator_id,
            recv,
        }
    }

    async fn run(mut self) {
        while let Some(action) = self.recv.recv().await {
            assert!(action.node_id == self.node_id);
            assert!(action.operator_id == self.operator_id);

            if let Err(e) = self.handle_action(action.action).await {
                error!("error handling action `{:?}`: {}", action, e);
            }
        }

        // Only happens if the `Worker` is dropped.
        debug!(
            "Dropping operator handler for node {} and operator {}",
            self.node_id, self.operator_id
        );
    }

    #[instrument(skip(self), err)]
    async fn handle_action(&self, action: Action) -> Result<(), WorkerError> {
        use WorkerError::*;

        match action {
            Action::Start => {
                let operator = Operator::load(
                    self.operator_id,
                    self.config.clone(),
                    self.metadata_db.clone(),
                )
                .await
                .map_err(OperatorLoadError)?;
                spawn_operator(self.config.clone(), self.metadata_db.clone(), operator);
            }

            Action::Stop => {
                // No code path currently calls this.
                unimplemented!()
            }
        }
        Ok(())
    }
}

fn spawn_operator(
    config: Arc<Config>,
    metadata_db: MetadataDb,
    operator: Operator,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let operator_desc = operator.to_string();
        match operator.run(config, metadata_db).await {
            Ok(()) => {
                info!("operator {} finished running", operator_desc);
            }
            Err(e) => {
                error!("error running operator {}: {}", operator_desc, e);
            }
        }
    })
}
