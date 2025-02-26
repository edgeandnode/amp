use std::{collections::BTreeMap, pin::pin, sync::Arc, time::Duration};

use common::{config::Config, BoxError};
use futures::{TryFutureExt as _, TryStreamExt};
use log::{debug, error, warn};
use metadata_db::MetadataDb;
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
/// - Accept the scheduling by creating an entry in the `scheduled_operators` table.
/// - Lock the locations by setting `locked_by` in the `locations` table.
/// - Start the operator.
///
/// Stop action:
/// - Stop the operator.
/// - Release the location by deleting the row from the `scheduled_operators` table.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum Action {
    Start,
    Stop,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct WorkerAction {
    pub node_id: String,
    pub operator: Operator,
    pub action: Action,
}

pub struct Worker {
    config: Arc<Config>,
    metadata_db: MetadataDb,
    node_id: String,

    // To prevent start/stop race conditions, actions for a same operator are processed sequentially.
    // Each operator has a dedicated handler task.
    action_queue: BTreeMap<Operator, UnboundedSender<WorkerAction>>,
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

        // Heartbeat task. This will also register the worker if running for the first time.
        let heartbeat_interval = Duration::from_secs(1);
        let heartbeat_task: JoinHandle<Result<(), WorkerError>> = {
            let node_id = self.node_id.clone();
            let metadata_db = self.metadata_db.clone();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(heartbeat_interval);
                loop {
                    metadata_db
                        .heartbeat(&node_id)
                        .await
                        .map_err(|e| HeartbeatError(e.into()))?;
                    interval.tick().await;
                }
            })
        };

        let scheduler_loop = {
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

            async move {
                let mut stream = pin!(action_stream);
                while let Some(action) = stream.try_next().await? {
                    self.handle_action(action)?;
                }
                Ok(())
            }
        };

        // Run forever or until a fatal error
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
            .entry(action.operator.clone())
            .or_insert_with(|| {
                let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
                let operator_handler = OperatorHandler::new(
                    self.config.clone(),
                    self.metadata_db.clone(),
                    self.node_id.clone(),
                    action.operator.clone(),
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
    operator: Operator,
    recv: UnboundedReceiver<WorkerAction>,
}

impl OperatorHandler {
    fn new(
        config: Arc<Config>,
        metadata_db: MetadataDb,
        node_id: String,
        operator: Operator,
        recv: UnboundedReceiver<WorkerAction>,
    ) -> Self {
        Self {
            config,
            metadata_db,
            node_id,
            operator,
            recv,
        }
    }

    async fn run(mut self) {
        while let Some(action) = self.recv.recv().await {
            assert!(action.node_id == self.node_id);
            assert!(action.operator == self.operator);

            // TODO: Remove unwrap, or make the function not error.
            self.handle_action(action).await.unwrap();
        }

        // Only happens if the `Worker` is dropped.
        debug!(
            "Dropping operator handler for node {} and operator {}",
            self.node_id, self.operator
        );
    }

    #[instrument(skip(self), err)]
    async fn handle_action(&self, action: WorkerAction) -> Result<(), WorkerError> {
        match action.action {
            Action::Start => {
                let json = serde_json::to_string(&self.operator).unwrap();

                if self.metadata_db.operator_is_scheduled(&self.node_id, &json).await? {
                    warn!("operator already scheduled to this node, ignoring");
                    return Ok(());
                }

                self.metadata_db
                    .schedule_operator(&self.node_id, &json, action.operator.output_locations())
                    .await?;

                // Spawn the operator.
                let config = self.config.clone();
                let metadata_db = self.metadata_db.clone();
                tokio::spawn(async move {
                    let operator_desc = action.operator.to_string();
                    match action.operator.run(config, metadata_db).await {
                        Ok(()) => {
                            info!("operator {} finished running", operator_desc);
                        }
                        Err(e) => {
                            error!("error running operator {}: {}", operator_desc, e);
                        }
                    }
                });
            }

            Action::Stop => {
                // No code path currently calls this.
                unimplemented!()
            }
        }
        Ok(())
    }
}
