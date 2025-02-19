use std::{collections::BTreeMap, pin::pin, sync::Arc, time::Duration};

use common::{config::Config, BoxError};
use futures::{TryFutureExt as _, TryStreamExt};
use log::{debug, error};
use metadata_db::{LocationId, MetadataDb, WorkerAction};
use thiserror::Error;
use tokio::{
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
    task::JoinHandle,
};

use crate::WORKER_ACTIONS_PG_CHANNEL;

pub struct Worker {
    config: Arc<Config>,
    metadata_db: MetadataDb,
    node_id: String,

    // To prevent race conditions, actions for a same location are processed sequentially.
    // Each location has a dedicated handler task.
    action_queue: BTreeMap<LocationId, UnboundedSender<WorkerAction>>,
}

#[derive(Error, Debug)]
pub enum WorkerError {
    #[error("heartbeat task failed: {0}")]
    HeartbeatError(BoxError),

    #[error("error listening to worker actions: {0}")]
    ListenError(sqlx::Error),

    #[error("location handler panicked")]
    HandlerPanic,
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
        let location_task = self.action_queue.entry(action.location).or_insert_with(|| {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            let location_handler = LocationHandler::new(
                self.metadata_db.clone(),
                self.node_id.clone(),
                action.location,
                rx,
            );
            tokio::spawn(location_handler.run());
            tx
        });
        location_task
            .send(action)
            .map_err(|_| WorkerError::HandlerPanic)
    }
}

struct LocationHandler {
    metadata_db: MetadataDb,
    node_id: String,
    location: LocationId,
    recv: UnboundedReceiver<WorkerAction>,
}

impl LocationHandler {
    fn new(
        metadata_db: MetadataDb,
        node_id: String,
        location: LocationId,
        recv: UnboundedReceiver<WorkerAction>,
    ) -> Self {
        Self {
            metadata_db,
            node_id,
            location,
            recv,
        }
    }

    async fn run(mut self) {
        while let Some(action) = self.recv.recv().await {
            assert!(action.node_id == self.node_id);
            assert!(action.location == self.location);
            self.handle_action(action).await.unwrap();
        }

        // Only happens if the `Worker` is dropped.
        debug!(
            "Dropping location handler for node {} and location {}",
            self.node_id, self.location
        );
    }

    async fn handle_action(&self, action: WorkerAction) -> Result<(), WorkerError> {
        todo!()
    }
}
