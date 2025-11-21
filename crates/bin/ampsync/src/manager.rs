//! Stream management and orchestration.
//!
//! This module handles spawning streaming tasks for all tables in a dataset,
//! managing restart logic with exponential backoff, and coordinating graceful
//! shutdown of all tasks.

use std::time::Duration;

use amp_client::AmpClient;
use common::BlockNum;
use datasets_common::reference::Reference;
use monitoring::logging;
use sqlx::PgPool;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::{config::SyncConfig, engine::Engine, task::StreamTask};

/// Maximum number of restart attempts per table
const MAX_RESTART_ATTEMPTS: u32 = 10;

/// Initial backoff duration in seconds
const INITIAL_BACKOFF_SECS: u64 = 1;

/// Maximum backoff duration in seconds (5 minutes)
const MAX_BACKOFF_SECS: u64 = 300;

/// Configuration for a single table's streaming task.
#[derive(Clone)]
struct TableTaskConfig {
    table_name: String,
    dataset: Reference,
    engine: Engine,
    client: AmpClient,
    pool: PgPool,
    retention: BlockNum,
    shutdown: CancellationToken,
}

/// Manages streaming tasks for multiple tables with restart logic and graceful shutdown.
pub struct StreamManager {
    tasks: Vec<JoinHandle<()>>,
    shutdown: CancellationToken,
}

impl StreamManager {
    /// Creates a new StreamManager and spawns streaming tasks for all specified tables.
    ///
    /// Each table gets a dedicated task that:
    /// - Processes streaming data from Amp
    /// - Auto-restarts on failure with exponential backoff
    /// - Respects shutdown signals
    ///
    /// # Arguments
    ///
    /// * `tables` - List of table names to sync
    /// * `dataset` - Fully resolved dataset reference
    /// * `config` - Ampsync configuration
    /// * `engine` - Database engine for table operations
    /// * `client` - Amp client for streaming queries
    /// * `pool` - PostgreSQL connection pool
    pub fn new(
        tables: &[String],
        dataset: Reference,
        config: &SyncConfig,
        engine: Engine,
        client: AmpClient,
        pool: PgPool,
    ) -> Self {
        let shutdown = CancellationToken::new();

        let tasks = tables
            .iter()
            .map(|table_name| {
                info!("Creating stream for table: {}", table_name);

                let task_config = TableTaskConfig {
                    table_name: table_name.clone(),
                    dataset: dataset.clone(),
                    engine: engine.clone(),
                    client: client.clone(),
                    pool: pool.clone(),
                    retention: config.retention_blocks,
                    shutdown: shutdown.clone(),
                };

                let handle = tokio::spawn(Self::run_task_with_restart(task_config));
                info!("Spawned task for table: {}", table_name);
                handle
            })
            .collect();

        Self { tasks, shutdown }
    }

    /// Runs a single table's streaming task with automatic restart on failure.
    async fn run_task_with_restart(config: TableTaskConfig) {
        let mut restart_count = 0;

        loop {
            // Check shutdown before (re)starting
            if config.shutdown.is_cancelled() {
                info!(table = %config.table_name, "shutdown_before_restart");
                break;
            }

            // Build and run task
            match Self::build_and_run_task(&config).await {
                Ok(()) => {
                    info!(table = %config.table_name, "task_stopped_cleanly");
                    break;
                }
                Err(err) => {
                    restart_count += 1;

                    if restart_count >= MAX_RESTART_ATTEMPTS {
                        error!(
                            table = %config.table_name,
                            error = %err,
                            error_source = logging::error_source(err.as_ref()),
                            restart_count = restart_count,
                            "max_restart_attempts_reached"
                        );
                        break;
                    }

                    let backoff_duration = calculate_backoff(restart_count);
                    warn!(
                        table = %config.table_name,
                        error = %err,
                        error_source = logging::error_source(err.as_ref()),
                        restart_count = restart_count,
                        backoff_secs = backoff_duration.as_secs(),
                        "task_failed_restarting"
                    );

                    tokio::time::sleep(backoff_duration).await;
                }
            }
        }
    }

    /// Builds and runs a single streaming task.
    async fn build_and_run_task(
        config: &TableTaskConfig,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let task = StreamTask::new(
            config.table_name.clone(),
            config.dataset.clone(),
            config.engine.clone(),
            config.client.clone(),
            config.pool.clone(),
            config.retention,
            config.shutdown.clone(),
        )
        .await
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        task.run()
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
    }

    /// Gracefully shuts down all streaming tasks.
    ///
    /// This method:
    /// 1. Cancels the shutdown token (all tasks receive signal)
    /// 2. Waits for all tasks to complete cleanly
    pub async fn shutdown(self) {
        info!("Shutdown signal received, stopping tasks...");
        self.shutdown.cancel();

        // Wait for all tasks to complete
        for task in self.tasks {
            let _ = task.await;
        }

        info!("All tasks stopped");
    }
}

/// Calculate exponential backoff duration with cap.
///
/// Uses exponential backoff similar to `backon`, but with manual implementation
/// to support shutdown signaling and restart count tracking.
///
/// # Arguments
///
/// - `restart_count`: Number of restart attempts (1-indexed)
///
/// # Returns
///
/// Backoff duration, capped at MAX_BACKOFF_SECS (300s / 5 minutes)
fn calculate_backoff(restart_count: u32) -> Duration {
    let backoff_secs = INITIAL_BACKOFF_SECS
        .saturating_mul(2u64.pow(restart_count.saturating_sub(1).min(8)))
        .min(MAX_BACKOFF_SECS);
    Duration::from_secs(backoff_secs)
}
