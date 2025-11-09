//! Stream management and orchestration.
//!
//! This module handles spawning streaming tasks for all tables in a dataset,
//! managing restart logic with exponential backoff, and coordinating graceful
//! shutdown of all tasks.

use std::time::Duration;

use amp_client::AmpClient;
use monitoring::logging;
use sqlx::PgPool;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::{config::SyncConfig, engine::Engine, manifest::Manifest, task::StreamTask};

/// Maximum number of restart attempts per table
const MAX_RESTART_ATTEMPTS: u32 = 10;

/// Initial backoff duration in seconds
const INITIAL_BACKOFF_SECS: u64 = 1;

/// Maximum backoff duration in seconds (5 minutes)
const MAX_BACKOFF_SECS: u64 = 300;

/// Manages streaming tasks for multiple tables with restart logic and graceful shutdown.
pub struct StreamManager {
    tasks: Vec<JoinHandle<()>>,
    shutdown: CancellationToken,
}

impl StreamManager {
    /// Spawns streaming tasks for all tables in the manifest.
    ///
    /// Each table gets a dedicated task that:
    /// - Processes streaming data from Amp
    /// - Auto-restarts on failure with exponential backoff
    /// - Respects shutdown signals
    ///
    /// # Arguments
    ///
    /// * `manifest` - Dataset manifest with table definitions
    /// * `config` - Ampsync configuration
    /// * `engine` - Database engine for table operations
    /// * `client` - Amp client for streaming queries
    /// * `pool` - PostgreSQL connection pool
    pub fn spawn_all(
        manifest: &Manifest,
        config: &SyncConfig,
        engine: Engine,
        client: AmpClient,
        pool: PgPool,
    ) -> Self {
        let shutdown = CancellationToken::new();
        let mut tasks = Vec::new();

        for table_name in manifest.tables.keys() {
            // Build streaming query
            let query = format!(
                "SELECT * FROM \"{}\".\"{}\" SETTINGS stream = true",
                config.dataset_name, table_name
            );

            info!("Creating stream for table: {}", table_name);

            // Clone data needed for task restart loop
            let task_table_name = table_name.clone();
            let task_dataset_name = config.dataset_name.clone();
            let task_query = query.clone();
            let task_engine = engine.clone();
            let task_client = client.clone();
            let task_pool = pool.clone();
            let task_retention = config.retention_blocks;
            let task_shutdown = shutdown.clone();

            // Spawn task with restart logic
            let task_handle = tokio::spawn(async move {
                let mut restart_count = 0;

                loop {
                    // Check shutdown before (re)starting
                    if task_shutdown.is_cancelled() {
                        info!(
                            table = %task_table_name,
                            "shutdown_before_restart"
                        );
                        break;
                    }

                    // Create new task instance for each attempt
                    let task = StreamTask::new(
                        task_table_name.clone(),
                        task_dataset_name.clone(),
                        task_query.clone(),
                        task_engine.clone(),
                        task_client.clone(),
                        task_pool.clone(),
                        task_retention,
                        task_shutdown.clone(),
                    );

                    // Run the task
                    let result = task.run().await;

                    match result {
                        Ok(()) => {
                            info!(
                                table = %task_table_name,
                                "task_stopped_cleanly"
                            );
                            break;
                        }
                        Err(err) => {
                            restart_count += 1;

                            if restart_count >= MAX_RESTART_ATTEMPTS {
                                error!(
                                    table = %task_table_name,
                                    error = %err, error_source = logging::error_source(&err),
                                    restart_count = restart_count,
                                    "max_restart_attempts_reached"
                                );
                                break;
                            }

                            // Calculate exponential backoff with cap
                            let backoff_duration = calculate_backoff(restart_count);

                            warn!(
                                table = %task_table_name,
                                error = %err, error_source = logging::error_source(&err),
                                restart_count = restart_count,
                                backoff_secs = backoff_duration.as_secs(),
                                "task_failed_restarting"
                            );

                            // Wait before restart
                            tokio::time::sleep(backoff_duration).await;
                        }
                    }
                }
            });

            tasks.push(task_handle);
            info!("Spawned task for table: {}", table_name);
        }

        Self { tasks, shutdown }
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
