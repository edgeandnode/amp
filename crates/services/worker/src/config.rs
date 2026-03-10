use std::{path::PathBuf, time::Duration};

use amp_config::{WorkerEventsConfig, worker_core::ParquetConfig};

/// Configuration specific to the worker service
///
/// This configuration contains all fields needed by the worker service to execute
/// job processing operations. It is created from the common configuration by the
/// ampd binary.
#[derive(Debug, Clone)]
pub struct Config {
    /// Microbatch maximum interval for derived datasets (in blocks)
    pub microbatch_max_interval: u64,

    /// Poll interval for raw datasets
    pub poll_interval: Duration,

    /// Keep-alive interval for streaming queries
    pub keep_alive_interval: u64,

    /// Maximum memory usage for DataFusion query environment (in MB)
    pub max_mem_mb: usize,

    /// Maximum memory per query for DataFusion (in MB)
    pub query_max_mem_mb: usize,

    /// Directory paths for DataFusion query spilling
    pub spill_location: Vec<PathBuf>,

    /// Parquet file configuration
    pub parquet: ParquetConfig,

    /// Optional event streaming configuration
    pub events_config: WorkerEventsConfig,
}
