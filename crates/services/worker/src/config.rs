use std::{path::PathBuf, time::Duration};

use crate::info::WorkerInfo;

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
    pub keep_alive_interval: Option<u64>,

    /// Maximum memory usage for DataFusion query environment (in MB)
    pub max_mem_mb: usize,

    /// Maximum memory per query for DataFusion (in MB)
    pub query_max_mem_mb: usize,

    /// Directory paths for DataFusion query spilling
    pub spill_location: Vec<PathBuf>,

    /// Parquet file configuration
    pub parquet: common::config::ParquetConfig,

    /// Worker build/version information for registration
    pub worker_info: WorkerInfo,
}

impl Config {
    /// Create a dump::config::Config from this worker configuration
    ///
    /// This method extracts only the fields needed by the dump crate for
    /// executing dataset dump operations.
    pub fn dump_config(&self) -> dump::config::Config {
        dump::config::Config {
            poll_interval: self.poll_interval,
            keep_alive_interval: self.keep_alive_interval,
            max_mem_mb: self.max_mem_mb,
            query_max_mem_mb: self.query_max_mem_mb,
            spill_location: self.spill_location.clone(),
            parquet: self.parquet.clone(),
        }
    }
}
