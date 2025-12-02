use std::{path::PathBuf, time::Duration};

use common::query_context::QueryEnv;
use datafusion::common::DataFusionError;

/// Configuration specific to dump operations
///
/// This configuration contains only the fields needed by the dump crate for executing
/// dataset dump operations. It is created from a larger configuration by the worker service.
#[derive(Debug, Clone)]
pub struct Config {
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
}

impl Config {
    /// Create a DataFusion query environment from this configuration
    pub fn make_query_env(&self) -> Result<QueryEnv, DataFusionError> {
        common::query_context::create_query_env(
            self.max_mem_mb,
            self.query_max_mem_mb,
            &self.spill_location,
            self.parquet.cache_size_mb,
        )
    }
}
