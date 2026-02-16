use std::path::PathBuf;

/// Server-specific configuration
///
/// Contains only the configuration fields needed by the server service
/// for query execution and streaming.
#[derive(Debug, Clone)]
pub struct Config {
    /// Maximum interval for streaming server microbatches (in blocks)
    pub server_microbatch_max_interval: u64,
    /// Keep-alive interval for streaming server (in seconds)
    pub keep_alive_interval: u64,
    /// Maximum memory the server can use (in MB, 0 = unlimited)
    pub max_mem_mb: usize,
    /// Per-query memory limit (in MB, 0 = unlimited)
    pub query_max_mem_mb: usize,
    /// Paths for DataFusion temporary files for spill-to-disk
    pub spill_location: Vec<PathBuf>,
}
