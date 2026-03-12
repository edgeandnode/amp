use std::{path::PathBuf, sync::Arc, time::Duration};

use amp_data_store::DataStore;
use amp_worker_core::{ParquetConfig, metrics::MetricsRegistry, progress::ProgressReporter};
use common::{datasets_cache::DatasetsCache, udfs::eth_call::EthCallUdfsCache};
use js_runtime::isolate_pool::IsolatePool;
use metadata_db::{MetadataDb, NotificationMultiplexerHandle};

/// Job context for derived dataset materialization.
///
/// Contains configuration and runtime dependencies needed by the derived dataset crate,
/// making dependencies explicit. Constructed by the worker service before
/// dispatching a derived dataset job.
#[derive(Clone)]
pub struct Context {
    /// Job configuration parameters.
    pub config: Config,
    /// Connection pool for the metadata database.
    pub metadata_db: MetadataDb,
    /// In-memory cache of resolved dataset definitions.
    pub datasets_cache: DatasetsCache,
    /// Cache of compiled ethcall UDF definitions.
    pub ethcall_udfs_cache: EthCallUdfsCache,
    /// Object store abstraction for reading/writing Parquet files.
    pub data_store: DataStore,
    /// Pool of reusable V8 isolates for JavaScript UDF execution.
    pub isolate_pool: IsolatePool,
    /// Shared notification multiplexer for streaming queries.
    pub notification_multiplexer: Arc<NotificationMultiplexerHandle>,
    /// Optional job-specific metrics registry.
    pub metrics: Option<Arc<MetricsRegistry>>,
    /// Optional progress reporter for external event streaming.
    pub progress_reporter: Option<Arc<dyn ProgressReporter>>,
}

/// Configuration parameters for derived dataset materialization.
///
/// Groups tunable settings that control how the derived dataset job operates,
/// separate from runtime service dependencies.
#[derive(Clone)]
pub struct Config {
    /// Keep-alive interval for streaming queries.
    pub keep_alive_interval: u64,
    /// Maximum memory usage for DataFusion query environment (in MB).
    pub max_mem_mb: usize,
    /// Maximum memory per query for DataFusion (in MB).
    pub query_max_mem_mb: usize,
    /// Directory paths for DataFusion query spilling.
    pub spill_location: Vec<PathBuf>,
    /// Progress event emission interval.
    pub progress_interval: Duration,
    /// Parquet file configuration.
    pub parquet_writer: ParquetConfig,
    /// Maximum block interval for micro-batch processing.
    pub microbatch_max_interval: u64,
}
