use std::{path::PathBuf, sync::Arc};

use amp_data_store::DataStore;
use datafusion::{
    common::ScalarValue,
    execution::{
        cache::cache_manager::CacheManager,
        config::SessionConfig,
        disk_manager::{DiskManager, DiskManagerBuilder, DiskManagerMode},
        memory_pool::MemoryPool,
        object_store::ObjectStoreRegistry,
        runtime_env::RuntimeEnvBuilder,
    },
};
use js_runtime::isolate_pool::IsolatePool;

use crate::{
    dataset_store::DatasetStore,
    memory_pool::{MemoryPoolKind, make_memory_pool},
};

/// Returns the default DataFusion catalog name used across all session contexts.
fn default_catalog_name() -> ScalarValue {
    ScalarValue::Utf8(Some("amp".to_string()))
}

/// Creates a [`SessionConfig`] with project-wide defaults.
///
/// This loads configuration from environment variables via `SessionConfig::from_env()`,
/// sets the default catalog name, and applies default optimizer/execution settings:
///
/// - `prefer_existing_sort = true` (takes advantage of time-partitioned, sorted files)
/// - `parquet.pushdown_filters = true` (optimizes selective queries)
///
/// Each default is only applied when the corresponding environment variable is not set,
/// allowing overrides without recompilation.
pub fn default_session_config() -> Result<SessionConfig, datafusion::error::DataFusionError> {
    let mut config = SessionConfig::from_env()?.set(
        "datafusion.catalog.default_catalog",
        &default_catalog_name(),
    );

    let opts = config.options_mut();

    // Set `prefer_existing_sort` by default.
    if std::env::var_os("DATAFUSION_OPTIMIZER_PREFER_EXISTING_SORT").is_none() {
        opts.optimizer.prefer_existing_sort = true;
    }

    // Set `parquet.pushdown_filters` by default.
    //
    // See https://github.com/apache/datafusion/issues/3463 for upstream default tracking.
    if std::env::var_os("DATAFUSION_EXECUTION_PARQUET_PUSHDOWN_FILTERS").is_none() {
        opts.execution.parquet.pushdown_filters = true;
    }

    Ok(config)
}

/// Handle to the environment resources used by the query engine.
#[derive(Clone, Debug)]
pub struct ExecEnv {
    /// DataFusion session configuration with project-wide defaults.
    pub session_config: SessionConfig,

    // Inline RuntimeEnv fields for per-query customization
    pub global_memory_pool: Arc<dyn MemoryPool>,
    pub disk_manager: Arc<DiskManager>,
    pub cache_manager: Arc<CacheManager>,
    pub object_store_registry: Arc<dyn ObjectStoreRegistry>,

    // Existing fields
    pub isolate_pool: IsolatePool,

    // Per-query memory limit configuration
    pub query_max_mem_mb: usize,

    /// The data store used for query execution.
    pub store: DataStore,

    /// The dataset store used for dataset resolution and loading.
    pub dataset_store: DatasetStore,
}

/// Creates a ExecEnv with specified memory and cache configuration
///
/// Configures DataFusion runtime environment including memory pools, disk spilling,
/// and parquet footer caching for query execution.
pub fn create(
    max_mem_mb: usize,
    query_max_mem_mb: usize,
    spill_location: &[PathBuf],
    store: DataStore,
    dataset_store: DatasetStore,
) -> Result<ExecEnv, datafusion::error::DataFusionError> {
    let spill_allowed = !spill_location.is_empty();
    let disk_manager_mode = if spill_allowed {
        DiskManagerMode::Directories(spill_location.to_vec())
    } else {
        DiskManagerMode::Disabled
    };

    let disk_manager_builder = DiskManagerBuilder::default().with_mode(disk_manager_mode);

    let memory_pool = {
        let max_mem_bytes = max_mem_mb * 1024 * 1024;
        make_memory_pool(MemoryPoolKind::Greedy, max_mem_bytes)
    };

    let mut runtime_config =
        RuntimeEnvBuilder::new().with_disk_manager_builder(disk_manager_builder);

    runtime_config = runtime_config.with_memory_pool(memory_pool);

    // Build RuntimeEnv to extract components
    let runtime_env = runtime_config.build()?;
    let isolate_pool = IsolatePool::new();

    let session_config = default_session_config()?;

    Ok(ExecEnv {
        session_config,
        global_memory_pool: runtime_env.memory_pool,
        disk_manager: runtime_env.disk_manager,
        cache_manager: runtime_env.cache_manager,
        object_store_registry: runtime_env.object_store_registry,
        isolate_pool,
        query_max_mem_mb,
        store,
        dataset_store,
    })
}
