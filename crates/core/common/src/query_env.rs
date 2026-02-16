use std::{path::PathBuf, sync::Arc};

use amp_data_store::DataStore;
use datafusion::execution::{
    cache::cache_manager::CacheManager,
    disk_manager::{DiskManager, DiskManagerBuilder, DiskManagerMode},
    memory_pool::MemoryPool,
    object_store::ObjectStoreRegistry,
    runtime_env::RuntimeEnvBuilder,
};
use js_runtime::isolate_pool::IsolatePool;

use crate::memory_pool::{MemoryPoolKind, make_memory_pool};

/// Handle to the environment resources used by the query engine.
#[derive(Clone, Debug)]
pub struct QueryEnv {
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
}

/// Creates a QueryEnv with specified memory and cache configuration
///
/// Configures DataFusion runtime environment including memory pools, disk spilling,
/// and parquet footer caching for query execution.
pub fn create(
    max_mem_mb: usize,
    query_max_mem_mb: usize,
    spill_location: &[PathBuf],
    store: DataStore,
) -> Result<QueryEnv, datafusion::error::DataFusionError> {
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

    Ok(QueryEnv {
        global_memory_pool: runtime_env.memory_pool,
        disk_manager: runtime_env.disk_manager,
        cache_manager: runtime_env.cache_manager,
        object_store_registry: runtime_env.object_store_registry,
        isolate_pool,
        query_max_mem_mb,
        store,
    })
}
