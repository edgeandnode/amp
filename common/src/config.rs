use std::{path::PathBuf, sync::Arc};

use datafusion::{
    error::DataFusionError,
    execution::{
        disk_manager::DiskManagerConfig,
        memory_pool::{FairSpillPool, GreedyMemoryPool, MemoryPool},
        runtime_env::{RuntimeConfig, RuntimeEnv},
    },
};
use serde::Deserialize;

use crate::{BoxError, Store};

pub struct Config {
    pub data_store: Store,
    pub providers_store: Store,
    pub dataset_defs_store: Store,
    pub max_mem_mb: usize,
    pub spill_location: Vec<PathBuf>,
}

#[derive(Debug, Clone, Deserialize)]
struct ConfigFile {
    pub data_dir: String,
    pub providers_dir: String,
    pub dataset_defs_dir: String,
    pub max_mem_mb: usize,
    pub spill_location: Vec<PathBuf>,
}

impl Config {
    pub fn load(file: impl Into<PathBuf>) -> Result<Self, BoxError> {
        let contents = std::fs::read_to_string(file.into())?;
        let config_file: ConfigFile = toml::from_str(&contents)?;
        let data_store = Store::new(config_file.data_dir)?;
        let providers_store = Store::new(config_file.providers_dir)?;
        let dataset_defs_store = Store::new(config_file.dataset_defs_dir)?;

        Ok(Self {
            data_store,
            providers_store,
            dataset_defs_store,
            max_mem_mb: config_file.max_mem_mb,
            spill_location: config_file.spill_location,
        })
    }

    /// For testing purposes only.
    pub fn in_memory() -> Self {
        let data_store = Store::in_memory();
        let providers_store = Store::in_memory();
        let dataset_defs_store = Store::in_memory();

        Self {
            data_store,
            providers_store,
            dataset_defs_store,
            max_mem_mb: 0,
            spill_location: vec![],
        }
    }

    pub fn to_runtime_env(&self) -> Result<RuntimeEnv, DataFusionError> {
        use datafusion::execution::cache::{
            cache_manager::CacheManagerConfig, cache_unit::DefaultFileStatisticsCache,
        };

        let spill_allowed = !self.spill_location.is_empty();
        let disk_manager = if spill_allowed {
            DiskManagerConfig::Disabled
        } else {
            DiskManagerConfig::NewSpecified(self.spill_location.clone())
        };
        let memory_pool: Option<Arc<dyn MemoryPool>> = if self.max_mem_mb > 0 {
            let max_mem_bytes = self.max_mem_mb * 1024 * 1024;

            if spill_allowed {
                Some(Arc::new(FairSpillPool::new(max_mem_bytes)))
            } else {
                Some(Arc::new(GreedyMemoryPool::new(max_mem_bytes)))
            }
        } else {
            None
        };
        let cache_manager = CacheManagerConfig {
            // Caches parquet file statistics. Seems like a good thing.
            table_files_statistics_cache: Some(Arc::new(DefaultFileStatisticsCache::default())),
            // Seems it might lead to staleness in the ListingTable, better not.
            list_files_cache: None,
        };

        let runtime_config = RuntimeConfig {
            disk_manager,
            memory_pool,
            cache_manager,
            ..Default::default()
        };

        RuntimeEnv::new(runtime_config)
    }
}
