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

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub data_location: String,
    pub max_mem_mb: usize,
    pub spill_location: Vec<PathBuf>,
}

impl Config {
    pub fn load(file: PathBuf) -> anyhow::Result<Self> {
        let contents = std::fs::read_to_string(file)?;
        toml::from_str(&contents).map_err(Into::into)
    }

    pub fn location_only(data_location: String) -> Self {
        Self {
            data_location,
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
