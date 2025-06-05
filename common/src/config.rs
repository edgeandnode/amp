use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use datafusion::{
    error::DataFusionError,
    execution::{
        disk_manager::{DiskManagerBuilder, DiskManagerMode},
        memory_pool::{FairSpillPool, GreedyMemoryPool, MemoryPool},
        runtime_env::RuntimeEnvBuilder,
    },
};
use figment::{
    providers::{Env, Format as _, Toml},
    Figment,
};
use fs_err as fs;
use js_runtime::isolate_pool::IsolatePool;
use metadata_db::{temp_metadata_db, MetadataDb, ALLOW_TEMP_DB, KEEP_TEMP_DIRS};
use serde::Deserialize;

use crate::{query_context::QueryEnv, BoxError, Store};

#[derive(Debug, Clone)]
pub struct Config {
    pub data_store: Arc<Store>,
    pub providers_store: Arc<Store>,
    pub dataset_defs_store: Arc<Store>,
    pub metadata_db_url: String,
    pub max_mem_mb: usize,
    pub spill_location: Vec<PathBuf>,
    /// Addresses to bind the server to. Used during testing.
    pub addrs: Addrs,
}

#[derive(Debug, Clone)]
pub struct Addrs {
    pub flight_addr: SocketAddr,
    pub jsonl_addr: SocketAddr,
    pub registry_service_addr: SocketAddr,
    pub admin_api_addr: SocketAddr,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ConfigFile {
    pub data_dir: String,
    pub providers_dir: String,
    pub dataset_defs_dir: String,
    pub metadata_db_url: Option<String>,
    pub max_mem_mb: usize,
    pub spill_location: Vec<PathBuf>,
}

pub type FigmentJson = figment::providers::Data<figment::providers::Json>;

impl Config {
    ///
    /// `env_override` allows env vars prefixed with `NOZZLE_CONFIG_` to override config values.
    pub async fn load(
        file: impl Into<PathBuf>,
        env_override: bool,
        config_override: Option<Figment>,
        addrs: Addrs,
    ) -> Result<Self, BoxError> {
        let config_path: PathBuf = fs::canonicalize(file.into())?;
        let contents = fs::read_to_string(&config_path)?;

        let config_file: ConfigFile = {
            let mut config_builder = Figment::new().merge(Toml::string(&contents));
            if env_override {
                config_builder = config_builder.merge(Env::prefixed("NOZZLE_CONFIG_"));
            }
            if let Some(config_override) = config_override {
                config_builder = config_builder.merge(config_override);
            }
            config_builder.extract()?
        };

        // Resolve any filesystem paths relative to the directory of the config file.
        let base = config_path.parent();
        let data_store = Store::new(config_file.data_dir, base)?;
        let providers_store = Store::new(config_file.providers_dir, base)?;
        let dataset_defs_store = Store::new(config_file.dataset_defs_dir, base)?;

        let metadata_db_url = match (config_file.metadata_db_url, *ALLOW_TEMP_DB) {
            (Some(url), _) => url,
            (None, true) => temp_metadata_db(*KEEP_TEMP_DIRS).await.url().to_string(),
            (None, false) => {
                return Err("No metadata db url provided and allow_use_temp_db is false".into())
            }
        };

        Ok(Self {
            data_store: Arc::new(data_store),
            providers_store: Arc::new(providers_store),
            dataset_defs_store: Arc::new(dataset_defs_store),
            metadata_db_url,
            max_mem_mb: config_file.max_mem_mb,
            spill_location: config_file.spill_location,
            addrs,
        })
    }

    pub fn make_query_env(&self) -> Result<QueryEnv, DataFusionError> {
        use datafusion::execution::cache::{
            cache_manager::CacheManagerConfig, cache_unit::DefaultFileStatisticsCache,
        };

        let spill_allowed = !self.spill_location.is_empty();
        let disk_manager_mode = if spill_allowed {
            DiskManagerMode::Disabled
        } else {
            DiskManagerMode::Directories(self.spill_location.clone())
        };

        let disk_manager_builder = DiskManagerBuilder::default().with_mode(disk_manager_mode);

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
        let mut runtime_config = RuntimeEnvBuilder::new()
            .with_disk_manager_builder(disk_manager_builder)
            .with_cache_manager(cache_manager);

        if let Some(memory_pool) = memory_pool {
            runtime_config = runtime_config.with_memory_pool(memory_pool);
        }

        let runtime_env = runtime_config.build()?;
        let isolate_pool = IsolatePool::new();
        return Ok(QueryEnv {
            df_env: Arc::new(runtime_env),
            isolate_pool,
        });
    }

    pub async fn metadata_db(&self) -> Result<MetadataDb, BoxError> {
        MetadataDb::connect(&self.metadata_db_url)
            .await
            .map_err(Into::into)
    }
}

impl Default for Addrs {
    fn default() -> Self {
        Self {
            flight_addr: ([0, 0, 0, 0], 1602).into(),
            jsonl_addr: ([0, 0, 0, 0], 1603).into(),
            registry_service_addr: ([0, 0, 0, 0], 1611).into(),
            admin_api_addr: ([0, 0, 0, 0], 1610).into(),
        }
    }
}
