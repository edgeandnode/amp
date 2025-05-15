use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use datafusion::{
    error::DataFusionError,
    execution::{
        disk_manager::DiskManagerConfig,
        memory_pool::{FairSpillPool, GreedyMemoryPool, MemoryPool},
        runtime_env::{RuntimeEnv, RuntimeEnvBuilder},
    },
};
use figment::{
    providers::{Env, Format as _, Toml},
    Figment,
};
use fs_err as fs;
use metadata_db::{test_metadata_db, MetadataDb, ALLOW_TEMP_DB, KEEP_TEMP_DIRS};
use serde::Deserialize;

use crate::{BoxError, Store};

#[derive(Debug)]
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

#[derive(Debug)]
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
        literal_override: Option<FigmentJson>,
        addrs: Addrs,
    ) -> Result<Self, BoxError> {
        let config_path: PathBuf = fs::canonicalize(file.into())?;
        let contents = fs::read_to_string(&config_path)?;

        let config_file: ConfigFile = {
            let mut config_builder = Figment::new().merge(Toml::string(&contents));
            if env_override {
                config_builder = config_builder.merge(Env::prefixed("NOZZLE_CONFIG_"));
            }
            if let Some(literal_override) = literal_override {
                config_builder = config_builder.merge(literal_override);
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
            (None, true) => test_metadata_db(*KEEP_TEMP_DIRS).await.url().to_string(),
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

    /// For testing purposes only.
    pub async fn in_memory() -> Self {
        let data_store = Arc::new(Store::in_memory());
        let providers_store = Arc::new(Store::in_memory());
        let dataset_defs_store = Arc::new(Store::in_memory());

        let metadata_db_url = test_metadata_db(*KEEP_TEMP_DIRS).await.url().to_string();

        Self {
            data_store,
            providers_store,
            dataset_defs_store,
            metadata_db_url,
            max_mem_mb: 0,
            spill_location: vec![],
            addrs: Default::default(),
        }
    }

    pub fn make_runtime_env(&self) -> Result<RuntimeEnv, DataFusionError> {
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

        let runtime_config = RuntimeEnvBuilder {
            disk_manager,
            memory_pool,
            cache_manager,
            ..Default::default()
        };

        runtime_config.build()
    }

    pub async fn metadata_db(&self) -> Result<MetadataDb, BoxError> {
        MetadataDb::connect(&self.metadata_db_url)
            .await
            .map_err(Into::into)
    }

    pub fn metadata_db_lazy(&self) -> Result<MetadataDb, BoxError> {
        MetadataDb::connect_lazy(&self.metadata_db_url).map_err(Into::into)
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
