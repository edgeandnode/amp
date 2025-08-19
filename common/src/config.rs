use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use datafusion::{
    error::DataFusionError,
    execution::{
        disk_manager::{DiskManagerBuilder, DiskManagerMode},
        memory_pool::{FairSpillPool, GreedyMemoryPool, MemoryPool},
        runtime_env::RuntimeEnvBuilder,
    },
    parquet::basic::{Compression, ZstdLevel},
};
use figment::{
    Figment,
    providers::{Env, Format as _, Toml},
};
use fs_err as fs;
use js_runtime::isolate_pool::IsolatePool;
use metadata_db::{DEFAULT_POOL_SIZE, KEEP_TEMP_DIRS, MetadataDb, temp_metadata_db};
use serde::Deserialize;
use thiserror::Error;

use crate::{Store, query_context::QueryEnv};

#[derive(Debug, Clone)]
pub struct Config {
    pub data_store: Arc<Store>,
    pub providers_store: Arc<Store>,
    pub dataset_defs_store: Arc<Store>,
    pub metadata_db: MetadataDbConfig,
    pub max_mem_mb: usize,
    pub spill_location: Vec<PathBuf>,
    pub microbatch_max_interval: u64,
    pub parquet: ParquetConfig,
    /// Addresses to bind the server to. Used during testing.
    pub addrs: Addrs,
    pub config_path: PathBuf,
}

#[derive(Debug, Clone)]
pub struct Addrs {
    pub flight_addr: SocketAddr,
    pub jsonl_addr: SocketAddr,
    pub registry_service_addr: SocketAddr,
    pub admin_api_addr: SocketAddr,
}

fn default_pool_size() -> u32 {
    DEFAULT_POOL_SIZE
}

fn default_auto_migrate() -> bool {
    true
}

#[derive(Debug, Clone, Deserialize)]
pub struct MetadataDbConfig {
    pub url: Option<String>,
    #[serde(default = "default_pool_size")]
    pub pool_size: u32,
    #[serde(default = "default_auto_migrate")]
    pub auto_migrate: bool,
}

impl Default for MetadataDbConfig {
    fn default() -> Self {
        Self {
            url: None,
            pool_size: DEFAULT_POOL_SIZE,
            auto_migrate: true,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ParquetConfig {
    #[serde(
        default = "default_compression",
        deserialize_with = "deserialize_compression"
    )]
    pub compression: Compression,
    #[serde(default)]
    pub bloom_filters: bool,
}

impl Default for ParquetConfig {
    fn default() -> Self {
        Self {
            compression: default_compression(),
            bloom_filters: false,
        }
    }
}

fn default_compression() -> Compression {
    Compression::ZSTD(ZstdLevel::try_new(1).unwrap())
}

fn deserialize_compression<'de, D>(deserializer: D) -> Result<Compression, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use std::str::FromStr;
    let s = String::deserialize(deserializer)?;
    Compression::from_str(&s).map_err(serde::de::Error::custom)
}

#[derive(Debug, Clone, Deserialize)]
pub struct ConfigFile {
    pub data_dir: String,
    pub providers_dir: String,
    pub dataset_defs_dir: String,
    pub metadata_db_url: Option<String>,
    #[serde(default)]
    pub metadata_db: MetadataDbConfig,
    #[serde(default)]
    pub max_mem_mb: usize,
    #[serde(default)]
    pub spill_location: Vec<PathBuf>,
    pub microbatch_max_interval: Option<u64>,
    pub flight_addr: Option<String>,
    pub jsonl_addr: Option<String>,
    pub registry_service_addr: Option<String>,
    pub admin_api_addr: Option<String>,
    #[serde(default)]
    pub parquet: ParquetConfig,
}

pub type FigmentJson = figment::providers::Data<figment::providers::Json>;

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("IO error at {0}: {1}")]
    Io(PathBuf, std::io::Error),
    #[error("Missing required config at {0}: {1}")]
    MissingConfig(PathBuf, &'static str),
    #[error("Config parse error at {0}: {1}")]
    Figment(PathBuf, figment::Error),
    #[error("Store error at {0}: {1}")]
    Store(PathBuf, crate::store::StoreError),
    #[error("Metadata DB error at {0}: {1}")]
    MetadataDb(PathBuf, metadata_db::Error),
    #[error("Invalid address format for {0}: {1}")]
    InvalidAddress(String, String),
}

impl Config {
    ///
    /// `env_override` allows env vars prefixed with `NOZZLE_CONFIG_` to override config values.
    pub async fn load(
        file: impl Into<PathBuf>,
        env_override: bool,
        config_override: Option<Figment>,
        allow_temp_db: bool,
    ) -> Result<Self, ConfigError> {
        let input_path = file.into();
        let config_path = fs::canonicalize(&input_path)
            .map_err(|err| ConfigError::Io(input_path.clone(), err))?;
        let contents = fs::read_to_string(&config_path)
            .map_err(|err| ConfigError::Io(config_path.clone(), err))?;

        let config_file: ConfigFile = {
            let mut config_builder = Figment::new().merge(Toml::string(&contents));
            if env_override {
                config_builder = config_builder.merge(Env::prefixed("NOZZLE_CONFIG_"));
            }
            if let Some(config_override) = config_override {
                config_builder = config_builder.merge(config_override);
            }
            config_builder
                .extract()
                .map_err(|e| ConfigError::Figment(config_path.clone(), e))?
        };

        // Resolve any filesystem paths relative to the directory of the config file.
        let base = config_path.parent();
        let addrs = Addrs::from_config_file(&config_file, Addrs::default())?;
        let data_store = Store::new(config_file.data_dir, base)
            .map_err(|e| ConfigError::Store(config_path.clone(), e))?;
        let providers_store = Store::new(config_file.providers_dir, base)
            .map_err(|e| ConfigError::Store(config_path.clone(), e))?;
        let dataset_defs_store = Store::new(config_file.dataset_defs_dir, base)
            .map_err(|e| ConfigError::Store(config_path.clone(), e))?;

        let metadata_db = if config_file.metadata_db.url.is_some() {
            let db_config = config_file.metadata_db.clone();
            db_config
        } else if let Some(url) = config_file.metadata_db_url {
            MetadataDbConfig {
                url: Some(url),
                pool_size: config_file.metadata_db.pool_size,
                auto_migrate: config_file.metadata_db.auto_migrate,
            }
        } else if allow_temp_db {
            MetadataDbConfig {
                url: Some(
                    temp_metadata_db(*KEEP_TEMP_DIRS, config_file.metadata_db.pool_size)
                        .await
                        .url()
                        .to_string(),
                ),
                pool_size: config_file.metadata_db.pool_size,
                auto_migrate: config_file.metadata_db.auto_migrate,
            }
        } else {
            return Err(ConfigError::MissingConfig(
                config_path.clone(),
                "metadata_db_url or metadata_db.url",
            ));
        };

        Ok(Self {
            data_store: Arc::new(data_store),
            providers_store: Arc::new(providers_store),
            dataset_defs_store: Arc::new(dataset_defs_store),
            metadata_db,
            max_mem_mb: config_file.max_mem_mb,
            spill_location: config_file.spill_location,
            microbatch_max_interval: config_file.microbatch_max_interval.unwrap_or(100_000),
            parquet: config_file.parquet,
            addrs,
            config_path,
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
            table_files_statistics_cache: Some(Arc::new(DefaultFileStatisticsCache::default())),
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
        Ok(QueryEnv {
            df_env: Arc::new(runtime_env),
            isolate_pool,
        })
    }

    pub async fn metadata_db(&self) -> Result<MetadataDb, ConfigError> {
        let url = self.metadata_db.url.as_ref().ok_or_else(|| {
            ConfigError::MissingConfig(self.config_path.clone(), "metadata_db.url")
        })?;
        MetadataDb::connect_with_config(
            url,
            self.metadata_db.pool_size,
            self.metadata_db.auto_migrate,
        )
        .await
        .map_err(|e| ConfigError::MetadataDb(self.config_path.clone(), e))
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

impl Addrs {
    pub fn from_config_file(
        config_file: &ConfigFile,
        default_addrs: Addrs,
    ) -> Result<Self, ConfigError> {
        let parse_addr = |addr_str: &Option<String>,
                          default: SocketAddr,
                          name: &str|
         -> Result<SocketAddr, ConfigError> {
            match addr_str {
                Some(addr) => addr
                    .parse::<SocketAddr>()
                    .map_err(|e| ConfigError::InvalidAddress(name.to_string(), e.to_string())),
                None => Ok(default),
            }
        };

        Ok(Self {
            flight_addr: parse_addr(
                &config_file.flight_addr,
                default_addrs.flight_addr,
                "flight_addr",
            )?,
            jsonl_addr: parse_addr(
                &config_file.jsonl_addr,
                default_addrs.jsonl_addr,
                "jsonl_addr",
            )?,
            registry_service_addr: parse_addr(
                &config_file.registry_service_addr,
                default_addrs.registry_service_addr,
                "registry_service_addr",
            )?,
            admin_api_addr: parse_addr(
                &config_file.admin_api_addr,
                default_addrs.admin_api_addr,
                "admin_api_addr",
            )?,
        })
    }
}
