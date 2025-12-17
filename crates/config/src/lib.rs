use std::{net::SocketAddr, path::PathBuf, sync::LazyLock, time::Duration};

use common::{query_context::QueryEnv, store::ObjectStoreUrl};
use datafusion::error::DataFusionError;
pub use dump::{
    CollectorConfig, CompactionAlgorithmConfig, CompactorConfig, ConfigDuration, ParquetConfig,
    SizeLimitConfig,
};
use figment::{
    Figment,
    providers::{Env, Format as _, Toml},
};
use fs_err as fs;
use metadata_db::{DEFAULT_POOL_SIZE, MetadataDb};
use metadata_db_postgres::service::Handle;
pub use monitoring::config::OpenTelemetryConfig;
use serde::Deserialize;
use thiserror::Error;
use tokio::sync::OnceCell;

/// Whether to keep the temporary directory after the database is dropped
///
/// This is set to `false` by default, but can be overridden by the `KEEP_TEMP_DIRS` environment
/// variable.
static KEEP_TEMP_DIRS: LazyLock<bool> = LazyLock::new(|| {
    std::env::var("KEEP_TEMP_DIRS")
        .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
        .unwrap_or(false)
});

/// Global singleton temporary metadata database instance
///
/// This is a shared instance of the temporary database that can be used by the config system.
/// The service future is spawned automatically when the database is first accessed.
static GLOBAL_METADATA_DB: OnceCell<Handle> = OnceCell::const_new();

/// Gets or creates the global singleton temporary metadata database
///
/// Service is spawned automatically on first access. Used by config system
/// for backward compatibility with the old `temp_metadata_db()` function.
async fn global_metadata_db(keep: bool) -> &'static Handle {
    GLOBAL_METADATA_DB
        .get_or_init(|| async {
            let (handle, fut) = metadata_db_postgres::service::new(keep);
            // Spawn the service future to keep the database alive
            tokio::spawn(fut);
            handle
        })
        .await
}

#[derive(Debug, Clone)]
pub struct Config {
    pub data_store_url: ObjectStoreUrl,
    pub providers_store_url: ObjectStoreUrl,
    pub manifests_store_url: ObjectStoreUrl,
    pub metadata_db: MetadataDbConfig,
    pub max_mem_mb: usize,
    pub query_max_mem_mb: usize,
    pub spill_location: Vec<PathBuf>,
    /// Maximum interval for derived dataset dump microbatches
    pub microbatch_max_interval: u64,
    /// Maximum interval for streaming server microbatches
    pub server_microbatch_max_interval: u64,
    pub opentelemetry: Option<OpenTelemetryConfig>,
    /// Addresses to bind the server to. Used during testing.
    pub addrs: Addrs,
    pub config_path: PathBuf,
    pub parquet: ParquetConfig,
    pub poll_interval: Duration,
    pub keep_alive_interval: Option<u64>,
    /// Build information (version, commit SHA, timestamps)
    pub build_info: BuildInfo,
}

#[derive(Debug, Clone)]
pub struct Addrs {
    pub flight_addr: SocketAddr,
    pub jsonl_addr: SocketAddr,
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
    /// Database connection URL
    pub url: Option<String>,
    /// Size of the connection pool (default: 10)
    #[serde(default = "default_pool_size")]
    pub pool_size: u32,
    /// Automatically run database migrations on startup (default: true)
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

/// Build information populated from vergen at compile time.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BuildInfo {
    /// Git describe output (e.g. `v0.0.22-15-g8b065bde`)
    pub version: String,
    /// Full commit SHA hash (e.g. `8b065bde1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d`)
    pub commit_sha: String,
    /// Commit timestamp in ISO 8601 format (e.g. `2025-10-30T11:14:07Z`)
    pub commit_timestamp: String,
    /// Build date (e.g. `2025-10-30`)
    pub build_date: String,
}

impl Default for BuildInfo {
    fn default() -> Self {
        Self {
            version: "unknown".to_string(),
            commit_sha: "unknown".to_string(),
            commit_timestamp: "unknown".to_string(),
            build_date: "unknown".to_string(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ConfigFile {
    // Storage paths
    /// Where the extracted datasets are stored.
    data_dir: String,
    /// Path to a providers directory. Each provider is configured as a separate toml file in this directory.
    providers_dir: String,
    /// Path to a directory containing dataset manifest files.
    #[serde(default, alias = "dataset_defs_dir")]
    manifests_dir: String,

    // Memory and performance
    /// How much memory the server can use in MB. 0 means unlimited (default: 0)
    #[serde(default)]
    pub max_mem_mb: usize,
    /// Per-query memory limit in MB. 0 means unlimited per-query (default: 0)
    #[serde(default)]
    pub query_max_mem_mb: usize,
    /// Paths for DataFusion temporary files for spill-to-disk (default: [])
    #[serde(default)]
    pub spill_location: Vec<PathBuf>,

    // Operational timing
    /// Polling interval for new blocks during dump in seconds (default: 1.0)
    pub poll_interval_secs: ConfigDuration<1>,
    /// Max interval for derived dataset dump microbatches in blocks (default: 100000)
    pub microbatch_max_interval: Option<u64>,
    /// Max interval for streaming server microbatches in blocks (default: 1000)
    pub server_microbatch_max_interval: Option<u64>,
    /// Keep-alive interval for streaming server in seconds (default: 30; min: 30)
    pub keep_alive_interval: Option<u64>,

    // Service addresses
    /// Arrow Flight RPC server address (default: "0.0.0.0:1602")
    pub flight_addr: Option<String>,
    /// JSON Lines server address (default: "0.0.0.0:1603")
    pub jsonl_addr: Option<String>,
    /// Admin API server address (default: "0.0.0.0:1610")
    pub admin_api_addr: Option<String>,

    // Database configuration (legacy top-level field + table)
    /// Connection to the metadata DB (legacy, prefer metadata_db.url)
    pub metadata_db_url: Option<String>,
    #[serde(default)]
    pub metadata_db: MetadataDbConfig,

    // Observability
    pub opentelemetry: Option<OpenTelemetryConfig>,

    // Writer/Parquet configuration
    #[serde(default)]
    pub writer: ParquetConfig,
}

impl ConfigFile {
    /// Returns the data directory path where Parquet files are stored.
    pub fn data_dir(&self) -> &str {
        &self.data_dir
    }

    /// Returns the providers directory path containing external service configurations.
    pub fn providers_dir(&self) -> &str {
        &self.providers_dir
    }

    /// Returns the manifests directory path, falling back to `dataset_defs_dir` if not set.
    pub fn manifests_dir(&self) -> &str {
        &self.manifests_dir
    }
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
    #[error("Invalid object store URL at {0}: {1}")]
    InvalidObjectStoreUrl(PathBuf, common::store::InvalidObjectStoreUrlError),
    #[error("Store error at {0}: {1}")]
    Store(PathBuf, common::store::StoreError),
    #[error("Metadata DB error at {0}: {1}")]
    MetadataDb(PathBuf, metadata_db::Error),
    #[error("Invalid address format for {0}: {1}")]
    InvalidAddress(String, String),
}

impl Config {
    /// Load configuration from file with optional environment variable overrides.
    ///
    /// `env_override` allows env vars prefixed with `AMP_CONFIG_` to override config values.
    /// Nested config values use double underscore separators, e.g. `AMP_CONFIG_METADATA_DB__URL`
    /// overrides `metadata_db.url` in the config file.
    pub async fn load(
        file: impl Into<PathBuf>,
        env_override: bool,
        config_override: Option<Figment>,
        allow_temp_db: bool,
        build_info: impl Into<Option<BuildInfo>>,
    ) -> Result<Self, ConfigError> {
        let input_path = file.into();
        let config_path = fs::canonicalize(&input_path)
            .map_err(|err| ConfigError::Io(input_path.clone(), err))?;
        let contents = fs::read_to_string(&config_path)
            .map_err(|err| ConfigError::Io(config_path.clone(), err))?;

        let config_file: ConfigFile = {
            let mut config_builder = Figment::new().merge(Toml::string(&contents));
            if env_override {
                config_builder = config_builder.merge(Env::prefixed("AMP_CONFIG_").split("__"));
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
        let data_store_url = ObjectStoreUrl::new_with_base(config_file.data_dir(), base)
            .map_err(|err| ConfigError::InvalidObjectStoreUrl(config_path.clone(), err))?;
        let providers_store_url = ObjectStoreUrl::new_with_base(config_file.providers_dir(), base)
            .map_err(|err| ConfigError::InvalidObjectStoreUrl(config_path.clone(), err))?;
        let manifests_store_url = ObjectStoreUrl::new_with_base(config_file.manifests_dir(), base)
            .map_err(|err| ConfigError::InvalidObjectStoreUrl(config_path.clone(), err))?;

        let metadata_db = if config_file.metadata_db.url.is_some() {
            config_file.metadata_db.clone()
        } else if let Some(url) = config_file.metadata_db_url {
            MetadataDbConfig {
                url: Some(url),
                pool_size: config_file.metadata_db.pool_size,
                auto_migrate: config_file.metadata_db.auto_migrate,
            }
        } else if allow_temp_db {
            MetadataDbConfig {
                url: Some(global_metadata_db(*KEEP_TEMP_DIRS).await.url().to_string()),
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
            data_store_url,
            providers_store_url,
            manifests_store_url,
            metadata_db,
            max_mem_mb: config_file.max_mem_mb,
            query_max_mem_mb: config_file.query_max_mem_mb,
            spill_location: config_file.spill_location,
            microbatch_max_interval: config_file.microbatch_max_interval.unwrap_or(100_000),
            server_microbatch_max_interval: config_file
                .server_microbatch_max_interval
                .unwrap_or(1_000),
            parquet: config_file.writer,
            opentelemetry: config_file.opentelemetry,
            addrs,
            config_path,
            poll_interval: config_file.poll_interval_secs.into(),
            build_info: build_info.into().unwrap_or_default(),
            keep_alive_interval: config_file.keep_alive_interval,
        })
    }

    pub fn make_query_env(&self) -> Result<QueryEnv, DataFusionError> {
        common::query_context::create_query_env(
            self.max_mem_mb,
            self.query_max_mem_mb,
            &self.spill_location,
            self.parquet.cache_size_mb,
        )
    }

    pub async fn metadata_db(&self) -> Result<MetadataDb, ConfigError> {
        let url = self.metadata_db.url.as_ref().ok_or_else(|| {
            ConfigError::MissingConfig(self.config_path.clone(), "metadata_db.url")
        })?;
        metadata_db::connect_pool_with_config(
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
            admin_api_addr: ([0, 0, 0, 0], 1610).into(),
        }
    }
}

impl Addrs {
    #[expect(clippy::result_large_err)]
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
            admin_api_addr: parse_addr(
                &config_file.admin_api_addr,
                default_addrs.admin_api_addr,
                "admin_api_addr",
            )?,
        })
    }
}
