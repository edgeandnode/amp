use std::{net::SocketAddr, num::NonZeroUsize, path::PathBuf, sync::Arc, time::Duration};

use datafusion::{
    error::DataFusionError,
    execution::{
        disk_manager::{DiskManagerBuilder, DiskManagerMode},
        memory_pool::{FairSpillPool, GreedyMemoryPool, MemoryPool, TrackConsumersPool},
        runtime_env::RuntimeEnvBuilder,
    },
    parquet::{
        basic::{Compression, ZstdLevel},
        file::metadata::ParquetMetaData,
    },
};
use figment::{
    Figment,
    providers::{Env, Format as _, Toml},
};
use foyer::CacheBuilder;
use fs_err as fs;
use js_runtime::isolate_pool::IsolatePool;
use metadata_db::{DEFAULT_POOL_SIZE, KEEP_TEMP_DIRS, MetadataDb, temp_metadata_db};
use serde::Deserialize;
use thiserror::Error;

use crate::{Store, metadata::Overflow, query_context::QueryEnv, store::ObjectStoreUrl};

#[derive(Debug, Clone)]
pub struct Config {
    pub data_store: Arc<Store>,
    pub providers_store: Arc<Store>,
    pub manifests_store: Arc<Store>,
    pub metadata_db: MetadataDbConfig,
    pub max_mem_mb: usize,
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
pub struct ParquetConfig {
    /// Compression algorithm: zstd, lz4, gzip, brotli, snappy, uncompressed (default: zstd(1))
    #[serde(
        default = "default_compression",
        deserialize_with = "deserialize_compression"
    )]
    pub compression: Compression,
    /// Enable bloom filters (default: false)
    #[serde(default)]
    pub bloom_filters: bool,
    /// Parquet metadata cache size in MB (default: 1024)
    #[serde(default = "default_cache_size_mb")]
    pub cache_size_mb: u64,
    /// Max row group size in MB (default: 512)
    #[serde(default = "default_max_row_group_mb")]
    pub max_row_group_mb: u64,
    /// Target partition size configuration (flattened fields: overflow, bytes, rows)
    #[serde(
        alias = "file_size",
        flatten,
        default = "SizeLimitConfig::default_upper_limit",
        deserialize_with = "SizeLimitConfig::deserialize_upper_limit"
    )]
    pub target_size: SizeLimitConfig,
    #[serde(default)]
    pub compactor: CompactorConfig,
    #[serde(alias = "garbage_collector", default)]
    pub collector: CollectorConfig,
}

impl Default for ParquetConfig {
    fn default() -> Self {
        Self {
            compression: default_compression(),
            bloom_filters: false,
            cache_size_mb: default_cache_size_mb(),
            max_row_group_mb: default_max_row_group_mb(),
            target_size: SizeLimitConfig::default_upper_limit(),
            compactor: CompactorConfig::default(),
            collector: CollectorConfig::default(),
        }
    }
}

#[derive(Debug, Default, Clone, Deserialize)]
#[serde(default)]
pub struct CollectorConfig {
    /// Enable or disable the collector (default: false)
    pub active: bool,
    /// Interval in seconds to run the garbage collector (default: 30.0)
    pub min_interval: ConfigDuration<30>,
    /// Duration in seconds to hold deletion lock on compacted files (default: 1800.0 = 30 minutes)
    pub deletion_lock_duration: ConfigDuration<1800>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct CompactorConfig {
    /// Enable or disable the compactor (default: false)
    pub active: bool,
    /// Max concurrent metadata operations (default: 2)
    pub metadata_concurrency: usize,
    /// Max concurrent compaction write operations (default: 2)
    pub write_concurrency: usize,
    /// Interval in seconds to run the compactor (default: 1.0)
    pub min_interval: ConfigDuration<1>,
    /// Compaction algorithm configuration (flattened fields: cooldown_duration, overflow, bytes, rows)
    #[serde(flatten)]
    pub algorithm: CompactionAlgorithmConfig,
}

impl Default for CompactorConfig {
    fn default() -> Self {
        Self {
            active: false,
            metadata_concurrency: 2,
            write_concurrency: 2,
            min_interval: ConfigDuration::default(),
            algorithm: CompactionAlgorithmConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct CompactionAlgorithmConfig {
    /// Base cooldown duration in seconds (default: 1024.0)
    pub cooldown_duration: ConfigDuration<1024>,
    /// Eager compaction limits (flattened fields: overflow, bytes, rows)
    #[serde(
        flatten,
        default = "SizeLimitConfig::default_eager_limit",
        deserialize_with = "SizeLimitConfig::deserialize_eager_limit"
    )]
    pub eager_compaction_limit: SizeLimitConfig,
}

impl Default for CompactionAlgorithmConfig {
    fn default() -> Self {
        Self {
            cooldown_duration: ConfigDuration::default(),
            eager_compaction_limit: SizeLimitConfig::default_eager_limit(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct SizeLimitConfig {
    pub file_count: u32,
    pub generation: u64,
    /// Overflow multiplier: 1x target size (default: "1"), can use "1.5" for 1.5x, etc.
    pub overflow: Overflow,
    #[serde(skip)]
    pub blocks: u64,
    /// Target bytes per file (default: 2147483648 = 2GB for target_size, 0 for eager limits)
    pub bytes: u64,
    /// Target rows per file, 0 means no limit (default: 0)
    pub rows: u64,
}

impl Default for SizeLimitConfig {
    fn default() -> Self {
        Self {
            file_count: 0,
            generation: 0,
            overflow: Overflow::default(),
            blocks: 0,
            bytes: 2 * 1024 * 1024 * 1024, // 2GB
            rows: 0,
        }
    }
}

#[derive(Deserialize)]
struct SizeLimitHelper {
    pub overflow: Option<Overflow>,
    pub bytes: Option<u64>,
    pub rows: Option<u64>,
}

impl SizeLimitConfig {
    fn default_eager_limit() -> Self {
        Self {
            bytes: 0,
            blocks: 0,
            ..Default::default()
        }
    }

    fn deserialize_eager_limit<'de, D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let helper = SizeLimitHelper::deserialize(deserializer)?;

        let mut this = Self::default_eager_limit();

        helper
            .overflow
            .inspect(|overflow| this.overflow = *overflow);
        helper.bytes.inspect(|bytes| this.bytes = *bytes);
        helper.rows.inspect(|rows| this.rows = *rows);

        Ok(this)
    }

    fn default_upper_limit() -> Self {
        Self {
            ..Default::default()
        }
    }

    fn deserialize_upper_limit<'de, D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let helper = SizeLimitHelper::deserialize(deserializer)?;

        let mut this = Self::default_upper_limit();

        helper
            .overflow
            .inspect(|overflow| this.overflow = *overflow);
        helper.bytes.inspect(|bytes| this.bytes = *bytes);
        helper.rows.inspect(|rows| this.rows = *rows);

        Ok(this)
    }
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

fn default_compression() -> Compression {
    Compression::ZSTD(ZstdLevel::try_new(1).unwrap())
}

fn default_cache_size_mb() -> u64 {
    1024 // 1GB default cache size
}

fn default_max_row_group_mb() -> u64 {
    512 // 512MB default row group size
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
pub struct OpenTelemetryConfig {
    /// Remote OpenTelemetry metrics collector endpoint. Metrics are sent over binary HTTP.
    pub metrics_url: Option<String>,
    /// The interval (in seconds) at which to export metrics to the OpenTelemetry collector.
    #[serde(
        default,
        rename = "metrics_export_interval_secs",
        deserialize_with = "deserialize_duration"
    )]
    pub metrics_export_interval: Option<Duration>,
    /// Remote OpenTelemetry traces collector endpoint. Traces are sent over HTTP.
    pub trace_url: Option<String>,
    /// The ratio of traces to sample (f64). Samples all traces by default (equivalent to 1.0).
    pub trace_ratio: Option<f64>,
}

fn deserialize_duration<'de, D>(deserializer: D) -> Result<Option<Duration>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    <Option<f64>>::deserialize(deserializer).map(|option| option.map(Duration::from_secs_f64))
}

#[derive(Debug, Clone)]
pub struct ConfigDuration<const DEFAULT_SECS: u64>(Duration);

impl<const DEFAULT_SECS: u64> Default for ConfigDuration<DEFAULT_SECS> {
    fn default() -> Self {
        Self(Duration::from_secs(DEFAULT_SECS))
    }
}

impl<const DEFAULT_SECS: u64> From<ConfigDuration<DEFAULT_SECS>> for Duration {
    fn from(val: ConfigDuration<DEFAULT_SECS>) -> Self {
        val.0
    }
}

impl<'de, const DEFAULT_SECS: u64> serde::Deserialize<'de> for ConfigDuration<DEFAULT_SECS> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserialize_duration(deserializer).map(|opt| opt.map_or_else(Self::default, Self))
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
    InvalidObjectStoreUrl(PathBuf, crate::store::InvalidObjectStoreUrlError),
    #[error("Store error at {0}: {1}")]
    Store(PathBuf, crate::store::StoreError),
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
        let data_store = Store::new(
            ObjectStoreUrl::new_with_base(config_file.data_dir(), base)
                .map_err(|err| ConfigError::InvalidObjectStoreUrl(config_path.clone(), err))?,
        )
        .map_err(|err| ConfigError::Store(config_path.clone(), err))?;
        let providers_store = Store::new(
            ObjectStoreUrl::new_with_base(config_file.providers_dir(), base)
                .map_err(|err| ConfigError::InvalidObjectStoreUrl(config_path.clone(), err))?,
        )
        .map_err(|err| ConfigError::Store(config_path.clone(), err))?;
        let manifests_store = Store::new(
            ObjectStoreUrl::new_with_base(config_file.manifests_dir(), base)
                .map_err(|err| ConfigError::InvalidObjectStoreUrl(config_path.clone(), err))?,
        )
        .map_err(|err| ConfigError::Store(config_path.clone(), err))?;

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
            manifests_store: Arc::new(manifests_store),
            metadata_db,
            max_mem_mb: config_file.max_mem_mb,
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
        let spill_allowed = !self.spill_location.is_empty();
        let disk_manager_mode = if spill_allowed {
            DiskManagerMode::Disabled
        } else {
            DiskManagerMode::Directories(self.spill_location.clone())
        };

        let disk_manager_builder = DiskManagerBuilder::default().with_mode(disk_manager_mode);

        let memory_pool: Option<Arc<dyn MemoryPool>> = if self.max_mem_mb > 0 {
            let max_mem_bytes = self.max_mem_mb * 1024 * 1024;
            make_memory_pool(spill_allowed, max_mem_bytes)
        } else {
            None
        };

        let mut runtime_config =
            RuntimeEnvBuilder::new().with_disk_manager_builder(disk_manager_builder);

        if let Some(memory_pool) = memory_pool {
            runtime_config = runtime_config.with_memory_pool(memory_pool);
        }

        let runtime_env = runtime_config.build()?;
        let isolate_pool = IsolatePool::new();

        // Create ParquetMetaData footer cache with the configured memory limit
        let cache_size_bytes = self.parquet.cache_size_mb * 1024 * 1024; // Convert MB to bytes
        let parquet_footer_cache = CacheBuilder::new(cache_size_bytes as usize)
            .with_weighter(|_k, v: &Arc<ParquetMetaData>| v.memory_size())
            .build();

        Ok(QueryEnv {
            df_env: Arc::new(runtime_env),
            isolate_pool,
            parquet_footer_cache,
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

fn make_memory_pool(
    spill_allowed: bool,
    max_mem_bytes: usize,
) -> Option<Arc<dyn MemoryPool + 'static>> {
    let report_top_k_consumers = NonZeroUsize::new(5).unwrap();
    if spill_allowed {
        Some(Arc::new(TrackConsumersPool::new(
            FairSpillPool::new(max_mem_bytes),
            report_top_k_consumers,
        )))
    } else {
        Some(Arc::new(TrackConsumersPool::new(
            GreedyMemoryPool::new(max_mem_bytes),
            report_top_k_consumers,
        )))
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
