use std::{path::PathBuf, time::Duration};

use common::{metadata::Overflow, query_context::QueryEnv};
use datafusion::{
    common::DataFusionError,
    parquet::basic::{Compression, ZstdLevel},
};
use serde::Deserialize as _;

/// Configuration specific to dump operations
///
/// This configuration contains only the fields needed by the dump crate for executing
/// dataset dump operations. It is created from a larger configuration by the worker service.
#[derive(Debug, Clone)]
pub struct Config {
    /// Poll interval for raw datasets
    pub poll_interval: Duration,

    /// Keep-alive interval for streaming queries
    pub keep_alive_interval: Option<u64>,

    /// Maximum memory usage for DataFusion query environment (in MB)
    pub max_mem_mb: usize,

    /// Maximum memory per query for DataFusion (in MB)
    pub query_max_mem_mb: usize,

    /// Directory paths for DataFusion query spilling
    pub spill_location: Vec<PathBuf>,

    /// Parquet file configuration
    pub parquet: ParquetConfig,
}

impl Config {
    /// Create a DataFusion query environment from this configuration
    pub fn make_query_env(&self) -> Result<QueryEnv, DataFusionError> {
        common::query_context::create_query_env(
            self.max_mem_mb,
            self.query_max_mem_mb,
            &self.spill_location,
            self.parquet.cache_size_mb,
        )
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
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

#[derive(Debug, Default, Clone, serde::Deserialize)]
#[serde(default)]
pub struct CollectorConfig {
    /// Enable or disable the collector (default: false)
    pub active: bool,
    /// Interval in seconds to run the garbage collector (default: 30.0)
    pub min_interval: ConfigDuration<30>,
    /// Duration in seconds to hold deletion lock on compacted files (default: 1800.0 = 30 minutes)
    pub deletion_lock_duration: ConfigDuration<1800>,
}

#[derive(Debug, Clone, serde::Deserialize)]
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

#[derive(Debug, Clone, serde::Deserialize)]
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

#[derive(Debug, Clone, serde::Deserialize)]
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

#[derive(Debug, serde::Deserialize)]
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
    let s: String = serde::Deserialize::deserialize(deserializer)?;
    s.parse().map_err(serde::de::Error::custom)
}

fn deserialize_duration<'de, D>(deserializer: D) -> Result<Option<Duration>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    <Option<f64>>::deserialize(deserializer).map(|option| option.map(Duration::from_secs_f64))
}
