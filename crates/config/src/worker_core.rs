//! Worker-core configuration types duplicated for JSON Schema generation.
//!
//! These types mirror `amp_worker_core::config` structs with `schemars` support,
//! so that only `amp-config` carries the `schemars` dependency. Each type has a
//! `From` impl to convert into its `amp_worker_core` counterpart.

use std::time::Duration;

use amp_common::{metadata::Overflow, parquet::basic as parquet_basic};
use serde::Deserialize as _;

// ---------------------------------------------------------------------------
// Compression
// ---------------------------------------------------------------------------

/// Parquet compression algorithm.
///
/// Default: `Zstd(1)`.
#[derive(Debug, Clone)]
pub enum Compression {
    /// Zstandard compression with a configurable level (1–22). Default level: 1.
    Zstd(i32),
    /// LZ4 raw compression.
    Lz4,
    /// Gzip / deflate compression.
    Gzip,
    /// Brotli compression.
    Brotli,
    /// Snappy compression.
    Snappy,
    /// No compression.
    Uncompressed,
}

#[cfg(feature = "schemars")]
impl schemars::JsonSchema for Compression {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "Compression".into()
    }

    fn json_schema(_generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "type": "string",
            "description": "Compression algorithm. Supported: zstd(N), lz4, gzip, brotli, snappy, uncompressed.",
            "default": "zstd(1)",
            "examples": ["zstd(1)", "zstd(3)", "lz4", "gzip", "snappy", "uncompressed"]
        })
    }
}

impl Default for Compression {
    fn default() -> Self {
        Self::Zstd(1)
    }
}

impl<'de> serde::Deserialize<'de> for Compression {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s: String = serde::Deserialize::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

impl std::str::FromStr for Compression {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let lower = s.trim().to_lowercase();
        if lower == "lz4" {
            Ok(Self::Lz4)
        } else if lower == "gzip" {
            Ok(Self::Gzip)
        } else if lower == "brotli" {
            Ok(Self::Brotli)
        } else if lower == "snappy" {
            Ok(Self::Snappy)
        } else if lower == "uncompressed" {
            Ok(Self::Uncompressed)
        } else if lower.starts_with("zstd") {
            // Accept "zstd", "zstd(1)", "zstd(3)", etc.
            if lower == "zstd" {
                return Ok(Self::Zstd(1));
            }
            let inner = lower
                .strip_prefix("zstd(")
                .and_then(|s| s.strip_suffix(')'))
                .ok_or_else(|| format!("invalid compression: {s}"))?;
            let level: i32 = inner
                .parse()
                .map_err(|_| format!("invalid zstd level: {inner}"))?;
            Ok(Self::Zstd(level))
        } else {
            Err(format!(
                "unknown compression algorithm: {s}. \
                 Supported: zstd(N), lz4, gzip, brotli, snappy, uncompressed"
            ))
        }
    }
}

impl std::fmt::Display for Compression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Zstd(level) => write!(f, "zstd({level})"),
            Self::Lz4 => write!(f, "lz4"),
            Self::Gzip => write!(f, "gzip"),
            Self::Brotli => write!(f, "brotli"),
            Self::Snappy => write!(f, "snappy"),
            Self::Uncompressed => write!(f, "uncompressed"),
        }
    }
}

impl From<&Compression> for parquet_basic::Compression {
    fn from(c: &Compression) -> Self {
        match c {
            Compression::Zstd(level) => {
                let level = parquet_basic::ZstdLevel::try_new(*level)
                    .expect("zstd level should be valid");
                parquet_basic::Compression::ZSTD(level)
            }
            Compression::Lz4 => parquet_basic::Compression::LZ4_RAW,
            Compression::Gzip => {
                parquet_basic::Compression::GZIP(parquet_basic::GzipLevel::default())
            }
            Compression::Brotli => {
                parquet_basic::Compression::BROTLI(parquet_basic::BrotliLevel::default())
            }
            Compression::Snappy => parquet_basic::Compression::SNAPPY,
            Compression::Uncompressed => parquet_basic::Compression::UNCOMPRESSED,
        }
    }
}

fn default_compression() -> Compression {
    Compression::default()
}

// ---------------------------------------------------------------------------
// ConfigDuration
// ---------------------------------------------------------------------------

/// Duration in seconds with a compile-time default.
///
/// Deserializes from an optional floating-point number of seconds. When the value
/// is absent or `null`, the compile-time `DEFAULT_SECS` is used.
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

#[cfg(feature = "schemars")]
impl<const DEFAULT_SECS: u64> schemars::JsonSchema for ConfigDuration<DEFAULT_SECS> {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "ConfigDuration".into()
    }

    fn json_schema(_generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "type": "number",
            "description": "Duration in seconds (floating-point)"
        })
    }
}

impl<const N: u64> From<&ConfigDuration<N>> for amp_worker_core::ConfigDuration<N> {
    fn from(d: &ConfigDuration<N>) -> Self {
        d.0.into()
    }
}

// ---------------------------------------------------------------------------
// SizeLimitConfig
// ---------------------------------------------------------------------------

/// Size-based limits for Parquet file partitioning and compaction.
///
/// Controls when files are considered "full" and when compaction should trigger.
#[derive(Debug, Clone, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct SizeLimitConfig {
    /// Number of files to target per partition.
    pub file_count: u32,
    /// Compaction generation tracking.
    pub generation: u64,
    /// Overflow multiplier: 1x target size (default: `"1"`), can use `"1.5"` for 1.5x, etc.
    ///
    /// Accepts an integer, float, or fraction string (e.g. `1`, `1.5`, `"3/2"`).
    #[cfg_attr(feature = "schemars", schemars(with = "OverflowSchema"))]
    pub overflow: Overflow,
    /// Internal tracking only — not exposed in config files.
    #[serde(skip)]
    pub blocks: u64,
    /// Target bytes per file (default: 2147483648 = 2 GB for target_size, 0 for eager limits).
    pub bytes: u64,
    /// Target rows per file, 0 means no limit (default: 0).
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

/// Schemars helper: JSON schema for the `Overflow` type.
///
/// Defined locally because the `common` crate no longer carries a schemars dependency.
#[cfg(feature = "schemars")]
struct OverflowSchema;

#[cfg(feature = "schemars")]
impl schemars::JsonSchema for OverflowSchema {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "Overflow".into()
    }

    fn json_schema(_generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "oneOf": [
                { "type": "integer", "minimum": 1 },
                { "type": "number", "exclusiveMinimum": 0 },
                { "type": "string" }
            ],
            "description": "Overflow multiplier as an integer, float, or fraction (e.g. 1, 1.5, \"3/2\")"
        })
    }
}

impl From<&SizeLimitConfig> for amp_worker_core::SizeLimitConfig {
    fn from(config: &SizeLimitConfig) -> Self {
        Self {
            file_count: config.file_count,
            generation: config.generation,
            overflow: config.overflow,
            blocks: config.blocks,
            bytes: config.bytes,
            rows: config.rows,
        }
    }
}

// ---------------------------------------------------------------------------
// CollectorConfig
// ---------------------------------------------------------------------------

/// Garbage collection configuration for expired Parquet segment files.
#[derive(Debug, Default, Clone, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(default)]
pub struct CollectorConfig {
    /// Enable or disable the garbage collector (default: false).
    pub active: bool,
    /// Interval in seconds between garbage collection runs (default: 30).
    pub min_interval: ConfigDuration<30>,
    /// Duration in seconds to hold a deletion lock on compacted files (default: 1800 = 30 min).
    pub deletion_lock_duration: ConfigDuration<1800>,
}

impl From<&CollectorConfig> for amp_worker_core::CollectorConfig {
    fn from(config: &CollectorConfig) -> Self {
        Self {
            active: config.active,
            min_interval: (&config.min_interval).into(),
            deletion_lock_duration: (&config.deletion_lock_duration).into(),
        }
    }
}

// ---------------------------------------------------------------------------
// CompactionAlgorithmConfig
// ---------------------------------------------------------------------------

/// Compaction algorithm tuning parameters.
///
/// Controls cooldown between compaction runs and eager compaction size limits.
#[derive(Debug, Clone, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(default)]
pub struct CompactionAlgorithmConfig {
    /// Base cooldown duration in seconds between compaction runs (default: 1024).
    pub cooldown_duration: ConfigDuration<1024>,
    /// Eager compaction limits (flattened fields: `overflow`, `bytes`, `rows`).
    #[serde(
        flatten,
        default = "SizeLimitConfig::default_eager_limit",
        deserialize_with = "SizeLimitConfig::deserialize_eager_limit"
    )]
    #[cfg_attr(feature = "schemars", schemars(with = "SizeLimitConfig"))]
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

impl From<&CompactionAlgorithmConfig> for amp_worker_core::CompactionAlgorithmConfig {
    fn from(config: &CompactionAlgorithmConfig) -> Self {
        Self {
            cooldown_duration: (&config.cooldown_duration).into(),
            eager_compaction_limit: (&config.eager_compaction_limit).into(),
        }
    }
}

// ---------------------------------------------------------------------------
// CompactorConfig
// ---------------------------------------------------------------------------

/// File compaction configuration.
///
/// Controls concurrency, intervals, and algorithm parameters for merging
/// small Parquet segments into larger files.
#[derive(Debug, Clone, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(default)]
pub struct CompactorConfig {
    /// Enable or disable the compactor (default: false).
    pub active: bool,
    /// Maximum concurrent metadata operations (default: 2).
    pub metadata_concurrency: usize,
    /// Maximum concurrent compaction write operations (default: 2).
    pub write_concurrency: usize,
    /// Interval in seconds between compactor runs (default: 1).
    pub min_interval: ConfigDuration<1>,
    /// Compaction algorithm configuration (flattened fields: `cooldown_duration`,
    /// `overflow`, `bytes`, `rows`).
    #[serde(flatten)]
    #[cfg_attr(feature = "schemars", schemars(with = "CompactionAlgorithmConfig"))]
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

impl From<&CompactorConfig> for amp_worker_core::CompactorConfig {
    fn from(config: &CompactorConfig) -> Self {
        Self {
            active: config.active,
            metadata_concurrency: config.metadata_concurrency,
            write_concurrency: config.write_concurrency,
            min_interval: (&config.min_interval).into(),
            algorithm: (&config.algorithm).into(),
        }
    }
}

// ---------------------------------------------------------------------------
// ParquetConfig
// ---------------------------------------------------------------------------

/// Parquet writer and file configuration.
///
/// Controls compression, caching, segment sizing, compaction, and garbage collection
/// for Parquet files produced by the worker.
#[derive(Debug, Clone, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct ParquetConfig {
    /// Compression algorithm (default: `Zstd(1)`).
    #[serde(default = "default_compression")]
    pub compression: Compression,
    /// Enable Parquet bloom filters (default: false).
    #[serde(default)]
    pub bloom_filters: bool,
    /// Parquet metadata cache size in MB (default: 1024).
    #[serde(default = "default_cache_size_mb")]
    pub cache_size_mb: u64,
    /// Maximum row group size in MB (default: 512).
    #[serde(default = "default_max_row_group_mb")]
    pub max_row_group_mb: u64,
    /// Target partition size configuration (flattened fields: `overflow`, `bytes`, `rows`).
    #[serde(
        alias = "file_size",
        flatten,
        default = "SizeLimitConfig::default_upper_limit",
        deserialize_with = "SizeLimitConfig::deserialize_upper_limit"
    )]
    #[cfg_attr(feature = "schemars", schemars(with = "SizeLimitConfig"))]
    pub target_size: SizeLimitConfig,
    /// Compaction settings.
    #[serde(default)]
    pub compactor: CompactorConfig,
    /// Garbage collection settings.
    #[serde(alias = "garbage_collector", default)]
    pub collector: CollectorConfig,
    /// Maximum wall-clock time before closing a segment, in seconds (default: 600 = 10 min).
    #[serde(default)]
    pub segment_flush_interval_secs: ConfigDuration<600>,
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
            segment_flush_interval_secs: ConfigDuration::default(),
        }
    }
}

impl From<&ParquetConfig> for amp_worker_core::ParquetConfig {
    fn from(config: &ParquetConfig) -> Self {
        Self {
            compression: (&config.compression).into(),
            bloom_filters: config.bloom_filters,
            cache_size_mb: config.cache_size_mb,
            max_row_group_mb: config.max_row_group_mb,
            target_size: (&config.target_size).into(),
            compactor: (&config.compactor).into(),
            collector: (&config.collector).into(),
            segment_flush_interval_secs: (&config.segment_flush_interval_secs).into(),
        }
    }
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

fn default_cache_size_mb() -> u64 {
    1024 // 1 GB default cache size
}

fn default_max_row_group_mb() -> u64 {
    512 // 512 MB default row group size
}

fn deserialize_duration<'de, D>(deserializer: D) -> Result<Option<Duration>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    <Option<f64>>::deserialize(deserializer).map(|option| option.map(Duration::from_secs_f64))
}
