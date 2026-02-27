use std::{path::PathBuf, time::Duration};

use common::metadata::Overflow;
use datafusion::parquet::basic::{Compression, ZstdLevel};

/// Configuration specific to dump operations
///
/// This configuration contains only the fields needed by the dump crate for executing
/// dataset dump operations. It is created from a larger configuration by the worker service.
#[derive(Debug, Clone)]
pub struct Config {
    /// Poll interval for raw datasets
    pub poll_interval: Duration,

    /// Keep-alive interval for streaming queries
    pub keep_alive_interval: u64,

    /// Maximum memory usage for DataFusion query environment (in MB)
    pub max_mem_mb: usize,

    /// Maximum memory per query for DataFusion (in MB)
    pub query_max_mem_mb: usize,

    /// Directory paths for DataFusion query spilling
    pub spill_location: Vec<PathBuf>,

    /// Parquet file configuration
    pub parquet: ParquetConfig,

    /// Progress event emission interval.
    /// Progress events are emitted at most once per this interval when there is new progress.
    /// Default: 10 seconds.
    pub progress_interval: Duration,
}

#[derive(Debug, Clone)]
pub struct ParquetConfig {
    /// Compression algorithm: zstd, lz4, gzip, brotli, snappy, uncompressed (default: zstd(1))
    pub compression: Compression,
    /// Enable bloom filters (default: false)
    pub bloom_filters: bool,
    /// Parquet metadata cache size in MB (default: 1024)
    pub cache_size_mb: u64,
    /// Max row group size in MB (default: 512)
    pub max_row_group_mb: u64,
    /// Target partition size configuration
    pub target_size: SizeLimitConfig,
    pub compactor: CompactorConfig,
    pub collector: CollectorConfig,
    /// Max wall-clock time before closing a segment, in seconds (default: 600 = 10 min)
    pub segment_flush_interval_secs: ConfigDuration<600>,
}

impl Default for ParquetConfig {
    fn default() -> Self {
        Self {
            compression: Compression::ZSTD(ZstdLevel::default()),
            bloom_filters: false,
            cache_size_mb: 1024,   // 1 GB
            max_row_group_mb: 512, // 512 MB
            target_size: SizeLimitConfig::default(),
            compactor: CompactorConfig::default(),
            collector: CollectorConfig::default(),
            segment_flush_interval_secs: ConfigDuration::default(),
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct CollectorConfig {
    /// Enable or disable the collector (default: false)
    pub active: bool,
    /// Interval in seconds to run the garbage collector (default: 30.0)
    pub min_interval: ConfigDuration<30>,
    /// Duration in seconds to hold deletion lock on compacted files (default: 1800.0 = 30 minutes)
    pub deletion_lock_duration: ConfigDuration<1800>,
}

#[derive(Debug, Clone)]
pub struct CompactorConfig {
    /// Enable or disable the compactor (default: false)
    pub active: bool,
    /// Max concurrent metadata operations (default: 2)
    pub metadata_concurrency: usize,
    /// Max concurrent compaction write operations (default: 2)
    pub write_concurrency: usize,
    /// Interval in seconds to run the compactor (default: 1.0)
    pub min_interval: ConfigDuration<1>,
    /// Compaction algorithm configuration
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

#[derive(Debug, Clone)]
pub struct CompactionAlgorithmConfig {
    /// Base cooldown duration in seconds (default: 1024.0)
    pub cooldown_duration: ConfigDuration<1024>,
    /// Eager compaction limits
    pub eager_compaction_limit: SizeLimitConfig,
}

impl Default for CompactionAlgorithmConfig {
    fn default() -> Self {
        Self {
            cooldown_duration: ConfigDuration::default(),
            eager_compaction_limit: SizeLimitConfig {
                bytes: 0,
                ..Default::default()
            },
        }
    }
}

#[derive(Debug, Clone)]
pub struct SizeLimitConfig {
    pub file_count: u32,
    pub generation: u64,
    /// Overflow multiplier: 1x target size (default: "1"), can use "1.5" for 1.5x, etc.
    pub overflow: Overflow,
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

impl<const DEFAULT_SECS: u64> From<Duration> for ConfigDuration<DEFAULT_SECS> {
    fn from(d: Duration) -> Self {
        Self(d)
    }
}
