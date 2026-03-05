//! Worker-core configuration types for deserialization and JSON Schema generation.
//!
//! These are the authoritative configuration types that own serde deserialization,
//! validation, defaults, and JSON Schema generation (`schemars`). Worker-core's
//! config types are plain domain structs constructed from these via `From` impls.

use std::{num::NonZeroU64, time::Duration};

/// Parquet writer and file configuration.
///
/// Controls compression, caching, segment sizing, compaction, and garbage collection
/// for Parquet files produced by the worker.
#[derive(Debug, Clone, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct ParquetConfig {
    /// Compression algorithm (default: `zstd(1)`).
    #[serde(default)]
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
        default,
        deserialize_with = "SizeLimitConfig::deserialize_upper_limit"
    )]
    pub target_size: SizeLimitConfig,
    /// Compaction settings.
    #[serde(default)]
    pub compactor: CompactorConfig,
    /// Garbage collection settings.
    #[serde(default, alias = "garbage_collector")]
    pub collector: CollectorConfig,
    /// Maximum wall-clock time before closing a segment, in seconds (default: 600 = 10 min).
    #[serde(default)]
    pub segment_flush_interval_secs: ConfigDuration<600>,
}

impl Default for ParquetConfig {
    fn default() -> Self {
        Self {
            compression: Compression::default(),
            bloom_filters: false,
            cache_size_mb: default_cache_size_mb(),
            max_row_group_mb: default_max_row_group_mb(),
            target_size: SizeLimitConfig::default(),
            compactor: CompactorConfig::default(),
            collector: CollectorConfig::default(),
            segment_flush_interval_secs: ConfigDuration::default(),
        }
    }
}

fn default_cache_size_mb() -> u64 {
    1024 // 1 GB default cache size
}

fn default_max_row_group_mb() -> u64 {
    512 // 512 MB default row group size
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

/// Parquet compression algorithm.
///
/// Parsed from a case-insensitive string with the following accepted values:
///
/// | Input              | Result                                          |
/// |--------------------|-------------------------------------------------|
/// | `zstd`             | Zstandard at the default level (1)              |
/// | `zstd(N)`          | Zstandard at level N (1–22)                     |
/// | `lz4`              | LZ4 raw                                         |
/// | `gzip`             | Gzip at the parquet-default level               |
/// | `brotli`           | Brotli at the parquet-default level             |
/// | `snappy`           | Snappy                                          |
/// | `uncompressed`     | No compression                                  |
///
/// Only `zstd` supports an explicit level. `gzip` and `brotli` always use
/// parquet's default compression level; per-level configuration for those
/// algorithms is not exposed.
///
/// Default: `zstd(1)`.
#[derive(Debug, Clone)]
pub enum Compression {
    /// Zstandard compression with a configurable level (1–22). Default level: 1.
    Zstd(ZstdLevel),
    /// LZ4 raw compression.
    Lz4,
    /// Gzip compression at parquet's default level (not configurable).
    Gzip,
    /// Brotli compression at parquet's default level (not configurable).
    Brotli,
    /// Snappy compression.
    Snappy,
    /// No compression.
    Uncompressed,
}

/// Zstandard compression level.
///
/// Wraps parquet's [`ZstdLevel`](amp_common::parquet::basic::ZstdLevel) so that
/// validation happens once at parse time against parquet's own constraints. The
/// inner parquet type is an implementation detail — callers interact with this
/// newtype only.
#[derive(Debug, Default, Clone, Copy)]
pub struct ZstdLevel(amp_common::parquet::basic::ZstdLevel);

impl ZstdLevel {
    /// Create a new `ZstdLevel`, validated against parquet's accepted range.
    pub fn try_new(level: i32) -> Result<Self, String> {
        amp_common::parquet::basic::ZstdLevel::try_new(level)
            .map(Self)
            .map_err(|err| format!("invalid zstd level: {err}"))
    }

    /// Return the raw compression level.
    pub fn compression_level(self) -> i32 {
        self.0.compression_level()
    }

    /// Return the inner parquet `ZstdLevel`.
    fn parquet_level(self) -> amp_common::parquet::basic::ZstdLevel {
        self.0
    }
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
        Self::Zstd(ZstdLevel::default())
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
    // String is used because this error is only consumed by serde via
    // `serde::de::Error::custom`, which accepts any Display type.
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let lower = s.trim().to_lowercase();
        match lower.as_str() {
            "lz4" => Ok(Self::Lz4),
            "gzip" => Ok(Self::Gzip),
            "brotli" => Ok(Self::Brotli),
            "snappy" => Ok(Self::Snappy),
            "uncompressed" => Ok(Self::Uncompressed),
            // Accept "zstd", "zstd(1)", "zstd(3)", etc.
            "zstd" => Ok(Self::Zstd(ZstdLevel::default())),
            s if s.starts_with("zstd(") => {
                let inner = s
                    .strip_prefix("zstd(")
                    .and_then(|s| s.strip_suffix(')'))
                    .ok_or_else(|| format!("invalid compression: {s}"))?;
                let raw: i32 = inner
                    .parse()
                    .map_err(|_| format!("invalid zstd level: {inner}"))?;
                let level = ZstdLevel::try_new(raw)?;
                Ok(Self::Zstd(level))
            }
            _ => Err(format!(
                "unknown compression algorithm: {s}. \
                 Supported: zstd(N), lz4, gzip, brotli, snappy, uncompressed"
            )),
        }
    }
}

impl std::fmt::Display for Compression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Zstd(level) => write!(f, "zstd({})", level.compression_level()),
            Self::Lz4 => write!(f, "lz4"),
            Self::Gzip => write!(f, "gzip"),
            Self::Brotli => write!(f, "brotli"),
            Self::Snappy => write!(f, "snappy"),
            Self::Uncompressed => write!(f, "uncompressed"),
        }
    }
}

impl From<&Compression> for amp_common::parquet::basic::Compression {
    fn from(c: &Compression) -> Self {
        use amp_common::parquet::basic as pq;

        match c {
            Compression::Zstd(level) => pq::Compression::ZSTD(level.parquet_level()),
            Compression::Lz4 => pq::Compression::LZ4_RAW,
            Compression::Gzip => pq::Compression::GZIP(pq::GzipLevel::default()),
            Compression::Brotli => pq::Compression::BROTLI(pq::BrotliLevel::default()),
            Compression::Snappy => pq::Compression::SNAPPY,
            Compression::Uncompressed => pq::Compression::UNCOMPRESSED,
        }
    }
}

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

/// Size-based limits for Parquet file partitioning and compaction.
///
/// Contains only user-configurable fields. Runtime-internal fields (`file_count`,
/// `generation`, `blocks`) are set to defaults during conversion to the worker-core type.
#[derive(Debug, Clone, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct SizeLimitConfig {
    /// Overflow multiplier: 1x target size (default: `"1"`), can use `"1.5"` for 1.5x, etc.
    ///
    /// Accepts an integer, float, or fraction string (e.g. `1`, `1.5`, `"3/2"`).
    pub overflow: Overflow,
    /// Target bytes per file (default: 2147483648 = 2 GB for target_size, 0 for eager limits).
    pub bytes: u64,
    /// Target rows per file, 0 means no limit (default: 0).
    pub rows: u64,
}

impl Default for SizeLimitConfig {
    fn default() -> Self {
        Self {
            overflow: Overflow::default(),
            bytes: 2 * 1024 * 1024 * 1024, // 2GB
            rows: 0,
        }
    }
}

impl From<&SizeLimitConfig> for amp_worker_core::SizeLimitConfig {
    fn from(config: &SizeLimitConfig) -> Self {
        Self {
            file_count: 0,
            generation: 0,
            overflow: config.overflow.into(),
            blocks: 0,
            bytes: config.bytes,
            rows: config.rows,
        }
    }
}

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
        <Option<f64>>::deserialize(deserializer)?
            .map(|secs| Duration::try_from_secs_f64(secs).map_err(serde::de::Error::custom))
            .transpose()
            .map(|opt| opt.map_or_else(Self::default, Self))
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

/// Deserialization helper for partial `SizeLimitConfig` overrides.
#[derive(Debug, serde::Deserialize)]
struct SizeLimitHelper {
    overflow: Option<Overflow>,
    bytes: Option<u64>,
    rows: Option<u64>,
}

impl SizeLimitHelper {
    /// Apply optional overrides onto a base `SizeLimitConfig`.
    fn apply_to(self, mut base: SizeLimitConfig) -> SizeLimitConfig {
        self.overflow.inspect(|overflow| base.overflow = *overflow);
        self.bytes.inspect(|bytes| base.bytes = *bytes);
        self.rows.inspect(|rows| base.rows = *rows);
        base
    }
}

impl SizeLimitConfig {
    fn default_eager_limit() -> Self {
        Self {
            bytes: 0,
            ..Default::default()
        }
    }

    fn deserialize_with_base<'de, D>(deserializer: D, base: Self) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        serde::Deserialize::deserialize(deserializer)
            .map(|helper: SizeLimitHelper| helper.apply_to(base))
    }

    fn deserialize_eager_limit<'de, D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Self::deserialize_with_base(deserializer, Self::default_eager_limit())
    }

    fn deserialize_upper_limit<'de, D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Self::deserialize_with_base(deserializer, Self::default())
    }
}

/// Config-local overflow multiplier for deserialization.
///
/// Accepts integers, floats, and fraction strings (e.g. `1`, `1.5`, `"3/2"`).
#[derive(Debug, Clone, Copy)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(
    feature = "schemars",
    schemars(
        description = r#"Overflow multiplier as an integer, float, or fraction (e.g. 1, 1.5, "3/2")"#,
        schema_with = "overflow_schema"
    )
)]
pub struct Overflow(pub NonZeroU64, pub NonZeroU64);

impl Default for Overflow {
    fn default() -> Self {
        Self(NonZeroU64::new(1).unwrap(), NonZeroU64::new(1).unwrap())
    }
}

impl From<Overflow> for amp_worker_core::compaction::Overflow {
    fn from(value: Overflow) -> Self {
        Self::new(value.0, value.1)
    }
}

impl std::str::FromStr for Overflow {
    type Err = OverflowParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();
        if s.is_empty() {
            return Err(OverflowParseError::Empty);
        }

        if let Some((n, d)) = s.split_once('/') {
            let n: u64 = n
                .trim()
                .parse()
                .map_err(OverflowParseError::InvalidNumerator)?;
            let d: u64 = d
                .trim()
                .parse()
                .map_err(OverflowParseError::InvalidDenominator)?;
            let nz_n = NonZeroU64::new(n).ok_or(OverflowParseError::ZeroNumerator)?;
            let nz_d = NonZeroU64::new(d).ok_or(OverflowParseError::ZeroDenominator)?;
            Ok(Self(nz_n, nz_d))
        } else if let Ok(value) = s.parse::<u64>() {
            let nz_n = NonZeroU64::new(value).ok_or(OverflowParseError::ZeroNumerator)?;
            Ok(Self(nz_n, NonZeroU64::new(1).unwrap()))
        } else {
            let value: f64 = s.parse().map_err(OverflowParseError::InvalidFloat)?;
            if value <= 0.0 || value.is_nan() || value.is_infinite() {
                return Err(OverflowParseError::NotPositive);
            }
            Ok(amp_worker_core::compaction::Overflow::from(value).into())
        }
    }
}

impl From<amp_worker_core::compaction::Overflow> for Overflow {
    fn from(value: amp_worker_core::compaction::Overflow) -> Self {
        Self(value.0, value.1)
    }
}

impl<'de> serde::Deserialize<'de> for Overflow {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct OverflowVisitor;

        impl serde::de::Visitor<'_> for OverflowVisitor {
            type Value = Overflow;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("an integer, float, or fraction string (e.g. 1, 1.5, \"3/2\")")
            }

            fn visit_u64<E: serde::de::Error>(self, value: u64) -> Result<Overflow, E> {
                let nz =
                    NonZeroU64::new(value).ok_or_else(|| E::custom("overflow must be non-zero"))?;
                Ok(Overflow(nz, NonZeroU64::new(1).unwrap()))
            }

            fn visit_i64<E: serde::de::Error>(self, value: i64) -> Result<Overflow, E> {
                let value: u64 = value
                    .try_into()
                    .map_err(|_| E::custom("overflow must be a positive integer"))?;
                self.visit_u64(value)
            }

            fn visit_f64<E: serde::de::Error>(self, value: f64) -> Result<Overflow, E> {
                if value.is_nan() || value.is_infinite() || value <= 0.0 {
                    return Err(E::custom(format!(
                        "overflow float must be finite and positive, got {value}"
                    )));
                }
                Ok(amp_worker_core::compaction::Overflow::from(value).into())
            }

            fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Overflow, E> {
                value.parse().map_err(E::custom)
            }
        }

        deserializer.deserialize_any(OverflowVisitor)
    }
}

#[cfg(feature = "schemars")]
fn overflow_schema(_gen: &mut schemars::SchemaGenerator) -> schemars::Schema {
    schemars::json_schema!({
        "oneOf": [
            { "type": "integer", "minimum": 1 },
            { "type": "number", "exclusiveMinimum": 0 },
            { "type": "string" }
        ]
    })
}

/// Errors that can occur when parsing an [`Overflow`] from a string.
#[derive(Debug, thiserror::Error)]
pub enum OverflowParseError {
    /// The input string was empty or contained only whitespace.
    #[error("input is empty")]
    Empty,
    /// The numerator in a `"n/d"` fraction could not be parsed as an integer.
    #[error("invalid numerator")]
    InvalidNumerator(#[source] std::num::ParseIntError),
    /// The denominator in a `"n/d"` fraction could not be parsed as an integer.
    #[error("invalid denominator")]
    InvalidDenominator(#[source] std::num::ParseIntError),
    /// The numerator resolved to zero, which is not a valid overflow multiplier.
    #[error("numerator must be non-zero")]
    ZeroNumerator,
    /// The denominator resolved to zero, which would cause a division-by-zero.
    #[error("denominator must be non-zero")]
    ZeroDenominator,
    /// The input looked like a float but could not be parsed as `f64`.
    #[error("invalid float")]
    InvalidFloat(#[source] std::num::ParseFloatError),
    /// The parsed float value was not positive (zero, negative, NaN, or infinite).
    #[error("value must be positive")]
    NotPositive,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_str_with_simple_fraction_parses_correctly() {
        //* Given
        let input = "3/2";

        //* When
        let result: Overflow = input.parse().expect("should parse valid fraction");

        //* Then
        assert_eq!(result.0.get(), 3, "numerator should be 3");
        assert_eq!(result.1.get(), 2, "denominator should be 2");
    }

    #[test]
    fn from_str_with_unreduced_fraction_parses_correctly() {
        //* Given
        let input = "12345/10000";

        //* When
        let result: Overflow = input.parse().expect("should parse valid fraction");

        //* Then
        assert_eq!(result.0.get(), 12345, "numerator should be 12345");
        assert_eq!(result.1.get(), 10000, "denominator should be 10000");
    }

    #[test]
    fn from_str_with_integer_parses_correctly() {
        //* Given
        let input = "3";

        //* When
        let result: Overflow = input.parse().expect("should parse integer");

        //* Then
        assert_eq!(result.0.get(), 3, "numerator should be 3");
        assert_eq!(result.1.get(), 1, "denominator should be 1");
    }

    #[test]
    fn from_str_with_one_parses_correctly() {
        //* Given
        let input = "1";

        //* When
        let result: Overflow = input.parse().expect("should parse integer one");

        //* Then
        assert_eq!(result.0.get(), 1, "numerator should be 1");
        assert_eq!(result.1.get(), 1, "denominator should be 1");
    }

    #[test]
    fn from_str_with_float_parses_correctly() {
        //* Given
        let input = "1.5";

        //* When
        let result: Overflow = input.parse().expect("should parse float");

        //* Then
        assert_eq!(result.0.get(), 3, "numerator should be 3");
        assert_eq!(result.1.get(), 2, "denominator should be 2");
    }

    #[test]
    fn from_str_with_decimal_float_parses_correctly() {
        //* Given
        let input = "1.2345";

        //* When
        let result: Overflow = input.parse().expect("should parse decimal float");

        //* Then
        assert_eq!(result.0.get(), 2469, "numerator should be 2469");
        assert_eq!(result.1.get(), 2000, "denominator should be 2000");
    }

    #[test]
    fn from_str_with_empty_string_returns_error() {
        //* Given
        let input = "";

        //* When
        let result = input.parse::<Overflow>();

        //* Then
        let err = result.expect_err("empty string should fail to parse");
        assert!(
            matches!(err, OverflowParseError::Empty),
            "expected Empty, got {err:?}"
        );
    }

    #[test]
    fn from_str_with_whitespace_returns_error() {
        //* Given
        let input = "  ";

        //* When
        let result = input.parse::<Overflow>();

        //* Then
        let err = result.expect_err("whitespace-only string should fail to parse");
        assert!(
            matches!(err, OverflowParseError::Empty),
            "expected Empty, got {err:?}"
        );
    }

    #[test]
    fn from_str_with_zero_returns_error() {
        //* Given
        let input = "0";

        //* When
        let result = input.parse::<Overflow>();

        //* Then
        let err = result.expect_err("zero should fail to parse");
        assert!(
            matches!(err, OverflowParseError::ZeroNumerator),
            "expected ZeroNumerator, got {err:?}"
        );
    }

    #[test]
    fn from_str_with_zero_numerator_returns_error() {
        //* Given
        let input = "0/1";

        //* When
        let result = input.parse::<Overflow>();

        //* Then
        let err = result.expect_err("zero numerator should fail to parse");
        assert!(
            matches!(err, OverflowParseError::ZeroNumerator),
            "expected ZeroNumerator, got {err:?}"
        );
    }

    #[test]
    fn from_str_with_zero_denominator_returns_error() {
        //* Given
        let input = "1/0";

        //* When
        let result = input.parse::<Overflow>();

        //* Then
        let err = result.expect_err("zero denominator should fail to parse");
        assert!(
            matches!(err, OverflowParseError::ZeroDenominator),
            "expected ZeroDenominator, got {err:?}"
        );
    }

    #[test]
    fn from_str_with_non_numeric_returns_error() {
        //* Given
        let input = "abc";

        //* When
        let result = input.parse::<Overflow>();

        //* Then
        let err = result.expect_err("non-numeric input should fail to parse");
        assert!(
            matches!(err, OverflowParseError::InvalidFloat(_)),
            "expected InvalidFloat, got {err:?}"
        );
    }

    #[test]
    fn from_str_with_negative_value_returns_error() {
        //* Given
        let input = "-1";

        //* When
        let result = input.parse::<Overflow>();

        //* Then
        let err = result.expect_err("negative value should fail to parse");
        assert!(
            matches!(err, OverflowParseError::NotPositive),
            "expected NotPositive, got {err:?}"
        );
    }

    #[derive(serde::Deserialize, Debug)]
    #[allow(dead_code)]
    struct Config {
        overflow: Overflow,
    }

    #[test]
    fn deserialize_with_float_parses_correctly() {
        //* Given
        let json = r#"{"overflow": 1.5}"#;

        //* When
        let config: Config = serde_json::from_str(json).expect("should deserialize float");

        //* Then
        assert_eq!(config.overflow.0.get(), 3, "numerator should be 3");
        assert_eq!(config.overflow.1.get(), 2, "denominator should be 2");
    }

    #[test]
    fn deserialize_with_integer_parses_correctly() {
        //* Given
        let json = r#"{"overflow": 3}"#;

        //* When
        let config: Config = serde_json::from_str(json).expect("should deserialize integer");

        //* Then
        assert_eq!(config.overflow.0.get(), 3, "numerator should be 3");
        assert_eq!(config.overflow.1.get(), 1, "denominator should be 1");
    }

    #[test]
    fn deserialize_with_fraction_string_parses_correctly() {
        //* Given
        let json = r#"{"overflow": "3/2"}"#;

        //* When
        let config: Config =
            serde_json::from_str(json).expect("should deserialize fraction string");

        //* Then
        assert_eq!(config.overflow.0.get(), 3, "numerator should be 3");
        assert_eq!(config.overflow.1.get(), 2, "denominator should be 2");
    }

    #[test]
    fn deserialize_with_float_string_parses_correctly() {
        //* Given
        let json = r#"{"overflow": "1.2345"}"#;

        //* When
        let config: Config = serde_json::from_str(json).expect("should deserialize float string");

        //* Then
        assert_eq!(config.overflow.0.get(), 2469, "numerator should be 2469");
        assert_eq!(config.overflow.1.get(), 2000, "denominator should be 2000");
    }

    #[test]
    fn deserialize_with_zero_integer_returns_error() {
        //* Given
        let json = r#"{"overflow": 0}"#;

        //* When
        let result = serde_json::from_str::<Config>(json);

        //* Then
        result.expect_err("zero integer should fail to deserialize");
    }

    #[test]
    fn deserialize_with_negative_integer_returns_error() {
        //* Given
        let json = r#"{"overflow": -1}"#;

        //* When
        let result = serde_json::from_str::<Config>(json);

        //* Then
        result.expect_err("negative integer should fail to deserialize");
    }

    #[test]
    fn deserialize_with_zero_float_returns_error() {
        //* Given
        let json = r#"{"overflow": 0.0}"#;

        //* When
        let result = serde_json::from_str::<Config>(json);

        //* Then
        result.expect_err("zero float should fail to deserialize");
    }

    #[test]
    fn deserialize_with_zero_string_returns_error() {
        //* Given
        let json = r#"{"overflow": "0"}"#;

        //* When
        let result = serde_json::from_str::<Config>(json);

        //* Then
        result.expect_err("zero string should fail to deserialize");
    }
}
