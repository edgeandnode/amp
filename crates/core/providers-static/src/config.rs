use std::num::NonZeroU64;

use crate::kind::StaticProviderKind;

/// Static provider configuration for parsing TOML config.
///
/// This structure defines the parameters required to serve static datasets
/// via object store. The `kind` field validates that the config belongs to
/// a `static` provider at deserialization time.
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct StaticProviderConfig {
    /// The provider kind, must be `"static"`.
    pub kind: StaticProviderKind,

    /// Root URL of the object store containing static data files.
    ///
    /// Supports any scheme recognized by `amp-object-store` (e.g., `file://`,
    /// `s3://`, `gs://`). Stored as a string and validated at provider
    /// initialization time because `ObjectStoreUrl` does not implement
    /// `Deserialize`.
    pub object_store_root: String,

    /// Maximum bytes for the in-memory object store cache.
    ///
    /// Defaults to 10 MiB (`10_485_760`). Set to `0` to disable caching.
    #[serde(default = "default_in_memory_max_bytes")]
    pub in_memory_max_bytes: u64,

    /// Maximum number of rows used for schema inference.
    ///
    /// Defaults to `256`. Must be at least `1`.
    #[serde(default = "default_schema_inference_max_rows")]
    pub schema_inference_max_rows: NonZeroU64,
}

/// Default maximum bytes for in-memory object store cache (10 MiB).
///
/// Set to `0` to disable caching entirely.
const fn default_in_memory_max_bytes() -> u64 {
    10_485_760
}

/// Default maximum number of rows used for schema inference.
const fn default_schema_inference_max_rows() -> NonZeroU64 {
    // SAFETY: 256 is non-zero
    unsafe { NonZeroU64::new_unchecked(256) }
}
