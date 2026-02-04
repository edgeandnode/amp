//! TOML configuration file loading with environment variable overrides.
//!
//! Deserializes a [`ConfigFile`] from a TOML file using [Figment], merging
//! `AMP_CONFIG_*` environment variables and handling deprecated key migration.
//! The raw [`ConfigFile`] is later resolved into a [`Config`](crate::Config)
//! by [`crate::load_config`].
//!
//! ## Priority chain
//!
//! [Figment] layers providers with two strategies: [`merge`](Figment::merge)
//! (overwrites existing values) and [`join`](Figment::join) (fills gaps only).
//!
//! The resulting priority from highest to lowest:
//!
//! | Priority | Source | Mechanism |
//! |----------|--------|-----------|
//! | 1 (highest) | `AMP_CONFIG_*` env vars | `merge` — always wins |
//! | 2 | TOML file values | `merge` — base configuration |
//! | 3 | Deprecated flat keys (`metadata_db_url`, `dataset_defs_dir`) | `join` — fallback only |
//! | 4 (lowest) | Caller-provided `ConfigDefaultsOverride`s | `join` — fallback only |
//!
//! ## Environment variables
//!
//! All env vars are prefixed with `AMP_CONFIG_` and use double underscores to
//! separate nested keys. For example, `AMP_CONFIG_METADATA_DB__URL` maps to
//! `metadata_db.url` in the config file.

use std::path::{Path, PathBuf};

use dump::{ConfigDuration, ParquetConfig};
use figment::{
    Figment,
    providers::{Env, Format as _, Serialized, Toml},
};
use monitoring::config::OpenTelemetryConfig;

/// Default data directory name - stores Parquet files
pub const DEFAULT_DATA_DIRNAME: &str = "data";

/// Default providers directory name - stores provider configs
pub const DEFAULT_PROVIDERS_DIRNAME: &str = "providers";

/// Default manifests directory name - stores dataset manifests
pub const DEFAULT_MANIFESTS_DIRNAME: &str = "manifests";

/// Default maximum interval for derived dataset dump microbatches (in blocks)
pub const DEFAULT_MICROBATCH_MAX_INTERVAL: u64 = 100_000;

/// Default maximum interval for streaming server microbatches (in blocks)
pub const DEFAULT_SERVER_MICROBATCH_MAX_INTERVAL: u64 = 1_000;

/// Default keep-alive interval for streaming server (in seconds)
pub const DEFAULT_KEEP_ALIVE_INTERVAL: u64 = 30;

/// A low-priority configuration default applied via Figment's `join` strategy.
/// Fills gaps only — never overwrites TOML file or env var values.
pub struct ConfigDefaultsOverride(Figment);

impl ConfigDefaultsOverride {
    /// Provide a default `data_dir` path as an absolute path.
    pub fn data_dir(base_dir: &Path) -> Self {
        Self(
            Figment::new().join(Serialized::default(
                "data_dir",
                base_dir
                    .join(DEFAULT_DATA_DIRNAME)
                    .to_string_lossy()
                    .into_owned(),
            )),
        )
    }

    /// Provide a default `providers_dir` path as an absolute path.
    pub fn providers_dir(base_dir: &Path) -> Self {
        Self(
            Figment::new().join(Serialized::default(
                "providers_dir",
                base_dir
                    .join(DEFAULT_PROVIDERS_DIRNAME)
                    .to_string_lossy()
                    .into_owned(),
            )),
        )
    }

    /// Provide a default `manifests_dir` path as an absolute path.
    pub fn manifests_dir(base_dir: &Path) -> Self {
        Self(
            Figment::new().join(Serialized::default(
                "manifests_dir",
                base_dir
                    .join(DEFAULT_MANIFESTS_DIRNAME)
                    .to_string_lossy()
                    .into_owned(),
            )),
        )
    }
}

/// Empty default set for callers that don't need defaults.
pub fn no_defaults_override() -> Option<ConfigDefaultsOverride> {
    None
}

/// Load a [`ConfigFile`] from a TOML file with env-var overrides.
///
/// See the [module-level docs](self) for the priority chain.
///
/// # MetadataDbConfig Extraction
///
/// This function no longer extracts `MetadataDbConfig`. Use
/// [`crate::metadb::load`] separately to load the database
/// configuration with its own Figment pipeline.
pub fn load(
    config_path: &Path,
    defaults: impl IntoIterator<Item = ConfigDefaultsOverride>,
) -> Result<ConfigFile, LoadConfigFileError> {
    let mut figment = Figment::new()
        .merge(Toml::file(config_path))
        .merge(Env::prefixed("AMP_CONFIG_").split("__"));

    // Remap deprecated dataset_defs_dir key (lower priority — won't override if manifests_dir exists)
    if let Ok(value) = figment.extract_inner::<String>("dataset_defs_dir") {
        tracing::warn!(
            "config key `dataset_defs_dir` is deprecated; \
            use `manifests_dir` instead"
        );
        figment = figment.join(Serialized::default("manifests_dir", value));
    }

    // Caller-provided ConfigDefaultsOverride (lowest priority — fills gaps only)
    for ConfigDefaultsOverride(provider) in defaults {
        figment = figment.join(provider);
    }

    figment
        .extract()
        .map_err(|err| LoadConfigFileError(Box::new(err)))
}

/// Raw configuration as deserialized from the TOML config file.
///
/// Fields use serde defaults and are resolved into a [`Config`](crate::Config) by [`load_config`](crate::load_config).
#[derive(Debug, Clone, serde::Deserialize)]
pub struct ConfigFile {
    // Storage paths
    /// Where the extracted datasets are stored (default: `data`)
    #[serde(default = "default_data_dir")]
    data_dir: String,
    /// Path to a providers directory. Each provider is configured as a separate toml file in this directory (default: `providers`)
    #[serde(default = "default_providers_dir")]
    providers_dir: String,
    /// Path to a directory containing dataset manifest files (default: `manifests`)
    #[serde(default = "default_manifests_dir")]
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
    #[serde(default)]
    pub poll_interval_secs: ConfigDuration<1>,
    /// Max interval for derived dataset dump microbatches in blocks (default: 100000)
    #[serde(default = "default_microbatch_max_interval")]
    pub microbatch_max_interval: u64,
    /// Max interval for streaming server microbatches in blocks (default: 1000)
    #[serde(default = "default_server_microbatch_max_interval")]
    pub server_microbatch_max_interval: u64,
    /// Keep-alive interval for streaming server in seconds (default: 30; min: 30)
    #[serde(default = "default_keep_alive_interval")]
    pub keep_alive_interval: u64,

    // Service addresses
    /// Arrow Flight RPC server address (default: "0.0.0.0:1602")
    pub flight_addr: Option<String>,
    /// JSON Lines server address (default: "0.0.0.0:1603")
    pub jsonl_addr: Option<String>,
    /// Admin API server address (default: "0.0.0.0:1610")
    pub admin_api_addr: Option<String>,

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

/// Serde default for [`ConfigFile::data_dir`]. Returns [`DEFAULT_DATA_DIRNAME`].
fn default_data_dir() -> String {
    DEFAULT_DATA_DIRNAME.into()
}

/// Serde default for [`ConfigFile::providers_dir`]. Returns [`DEFAULT_PROVIDERS_DIRNAME`].
fn default_providers_dir() -> String {
    DEFAULT_PROVIDERS_DIRNAME.into()
}

/// Serde default for [`ConfigFile::manifests_dir`]. Returns [`DEFAULT_MANIFESTS_DIRNAME`].
fn default_manifests_dir() -> String {
    DEFAULT_MANIFESTS_DIRNAME.into()
}

/// Serde default for [`ConfigFile::microbatch_max_interval`]. Returns [`DEFAULT_MICROBATCH_MAX_INTERVAL`].
fn default_microbatch_max_interval() -> u64 {
    DEFAULT_MICROBATCH_MAX_INTERVAL
}

/// Serde default for [`ConfigFile::server_microbatch_max_interval`]. Returns [`DEFAULT_SERVER_MICROBATCH_MAX_INTERVAL`].
fn default_server_microbatch_max_interval() -> u64 {
    DEFAULT_SERVER_MICROBATCH_MAX_INTERVAL
}

/// Serde default for [`ConfigFile::keep_alive_interval`]. Returns [`DEFAULT_KEEP_ALIVE_INTERVAL`].
fn default_keep_alive_interval() -> u64 {
    DEFAULT_KEEP_ALIVE_INTERVAL
}

/// Error when loading configuration from a TOML file.
#[derive(Debug, thiserror::Error)]
#[error("Failed to load configuration file")]
pub struct LoadConfigFileError(#[source] pub Box<figment::Error>);
