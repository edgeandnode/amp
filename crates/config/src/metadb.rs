use std::path::Path;

use figment::{
    Figment,
    providers::{Env, Format as _, Serialized, Toml},
};
pub use metadata_db::DEFAULT_POOL_SIZE as DEFAULT_METADB_CONN_POOL_SIZE;

/// Default metadata database directory name (inside `.amp/`) - stores PostgreSQL data
pub const DEFAULT_METADB_DIRNAME: &str = "metadb";

/// Metadata database connection and behavior settings.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct MetadataDbConfig {
    /// Database connection URL (required)
    pub url: String,
    /// Size of the connection pool (default: 10)
    #[serde(default = "default_pool_size")]
    pub pool_size: u32,
    /// Automatically run database migrations on startup (default: true)
    #[serde(default = "default_auto_migrate")]
    pub auto_migrate: bool,
}

/// Serde default for [`MetadataDbConfig::pool_size`]. Returns [`DEFAULT_POOL_SIZE`].
fn default_pool_size() -> u32 {
    DEFAULT_METADB_CONN_POOL_SIZE
}

/// Serde default for [`MetadataDbConfig::auto_migrate`]. Returns `true`.
fn default_auto_migrate() -> bool {
    true
}

/// Load metadata database configuration from TOML file and environment variables.
///
/// Builds a Figment pipeline for MetadataDbConfig extraction:
/// 1. TOML file (if exists)
/// 2. `AMP_CONFIG_METADATA_DB__*` env vars (highest priority)
/// 3. Deprecated `metadata_db_url` flat key (backward compatibility)
/// 4. Optional fallback URL (lowest priority — solo mode managed pg URL)
///
/// Returns `None` when no source provides `metadata_db.url` (including when
/// `fallback_url` is `None`). This allows callers to distinguish between
/// "user provided DB URL" and "no DB URL provided" for external vs managed
/// postgres detection in solo mode.
///
/// # Parameters
///
/// - `config_path`: Path to TOML config file (may not exist)
/// - `fallback_url`: Optional fallback URL used as lowest-priority default
///   (typically the managed PostgreSQL URL in solo mode)
///
/// # Priority Chain
///
/// Highest to lowest:
/// 1. `AMP_CONFIG_METADATA_DB__URL` env var
/// 2. `metadata_db.url` in TOML file
/// 3. Deprecated `metadata_db_url` flat key
/// 4. `fallback_url` parameter (if `Some`)
pub fn load(config_path: &Path, fallback_url: Option<&str>) -> Option<MetadataDbConfig> {
    let mut figment = Figment::new()
        .merge(Toml::file(config_path))
        .merge(Env::prefixed("AMP_CONFIG_").split("__"));

    // Handle deprecated flat key (lower priority — won't override if metadata_db.url exists)
    if let Ok(url) = figment.extract_inner::<String>("metadata_db_url") {
        tracing::warn!(
            "config key `metadata_db_url` is deprecated; \
             use `[metadata_db]` section with `url` instead"
        );
        figment = figment.join(Serialized::default("metadata_db.url", url));
    }

    // Join fallback URL if provided (lowest priority — fills gaps only)
    if let Some(url) = fallback_url {
        figment = figment.join(Serialized::default("metadata_db.url", url));
    }

    // Extract MetadataDbConfig from the "metadata_db" section
    figment
        .extract_inner::<MetadataDbConfig>("metadata_db")
        .ok()
}
