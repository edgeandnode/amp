use std::path::{Path, PathBuf};

use amp_config::{Config, ConfigDefaultsOverride};

/// Loads configuration (for non-solo commands).
///
/// Requires a config file. Use `amp_config::metadb::load()` separately
/// to load database configuration.
///
/// Directory defaults (`data_dir`, `providers_dir`, `manifests_dir`) are derived from
/// the config file's parent directory using [`ConfigDefaultsOverride`] per-dir defaults, matching
/// solo mode's dynamic defaults pattern.
pub fn load(config_path: impl Into<PathBuf>) -> Result<Config, LoadError> {
    let config_path = config_path.into();
    let base_dir = config_path.parent().unwrap_or(Path::new("."));
    let defaults = [
        ConfigDefaultsOverride::data_dir(base_dir),
        ConfigDefaultsOverride::providers_dir(base_dir),
        ConfigDefaultsOverride::manifests_dir(base_dir),
    ];
    amp_config::load_config(config_path, defaults).map_err(LoadError)
}

/// Failed to load configuration from file.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct LoadError(#[from] amp_config::LoadConfigError);
