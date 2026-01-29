use anyhow::{Context, Result};
use figment::{
    Figment,
    providers::{Env, Format, Toml},
};
use serde::Deserialize;

/// CLI configuration with environment and file-based overrides.
#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct Config {
    /// Local Arrow Flight gRPC query service URL.
    #[allow(dead_code)]
    pub local_query_url: String,
    /// Local admin API URL.
    pub local_admin_url: String,
    /// Dataset registry API URL.
    pub registry_url: String,
    /// Default data source: "registry" or "local".
    pub default_source: String,
    /// Authentication service URL.
    pub auth_url: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            local_query_url: "grpc://localhost:1602".to_string(),
            local_admin_url: "http://localhost:1610".to_string(),
            registry_url: "https://api.registry.amp.staging.thegraph.com".to_string(),
            default_source: "registry".to_string(),
            auth_url: "https://auth.amp.thegraph.com".to_string(),
        }
    }
}

impl Config {
    /// Loads configuration from file and environment with precedence.
    ///
    /// Sources (highest to lowest precedence):
    /// 1. Environment variables with `AMP_CC_` prefix
    /// 2. Config file at `~/.config/ampcc/config.toml`
    /// 3. Built-in defaults
    pub fn load() -> Result<Self> {
        let mut figment = Figment::new();

        // Load from ~/.config/ampcc/config.toml
        if let Some(dirs) = directories::ProjectDirs::from("com", "amp", "ampcc") {
            let config_path = dirs.config_dir().join("config.toml");
            figment = figment.merge(Toml::file(config_path));
        }

        // Env vars override: AMP_CC_LOCAL_QUERY_URL, etc.
        figment = figment.merge(Env::prefixed("AMP_CC_"));

        figment.extract().context("failed to load configuration")
    }
}
