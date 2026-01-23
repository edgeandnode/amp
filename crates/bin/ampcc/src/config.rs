use anyhow::{Context, Result};
use figment::{
    Figment,
    providers::{Env, Format, Toml},
};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Config {
    #[allow(dead_code)]
    #[serde(default = "default_local_query_url")]
    pub local_query_url: String,
    #[serde(default = "default_local_admin_url")]
    pub local_admin_url: String,
    #[serde(default = "default_registry_url")]
    pub registry_url: String,
    #[serde(default = "default_source")]
    pub default_source: String,
    #[serde(default = "default_auth_url")]
    pub auth_url: String,
}

fn default_local_query_url() -> String {
    "grpc://localhost:1602".into()
}
fn default_local_admin_url() -> String {
    "http://localhost:1610".into()
}
fn default_registry_url() -> String {
    "https://api.registry.amp.staging.thegraph.com".into()
}
fn default_source() -> String {
    "registry".into()
}
fn default_auth_url() -> String {
    "https://auth.amp.thegraph.com".into()
}

impl Config {
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
