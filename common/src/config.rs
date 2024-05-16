use std::path::PathBuf;

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub data_location: String,
    pub max_mem_mb: usize,
    pub spill_location: Vec<PathBuf>,
}

impl Config {
    pub fn load(file: PathBuf) -> anyhow::Result<Self> {
        let contents = std::fs::read_to_string(file)?;
        toml::from_str(&contents).map_err(Into::into)
    }

    pub fn location_only(data_location: String) -> Self {
        Self {
            data_location,
            max_mem_mb: 0,
            spill_location: vec![],
        }
    }
}
