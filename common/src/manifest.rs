//! Manifest for a dataset.
//!
//! Note: This replaces the "sql dataset".

use std::collections::BTreeMap;

use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct DatasetDef {
    pub name: String,
    pub version: String,
    pub repository: Option<String>,
    // Optional, defaults to `Dataset.md` if not provided
    pub readme: Option<String>,
    pub dependencies: BTreeMap<String, Dependency>,
    pub tables: BTreeMap<String, Query>,
}

#[derive(Debug, Deserialize)]
pub struct Dependency {
    pub owner: String,
    pub name: String,
    pub version: String,
}

#[derive(Debug, Deserialize)]
pub struct Query {
    pub sql: String,
}
