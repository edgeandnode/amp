//! Manifest for a dataset.
//!
//! Note: This replaces the "sql dataset".

use std::collections::BTreeMap;

use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct DatasetManifest {
    pub name: String,
    pub version: String,
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
