//! Dataset registry and manifest management
//!
//! This module provides a unified interface for managing dataset registrations,
//! manifests, and version tags. It encompasses all database operations related to
//! dataset metadata, including:
//!
//! - **Type definitions**: Dataset identification (namespace, name, version, hash)
//! - **manifest_files**: Content-addressable manifest storage indexed by SHA256 hash
//! - **manifests**: Many-to-many junction table linking datasets to manifests
//! - **tags**: Version tags (semver, "latest", "dev") pointing to dataset-manifest combinations

mod hash;
mod name;
mod namespace;
mod version;

pub mod manifest_files;
pub mod manifests;
pub mod tags;

pub use self::{
    hash::{Hash, HashOwned},
    name::{Name, NameOwned},
    namespace::{Namespace, NamespaceOwned},
    version::{Version, VersionOwned},
};
