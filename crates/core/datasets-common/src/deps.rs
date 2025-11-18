//! Dependency alias types for derived datasets.
//!
//! This module provides types for working with dependency aliases in derived dataset manifests,
//! allowing datasets to reference their dependencies using short, convenient names.
//!
//! TODO: This module should eventually be moved to `datasets-derived` crate as it's specific
//! to derived datasets. It's temporarily in `datasets-common` to avoid circular dependencies
//! during the refactoring process.

pub mod alias;
pub mod reference;
