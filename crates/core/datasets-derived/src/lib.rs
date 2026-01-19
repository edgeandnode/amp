//! Derived dataset implementation.
//!
//! This crate implements the "derived" dataset type, which allows defining datasets
//! through declarative manifests that specify transformations over other datasets.
//!
//! ## Derived Datasets
//!
//! Derived datasets are defined through JSON manifests that specify:
//! - Source datasets to query from
//! - SQL transformations to apply
//! - Output schema and metadata
//!
//! See [`manifest::Manifest`] for the complete derived dataset specification.

mod dataset;
mod dataset_kind;
pub mod logical;
pub mod manifest;

pub use self::{
    dataset_kind::{DerivedDatasetKind, DerivedDatasetKindError},
    logical::{
        DatasetError, ManifestValidationError, SortTablesByDependenciesError,
        TableDependencySortError, dataset, sort_tables_by_dependencies, validate,
    },
    manifest::Manifest,
};
