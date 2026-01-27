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

pub mod dataset;
mod dataset_kind;
pub mod deps;
pub mod function;
pub mod manifest;
pub mod sql_str;

pub use self::{
    dataset::Dataset,
    dataset_kind::{DerivedDatasetKind, DerivedDatasetKindError},
    manifest::Manifest,
};
