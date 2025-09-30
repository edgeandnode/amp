//! Derived dataset implementation and legacy SQL dataset support.
//!
//! This crate implements the "derived" dataset type, which allows defining datasets
//! through declarative manifests that specify transformations over other datasets.
//! It also provides backward compatibility for legacy SQL datasets.
//!
//! ## Derived Datasets
//!
//! Derived datasets are defined through JSON manifests that specify:
//! - Source datasets to query from
//! - SQL transformations to apply
//! - Output schema and metadata
//!
//! See [`manifest::Manifest`] for the complete derived dataset specification.
//!
//! ## Legacy SQL Datasets
//!
//! The `sql_dataset` module (feature-gated) provides support for the legacy
//! SQL dataset format, which stores SQL queries in separate `.sql` files
//! alongside TOML/JSON metadata files. New datasets should use the derived
//! format instead.

mod dataset_kind;
pub mod manifest;

#[cfg(feature = "sql_dataset")]
pub mod sql_dataset;

pub use self::{
    dataset_kind::{DATASET_KIND, DerivedDatasetKind, DerivedDatasetKindError},
    manifest::Manifest,
};
