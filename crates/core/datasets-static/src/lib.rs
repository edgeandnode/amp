//! Static dataset manifest definition.
//!
//! This crate implements the "static" dataset type, which allows defining datasets
//! backed by static CSV files with explicit schema definitions.
//!
//! ## Static Datasets
//!
//! Static datasets are defined through JSON manifests that specify:
//! - File paths to static data files (e.g., CSV)
//! - File format and header configuration
//! - Arrow schema for each table
//!
//! See [`manifest::Manifest`] for the complete static dataset specification.

pub mod dataset;
pub mod dataset_kind;
pub mod file_format;
pub mod file_path;
pub mod manifest;
