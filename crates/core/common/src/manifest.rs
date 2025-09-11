//! Dataset manifest definitions and utilities.
//!
//! This module provides the manifest system for defining datasets in Nozzle.
//! It includes both the derived dataset format and legacy SQL dataset support.
//!
//! ## Modules
//!
//! - [`common`] - Common types and utilities shared across manifest formats
//! - [`derived`] - Derived dataset definitions with versioning and dependencies  
//! - [`sql_datasets`] - Legacy SQL dataset format (deprecated, use derived datasets for new datasets)
//!
//! ## Dataset Categories
//!
//! Derived datasets (defined in [`derived`]) replace the legacy SQL dataset format.
//! Derived datasets transform and combine data from existing datasets and provide:
//!
//! - Semantic versioning
//! - Explicit dependency management  
//! - Type-safe schema definitions
//! - User-defined function support

pub mod common;
pub mod derived;
pub mod sql_datasets;
