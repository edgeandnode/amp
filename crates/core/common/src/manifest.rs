//! Dataset manifest definitions and utilities.
//!
//! This module provides the manifest system for defining datasets in Nozzle.
//!
//! ## Modules
//!
//! - [`common`] - Common types and utilities shared across manifest formats
//! - [`derived`] - Derived dataset definitions with versioning and dependencies
//!
//! ## Dataset Categories
//!
//! Derived datasets transform and combine data from existing datasets and provide:
//!
//! - Semantic versioning
//! - Explicit dependency management
//! - Type-safe schema definitions
//! - User-defined function support

pub mod common;
pub mod derived;
