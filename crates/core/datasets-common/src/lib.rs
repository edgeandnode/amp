//! Common types and utilities for dataset definitions.
//!
//! This module provides shared structures used across different dataset definition formats,
//! including serializable schema representations and common dataset metadata.

pub mod dataset;
pub mod deps;
pub mod fqn;
pub mod func_name;
pub mod hash;
pub mod hash_reference;
pub mod manifest;
pub mod name;
pub mod namespace;
pub mod partial_reference;
pub mod raw_dataset_kind;
pub mod reference;
pub mod revision;
pub mod table_name;
pub mod version;

/// Re-exports of UDF-related types for derived datasets and the common crate.
///
/// This module provides convenient access to types needed for implementing
/// the [`dataset::DatasetWithFunctions`] trait and for query execution contexts
/// that support user-defined JavaScript functions.
pub mod udf {
    pub use datafusion::logical_expr::{ScalarUDF, async_udf::AsyncScalarUDF};
    pub use js_runtime::{isolate_pool::IsolatePool, js_udf::JsUdf};
}
