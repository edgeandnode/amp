//! Common types and utilities for dataset definitions.
//!
//! This module provides shared structures used across different dataset definition formats,
//! including serializable schema representations and common dataset metadata.

pub mod block_range;
pub mod dataset;
pub mod dataset_kind_str;
pub mod fqn;
pub mod hash;
pub mod hash_reference;
pub mod manifest;
pub mod name;
pub mod namespace;
pub mod partial_reference;
pub mod reference;
pub mod revision;
pub mod table_name;
pub mod version;

pub use self::dataset::{BlockNum, SPECIAL_BLOCK_NUM};
