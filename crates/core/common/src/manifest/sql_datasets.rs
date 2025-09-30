//! Legacy SQL dataset definitions and utilities.
//!
//! This module provides support for the legacy SQL dataset format, which is being
//! replaced by derived datasets in [`crate::manifest::derived`].
//! New datasets should use derived datasets instead.

use std::collections::BTreeMap;

use datafusion::sql::parser;

use crate::Dataset;

/// A legacy SQL dataset with its associated queries.
///
/// This structure represents the legacy SQL dataset format that is being phased out
/// in favor of derived datasets. It contains a dataset definition
/// and a mapping of table names to their defining SQL queries.
pub struct SqlDataset {
    /// The underlying dataset definition
    pub dataset: Dataset,
    /// Maps table names to their defining SQL queries
    pub queries: BTreeMap<String, parser::Statement>,
}
