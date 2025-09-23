//! Common types and utilities for dataset definitions.
//!
//! This module provides shared structures used across different dataset definition formats,
//! including serializable schema representations and common dataset metadata.

use std::collections::HashMap;

use datasets_common::manifest::Schema;

use crate::{SPECIAL_BLOCK_NUM, Table as LogicalTable};

/// Create a Schema from a vector of LogicalTable.
pub fn schema_from_tables(tables: Vec<LogicalTable>) -> Schema {
    let mut schema = Schema::new();
    for table in tables {
        let inner_map = table
            .schema()
            .fields()
            .iter()
            .filter(|&field| field.name() != SPECIAL_BLOCK_NUM)
            .fold(HashMap::new(), |mut acc, field| {
                acc.insert(field.name().clone(), field.data_type().clone().into());
                acc
            });
        schema.insert_table(table.name().to_string(), inner_map);
    }
    schema
}

/// Create a Schema from a slice of LogicalTable.
pub fn schema_from_table_slice(tables: &[LogicalTable]) -> Schema {
    let mut schema = Schema::new();
    for table in tables {
        let inner_map = table
            .schema()
            .fields()
            .iter()
            .filter(|&field| field.name() != SPECIAL_BLOCK_NUM)
            .fold(HashMap::new(), |mut acc, field| {
                acc.insert(field.name().clone(), field.data_type().clone().into());
                acc
            });
        schema.insert_table(table.name().to_string(), inner_map);
    }
    schema
}
