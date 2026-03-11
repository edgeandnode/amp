//! Static dataset definition and manifest-to-dataset conversion.
//!
//! Converts a parsed [`Manifest`] into a concrete [`Dataset`] that implements
//! the common [`datasets_common::dataset::Dataset`] trait for uniform downstream
//! consumption via `Arc<dyn Dataset>`.

use std::{collections::BTreeSet, sync::Arc};

use datafusion::arrow::datatypes::SchemaRef;
use datasets_common::{
    block_num::BlockNum, dataset::Table as TableTrait, dataset_kind_str::DatasetKindStr,
    hash_reference::HashReference, table_name::TableName,
};

use crate::{
    dataset_kind::StaticDatasetKind, file_format::FileFormat, file_path::FilePath,
    manifest::Manifest,
};

/// Convert a static dataset manifest into a logical dataset representation.
///
/// Validates cross-field invariants and converts manifest tables into the
/// internal `Dataset` structure used by the query engine.
pub fn dataset(reference: HashReference, manifest: Manifest) -> Result<Dataset, DatasetError> {
    if manifest.tables.is_empty() {
        return Err(DatasetError::EmptyTables);
    }

    for (table_name, table) in &manifest.tables {
        if table.schema.arrow.fields.is_empty() {
            return Err(DatasetError::EmptySchema {
                table_name: table_name.clone(),
            });
        }
    }

    let tables: Vec<Arc<dyn TableTrait>> = manifest
        .tables
        .into_iter()
        .map(|(name, table)| {
            Arc::new(Table {
                name,
                path: table.path,
                format: table.format,
                schema: table.schema.arrow.into(),
                sorted_by: table.sorted_by,
            }) as Arc<dyn TableTrait>
        })
        .collect();

    Ok(Dataset {
        kind: manifest.kind,
        reference,
        tables,
    })
}

/// A [`Dataset`](datasets_common::dataset::Dataset) backed by static data files.
///
/// Static datasets have no block concept, no dependencies, and no SQL
/// transformations. They simply describe data files with their schemas.
pub struct Dataset {
    kind: StaticDatasetKind,
    reference: HashReference,
    tables: Vec<Arc<dyn TableTrait>>,
}

impl datasets_common::dataset::Dataset for Dataset {
    fn reference(&self) -> &HashReference {
        &self.reference
    }

    fn kind(&self) -> DatasetKindStr {
        self.kind.into()
    }

    fn tables(&self) -> &[Arc<dyn TableTrait>] {
        &self.tables
    }

    fn table_names(&self) -> Vec<TableName> {
        self.tables.iter().map(|t| t.name().clone()).collect()
    }

    fn get_table(&self, name: &TableName) -> Option<&Arc<dyn TableTrait>> {
        self.tables.iter().find(|t| t.name() == name)
    }

    fn has_table(&self, name: &TableName) -> bool {
        self.tables.iter().any(|t| t.name() == name)
    }

    fn start_block(&self) -> Option<BlockNum> {
        None
    }

    fn finalized_blocks_only(&self) -> bool {
        false
    }
}

/// Errors that occur when converting a static dataset manifest to a logical dataset.
#[derive(Debug, thiserror::Error)]
pub enum DatasetError {
    /// Manifest has no tables defined.
    #[error("static dataset must have at least one table")]
    EmptyTables,

    /// A table's schema has no fields.
    #[error("table '{table_name}' has no fields in its schema")]
    EmptySchema { table_name: TableName },
}

/// A table definition for a static dataset.
///
/// Contains the table name, data file path, file format, Arrow schema,
/// and optional sort-order declaration.
#[derive(Clone, Debug)]
pub struct Table {
    name: TableName,
    path: FilePath,
    format: FileFormat,
    schema: SchemaRef,
    sorted_by: BTreeSet<String>,
}

impl Table {
    /// Returns the data file path for this table.
    pub fn path(&self) -> &FilePath {
        &self.path
    }

    /// Returns the file format for this table.
    pub fn format(&self) -> &FileFormat {
        &self.format
    }
}

impl TableTrait for Table {
    fn name(&self) -> &TableName {
        &self.name
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn sorted_by(&self) -> &BTreeSet<String> {
        &self.sorted_by
    }
}
