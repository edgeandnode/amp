use std::{
    error::Error as StdError,
    fmt::{Display, Formatter, Result as FmtResult},
};

pub(crate) use crate::arrow::{FieldRef, SchemaRef};
use crate::{SchemaExt, error::sqlite::Error, schema::{TableKind, TableRef}};

#[derive(Clone, Debug)]
pub struct Schema {
    pub schema: SchemaRef,
    pub table_ref: TableRef,
    pub table_kind: TableKind,
}

impl SchemaExt for Schema {
    type ErrorType = Error;
    type Statement = super::statement::Statement;
    fn new(schema: SchemaRef, table_ref: TableRef, table_kind: TableKind) -> Self {
        Self {
            schema,
            table_ref,
            table_kind,
        }
    }

    fn as_external_types(
        &self,
    ) -> Result<Vec<(String, String)>, Self::ErrorType> {
        Err(Error::unimplemented_feature(
            "sqlite::Schema::as_external_types",
            "Map Arrow types to SQLite types",
        ))
    }

    fn as_insert_stmt(&self) -> Result<String, Self::ErrorType> {
        Err(Error::unimplemented_feature(
            "sqlite::Schema::as_insert_stmt",
            "Generate INSERT INTO statement for SQLite",
        ))
    }

    fn as_table_ddl(&self) -> Result<String, Self::ErrorType> {
        Err(Error::unimplemented_feature(
            "sqlite::Schema::as_table_ddl",
            "Generate CREATE TABLE DDL for SQLite",
        ))
    }

    fn table_kind(&self) -> &TableKind {
        &self.table_kind
    }

    fn table_ref(&self) -> &crate::schema::TableRef {
        &self.table_ref
    }
}

#[derive(Debug)]
pub enum SchemaError {
    UnsupportedTypes { fields: Vec<FieldRef> },
}

impl Display for SchemaError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            SchemaError::UnsupportedTypes { fields } => {
                write!(f, "Unsupported field types: {:?}", fields)
            }
        }
    }
}

impl StdError for SchemaError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            SchemaError::UnsupportedTypes { .. } => None,
        }
    }
}
