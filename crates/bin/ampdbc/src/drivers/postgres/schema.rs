use std::{
    error::Error as StdError,
    fmt::{Display, Formatter, Result as FmtResult},
};

pub(crate) use crate::arrow::{FieldRef, SchemaRef};
use crate::{SchemaExt, schema::{TableKind, TableRef}};

#[derive(Clone, Debug)]
pub struct Schema {
    pub schema: SchemaRef,
    pub table_ref: TableRef,
    pub table_kind: TableKind,
}


impl SchemaExt for Schema {
    type ErrorType = super::error::Error;
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
        Ok(self.schema.fields().iter().map(|field| (field.name().to_string(), field.data_type().to_string())).collect())
    }
    fn as_insert_stmt(&self) -> Result<String, Self::ErrorType> {
        todo!()
    }

    fn as_table_ddl(&self) -> Result<String, Self::ErrorType> {
        todo!()
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
