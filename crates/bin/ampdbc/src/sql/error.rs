use std::{
    error::Error as StdError,
    fmt::{Display, Formatter, Result as FmtResult},
};

use common::{BoxError, query_context::Error as ContextError};
use dataset_store::CatalogForSqlError;

use crate::datafusion::{DataFusionError as QueryEnvError, DataFusionError as PlanningError};

#[derive(Debug)]
pub enum Error {
    // Failed to obtain catalog for SQL query
    Catalog(CatalogForSqlError),
    // Error in query planning or execution contexts
    Context(ContextError),
    // Error thrown if query is not incremental
    Incremental(BoxError),
    // Error in query environment initialization
    Environment(QueryEnvError),
    // Planning error
    Planning(PlanningError),
}

impl From<CatalogForSqlError> for Error {
    fn from(error: CatalogForSqlError) -> Self {
        Error::Catalog(error)
    }
}

impl From<ContextError> for Error {
    fn from(error: ContextError) -> Self {
        Error::Context(error)
    }
}

impl From<BoxError> for Error {
    fn from(error: BoxError) -> Self {
        Error::Incremental(error)
    }
}

impl From<QueryEnvError> for Error {
    fn from(error: QueryEnvError) -> Self {
        Error::Environment(error)
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        use Error::*;
        match self {
            Catalog(e) => e.fmt(f),
            Context(e) => e.fmt(f),
            Incremental(e) => e.fmt(f),
            Environment(e) => e.fmt(f),
            Planning(e) => e.fmt(f),
        }
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        use Error::*;
        match self {
            Catalog(e) => e.source(),
            Context(e) => e.source(),
            Incremental(e) => Some(e.as_ref()),
            Environment(e) => e.source(),
            Planning(e) => e.source(),
        }
    }
}
