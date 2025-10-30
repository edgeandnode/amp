use std::{
    error::Error as StdError,
    fmt::{Display, Formatter, Result as FmtResult},
};

use crate::{adbc::error::AdbcError, bigquery::schema::SchemaError};

#[derive(Debug)]
pub enum Error {
    NotImplemented,
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Error::NotImplemented => write!(f, "Feature not implemented"),
        }
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        use Error::*;

        match self {
            NotImplemented => None,
        }
    }
}

impl From<SchemaError> for Error {
    fn from(_error: SchemaError) -> Self {
        Error::NotImplemented
    }
}

impl From<AdbcError> for Error {
    fn from(_error: AdbcError) -> Self {
        Error::NotImplemented
    }
}

impl From<Error> for crate::DriverError {
    fn from(error: Error) -> Self {
        crate::DriverError::BigQuery(error)
    }
}

impl From<Error> for crate::Error {
    fn from(error: Error) -> Self {
        crate::Error::Driver(error.into())
    }
}

impl From<SchemaError> for crate::Error {
    fn from(error: SchemaError) -> Self {
        Error::from(error).into()
    }
}
