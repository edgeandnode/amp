use std::{
    error::Error as StdError,
    fmt::{Display, Formatter, Result as FmtResult},
};

use super::schema::SchemaError;
use crate::adbc::error::AdbcError;

#[derive(Debug)]
pub enum Error {
    Adbc(AdbcError),
    Schema(SchemaError),
    NotImplemented,
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Error::Adbc(err) => write!(f, "ADBC error: {}", err),
            Error::Schema(err) => write!(f, "Schema error: {}", err),
            Error::NotImplemented => write!(f, "Feature not implemented"),
        }
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        use Error::*;

        match self {
            Adbc(err) => err.source(),
            Schema(err) => err.source(),
            NotImplemented => None,
        }
    }
}

impl From<AdbcError> for Error {
    fn from(_err: AdbcError) -> Self {
        Error::NotImplemented
    }
}

impl From<SchemaError> for Error {
    fn from(value: SchemaError) -> Self {
        Error::Schema(value)
    }
}
impl From<Error> for crate::DriverError {
    fn from(value: Error) -> Self {
        crate::DriverError::Postgres(value)
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
