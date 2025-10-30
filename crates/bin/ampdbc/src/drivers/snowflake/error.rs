use std::{
    io::Error as IoError,
    error::Error as StdError,
    fmt::{Display, Formatter, Result as FmtResult},
};

use super::schema::SchemaError;
use crate::adbc::error::AdbcError;

#[derive(Debug)]
pub enum Error {
    Adbc(AdbcError),
    Io(IoError),
    Schema(SchemaError),
    Unimplemented { method: String, msg: String },
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        use Error::*;

        match self {
            Adbc(err) => write!(f, "ADBC error: {}", err),
            Io(err) => write!(f, "I/O error: {}", err),
            Schema(err) => write!(f, "Schema error: {}", err),
            Unimplemented { method, msg } => f
                .debug_struct("Unimplemented Feature")
                .field("driver", &"SQLite")
                .field("method", method)
                .field("message", msg)
                .finish(),
        }
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        use Error::*;

        match self {
            Adbc(err) => err.source(),
            Io(err) => Some(err),
            Schema(err) => err.source(),
            Unimplemented { .. } => None,
        }
    }
}

impl From<AdbcError> for Error {
    fn from(e: AdbcError) -> Self {
        Error::Adbc(e)
    }
}

impl From<IoError> for Error {
    fn from(e: IoError) -> Self {
        Error::Io(e)
    }
}

impl From<SchemaError> for Error {
    fn from(e: SchemaError) -> Self {
        Error::Schema(e)
    }
}

impl From<Error> for crate::DriverError {
    fn from(error: Error) -> Self {
        crate::DriverError::Snowflake(error)
    }
}

impl From<Error> for crate::Error {
    fn from(error: Error) -> Self {
        crate::Error::Driver(error.into())
    }
}

impl From<AdbcError> for crate::Error {
    fn from(e: AdbcError) -> Self {
        Error::Adbc(e).into()
    }
}

impl From<IoError> for crate::Error {
    fn from(e: IoError) -> Self {
        Error::Io(e).into()
    }
}

impl From<SchemaError> for crate::Error {
    fn from(e: SchemaError) -> Self {
        Error::Schema(e).into()
    }
}