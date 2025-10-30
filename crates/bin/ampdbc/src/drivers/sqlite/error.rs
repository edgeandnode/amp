use std::{
    error::Error as StdError,
    fmt::{Display, Formatter, Result as FmtResult},
};

use crate::adbc::error::AdbcError;

use super::schema::SchemaError;

#[derive(Debug)]
pub enum Error {
    Adbc(AdbcError),
    Schema(SchemaError),
    Unimplemented {
        method: String,
        msg: String,
    },
}

impl Error {
    pub fn unimplemented_feature(method: &str, msg: &str) -> Self {
        Error::Unimplemented {
            method: method.to_string(),
            msg: msg.to_string(),
        }
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Error::Adbc(e) => write!(f, "ADBC Error: {}", e),
            Error::Schema(e) => e.fmt(f),
            Error::Unimplemented { method, msg } => {
                f.debug_struct("Unimplemented Feature")
                    .field("driver", &"SQLite")
                    .field("method", method)
                    .field("message", msg)
                    .finish()
            }
        }
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        use Error::*;

        match self {
            Adbc(e) => e.source(),
            Schema(e) => e.source(),
            Unimplemented{..} => None,
        }
    }
}

impl From<AdbcError> for Error {
    fn from(e: AdbcError) -> Self {
        Error::Adbc(e)
    }
}

impl From<SchemaError> for Error {
    fn from(e: SchemaError) -> Self {
        Error::Schema(e)
    }
}

impl From<Error> for crate::DriverError {
    fn from(value: Error) -> Self {
        crate::DriverError::Sqlite(value)
    }
}

impl From<Error> for crate::Error {
    fn from(error: Error) -> Self {
        crate::Error::Driver(error.into())
    }
}

impl From<SchemaError> for crate::Error {
    fn from(e: SchemaError) -> Self {
        Error::Schema(e).into()
    }
}
