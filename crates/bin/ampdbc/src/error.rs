use std::{
    error::Error as StdError,
    fmt::{Display, Formatter, Result as FmtResult},
    path::PathBuf,
    sync::Arc,
};

use amp_client::{Error as AmpClientError, InvalidationRange, Metadata};
use common::{BoxError, DataFusionError, config::ConfigError, metadata::segments::ResumeWatermark};
use dataset_store::CatalogForSqlError;
use tokio::sync::broadcast::error::SendError;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

pub use crate::drivers::{
    bigquery::error as bigquery, postgres::error as postgres, snowflake::error as snowflake,
    sqlite::error as sqlite,
};
use crate::{
    Connection,
    adbc::{AdbcConnection, error::AdbcError},
    arrow::{RecordBatch, error::ArrowError},
    sql::error::Error as QueryError,
};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Arrow(ArrowError),
    Client(AmpClientError),
    Config(ConfigError),
    Execution(ExecutionError),
    Query(QueryError),
    Driver(DriverError),
    Toml(toml::de::Error),
}

impl From<ArrowError> for Error {
    fn from(error: ArrowError) -> Self {
        Error::Arrow(error)
    }
}

impl From<CatalogForSqlError> for Error {
    fn from(error: CatalogForSqlError) -> Self {
        Error::Query(QueryError::Catalog(error))
    }
}

impl From<DriverError> for Error {
    fn from(error: DriverError) -> Self {
        Error::Driver(error)
    }
}

impl From<AmpClientError> for Error {
    fn from(error: AmpClientError) -> Self {
        Error::Client(error)
    }
}

impl From<BroadcastStreamRecvError> for Error {
    fn from(error: BroadcastStreamRecvError) -> Self {
        Error::Execution(ExecutionError::BroadcastStreamRecv(error))
    }
}

impl From<ConfigError> for Error {
    fn from(error: ConfigError) -> Self {
        Error::Config(error)
    }
}

impl From<common::query_context::Error> for Error {
    fn from(error: common::query_context::Error) -> Self {
        Error::Query(QueryError::Context(error))
    }
}

impl From<QueryError> for Error {
    fn from(error: QueryError) -> Self {
        Error::Query(error)
    }
}

impl From<SendError<(RecordBatch, Metadata)>> for Error {
    fn from(error: SendError<(RecordBatch, Metadata)>) -> Self {
        Error::Execution(ExecutionError::DataSend(error))
    }
}

impl From<SendError<Arc<[InvalidationRange]>>> for Error {
    fn from(error: SendError<Arc<[InvalidationRange]>>) -> Self {
        Error::Execution(ExecutionError::ReorgSend(error))
    }
}

impl From<SendError<Arc<ResumeWatermark>>> for Error {
    fn from(error: SendError<Arc<ResumeWatermark>>) -> Self {
        Error::Execution(ExecutionError::WatermarkSend(error))
    }
}

impl From<toml::de::Error> for Error {
    fn from(error: toml::de::Error) -> Self {
        Error::Toml(error)
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        use Error::*;
        match self {
            Arrow(e) => e.fmt(f),
            Client(e) => e.fmt(f),
            Config(e) => e.fmt(f),
            Execution(e) => e.fmt(f),
            Driver(e) => e.fmt(f),
            Query(e) => e.fmt(f),
            Toml(e) => e.fmt(f),
        }
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        use Error::*;
        match self {
            Arrow(e) => e.source(),
            Client(e) => e.source(),
            Config(e) => e.source(),
            Execution(e) => e.source(),
            Driver(e) => e.source(),
            Query(e) => e.source(),
            Toml(e) => e.source(),
        }
    }
}

impl Error {
    pub fn incremental(e: BoxError) -> Self {
        Error::Query(QueryError::Incremental(e))
    }

    pub fn planning(e: DataFusionError) -> Self {
        Error::Query(QueryError::Planning(e))
    }

    pub fn query_env(e: DataFusionError) -> Self {
        Error::Query(QueryError::Environment(e))
    }

    pub fn stream_start(e: AmpClientError) -> Self {
        Error::Execution(ExecutionError::StreamStart(e))
    }

    pub fn stream_recv(e: BroadcastStreamRecvError) -> Self {
        Error::Execution(ExecutionError::BroadcastStreamRecv(e))
    }

    pub fn rollback<'a>(
        connection: &'a mut Connection,
        message: impl Display + 'a,
    ) -> impl FnMut(AdbcError) -> Self + 'a {
        tracing::error!("{}: rolling back current transaction", message);
        move |e: AdbcError| {
            let _ = connection.rollback();
            Error::Execution(ExecutionError::TransactionRollback(e))
        }
    }

    pub fn stream_read(e: ArrowError) -> Self {
        Error::Execution(ExecutionError::StreamRead(e))
    }

    pub fn io_error(path: PathBuf) -> impl FnMut(std::io::Error) -> Self {
        move |e: std::io::Error| Error::Config(ConfigError::Io(path.clone(), e))
    }
}

#[derive(Debug)]
pub enum DriverError {
    BigQuery(bigquery::Error),
    Postgres(postgres::Error),
    Snowflake(snowflake::Error),
    Sqlite(sqlite::Error),
    MissingConfig { expected: usize, found: usize },
    Unknown(Box<dyn StdError + Send + Sync>),
}

impl DriverError {
    pub fn missing_config(expected: usize, found: usize) -> Self {
        DriverError::MissingConfig { expected, found }
    }
}

impl Display for DriverError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            DriverError::BigQuery(e) => e.fmt(f),
            DriverError::Postgres(e) => e.fmt(f),
            DriverError::Snowflake(e) => e.fmt(f),
            DriverError::Sqlite(e) => e.fmt(f),
            DriverError::MissingConfig { expected, found } => {
                write!(f, "Missing drivers: expected {}, found {}", expected, found)
            }
            DriverError::Unknown(e) => e.fmt(f),
        }
    }
}

impl StdError for DriverError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        use DriverError::*;

        match self {
            BigQuery(e) => e.source(),
            Postgres(e) => e.source(),
            Snowflake(e) => e.source(),
            Sqlite(e) => e.source(),
            MissingConfig { .. } => None,
            Unknown(e) => Some(&**e),
        }
    }
}

impl From<AdbcError> for DriverError {
    fn from(err: AdbcError) -> Self {
        DriverError::Unknown(Box::new(err))
    }
}

#[derive(Debug)]
pub enum ExecutionError {
    BroadcastStreamRecv(BroadcastStreamRecvError),
    DataSend(SendError<(RecordBatch, Metadata)>),
    ReorgSend(SendError<Arc<[InvalidationRange]>>),
    StreamStart(AmpClientError),
    StreamRead(ArrowError),
    TransactionRollback(AdbcError),
    UnsafeBroadcastStart,
    WatermarkSend(SendError<Arc<ResumeWatermark>>),
}

impl Display for ExecutionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        use ExecutionError::*;
        match self {
            BroadcastStreamRecv(e) => write!(f, "Broadcast stream receive error: {}", e),
            DataSend(e) => write!(f, "Failed to send data batch: {:?}", e.0.1),
            ReorgSend(e) => write!(f, "Failed to send reorganization notification: {:?}", e.0),
            StreamStart(e) => write!(f, "Failed to start stream: {}", e),
            StreamRead(e) => write!(f, "Error reading from stream: {}", e),
            TransactionRollback(e) => {
                write!(
                    f,
                    "Transaction was rolled back due to an error during execution: {}",
                    e
                )
            }
            UnsafeBroadcastStart => {
                write!(
                    f,
                    "Attempted to start a broadcast stream while another was active. \
                    Aborting both streams to prevent data corruption."
                )
            }
            WatermarkSend(e) => write!(f, "Failed to send watermark: {}", e),
        }
    }
}

impl StdError for ExecutionError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            ExecutionError::BroadcastStreamRecv(e) => Some(e),
            ExecutionError::DataSend(e) => Some(e),
            ExecutionError::ReorgSend(e) => Some(e),
            ExecutionError::StreamStart(e) => Some(e),
            ExecutionError::StreamRead(e) => Some(e),
            ExecutionError::TransactionRollback(e) => e.source(),
            ExecutionError::UnsafeBroadcastStart => None,
            ExecutionError::WatermarkSend(e) => Some(e),
        }
    }
}

impl From<ExecutionError> for Error {
    fn from(error: ExecutionError) -> Self {
        Error::Execution(error)
    }
}
