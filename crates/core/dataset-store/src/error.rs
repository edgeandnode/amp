use common::{BoxError, query_context, store::StoreError};
use datasets_common::{name::Name, version::Version};

use crate::{DatasetKind, UnsupportedKindError, providers};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed to fetch: {0}")]
    FetchError(#[from] StoreError),

    #[error("TOML parse error: {0}")]
    Toml(#[from] toml::de::Error),

    #[error("JSON parse error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Metadata db error: {0}")]
    MetadataDbError(#[from] metadata_db::Error),

    #[error("unsupported dataset kind '{0}'")]
    UnsupportedKind(String),

    #[error("dataset field 'name = \"{0}\"' does not match filename '{1}'")]
    NameMismatch(String, String),

    #[error("Schema mismatch")]
    SchemaMismatch,

    #[error("`schema` field is missing, but required for dataset kind {dataset_kind}")]
    SchemaMissing { dataset_kind: DatasetKind },

    #[error("unsupported table name: {0}")]
    UnsupportedName(BoxError),

    #[error("unsupported function name: {0}")]
    UnsupportedFunctionName(String),

    #[error("provider configuration error: {0}")]
    ProviderConfigError(BoxError),

    #[error("IPC connection error: {0}")]
    IpcConnectionError(BoxError),

    #[error("EVM RPC error: {0}")]
    EvmRpcError(#[from] evm_rpc_datasets::Error),

    #[error("firehose error: {0}")]
    FirehoseError(#[from] firehose_datasets::Error),

    #[error("error loading sql dataset: {0}")]
    SqlDatasetError(BoxError),

    #[error("error deserializing manifest: {0}")]
    ManifestError(serde_json::Error),

    #[error("error parsing SQL: {0}")]
    SqlParseError(query_context::Error),

    #[error("provider not found for dataset kind '{dataset_kind}' and network '{network}'")]
    ProviderNotFound {
        dataset_kind: DatasetKind,
        network: String,
    },

    #[error("provider configuration file is not valid UTF-8 at {location}: {source}")]
    ProviderInvalidUtf8 {
        location: String,
        #[source]
        source: std::string::FromUtf8Error,
    },

    #[error("dataset '{0}' version '{1}' not found")]
    DatasetVersionNotFound(String, String),

    #[error("{0}")]
    Unknown(BoxError),
}

impl From<providers::FetchError> for Error {
    fn from(err: providers::FetchError) -> Self {
        match err {
            providers::FetchError::StoreFetchFailed(box_error) => {
                // Try to downcast BoxError to StoreError, otherwise map to Unknown
                match box_error.downcast::<StoreError>() {
                    Ok(store_error) => Error::FetchError(*store_error),
                    Err(box_error) => Error::Unknown(box_error),
                }
            }
            providers::FetchError::TomlParseError(toml_error) => Error::Toml(toml_error),
            providers::FetchError::InvalidUtf8 { location, source } => {
                Error::ProviderInvalidUtf8 { location, source }
            }
        }
    }
}

impl From<UnsupportedKindError> for Error {
    fn from(err: UnsupportedKindError) -> Self {
        Error::UnsupportedKind(err.kind)
    }
}

#[derive(Debug, thiserror::Error)]
pub struct DatasetError {
    pub dataset: Option<String>,

    #[source]
    error: Error,
}

impl DatasetError {
    pub(crate) fn no_context(error: impl Into<Error>) -> Self {
        Self {
            dataset: None,
            error: error.into(),
        }
    }

    pub(crate) fn unknown(error: impl Into<BoxError>) -> Self {
        Self {
            dataset: None,
            error: Error::Unknown(error.into()),
        }
    }

    pub fn is_not_found(&self) -> bool {
        matches!(
            &self.error,
            Error::FetchError(err) if err.is_not_found()
        ) || matches!(&self.error, Error::DatasetVersionNotFound(_, _))
    }
}

impl From<(&str, Error)> for DatasetError {
    fn from((dataset, error): (&str, Error)) -> Self {
        Self {
            dataset: Some(dataset.to_string()),
            error,
        }
    }
}

impl From<(String, Error)> for DatasetError {
    fn from((dataset, error): (String, Error)) -> Self {
        Self {
            dataset: Some(dataset),
            error,
        }
    }
}

impl std::fmt::Display for DatasetError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let e = &self.error;
        match &self.dataset {
            Some(dataset) if self.is_not_found() => {
                write!(f, "dataset '{}' not found, full error: {}", dataset, e)
            }
            Some(dataset) => write!(f, "error with dataset '{}': {}", dataset, e),
            None => write!(f, "{}", e),
        }
    }
}

/// Errors specific to manifest registration operations
#[derive(Debug, thiserror::Error)]
pub enum RegistrationError {
    /// Dataset already exists in the registry
    #[error("Dataset '{name}' version '{version}' already registered")]
    DatasetExists { name: Name, version: Version },

    /// Failed to serialize manifest to JSON
    #[error("Failed to serialize manifest to JSON: {0}")]
    ManifestSerialization(serde_json::Error),

    /// Failed to store manifest in dataset definitions store
    #[error("Failed to store manifest in dataset definitions store: {0}")]
    ManifestStorage(object_store::Error),

    /// Failed to register dataset in metadata database
    #[error("Failed to register dataset in metadata database: {0}")]
    MetadataRegistration(metadata_db::Error),

    /// Failed to check if dataset exists in metadata database
    #[error("Failed to check dataset existence in metadata database: {0}")]
    ExistenceCheck(metadata_db::Error),
}
