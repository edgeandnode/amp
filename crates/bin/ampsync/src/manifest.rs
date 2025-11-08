//! Manifest fetching.
//!
//! The manifest contains table definitions with Arrow schemas that are used
//! to create PostgreSQL tables.

use std::collections::BTreeMap;

use backon::{ExponentialBuilder, Retryable};
use dataset_store::DatasetKind;
use datasets_common::manifest::TableSchema;
use reqwest::Client;

use crate::{config::Config, sql};

/// Generic manifest that extracts table schemas from any dataset kind.
///
/// This is a unified view over different dataset manifest types (evm-rpc,
/// firehose, eth-beacon, derived), extracting just the table information
/// needed for syncing to PostgreSQL.
#[derive(Debug, Clone)]
pub struct Manifest {
    /// Dataset kind
    pub kind: DatasetKind,

    /// Tables in this dataset, mapping table name to table schema
    pub tables: BTreeMap<String, TableSchema>,
}

/// Helper trait for extracting table schemas from manifests.
trait HasTables {
    fn into_tables(self) -> BTreeMap<String, TableSchema>;
}

impl HasTables for evm_rpc_datasets::Manifest {
    fn into_tables(self) -> BTreeMap<String, TableSchema> {
        self.tables
            .into_iter()
            .map(|(name, table)| (name, table.schema))
            .collect()
    }
}

impl HasTables for eth_beacon_datasets::Manifest {
    fn into_tables(self) -> BTreeMap<String, TableSchema> {
        self.tables
            .into_iter()
            .map(|(name, table)| (name, table.schema))
            .collect()
    }
}

impl HasTables for firehose_datasets::dataset::Manifest {
    fn into_tables(self) -> BTreeMap<String, TableSchema> {
        self.tables
            .into_iter()
            .map(|(name, table)| (name, table.schema))
            .collect()
    }
}

impl HasTables for datasets_derived::manifest::Manifest {
    fn into_tables(self) -> BTreeMap<String, TableSchema> {
        self.tables
            .into_iter()
            .map(|(name, table)| (name, table.schema))
            .collect()
    }
}

/// Envelope for two-phase manifest deserialization.
///
/// Deserializes only the `kind` field upfront, keeping the rest as validated
/// but unparsed JSON. Once we know the kind, we deserialize the full manifest
/// directly to the appropriate type.
///
/// This avoids the expensive clone() operation and intermediate Value allocation.
#[derive(serde::Deserialize)]
struct ManifestEnvelope {
    kind: String,
    #[serde(flatten)]
    rest: Box<serde_json::value::RawValue>,
}

/// Parse manifest from JSON string and extract table schemas.
fn parse_manifest<T>(
    json_str: &str,
    kind_name: &str,
) -> Result<BTreeMap<String, TableSchema>, FetchManifestError>
where
    T: serde::de::DeserializeOwned + HasTables,
{
    let manifest: T =
        serde_json::from_str(json_str).map_err(|e| FetchManifestError::ParseJsonSerde {
            message: format!("Failed to parse {} manifest: {}", kind_name, e),
            source: e,
        })?;
    Ok(manifest.into_tables())
}

/// Validate table name using the centralized SQL validation module.
///
/// This delegates to `sql::validate_identifier()` which uses sqlparser
/// and character restrictions to ensure the name is a safe PostgreSQL identifier.
///
/// See the `sql` module for detailed validation logic.
fn validate_table_name(name: &str) -> Result<(), FetchManifestError> {
    sql::validate_identifier(name).map_err(|e| FetchManifestError::InvalidTableName {
        name: name.to_string(),
        reason: e.to_string(),
    })
}

/// Errors that occur when fetching or parsing manifests
#[derive(Debug, thiserror::Error)]
pub enum FetchManifestError {
    /// Failed to create HTTP client
    #[error("Failed to create HTTP client")]
    CreateClient(#[source] reqwest::Error),

    /// Failed to fetch manifest from Admin API
    #[error("Failed to fetch manifest from Admin API at '{url}' after retries")]
    FetchFromApi {
        url: String,
        #[source]
        source: reqwest::Error,
    },

    /// Admin API returned non-success status
    #[error("Admin API returned status {status} for '{url}'")]
    ApiStatusError {
        status: reqwest::StatusCode,
        url: String,
    },

    /// Failed to parse manifest JSON response (from reqwest)
    #[error("Failed to parse manifest JSON response: {message}")]
    ParseJsonHttp {
        message: String,
        #[source]
        source: reqwest::Error,
    },

    /// Failed to parse manifest JSON (from serde)
    #[error("Failed to parse manifest JSON: {message}")]
    ParseJsonSerde {
        message: String,
        #[source]
        source: serde_json::Error,
    },

    /// Invalid Arrow type in manifest
    #[error("Invalid Arrow type in manifest: {message}")]
    InvalidArrowType { message: String },

    /// Manifest contains no tables
    #[error("Manifest contains no tables")]
    NoTables,

    /// Invalid table name in manifest
    #[error("Invalid table name '{name}': {reason}")]
    InvalidTableName { name: String, reason: String },
}

/// Fetch dataset manifest from Admin API with retry logic.
///
/// This function fetches the table schemas for a dataset from the Admin API,
/// with automatic retry on transient failures.
///
/// # Retry Behavior
///
/// - **Retryable errors**: Network errors, 5xx server errors (max 5 attempts)
/// - **Non-retryable errors**: 4xx client errors (fail immediately)
/// - **Backoff strategy**: Exponential with jitter
/// - Each retry is logged with timing information
///
/// # Arguments
///
/// - `config`: Configuration containing Admin API address and dataset information
///
/// # Returns
///
/// The manifest containing table names and Arrow schemas.
///
/// # Errors
///
/// Returns error if:
/// - Network request fails after all retries
/// - Admin API returns 4xx status (e.g., 404 Not Found)
/// - Admin API returns 5xx status after all retries
/// - Response JSON cannot be parsed
/// - Arrow schema conversion fails (unsupported types)
pub async fn fetch_manifest(config: &Config) -> Result<Manifest, FetchManifestError> {
    let url = format!(
        "{}/datasets/{}/versions/{}/manifest",
        config.amp_admin_api_addr, config.dataset_name, config.dataset_version
    );

    tracing::info!(
        dataset = %config.dataset_name,
        version = %config.dataset_version,
        url = %url,
        "fetching_manifest"
    );

    // Create HTTP client (reused across retries)
    let client = Client::new();

    // Create retry-able fetch function
    let fetch = || async {
        // Fetch response
        let response =
            client
                .get(&url)
                .send()
                .await
                .map_err(|e| FetchManifestError::FetchFromApi {
                    url: url.clone(),
                    source: e,
                })?;

        // Check for HTTP errors
        let status = response.status();
        if !status.is_success() {
            return Err(FetchManifestError::ApiStatusError {
                status,
                url: url.clone(),
            });
        }

        // Deserialize envelope with kind + raw JSON remainder
        let envelope: ManifestEnvelope =
            response
                .json()
                .await
                .map_err(|e| FetchManifestError::ParseJsonHttp {
                    message: "Failed to parse JSON response".to_string(),
                    source: e,
                })?;

        // Parse the dataset kind from envelope
        let kind: DatasetKind =
            envelope
                .kind
                .parse()
                .map_err(|e: dataset_store::UnsupportedKindError| {
                    FetchManifestError::InvalidArrowType {
                        message: format!("Unsupported dataset kind: {}", e.kind),
                    }
                })?;

        // Deserialize full manifest based on kind
        let json_str = envelope.rest.get();
        let tables = match kind {
            DatasetKind::EvmRpc => {
                parse_manifest::<evm_rpc_datasets::Manifest>(json_str, "EVM-RPC")?
            }
            DatasetKind::EthBeacon => {
                parse_manifest::<eth_beacon_datasets::Manifest>(json_str, "Eth-Beacon")?
            }
            DatasetKind::Firehose => {
                parse_manifest::<firehose_datasets::dataset::Manifest>(json_str, "Firehose")?
            }
            DatasetKind::Derived => {
                parse_manifest::<datasets_derived::manifest::Manifest>(json_str, "Derived")?
            }
        };

        let manifest = Manifest { kind, tables };

        // Validate manifest has tables
        if manifest.tables.is_empty() {
            return Err(FetchManifestError::NoTables);
        }

        Ok(manifest)
    };

    // Retry with exponential backoff
    let manifest = fetch
        .retry(
            ExponentialBuilder::default()
                .with_max_times(config.manifest_fetch_max_retries as usize),
        )
        .notify(|err, duration| {
            tracing::warn!(
                error = %err,
                retry_after_ms = duration.as_millis(),
                "manifest_fetch_failed_retrying"
            );
        })
        .await?;

    tracing::info!(
        dataset = %config.dataset_name,
        tables = manifest.tables.len(),
        "manifest_fetched_successfully"
    );

    // Validate all table names before returning
    for table_name in manifest.tables.keys() {
        validate_table_name(table_name)?;
    }

    Ok(manifest)
}
