//! Manifest fetching.
//!
//! The manifest contains table definitions with Arrow schemas that are used
//! to create PostgreSQL tables.

use std::{collections::BTreeMap, time::Duration};

use ampctl::client;
use backon::{ExponentialBuilder, Retryable};
use dataset_store::DatasetKind;
use datasets_common::{manifest::TableSchema, reference::Reference};
use monitoring::logging;

use crate::{config::SyncConfig, sql};

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

    /// Fully resolved dataset reference
    pub reference: Reference,
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
            .map(|(name, table)| (name.to_string(), table.schema))
            .collect()
    }
}

/// Envelope for two-phase manifest deserialization.
///
/// Deserializes only the `kind` field upfront to determine which manifest
/// type to use for full deserialization.
#[derive(serde::Deserialize)]
struct ManifestEnvelope {
    kind: String,
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

/// Parse manifest JSON text into a Manifest with table schemas.
///
/// This function performs two-phase deserialization:
/// 1. Extract the `kind` field to determine the manifest type
/// 2. Deserialize the full JSON into the appropriate manifest type
///
/// # Arguments
///
/// - `text`: JSON text of the manifest
/// - `reference`: Fully resolved dataset reference
///
/// # Returns
///
/// The parsed manifest with kind and table schemas.
///
/// # Errors
///
/// Returns error if:
/// - JSON is malformed
/// - `kind` field is missing or unsupported
/// - Manifest deserialization fails
/// - Manifest contains no tables
fn parse_manifest_from_text(
    text: &str,
    reference: Reference,
) -> Result<Manifest, FetchManifestError> {
    // Extract the kind field first to determine manifest type
    let envelope: ManifestEnvelope =
        serde_json::from_str(text).map_err(|e| FetchManifestError::ParseJsonSerde {
            message: "Failed to parse manifest envelope".to_string(),
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
    let tables = match kind {
        DatasetKind::EvmRpc => parse_manifest::<evm_rpc_datasets::Manifest>(text, "EVM-RPC")?,
        DatasetKind::EthBeacon => {
            parse_manifest::<eth_beacon_datasets::Manifest>(text, "Eth-Beacon")?
        }
        DatasetKind::Firehose => {
            parse_manifest::<firehose_datasets::dataset::Manifest>(text, "Firehose")?
        }
        DatasetKind::Derived => {
            parse_manifest::<datasets_derived::manifest::Manifest>(text, "Derived")?
        }
    };

    let manifest = Manifest {
        kind,
        tables,
        reference,
    };

    // Validate manifest has tables
    if manifest.tables.is_empty() {
        return Err(FetchManifestError::NoTables);
    }

    Ok(manifest)
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
    /// Failed to build ampctl client
    #[error("Failed to build ampctl client")]
    BuildClient(#[source] client::BuildError),

    /// Failed to fetch manifest from Admin API
    #[error("Failed to fetch manifest from Admin API")]
    GetManifest(#[source] client::datasets::GetManifestError),

    /// Manifest not found (404)
    #[error("Manifest not found for dataset")]
    ManifestNotFound,

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

impl FetchManifestError {
    /// Classify error as retryable or fatal.
    ///
    /// Retryable errors will retry indefinitely with exponential backoff (capped).
    /// Fatal errors fail immediately without retries.
    fn is_retryable(&self) -> bool {
        match self {
            // Network and API errors are retryable
            Self::GetManifest(_) => true,

            // Client build errors are not retryable
            Self::BuildClient(_) => false,

            // Schema/validation errors are not retryable
            Self::ManifestNotFound => false,
            Self::ParseJsonSerde { .. } => false,
            Self::InvalidArrowType { .. } => false,
            Self::InvalidTableName { .. } => false,
            Self::NoTables => false,
        }
    }
}

/// Fetch dataset manifest from Admin API with retry logic.
///
/// This function fetches the table schemas for a dataset from the Admin API,
/// with automatic retry on transient failures.
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
/// - Response JSON cannot be parsed (malformed manifest)
/// - Arrow schema conversion fails (unsupported types)
/// - Manifest validation fails (no tables, invalid names)
pub async fn fetch_manifest(config: &SyncConfig) -> Result<Manifest, FetchManifestError> {
    // Convert PartialReference to full Reference by filling in defaults
    let dataset = config.dataset.to_full_reference();

    tracing::info!(
        dataset = %config.dataset,
        admin_api = %config.amp_admin_api_addr,
        "fetching_manifest"
    );

    // Parse Admin API URL
    let base_url = url::Url::parse(&config.amp_admin_api_addr)
        .expect("amp_admin_api_addr should be a valid URL");

    // Build ampctl client with optional authentication
    let ampctl_client = if let Some(token) = &config.auth_token {
        client::build(base_url)
            .with_bearer_token(token.clone())
            .build()
            .map_err(FetchManifestError::BuildClient)?
    } else {
        client::Client::new(base_url)
    };

    // Create retry-able fetch function
    let fetch = || async {
        // Fetch manifest using ampctl client
        let manifest_json = ampctl_client
            .datasets()
            .get_manifest(&dataset)
            .await
            .map_err(FetchManifestError::GetManifest)?;

        // Handle 404 case
        let manifest_json = manifest_json.ok_or(FetchManifestError::ManifestNotFound)?;

        // Convert to JSON string for parsing
        let text = serde_json::to_string(&manifest_json).map_err(|e| {
            FetchManifestError::ParseJsonSerde {
                message: "Failed to serialize manifest".to_string(),
                source: e,
            }
        })?;

        // Parse the manifest from the text
        parse_manifest_from_text(&text, dataset.clone())
    };

    // Retry with exponential backoff, jitter, and error classification
    let manifest = fetch
        .retry(
            ExponentialBuilder::default()
                .with_jitter() // Prevent thundering herd
                .with_max_delay(Duration::from_secs(config.manifest_fetch_max_backoff_secs)),
        )
        .when(FetchManifestError::is_retryable) // Only retry retryable errors
        .notify(|err, duration| {
            tracing::warn!(
                error = %err, error_source = logging::error_source(&err),
                retry_after_ms = duration.as_millis(),
                retryable = err.is_retryable(),
                "manifest_fetch_failed_retrying"
            );
        })
        .await?;

    tracing::info!(
        dataset = %config.dataset,
        tables = manifest.tables.len(),
        "manifest_fetched_successfully"
    );

    // Validate all table names before returning
    for table_name in manifest.tables.keys() {
        validate_table_name(table_name)?;
    }

    Ok(manifest)
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use datasets_common::{name::Name, namespace::Namespace, revision::Revision};

    use super::*;

    const DERIVED_MANIFEST: &str =
        include_str!("../../../../tests/config/manifests/register_test_dataset__1_0_0.json");
    const EVM_RPC_MANIFEST: &str = include_str!("../../../../tests/config/manifests/eth_rpc.json");
    const ETH_BEACON_MANIFEST: &str =
        include_str!("../../../../tests/config/manifests/eth_beacon.json");
    const FIREHOSE_MANIFEST: &str =
        include_str!("../../../../tests/config/manifests/eth_firehose.json");

    fn test_reference() -> Reference {
        Reference::new(
            Namespace::from_str("_").unwrap(),
            Name::try_from("test".to_string()).unwrap(),
            Revision::Latest,
        )
    }

    #[test]
    fn test_parse_derived_manifest() {
        let manifest = parse_manifest_from_text(DERIVED_MANIFEST, test_reference())
            .expect("Failed to parse manifest");

        assert_eq!(manifest.kind, DatasetKind::Derived);
        assert_eq!(manifest.tables.len(), 1);
        assert!(manifest.tables.contains_key("erc20_transfers"));

        let table = &manifest.tables["erc20_transfers"];
        assert_eq!(table.arrow.fields.len(), 5);
        assert_eq!(table.arrow.fields[0].name, "_block_num");
        assert_eq!(table.arrow.fields[1].name, "block_num");
        assert_eq!(table.arrow.fields[2].name, "miner");
    }

    #[test]
    fn test_parse_evm_rpc_manifest() {
        let manifest = parse_manifest_from_text(EVM_RPC_MANIFEST, test_reference())
            .expect("Failed to parse manifest");

        assert_eq!(manifest.kind, DatasetKind::EvmRpc);
        assert!(manifest.tables.contains_key("blocks"));
        assert!(manifest.tables.contains_key("transactions"));
        assert!(manifest.tables.contains_key("logs"));
    }

    #[test]
    fn test_parse_eth_beacon_manifest() {
        let manifest = parse_manifest_from_text(ETH_BEACON_MANIFEST, test_reference())
            .expect("Failed to parse manifest");

        assert_eq!(manifest.kind, DatasetKind::EthBeacon);
        assert!(manifest.tables.contains_key("blocks"));
    }

    #[test]
    fn test_parse_firehose_manifest() {
        let manifest = parse_manifest_from_text(FIREHOSE_MANIFEST, test_reference())
            .expect("Failed to parse manifest");

        assert_eq!(manifest.kind, DatasetKind::Firehose);
        assert!(manifest.tables.contains_key("blocks"));
        assert!(manifest.tables.contains_key("transactions"));
        assert!(manifest.tables.contains_key("logs"));
    }

    #[test]
    fn test_parse_manifest_unsupported_kind() {
        let json = r#"{
            "kind": "unknown-kind",
            "tables": {}
        }"#;

        let result = parse_manifest_from_text(json, test_reference());
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            FetchManifestError::InvalidArrowType { .. }
        ));
    }

    #[test]
    fn test_parse_manifest_no_tables() {
        let json = r#"{
            "kind": "manifest",
            "dependencies": {},
            "functions": {},
            "tables": {}
        }"#;

        let result = parse_manifest_from_text(json, test_reference());
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), FetchManifestError::NoTables));
    }

    #[test]
    fn test_parse_manifest_malformed_json() {
        let json = r#"{
            "kind": "manifest",
            "tables": {
        "#;

        let result = parse_manifest_from_text(json, test_reference());
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            FetchManifestError::ParseJsonSerde { .. }
        ));
    }

    #[test]
    fn test_parse_manifest_missing_kind() {
        let json = r#"{
            "tables": {
                "test": {
                    "schema": {
                        "arrow": {
                            "fields": []
                        }
                    }
                }
            }
        }"#;

        let result = parse_manifest_from_text(json, test_reference());
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            FetchManifestError::ParseJsonSerde { .. }
        ));
    }
}
