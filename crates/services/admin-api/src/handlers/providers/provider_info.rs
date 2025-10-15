//! Provider information types for API requests and responses

use super::convert;
use crate::handlers::common::NonEmptyString;

/// Provider information used for both API requests and responses
///
/// This struct represents provider metadata and configuration in a format
/// suitable for both creating providers (POST requests) and retrieving them
/// (GET responses). It includes the complete provider configuration.
///
/// ## Security Note
///
/// The `rest` field contains the full provider configuration. Ensure that
/// sensitive information like API keys and tokens are not stored in the
/// provider configuration if this data will be exposed through APIs.
#[serde_with::serde_as]
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct ProviderInfo {
    /// The name/identifier of the provider
    #[cfg_attr(feature = "utoipa", schema(value_type = String))]
    pub name: NonEmptyString,
    /// The type of provider (e.g., "evm-rpc", "firehose")
    #[serde_as(as = "serde_with::DisplayFromStr")]
    #[cfg_attr(feature = "utoipa", schema(value_type = String))]
    pub kind: dataset_store::DatasetKind,
    /// The blockchain network (e.g., "mainnet", "goerli", "polygon")
    #[cfg_attr(feature = "utoipa", schema(value_type = String))]
    pub network: NonEmptyString,
    /// Additional provider-specific configuration fields
    #[serde(flatten)]
    pub rest: serde_json::Map<String, serde_json::Value>,
}

impl TryFrom<(String, dataset_store::providers::ProviderConfig)> for ProviderInfo {
    type Error = serde_json::Error;

    fn try_from(
        (name, config): (String, dataset_store::providers::ProviderConfig),
    ) -> Result<Self, Self::Error> {
        // SAFETY: Provider names from the dataset store are guaranteed to be non-empty
        // as they are validated during provider registration and storage.
        let name = unsafe { NonEmptyString::new_unchecked(name) };
        // SAFETY: Provider networks from the dataset store are guaranteed to be non-empty
        // as they are validated during provider registration and storage.
        let network = unsafe { NonEmptyString::new_unchecked(config.network) };

        let rest = convert::from_toml_table_to_json_map(config.rest)?;

        Ok(Self {
            name,
            kind: config.kind,
            network,
            rest,
        })
    }
}
