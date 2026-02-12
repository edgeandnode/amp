//! Provider information types for API requests and responses

use amp_providers_common::{
    ProviderName,
    config::{ConfigHeader, ProviderConfigRaw, TryIntoConfig},
    kind::ProviderKindStr,
};

use super::convert;

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
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct ProviderInfo {
    /// The name/identifier of the provider
    #[cfg_attr(feature = "utoipa", schema(value_type = String))]
    pub name: ProviderName,
    /// The type of provider (e.g., "evm-rpc", "firehose")
    #[cfg_attr(feature = "utoipa", schema(value_type = String))]
    pub kind: ProviderKindStr,
    /// Additional provider-specific configuration fields
    #[serde(flatten)]
    pub rest: serde_json::Map<String, serde_json::Value>,
}

impl TryFrom<(ProviderName, ProviderConfigRaw)> for ProviderInfo {
    type Error = ProviderInfoConversionError;

    fn try_from((name, config): (ProviderName, ProviderConfigRaw)) -> Result<Self, Self::Error> {
        // Extract kind from the config
        let ConfigHeader { kind } = config
            .try_into_config::<ConfigHeader>()
            .map_err(ProviderInfoConversionError::HeaderParse)?;

        // Convert the full TOML table to JSON map
        let mut rest = convert::from_toml_table_to_json_map(config.into_inner())
            .map_err(ProviderInfoConversionError::JsonConversion)?;

        // Remove explicit fields from rest to avoid duplication in flattened output
        rest.remove("name");
        rest.remove("kind");

        Ok(Self { name, kind, rest })
    }
}

/// Error that can occur when converting from ProviderConfigRaw to ProviderInfo
#[derive(Debug, thiserror::Error)]
pub enum ProviderInfoConversionError {
    /// Failed to parse the header fields (kind) from the config
    #[error("failed to parse provider config header: {0}")]
    HeaderParse(#[source] amp_providers_common::config::InvalidConfigError),

    /// Failed to convert TOML table to JSON map
    #[error("failed to convert TOML table to JSON map: {0}")]
    JsonConversion(#[source] serde_json::Error),
}
