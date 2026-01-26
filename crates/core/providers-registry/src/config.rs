use datasets_common::raw_dataset_kind::RawDatasetKind;

/// Provider configuration with required and provider-specific fields.
///
/// This struct captures the required fields (`kind` and `network`) that must be present
/// in all provider configurations, while using serde's `flatten` attribute to collect
/// all additional provider-specific configuration fields in the `rest` field.
#[derive(Clone, serde::Deserialize, serde::Serialize)]
pub struct ProviderConfig {
    /// Unique name of the provider configuration
    #[serde(default)]
    pub name: String,
    /// The type of provider as string (e.g., "evm-rpc", "firehose")
    pub kind: RawDatasetKind,
    /// The blockchain network (e.g., "mainnet", "goerli", "polygon")
    pub network: String,
    /// All other provider-specific configuration fields
    #[serde(flatten)]
    pub rest: toml::Table,
}

impl std::fmt::Debug for ProviderConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProviderConfig")
            .field("name", &self.name)
            .field("kind", &self.kind)
            .field("network", &self.network)
            .field("rest", &"<redacted>")
            .finish()
    }
}

impl ProviderConfig {
    /// Convert this provider configuration into a specific configuration type.
    ///
    /// Deserializes all fields (`kind`, `network`, and provider-specific fields from `rest`)
    /// into a strongly-typed configuration struct. The conversion can fail if required fields
    /// are missing, field types don't match, or values are invalid for the target type.
    pub fn try_into_config<T>(&self) -> Result<T, ParseConfigError>
    where
        T: for<'de> serde::Deserialize<'de>,
    {
        let value = toml::Value::try_from(self).unwrap_or_else(|err| {
            unreachable!(
                "Failed to convert ProviderConfig to toml::Value: {err}. This should never happen."
            )
        });
        value.try_into().map_err(ParseConfigError)
    }
}

/// Error that can occur when parsing provider configuration into specific config types.
#[derive(Debug, thiserror::Error)]
#[error("Failed to parse provider configuration")]
pub struct ParseConfigError(#[source] toml::de::Error);

/// Internal EVM RPC provider configuration for parsing TOML config.
///
/// This struct contains only the fields needed for provider construction,
/// and is used internally by the providers registry. External consumers
/// should use the constructed `RootProvider` returned by `find_evm_rpc_provider`.
#[derive(Debug, Clone, serde::Deserialize)]
pub(crate) struct EvmRpcProviderConfig {
    /// The URL of the EVM RPC endpoint (HTTP, HTTPS, or IPC).
    pub url: url::Url,
    /// Optional rate limit for requests per minute.
    pub rate_limit_per_minute: Option<std::num::NonZeroU32>,
}
