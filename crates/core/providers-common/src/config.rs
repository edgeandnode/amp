use datasets_common::network_id::NetworkId;

use crate::kind::ProviderKindStr;

/// Raw provider configuration as a TOML table.
///
/// This newtype wraps a raw TOML table containing provider configuration fields.
/// It supports conversion to strongly-typed configuration structs via `try_into_config()`.
///
/// ## Design
///
/// - Uses `#[serde(transparent)]` for seamless TOML serialization/deserialization
/// - Environment variable substitution via `with_env_substitution()` method
/// - Conversion to typed configs via `try_into_config()` method
/// - Partial views via `ConfigHeader` and `ConfigHeaderWithNetwork` for typed field access
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
#[serde(transparent)]
pub struct ProviderConfigRaw(toml::Table);

impl ProviderConfigRaw {
    /// Create a new raw provider configuration from a TOML table.
    pub fn new(table: toml::Table) -> Self {
        Self(table)
    }

    /// Consume this config and return the underlying TOML table.
    pub fn into_inner(self) -> toml::Table {
        self.0
    }

    /// Apply environment variable substitution to all values in this configuration.
    ///
    /// Returns a new `ProviderResolvedConfigRaw` with all `${VAR}` patterns replaced with
    /// their corresponding environment variable values. The substitution is
    /// applied recursively to all string values in the TOML table.
    ///
    /// The returned type has a redacted `Debug` implementation to prevent accidentally
    /// logging resolved secrets (API keys, tokens, etc.) that may be present after substitution.
    ///
    /// Returns an error if any referenced environment variable is not set or
    /// if the substitution syntax is invalid.
    pub fn with_env_substitution(&self) -> Result<ProviderResolvedConfigRaw, crate::envsub::Error> {
        let mut table = self.0.clone();
        for (_key, value) in table.iter_mut() {
            crate::envsub::substitute_env_vars(value)?;
        }
        Ok(ProviderResolvedConfigRaw(table))
    }
}

impl TryIntoConfig for ProviderConfigRaw {
    fn try_into_config<T>(&self) -> Result<T, InvalidConfigError>
    where
        T: for<'de> serde::Deserialize<'de>,
    {
        toml::Value::Table(self.0.clone())
            .try_into()
            .map_err(InvalidConfigError)
    }
}

/// Provider configuration after environment variable substitution.
///
/// This type is returned by `ProviderConfigRaw::with_env_substitution()`.
/// After substitution, the configuration may contain resolved secrets (API keys, tokens, etc.).
///
/// ## Security
///
/// This type has a redacted `Debug` implementation to prevent secrets from leaking into logs.
/// The underlying TOML table is not exposed directly - use `try_into_config()` to deserialize
/// into strongly-typed configuration structs.
///
/// - No `Clone` - resolved configs should not be duplicated unnecessarily
/// - No `Serialize` - resolved configs should not be persisted
/// - No public field access - prevents accidental exposure of secrets
pub struct ProviderResolvedConfigRaw(toml::Table);

impl std::fmt::Debug for ProviderResolvedConfigRaw {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("ProviderResolvedConfigRaw")
            .field(&"<redacted>")
            .finish()
    }
}

impl TryIntoConfig for ProviderResolvedConfigRaw {
    fn try_into_config<T>(&self) -> Result<T, InvalidConfigError>
    where
        T: for<'de> serde::Deserialize<'de>,
    {
        toml::Value::Table(self.0.clone())
            .try_into()
            .map_err(InvalidConfigError)
    }
}

/// Trait for types that can be converted into strongly-typed provider configurations.
///
/// This trait provides a uniform interface for converting raw TOML-based configurations
/// into provider-specific typed configuration structs.
pub trait TryIntoConfig {
    /// Convert this raw configuration into a strongly-typed configuration struct.
    ///
    /// Deserializes the configuration into type `T`. This allows converting
    /// raw configurations into provider-specific config types like `EvmRpcProviderConfig`.
    ///
    /// The conversion can fail if required fields are missing, field types don't match,
    /// or values are invalid for the target type.
    fn try_into_config<T>(&self) -> Result<T, InvalidConfigError>
    where
        T: for<'de> serde::Deserialize<'de>;
}

/// Error that can occur when parsing provider configuration into specific config types.
///
/// This occurs when deserializing a raw provider configuration into a
/// provider-specific config struct (e.g., `EvmRpcProviderConfig`). Common causes
/// include missing required fields, type mismatches in provider-specific config fields,
/// or invalid TOML syntax in the configuration.
#[derive(Debug, thiserror::Error)]
#[error("Failed to parse provider configuration")]
pub struct InvalidConfigError(#[source] pub toml::de::Error);

/// Partial view of provider configuration containing only the `kind` field.
///
/// This lightweight type can be obtained by calling `raw.try_into_config::<ConfigHeader>()`
/// on a `ProviderConfigRaw`. Serde ignores unknown fields, so only `kind` is extracted.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct ConfigHeader {
    pub kind: ProviderKindStr,
}

/// Partial view of provider configuration containing `kind` and `network` fields.
///
/// This lightweight type can be obtained by calling `raw.try_into_config::<ConfigHeaderWithNetwork>()`
/// on a `ProviderConfigRaw`. Serde ignores unknown fields, so only `kind` and `network` are extracted.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct ConfigHeaderWithNetwork {
    pub kind: ProviderKindStr,
    pub network: NetworkId,
}
