//! EVM-RPC provider kind type and parsing utilities.
//!
//! This module defines the type-safe representation of the EVM-RPC provider kind
//! and provides parsing functionality with proper error handling.

use amp_providers_common::kind::ProviderKindStr;

/// The canonical string identifier for EVM-RPC providers.
///
/// This constant defines the string representation used in provider configurations
/// to identify providers that interact with Ethereum-compatible JSON-RPC endpoints.
const PROVIDER_KIND: &str = "evm-rpc";

/// Type-safe representation of the EVM-RPC provider kind.
///
/// This zero-sized type represents the "evm-rpc" provider kind, which interacts
/// with Ethereum-compatible JSON-RPC endpoints for blockchain data extraction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(inline))]
#[cfg_attr(
    feature = "schemars",
    schemars(schema_with = "evm_rpc_provider_kind_schema")
)]
pub struct EvmRpcProviderKind;

impl EvmRpcProviderKind {
    /// Returns the canonical string identifier for this provider kind.
    #[inline]
    pub const fn as_str(self) -> &'static str {
        PROVIDER_KIND
    }
}

impl AsRef<str> for EvmRpcProviderKind {
    fn as_ref(&self) -> &str {
        PROVIDER_KIND
    }
}

impl From<EvmRpcProviderKind> for ProviderKindStr {
    fn from(value: EvmRpcProviderKind) -> Self {
        // SAFETY: The constant PROVIDER_KIND is "evm-rpc", which is non-empty
        ProviderKindStr::new_unchecked(value.to_string())
    }
}

#[cfg(feature = "schemars")]
fn evm_rpc_provider_kind_schema(_gen: &mut schemars::SchemaGenerator) -> schemars::Schema {
    let schema_obj = serde_json::json!({
        "const": PROVIDER_KIND
    });
    serde_json::from_value(schema_obj).unwrap()
}

impl std::str::FromStr for EvmRpcProviderKind {
    type Err = EvmRpcProviderKindError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s != PROVIDER_KIND {
            return Err(EvmRpcProviderKindError(s.to_string()));
        }

        Ok(EvmRpcProviderKind)
    }
}

impl std::fmt::Display for EvmRpcProviderKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        PROVIDER_KIND.fmt(f)
    }
}

impl serde::Serialize for EvmRpcProviderKind {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(PROVIDER_KIND)
    }
}

impl<'de> serde::Deserialize<'de> for EvmRpcProviderKind {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

impl PartialEq<str> for EvmRpcProviderKind {
    fn eq(&self, other: &str) -> bool {
        PROVIDER_KIND == other
    }
}

impl PartialEq<EvmRpcProviderKind> for str {
    fn eq(&self, _other: &EvmRpcProviderKind) -> bool {
        self == PROVIDER_KIND
    }
}

impl PartialEq<&str> for EvmRpcProviderKind {
    fn eq(&self, other: &&str) -> bool {
        PROVIDER_KIND == *other
    }
}

impl PartialEq<EvmRpcProviderKind> for &str {
    fn eq(&self, _other: &EvmRpcProviderKind) -> bool {
        *self == PROVIDER_KIND
    }
}

impl PartialEq<String> for EvmRpcProviderKind {
    fn eq(&self, other: &String) -> bool {
        PROVIDER_KIND == other.as_str()
    }
}

impl PartialEq<EvmRpcProviderKind> for String {
    fn eq(&self, _other: &EvmRpcProviderKind) -> bool {
        self.as_str() == PROVIDER_KIND
    }
}

impl PartialEq<ProviderKindStr> for EvmRpcProviderKind {
    fn eq(&self, other: &ProviderKindStr) -> bool {
        PROVIDER_KIND == other.as_str()
    }
}

/// Error returned when parsing an invalid EVM-RPC provider kind string.
///
/// This error is returned when attempting to parse a string that does not
/// match the expected "evm-rpc" provider kind identifier.
#[derive(Debug, thiserror::Error)]
#[error("invalid provider kind: {}, expected: {}", .0, PROVIDER_KIND)]
pub struct EvmRpcProviderKindError(String);
