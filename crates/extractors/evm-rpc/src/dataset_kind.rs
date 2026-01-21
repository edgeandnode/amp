//! EVM-RPC dataset kind type and parsing utilities.
//!
//! This module defines the type-safe representation of the EVM-RPC dataset kind
//! and provides parsing functionality with proper error handling.

use datasets_common::raw_dataset_kind::RawDatasetKind;

/// The canonical string identifier for EVM-RPC datasets.
///
/// This constant defines the string representation used in dataset manifests
/// and configuration files to identify datasets that extract blockchain data
/// from Ethereum-compatible JSON-RPC endpoints.
const DATASET_KIND: &str = "evm-rpc";

/// Type-safe representation of the EVM-RPC dataset kind.
///
/// This zero-sized type represents the "evm-rpc" dataset kind, which extracts
/// blockchain data directly from Ethereum-compatible JSON-RPC endpoints.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(inline))]
#[cfg_attr(
    feature = "schemars",
    schemars(schema_with = "evm_rpc_dataset_kind_schema")
)]
pub struct EvmRpcDatasetKind;

impl EvmRpcDatasetKind {
    /// Returns the canonical string identifier for this dataset kind.
    #[inline]
    pub const fn as_str(self) -> &'static str {
        DATASET_KIND
    }
}

impl From<EvmRpcDatasetKind> for RawDatasetKind {
    fn from(value: EvmRpcDatasetKind) -> Self {
        RawDatasetKind::new(value.to_string())
    }
}

#[cfg(feature = "schemars")]
fn evm_rpc_dataset_kind_schema(_gen: &mut schemars::SchemaGenerator) -> schemars::Schema {
    let schema_obj = serde_json::json!({
        "const": DATASET_KIND
    });
    serde_json::from_value(schema_obj).unwrap()
}

impl std::str::FromStr for EvmRpcDatasetKind {
    type Err = EvmRpcDatasetKindError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s != DATASET_KIND {
            return Err(EvmRpcDatasetKindError(s.to_string()));
        }

        Ok(EvmRpcDatasetKind)
    }
}

impl std::fmt::Display for EvmRpcDatasetKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        DATASET_KIND.fmt(f)
    }
}

impl serde::Serialize for EvmRpcDatasetKind {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(DATASET_KIND)
    }
}

impl<'de> serde::Deserialize<'de> for EvmRpcDatasetKind {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

impl PartialEq<str> for EvmRpcDatasetKind {
    fn eq(&self, other: &str) -> bool {
        DATASET_KIND == other
    }
}

impl PartialEq<EvmRpcDatasetKind> for str {
    fn eq(&self, _other: &EvmRpcDatasetKind) -> bool {
        self == DATASET_KIND
    }
}

impl PartialEq<&str> for EvmRpcDatasetKind {
    fn eq(&self, other: &&str) -> bool {
        DATASET_KIND == *other
    }
}

impl PartialEq<EvmRpcDatasetKind> for &str {
    fn eq(&self, _other: &EvmRpcDatasetKind) -> bool {
        *self == DATASET_KIND
    }
}

impl PartialEq<String> for EvmRpcDatasetKind {
    fn eq(&self, other: &String) -> bool {
        DATASET_KIND == other.as_str()
    }
}

impl PartialEq<EvmRpcDatasetKind> for String {
    fn eq(&self, _other: &EvmRpcDatasetKind) -> bool {
        self.as_str() == DATASET_KIND
    }
}

impl PartialEq<RawDatasetKind> for EvmRpcDatasetKind {
    fn eq(&self, other: &RawDatasetKind) -> bool {
        DATASET_KIND == other.as_str()
    }
}

impl PartialEq<EvmRpcDatasetKind> for RawDatasetKind {
    fn eq(&self, _other: &EvmRpcDatasetKind) -> bool {
        self.as_str() == DATASET_KIND
    }
}

/// Error returned when parsing an invalid EVM-RPC dataset kind string.
///
/// This error is returned when attempting to parse a string that does not
/// match the expected "evm-rpc" dataset kind identifier.
#[derive(Debug, thiserror::Error)]
#[error("invalid dataset kind: {}, expected: {}", .0, DATASET_KIND)]
pub struct EvmRpcDatasetKindError(String);
