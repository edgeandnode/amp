//! Solana dataset kind type and parsing utilities.
//!
//! This module defines the type-safe representation of the Solana dataset kind
//! and provides parsing functionality with proper error handling.

use datasets_common::raw_dataset_kind::RawDatasetKind;

/// The canonical string identifier for Solana datasets.
///
/// This constant defines the string representation used in dataset manifests
/// and configuration files to identify datasets that extract blockchain data
/// from Solana sources (Old Faithful archives).
const DATASET_KIND: &str = "solana";

/// Type-safe representation of the Solana dataset kind.
///
/// This zero-sized type represents the "solana" dataset kind, which extracts
/// blockchain data from Solana sources (Old Faithful archives).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(inline))]
#[cfg_attr(
    feature = "schemars",
    schemars(schema_with = "solana_dataset_kind_schema")
)]
pub struct SolanaDatasetKind;

impl SolanaDatasetKind {
    /// Returns the canonical string identifier for this dataset kind.
    #[inline]
    pub const fn as_str(self) -> &'static str {
        DATASET_KIND
    }
}

impl From<SolanaDatasetKind> for RawDatasetKind {
    fn from(value: SolanaDatasetKind) -> Self {
        RawDatasetKind::new(value.to_string())
    }
}

#[cfg(feature = "schemars")]
fn solana_dataset_kind_schema(_gen: &mut schemars::SchemaGenerator) -> schemars::Schema {
    let schema_obj = serde_json::json!({
        "const": DATASET_KIND
    });
    serde_json::from_value(schema_obj).unwrap()
}

impl std::str::FromStr for SolanaDatasetKind {
    type Err = SolanaDatasetKindError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s != DATASET_KIND {
            return Err(SolanaDatasetKindError(s.to_string()));
        }

        Ok(SolanaDatasetKind)
    }
}

impl std::fmt::Display for SolanaDatasetKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        DATASET_KIND.fmt(f)
    }
}

impl serde::Serialize for SolanaDatasetKind {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(DATASET_KIND)
    }
}

impl<'de> serde::Deserialize<'de> for SolanaDatasetKind {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

impl PartialEq<str> for SolanaDatasetKind {
    fn eq(&self, other: &str) -> bool {
        DATASET_KIND == other
    }
}

impl PartialEq<SolanaDatasetKind> for str {
    fn eq(&self, _other: &SolanaDatasetKind) -> bool {
        self == DATASET_KIND
    }
}

impl PartialEq<&str> for SolanaDatasetKind {
    fn eq(&self, other: &&str) -> bool {
        DATASET_KIND == *other
    }
}

impl PartialEq<SolanaDatasetKind> for &str {
    fn eq(&self, _other: &SolanaDatasetKind) -> bool {
        *self == DATASET_KIND
    }
}

impl PartialEq<String> for SolanaDatasetKind {
    fn eq(&self, other: &String) -> bool {
        DATASET_KIND == other.as_str()
    }
}

impl PartialEq<SolanaDatasetKind> for String {
    fn eq(&self, _other: &SolanaDatasetKind) -> bool {
        self.as_str() == DATASET_KIND
    }
}

impl PartialEq<RawDatasetKind> for SolanaDatasetKind {
    fn eq(&self, other: &RawDatasetKind) -> bool {
        DATASET_KIND == other.as_str()
    }
}

impl PartialEq<SolanaDatasetKind> for RawDatasetKind {
    fn eq(&self, _other: &SolanaDatasetKind) -> bool {
        self.as_str() == DATASET_KIND
    }
}

impl PartialEq<SolanaDatasetKind> for &RawDatasetKind {
    fn eq(&self, _other: &SolanaDatasetKind) -> bool {
        self.as_str() == DATASET_KIND
    }
}

/// Error returned when parsing an invalid Solana dataset kind string.
///
/// This error is returned when attempting to parse a string that does not
/// match the expected [DATASET_KIND] identifier.
#[derive(Debug, thiserror::Error)]
#[error("invalid dataset kind: {}, expected: {}", .0, DATASET_KIND)]
pub struct SolanaDatasetKindError(String);
