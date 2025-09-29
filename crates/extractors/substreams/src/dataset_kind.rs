//! Substreams dataset kind type and parsing utilities.
//!
//! This module defines the type-safe representation of the Substreams dataset kind
//! and provides parsing functionality with proper error handling.

/// The canonical string identifier for Substreams datasets.
///
/// This constant defines the string representation used in dataset manifests
/// and configuration files to identify datasets that extract blockchain data
/// from Substreams endpoints.
pub const DATASET_KIND: &str = "substreams";

/// Type-safe representation of the Substreams dataset kind.
///
/// This zero-sized type represents the "substreams" dataset kind, which extracts
/// blockchain data directly from Substreams endpoints.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(inline))]
#[cfg_attr(
    feature = "schemars",
    schemars(schema_with = "substreams_dataset_kind_schema")
)]
pub struct SubstreamsDatasetKind;

#[cfg(feature = "schemars")]
fn substreams_dataset_kind_schema(_gen: &mut schemars::SchemaGenerator) -> schemars::Schema {
    let schema_obj = serde_json::json!({
        "const": DATASET_KIND
    });
    serde_json::from_value(schema_obj).unwrap()
}

impl std::str::FromStr for SubstreamsDatasetKind {
    type Err = SubstreamsDatasetKindError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s != DATASET_KIND {
            return Err(SubstreamsDatasetKindError(s.to_string()));
        }

        Ok(SubstreamsDatasetKind)
    }
}

impl std::fmt::Display for SubstreamsDatasetKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        DATASET_KIND.fmt(f)
    }
}

impl serde::Serialize for SubstreamsDatasetKind {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(DATASET_KIND)
    }
}

impl<'de> serde::Deserialize<'de> for SubstreamsDatasetKind {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

/// Error returned when parsing an invalid Substreams dataset kind string.
///
/// This error is returned when attempting to parse a string that does not
/// match the expected "substreams" dataset kind identifier.
#[derive(Debug, thiserror::Error)]
#[error("invalid dataset kind: {}, expected: {}", .0, DATASET_KIND)]
pub struct SubstreamsDatasetKindError(String);
