//! Firehose dataset kind type and parsing utilities.
//!
//! This module defines the type-safe representation of the Firehose dataset kind
//! and provides parsing functionality with proper error handling.

/// The canonical string identifier for Firehose datasets.
///
/// This constant defines the string representation used in dataset manifests
/// and configuration files to identify datasets that extract blockchain data
/// from Firehose endpoints.
const DATASET_KIND: &str = "firehose";

/// Type-safe representation of the Firehose dataset kind.
///
/// This zero-sized type represents the "firehose" dataset kind, which extracts
/// blockchain data directly from Firehose endpoints.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(inline))]
#[cfg_attr(
    feature = "schemars",
    schemars(schema_with = "firehose_dataset_kind_schema")
)]
pub struct FirehoseDatasetKind;

impl FirehoseDatasetKind {
    /// Returns the canonical string identifier for this dataset kind.
    #[inline]
    pub const fn as_str(self) -> &'static str {
        DATASET_KIND
    }
}

#[cfg(feature = "schemars")]
fn firehose_dataset_kind_schema(_gen: &mut schemars::SchemaGenerator) -> schemars::Schema {
    let schema_obj = serde_json::json!({
        "const": DATASET_KIND
    });
    serde_json::from_value(schema_obj).unwrap()
}

impl std::str::FromStr for FirehoseDatasetKind {
    type Err = FirehoseDatasetKindError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s != DATASET_KIND {
            return Err(FirehoseDatasetKindError(s.to_string()));
        }

        Ok(FirehoseDatasetKind)
    }
}

impl std::fmt::Display for FirehoseDatasetKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        DATASET_KIND.fmt(f)
    }
}

impl serde::Serialize for FirehoseDatasetKind {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(DATASET_KIND)
    }
}

impl<'de> serde::Deserialize<'de> for FirehoseDatasetKind {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

impl PartialEq<str> for FirehoseDatasetKind {
    fn eq(&self, other: &str) -> bool {
        DATASET_KIND == other
    }
}

impl PartialEq<FirehoseDatasetKind> for str {
    fn eq(&self, _other: &FirehoseDatasetKind) -> bool {
        self == DATASET_KIND
    }
}

impl PartialEq<&str> for FirehoseDatasetKind {
    fn eq(&self, other: &&str) -> bool {
        DATASET_KIND == *other
    }
}

impl PartialEq<FirehoseDatasetKind> for &str {
    fn eq(&self, _other: &FirehoseDatasetKind) -> bool {
        *self == DATASET_KIND
    }
}

impl PartialEq<String> for FirehoseDatasetKind {
    fn eq(&self, other: &String) -> bool {
        DATASET_KIND == other.as_str()
    }
}

impl PartialEq<FirehoseDatasetKind> for String {
    fn eq(&self, _other: &FirehoseDatasetKind) -> bool {
        self.as_str() == DATASET_KIND
    }
}

/// Error returned when parsing an invalid Firehose dataset kind string.
///
/// This error is returned when attempting to parse a string that does not
/// match the expected "firehose" dataset kind identifier.
#[derive(Debug, thiserror::Error)]
#[error("invalid dataset kind: {}, expected: {}", .0, DATASET_KIND)]
pub struct FirehoseDatasetKindError(String);
