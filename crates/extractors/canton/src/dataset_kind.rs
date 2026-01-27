//! Canton dataset kind type and parsing utilities.
//!
//! This module defines the type-safe representation of the Canton dataset kind
//! and provides parsing functionality with proper error handling.

use datasets_common::raw_dataset_kind::RawDatasetKind;

/// The canonical string identifier for Canton datasets.
///
/// This constant defines the string representation used in dataset manifests
/// and configuration files to identify datasets that extract ledger data
/// from Canton Network via canton-bridge Arrow Flight endpoints.
const DATASET_KIND: &str = "canton";

/// Type-safe representation of the Canton dataset kind.
///
/// This zero-sized type represents the "canton" dataset kind, which extracts
/// ledger data from Canton Network via canton-bridge Arrow Flight endpoints.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(inline))]
#[cfg_attr(
    feature = "schemars",
    schemars(schema_with = "canton_dataset_kind_schema")
)]
pub struct CantonDatasetKind;

impl CantonDatasetKind {
    /// Returns the canonical string identifier for this dataset kind.
    #[inline]
    pub const fn as_str(self) -> &'static str {
        DATASET_KIND
    }
}

impl From<CantonDatasetKind> for RawDatasetKind {
    fn from(value: CantonDatasetKind) -> Self {
        RawDatasetKind::new(value.to_string())
    }
}

#[cfg(feature = "schemars")]
fn canton_dataset_kind_schema(_gen: &mut schemars::SchemaGenerator) -> schemars::Schema {
    let schema_obj = serde_json::json!({
        "const": DATASET_KIND
    });
    serde_json::from_value(schema_obj).unwrap()
}

impl std::str::FromStr for CantonDatasetKind {
    type Err = CantonDatasetKindError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s != DATASET_KIND {
            return Err(CantonDatasetKindError(s.to_string()));
        }

        Ok(CantonDatasetKind)
    }
}

impl std::fmt::Display for CantonDatasetKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        DATASET_KIND.fmt(f)
    }
}

impl serde::Serialize for CantonDatasetKind {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(DATASET_KIND)
    }
}

impl<'de> serde::Deserialize<'de> for CantonDatasetKind {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

impl PartialEq<str> for CantonDatasetKind {
    fn eq(&self, other: &str) -> bool {
        DATASET_KIND == other
    }
}

impl PartialEq<CantonDatasetKind> for str {
    fn eq(&self, _other: &CantonDatasetKind) -> bool {
        self == DATASET_KIND
    }
}

impl PartialEq<&str> for CantonDatasetKind {
    fn eq(&self, other: &&str) -> bool {
        DATASET_KIND == *other
    }
}

impl PartialEq<CantonDatasetKind> for &str {
    fn eq(&self, _other: &CantonDatasetKind) -> bool {
        *self == DATASET_KIND
    }
}

impl PartialEq<String> for CantonDatasetKind {
    fn eq(&self, other: &String) -> bool {
        DATASET_KIND == other.as_str()
    }
}

impl PartialEq<CantonDatasetKind> for String {
    fn eq(&self, _other: &CantonDatasetKind) -> bool {
        self.as_str() == DATASET_KIND
    }
}

impl PartialEq<RawDatasetKind> for CantonDatasetKind {
    fn eq(&self, other: &RawDatasetKind) -> bool {
        DATASET_KIND == other.as_str()
    }
}

impl PartialEq<CantonDatasetKind> for RawDatasetKind {
    fn eq(&self, _other: &CantonDatasetKind) -> bool {
        self.as_str() == DATASET_KIND
    }
}

impl PartialEq<CantonDatasetKind> for &RawDatasetKind {
    fn eq(&self, _other: &CantonDatasetKind) -> bool {
        self.as_str() == DATASET_KIND
    }
}

/// Error returned when parsing an invalid Canton dataset kind string.
///
/// This error is returned when attempting to parse a string that does not
/// match the expected "canton" dataset kind identifier.
#[derive(Debug, thiserror::Error)]
#[error("invalid dataset kind: {}, expected: {}", .0, DATASET_KIND)]
pub struct CantonDatasetKindError(String);
