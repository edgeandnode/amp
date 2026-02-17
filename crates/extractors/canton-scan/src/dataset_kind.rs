//! Canton Scan API dataset kind type and parsing utilities.
//!
//! This module defines the type-safe representation of the Canton Scan API dataset kind
//! and provides parsing functionality with proper error handling.

use datasets_common::dataset_kind_str::DatasetKindStr;

/// The canonical string identifier for Canton Scan API datasets.
///
/// This constant defines the string representation used in dataset manifests
/// and configuration files to identify datasets that extract blockchain data
/// from Canton Network Scan API endpoints.
const DATASET_KIND: &str = "canton-scan";

/// Type-safe representation of the Canton Scan API dataset kind.
///
/// This zero-sized type represents the "canton-scan" dataset kind, which extracts
/// blockchain data from Canton Network via the HTTP REST Scan API.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(inline))]
#[cfg_attr(
    feature = "schemars",
    schemars(schema_with = "canton_scan_dataset_kind_schema")
)]
pub struct CantonScanDatasetKind;

impl CantonScanDatasetKind {
    /// Returns the canonical string identifier for this dataset kind.
    #[inline]
    pub const fn as_str(self) -> &'static str {
        DATASET_KIND
    }
}

impl From<CantonScanDatasetKind> for DatasetKindStr {
    fn from(value: CantonScanDatasetKind) -> Self {
        DatasetKindStr::new(value.to_string())
    }
}

#[cfg(feature = "schemars")]
fn canton_scan_dataset_kind_schema(_gen: &mut schemars::SchemaGenerator) -> schemars::Schema {
    let schema_obj = serde_json::json!({
        "const": DATASET_KIND
    });
    serde_json::from_value(schema_obj).unwrap()
}

impl std::str::FromStr for CantonScanDatasetKind {
    type Err = CantonScanDatasetKindError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s != DATASET_KIND {
            return Err(CantonScanDatasetKindError(s.to_string()));
        }

        Ok(CantonScanDatasetKind)
    }
}

impl std::fmt::Display for CantonScanDatasetKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        DATASET_KIND.fmt(f)
    }
}

impl serde::Serialize for CantonScanDatasetKind {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(DATASET_KIND)
    }
}

impl<'de> serde::Deserialize<'de> for CantonScanDatasetKind {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

impl PartialEq<str> for CantonScanDatasetKind {
    fn eq(&self, other: &str) -> bool {
        DATASET_KIND == other
    }
}

impl PartialEq<CantonScanDatasetKind> for str {
    fn eq(&self, _other: &CantonScanDatasetKind) -> bool {
        self == DATASET_KIND
    }
}

impl PartialEq<&str> for CantonScanDatasetKind {
    fn eq(&self, other: &&str) -> bool {
        DATASET_KIND == *other
    }
}

impl PartialEq<CantonScanDatasetKind> for &str {
    fn eq(&self, _other: &CantonScanDatasetKind) -> bool {
        *self == DATASET_KIND
    }
}

impl PartialEq<String> for CantonScanDatasetKind {
    fn eq(&self, other: &String) -> bool {
        DATASET_KIND == other.as_str()
    }
}

impl PartialEq<CantonScanDatasetKind> for String {
    fn eq(&self, _other: &CantonScanDatasetKind) -> bool {
        self.as_str() == DATASET_KIND
    }
}

impl PartialEq<DatasetKindStr> for CantonScanDatasetKind {
    fn eq(&self, other: &DatasetKindStr) -> bool {
        DATASET_KIND == other.as_str()
    }
}

impl PartialEq<CantonScanDatasetKind> for DatasetKindStr {
    fn eq(&self, _other: &CantonScanDatasetKind) -> bool {
        self.as_str() == DATASET_KIND
    }
}

impl PartialEq<CantonScanDatasetKind> for &DatasetKindStr {
    fn eq(&self, _other: &CantonScanDatasetKind) -> bool {
        self.as_str() == DATASET_KIND
    }
}

/// Error returned when parsing an invalid Canton Scan API dataset kind string.
///
/// This error is returned when attempting to parse a string that does not
/// match the expected "canton-scan" dataset kind identifier.
#[derive(Debug, thiserror::Error)]
#[error("invalid dataset kind: {}, expected: {}", .0, DATASET_KIND)]
pub struct CantonScanDatasetKindError(String);
