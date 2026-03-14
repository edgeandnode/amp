//! Static dataset kind type and parsing utilities.
//!
//! This module defines the type-safe representation of the static dataset kind
//! and provides parsing functionality with proper error handling.

use datasets_common::dataset_kind_str::DatasetKindStr;

/// The canonical string identifier for static datasets.
const DATASET_KIND: &str = "static";

/// Type-safe representation of the static dataset kind.
///
/// This zero-sized type represents the "static" dataset kind, which defines
/// datasets backed by static data files such as CSV.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(inline))]
#[cfg_attr(
    feature = "schemars",
    schemars(schema_with = "static_dataset_kind_schema")
)]
pub struct StaticDatasetKind;

impl StaticDatasetKind {
    /// Returns the canonical string identifier for this dataset kind.
    #[inline]
    pub const fn as_str(self) -> &'static str {
        DATASET_KIND
    }
}

impl From<StaticDatasetKind> for DatasetKindStr {
    fn from(value: StaticDatasetKind) -> Self {
        // SAFETY: StaticDatasetKind is a strongly-typed ZST whose Display impl produces
        // a valid dataset kind string.
        DatasetKindStr::new_unchecked(value.to_string())
    }
}

#[cfg(feature = "schemars")]
fn static_dataset_kind_schema(_gen: &mut schemars::SchemaGenerator) -> schemars::Schema {
    let schema_obj = serde_json::json!({
        "const": DATASET_KIND
    });
    serde_json::from_value(schema_obj).unwrap()
}

impl std::str::FromStr for StaticDatasetKind {
    type Err = StaticDatasetKindError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s != DATASET_KIND {
            return Err(StaticDatasetKindError(s.to_string()));
        }

        Ok(StaticDatasetKind)
    }
}

impl std::fmt::Display for StaticDatasetKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        DATASET_KIND.fmt(f)
    }
}

impl serde::Serialize for StaticDatasetKind {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(DATASET_KIND)
    }
}

impl<'de> serde::Deserialize<'de> for StaticDatasetKind {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

impl PartialEq<str> for StaticDatasetKind {
    fn eq(&self, other: &str) -> bool {
        DATASET_KIND == other
    }
}

impl PartialEq<StaticDatasetKind> for str {
    fn eq(&self, _other: &StaticDatasetKind) -> bool {
        self == DATASET_KIND
    }
}

impl PartialEq<&str> for StaticDatasetKind {
    fn eq(&self, other: &&str) -> bool {
        DATASET_KIND == *other
    }
}

impl PartialEq<StaticDatasetKind> for &str {
    fn eq(&self, _other: &StaticDatasetKind) -> bool {
        *self == DATASET_KIND
    }
}

impl PartialEq<String> for StaticDatasetKind {
    fn eq(&self, other: &String) -> bool {
        DATASET_KIND == other.as_str()
    }
}

impl PartialEq<StaticDatasetKind> for String {
    fn eq(&self, _other: &StaticDatasetKind) -> bool {
        self.as_str() == DATASET_KIND
    }
}

impl PartialEq<DatasetKindStr> for StaticDatasetKind {
    fn eq(&self, other: &DatasetKindStr) -> bool {
        DATASET_KIND == other.as_str()
    }
}

impl PartialEq<StaticDatasetKind> for DatasetKindStr {
    fn eq(&self, _other: &StaticDatasetKind) -> bool {
        self.as_str() == DATASET_KIND
    }
}

impl PartialEq<StaticDatasetKind> for &DatasetKindStr {
    fn eq(&self, _other: &StaticDatasetKind) -> bool {
        self.as_str() == DATASET_KIND
    }
}

/// Error returned when parsing an invalid static dataset kind string.
#[derive(Debug, thiserror::Error)]
#[error("invalid dataset kind: {}, expected: {}", .0, DATASET_KIND)]
pub struct StaticDatasetKindError(String);
