//! SQL dataset kind type and parsing utilities.
//!
//! This module defines the type-safe representation of the SQL dataset kind
//! and provides parsing functionality with proper error handling.

/// The canonical string identifier for SQL datasets.
///
/// This constant defines the string representation used in dataset manifests
/// and configuration files to identify datasets that execute SQL queries
/// over other datasets.
pub const DATASET_KIND: &str = "sql";

/// Type-safe representation of the SQL dataset kind.
///
/// This zero-sized type represents the "sql" dataset kind, which executes
/// SQL queries over other datasets to create derived views.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(inline))]
#[cfg_attr(
    feature = "schemars",
    schemars(schema_with = "sql_dataset_kind_schema")
)]
pub struct SqlDatasetKind;

#[cfg(feature = "schemars")]
fn sql_dataset_kind_schema(_gen: &mut schemars::SchemaGenerator) -> schemars::Schema {
    let schema_obj = serde_json::json!({
        "const": DATASET_KIND
    });
    serde_json::from_value(schema_obj).unwrap()
}

impl std::str::FromStr for SqlDatasetKind {
    type Err = SqlDatasetKindError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s != DATASET_KIND {
            return Err(SqlDatasetKindError(s.to_string()));
        }

        Ok(SqlDatasetKind)
    }
}

impl std::fmt::Display for SqlDatasetKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        DATASET_KIND.fmt(f)
    }
}

impl serde::Serialize for SqlDatasetKind {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(DATASET_KIND)
    }
}

impl<'de> serde::Deserialize<'de> for SqlDatasetKind {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

/// Error returned when parsing an invalid SQL dataset kind string.
///
/// This error is returned when attempting to parse a string that does not
/// match the expected "sql" dataset kind identifier.
#[derive(Debug, thiserror::Error)]
#[error("invalid dataset kind: {}, expected: {}", .0, DATASET_KIND)]
pub struct SqlDatasetKindError(String);
