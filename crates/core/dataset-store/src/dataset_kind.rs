//! Dataset kind types and parsing utilities.
//!
//! This module defines the different types of datasets supported by the nozzle system
//! and provides utilities for parsing dataset kind strings into strongly-typed enums.
//!
//! # Dataset Types
//!
//! The system supports several different dataset kinds:
//! - **EVM RPC**: Direct connection to Ethereum-compatible JSON-RPC endpoints
//! - **Firehose**: StreamingFast Firehose protocol for real-time blockchain data
//! - **Substreams**: Processing of Substreams packages with dynamic schema inference
//! - **SQL**: SQL-based dataset definitions (deprecated in favor of Manifest)
//! - **Manifest**: Modern manifest-based dataset definitions

/// Represents the different types of datasets supported by the system.
///
/// Dataset kinds determine how data is extracted, processed, and served.
/// Raw datasets (`EvmRpc`, `Firehose`, `Substreams`) extract data directly
/// from blockchain sources, while derived datasets (`Sql`, `Manifest`)
/// transform data from other datasets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum DatasetKind {
    /// Ethereum-compatible JSON-RPC dataset for direct blockchain access.
    EvmRpc,
    /// StreamingFast Firehose dataset for high-throughput blockchain streaming.
    Firehose,
    /// Substreams dataset for processing custom blockchain transformations.
    Substreams,
    /// SQL-based dataset definition using `.sql` files.
    Sql, // Will be deprecated in favor of `Manifest`
    /// Manifest-based dataset definition using structured configuration.
    Manifest,
}

impl DatasetKind {
    /// Returns `true` if this dataset kind extracts raw data from blockchain sources.
    ///
    /// Raw datasets (`EvmRpc`, `Firehose`, `Substreams`) connect directly to blockchain
    /// infrastructure, while derived datasets (`Sql`, `Manifest`) process data from
    /// other datasets.
    pub fn is_raw(&self) -> bool {
        matches!(self, Self::EvmRpc | Self::Firehose | Self::Substreams)
    }

    /// Returns the string representation of this dataset kind.
    ///
    /// This returns the canonical string identifier used in dataset definitions
    /// and configuration files.
    pub fn as_str(&self) -> &str {
        match self {
            Self::EvmRpc => evm_rpc_datasets::DATASET_KIND,
            Self::Firehose => firehose_datasets::DATASET_KIND,
            Self::Substreams => substreams_datasets::DATASET_KIND,
            Self::Sql => crate::sql_datasets::DATASET_KIND,
            Self::Manifest => common::manifest::DATASET_KIND,
        }
    }
}

impl std::fmt::Display for DatasetKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EvmRpc => f.write_str(evm_rpc_datasets::DATASET_KIND),
            Self::Firehose => f.write_str(firehose_datasets::DATASET_KIND),
            Self::Substreams => f.write_str(substreams_datasets::DATASET_KIND),
            Self::Sql => f.write_str(crate::sql_datasets::DATASET_KIND),
            Self::Manifest => f.write_str(common::manifest::DATASET_KIND),
        }
    }
}

impl std::str::FromStr for DatasetKind {
    type Err = UnsupportedKindError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            evm_rpc_datasets::DATASET_KIND => Ok(Self::EvmRpc),
            firehose_datasets::DATASET_KIND => Ok(Self::Firehose),
            substreams_datasets::DATASET_KIND => Ok(Self::Substreams),
            crate::sql_datasets::DATASET_KIND => Ok(Self::Sql),
            common::manifest::DATASET_KIND => Ok(Self::Manifest),
            k => Err(UnsupportedKindError {
                kind: k.to_string(),
            }),
        }
    }
}

impl<'de> serde::Deserialize<'de> for DatasetKind {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // NOTE: We use `String` instead of `&str` because TOML (and other formats)
        // often provide owned strings when deserializing struct fields. While `&str`
        // would enable zero-copy deserialization in some cases (like JSON), it fails
        // when the deserializer can only provide owned strings. Using `String` ensures
        // compatibility with all serde formats at the cost of a small allocation.
        let s: String = serde::Deserialize::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

impl serde::Serialize for DatasetKind {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

/// Error returned when parsing an unsupported dataset kind string.
///
/// This error is returned by [`DatasetKind::from_str`] when the provided string
/// does not match any of the supported dataset kind identifiers.
#[derive(Debug, thiserror::Error)]
#[error("unsupported dataset kind '{kind}'")]
pub struct UnsupportedKindError {
    /// The unsupported dataset kind string that caused this error.
    pub kind: String,
}
