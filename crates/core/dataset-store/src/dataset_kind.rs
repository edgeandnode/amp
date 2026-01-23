//! Dataset kind types and parsing utilities.
//!
//! This module defines the different types of datasets supported by the amp system
//! and provides utilities for parsing dataset kind strings into strongly-typed enums.
//!
//! # Dataset Types
//!
//! The system supports several different dataset kinds:
//! - **EVM RPC**: Direct connection to Ethereum-compatible JSON-RPC endpoints
//! - **Firehose**: StreamingFast Firehose protocol for real-time blockchain data
//! - **Derived**: Modern manifest-based dataset definitions

use datasets_common::raw_dataset_kind::RawDatasetKind;
use datasets_derived::DerivedDatasetKind;
use evm_rpc_datasets::EvmRpcDatasetKind;
use firehose_datasets::FirehoseDatasetKind;
use solana_datasets::SolanaDatasetKind;

/// Represents the different types of datasets supported by the system.
///
/// Dataset kinds determine how data is extracted, processed, and served.
/// Raw datasets (`EvmRpc`, `Firehose`) extract data directly
/// from blockchain sources, while derived datasets (`Derived`)
/// transform data from other datasets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum DatasetKind {
    /// Ethereum-compatible JSON-RPC dataset for direct blockchain access.
    EvmRpc,
    /// Solana dataset for direct blockchain access.
    Solana,
    /// StreamingFast Firehose dataset for high-throughput blockchain streaming.
    Firehose,
    /// Derived dataset.
    ///
    /// Modern dataset definition using structured configuration.
    Derived,
}

impl DatasetKind {
    /// Returns `true` if this dataset kind extracts raw data from blockchain sources.
    ///
    /// Raw datasets (`EvmRpc`, `Firehose`) connect directly to blockchain
    /// infrastructure, while derived datasets (`Derived`) process data from other datasets.
    pub fn is_raw(&self) -> bool {
        matches!(self, Self::EvmRpc | Self::Solana | Self::Firehose)
    }

    /// Returns the string representation of this dataset kind.
    ///
    /// This returns the canonical string identifier used in dataset definitions
    /// and configuration files.
    pub fn as_str(&self) -> &str {
        match self {
            Self::EvmRpc => EvmRpcDatasetKind.as_str(),
            Self::Solana => SolanaDatasetKind.as_str(),
            Self::Firehose => FirehoseDatasetKind.as_str(),
            Self::Derived => DerivedDatasetKind.as_str(),
        }
    }
}

impl std::fmt::Display for DatasetKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.as_str().fmt(f)
    }
}

impl std::str::FromStr for DatasetKind {
    type Err = UnsupportedKindError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            s if s == EvmRpcDatasetKind => Ok(Self::EvmRpc),
            s if s == SolanaDatasetKind => Ok(Self::Solana),
            s if s == FirehoseDatasetKind => Ok(Self::Firehose),
            s if s == DerivedDatasetKind => Ok(Self::Derived),
            _ => Err(UnsupportedKindError {
                kind: s.to_string(),
            }),
        }
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

impl TryFrom<&RawDatasetKind> for DatasetKind {
    type Error = UnsupportedKindError;

    fn try_from(value: &RawDatasetKind) -> Result<Self, Self::Error> {
        value.as_str().parse().map_err(|_| UnsupportedKindError {
            kind: value.to_string(),
        })
    }
}

impl From<DatasetKind> for RawDatasetKind {
    fn from(value: DatasetKind) -> Self {
        RawDatasetKind::new(value.to_string())
    }
}
