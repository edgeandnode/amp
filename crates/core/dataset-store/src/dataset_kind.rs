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

use datasets_derived::DerivedDatasetKind;
use eth_beacon_datasets::EthBeaconDatasetKind;
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
    /// Ethereum Beacon node (consensus layer) dataset.
    EthBeacon,
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
        matches!(
            self,
            Self::EvmRpc | Self::Solana | Self::EthBeacon | Self::Firehose
        )
    }

    /// Returns the string representation of this dataset kind.
    ///
    /// This returns the canonical string identifier used in dataset definitions
    /// and configuration files.
    pub fn as_str(&self) -> &str {
        match self {
            Self::EvmRpc => EvmRpcDatasetKind.as_str(),
            Self::Solana => SolanaDatasetKind.as_str(),
            Self::EthBeacon => EthBeaconDatasetKind.as_str(),
            Self::Firehose => FirehoseDatasetKind.as_str(),
            Self::Derived => DerivedDatasetKind.as_str(),
        }
    }
}

/// Macro to generate `From` implementations for converting specific dataset kind
/// types into `DatasetKind`.
macro_rules! impl_from_for_kind {
    ($kind_type:ty, $variant:path) => {
        impl From<$kind_type> for DatasetKind {
            fn from(_: $kind_type) -> Self {
                $variant
            }
        }
    };
}

impl_from_for_kind!(EvmRpcDatasetKind, DatasetKind::EvmRpc);
impl_from_for_kind!(SolanaDatasetKind, DatasetKind::Solana);
impl_from_for_kind!(EthBeaconDatasetKind, DatasetKind::EthBeacon);
impl_from_for_kind!(FirehoseDatasetKind, DatasetKind::Firehose);
impl_from_for_kind!(DerivedDatasetKind, DatasetKind::Derived);

/// Macro to generate bidirectional `PartialEq` implementations between
/// `DatasetKind` and specific dataset kind types.
macro_rules! impl_partial_eq_for_kind {
    ($kind_type:ty, $variant:path) => {
        impl PartialEq<$kind_type> for DatasetKind {
            fn eq(&self, _other: &$kind_type) -> bool {
                matches!(self, $variant)
            }
        }

        impl PartialEq<DatasetKind> for $kind_type {
            fn eq(&self, other: &DatasetKind) -> bool {
                other.eq(self)
            }
        }
    };
}

impl_partial_eq_for_kind!(EvmRpcDatasetKind, DatasetKind::EvmRpc);
impl_partial_eq_for_kind!(SolanaDatasetKind, DatasetKind::Solana);
impl_partial_eq_for_kind!(EthBeaconDatasetKind, DatasetKind::EthBeacon);
impl_partial_eq_for_kind!(FirehoseDatasetKind, DatasetKind::Firehose);
impl_partial_eq_for_kind!(DerivedDatasetKind, DatasetKind::Derived);

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
            s if s == EthBeaconDatasetKind => Ok(Self::EthBeacon),
            s if s == FirehoseDatasetKind => Ok(Self::Firehose),
            s if s == DerivedDatasetKind => Ok(Self::Derived),
            _ => Err(UnsupportedKindError {
                kind: s.to_string(),
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
