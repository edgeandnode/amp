//! Raw dataset manifest types.

use datasets_common::{dataset_kind_str::DatasetKindStr, network_id::NetworkId};

/// Common metadata fields for raw dataset manifests.
///
/// Raw datasets always require a network field to identify which blockchain network
/// the data comes from.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RawDatasetManifest {
    /// Dataset kind. Supported values: `evm-rpc`, `firehose`, `solana`.
    pub kind: DatasetKindStr,

    /// Network name, e.g., `mainnet`, `sepolia`.
    ///
    /// Required for all raw datasets to identify the blockchain network.
    pub network: NetworkId,
}
