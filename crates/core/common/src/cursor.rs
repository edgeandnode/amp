use std::collections::BTreeMap;

use alloy::primitives::BlockHash;
use datasets_common::{block_num::BlockNum, block_range::BlockRange, network_id::NetworkId};

/// A watermark represents a monotonically increasing block number in materialized parquet segments.
/// Currently stored in the `_block_num` column.
pub type Watermark = BlockNum;

/// A cursor for a single network, containing both block number and hash.
/// Used for stream resumption to verify chain continuity.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct NetworkCursor {
    /// The segment end block.
    pub number: BlockNum,
    /// The hash associated with the segment end block.
    pub hash: BlockHash,
}

impl From<&BlockRange> for NetworkCursor {
    fn from(range: &BlockRange) -> Self {
        Self {
            number: range.end(),
            hash: range.hash,
        }
    }
}

/// Cursor for resuming a stream from a specific point.
///
/// A cursor contains a pair of block number and block hash for each network,
/// representing the last successfully processed block per network. This allows
/// stream resumption to continue from the correct position across multiple networks.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Cursor(pub BTreeMap<NetworkId, NetworkCursor>);

/// Error when extracting a cursor for a specific network from a multi-network Cursor
///
/// This occurs when the Cursor does not contain an entry for the requested network.
#[derive(Debug, thiserror::Error)]
#[error("expected cursor for network '{0}'")]
pub struct CursorNetworkNotFoundError(pub NetworkId);

impl Cursor {
    /// Create a Cursor from a slice of BlockRanges.
    pub fn from_ranges(ranges: &[BlockRange]) -> Self {
        let cursors = ranges
            .iter()
            .map(|r| (r.network.clone(), r.into()))
            .collect();
        Self(cursors)
    }

    /// Extract the single-network cursor for a specific network.
    pub fn to_single_network(
        self,
        network: &NetworkId,
    ) -> Result<NetworkCursor, CursorNetworkNotFoundError> {
        self.0
            .into_iter()
            .find(|(n, _)| n == network)
            .map(|(_, c)| c)
            .ok_or_else(|| CursorNetworkNotFoundError(network.clone()))
    }
}

// encoding to remote plan
impl From<Cursor> for BTreeMap<NetworkId, (BlockNum, [u8; 32])> {
    fn from(value: Cursor) -> Self {
        value
            .0
            .into_iter()
            .map(|(network, NetworkCursor { number, hash })| (network, (number, hash.0)))
            .collect()
    }
}
// decoding from remote plan
impl From<BTreeMap<NetworkId, (BlockNum, [u8; 32])>> for Cursor {
    fn from(value: BTreeMap<NetworkId, (BlockNum, [u8; 32])>) -> Self {
        let inner = value
            .into_iter()
            .map(|(network, (number, hash))| {
                let hash = hash.into();
                (network, NetworkCursor { number, hash })
            })
            .collect();
        Self(inner)
    }
}
