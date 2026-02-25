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
/// The error includes both the expected network and the networks actually present
/// in the cursor, making network name mismatches easy to diagnose.
#[derive(Debug, thiserror::Error)]
#[error("expected cursor for network '{expected}', but cursor only contains networks: {available}")]
pub struct CursorNetworkNotFoundError {
    /// The network that was requested but not found in the cursor.
    pub expected: NetworkId,
    /// The networks that are present in the cursor.
    pub available: NetworkIdList,
}

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
        let available: Vec<NetworkId> = self.0.keys().cloned().collect();
        self.0
            .into_iter()
            .find(|(n, _)| n == network)
            .map(|(_, c)| c)
            .ok_or_else(|| CursorNetworkNotFoundError {
                expected: network.clone(),
                available: NetworkIdList(available),
            })
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

/// A wrapper around an array of network identifiers for displaying in error messages.
#[derive(Debug)]
pub struct NetworkIdList(pub Vec<NetworkId>);

impl std::fmt::Display for NetworkIdList {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let names: Vec<&str> = self.0.iter().map(|n| n.as_str()).collect();
        write!(f, "[{}]", names.join(","))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn to_single_network_with_matching_network_succeeds() {
        //* Given
        let network: NetworkId = "mainnet".parse().expect("should parse mainnet network ID");
        let expected_cursor = NetworkCursor {
            number: 100,
            hash: BlockHash::ZERO,
        };
        let cursor = Cursor(BTreeMap::from([(network.clone(), expected_cursor.clone())]));

        //* When
        let result = cursor.to_single_network(&network);

        //* Then
        assert_eq!(
            result.expect("should return cursor for matching network"),
            expected_cursor,
            "cursor should match expected network cursor"
        );
    }

    #[test]
    fn to_single_network_with_mismatched_network_shows_available() {
        //* Given
        let mut map = BTreeMap::new();
        map.insert(
            "ethereum-mainnet"
                .parse::<NetworkId>()
                .expect("should parse ethereum-mainnet network ID"),
            NetworkCursor {
                number: 100,
                hash: BlockHash::ZERO,
            },
        );
        let cursor = Cursor(map);

        //* When
        let result =
            cursor.to_single_network(&"mainnet".parse().expect("should parse mainnet network ID"));

        //* Then
        let err = result.expect_err("should fail when network not found in cursor");
        assert_eq!(
            err.expected, "mainnet",
            "error should report expected network"
        );
        assert_eq!(
            err.available.0.len(),
            1,
            "error should list one available network"
        );
        assert_eq!(
            err.available.0[0], "ethereum-mainnet",
            "error should list ethereum-mainnet as available"
        );
        assert_eq!(
            err.to_string(),
            "expected cursor for network 'mainnet', but cursor only contains networks: [ethereum-mainnet]",
            "error message should include both expected and available networks"
        );
    }
}
