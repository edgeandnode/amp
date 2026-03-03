use std::collections::BTreeMap;

use datasets_common::{block_range::BlockRange, network_id::NetworkId};

#[derive(Debug, Clone)]
pub struct NetworkWatermarkInfo {
    pub start: u64,
    pub end: u64,
    pub offset: u64,
}

#[derive(Debug, Clone)]
pub struct WatermarkContext {
    pub base_watermark: u64,
    pub networks: BTreeMap<NetworkId, NetworkWatermarkInfo>,
}

impl WatermarkContext {
    pub fn from_block_ranges(base_watermark: u64, ranges: &[BlockRange]) -> Self {
        let mut sorted_ranges: Vec<_> = ranges.iter().collect();
        sorted_ranges.sort_by(|a, b| a.network.cmp(&b.network));

        let mut networks = BTreeMap::new();
        let mut cumulative_offset = 0u64;

        for range in sorted_ranges {
            let block_count = range.end().saturating_sub(range.start()).saturating_add(1);
            networks.insert(
                range.network.clone(),
                NetworkWatermarkInfo {
                    start: range.start(),
                    end: range.end(),
                    offset: cumulative_offset,
                },
            );
            cumulative_offset = cumulative_offset.saturating_add(block_count);
        }

        Self {
            base_watermark,
            networks,
        }
    }

    pub fn is_multi_network(&self) -> bool {
        self.networks.len() > 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::BlockHash;

    fn create_test_range(start: u64, end: u64, network: &str) -> BlockRange {
        BlockRange {
            numbers: start..=end,
            network: NetworkId::new_unchecked(network.to_string()),
            hash: BlockHash::ZERO,
            prev_hash: BlockHash::ZERO,
            timestamp: None,
        }
    }

    #[test]
    fn test_single_network() {
        let ranges = vec![create_test_range(100, 105, "mainnet")];

        let ctx = WatermarkContext::from_block_ranges(0, &ranges);
        assert!(!ctx.is_multi_network());
        assert_eq!(ctx.networks.len(), 1);

        let info = ctx
            .networks
            .get(&NetworkId::new_unchecked("mainnet".to_string()))
            .unwrap();
        assert_eq!(info.start, 100);
        assert_eq!(info.end, 105);
        assert_eq!(info.offset, 0);
    }

    #[test]
    fn test_multi_network_ordering() {
        let ranges = vec![
            create_test_range(200, 204, "polygon"),
            create_test_range(100, 109, "mainnet"),
        ];

        let ctx = WatermarkContext::from_block_ranges(0, &ranges);
        assert!(ctx.is_multi_network());
        assert_eq!(ctx.networks.len(), 2);

        let mainnet = ctx
            .networks
            .get(&NetworkId::new_unchecked("mainnet".to_string()))
            .unwrap();
        assert_eq!(mainnet.start, 100);
        assert_eq!(mainnet.end, 109);
        assert_eq!(mainnet.offset, 0);

        let polygon = ctx
            .networks
            .get(&NetworkId::new_unchecked("polygon".to_string()))
            .unwrap();
        assert_eq!(polygon.start, 200);
        assert_eq!(polygon.end, 204);
        assert_eq!(polygon.offset, 10);
    }

    #[test]
    fn test_base_watermark() {
        let ranges = vec![create_test_range(100, 105, "mainnet")];

        let ctx = WatermarkContext::from_block_ranges(500, &ranges);
        assert_eq!(ctx.base_watermark, 500);
    }
}
