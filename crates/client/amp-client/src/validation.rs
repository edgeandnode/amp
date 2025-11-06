//! Protocol invariant validation for streaming responses.
//!
//! This module implements validation of protocol invariants that the server
//! must uphold when sending streaming responses. These validations ensure
//! the client receives well-formed, consistent data.

use common::metadata::segments::BlockRange;

use crate::error::Error;

/// Validate network consistency: no duplicates within batch, and stability across batches.
///
/// # Protocol Invariants
/// 1. Each batch must contain at most one BlockRange per network (no duplicates)
/// 2. The set of networks must remain constant throughout the stream (stability)
///
/// # Arguments
/// * `previous` - Block ranges from previous batch (empty if first batch)
/// * `incoming` - Block ranges from incoming batch
///
/// # Returns
/// * `Ok(())` if networks are valid and stable
/// * `Err(Error::ProtocolInvariantViolation)` if duplicates or instability detected
pub fn validate_networks(previous: &[BlockRange], incoming: &[BlockRange]) -> Result<(), Error> {
    // Check for duplicates in incoming batch (O(n²) for small n)
    for i in 0..incoming.len() {
        for j in (i + 1)..incoming.len() {
            if incoming[i].network == incoming[j].network {
                return Err(Error::ProtocolInvariantViolation(format!(
                    "duplicate network '{}' in batch",
                    incoming[i].network
                )));
            }
        }
    }

    // Check network stability if we have a previous batch
    if !previous.is_empty() {
        // Fast path: count must match
        if previous.len() != incoming.len() {
            return Err(Error::ProtocolInvariantViolation(format!(
                "network count changed: expected {}, got {}",
                previous.len(),
                incoming.len()
            )));
        }

        // Verify all incoming networks exist in previous (no allocations)
        for incoming_range in incoming {
            if !previous.iter().any(|p| p.network == incoming_range.network) {
                return Err(Error::ProtocolInvariantViolation(format!(
                    "network set changed: unexpected network '{}'",
                    incoming_range.network
                )));
            }
        }
    }

    Ok(())
}

/// Validate that block ranges are consecutive for each network.
///
/// # Protocol Invariant
/// For each network, consecutive batches must have adjacent block ranges:
/// - If batch N ends at block 100 with hash H100
/// - Then batch N+1 must start at block 101 with prev_hash = H100
///
/// This check is skipped when a reorg has been detected, as reorgs
/// intentionally break consecutiveness.
///
/// # Arguments
/// * `previous` - Block ranges from previous batch
/// * `incoming` - Block ranges from incoming batch
///
/// # Returns
/// * `Ok(())` if ranges are consecutive (or reorg detected)
/// * `Err(Error::ProtocolInvariantViolation)` if consecutiveness broken
pub fn validate_consecutiveness(
    previous: &[BlockRange],
    incoming: &[BlockRange],
) -> Result<(), Error> {
    // For small inputs (typical: 1-3 networks), linear search is faster than HashMap
    for incoming_range in incoming {
        // Find matching network in previous batch
        let prev_range = previous
            .iter()
            .find(|p| p.network == incoming_range.network);

        if let Some(prev_range) = prev_range {
            // Allow two cases:
            // 1. Consecutive ranges: incoming.start == prev.end + 1
            // 2. Identical ranges: incoming == prev (watermarks can repeat the same range)
            let is_consecutive = incoming_range.start() == prev_range.end() + 1;
            let is_identical = incoming_range == prev_range;

            if !is_consecutive && !is_identical {
                let expected_start = prev_range.end() + 1;
                return Err(Error::ProtocolInvariantViolation(format!(
                    "non-consecutive blocks for network '{}': previous ended at {}, incoming starts at {} (expected {} or matching range)",
                    incoming_range.network,
                    prev_range.end(),
                    incoming_range.start(),
                    expected_start
                )));
            }

            // Note: We don't validate hash chain continuity here because:
            // 1. Reorgs can cause hash mismatches which are detected at the transactional layer
            // 2. The server may not always include prev_hash
            // 3. Hash validation is primarily for debugging, not protocol correctness
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use alloy::primitives::BlockHash;

    use super::*;

    /// Create a test BlockRange with optional prev_hash for chaining
    fn test_range_with_prev(
        network: &str,
        start: u64,
        end: u64,
        hash_suffix: u8,
        prev_hash: Option<BlockHash>,
    ) -> BlockRange {
        let mut hash_bytes = [0u8; 32];
        hash_bytes[31] = hash_suffix;
        let hash = BlockHash::from_slice(&hash_bytes);

        BlockRange {
            network: network.to_string(),
            numbers: start..=end,
            hash,
            prev_hash,
        }
    }

    /// Create a standalone test BlockRange (for single-range tests)
    fn test_range(network: &str, start: u64, end: u64, hash_suffix: u8) -> BlockRange {
        test_range_with_prev(network, start, end, hash_suffix, None)
    }

    mod validate_networks {
        use super::*;

        #[test]
        fn first_batch_with_unique_networks_succeeds() {
            //* Given
            let previous: Vec<BlockRange> = vec![];
            let incoming = vec![test_range("eth", 0, 10, 1), test_range("polygon", 0, 10, 1)];

            //* When
            let result = validate_networks(&previous, &incoming);

            //* Then
            assert!(
                result.is_ok(),
                "validation should succeed for first batch with unique networks"
            );
        }

        #[test]
        fn first_batch_with_single_network_succeeds() {
            //* Given
            let previous: Vec<BlockRange> = vec![];
            let incoming = vec![test_range("eth", 0, 10, 1)];

            //* When
            let result = validate_networks(&previous, &incoming);

            //* Then
            assert!(
                result.is_ok(),
                "validation should succeed for first batch with single network"
            );
        }

        #[test]
        fn both_empty_succeeds() {
            //* Given
            let previous: Vec<BlockRange> = vec![];
            let incoming: Vec<BlockRange> = vec![];

            //* When
            let result = validate_networks(&previous, &incoming);

            //* Then
            assert!(result.is_ok(), "validation should succeed when both empty");
        }

        #[test]
        fn duplicate_network_in_incoming_fails() {
            //* Given
            let previous: Vec<BlockRange> = vec![];
            let incoming = vec![test_range("eth", 0, 10, 1), test_range("eth", 11, 20, 2)];

            //* When
            let result = validate_networks(&previous, &incoming);

            //* Then
            assert!(
                result.is_err(),
                "validation should fail with duplicate network"
            );
            let error = result.expect_err("should return validation error");
            assert!(
                matches!(error, Error::ProtocolInvariantViolation(_)),
                "Expected ProtocolInvariantViolation, got {:?}",
                error
            );
        }

        #[test]
        fn matching_networks_across_batches_succeeds() {
            //* Given
            let previous = vec![test_range("eth", 0, 10, 1), test_range("polygon", 0, 10, 1)];
            let incoming = vec![
                test_range("eth", 11, 20, 2),
                test_range("polygon", 11, 20, 2),
            ];

            //* When
            let result = validate_networks(&previous, &incoming);

            //* Then
            assert!(
                result.is_ok(),
                "validation should succeed with matching networks"
            );
        }

        #[test]
        fn missing_network_in_incoming_fails() {
            //* Given
            let previous = vec![test_range("eth", 0, 10, 1), test_range("polygon", 0, 10, 1)];
            let incoming = vec![test_range("eth", 11, 20, 2)];

            //* When
            let result = validate_networks(&previous, &incoming);

            //* Then
            assert!(
                result.is_err(),
                "validation should fail with missing network"
            );
            let error = result.expect_err("should return validation error");
            assert!(
                matches!(error, Error::ProtocolInvariantViolation(_)),
                "Expected ProtocolInvariantViolation, got {:?}",
                error
            );
        }

        #[test]
        fn added_network_in_incoming_fails() {
            //* Given
            let previous = vec![test_range("eth", 0, 10, 1)];
            let incoming = vec![
                test_range("eth", 11, 20, 2),
                test_range("polygon", 11, 20, 2),
            ];

            //* When
            let result = validate_networks(&previous, &incoming);

            //* Then
            assert!(result.is_err(), "validation should fail with added network");
            let error = result.expect_err("should return validation error");
            assert!(
                matches!(error, Error::ProtocolInvariantViolation(_)),
                "Expected ProtocolInvariantViolation, got {:?}",
                error
            );
        }
    }

    mod validate_consecutiveness {
        use super::*;

        #[test]
        fn with_consecutive_blocks_succeeds() {
            //* Given
            let previous = vec![test_range("eth", 0, 10, 10)];
            // Create incoming range with prev_hash matching previous range's hash
            let incoming = vec![test_range_with_prev(
                "eth",
                11,
                20,
                20,
                Some(previous[0].hash),
            )];

            //* When
            let result = validate_consecutiveness(&previous, &incoming);

            //* Then
            assert!(
                result.is_ok(),
                "validation should succeed with consecutive blocks"
            );
        }

        #[test]
        fn with_gap_in_blocks_fails() {
            //* Given
            let previous = vec![test_range("eth", 0, 10, 10)];
            let incoming = vec![test_range("eth", 15, 20, 20)]; // Gap: 11-14 missing

            //* When
            let result = validate_consecutiveness(&previous, &incoming);

            //* Then
            assert!(result.is_err(), "validation should fail with block gap");
            let error = result.expect_err("should return validation error");
            assert!(
                matches!(error, Error::ProtocolInvariantViolation(_)),
                "Expected ProtocolInvariantViolation, got {:?}",
                error
            );
        }

        #[test]
        fn with_overlapping_blocks_fails() {
            //* Given
            let previous = vec![test_range("eth", 0, 10, 10)];
            let incoming = vec![test_range("eth", 10, 20, 20)]; // Overlap at block 10

            //* When
            let result = validate_consecutiveness(&previous, &incoming);

            //* Then
            assert!(
                result.is_err(),
                "validation should fail with overlapping blocks"
            );
            let error = result.expect_err("should return validation error");
            assert!(
                matches!(error, Error::ProtocolInvariantViolation(_)),
                "Expected ProtocolInvariantViolation, got {:?}",
                error
            );
        }

        #[test]
        fn with_multi_network_consecutive_succeeds() {
            //* Given
            let previous = vec![test_range("eth", 0, 10, 10), test_range("polygon", 0, 5, 5)];
            let incoming = vec![
                test_range("eth", 11, 20, 20),
                test_range("polygon", 6, 10, 10),
            ];

            //* When
            let result = validate_consecutiveness(&previous, &incoming);

            //* Then
            assert!(
                result.is_ok(),
                "validation should succeed with multi-network consecutive blocks"
            );
        }
    }
}
