//! Protocol invariant validation for streaming responses.
//!
//! This module implements validation of protocol invariants that the server
//! must uphold when sending streaming responses. These validations ensure
//! the client receives well-formed, consistent data.

use std::cmp::Ordering;

use alloy::primitives::BlockHash;

use crate::{
    BlockRange,
    error::{Error, ValidationError},
};

/// Validate parent hash presence and correctness.
///
/// # Protocol Invariant
/// - Blocks starting at 0 (genesis) MUST have None or zero hash for parent hash
/// - All other blocks (start > 0) MUST have Some(non-zero hash) for parent hash
///
/// # Arguments
/// * `incoming` - Block range to validate
///
/// # Returns
/// * `Ok(())` if parent hash is valid
/// * `Err(Error::Validation(ValidationError::InvalidPrevHash))` if genesis has non-zero parent hash
/// * `Err(Error::Validation(ValidationError::MissingPrevHash))` if non-genesis missing or zero parent hash
pub fn validate_prev_hash(incoming: &BlockRange) -> Result<(), Error> {
    // Check if prev_hash is "actually set": Some AND not zero hash
    let is_actually_set = matches!(incoming.prev_hash, Some(hash) if hash != BlockHash::ZERO);

    if incoming.start() == 0 {
        // Genesis block must have None or zero hash
        if is_actually_set {
            return Err(Error::Validation(ValidationError::InvalidPrevHash));
        }
        return Ok(());
    }

    // All non-genesis blocks must have Some(non-zero hash)
    if !is_actually_set {
        return Err(Error::Validation(ValidationError::MissingPrevHash {
            network: incoming.network.clone(),
            block: incoming.start(),
        }));
    }

    Ok(())
}

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
/// * `Err(Error::Validation(ValidationError::DuplicateNetwork))` if duplicates detected
/// * `Err(Error::Validation(ValidationError::NetworkCountChanged))` if network count differs
/// * `Err(Error::Validation(ValidationError::UnexpectedNetwork))` if new network appears
pub fn validate_networks(previous: &[BlockRange], incoming: &[BlockRange]) -> Result<(), Error> {
    // Check for duplicates in incoming batch
    for i in 0..incoming.len() {
        for j in (i + 1)..incoming.len() {
            if incoming[i].network == incoming[j].network {
                return Err(Error::Validation(ValidationError::DuplicateNetwork {
                    network: incoming[i].network.clone(),
                }));
            }
        }
    }

    // Check network stability if we have a previous batch
    if !previous.is_empty() {
        // Check that the network count matches
        if previous.len() != incoming.len() {
            return Err(Error::Validation(ValidationError::NetworkCountChanged {
                expected: previous.len(),
                actual: incoming.len(),
            }));
        }

        // Verify all incoming networks exist in previous ranges
        for incoming_range in incoming {
            if !previous.iter().any(|p| p.network == incoming_range.network) {
                return Err(Error::Validation(ValidationError::UnexpectedNetwork {
                    network: incoming_range.network.clone(),
                }));
            }
        }
    }

    Ok(())
}

/// Validate that block ranges are consecutive for each network with hash chain continuity.
///
/// # Protocol Invariants
///
/// For each network, validates based on block range position:
///
/// ## 1. Consecutive Blocks (start of incoming range == end of previous range + 1)
/// - Hash chain MUST match (parent hash of incoming == previous block hash)
/// - Represents normal forward progression on the same chain
/// - Hash mismatch indicates protocol violation (cannot "reorg forward")
///
/// ## 2. Backwards Jump (start of incoming range < end of previous range + 1)
/// - Hash chain MUST mismatch (parent hash of incoming != previous block hash)
/// - Represents a valid blockchain reorg to a different chain
/// - Endpoint can be anywhere (reorg can extend beyond previous endpoint)
///
/// ## 3. Forward Gap (start of incoming range > end of previous range + 1)
/// - Always a protocol violation
/// - Blocks cannot be skipped
///
/// ## 4. Identical Range (incoming == previous)
/// - Allowed for watermark repeats
/// - Hash must match
///
/// # Arguments
/// * `previous` - Block ranges from previous batch
/// * `incoming` - Block ranges from incoming batch
///
/// # Returns
/// * `Ok(())` if all validations pass
/// * `Err(Error::Validation(ValidationError::*))` for protocol violations
pub fn validate_consecutiveness(
    previous: &[BlockRange],
    incoming: &[BlockRange],
) -> Result<(), Error> {
    for incoming_range in incoming {
        // Find matching network in previous batch
        let prev_range = previous
            .iter()
            .find(|p| p.network == incoming_range.network);

        if let Some(prev_range) = prev_range {
            // Check if ranges are identical (watermarks can repeat)
            if incoming_range == prev_range {
                continue;
            }

            // Validate based on block position
            match incoming_range.start().cmp(&(prev_range.end() + 1)) {
                Ordering::Greater => {
                    // Forward gaps are always invalid
                    return Err(Error::Validation(ValidationError::Gap {
                        network: incoming_range.network.clone(),
                        missing_start: prev_range.end() + 1,
                        missing_end: incoming_range.start() - 1,
                    }));
                }

                Ordering::Equal => {
                    // The hash chain must match for consecutive blocks
                    if incoming_range.prev_hash != Some(prev_range.hash) {
                        return Err(Error::Validation(
                            ValidationError::HashMismatchOnConsecutiveBlocks {
                                network: incoming_range.network.clone(),
                                expected_hash: prev_range.hash,
                                actual_prev_hash: incoming_range
                                    .prev_hash
                                    .unwrap_or(BlockHash::ZERO),
                            },
                        ));
                    }
                }

                Ordering::Less => {
                    // The hash chain must not match for backwards jumps (reorg)
                    if incoming_range.prev_hash == Some(prev_range.hash) {
                        return Err(Error::Validation(ValidationError::InvalidReorg {
                            network: incoming_range.network.clone(),
                        }));
                    }
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use alloy::primitives::BlockHash;

    use super::*;

    /// Create a test BlockRange with specified prev_hash
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
                matches!(
                    error,
                    Error::Validation(ValidationError::DuplicateNetwork { .. })
                ),
                "Expected ValidationError::DuplicateNetwork, got {:?}",
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
                matches!(
                    error,
                    Error::Validation(ValidationError::NetworkCountChanged { .. })
                ),
                "Expected ValidationError::NetworkCountChanged, got {:?}",
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
                matches!(
                    error,
                    Error::Validation(ValidationError::NetworkCountChanged { .. })
                ),
                "Expected ValidationError::NetworkCountChanged, got {:?}",
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
                matches!(error, Error::Validation(ValidationError::Gap { .. })),
                "Expected ValidationError::Gap, got {:?}",
                error
            );
        }

        #[test]
        fn with_overlapping_blocks_fails() {
            //* Given
            let previous = vec![test_range_with_prev("eth", 0, 10, 10, None)];
            // Overlap at block 10 (backwards jump with matching hash = invalid reorg)
            let incoming = vec![test_range_with_prev(
                "eth",
                10,
                20,
                20,
                Some(previous[0].hash), // Same hash = invalid reorg
            )];

            //* When
            let result = validate_consecutiveness(&previous, &incoming);

            //* Then
            assert!(
                result.is_err(),
                "validation should fail with overlapping blocks (invalid reorg)"
            );
            let error = result.expect_err("should return validation error");
            assert!(
                matches!(
                    error,
                    Error::Validation(ValidationError::InvalidReorg { .. })
                ),
                "Expected ValidationError::InvalidReorg, got {:?}",
                error
            );
        }

        #[test]
        fn with_multi_network_consecutive_succeeds() {
            //* Given
            let previous = vec![
                test_range_with_prev("eth", 0, 10, 10, None),
                test_range_with_prev("polygon", 0, 5, 5, None),
            ];
            let incoming = vec![
                test_range_with_prev("eth", 11, 20, 20, Some(previous[0].hash)),
                test_range_with_prev("polygon", 6, 10, 10, Some(previous[1].hash)),
            ];

            //* When
            let result = validate_consecutiveness(&previous, &incoming);

            //* Then
            assert!(
                result.is_ok(),
                "validation should succeed with multi-network consecutive blocks"
            );
        }

        #[test]
        fn forward_gap_fails() {
            //* Given
            let previous = vec![test_range_with_prev("eth", 0, 100, 100, None)];
            let incoming = vec![test_range_with_prev(
                "eth",
                150,
                200,
                200,
                Some(previous[0].hash),
            )];

            //* When
            let result = validate_consecutiveness(&previous, &incoming);

            //* Then
            assert!(result.is_err(), "validation should fail with forward gap");
            let error = result.expect_err("should return validation error");
            assert!(
                matches!(error, Error::Validation(ValidationError::Gap { .. })),
                "Expected ValidationError::Gap, got {:?}",
                error
            );
        }

        #[test]
        fn consecutive_blocks_with_hash_mismatch_fails() {
            //* Given
            let previous = vec![test_range_with_prev("eth", 0, 100, 100, None)];
            // Create a different hash for the prev_hash (simulating a chain mismatch)
            let mut fake_hash = [0u8; 32];
            fake_hash[31] = 99; // Different from previous hash
            let incoming = vec![test_range_with_prev(
                "eth",
                101,
                200,
                200,
                Some(BlockHash::from_slice(&fake_hash)),
            )];

            //* When
            let result = validate_consecutiveness(&previous, &incoming);

            //* Then
            assert!(
                result.is_err(),
                "validation should fail with hash mismatch on consecutive blocks"
            );
            let error = result.expect_err("should return validation error");
            assert!(
                matches!(
                    error,
                    Error::Validation(ValidationError::HashMismatchOnConsecutiveBlocks { .. })
                ),
                "Expected ValidationError::HashMismatchOnConsecutiveBlocks, got {:?}",
                error
            );
        }

        #[test]
        fn backwards_jump_with_hash_mismatch_succeeds() {
            //* Given
            let previous = vec![test_range_with_prev("eth", 0, 100, 100, None)];
            // Different hash for reorg
            let mut reorg_hash = [0u8; 32];
            reorg_hash[31] = 50; // Different hash
            let incoming = vec![test_range_with_prev(
                "eth",
                50,
                150,
                150,
                Some(BlockHash::from_slice(&reorg_hash)),
            )];

            //* When
            let result = validate_consecutiveness(&previous, &incoming);

            //* Then
            assert!(
                result.is_ok(),
                "validation should succeed with backwards jump and hash mismatch (valid reorg)"
            );
        }

        #[test]
        fn backwards_jump_with_matching_hash_fails() {
            //* Given
            let previous = vec![test_range_with_prev("eth", 0, 100, 100, None)];
            // Same hash as previous (invalid reorg)
            let incoming = vec![test_range_with_prev(
                "eth",
                50,
                100,
                100,
                Some(previous[0].hash), // Same hash - invalid!
            )];

            //* When
            let result = validate_consecutiveness(&previous, &incoming);

            //* Then
            assert!(
                result.is_err(),
                "validation should fail with backwards jump but matching hash"
            );
            let error = result.expect_err("should return validation error");
            assert!(
                matches!(
                    error,
                    Error::Validation(ValidationError::InvalidReorg { .. })
                ),
                "Expected ValidationError::InvalidReorg, got {:?}",
                error
            );
        }

        #[test]
        fn reorg_extending_beyond_previous_endpoint_succeeds() {
            //* Given
            let previous = vec![test_range_with_prev("eth", 0, 100, 100, None)];
            // Reorg to block 50, but extends to 200 (new chain has more blocks)
            let mut reorg_hash = [0u8; 32];
            reorg_hash[31] = 50;
            let incoming = vec![test_range_with_prev(
                "eth",
                50,
                200,
                200,
                Some(BlockHash::from_slice(&reorg_hash)),
            )];

            //* When
            let result = validate_consecutiveness(&previous, &incoming);

            //* Then
            assert!(
                result.is_ok(),
                "validation should succeed with reorg extending beyond previous endpoint"
            );
        }
    }

    mod validate_prev_hash {
        use super::*;

        #[test]
        fn genesis_without_prev_hash_succeeds() {
            //* Given
            let range = test_range("eth", 0, 10, 1);

            //* When
            let result = validate_prev_hash(&range);

            //* Then
            assert!(
                result.is_ok(),
                "validation should succeed for genesis without prev_hash"
            );
        }

        #[test]
        fn genesis_with_prev_hash_fails() {
            //* Given
            let mut non_zero_hash = [0u8; 32];
            non_zero_hash[31] = 1;
            let range =
                test_range_with_prev("eth", 0, 10, 1, Some(BlockHash::from_slice(&non_zero_hash)));

            //* When
            let result = validate_prev_hash(&range);

            //* Then
            assert!(
                result.is_err(),
                "validation should fail for genesis with prev_hash"
            );
            let error = result.expect_err("should return validation error");
            assert!(
                matches!(error, Error::Validation(ValidationError::InvalidPrevHash)),
                "Expected ValidationError::InvalidPrevHash, got {:?}",
                error
            );
        }

        #[test]
        fn non_genesis_with_prev_hash_succeeds() {
            //* Given
            let mut non_zero_hash = [0u8; 32];
            non_zero_hash[31] = 1;
            let range = test_range_with_prev(
                "eth",
                100,
                110,
                1,
                Some(BlockHash::from_slice(&non_zero_hash)),
            );

            //* When
            let result = validate_prev_hash(&range);

            //* Then
            assert!(
                result.is_ok(),
                "validation should succeed for non-genesis block with prev_hash"
            );
        }

        #[test]
        fn non_genesis_without_prev_hash_fails() {
            //* Given
            let range = test_range("eth", 100, 110, 1);

            //* When
            let result = validate_prev_hash(&range);

            //* Then
            assert!(
                result.is_err(),
                "validation should fail for non-genesis block without prev_hash"
            );
            let error = result.expect_err("should return validation error");
            assert!(
                matches!(
                    error,
                    Error::Validation(ValidationError::MissingPrevHash { .. })
                ),
                "Expected ValidationError::MissingPrevHash, got {:?}",
                error
            );
        }

        #[test]
        fn reorg_to_genesis_succeeds() {
            //* Given - reorg back to genesis (no prev_hash)
            let range = test_range("eth", 0, 10, 1);

            //* When
            let result = validate_prev_hash(&range);

            //* Then
            assert!(
                result.is_ok(),
                "validation should succeed for reorg to genesis (no prev_hash)"
            );
        }
    }
}
