use common::BlockNum;

/// Resolves start and end block numbers, potentially relative to the latest block
///
/// If start or end block numbers are negative, they are considered relative to the
/// `latest_block`. Otherwise, they are treated as absolute block numbers.
/// The end block defaults to `latest_block` if not provided.
///
/// Returns an error if the resolved start block is negative or if the resolved end block is less
/// than the resolved start block.
pub fn resolve_relative(
    start: i64,
    end: Option<i64>,
    latest_block: BlockNum,
) -> Result<(BlockNum, BlockNum), ResolutionError> {
    if start >= 0 {
        if let Some(end) = end {
            if end > 0 {
                return Ok((start as BlockNum, end as BlockNum));
            }
        }
    }

    let start_block = if start >= 0 {
        start
    } else {
        latest_block as i64 + start // Using + because start is negative
    };

    if start_block < 0 {
        return Err(ResolutionError::invalid_start_block(start_block));
    }

    let end_block = match end {
        // Absolute block number
        Some(e) if e > 0 => e as BlockNum,

        // Relative to latest block
        Some(e) => {
            let end = latest_block as i64 + e; // Using + because e is negative
            if end < start_block {
                return Err(ResolutionError::invalid_end_block(start_block, end));
            }
            end as BlockNum
        }

        // Default to latest block
        None => latest_block,
    };

    Ok((start_block as BlockNum, end_block))
}

/// Errors that can occur when resolving a block range
#[derive(Debug, thiserror::Error)]
pub enum ResolutionError {
    /// The start block is negative
    #[error("start block ({start_block}) is invalid")]
    InvalidStartBlock { start_block: i64 },

    /// The end block is less than the start block
    #[error("end block ({end_block}) is less than start block ({start_block})")]
    InvalidEndBlock { start_block: i64, end_block: i64 },
}

impl ResolutionError {
    fn invalid_start_block(start_block: i64) -> Self {
        Self::InvalidStartBlock { start_block }
    }

    fn invalid_end_block(start_block: i64, end_block: i64) -> Self {
        Self::InvalidEndBlock {
            start_block,
            end_block,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{resolve_relative, ResolutionError};

    #[test]
    fn resolve_block_range_variants() {
        //* Params
        let latest_block = 100;
        let test_cases = [
            (50, None, Ok((50, 100))),
            (-80, None, Ok((20, 100))),
            (100, Some(150), Ok((100, 150))),
            (-80, Some(-10), Ok((20, 90))),
            // Overlapped (end < start)
            (
                70,
                Some(-50),
                Err(ResolutionError::invalid_end_block(70, -50)),
            ),
        ];

        //* Test
        for (start_block, end_block, expected) in test_cases {
            match resolve_relative(start_block, end_block, latest_block) {
                Ok(result) => assert_eq!(expected.unwrap(), result),
                Err(_) => assert!(expected.is_err()),
            }
        }
    }
}
