use common::BlockNum;

/// Resolves the end block number, potentially relative to the latest block.
///
/// If the end block number is negative, it is considered relative to the
/// `latest_block`. Otherwise, it is treated as an absolute block number.
/// The end block defaults to `latest_block` if not provided.
///
/// Returns an error if the resolved end block is less than the start block.
pub fn resolve_relative(
    start: BlockNum,
    end: Option<i64>,
    latest_block: BlockNum,
) -> Result<BlockNum, ResolutionError> {
    let end = match end {
        None => latest_block,
        Some(n) if n >= 0 => n as BlockNum,
        Some(n) => (latest_block as i64 + n) as BlockNum,
    };
    if end < start {
        return Err(ResolutionError::invalid_end_block(start, end));
    }
    Ok(end)
}

/// Errors that can occur when resolving a block range
#[derive(Debug, thiserror::Error)]
pub enum ResolutionError {
    /// The end block is less than the start block
    #[error("end block ({end_block}) is less than start block ({start_block})")]
    InvalidEndBlock {
        start_block: BlockNum,
        end_block: BlockNum,
    },
}

impl ResolutionError {
    fn invalid_end_block(start_block: BlockNum, end_block: BlockNum) -> Self {
        Self::InvalidEndBlock {
            start_block,
            end_block,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ResolutionError, resolve_relative};

    #[test]
    fn resolve_block_range_variants() {
        //* Params
        let latest_block = 100;
        let test_cases = [
            (50, None, Ok(100)),
            (100, Some(150), Ok(150)),
            // Overlapped (end < start)
            (
                70,
                Some(-50),
                Err(ResolutionError::invalid_end_block(70, 50)),
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
