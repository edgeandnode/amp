use std::future::Future;

use datasets_common::block_num::BlockNum;
pub use datasets_common::end_block::EndBlock;

/// Result of resolving an EndBlock configuration.
///
/// After resolution, the end block specification becomes one of:
/// - `Continuous`: Never stop dumping (from `EndBlock::None`)
/// - `Block(N)`: Stop at concrete block number N
/// - `NoDataAvailable`: Dependencies have no data available
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolvedEndBlock {
    /// Continuous dumping - never stops
    Continuous,
    /// Stop at this specific block number
    Block(BlockNum),
    /// No data available from dependencies
    NoDataAvailable,
}

/// Error type for the `get_latest` future in [`resolve_end_block`].
pub type GetLatestBlockError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Resolves an [`EndBlock`] configuration to a concrete [`ResolvedEndBlock`].
///
/// For `EndBlock::Latest` and `EndBlock::LatestMinus`, calls `get_latest` to
/// fetch the current chain tip. Returns `NoDataAvailable` when dependencies
/// have no data, or an error if the resolved block is before `start`.
#[tracing::instrument(skip(get_latest), err)]
pub async fn resolve_end_block<F>(
    end_block: &EndBlock,
    start: BlockNum,
    get_latest: F,
) -> Result<ResolvedEndBlock, ResolutionError>
where
    F: Future<Output = Result<Option<BlockNum>, GetLatestBlockError>>,
{
    match end_block {
        EndBlock::None => Ok(ResolvedEndBlock::Continuous),
        EndBlock::Latest => {
            let latest = get_latest
                .await
                .map_err(|err| ResolutionError::FetchLatestFailed(err.to_string()))?;
            match latest {
                Some(block) => Ok(ResolvedEndBlock::Block(block)),
                None => Ok(ResolvedEndBlock::NoDataAvailable),
            }
        }
        EndBlock::Absolute(n) => {
            let block = *n;
            if block < start {
                return Err(ResolutionError::invalid_end_block(start, block));
            }
            Ok(ResolvedEndBlock::Block(block))
        }
        EndBlock::LatestMinus(offset) => {
            let latest = get_latest
                .await
                .map_err(|err| ResolutionError::FetchLatestFailed(err.to_string()))?;
            match latest {
                Some(latest_block) => {
                    // Subtract offset from latest (offset is always positive)
                    let resolved = resolve_relative(start, Some(-(*offset as i64)), latest_block)?;
                    Ok(ResolvedEndBlock::Block(resolved))
                }
                None => Ok(ResolvedEndBlock::NoDataAvailable),
            }
        }
    }
}

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
    /// Failed to fetch the latest block number
    #[error("failed to fetch latest block: {0}")]
    FetchLatestFailed(String),
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
    use super::{
        EndBlock, GetLatestBlockError, ResolutionError, ResolvedEndBlock, resolve_end_block,
        resolve_relative,
    };

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

    #[tokio::test]
    async fn end_block_resolve_none() {
        let end_block = EndBlock::None;
        let result = resolve_end_block(&end_block, 0, async {
            Ok::<Option<u64>, GetLatestBlockError>(Some(100))
        })
        .await
        .unwrap();
        assert_eq!(result, ResolvedEndBlock::Continuous);
    }

    #[tokio::test]
    async fn end_block_resolve_latest() {
        let end_block = EndBlock::Latest;
        let result = resolve_end_block(&end_block, 0, async {
            Ok::<Option<u64>, GetLatestBlockError>(Some(100))
        })
        .await
        .unwrap();
        assert_eq!(result, ResolvedEndBlock::Block(100));
    }

    #[tokio::test]
    async fn end_block_resolve_absolute() {
        let end_block = EndBlock::Absolute(100);
        let result = resolve_end_block(&end_block, 0, async {
            Ok::<Option<u64>, GetLatestBlockError>(Some(200))
        })
        .await
        .unwrap();
        assert_eq!(result, ResolvedEndBlock::Block(100));
    }

    #[tokio::test]
    async fn end_block_resolve_latest_minus() {
        let end_block = EndBlock::LatestMinus(50);
        let result = resolve_end_block(&end_block, 0, async {
            Ok::<Option<u64>, GetLatestBlockError>(Some(100))
        })
        .await
        .unwrap();
        assert_eq!(result, ResolvedEndBlock::Block(50));
    }

    #[tokio::test]
    async fn end_block_resolve_absolute_validation() {
        let end_block = EndBlock::Absolute(10);
        let result = resolve_end_block(&end_block, 50, async {
            Ok::<Option<u64>, GetLatestBlockError>(Some(100))
        })
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn end_block_resolve_latest_no_data() {
        let end_block = EndBlock::Latest;
        let result = resolve_end_block(&end_block, 0, async {
            Ok::<Option<u64>, GetLatestBlockError>(None)
        })
        .await
        .unwrap();
        assert_eq!(result, ResolvedEndBlock::NoDataAvailable);
    }

    #[tokio::test]
    async fn end_block_resolve_latest_minus_no_data() {
        let end_block = EndBlock::LatestMinus(50);
        let result = resolve_end_block(&end_block, 0, async {
            Ok::<Option<u64>, GetLatestBlockError>(None)
        })
        .await
        .unwrap();
        assert_eq!(result, ResolvedEndBlock::NoDataAvailable);
    }
}
