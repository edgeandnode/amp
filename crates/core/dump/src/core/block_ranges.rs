use std::future::Future;

use common::{BlockNum, BoxError};
use tracing::instrument;

/// End block configuration for dump operations.
///
/// Defines when a dump should stop processing blocks:
/// - `None`: Continuous dumping (never stops)
/// - `Latest`: Stop at the latest available block
/// - `Absolute(N)`: Stop at specific block number N
/// - `LatestMinus(N)`: Stop at latest block - N (e.g., 100 means latest - 100)
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EndBlock {
    /// Continuous dumping - never stops
    None,
    /// Stop at the latest available block
    Latest,
    /// Stop at a specific block number
    Absolute(u64),
    /// Stop N blocks before the latest block
    LatestMinus(u64),
}

impl EndBlock {
    /// Resolves the end block to a concrete block number, if applicable.
    ///
    /// Returns `None` for continuous dumping mode (`EndBlock::None`).
    /// For other variants, resolves to a concrete block number using the provided
    /// `get_latest` function when needed.
    ///
    /// # Arguments
    /// * `start` - The start block number, used for validation
    /// * `get_latest` - Async function to fetch the latest block number
    ///
    /// # Returns
    /// * `Ok(None)` - For continuous mode
    /// * `Ok(Some(block))` - Resolved concrete block number
    /// * `Err(_)` - If resolution fails or validation fails
    #[instrument(skip(get_latest), err)]
    pub async fn resolve<F, Fut>(
        &self,
        start: BlockNum,
        get_latest: F,
    ) -> Result<Option<BlockNum>, ResolutionError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<BlockNum, BoxError>>,
    {
        match self {
            EndBlock::None => Ok(None),
            EndBlock::Latest => {
                let latest = get_latest()
                    .await
                    .map_err(|e| ResolutionError::FetchLatestFailed(e.to_string()))?;
                Ok(Some(latest))
            }
            EndBlock::Absolute(n) => {
                let block = *n;
                if block < start {
                    return Err(ResolutionError::invalid_end_block(start, block));
                }
                Ok(Some(block))
            }
            EndBlock::LatestMinus(offset) => {
                let latest = get_latest()
                    .await
                    .map_err(|e| ResolutionError::FetchLatestFailed(e.to_string()))?;
                // Subtract offset from latest (offset is always positive)
                let resolved = resolve_relative(start, Some(-(*offset as i64)), latest)?;
                Ok(Some(resolved))
            }
        }
    }
}

impl std::fmt::Display for EndBlock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EndBlock::None => write!(f, "none"),
            EndBlock::Latest => write!(f, "latest"),
            EndBlock::Absolute(n) => write!(f, "{}", n),
            EndBlock::LatestMinus(n) => write!(f, "-{}", n),
        }
    }
}

impl std::str::FromStr for EndBlock {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "latest" => Ok(EndBlock::Latest),
            s => {
                // Try parsing as integer
                match s.parse::<i64>() {
                    Ok(n) if n >= 0 => Ok(EndBlock::Absolute(n as u64)),
                    Ok(n) if n < 0 => {
                        // Convert negative to positive offset
                        Ok(EndBlock::LatestMinus((-n) as u64))
                    }
                    _ => Err(format!("invalid end block value: '{}'", s)),
                }
            }
        }
    }
}

impl serde::Serialize for EndBlock {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            EndBlock::None => serializer.serialize_none(),
            _ => serializer.serialize_str(&self.to_string()),
        }
    }
}

impl<'de> serde::Deserialize<'de> for EndBlock {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let opt: Option<String> = Option::deserialize(deserializer)?;
        match opt {
            None => Ok(EndBlock::None),
            Some(s) => s.parse().map_err(serde::de::Error::custom),
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
    use super::{EndBlock, ResolutionError, resolve_relative};

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

    #[test]
    fn end_block_from_str() {
        assert_eq!("latest".parse::<EndBlock>().unwrap(), EndBlock::Latest);
        assert_eq!("100".parse::<EndBlock>().unwrap(), EndBlock::Absolute(100));
        assert_eq!("0".parse::<EndBlock>().unwrap(), EndBlock::Absolute(0));
        assert_eq!(
            "-50".parse::<EndBlock>().unwrap(),
            EndBlock::LatestMinus(50)
        );
        assert!("invalid".parse::<EndBlock>().is_err());
    }

    #[test]
    fn end_block_display() {
        assert_eq!(EndBlock::None.to_string(), "none");
        assert_eq!(EndBlock::Latest.to_string(), "latest");
        assert_eq!(EndBlock::Absolute(100).to_string(), "100");
        assert_eq!(EndBlock::LatestMinus(50).to_string(), "-50");
    }

    #[tokio::test]
    async fn end_block_resolve_none() {
        let end_block = EndBlock::None;
        let result = end_block
            .resolve(0, || async { Ok::<u64, common::BoxError>(100) })
            .await
            .unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn end_block_resolve_latest() {
        let end_block = EndBlock::Latest;
        let result = end_block
            .resolve(0, || async { Ok::<u64, common::BoxError>(100) })
            .await
            .unwrap();
        assert_eq!(result, Some(100));
    }

    #[tokio::test]
    async fn end_block_resolve_absolute() {
        let end_block = EndBlock::Absolute(100);
        let result = end_block
            .resolve(0, || async { Ok::<u64, common::BoxError>(200) })
            .await
            .unwrap();
        assert_eq!(result, Some(100));
    }

    #[tokio::test]
    async fn end_block_resolve_latest_minus() {
        let end_block = EndBlock::LatestMinus(50);
        let result = end_block
            .resolve(0, || async { Ok::<u64, common::BoxError>(100) })
            .await
            .unwrap();
        assert_eq!(result, Some(50));
    }

    #[tokio::test]
    async fn end_block_resolve_absolute_validation() {
        let end_block = EndBlock::Absolute(10);
        let result = end_block
            .resolve(50, || async { Ok::<u64, common::BoxError>(100) })
            .await;
        assert!(result.is_err());
    }

    #[test]
    fn end_block_serde_none() {
        let end_block = EndBlock::None;
        let json = serde_json::to_string(&end_block).unwrap();
        assert_eq!(json, "null");
        let deserialized: EndBlock = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, EndBlock::None);
    }

    #[test]
    fn end_block_serde_latest() {
        let end_block = EndBlock::Latest;
        let json = serde_json::to_string(&end_block).unwrap();
        assert_eq!(json, "\"latest\"");
        let deserialized: EndBlock = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, EndBlock::Latest);
    }

    #[test]
    fn end_block_serde_absolute() {
        let end_block = EndBlock::Absolute(100);
        let json = serde_json::to_string(&end_block).unwrap();
        assert_eq!(json, "\"100\"");
        let deserialized: EndBlock = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, EndBlock::Absolute(100));
    }

    #[test]
    fn end_block_serde_latest_minus() {
        let end_block = EndBlock::LatestMinus(50);
        let json = serde_json::to_string(&end_block).unwrap();
        assert_eq!(json, "\"-50\"");
        let deserialized: EndBlock = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, EndBlock::LatestMinus(50));
    }
}
