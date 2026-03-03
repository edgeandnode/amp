//! End block configuration for dataset deployment operations.
//!
//! Defines when a deployment should stop processing blocks, with support for absolute
//! block numbers, relative offsets from latest, and continuous (never-stop) mode.

/// End block configuration for deployment operations.
///
/// Defines when a deployment should stop processing blocks:
/// - `None`: Continuous syncing (never stops)
/// - `Latest`: Stop at the latest available block
/// - `Absolute(N)`: Stop at specific block number N
/// - `LatestMinus(N)`: Stop at latest block - N (e.g., 100 means latest - 100)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub enum EndBlock {
    /// Continuous syncing - never stops
    #[default]
    None,
    /// Stop at the latest available block
    Latest,
    /// Stop at a specific block number
    Absolute(u64),
    /// Stop N blocks before the latest block
    LatestMinus(u64),
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
    type Err = InvalidEndBlockError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "latest" => Ok(EndBlock::Latest),
            s => match s.parse::<i64>() {
                Ok(n) if n >= 0 => Ok(EndBlock::Absolute(n as u64)),
                Ok(n) if n < 0 => Ok(EndBlock::LatestMinus((-n) as u64)),
                _ => Err(InvalidEndBlockError {
                    input: s.to_string(),
                }),
            },
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

/// Error returned when parsing an [`EndBlock`] from a string fails.
#[derive(Debug, thiserror::Error)]
#[error("invalid end block value: '{input}' (expected 'latest' or an integer)")]
pub struct InvalidEndBlockError {
    /// The input string that could not be parsed.
    pub input: String,
}
