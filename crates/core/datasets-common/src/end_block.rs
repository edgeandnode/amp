//! End block configuration for dataset dump operations.
//!
//! Defines when a dump should stop processing blocks, with support for absolute
//! block numbers, relative offsets from latest, and continuous (never-stop) mode.

use crate::block_num::BlockNum;

/// End block configuration for dump operations.
///
/// Defines when a dump should stop processing blocks:
/// - `None`: Continuous dumping (never stops)
/// - `Latest`: Stop at the latest available block
/// - `Absolute(N)`: Stop at specific block number N
/// - `LatestMinus(N)`: Stop at latest block - N (e.g., 100 means latest - 100)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub enum EndBlock {
    /// Continuous dumping - never stops
    #[default]
    None,
    /// Stop at the latest available block
    Latest,
    /// Stop at a specific block number
    Absolute(BlockNum),
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
            s => {
                // Try parsing as integer
                match s.parse::<i64>() {
                    Ok(n) if n >= 0 => Ok(EndBlock::Absolute(n as u64)),
                    Ok(n) if n < 0 => {
                        // Convert negative to positive offset
                        Ok(EndBlock::LatestMinus((-n) as u64))
                    }
                    _ => Err(InvalidEndBlockError {
                        input: s.to_string(),
                    }),
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

/// Error returned when parsing an [`EndBlock`] from a string fails.
#[derive(Debug, thiserror::Error)]
#[error("invalid end block value: '{input}' (expected 'latest' or an integer)")]
pub struct InvalidEndBlockError {
    /// The input string that could not be parsed.
    pub input: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_str_with_latest_returns_latest() {
        //* Given
        let input = "latest";

        //* When
        let result: Result<EndBlock, _> = input.parse();

        //* Then
        assert!(result.is_ok(), "parsing 'latest' should succeed");
        let end_block = result.expect("should return valid EndBlock");
        assert_eq!(end_block, EndBlock::Latest);
    }

    #[test]
    fn from_str_with_positive_integer_returns_absolute() {
        //* Given
        let input = "100";

        //* When
        let result: Result<EndBlock, _> = input.parse();

        //* Then
        assert!(
            result.is_ok(),
            "parsing positive integer should succeed and return Absolute"
        );
        let end_block = result.expect("should return valid EndBlock");
        assert_eq!(end_block, EndBlock::Absolute(100));
    }

    #[test]
    fn from_str_with_zero_returns_absolute_zero() {
        //* Given
        let input = "0";

        //* When
        let result: Result<EndBlock, _> = input.parse();

        //* Then
        assert!(result.is_ok(), "parsing zero should succeed");
        let end_block = result.expect("should return valid EndBlock");
        assert_eq!(end_block, EndBlock::Absolute(0));
    }

    #[test]
    fn from_str_with_negative_integer_returns_latest_minus() {
        //* Given
        let input = "-50";

        //* When
        let result: Result<EndBlock, _> = input.parse();

        //* Then
        assert!(
            result.is_ok(),
            "parsing negative integer should succeed and return LatestMinus"
        );
        let end_block = result.expect("should return valid EndBlock");
        assert_eq!(end_block, EndBlock::LatestMinus(50));
    }

    #[test]
    fn from_str_with_non_numeric_string_fails() {
        //* Given
        let input = "invalid";

        //* When
        let result: Result<EndBlock, _> = input.parse();

        //* Then
        assert!(
            result.is_err(),
            "parsing non-numeric string should fail with error"
        );
    }

    #[test]
    fn from_str_with_non_numeric_string_returns_invalid_end_block_error() {
        //* Given
        let input = "invalid";

        //* When
        let result: Result<EndBlock, _> = input.parse();

        //* Then
        let err = result.expect_err("should return InvalidEndBlockError");
        assert!(
            matches!(err, InvalidEndBlockError { .. }),
            "error should be InvalidEndBlockError variant"
        );
        assert!(
            err.to_string().contains("invalid end block value"),
            "error message should describe the failure"
        );
        assert!(
            err.to_string().contains("invalid"),
            "error message should include the invalid input"
        );
    }

    #[test]
    fn display_with_none_shows_none() {
        //* Given
        let end_block = EndBlock::None;

        //* When
        let result = end_block.to_string();

        //* Then
        assert_eq!(result, "none", "None should display as 'none'");
    }

    #[test]
    fn display_with_latest_shows_latest() {
        //* Given
        let end_block = EndBlock::Latest;

        //* When
        let result = end_block.to_string();

        //* Then
        assert_eq!(result, "latest", "Latest should display as 'latest'");
    }

    #[test]
    fn display_with_absolute_shows_number() {
        //* Given
        let end_block = EndBlock::Absolute(100);

        //* When
        let result = end_block.to_string();

        //* Then
        assert_eq!(result, "100", "Absolute(100) should display as '100'");
    }

    #[test]
    fn display_with_latest_minus_shows_negative() {
        //* Given
        let end_block = EndBlock::LatestMinus(50);

        //* When
        let result = end_block.to_string();

        //* Then
        assert_eq!(result, "-50", "LatestMinus(50) should display as '-50'");
    }

    #[test]
    fn serialize_with_none_produces_null() {
        //* Given
        let end_block = EndBlock::None;

        //* When
        let json = serde_json::to_string(&end_block)
            .expect("serialization should succeed with valid EndBlock");

        //* Then
        assert_eq!(json, "null", "None should serialize to JSON null");
    }

    #[test]
    fn serialize_with_latest_produces_quoted_latest() {
        //* Given
        let end_block = EndBlock::Latest;

        //* When
        let json = serde_json::to_string(&end_block)
            .expect("serialization should succeed with valid EndBlock");

        //* Then
        assert_eq!(
            json, "\"latest\"",
            "Latest should serialize to quoted 'latest'"
        );
    }

    #[test]
    fn serialize_with_absolute_produces_quoted_number() {
        //* Given
        let end_block = EndBlock::Absolute(100);

        //* When
        let json = serde_json::to_string(&end_block)
            .expect("serialization should succeed with valid EndBlock");

        //* Then
        assert_eq!(
            json, "\"100\"",
            "Absolute(100) should serialize to quoted '100'"
        );
    }

    #[test]
    fn serialize_with_latest_minus_produces_quoted_negative() {
        //* Given
        let end_block = EndBlock::LatestMinus(50);

        //* When
        let json = serde_json::to_string(&end_block)
            .expect("serialization should succeed with valid EndBlock");

        //* Then
        assert_eq!(
            json, "\"-50\"",
            "LatestMinus(50) should serialize to quoted '-50'"
        );
    }

    #[test]
    fn deserialize_with_null_returns_none() {
        //* Given
        let json = "null";

        //* When
        let result: Result<EndBlock, _> = serde_json::from_str(json);

        //* Then
        assert!(
            result.is_ok(),
            "deserializing null should succeed and return None"
        );
        let end_block = result.expect("should return valid EndBlock");
        assert_eq!(end_block, EndBlock::None);
    }

    #[test]
    fn deserialize_with_quoted_latest_returns_latest() {
        //* Given
        let json = "\"latest\"";

        //* When
        let result: Result<EndBlock, _> = serde_json::from_str(json);

        //* Then
        assert!(
            result.is_ok(),
            "deserializing quoted 'latest' should succeed and return Latest"
        );
        let end_block = result.expect("should return valid EndBlock");
        assert_eq!(end_block, EndBlock::Latest);
    }

    #[test]
    fn deserialize_with_quoted_number_returns_absolute() {
        //* Given
        let json = "\"100\"";

        //* When
        let result: Result<EndBlock, _> = serde_json::from_str(json);

        //* Then
        assert!(
            result.is_ok(),
            "deserializing quoted number should succeed and return Absolute"
        );
        let end_block = result.expect("should return valid EndBlock");
        assert_eq!(end_block, EndBlock::Absolute(100));
    }

    #[test]
    fn deserialize_with_quoted_negative_returns_latest_minus() {
        //* Given
        let json = "\"-50\"";

        //* When
        let result: Result<EndBlock, _> = serde_json::from_str(json);

        //* Then
        assert!(
            result.is_ok(),
            "deserializing quoted negative should succeed and return LatestMinus"
        );
        let end_block = result.expect("should return valid EndBlock");
        assert_eq!(end_block, EndBlock::LatestMinus(50));
    }
}
