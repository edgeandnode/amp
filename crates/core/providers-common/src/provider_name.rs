/// A validated provider name enforcing kebab-case invariant.
///
/// Provider names must:
/// - Be non-empty
/// - Contain only lowercase alphanumeric characters and hyphens
/// - Not start or end with a hyphen
/// - Not contain consecutive hyphens
///
/// Provider names are derived from config file names (e.g., `evm-mainnet.toml` → `evm-mainnet`).
/// At query time, provider names are normalized to snake_case for SQL catalog matching.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ProviderName(String);

impl ProviderName {
    /// Create a new ProviderName, validating kebab-case format.
    ///
    /// Returns an error if the name doesn't meet the kebab-case requirements.
    pub fn new(name: &str) -> Result<Self, InvalidProviderName> {
        // Must be non-empty
        if name.is_empty() {
            return Err(InvalidProviderName::Empty);
        }

        // Must contain only lowercase alphanumeric and hyphens
        if !name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        {
            return Err(InvalidProviderName::InvalidCharacters {
                name: name.to_string(),
            });
        }

        // Must not start or end with hyphen
        if name.starts_with('-') || name.ends_with('-') {
            return Err(InvalidProviderName::InvalidBoundaryHyphen {
                name: name.to_string(),
            });
        }

        // Must not contain consecutive hyphens
        if name.contains("--") {
            return Err(InvalidProviderName::ConsecutiveHyphens {
                name: name.to_string(),
            });
        }

        Ok(Self(name.to_string()))
    }

    /// Get the provider name as a string reference.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Convert the kebab-case provider name to snake_case for SQL catalog matching.
    ///
    /// This is used at query time to match catalog names in SQL queries.
    ///
    /// Example: `evm-mainnet` → `evm_mainnet`
    pub fn to_sql_name(&self) -> String {
        self.0.replace('-', "_")
    }
}

impl std::fmt::Display for ProviderName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Error that can occur when validating a provider name.
#[derive(Debug, thiserror::Error)]
pub enum InvalidProviderName {
    /// Provider name is empty
    ///
    /// This occurs when attempting to create a provider name from an empty string.
    #[error("Provider name cannot be empty")]
    Empty,

    /// Provider name contains invalid characters (must be lowercase alphanumeric + hyphens)
    ///
    /// This occurs when the provider name contains characters other than lowercase
    /// letters (a-z), digits (0-9), or hyphens (-). Uppercase letters, underscores,
    /// spaces, and special characters are not allowed.
    #[error(
        "Provider name '{name}' contains invalid characters (must be lowercase alphanumeric and hyphens only)"
    )]
    InvalidCharacters { name: String },

    /// Provider name starts or ends with a hyphen
    ///
    /// This occurs when the provider name has a hyphen as the first or last character.
    /// Hyphens are only allowed between alphanumeric segments.
    #[error("Provider name '{name}' cannot start or end with a hyphen")]
    InvalidBoundaryHyphen { name: String },

    /// Provider name contains consecutive hyphens
    ///
    /// This occurs when the provider name has two or more hyphens in a row.
    /// Only single hyphens are allowed as separators between alphanumeric segments.
    #[error("Provider name '{name}' cannot contain consecutive hyphens")]
    ConsecutiveHyphens { name: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_with_valid_kebab_case_succeeds() {
        //* Given
        let input = "evm-mainnet";

        //* When
        let result = ProviderName::new(input);

        //* Then
        assert!(result.is_ok(), "should accept valid kebab-case name");
        let name = result.expect("should return ProviderName");
        assert_eq!(name.as_str(), "evm-mainnet");
    }

    #[test]
    fn new_with_single_word_lowercase_succeeds() {
        //* Given
        let input = "drpc";

        //* When
        let result = ProviderName::new(input);

        //* Then
        assert!(result.is_ok(), "should accept single-word lowercase name");
        let name = result.expect("should return ProviderName");
        assert_eq!(name.as_str(), "drpc");
    }

    #[test]
    fn new_with_kebab_case_including_digits_succeeds() {
        //* Given
        let input = "evm-base-2";

        //* When
        let result = ProviderName::new(input);

        //* Then
        assert!(result.is_ok(), "should accept kebab-case with digits");
        let name = result.expect("should return ProviderName");
        assert_eq!(name.as_str(), "evm-base-2");
    }

    #[test]
    fn new_with_single_character_succeeds() {
        //* Given
        let input = "a";

        //* When
        let result = ProviderName::new(input);

        //* Then
        assert!(result.is_ok(), "should accept single-character name");
        let name = result.expect("should return ProviderName");
        assert_eq!(name.as_str(), "a");
    }

    #[test]
    fn new_with_multiple_hyphens_succeeds() {
        //* Given
        let input = "a-b-c-d";

        //* When
        let result = ProviderName::new(input);

        //* Then
        assert!(result.is_ok(), "should accept names with multiple hyphens");
        let name = result.expect("should return ProviderName");
        assert_eq!(name.as_str(), "a-b-c-d");
    }

    #[test]
    fn new_with_empty_string_fails() {
        //* When
        let result = ProviderName::new("");

        //* Then
        assert!(result.is_err(), "should reject empty string");
        let error = result.expect_err("should return InvalidProviderName error");
        assert!(
            matches!(error, InvalidProviderName::Empty),
            "expected Empty error, got {:?}",
            error
        );
    }

    #[test]
    fn new_with_uppercase_letters_fails() {
        //* Given
        let input = "Evm-Mainnet";

        //* When
        let result = ProviderName::new(input);

        //* Then
        assert!(result.is_err(), "should reject uppercase letters");
        let error = result.expect_err("should return InvalidProviderName error");
        assert!(
            matches!(error, InvalidProviderName::InvalidCharacters { .. }),
            "expected InvalidCharacters error, got {:?}",
            error
        );
    }

    #[test]
    fn new_with_underscores_fails() {
        //* Given
        let input = "evm_mainnet";

        //* When
        let result = ProviderName::new(input);

        //* Then
        assert!(result.is_err(), "should reject underscores");
        let error = result.expect_err("should return InvalidProviderName error");
        assert!(
            matches!(error, InvalidProviderName::InvalidCharacters { .. }),
            "expected InvalidCharacters error, got {:?}",
            error
        );
    }

    #[test]
    fn new_with_spaces_fails() {
        //* Given
        let input = "evm mainnet";

        //* When
        let result = ProviderName::new(input);

        //* Then
        assert!(result.is_err(), "should reject spaces");
        let error = result.expect_err("should return InvalidProviderName error");
        assert!(
            matches!(error, InvalidProviderName::InvalidCharacters { .. }),
            "expected InvalidCharacters error, got {:?}",
            error
        );
    }

    #[test]
    fn new_with_dots_fails() {
        //* Given
        let input = "evm.mainnet";

        //* When
        let result = ProviderName::new(input);

        //* Then
        assert!(result.is_err(), "should reject dots");
        let error = result.expect_err("should return InvalidProviderName error");
        assert!(
            matches!(error, InvalidProviderName::InvalidCharacters { .. }),
            "expected InvalidCharacters error, got {:?}",
            error
        );
    }

    #[test]
    fn new_with_leading_hyphen_fails() {
        //* Given
        let input = "-evm-mainnet";

        //* When
        let result = ProviderName::new(input);

        //* Then
        assert!(result.is_err(), "should reject leading hyphen");
        let error = result.expect_err("should return InvalidProviderName error");
        assert!(
            matches!(error, InvalidProviderName::InvalidBoundaryHyphen { .. }),
            "expected InvalidBoundaryHyphen error, got {:?}",
            error
        );
    }

    #[test]
    fn new_with_trailing_hyphen_fails() {
        //* Given
        let input = "evm-mainnet-";

        //* When
        let result = ProviderName::new(input);

        //* Then
        assert!(result.is_err(), "should reject trailing hyphen");
        let error = result.expect_err("should return InvalidProviderName error");
        assert!(
            matches!(error, InvalidProviderName::InvalidBoundaryHyphen { .. }),
            "expected InvalidBoundaryHyphen error, got {:?}",
            error
        );
    }

    #[test]
    fn new_with_double_hyphens_fails() {
        //* Given
        let input = "evm--mainnet";

        //* When
        let result = ProviderName::new(input);

        //* Then
        assert!(result.is_err(), "should reject consecutive hyphens");
        let error = result.expect_err("should return InvalidProviderName error");
        assert!(
            matches!(error, InvalidProviderName::ConsecutiveHyphens { .. }),
            "expected ConsecutiveHyphens error, got {:?}",
            error
        );
    }

    #[test]
    fn new_with_triple_hyphens_fails() {
        //* Given
        let input = "evm---mainnet";

        //* When
        let result = ProviderName::new(input);

        //* Then
        assert!(result.is_err(), "should reject triple consecutive hyphens");
        let error = result.expect_err("should return InvalidProviderName error");
        assert!(
            matches!(error, InvalidProviderName::ConsecutiveHyphens { .. }),
            "expected ConsecutiveHyphens error, got {:?}",
            error
        );
    }

    #[test]
    fn to_sql_name_with_hyphens_converts_to_underscores() {
        //* Given
        let name = ProviderName::new("evm-mainnet").expect("should create valid ProviderName");

        //* When
        let result = name.to_sql_name();

        //* Then
        assert_eq!(
            result, "evm_mainnet",
            "hyphens should be converted to underscores"
        );
    }

    #[test]
    fn to_sql_name_without_hyphens_preserves_name() {
        //* Given
        let name = ProviderName::new("drpc").expect("should create valid ProviderName");

        //* When
        let result = name.to_sql_name();

        //* Then
        assert_eq!(
            result, "drpc",
            "name without hyphens should remain unchanged"
        );
    }

    #[test]
    fn to_sql_name_with_multiple_hyphens_converts_all() {
        //* Given
        let name = ProviderName::new("a-b-c").expect("should create valid ProviderName");

        //* When
        let result = name.to_sql_name();

        //* Then
        assert_eq!(
            result, "a_b_c",
            "all hyphens should be converted to underscores"
        );
    }
}
