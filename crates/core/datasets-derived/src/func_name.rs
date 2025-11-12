//! UDF function name validation and types.
//!
//! This module provides the `FuncName` type for validated UDF function names that enforce
//! naming conventions compatible with DataFusion UDFs.

/// Maximum length for function identifiers (practical limit for reasonable function names).
const MAX_IDENTIFIER_LENGTH: usize = 255;

/// A validated UDF function name that enforces naming conventions.
///
/// Function names must follow identifier rules compatible with DataFusion UDFs.
///
/// ## Format Requirements
///
/// A valid function name must:
/// - **Start** with a letter (`a-z`, `A-Z`) or underscore (`_`)
/// - **Contain** only letters, digits (`0-9`), underscores (`_`), and dollar signs (`$`)
/// - **Not be empty** (minimum length of 1 character)
/// - **Not exceed** 255 bytes (practical limit)
///
/// ## Case Sensitivity
///
/// Function names are case-sensitive. The type stores the function name as-is without normalization.
#[derive(Debug, Clone, Eq, Ord, PartialOrd)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(transparent))]
pub struct FuncName(
    #[cfg_attr(
        feature = "schemars",
        schemars(regex(pattern = r"^[a-zA-Z_][a-zA-Z0-9_$]*$"))
    )]
    #[cfg_attr(feature = "schemars", schemars(length(min = 1, max = 255)))]
    String,
);

impl FuncName {
    /// Returns a reference to the inner string value.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes the FuncName and returns the inner String.
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl AsRef<str> for FuncName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl std::ops::Deref for FuncName {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl PartialEq for FuncName {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl PartialEq<str> for FuncName {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl PartialEq<FuncName> for str {
    fn eq(&self, other: &FuncName) -> bool {
        *self == other.0
    }
}

impl PartialEq<&str> for FuncName {
    fn eq(&self, other: &&str) -> bool {
        self.0 == **other
    }
}

impl std::hash::Hash for FuncName {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl std::fmt::Display for FuncName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::str::FromStr for FuncName {
    type Err = FuncNameError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        validate_func_name(s)?;
        Ok(FuncName(s.to_string()))
    }
}

impl serde::Serialize for FuncName {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for FuncName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(serde::de::Error::custom)
    }
}

/// Validates that a function name follows identifier rules for DataFusion UDFs.
///
/// Checks:
/// - Not empty
/// - Not longer than 255 bytes
/// - First character is a letter (`a-z`, `A-Z`) or underscore (`_`)
/// - All characters are letters, digits, underscores, or dollar signs (`[a-zA-Z0-9_$]`)
pub fn validate_func_name(name: &str) -> Result<(), FuncNameError> {
    if name.is_empty() {
        return Err(FuncNameError::Empty);
    }

    if name.len() > MAX_IDENTIFIER_LENGTH {
        return Err(FuncNameError::TooLong { length: name.len() });
    }

    // Check first character: must be letter or underscore
    let mut chars = name.chars();
    if let Some(first_char) = chars.next()
        && !(first_char.is_ascii_alphabetic() || first_char == '_')
    {
        return Err(FuncNameError::InvalidFirstCharacter {
            character: first_char,
            value: name.to_string(),
        });
    }

    // Check remaining characters: must be alphanumeric, underscore, or dollar sign
    if let Some(invalid_char) = name
        .chars()
        .find(|&c| !(c.is_ascii_alphanumeric() || c == '_' || c == '$'))
    {
        return Err(FuncNameError::InvalidCharacter {
            character: invalid_char,
            value: name.to_string(),
        });
    }

    Ok(())
}

/// Errors that occur when parsing or validating UDF function names.
///
/// This error type is used by [`FuncName::from_str`] when validating function name identifiers.
/// All variants provide detailed context about validation failures to help users correct
/// invalid function names.
#[derive(Debug, thiserror::Error)]
pub enum FuncNameError {
    /// Function name is empty (zero-length string)
    ///
    /// This occurs when attempting to create a function name from an empty string.
    /// Function names must contain at least one character.
    #[error("function name cannot be empty")]
    Empty,

    /// Function name exceeds maximum allowed length
    ///
    /// This occurs when the function name is longer than 255 bytes (the maximum identifier
    /// length). While most function names are much shorter, this limit prevents extremely
    /// long identifiers that could cause issues with SQL query length limits or system
    /// resource constraints.
    ///
    /// # Common causes
    /// - Auto-generated function names with excessive prefixes/suffixes
    /// - Accidental inclusion of documentation or code in the name field
    /// - Programmatic name construction that doesn't validate length
    ///
    /// # Note
    /// The limit is in bytes, not characters. Multi-byte UTF-8 characters count as multiple
    /// bytes toward this limit.
    #[error("function name is too long ({length} bytes, maximum is {MAX_IDENTIFIER_LENGTH})")]
    TooLong {
        /// The actual length of the invalid function name in bytes
        length: usize,
    },

    /// Function name starts with an invalid character
    ///
    /// This occurs when the first character of the function name is not a letter (a-z, A-Z)
    /// or underscore (_). DataFusion SQL identifiers must start with these characters.
    ///
    /// # Common causes
    /// - Starting with a digit: `123_func`
    /// - Starting with a dollar sign: `$helper`
    /// - Starting with a hyphen: `-myFunc`
    /// - Starting with special characters: `@func`, `#func`, `!func`
    ///
    /// # Valid alternatives
    /// - Prefix with underscore: `_123_func` instead of `123_func`
    /// - Use descriptive prefix: `func_123` instead of `123_func`
    /// - Remove special characters: `helper` instead of `$helper`
    #[error("function name '{value}' must start with a letter or underscore, not '{character}'")]
    InvalidFirstCharacter {
        /// The invalid first character that caused the error
        character: char,
        /// The complete function name that was rejected
        value: String,
    },

    /// Function name contains an invalid character
    ///
    /// This occurs when the function name contains characters that are not allowed in
    /// DataFusion SQL identifiers. Only letters (a-z, A-Z), digits (0-9), underscores (_),
    /// and dollar signs ($) are permitted.
    ///
    /// # Common causes
    /// - Hyphens/dashes: `my-func` (use `my_func` instead)
    /// - Spaces: `my func` (use `my_func` instead)
    /// - Dots: `my.func` (use `my_func` instead)
    /// - Special characters: `my@func`, `my#func`, `my!func`
    ///
    /// # Note
    /// Dollar signs ($) are allowed within function names (e.g., `func$1`, `my$helper`)
    /// but not as the first character.
    #[error(
        "invalid character '{character}' in function name '{value}' (only letters, digits, underscores, and dollar signs are allowed)"
    )]
    InvalidCharacter {
        /// The invalid character that caused the error
        character: char,
        /// The complete function name that was rejected
        value: String,
    },
}

#[cfg(test)]
mod tests {
    use super::{FuncNameError, validate_func_name};

    #[test]
    fn accept_valid_function_names() {
        // Lowercase
        assert!(validate_func_name("myfunction").is_ok());
        assert!(validate_func_name("my_function").is_ok());
        assert!(validate_func_name("func123").is_ok());

        // Uppercase
        assert!(validate_func_name("MyFunction").is_ok());
        assert!(validate_func_name("MY_FUNCTION").is_ok());
        assert!(validate_func_name("FUNC123").is_ok());

        // Mixed case (camelCase, PascalCase)
        assert!(validate_func_name("myFunction").is_ok());
        assert!(validate_func_name("calculateSum").is_ok());
        assert!(validate_func_name("getUserById").is_ok());

        // Starts with underscore
        assert!(validate_func_name("_private").is_ok());
        assert!(validate_func_name("_helper").is_ok());
        assert!(validate_func_name("_").is_ok());

        // Contains dollar sign (common in generated code)
        assert!(validate_func_name("func$1").is_ok());
        assert!(validate_func_name("my$func").is_ok());
        assert!(validate_func_name("jquery$style").is_ok()); // $ allowed in middle, not at start

        // Single character
        assert!(validate_func_name("a").is_ok());
        assert!(validate_func_name("A").is_ok());
        assert!(validate_func_name("_").is_ok());

        // Max length (255 bytes)
        let max_length_name = "a".repeat(255);
        assert!(validate_func_name(&max_length_name).is_ok());
    }

    #[test]
    fn reject_empty_function_name() {
        let result = validate_func_name("");
        assert!(matches!(result, Err(FuncNameError::Empty)));
    }

    #[test]
    fn reject_too_long_function_name() {
        let too_long_name = "a".repeat(256);
        let result = validate_func_name(&too_long_name);
        assert!(matches!(
            result,
            Err(FuncNameError::TooLong { length: 256 })
        ));
    }

    #[test]
    fn reject_starts_with_digit() {
        let result = validate_func_name("123func");
        assert!(matches!(
            result,
            Err(FuncNameError::InvalidFirstCharacter { character: '1', .. })
        ));
    }

    #[test]
    fn reject_starts_with_dollar() {
        let result = validate_func_name("$func");
        assert!(matches!(
            result,
            Err(FuncNameError::InvalidFirstCharacter { character: '$', .. })
        ));
    }

    #[test]
    fn reject_invalid_characters() {
        // Hyphen
        let result = validate_func_name("my-func");
        assert!(matches!(
            result,
            Err(FuncNameError::InvalidCharacter { character: '-', .. })
        ));

        // Space
        let result = validate_func_name("my func");
        assert!(matches!(
            result,
            Err(FuncNameError::InvalidCharacter { character: ' ', .. })
        ));

        // Dot
        let result = validate_func_name("my.func");
        assert!(matches!(
            result,
            Err(FuncNameError::InvalidCharacter { character: '.', .. })
        ));

        // Special characters
        let result = validate_func_name("my@func");
        assert!(matches!(
            result,
            Err(FuncNameError::InvalidCharacter { character: '@', .. })
        ));

        let result = validate_func_name("my#func");
        assert!(matches!(
            result,
            Err(FuncNameError::InvalidCharacter { character: '#', .. })
        ));

        let result = validate_func_name("my!func");
        assert!(matches!(
            result,
            Err(FuncNameError::InvalidCharacter { character: '!', .. })
        ));
    }

    #[test]
    fn accept_names_similar_to_reserved_words() {
        // Function names that happen to match keywords in various languages are still valid
        // since we only validate DataFusion identifier rules, not language-specific keywords
        assert!(validate_func_name("function").is_ok()); // JavaScript keyword, but valid identifier
        assert!(validate_func_name("return").is_ok()); // JavaScript keyword, but valid identifier
        assert!(validate_func_name("class").is_ok()); // JavaScript keyword, but valid identifier
        assert!(validate_func_name("Function").is_ok()); // Capital F
        assert!(validate_func_name("my_function").is_ok()); // Contains "function"
        assert!(validate_func_name("function_name").is_ok()); // Contains "function"
        assert!(validate_func_name("return_value").is_ok()); // Contains "return"
        assert!(validate_func_name("if_condition").is_ok()); // Contains "if"
        assert!(validate_func_name("forEach").is_ok()); // Common JavaScript name
        assert!(validate_func_name("className").is_ok()); // Common JavaScript name
    }

    #[test]
    fn test_fromstr_trait() {
        use std::str::FromStr;

        use super::FuncName;

        // Valid names
        assert!(FuncName::from_str("myFunc").is_ok());
        assert!(FuncName::from_str("_private").is_ok());
        assert!(FuncName::from_str("function").is_ok()); // Valid identifier (not checking keywords)

        // Invalid names
        assert!(FuncName::from_str("123invalid").is_err());
        assert!(FuncName::from_str("my-func").is_err());
    }
}
