//! Authentication types for the admin API client.

use reqwest::header::HeaderValue;

/// Bearer token for HTTP authentication.
///
/// This _new-type_ wraps a validated bearer token string. The token is validated at construction
/// time to ensure it contains only valid ASCII characters suitable for HTTP headers.
///
/// The token is stored without the "Bearer " prefix - that prefix is added when converting
/// to a header value.
///
/// **Security Note**: This type implements `Debug` to redact the token value, preventing
/// accidental exposure in logs or error messages. The token is displayed as `BearerToken([REDACTED])`.
#[derive(Clone, PartialEq, Eq)]
pub struct BearerToken(String);

impl BearerToken {
    /// Create a new bearer token from a static string, bypassing validation.
    ///
    /// # Safety
    ///
    /// This function bypasses all validation invariants. The caller must ensure that:
    /// - The token is non-empty
    /// - The token contains only ASCII characters
    /// - The token does not contain newline characters (`\n` or `\r`)
    ///
    /// Violating these invariants may lead to runtime errors when converting to a header value.
    pub fn from_static_unchecked(token: &'static str) -> Self {
        Self(token.to_string())
    }

    /// Get the raw token as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Convert the bearer token to a reqwest `HeaderValue`.
    ///
    /// The returned value includes the `Bearer ` prefix and contains sensitive authentication data.
    /// The `HeaderValue` is marked as sensitive, which redacts its value in debug output.
    ///
    /// # Security
    ///
    /// The returned `HeaderValue` contains sensitive credentials and is marked as sensitive
    /// for automatic redaction in debug output. Handle with care and avoid logging or
    /// displaying the value.
    pub fn as_header_value(&self) -> HeaderValue {
        // SAFETY: Token validated at construction to only contain visible ASCII (32-126).
        let mut header_value: HeaderValue = format!("Bearer {}", self.0)
            .parse()
            .expect("validated token should convert to HeaderValue");
        header_value.set_sensitive(true);
        header_value
    }
}

impl std::fmt::Debug for BearerToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("BearerToken").field(&"[REDACTED]").finish()
    }
}

impl TryFrom<String> for BearerToken {
    type Error = BearerTokenError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        validate_token(&value)?;
        Ok(Self(value))
    }
}

impl TryFrom<&str> for BearerToken {
    type Error = BearerTokenError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        validate_token(value)?;
        Ok(Self(value.to_string()))
    }
}

impl std::str::FromStr for BearerToken {
    type Err = BearerTokenError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::try_from(value)
    }
}

/// Validate a bearer token string.
///
/// This validation ensures that the token will always successfully convert to a `HeaderValue`.
/// It checks that all characters are visible ASCII (32-126), which is the requirement for
/// `HeaderValue::from_str()`:
///
/// - No control characters (0-31) including newlines, tabs, NUL, etc.
/// - No DEL character (127)
/// - No non-ASCII characters (128-255)
fn validate_token(token: &str) -> Result<(), BearerTokenError> {
    if token.is_empty() {
        return Err(BearerTokenError::Empty);
    }

    // Check each character to ensure it's in the visible ASCII range (32-126)
    for (idx, ch) in token.chars().enumerate() {
        let byte = ch as u32;
        if !(32..=126).contains(&byte) {
            return Err(BearerTokenError::InvalidCharacter {
                character: ch,
                position: idx,
            });
        }
    }

    Ok(())
}

/// Errors that can occur when constructing a [`BearerToken`].
#[derive(Debug, thiserror::Error)]
pub enum BearerTokenError {
    /// Token is empty
    #[error("bearer token cannot be empty")]
    Empty,

    /// Token contains invalid character for HTTP header value
    ///
    /// This occurs when:
    /// - The token contains control characters (0-31) including newlines, tabs, NUL, etc.
    /// - The token contains the DEL character (127)
    /// - The token contains non-ASCII characters (128-255)
    ///
    /// Only visible ASCII characters (space through tilde, 32-126) are allowed.
    #[error("bearer token contains invalid character '{character}' (U+{:04X}) at position {position}", *character as u32)]
    InvalidCharacter {
        /// The invalid character
        character: char,
        /// The position of the invalid character in the token
        position: usize,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_static_unchecked_with_valid_token_succeeds() {
        //* When
        let token = BearerToken::from_static_unchecked("static-token");

        //* Then
        assert_eq!(token.as_str(), "static-token");
    }

    #[test]
    fn try_from_with_valid_token_succeeds() {
        //* When
        let result = BearerToken::try_from("valid-token-123");

        //* Then
        assert!(result.is_ok(), "conversion should succeed with valid token");
        let token = result.expect("should create valid BearerToken");
        assert_eq!(token.as_str(), "valid-token-123");
    }

    #[test]
    fn as_header_value_with_valid_token_includes_bearer_prefix() {
        //* Given
        let token = BearerToken::try_from("my-token").expect("should create valid BearerToken");

        //* When
        let header_value = token.as_header_value();

        //* Then
        let header_str = header_value
            .to_str()
            .expect("header value should be valid UTF-8");
        assert_eq!(header_str, "Bearer my-token");
    }

    #[test]
    fn try_from_with_empty_token_fails() {
        //* When
        let result = BearerToken::try_from("");

        //* Then
        assert!(result.is_err(), "conversion should fail with empty token");
        let error = result.expect_err("should return validation error");
        assert!(
            matches!(error, BearerTokenError::Empty),
            "Expected Empty error, got {:?}",
            error
        );
    }

    #[test]
    fn try_from_with_newline_token_fails() {
        //* When
        let result = BearerToken::try_from("token\nwith\nnewlines");

        //* Then
        assert!(
            result.is_err(),
            "conversion should fail with token containing newlines"
        );
        let error = result.expect_err("should return validation error");
        assert!(
            matches!(error, BearerTokenError::InvalidCharacter { .. }),
            "Expected InvalidCharacter error, got {:?}",
            error
        );
    }

    #[test]
    fn try_from_with_non_ascii_token_fails() {
        //* When
        let result = BearerToken::try_from("token-with-émoji-💀");

        //* Then
        assert!(
            result.is_err(),
            "conversion should fail with non-ASCII token"
        );
        let error = result.expect_err("should return validation error");
        assert!(
            matches!(error, BearerTokenError::InvalidCharacter { .. }),
            "Expected InvalidCharacter error, got {:?}",
            error
        );
    }

    #[test]
    fn try_from_with_control_character_fails() {
        //* When
        let result = BearerToken::try_from("token-with-\ttab");

        //* Then
        assert!(
            result.is_err(),
            "conversion should fail with control characters"
        );
        let error = result.expect_err("should return validation error");
        assert!(
            matches!(
                error,
                BearerTokenError::InvalidCharacter {
                    position: 11,
                    character: '\t'
                }
            ),
            "Expected InvalidCharacter error at position 11, got {:?}",
            error
        );
    }

    #[test]
    fn try_from_with_del_character_fails() {
        //* When
        let result = BearerToken::try_from("token\x7Fwith-del");

        //* Then
        assert!(result.is_err(), "conversion should fail with DEL character");
        let error = result.expect_err("should return validation error");
        assert!(
            matches!(
                error,
                BearerTokenError::InvalidCharacter { position: 5, .. }
            ),
            "Expected InvalidCharacter error at position 5, got {:?}",
            error
        );
    }
}
