//! Common utility functions.

use alloy::primitives::Address;

use crate::auth::AuthStorage;

/// Shorten a string for display, showing first 6 and last 6 characters.
///
/// Strings <= 18 characters are returned as-is.
/// Longer strings become `first6...last6`.
///
/// Handles UTF-8 safely by operating on characters, not bytes.
pub fn shorten(val: &str) -> String {
    let char_count = val.chars().count();
    if char_count <= 18 {
        val.to_string()
    } else {
        let first: String = val.chars().take(6).collect();
        let last: String = val.chars().skip(char_count - 6).collect();
        format!("{}...{}", first, last)
    }
}

/// Check if a string is a valid EVM wallet address.
///
/// Uses alloy's Address type for proper validation including:
/// - Must start with "0x" prefix
/// - Correct length (42 characters with 0x prefix)
/// - Valid hexadecimal characters
pub fn is_evm_address(s: &str) -> bool {
    s.starts_with("0x") && s.parse::<Address>().is_ok()
}

/// Get the display account from auth storage.
///
/// Prefers the first valid EVM wallet address (0x...).
/// Falls back to user_id if no valid address is found.
/// Returns the shortened form for display.
pub fn get_account_display(auth: &AuthStorage) -> String {
    let account = auth
        .accounts
        .as_ref()
        .and_then(|accounts| accounts.iter().find(|a| is_evm_address(a)))
        .map(|s| s.as_str())
        .unwrap_or(&auth.user_id);
    shorten(account)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shorten_with_short_string_returns_unchanged() {
        //* Given
        let short = "hello";
        let exactly_18 = "exactly18chars!!!";

        //* When
        let result_short = shorten(short);
        let result_18 = shorten(exactly_18);

        //* Then
        assert_eq!(result_short, "hello", "short strings should be unchanged");
        assert_eq!(
            result_18, "exactly18chars!!!",
            "strings <= 18 chars should be unchanged"
        );
    }

    #[test]
    fn shorten_with_long_string_returns_truncated() {
        //* Given
        let long = "0x742d35Cc6634C0532925a3b844Bc9e7595f8fE71";

        //* When
        let result = shorten(long);

        //* Then
        assert_eq!(
            result, "0x742d...f8fE71",
            "long strings should be truncated to first6...last6"
        );
    }

    #[test]
    fn is_evm_address_with_valid_addresses_returns_true() {
        //* Given
        let lowercase = "0x742d35cc6634c0532925a3b844bc9e7595f8fe71";
        let checksummed = "0x742d35Cc6634C0532925a3b844Bc9e7595f8fE71";
        let all_zeros = "0x0000000000000000000000000000000000000000";

        //* When + Then
        assert!(
            is_evm_address(lowercase),
            "valid lowercase address should be accepted"
        );
        assert!(
            is_evm_address(checksummed),
            "valid checksummed address should be accepted"
        );
        assert!(
            is_evm_address(all_zeros),
            "valid all-zeros address should be accepted"
        );
    }

    #[test]
    fn is_evm_address_with_invalid_formats_returns_false() {
        //* Given
        let not_evm = "did:privy:cmfd6bf6u006vjx0b7xb2eybx";
        let too_short = "0x123";
        let missing_prefix = "742d35Cc6634C0532925a3b844Bc9e7595f8fE71";
        let empty = "";

        //* When + Then
        assert!(
            !is_evm_address(not_evm),
            "non-EVM format should be rejected"
        );
        assert!(
            !is_evm_address(too_short),
            "too short address should be rejected"
        );
        assert!(
            !is_evm_address(missing_prefix),
            "address without 0x prefix should be rejected"
        );
        assert!(!is_evm_address(empty), "empty string should be rejected");
    }
}
