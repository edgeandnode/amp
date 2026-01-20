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
    fn test_shorten_short_string() {
        assert_eq!(shorten("hello"), "hello");
        assert_eq!(shorten("exactly18chars!!!"), "exactly18chars!!!");
    }

    #[test]
    fn test_shorten_long_string() {
        let long = "0x742d35Cc6634C0532925a3b844Bc9e7595f8fE71";
        assert_eq!(shorten(long), "0x742d...f8fE71");
    }

    #[test]
    fn test_is_evm_address_valid() {
        // Valid lowercase address
        assert!(is_evm_address("0x742d35cc6634c0532925a3b844bc9e7595f8fe71"));
        // Valid checksummed address
        assert!(is_evm_address("0x742d35Cc6634C0532925a3b844Bc9e7595f8fE71"));
        // Valid all zeros
        assert!(is_evm_address("0x0000000000000000000000000000000000000000"));
    }

    #[test]
    fn test_is_evm_address_invalid() {
        // Not an EVM address format
        assert!(!is_evm_address("did:privy:cmfd6bf6u006vjx0b7xb2eybx"));
        // Too short
        assert!(!is_evm_address("0x123"));
        // Missing 0x prefix
        assert!(!is_evm_address("742d35Cc6634C0532925a3b844Bc9e7595f8fE71"));
        // Empty string
        assert!(!is_evm_address(""));
    }
}
