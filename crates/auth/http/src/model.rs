use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Ethereum-related linked account types for extracting wallet addresses
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum EthereumLinkedAccount {
    #[serde(rename = "smart_wallet")]
    SmartWallet {
        address: String,
        smart_wallet_type: String,
        #[serde(default)]
        smart_wallet_version: Option<String>,
        verified_at: i64,
        #[serde(default)]
        first_verified_at: Option<i64>,
        #[serde(default)]
        latest_verified_at: Option<i64>,
    },
    #[serde(rename = "wallet")]
    Ethereum {
        address: String,
        #[serde(default)]
        chain_id: Option<String>,
        chain_type: String, // Should be "ethereum"
        #[serde(default)]
        wallet_client: Option<String>,
        #[serde(default)]
        wallet_client_type: Option<String>,
        #[serde(default)]
        connector_type: Option<String>,
        verified_at: i64,
        #[serde(default)]
        first_verified_at: Option<i64>,
        #[serde(default)]
        latest_verified_at: Option<i64>,
    },
}

/// Ethereum embedded wallet (also uses "wallet" type but with specific fields)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EthereumEmbeddedWallet {
    #[serde(rename = "type")]
    pub account_type: String, // "wallet"
    pub address: String,
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub imported: bool,
    #[serde(default)]
    pub delegated: bool,
    #[serde(default)]
    pub wallet_index: Option<i32>,
    #[serde(default)]
    pub chain_id: Option<String>,
    pub chain_type: String, // "ethereum"
    #[serde(default)]
    pub wallet_client: Option<String>, // "privy"
    #[serde(default)]
    pub wallet_client_type: Option<String>, // "privy"
    #[serde(default)]
    pub connector_type: Option<String>, // "embedded"
    #[serde(default)]
    pub recovery_method: Option<String>,
    pub verified_at: i64,
    #[serde(default)]
    pub first_verified_at: Option<i64>,
    #[serde(default)]
    pub latest_verified_at: Option<i64>,
}

/// Representation of the Auth IDaaS user object used when fetching the user
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AuthUser {
    pub id: String,
    pub created_at: i64,
    pub has_accepted_terms: bool,
    pub is_guest: bool,
    pub linked_accounts: Vec<serde_json::Value>, // Keep as Value for flexibility
    pub mfa_methods: Vec<serde_json::Value>,
    #[serde(default)]
    pub custom_metadata: Option<serde_json::Value>,
}
impl AuthUser {
    /// Get the primary Ethereum wallet address from linked accounts
    /// Priority order: 1. Smart Wallet, 2. Ethereum Wallet, 3. Ethereum Embedded Wallet
    pub fn ethereum_address(&self) -> Option<String> {
        // First, try to find a smart wallet
        if let Some(address) = self.find_smart_wallet_address() {
            return Some(address);
        }

        // Then try regular Ethereum wallet
        if let Some(address) = self.find_ethereum_wallet_address() {
            return Some(address);
        }

        // Finally try Ethereum embedded wallet
        self.find_ethereum_embedded_wallet_address()
    }

    /// Helper method to determine if a wallet account is an embedded wallet
    fn is_embedded_wallet(account: &serde_json::Map<String, serde_json::Value>) -> bool {
        account.get("wallet_client").and_then(|v| v.as_str()) == Some("privy")
            && account.get("connector_type").and_then(|v| v.as_str()) == Some("embedded")
    }

    /// Find smart wallet address
    fn find_smart_wallet_address(&self) -> Option<String> {
        for account_value in &self.linked_accounts {
            if let Ok(account) = serde_json::from_value::<serde_json::Map<String, serde_json::Value>>(
                account_value.clone(),
            ) && account.get("type").and_then(|v| v.as_str()) == Some("smart_wallet")
                && let Some(address) = account.get("address").and_then(|v| v.as_str())
            {
                return Some(address.to_string());
            }
        }
        None
    }

    /// Find regular Ethereum wallet address
    fn find_ethereum_wallet_address(&self) -> Option<String> {
        for account_value in &self.linked_accounts {
            if let Ok(account) = serde_json::from_value::<serde_json::Map<String, serde_json::Value>>(
                account_value.clone(),
            ) && account.get("type").and_then(|v| v.as_str()) == Some("wallet")
                && let Some(chain_type) = account.get("chain_type").and_then(|v| v.as_str())
                && chain_type == "ethereum"
                && !Self::is_embedded_wallet(&account)
                && let Some(address) = account.get("address").and_then(|v| v.as_str())
            {
                return Some(address.to_string());
            }
        }
        None
    }

    /// Find Ethereum embedded wallet address
    fn find_ethereum_embedded_wallet_address(&self) -> Option<String> {
        for account_value in &self.linked_accounts {
            if let Ok(account) = serde_json::from_value::<serde_json::Map<String, serde_json::Value>>(
                account_value.clone(),
            ) && account.get("type").and_then(|v| v.as_str()) == Some("wallet")
                && let Some(chain_type) = account.get("chain_type").and_then(|v| v.as_str())
                && chain_type == "ethereum"
                && Self::is_embedded_wallet(&account)
                && let Some(address) = account.get("address").and_then(|v| v.as_str())
            {
                return Some(address.to_string());
            }
        }
        None
    }

    /// Get all Ethereum addresses (useful for debugging or multiple wallet scenarios)
    pub fn all_ethereum_addresses(&self) -> Vec<String> {
        let mut addresses = Vec::new();

        for account_value in &self.linked_accounts {
            if let Ok(account) = serde_json::from_value::<serde_json::Map<String, serde_json::Value>>(
                account_value.clone(),
            ) {
                let account_type = account.get("type").and_then(|v| v.as_str());

                match account_type {
                    Some("smart_wallet") => {
                        if let Some(address) = account.get("address").and_then(|v| v.as_str()) {
                            addresses.push(address.to_string());
                        }
                    }
                    Some("wallet") => {
                        if let Some(chain_type) = account.get("chain_type").and_then(|v| v.as_str())
                            && chain_type == "ethereum"
                            && let Some(address) = account.get("address").and_then(|v| v.as_str())
                        {
                            addresses.push(address.to_string());
                        }
                    }
                    _ => {}
                }
            }
        }

        addresses
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String,
    pub sid: Option<String>, // Privy session ID
    pub exp: Option<i64>,
    pub iat: Option<i64>,
    pub aud: Option<String>,
    pub iss: Option<String>,
    #[serde(flatten)]
    pub additional_claims: HashMap<String, serde_json::Value>,
}
impl Claims {
    /// Extracts the user ID from the Privy DID subject.
    ///
    /// Privy subjects are in the format "did:privy:user_id"
    /// This method returns just the user_id part.
    pub fn user_id(&self) -> String {
        // Split on "did:privy:" and take the last part
        self.sub
            .strip_prefix("did:privy:")
            .unwrap_or(&self.sub)
            .to_string()
    }
}

#[derive(Clone)]
pub struct AuthenticatedUser {
    pub claims: Claims,
    pub user: Option<AuthUser>,
}
impl AuthenticatedUser {
    /// Extracts the user ID from the claims.
    pub fn user_id(&self) -> String {
        self.claims.user_id()
    }

    /// Get the primary Ethereum wallet address for this authenticated user
    pub fn ethereum_address(&self) -> Option<String> {
        self.user.as_ref().and_then(|user| user.ethereum_address())
    }

    /// Get all Ethereum wallet addresses for this authenticated user
    pub fn all_ethereum_addresses(&self) -> Vec<String> {
        self.user
            .as_ref()
            .map(|user| user.all_ethereum_addresses())
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn create_test_user_with_accounts(accounts: Vec<serde_json::Value>) -> AuthUser {
        AuthUser {
            id: "test_user_id".to_string(),
            created_at: 1731974895,
            has_accepted_terms: true,
            is_guest: false,
            linked_accounts: accounts,
            mfa_methods: vec![],
            custom_metadata: None,
        }
    }

    #[test]
    fn test_smart_wallet_priority() {
        let accounts = vec![
            // Ethereum embedded wallet (lowest priority)
            json!({
                "type": "wallet",
                "address": "0x3333333333333333333333333333333333333333",
                "chain_type": "ethereum",
                "wallet_client": "privy",
                "connector_type": "embedded",
                "verified_at": 1731974895
            }),
            // Regular Ethereum wallet (middle priority)
            json!({
                "type": "wallet",
                "address": "0x2222222222222222222222222222222222222222",
                "chain_type": "ethereum",
                "wallet_client": "metamask",
                "connector_type": "injected",
                "verified_at": 1731974895
            }),
            // Smart wallet (highest priority)
            json!({
                "type": "smart_wallet",
                "address": "0x1111111111111111111111111111111111111111",
                "smart_wallet_type": "safe",
                "verified_at": 1731974895
            }),
        ];

        let user = create_test_user_with_accounts(accounts);

        // Should return smart wallet address (highest priority)
        assert_eq!(
            user.ethereum_address(),
            Some("0x1111111111111111111111111111111111111111".to_string())
        );
    }

    #[test]
    fn test_ethereum_wallet_priority() {
        let accounts = vec![
            // Ethereum embedded wallet (lowest priority)
            json!({
                "type": "wallet",
                "address": "0x3333333333333333333333333333333333333333",
                "chain_type": "ethereum",
                "wallet_client": "privy",
                "connector_type": "embedded",
                "verified_at": 1731974895
            }),
            // Regular Ethereum wallet (middle priority)
            json!({
                "type": "wallet",
                "address": "0x2222222222222222222222222222222222222222",
                "chain_type": "ethereum",
                "wallet_client": "metamask",
                "connector_type": "injected",
                "verified_at": 1731974895
            }),
            // Non-Ethereum wallet (should be ignored)
            json!({
                "type": "wallet",
                "address": "solana_address_here",
                "chain_type": "solana",
                "wallet_client": "phantom",
                "verified_at": 1731974895
            }),
        ];

        let user = create_test_user_with_accounts(accounts);

        // Should return regular Ethereum wallet address (no smart wallet present)
        assert_eq!(
            user.ethereum_address(),
            Some("0x2222222222222222222222222222222222222222".to_string())
        );
    }

    #[test]
    fn test_ethereum_embedded_wallet_fallback() {
        let accounts = vec![
            // Ethereum embedded wallet (only option)
            json!({
                "type": "wallet",
                "address": "0x3333333333333333333333333333333333333333",
                "chain_type": "ethereum",
                "wallet_client": "privy",
                "connector_type": "embedded",
                "verified_at": 1731974895
            }),
            // Non-Ethereum accounts (should be ignored)
            json!({
                "type": "wallet",
                "address": "solana_address",
                "chain_type": "solana",
                "wallet_client": "phantom",
                "verified_at": 1731974895
            }),
            json!({
                "type": "email",
                "address": "user@example.com",
                "verified_at": 1731974895
            }),
        ];

        let user = create_test_user_with_accounts(accounts);

        // Should return embedded wallet address (no other Ethereum wallets)
        assert_eq!(
            user.ethereum_address(),
            Some("0x3333333333333333333333333333333333333333".to_string())
        );
    }

    #[test]
    fn test_no_ethereum_wallets() {
        let accounts = vec![
            // Non-Ethereum accounts only
            json!({
                "type": "wallet",
                "address": "solana_address",
                "chain_type": "solana",
                "wallet_client": "phantom",
                "verified_at": 1731974895
            }),
            json!({
                "type": "email",
                "address": "user@example.com",
                "verified_at": 1731974895
            }),
            json!({
                "type": "farcaster",
                "fid": 12345,
                "username": "testuser",
                "verified_at": 1731974895
            }),
        ];

        let user = create_test_user_with_accounts(accounts);

        // Should return None (no Ethereum wallets)
        assert_eq!(user.ethereum_address(), None);
    }

    #[test]
    fn test_all_ethereum_addresses() {
        let accounts = vec![
            // Smart wallet
            json!({
                "type": "smart_wallet",
                "address": "0x1111111111111111111111111111111111111111",
                "smart_wallet_type": "safe",
                "verified_at": 1731974895
            }),
            // Regular Ethereum wallet
            json!({
                "type": "wallet",
                "address": "0x2222222222222222222222222222222222222222",
                "chain_type": "ethereum",
                "wallet_client": "metamask",
                "verified_at": 1731974895
            }),
            // Ethereum embedded wallet
            json!({
                "type": "wallet",
                "address": "0x3333333333333333333333333333333333333333",
                "chain_type": "ethereum",
                "wallet_client": "privy",
                "connector_type": "embedded",
                "verified_at": 1731974895
            }),
            // Non-Ethereum wallet (should be ignored)
            json!({
                "type": "wallet",
                "address": "solana_address",
                "chain_type": "solana",
                "verified_at": 1731974895
            }),
        ];

        let user = create_test_user_with_accounts(accounts);
        let all_addresses = user.all_ethereum_addresses();

        // Should return all Ethereum addresses
        assert_eq!(all_addresses.len(), 3);
        assert!(all_addresses.contains(&"0x1111111111111111111111111111111111111111".to_string()));
        assert!(all_addresses.contains(&"0x2222222222222222222222222222222222222222".to_string()));
        assert!(all_addresses.contains(&"0x3333333333333333333333333333333333333333".to_string()));
        assert!(!all_addresses.contains(&"solana_address".to_string()));
    }

    #[test]
    fn test_embedded_wallet_detection() {
        let accounts = vec![
            // This should be detected as embedded
            json!({
                "type": "wallet",
                "address": "0x1111111111111111111111111111111111111111",
                "chain_type": "ethereum",
                "wallet_client": "privy",
                "connector_type": "embedded",
                "verified_at": 1731974895
            }),
            // This should NOT be detected as embedded (missing connector_type)
            json!({
                "type": "wallet",
                "address": "0x2222222222222222222222222222222222222222",
                "chain_type": "ethereum",
                "wallet_client": "privy",
                "verified_at": 1731974895
            }),
            // This should NOT be detected as embedded (different wallet_client)
            json!({
                "type": "wallet",
                "address": "0x3333333333333333333333333333333333333333",
                "chain_type": "ethereum",
                "wallet_client": "metamask",
                "connector_type": "injected",
                "verified_at": 1731974895
            }),
        ];

        let user = create_test_user_with_accounts(accounts);

        // Should prioritize the non-embedded wallets first
        assert_eq!(
            user.ethereum_address(),
            Some("0x2222222222222222222222222222222222222222".to_string())
        );
    }

    #[test]
    fn test_malformed_accounts_ignored() {
        let accounts = vec![
            // Malformed JSON (missing required fields)
            json!({
                "type": "wallet",
                // missing address
                "chain_type": "ethereum",
                "verified_at": 1731974895
            }),
            // Valid smart wallet
            json!({
                "type": "smart_wallet",
                "address": "0x1111111111111111111111111111111111111111",
                "smart_wallet_type": "safe",
                "verified_at": 1731974895
            }),
        ];

        let user = create_test_user_with_accounts(accounts);

        // Should still find the valid smart wallet
        assert_eq!(
            user.ethereum_address(),
            Some("0x1111111111111111111111111111111111111111".to_string())
        );
    }

    #[test]
    fn test_empty_accounts_list() {
        let user = create_test_user_with_accounts(vec![]);

        assert_eq!(user.ethereum_address(), None);
        assert_eq!(user.all_ethereum_addresses(), Vec::<String>::new());
    }

    #[test]
    fn test_real_world_privy_response_structure() {
        // Based on the actual Privy API documentation example
        let accounts = vec![
            json!({
                "address": "tom.bombadill@privy.io",
                "type": "email",
                "first_verified_at": 1674788927,
                "latest_verified_at": 1674788927,
                "verified_at": 1674788927
            }),
            json!({
                "type": "farcaster",
                "fid": 4423,
                "owner_address": "0xE6bFb4137F3A8C069F98cc775f324A84FE45FdFF",
                "username": "payton",
                "display_name": "payton ↑",
                "verified_at": 1740678402,
                "first_verified_at": 1740678402,
                "latest_verified_at": 1741194370
            }),
            json!({
                "type": "passkey",
                "credential_id": "Il5vP-3Tm3hNmDVBmDlREgXzIOJnZEaiVnT-XMliXe-BufP9GL1-d3qhozk9IkZwQ_",
                "authenticator_name": "1Password",
                "enrolled_in_mfa": true,
                "verified_at": 1741194420,
                "first_verified_at": 1741194420,
                "latest_verified_at": 1741194420
            }),
            // Add a smart wallet for testing
            json!({
                "type": "smart_wallet",
                "address": "0x1234567890123456789012345678901234567890",
                "smart_wallet_type": "safe",
                "verified_at": 1741194420
            }),
        ];

        let user = create_test_user_with_accounts(accounts);

        // Should find the smart wallet address
        assert_eq!(
            user.ethereum_address(),
            Some("0x1234567890123456789012345678901234567890".to_string())
        );

        // Should return one address
        assert_eq!(user.all_ethereum_addresses().len(), 1);
    }
}
