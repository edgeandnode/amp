use std::collections::HashMap;

use privy_rs::generated::types::{LinkedAccount, User};
use serde::{Deserialize, Serialize};

/// Extension trait for privy_rs User type to provide Ethereum address extraction utilities
pub trait UserExt {
    /// Get the primary Ethereum wallet address from linked accounts
    /// Priority order: 1. Smart Wallet, 2. Ethereum Wallet, 3. Ethereum Embedded Wallet
    fn ethereum_address(&self) -> Option<String>;

    /// Get all Ethereum addresses (useful for debugging or multiple wallet scenarios)
    fn all_ethereum_addresses(&self) -> Vec<String>;
}

impl UserExt for User {
    fn ethereum_address(&self) -> Option<String> {
        // First, try to find a smart wallet
        if let Some(address) = find_smart_wallet_address(&self.linked_accounts) {
            return Some(address);
        }

        // Then try regular Ethereum wallet
        if let Some(address) = find_ethereum_wallet_address(&self.linked_accounts) {
            return Some(address);
        }

        // Finally try Ethereum embedded wallet
        find_ethereum_embedded_wallet_address(&self.linked_accounts)
    }

    fn all_ethereum_addresses(&self) -> Vec<String> {
        let mut addresses = Vec::new();

        for account in &self.linked_accounts {
            match account {
                LinkedAccount::SmartWallet(wallet) => {
                    addresses.push(wallet.address.clone());
                }
                LinkedAccount::Ethereum(wallet) => {
                    addresses.push(wallet.address.clone());
                }
                LinkedAccount::EthereumEmbeddedWallet(wallet) => {
                    addresses.push(wallet.address.clone());
                }
                _ => {}
            }
        }

        addresses
    }
}

/// Find smart wallet address from linked accounts
fn find_smart_wallet_address(linked_accounts: &[LinkedAccount]) -> Option<String> {
    for account in linked_accounts {
        if let LinkedAccount::SmartWallet(wallet) = account {
            return Some(wallet.address.clone());
        }
    }
    None
}

/// Find regular Ethereum wallet address (non-embedded)
fn find_ethereum_wallet_address(linked_accounts: &[LinkedAccount]) -> Option<String> {
    for account in linked_accounts {
        if let LinkedAccount::Ethereum(wallet) = account {
            // Regular Ethereum wallet (not embedded)
            return Some(wallet.address.clone());
        }
    }
    None
}

/// Find Ethereum embedded wallet address
fn find_ethereum_embedded_wallet_address(linked_accounts: &[LinkedAccount]) -> Option<String> {
    for account in linked_accounts {
        if let LinkedAccount::EthereumEmbeddedWallet(wallet) = account {
            return Some(wallet.address.clone());
        }
    }
    None
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
    pub user: Option<User>,
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
    use privy_rs::generated::types::{
        LinkedAccount, LinkedAccountEmail, LinkedAccountEmailType, LinkedAccountEthereum,
        LinkedAccountEthereumChainType, LinkedAccountEthereumEmbeddedWallet,
        LinkedAccountEthereumEmbeddedWalletChainType,
        LinkedAccountEthereumEmbeddedWalletConnectorType,
        LinkedAccountEthereumEmbeddedWalletRecoveryMethod, LinkedAccountEthereumEmbeddedWalletType,
        LinkedAccountEthereumEmbeddedWalletWalletClient,
        LinkedAccountEthereumEmbeddedWalletWalletClientType, LinkedAccountEthereumType,
        LinkedAccountEthereumWalletClient, LinkedAccountSmartWallet, LinkedAccountSmartWalletType,
        SmartWalletType,
    };

    use super::*;

    fn create_test_user_with_accounts(accounts: Vec<LinkedAccount>) -> User {
        User {
            id: "test_user_id".to_string(),
            created_at: 1731974895.0,
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
            LinkedAccount::EthereumEmbeddedWallet(LinkedAccountEthereumEmbeddedWallet {
                address: "0x3333333333333333333333333333333333333333".to_string(),
                chain_id: "1".to_string(),
                chain_type: LinkedAccountEthereumEmbeddedWalletChainType::Ethereum,
                connector_type: LinkedAccountEthereumEmbeddedWalletConnectorType::Embedded,
                delegated: false,
                first_verified_at: Some(1731974895.0),
                id: None,
                imported: false,
                latest_verified_at: Some(1731974895.0),
                recovery_method: LinkedAccountEthereumEmbeddedWalletRecoveryMethod::Privy,
                type_: LinkedAccountEthereumEmbeddedWalletType::Wallet,
                verified_at: 1731974895.0,
                wallet_client: LinkedAccountEthereumEmbeddedWalletWalletClient::Privy,
                wallet_client_type: LinkedAccountEthereumEmbeddedWalletWalletClientType::Privy,
                wallet_index: 0.0,
            }),
            // Regular Ethereum wallet (middle priority)
            LinkedAccount::Ethereum(LinkedAccountEthereum {
                address: "0x2222222222222222222222222222222222222222".to_string(),
                chain_id: Some("1".to_string()),
                chain_type: LinkedAccountEthereumChainType::Ethereum,
                connector_type: Some("injected".to_string()),
                first_verified_at: Some(1731974895.0),
                latest_verified_at: Some(1731974895.0),
                type_: LinkedAccountEthereumType::Wallet,
                verified_at: 1731974895.0,
                wallet_client: LinkedAccountEthereumWalletClient::Unknown,
                wallet_client_type: Some("browser".to_string()),
            }),
            // Smart wallet (highest priority)
            LinkedAccount::SmartWallet(LinkedAccountSmartWallet {
                address: "0x1111111111111111111111111111111111111111".to_string(),
                first_verified_at: Some(1731974895.0),
                latest_verified_at: Some(1731974895.0),
                smart_wallet_type: SmartWalletType::Safe,
                smart_wallet_version: None,
                type_: LinkedAccountSmartWalletType::SmartWallet,
                verified_at: 1731974895.0,
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
            LinkedAccount::EthereumEmbeddedWallet(LinkedAccountEthereumEmbeddedWallet {
                address: "0x3333333333333333333333333333333333333333".to_string(),
                chain_id: "1".to_string(),
                chain_type: LinkedAccountEthereumEmbeddedWalletChainType::Ethereum,
                connector_type: LinkedAccountEthereumEmbeddedWalletConnectorType::Embedded,
                delegated: false,
                first_verified_at: Some(1731974895.0),
                id: None,
                imported: false,
                latest_verified_at: Some(1731974895.0),
                recovery_method: LinkedAccountEthereumEmbeddedWalletRecoveryMethod::Privy,
                type_: LinkedAccountEthereumEmbeddedWalletType::Wallet,
                verified_at: 1731974895.0,
                wallet_client: LinkedAccountEthereumEmbeddedWalletWalletClient::Privy,
                wallet_client_type: LinkedAccountEthereumEmbeddedWalletWalletClientType::Privy,
                wallet_index: 0.0,
            }),
            LinkedAccount::Ethereum(LinkedAccountEthereum {
                address: "0x2222222222222222222222222222222222222222".to_string(),
                chain_id: Some("1".to_string()),
                chain_type: LinkedAccountEthereumChainType::Ethereum,
                connector_type: Some("injected".to_string()),
                first_verified_at: Some(1731974895.0),
                latest_verified_at: Some(1731974895.0),
                type_: LinkedAccountEthereumType::Wallet,
                verified_at: 1731974895.0,
                wallet_client: LinkedAccountEthereumWalletClient::Unknown,
                wallet_client_type: Some("browser".to_string()),
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
        let accounts = vec![LinkedAccount::EthereumEmbeddedWallet(
            LinkedAccountEthereumEmbeddedWallet {
                address: "0x3333333333333333333333333333333333333333".to_string(),
                chain_id: "1".to_string(),
                chain_type: LinkedAccountEthereumEmbeddedWalletChainType::Ethereum,
                connector_type: LinkedAccountEthereumEmbeddedWalletConnectorType::Embedded,
                delegated: false,
                first_verified_at: Some(1731974895.0),
                id: None,
                imported: false,
                latest_verified_at: Some(1731974895.0),
                recovery_method: LinkedAccountEthereumEmbeddedWalletRecoveryMethod::Privy,
                type_: LinkedAccountEthereumEmbeddedWalletType::Wallet,
                verified_at: 1731974895.0,
                wallet_client: LinkedAccountEthereumEmbeddedWalletWalletClient::Privy,
                wallet_client_type: LinkedAccountEthereumEmbeddedWalletWalletClientType::Privy,
                wallet_index: 0.0,
            },
        )];

        let user = create_test_user_with_accounts(accounts);

        // Should return embedded wallet address (no other Ethereum wallets)
        assert_eq!(
            user.ethereum_address(),
            Some("0x3333333333333333333333333333333333333333".to_string())
        );
    }

    #[test]
    fn test_no_ethereum_wallets() {
        let accounts = vec![LinkedAccount::Email(LinkedAccountEmail {
            address: "user@example.com".to_string(),
            first_verified_at: Some(1731974895.0),
            latest_verified_at: Some(1731974895.0),
            type_: LinkedAccountEmailType::Email,
            verified_at: 1731974895.0,
        })];

        let user = create_test_user_with_accounts(accounts);

        // Should return None (no Ethereum wallets)
        assert_eq!(user.ethereum_address(), None);
    }

    #[test]
    fn test_all_ethereum_addresses() {
        let accounts = vec![
            LinkedAccount::SmartWallet(LinkedAccountSmartWallet {
                address: "0x1111111111111111111111111111111111111111".to_string(),
                first_verified_at: Some(1731974895.0),
                latest_verified_at: Some(1731974895.0),
                smart_wallet_type: SmartWalletType::Safe,
                smart_wallet_version: None,
                type_: LinkedAccountSmartWalletType::SmartWallet,
                verified_at: 1731974895.0,
            }),
            LinkedAccount::Ethereum(LinkedAccountEthereum {
                address: "0x2222222222222222222222222222222222222222".to_string(),
                chain_id: Some("1".to_string()),
                chain_type: LinkedAccountEthereumChainType::Ethereum,
                connector_type: Some("injected".to_string()),
                first_verified_at: Some(1731974895.0),
                latest_verified_at: Some(1731974895.0),
                type_: LinkedAccountEthereumType::Wallet,
                verified_at: 1731974895.0,
                wallet_client: LinkedAccountEthereumWalletClient::Unknown,
                wallet_client_type: Some("browser".to_string()),
            }),
            LinkedAccount::EthereumEmbeddedWallet(LinkedAccountEthereumEmbeddedWallet {
                address: "0x3333333333333333333333333333333333333333".to_string(),
                chain_id: "1".to_string(),
                chain_type: LinkedAccountEthereumEmbeddedWalletChainType::Ethereum,
                connector_type: LinkedAccountEthereumEmbeddedWalletConnectorType::Embedded,
                delegated: false,
                first_verified_at: Some(1731974895.0),
                id: None,
                imported: false,
                latest_verified_at: Some(1731974895.0),
                recovery_method: LinkedAccountEthereumEmbeddedWalletRecoveryMethod::Privy,
                type_: LinkedAccountEthereumEmbeddedWalletType::Wallet,
                verified_at: 1731974895.0,
                wallet_client: LinkedAccountEthereumEmbeddedWalletWalletClient::Privy,
                wallet_client_type: LinkedAccountEthereumEmbeddedWalletWalletClientType::Privy,
                wallet_index: 0.0,
            }),
        ];

        let user = create_test_user_with_accounts(accounts);
        let all_addresses = user.all_ethereum_addresses();

        // Should return all Ethereum addresses
        assert_eq!(all_addresses.len(), 3);
        assert!(all_addresses.contains(&"0x1111111111111111111111111111111111111111".to_string()));
        assert!(all_addresses.contains(&"0x2222222222222222222222222222222222222222".to_string()));
        assert!(all_addresses.contains(&"0x3333333333333333333333333333333333333333".to_string()));
    }

    #[test]
    fn test_empty_accounts_list() {
        let user = create_test_user_with_accounts(vec![]);

        assert_eq!(user.ethereum_address(), None);
        assert_eq!(user.all_ethereum_addresses(), Vec::<String>::new());
    }

    #[test]
    fn test_real_world_privy_response_structure() {
        // Test with a mix of account types including a smart wallet
        let accounts = vec![
            LinkedAccount::Email(LinkedAccountEmail {
                address: "tom.bombadill@privy.io".to_string(),
                first_verified_at: Some(1674788927.0),
                latest_verified_at: Some(1674788927.0),
                type_: LinkedAccountEmailType::Email,
                verified_at: 1674788927.0,
            }),
            LinkedAccount::SmartWallet(LinkedAccountSmartWallet {
                address: "0x1234567890123456789012345678901234567890".to_string(),
                first_verified_at: Some(1741194420.0),
                latest_verified_at: Some(1741194420.0),
                smart_wallet_type: SmartWalletType::Safe,
                smart_wallet_version: None,
                type_: LinkedAccountSmartWalletType::SmartWallet,
                verified_at: 1741194420.0,
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
