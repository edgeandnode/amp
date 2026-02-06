//! Anvil fixture for local Ethereum blockchain testing.
//!
//! This fixture provides management of Anvil (local Ethereum node) instances for testing
//! blockchain interactions. It supports both IPC and HTTP connection modes and provides
//! convenient methods for mining blocks, triggering reorganizations, contract deployment,
//! and contract interaction.

use std::time::Duration;

use alloy::{
    eips::BlockId,
    network::{EthereumWallet, TransactionBuilder},
    node_bindings::{Anvil as AlloyAnvil, AnvilInstance},
    primitives::{Address, BlockHash, Bytes, TxHash, U256},
    providers::{Provider, ProviderBuilder, ext::AnvilApi},
    rpc::types::{TransactionRequest, anvil::ReorgOptions},
    signers::local::PrivateKeySigner,
};
use anyhow::{Result, anyhow};
use backon::{ConstantBuilder, Retryable as _};
use common::BlockNum;
use tempfile::NamedTempFile;

use super::contract_artifact::ContractArtifact;

/// Default retry interval for Anvil readiness checks.
const ANVIL_RETRY_INTERVAL: Duration = Duration::from_millis(200);

/// Anvil's default pre-funded account private key (Account #0).
///
/// This is the first of 10 pre-funded accounts that Anvil creates by default.
/// Address: 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266
/// Balance: 10,000 ETH
pub const DEFAULT_PRIVATE_KEY: &str =
    "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";

/// Connection mode for Anvil fixture.
#[derive(Debug)]
pub enum AnvilConnection {
    /// IPC connection with temporary socket file
    Ipc {
        _temp_file: NamedTempFile,
        path: String,
    },
    /// HTTP connection on specified port
    Http { port: u16 },
}

/// Fixture for managing Anvil instances in tests.
///
/// This fixture wraps an Anvil instance and provides convenient methods for
/// blockchain operations like mining blocks and triggering reorganizations.
/// The fixture automatically handles Anvil lifecycle and cleanup.
pub struct Anvil {
    _instance: AnvilInstance,
    provider: alloy::providers::DynProvider,
    connection: AnvilConnection,
}

impl Anvil {
    /// Create a new Anvil fixture with IPC connection.
    ///
    /// This creates a temporary IPC socket file and spawns Anvil configured to use it.
    /// The socket file is automatically cleaned up when the fixture is dropped.
    pub async fn new_ipc() -> Result<Self> {
        let temp_file = tempfile::Builder::new()
            .prefix("anvil")
            .tempfile()
            .map_err(|err| anyhow!("Failed to create temp file for Anvil IPC: {}", err))?;

        let ipc_path = temp_file.path().to_string_lossy().to_string();

        let instance = AlloyAnvil::new().ipc_path(&ipc_path).spawn();

        let provider = alloy::providers::ProviderBuilder::new()
            .connect_ipc(ipc_path.clone().into())
            .await
            .map_err(|err| anyhow!("Failed to connect to Anvil via IPC: {}", err))?
            .erased();

        Ok(Self {
            _instance: instance,
            provider,
            connection: AnvilConnection::Ipc {
                _temp_file: temp_file,
                path: ipc_path,
            },
        })
    }

    /// Create a new Anvil fixture with HTTP connection on the specified port.
    ///
    /// This spawns Anvil configured to listen on the given HTTP port.
    /// If port is 0, Anvil will automatically allocate an available port.
    pub async fn new_http(port: u16) -> Result<Self> {
        let instance = AlloyAnvil::new().port(port).spawn();
        let assigned_port = instance.port();

        // Connect to the existing Anvil instance via HTTP
        let url = format!("http://localhost:{}", assigned_port);
        let provider = alloy::providers::ProviderBuilder::new()
            .connect_http(url.parse()?)
            .erased();

        Ok(Self {
            _instance: instance,
            provider,
            connection: AnvilConnection::Http {
                port: assigned_port,
            },
        })
    }

    /// Get the connection URL for this Anvil instance.
    ///
    /// Returns the appropriate URL that can be used in provider configurations
    /// to connect to this Anvil instance.
    pub fn connection_url(&self) -> String {
        match &self.connection {
            AnvilConnection::Ipc { path, .. } => format!("ipc://{}", path),
            AnvilConnection::Http { port } => format!("http://localhost:{}", port),
        }
    }

    /// Get the underlying Alloy provider for this Anvil instance.
    ///
    /// This can be used for advanced blockchain operations not covered by
    /// the convenience methods on this fixture.
    pub fn provider(&self) -> &alloy::providers::DynProvider {
        &self.provider
    }

    /// Get the IPC path if this fixture uses IPC connection.
    ///
    /// Returns None if this fixture uses HTTP connection.
    pub fn ipc_path(&self) -> Option<&str> {
        match &self.connection {
            AnvilConnection::Ipc { path, .. } => Some(path),
            AnvilConnection::Http { .. } => None,
        }
    }

    /// Get the HTTP port if this fixture uses HTTP connection.
    ///
    /// Returns None if this fixture uses IPC connection.
    pub fn http_port(&self) -> Option<u16> {
        match &self.connection {
            AnvilConnection::Ipc { .. } => None,
            AnvilConnection::Http { port } => Some(*port),
        }
    }

    /// Create an Anvil provider configuration for this fixture.
    ///
    /// This creates a provider configuration that can connect to this Anvil instance
    /// using either IPC or HTTP. The provider will be named "anvil_rpc" and configured
    /// for the "anvil" network.
    pub fn new_provider_config(&self) -> String {
        indoc::formatdoc! {r#"
            kind = "evm-rpc"
            url = "{url}"
            network = "anvil"
        "#, url = self.connection_url()}
    }

    /// Get the default Anvil account's private key as a string.
    ///
    /// Returns the private key for Account #0 which has 10,000 ETH pre-funded.
    pub fn default_private_key(&self) -> &'static str {
        DEFAULT_PRIVATE_KEY
    }

    /// Create a signer for the default Anvil account.
    ///
    /// Returns a PrivateKeySigner that can be used to sign transactions
    /// with the default pre-funded account.
    pub fn default_signer(&self) -> Result<PrivateKeySigner> {
        DEFAULT_PRIVATE_KEY
            .parse()
            .map_err(|err| anyhow!("Failed to parse default private key: {}", err))
    }

    /// Create a wallet containing the default signer.
    ///
    /// Returns an EthereumWallet that can be used with ProviderBuilder
    /// to create a signing provider.
    pub fn default_wallet(&self) -> Result<EthereumWallet> {
        let signer = self.default_signer()?;
        Ok(EthereumWallet::from(signer))
    }

    /// Mine the specified number of blocks.
    ///
    /// This instructs Anvil to mine new blocks, which is useful for advancing
    /// the blockchain state in tests.
    pub async fn mine(&self, blocks: u64) -> Result<()> {
        tracing::info!(blocks, "Mining blocks");
        self.provider
            .anvil_mine(Some(blocks), None)
            .await
            .map_err(|err| anyhow!("Failed to mine blocks: {}", err))?;
        Ok(())
    }

    /// Trigger a blockchain reorganization with the specified depth.
    ///
    /// This causes Anvil to reorganize the blockchain by replacing the last
    /// `depth` blocks with alternative blocks, simulating a chain reorg.
    pub async fn reorg(&self, depth: u64) -> Result<()> {
        if depth == 0 {
            return Err(anyhow!("Reorg depth must be greater than 0"));
        }

        tracing::info!(depth, "Triggering blockchain reorg");

        let original_head = self.latest_block().await?;

        self.provider
            .anvil_reorg(ReorgOptions {
                depth,
                tx_block_pairs: vec![],
            })
            .await
            .map_err(|err| anyhow!("Failed to trigger reorg: {}", err))?;

        let new_head = self.latest_block().await?;

        // Verify the reorg happened as expected
        if original_head.block_num != new_head.block_num {
            return Err(anyhow!(
                "Reorg failed: block number changed from {} to {}",
                original_head.block_num,
                new_head.block_num
            ));
        }

        if original_head.hash == new_head.hash {
            return Err(anyhow!("Reorg failed: block hash did not change"));
        }

        tracing::info!(
            "Reorg successful: block {} hash changed from {} to {}",
            new_head.block_num,
            original_head.hash,
            new_head.hash
        );

        Ok(())
    }

    /// Get information about the latest block.
    ///
    /// Returns block number, hash, and parent hash for the current chain head.
    pub async fn latest_block(&self) -> Result<BlockInfo> {
        let block = self
            .provider
            .get_block(BlockId::latest())
            .await
            .map_err(|err| anyhow!("Failed to get latest block: {}", err))?
            .ok_or(anyhow!("Latest block not found"))?;

        Ok(BlockInfo {
            block_num: block.header.number,
            hash: block.header.hash,
            parent_hash: block.header.parent_hash,
        })
    }

    /// Wait for the Anvil service to be ready and responsive.
    ///
    /// This method polls the Anvil instance to ensure it's ready to accept requests.
    /// It performs a lightweight query (getting the current block number) with retries and
    /// fixed interval backoff to handle the startup delay that may occur when Anvil is first
    /// launched.
    pub async fn wait_for_ready(&self, timeout: Duration) -> Result<()> {
        tracing::debug!("Waiting for Anvil service to be ready");

        (|| async {
            match self.provider.get_block_number().await {
                Ok(_) => Ok(()),
                Err(err) => Err(format!("Failed to get block number: {}", err)),
            }
        })
        .retry(
            ConstantBuilder::default()
                .with_delay(ANVIL_RETRY_INTERVAL)
                .with_max_times(
                    timeout.as_millis() as usize / ANVIL_RETRY_INTERVAL.as_millis() as usize,
                ),
        )
        .sleep(tokio::time::sleep)
        .notify(|err, dur| {
            tracing::debug!(
                error = %err,
                "Anvil not ready yet, retrying in {:.1}s",
                dur.as_secs_f32()
            );
        })
        .await
        .map_err(|err| {
            anyhow!(
                "Anvil service did not become ready within {:?}. Last error: {}",
                timeout,
                err
            )
        })?;

        tracing::info!("Anvil service is ready");
        Ok(())
    }

    /// Deploy a contract using bytecode from a Forge artifact.
    ///
    /// Uses the default Anvil account for deployment. Waits for the transaction
    /// to be mined and returns the deployed contract address and transaction hash.
    pub async fn deploy_contract(&self, artifact: &ContractArtifact) -> Result<DeploymentResult> {
        self.deploy_contract_with_args(artifact, Bytes::new()).await
    }

    /// Deploy a contract with constructor arguments.
    ///
    /// The constructor_args should be ABI-encoded constructor parameters.
    /// Uses the default Anvil account for deployment.
    pub async fn deploy_contract_with_args(
        &self,
        artifact: &ContractArtifact,
        ctor_args: Bytes,
    ) -> Result<DeploymentResult> {
        tracing::debug!("Deploying contract");

        // Create a signing provider with the default wallet, using the same connection type
        let wallet = self.default_wallet()?;
        let provider = match &self.connection {
            AnvilConnection::Ipc { path, .. } => ProviderBuilder::new()
                .wallet(wallet)
                .connect_ipc(path.clone().into())
                .await
                .map_err(|err| {
                    anyhow!("Failed to connect to Anvil via IPC for deployment: {}", err)
                })?
                .erased(),
            AnvilConnection::Http { port } => {
                let url = format!("http://localhost:{}", port);
                ProviderBuilder::new()
                    .wallet(wallet)
                    .connect_http(url.parse()?)
                    .erased()
            }
        };

        // Construct deployment bytecode (contract bytecode + constructor args)
        let deploy_code = if ctor_args.is_empty() {
            artifact.bytecode.clone()
        } else {
            let mut combined = Vec::from(artifact.bytecode.as_ref());
            combined.extend_from_slice(&ctor_args);
            Bytes::from(combined)
        };

        // Create deployment transaction
        let tx = TransactionRequest::default().with_deploy_code(deploy_code);

        // Send transaction and wait for receipt
        let pending_tx = provider
            .send_transaction(tx)
            .await
            .map_err(|err| anyhow!("Failed to send deployment transaction: {}", err))?;

        let tx_hash = *pending_tx.tx_hash();

        let receipt = pending_tx
            .get_receipt()
            .await
            .map_err(|err| anyhow!("Failed to get deployment receipt: {}", err))?;

        let address = receipt.contract_address.ok_or(anyhow!(
            "Deployment receipt did not contain contract address"
        ))?;

        tracing::info!(
            address = %address,
            tx_hash = %tx_hash,
            "Contract deployed successfully"
        );

        Ok(DeploymentResult { address, tx_hash })
    }

    /// Call a contract function (read-only, no state change).
    ///
    /// Performs an `eth_call` to read data from a contract without
    /// sending a transaction or spending gas. The `data` parameter should
    /// be ABI-encoded function call data.
    pub async fn call(&self, to: Address, data: Bytes) -> Result<Bytes> {
        tracing::debug!(to = %to, "Calling contract");

        let tx = TransactionRequest::default().with_to(to).with_input(data);

        let result = self
            .provider
            .call(tx)
            .await
            .map_err(|err| anyhow!("Failed to call contract: {}", err))?;

        Ok(result)
    }

    /// Send a transaction to a contract (write operation, changes state).
    ///
    /// Uses the default Anvil account to sign and send the transaction.
    /// Waits for the transaction to be mined and returns the transaction hash.
    pub async fn send_transaction(&self, to: Address, data: Bytes) -> Result<TxHash> {
        self.send_transaction_with_value(to, data, U256::ZERO).await
    }

    /// Send a transaction with a specific ETH value.
    ///
    /// Uses the default Anvil account to sign and send the transaction.
    /// The `value` parameter specifies how much ETH to send with the transaction.
    pub async fn send_transaction_with_value(
        &self,
        to: Address,
        data: Bytes,
        value: U256,
    ) -> Result<TxHash> {
        tracing::debug!(to = %to, value = %value, "Sending transaction");

        // Create a signing provider with the default wallet
        let wallet = self.default_wallet()?;
        let provider = match &self.connection {
            AnvilConnection::Ipc { path, .. } => ProviderBuilder::new()
                .wallet(wallet)
                .connect_ipc(path.clone().into())
                .await
                .map_err(|err| anyhow!("Failed to connect to Anvil via IPC: {}", err))?
                .erased(),
            AnvilConnection::Http { port } => {
                let url = format!("http://localhost:{}", port);
                ProviderBuilder::new()
                    .wallet(wallet)
                    .connect_http(url.parse()?)
                    .erased()
            }
        };

        // Create transaction
        let tx = TransactionRequest::default()
            .with_to(to)
            .with_input(data)
            .with_value(value);

        // Send transaction and wait for receipt
        let pending_tx = provider
            .send_transaction(tx)
            .await
            .map_err(|err| anyhow!("Failed to send transaction: {}", err))?;

        let tx_hash = *pending_tx.tx_hash();

        pending_tx
            .get_receipt()
            .await
            .map_err(|err| anyhow!("Failed to get transaction receipt: {}", err))?;

        tracing::info!(tx_hash = %tx_hash, "Transaction mined successfully");

        Ok(tx_hash)
    }
}

impl Drop for Anvil {
    fn drop(&mut self) {
        tracing::debug!("Dropping Anvil fixture, Anvil instance will be terminated");
    }
}

/// Information about a blockchain block.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockInfo {
    pub block_num: BlockNum,
    pub hash: BlockHash,
    pub parent_hash: BlockHash,
}

/// Result of a successful contract deployment.
#[derive(Debug, Clone)]
pub struct DeploymentResult {
    /// The deployed contract's address.
    pub address: Address,
    /// The deployment transaction hash.
    pub tx_hash: TxHash,
}
