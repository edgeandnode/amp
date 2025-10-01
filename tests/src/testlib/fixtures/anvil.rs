//! Anvil fixture for local Ethereum blockchain testing.
//!
//! This fixture provides management of Anvil (local Ethereum node) instances for testing
//! blockchain interactions. It supports both IPC and HTTP connection modes and provides
//! convenient methods for mining blocks, triggering reorganizations, and other blockchain
//! operations needed for testing.

use std::time::Duration;

use alloy::{
    eips::BlockId,
    node_bindings::{Anvil as AlloyAnvil, AnvilInstance},
    primitives::BlockHash,
    providers::{Provider, ext::AnvilApi},
    rpc::types::anvil::ReorgOptions,
};
use backon::{ConstantBuilder, Retryable};
use common::{BlockNum, BoxError};
use tempfile::NamedTempFile;

/// Default retry interval for Anvil readiness checks.
const ANVIL_RETRY_INTERVAL: Duration = Duration::from_millis(200);

/// Information about a blockchain block.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockInfo {
    pub block_num: BlockNum,
    pub hash: BlockHash,
    pub parent_hash: BlockHash,
}

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
    pub async fn new_ipc() -> Result<Self, BoxError> {
        let temp_file = tempfile::Builder::new()
            .prefix("anvil")
            .tempfile()
            .map_err(|err| format!("Failed to create temp file for Anvil IPC: {}", err))?;

        let ipc_path = temp_file.path().to_string_lossy().to_string();

        let instance = AlloyAnvil::new().ipc_path(&ipc_path).spawn();

        let provider = alloy::providers::ProviderBuilder::new()
            .connect_ipc(ipc_path.clone().into())
            .await
            .map_err(|err| format!("Failed to connect to Anvil via IPC: {}", err))?
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
    pub async fn new_http(port: u16) -> Result<Self, BoxError> {
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

    /// Mine the specified number of blocks.
    ///
    /// This instructs Anvil to mine new blocks, which is useful for advancing
    /// the blockchain state in tests.
    pub async fn mine(&self, blocks: u64) -> Result<(), BoxError> {
        tracing::info!(blocks, "Mining blocks");
        self.provider
            .anvil_mine(Some(blocks), None)
            .await
            .map_err(|err| format!("Failed to mine blocks: {}", err))?;
        Ok(())
    }

    /// Trigger a blockchain reorganization with the specified depth.
    ///
    /// This causes Anvil to reorganize the blockchain by replacing the last
    /// `depth` blocks with alternative blocks, simulating a chain reorg.
    pub async fn reorg(&self, depth: u64) -> Result<(), BoxError> {
        if depth == 0 {
            return Err("Reorg depth must be greater than 0".into());
        }

        tracing::info!(depth, "Triggering blockchain reorg");

        let original_head = self.latest_block().await?;

        self.provider
            .anvil_reorg(ReorgOptions {
                depth,
                tx_block_pairs: vec![],
            })
            .await
            .map_err(|err| format!("Failed to trigger reorg: {}", err))?;

        let new_head = self.latest_block().await?;

        // Verify the reorg happened as expected
        if original_head.block_num != new_head.block_num {
            return Err(format!(
                "Reorg failed: block number changed from {} to {}",
                original_head.block_num, new_head.block_num
            )
            .into());
        }

        if original_head.hash == new_head.hash {
            return Err("Reorg failed: block hash did not change".into());
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
    pub async fn latest_block(&self) -> Result<BlockInfo, BoxError> {
        let block = self
            .provider
            .get_block(BlockId::latest())
            .await
            .map_err(|err| format!("Failed to get latest block: {}", err))?
            .ok_or("Latest block not found")?;

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
    pub async fn wait_for_ready(&self, timeout: Duration) -> Result<(), BoxError> {
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
        .map_err(|err| -> BoxError {
            format!(
                "Anvil service did not become ready within {:?}. Last error: {}",
                timeout, err
            )
            .into()
        })?;

        tracing::info!("Anvil service is ready");
        Ok(())
    }
}

impl Drop for Anvil {
    fn drop(&mut self) {
        tracing::debug!("Dropping Anvil fixture, Anvil instance will be terminated");
    }
}
