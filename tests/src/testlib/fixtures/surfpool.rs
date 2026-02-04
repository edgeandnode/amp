//! Surfpool fixture for local Solana blockchain testing.
//!
//! This fixture provides management of Surfpool (local Solana node backed by LiteSVM)
//! instances for testing Solana data extraction. Surfpool is spawned as a child process
//! that exposes a standard Solana JSON-RPC endpoint, analogous to how Anvil is used for
//! Ethereum testing.

use std::{
    net::TcpListener,
    process::{Child, Command},
    time::Duration,
};

use backon::{ConstantBuilder, Retryable as _};
use common::BoxError;
use solana_commitment_config::CommitmentConfig;
use solana_rpc_client::rpc_client::RpcClient;
use solana_sdk::{
    native_token::LAMPORTS_PER_SOL,
    pubkey::Pubkey,
    signature::{Keypair, Signature, Signer},
};

/// Default retry interval for Surfpool readiness checks.
const SURFPOOL_RETRY_INTERVAL: Duration = Duration::from_millis(200);

/// Fixture for managing Surfpool instances in tests.
///
/// This fixture wraps a Surfpool child process and provides convenient methods
/// for Solana blockchain operations. The child process is automatically killed
/// when the fixture is dropped.
pub struct Surfpool {
    child: Option<Child>,
    port: u16,
    rpc_client: RpcClient,
}

impl Surfpool {
    /// Create a new Surfpool fixture.
    ///
    /// Allocates a free port, spawns `surfpool start --ci --port <port> --offline` as
    /// a child process, and creates an RPC client connected to it.
    ///
    /// # Errors
    ///
    /// Returns an error if the `surfpool` binary is not found or fails to start.
    pub fn new() -> Result<Self, BoxError> {
        let port = allocate_free_port()?;

        let child = Command::new("surfpool")
            .args(["start", "--ci", "--port", &port.to_string(), "--offline"])
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .map_err(|err| {
                if err.kind() == std::io::ErrorKind::NotFound {
                    format!(
                        "Surfpool binary not found. Install with: \
                         brew install txtx/taps/surfpool (macOS) or from source. \
                         Error: {err}"
                    )
                } else {
                    format!("Failed to start Surfpool: {err}")
                }
            })?;

        let rpc_url = format!("http://127.0.0.1:{port}");
        let rpc_client = RpcClient::new_with_commitment(&rpc_url, CommitmentConfig::confirmed());

        Ok(Self {
            child: Some(child),
            port,
            rpc_client,
        })
    }

    /// Get the RPC URL for this Surfpool instance.
    pub fn rpc_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    /// Get the port this Surfpool instance is listening on.
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Get a reference to the underlying Solana RPC client.
    pub fn rpc_client(&self) -> &RpcClient {
        &self.rpc_client
    }

    /// Create a Solana provider configuration for this fixture.
    ///
    /// Returns a TOML string suitable for registration as a provider config.
    /// Uses `network = "localnet"`, `use_archive = "never"` (no Old Faithful),
    /// and a dummy `of1_car_directory` path.
    pub fn new_provider_config(&self) -> String {
        indoc::formatdoc! {r#"
            kind = "solana"
            network = "localnet"
            rpc_provider_url = "{url}"
            of1_car_directory = "/tmp/amp-test-of1"
            use_archive = "never"
        "#, url = self.rpc_url()}
    }

    /// Get the current slot number.
    pub fn latest_slot(&self) -> Result<u64, BoxError> {
        self.rpc_client
            .get_slot()
            .map_err(|err| format!("Failed to get slot: {err}").into())
    }

    /// Send a SOL transfer transaction.
    ///
    /// Transfers `lamports` from the given keypair to the given recipient.
    /// Returns the transaction signature.
    pub fn send_sol_transfer(
        &self,
        from: &Keypair,
        to: &Pubkey,
        lamports: u64,
    ) -> Result<Signature, BoxError> {
        let recent_blockhash = self
            .rpc_client
            .get_latest_blockhash()
            .map_err(|err| format!("Failed to get recent blockhash: {err}"))?;

        let tx = solana_system_transaction::transfer(from, to, lamports, recent_blockhash);

        self.rpc_client
            .send_and_confirm_transaction(&tx)
            .map_err(|err| format!("Failed to send SOL transfer: {err}").into())
    }

    /// Request an airdrop of SOL to the given address.
    ///
    /// Requests `lamports` SOL from Surfpool's built-in faucet and waits for
    /// the airdrop transaction to confirm.
    pub fn airdrop(&self, to: &Pubkey, lamports: u64) -> Result<Signature, BoxError> {
        let sig = self
            .rpc_client
            .request_airdrop(to, lamports)
            .map_err(|err| format!("Failed to request airdrop: {err}"))?;

        // Wait for the airdrop to confirm
        self.rpc_client
            .confirm_transaction(&sig)
            .map_err(|err| format!("Failed to confirm airdrop: {err}"))?;

        Ok(sig)
    }

    /// Wait for the Surfpool service to be ready and responsive.
    ///
    /// Polls `getSlot` with retry until Surfpool is accepting RPC requests.
    pub async fn wait_for_ready(&self, timeout: Duration) -> Result<(), BoxError> {
        tracing::debug!(port = self.port, "Waiting for Surfpool service to be ready");

        let rpc_url = self.rpc_url();

        (|| async {
            // Create a temporary client for each poll attempt since the server
            // may not be ready yet and we don't want stale connection state.
            let client = RpcClient::new(&rpc_url);
            match client.get_slot() {
                Ok(_) => Ok(()),
                Err(err) => Err(format!("Failed to get slot: {err}")),
            }
        })
        .retry(
            ConstantBuilder::default()
                .with_delay(SURFPOOL_RETRY_INTERVAL)
                .with_max_times(
                    timeout.as_millis() as usize / SURFPOOL_RETRY_INTERVAL.as_millis() as usize,
                ),
        )
        .sleep(tokio::time::sleep)
        .notify(|err, dur| {
            tracing::debug!(
                error = %err,
                "Surfpool not ready yet, retrying in {:.1}s",
                dur.as_secs_f32()
            );
        })
        .await
        .map_err(|err| -> BoxError {
            format!("Surfpool service did not become ready within {timeout:?}. Last error: {err}")
                .into()
        })?;

        tracing::info!(port = self.port, "Surfpool service is ready");
        Ok(())
    }

    /// Request an airdrop of 1 SOL to fund a test account, returning the funded keypair.
    ///
    /// Convenience method that generates a new keypair, airdrops 1 SOL to it, and returns
    /// the funded keypair for use in test transactions.
    pub fn fund_new_keypair(&self) -> Result<Keypair, BoxError> {
        let keypair = Keypair::new();
        self.airdrop(&keypair.pubkey(), LAMPORTS_PER_SOL)?;
        Ok(keypair)
    }
}

impl Drop for Surfpool {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            tracing::debug!(
                port = self.port,
                "Dropping Surfpool fixture, killing child process"
            );
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// Allocate a free port by binding to port 0, extracting the assigned port, and
/// closing the listener. There is a small TOCTOU window, but this is acceptable
/// for test fixtures.
fn allocate_free_port() -> Result<u16, BoxError> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .map_err(|err| format!("Failed to bind to port 0 for port allocation: {err}"))?;
    let port = listener
        .local_addr()
        .map_err(|err| format!("Failed to get local address: {err}"))?
        .port();
    drop(listener);
    Ok(port)
}
