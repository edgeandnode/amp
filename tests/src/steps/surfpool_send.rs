//! Test step for sending SOL transfer transactions on Surfpool.

use common::BoxError;

use crate::testlib::ctx::TestCtx;

/// Test step that sends a SOL transfer transaction on Surfpool.
///
/// Funds a new keypair via airdrop, then sends a SOL transfer to a second
/// new keypair. This produces on-chain activity (transactions, instructions,
/// messages) that can be extracted by the Solana pipeline.
///
/// The number of transfers to send is configurable via the `surfpool_send`
/// field, allowing tests to generate a controlled amount of on-chain data.
#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Step {
    /// The name of this test step.
    pub name: String,
    /// The number of SOL transfers to send.
    pub surfpool_send: u64,
}

/// Amount of lamports to transfer in each test transaction (0.001 SOL).
const TRANSFER_LAMPORTS: u64 = 1_000_000;

impl Step {
    /// Sends the specified number of SOL transfers on Surfpool.
    ///
    /// For each transfer: funds a new sender keypair via airdrop, generates a
    /// new recipient, and sends a 0.001 SOL transfer. Each transfer produces
    /// rows in the `block_headers`, `transactions`, `messages`, and
    /// `instructions` tables.
    pub async fn run(&self, ctx: &TestCtx) -> Result<(), BoxError> {
        let count = self.surfpool_send;
        tracing::debug!(count, "Sending SOL transfers");

        let surfpool = ctx.surfpool();

        for i in 0..count {
            let sender = surfpool.fund_new_keypair()?;
            let recipient = solana_sdk::pubkey::Pubkey::new_unique();

            let sig = surfpool.send_sol_transfer(&sender, &recipient, TRANSFER_LAMPORTS)?;
            tracing::debug!(
                transfer = i + 1,
                total = count,
                signature = %sig,
                "SOL transfer sent"
            );
        }

        tracing::info!(count, "Successfully sent all SOL transfers");
        Ok(())
    }
}
