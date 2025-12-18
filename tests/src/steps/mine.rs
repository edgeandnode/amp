//! Test step for mining blocks on Anvil.

use common::BoxError;

use crate::testlib::ctx::TestCtx;

/// Test step that mines blocks on the Anvil blockchain.
///
/// This step instructs Anvil to mine a specified number of blocks, advancing
/// the blockchain state. This is useful for generating test data and simulating
/// blockchain progression.
#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Step {
    /// The name of this test step.
    pub name: String,
    /// The number of blocks to mine.
    pub mine: u64,
}

impl Step {
    /// Mines the specified number of blocks on Anvil.
    ///
    /// Uses the Anvil fixture from the test context to mine blocks.
    /// Requires that the test context was configured with Anvil support.
    pub async fn run(&self, ctx: &TestCtx) -> Result<(), BoxError> {
        tracing::debug!("Mining {} blocks", self.mine);

        ctx.anvil().mine(self.mine).await?;

        tracing::info!("Successfully mined {} blocks", self.mine);
        Ok(())
    }
}
