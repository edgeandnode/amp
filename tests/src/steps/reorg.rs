//! Test step for triggering blockchain reorganizations on Anvil.

use common::BoxError;

use crate::testlib::ctx::TestCtx;

/// Test step that triggers a blockchain reorganization on Anvil.
///
/// This step simulates a chain reorganization by replacing the last N blocks
/// with alternative blocks. This is useful for testing reorg handling in
/// streaming queries and data synchronization.
#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Step {
    /// The name of this test step.
    pub name: String,
    /// The depth of the reorganization (number of blocks to replace).
    pub reorg: u64,
}

impl Step {
    /// Triggers a blockchain reorganization with the specified depth.
    ///
    /// Uses the Anvil fixture from the test context to trigger a reorg.
    /// Requires that the test context was configured with Anvil support.
    pub async fn run(&self, ctx: &TestCtx) -> Result<(), BoxError> {
        tracing::debug!("Triggering reorg with depth {}", self.reorg);

        ctx.anvil().reorg(self.reorg).await?;

        tracing::info!("Successfully triggered reorg with depth {}", self.reorg);
        Ok(())
    }
}
