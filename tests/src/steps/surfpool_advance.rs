//! Test step for waiting until Surfpool reaches a target slot.
//!
//! Surfpool auto-produces blocks on a slot timer, so "advancing" means polling
//! until the current slot reaches at least the specified target. This is
//! analogous to `anvil_mine` for Ethereum but adapted to Solana's slot-based
//! progression.

use std::time::Duration;

use backon::{ConstantBuilder, Retryable as _};
use common::BoxError;

use crate::testlib::ctx::TestCtx;

/// Poll interval when waiting for slot advancement.
const POLL_INTERVAL: Duration = Duration::from_millis(200);

/// Maximum time to wait for slot advancement before timing out.
const ADVANCE_TIMEOUT: Duration = Duration::from_secs(30);

/// Test step that waits until Surfpool reaches a target slot.
///
/// Polls `getSlot` until the current slot is at least the specified value.
/// This is useful for ensuring Surfpool has produced enough blocks before
/// running extraction or query steps.
#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Step {
    /// The name of this test step.
    pub name: String,
    /// The target slot number to wait for.
    pub surfpool_advance: u64,
}

impl Step {
    /// Waits until Surfpool reaches the target slot.
    ///
    /// Polls `getSlot` with a 200ms interval until the current slot is at
    /// least `surfpool_advance`. Times out after 30 seconds.
    pub async fn run(&self, ctx: &TestCtx) -> Result<(), BoxError> {
        let target_slot = self.surfpool_advance;
        tracing::debug!(target_slot, "Waiting for Surfpool to reach target slot");

        let surfpool = ctx.surfpool();
        let max_retries = ADVANCE_TIMEOUT.as_millis() as usize / POLL_INTERVAL.as_millis() as usize;

        (|| async {
            let current_slot = surfpool.latest_slot()?;
            if current_slot >= target_slot {
                Ok(())
            } else {
                Err(format!("Current slot {current_slot} < target slot {target_slot}").into())
            }
        })
        .retry(
            ConstantBuilder::default()
                .with_delay(POLL_INTERVAL)
                .with_max_times(max_retries),
        )
        .sleep(tokio::time::sleep)
        .notify(|err: &BoxError, dur| {
            tracing::debug!(
                error = %err,
                "Target slot not reached yet, retrying in {:.1}s",
                dur.as_secs_f32()
            );
        })
        .await?;

        tracing::info!(target_slot, "Surfpool reached target slot");
        Ok(())
    }
}
