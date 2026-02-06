//! Test step for restoring dataset snapshots.

use anyhow::Result;
use datasets_common::reference::Reference;

use crate::testlib::{ctx::TestCtx, helpers as test_helpers};

/// Test step that restores dataset snapshots from storage.
///
/// This step loads previously dumped dataset snapshots back into the system,
/// allowing tests to work with pre-existing data states.
#[derive(Debug, serde::Deserialize)]
pub struct Step {
    /// The name of this test step.
    pub name: String,
    /// The name of the dataset snapshot to restore.
    #[serde(rename = "restore")]
    pub snapshot_name: Reference,
}

impl Step {
    /// Restores the specified dataset snapshot.
    ///
    /// Uses the test helper functions to restore a dataset snapshot from
    /// storage back into the metadata database via the Admin API.
    pub async fn run(&self, ctx: &TestCtx) -> Result<()> {
        tracing::debug!("Restoring dataset snapshot '{}'", self.snapshot_name);

        let ampctl = ctx.new_ampctl();
        test_helpers::restore_dataset_snapshot(
            &ampctl,
            ctx.daemon_controller().dataset_store(),
            ctx.daemon_server().data_store(),
            &self.snapshot_name,
        )
        .await?;

        Ok(())
    }
}
