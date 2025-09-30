//! Test step for dumping dataset data to storage.

use common::BoxError;

use crate::testlib::{ctx::TestCtx, helpers as test_helpers};

/// Test step that dumps dataset data from blockchain sources to storage.
///
/// This step extracts blockchain data for a specified dataset up to a given
/// block number and validates the consistency of the resulting data tables.
/// It supports expecting failure scenarios for negative testing.
#[derive(Debug, serde::Deserialize)]
pub struct Step {
    /// The name of this test step.
    pub name: String,
    /// The name of the dataset to dump.
    pub dataset: String,
    /// The ending block number for the dump operation.
    pub end: u64,
    /// Whether this dump operation is expected to fail.
    #[serde(default)]
    pub expect_fail: bool,
}

impl Step {
    /// Executes the dataset dump operation.
    ///
    /// Performs the dump operation using test helpers, validates table consistency,
    /// and handles expected failure scenarios. If expect_fail is true, the step
    /// succeeds only if the dump operation fails.
    pub async fn run(&self, ctx: &TestCtx) -> Result<(), BoxError> {
        tracing::debug!(
            "Dumping dataset '{}' up to block {}, expect_fail={}",
            self.dataset,
            self.end,
            self.expect_fail
        );

        let result = async {
            let physical_tables = test_helpers::dump_dataset(
                ctx.daemon_server().config(),
                ctx.metadata_db(),
                &self.dataset,
                self.end,
                1,
                None,
            )
            .await?;

            for table in physical_tables {
                test_helpers::check_table_consistency(&table).await?;
            }

            Ok(())
        }
        .await;

        if self.expect_fail {
            assert!(result.is_err(), "Expected dump to fail, but it succeeded");
            Ok(())
        } else {
            result
        }
    }
}
