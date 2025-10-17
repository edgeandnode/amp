//! Test step for dumping dataset data to storage.

use common::BoxError;
use datasets_common::reference::Reference;

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
    pub dataset: Reference,
    /// The ending block number for the dump operation.
    pub end: u64,
    /// Expected failure message substring (if dump should fail).
    pub failure: Option<String>,
}

impl Step {
    /// Executes the dataset dump operation.
    ///
    /// Performs the dump operation using test helpers, validates table consistency,
    /// and handles expected failure scenarios. If failure is specified, the step
    /// succeeds only if the dump operation fails with the expected error.
    pub async fn run(&self, ctx: &TestCtx) -> Result<(), BoxError> {
        tracing::debug!(
            "Dumping dataset '{}' up to block {}, failure={:?}",
            self.dataset,
            self.end,
            self.failure
        );

        let result: Result<(), BoxError> = async {
            let physical_tables = test_helpers::dump_dataset(
                ctx.daemon_server().config(),
                ctx.metadata_db(),
                self.dataset.clone(),
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

        // Handle expected failure cases
        if let Some(expected_substring) = &self.failure {
            match result {
                Err(actual_error) => {
                    let actual_error_str = actual_error.to_string();
                    if !actual_error_str.contains(expected_substring.trim()) {
                        return Err(format!(
                            "Expected error to contain: \"{}\"\nActual error: \"{}\"",
                            expected_substring.trim(),
                            actual_error_str
                        )
                        .into());
                    }
                    Ok(())
                }
                Ok(_) => Err("Expected dump to fail, but it succeeded".into()),
            }
        } else {
            result
        }
    }
}
