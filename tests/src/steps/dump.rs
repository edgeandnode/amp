//! Test step for dumping dataset data to storage.

use std::time::Duration;

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
            let ampctl = ctx.new_ampctl();

            test_helpers::deploy_and_wait(
                &ampctl,
                &self.dataset,
                Some(self.end),
                Duration::from_secs(30),
            )
            .await
            .expect("Failed to dump dataset via worker");

            let data_store = ctx.daemon_server().data_store();
            let dataset_store = ctx.daemon_server().dataset_store();
            let physical_tables =
                test_helpers::load_physical_tables(dataset_store, data_store, &self.dataset)
                    .await?;
            for table in physical_tables {
                test_helpers::check_table_consistency(&table, data_store).await?;
            }

            Ok(())
        }
        .await;

        // Handle expected failure cases
        if let Some(expected_substring) = &self.failure {
            match result {
                Err(err) => {
                    let err_str = err.to_string();

                    // First check the main error message
                    if err_str.contains(expected_substring.trim()) {
                        return Ok(());
                    }

                    // If not found, traverse the error chain
                    let mut found = false;
                    let mut error_chain = vec![err_str.clone()];
                    let mut current_error = err.source();

                    while let Some(err) = current_error {
                        let err_str = err.to_string();
                        error_chain.push(err_str.clone());
                        if err_str.contains(expected_substring.trim()) {
                            found = true;
                            break;
                        }
                        current_error = err.source();
                    }

                    if !found {
                        let full_error_chain = error_chain.join("\n  caused by: ");
                        return Err(format!(
                            "Expected error to contain: \"{}\"\nActual error chain:\n  {}",
                            expected_substring.trim(),
                            full_error_chain
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
