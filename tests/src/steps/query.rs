//! Test step for executing SQL queries.

use anyhow::Result;

// Re-export SqlTestResult from stream_take for compatibility
pub use super::stream_take::SqlTestResult;
use crate::testlib::fixtures::FlightClient;

/// Test step that executes SQL queries and validates results.
///
/// This step runs SQL queries using the Flight client with optional
/// streaming configurations and validates the results against expected outcomes.
#[derive(Debug, serde::Deserialize)]
pub struct Step {
    /// The name of this test step.
    pub name: String,
    /// The SQL query to execute.
    pub query: String,
    /// The expected results for validation.
    #[serde(flatten)]
    pub result: SqlTestResult,
    /// Optional streaming configuration.
    #[serde(rename = "streamingOptions", default)]
    pub streaming_options: Option<StreamingOptions>,
}

impl Step {
    /// Executes the SQL query and validates the results.
    ///
    /// Runs the specified SQL query using the Flight client with optional
    /// streaming settings, then validates the actual results against expected results.
    pub async fn run(&self, client: &mut FlightClient) -> Result<()> {
        tracing::debug!(
            "Executing query: {} (streaming_options: {:?})",
            self.query,
            self.streaming_options
        );

        let actual_result = client
            .run_query(
                &self.query,
                self.streaming_options
                    .as_ref()
                    .and_then(|s| s.at_least_rows),
            )
            .await;

        self.result.assert_eq(actual_result)?;

        Ok(())
    }
}

/// Streaming options for query execution.
///
/// Configures streaming behavior for SQL queries, specifying minimum
/// row thresholds for streaming operations.
#[derive(Debug, serde::Deserialize)]
pub struct StreamingOptions {
    /// Minimum number of rows to return before streaming begins.
    #[serde(rename = "atLeastRows")]
    pub at_least_rows: Option<usize>,
}
