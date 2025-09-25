//! Test step for taking data from registered streams.

use common::BoxError;

use crate::testlib::fixtures::FlightClient;

/// Test step that takes a specified number of rows from a registered stream.
///
/// This step retrieves data from a previously registered stream and validates
/// the results against expected outcomes.
#[derive(Debug, serde::Deserialize)]
pub struct Step {
    /// The name of this test step.
    pub name: String,
    /// The name of the stream to take data from.
    pub stream: String,
    /// The number of rows to take from the stream.
    pub take: usize,
    /// The expected results for validation.
    #[serde(flatten)]
    pub results: SqlTestResult,
}

impl Step {
    /// Takes data from the stream and validates the results.
    ///
    /// Retrieves the specified number of rows from the named stream using
    /// the Flight client, then validates the actual results against expected results.
    pub async fn run(&self, client: &mut FlightClient) -> Result<(), BoxError> {
        tracing::debug!("Taking {} rows from stream '{}'", self.take, self.stream);

        let actual_result = client.take_from_stream(&self.stream, self.take).await;
        let result = self.results.assert_eq(actual_result);

        result
    }
}

/// Test result validation for SQL operations.
///
/// Represents either expected success with specific results or expected
/// failure with a specific error message substring.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(untagged)]
pub enum SqlTestResult {
    /// Expected successful result with JSON-encoded data.
    Success { results: String },
    /// Expected failure with a specific error message substring.
    Failure { failure: String },
}

impl SqlTestResult {
    /// Validates actual results against expected results.
    ///
    /// For success cases, compares JSON-serialized results for equality.
    /// For failure cases, checks that the error message contains the expected substring.
    pub(crate) fn assert_eq(
        &self,
        actual_result: Result<serde_json::Value, BoxError>,
    ) -> Result<(), BoxError> {
        match self {
            SqlTestResult::Success {
                results: expected_json_str,
            } => {
                let expected: serde_json::Value = serde_json::from_str(expected_json_str)
                    .unwrap_or_else(|err| panic!("failed to parse expected JSON: {err}"));
                let actual = actual_result.expect("expected success, got error");

                pretty_assertions::assert_str_eq!(
                    actual.to_string(),
                    expected.to_string(),
                    "Test returned unexpected results",
                );
            }

            SqlTestResult::Failure { failure } => {
                let expected_substring = failure.trim();
                let actual_error = actual_result.expect_err("expected failure, got success");

                if !actual_error.to_string().contains(expected_substring) {
                    panic!(
                        "Expected substring: \"{}\"\nActual error: \"{}\"",
                        expected_substring,
                        actual_error.to_string()
                    );
                }
            }
        }
        Ok(())
    }
}
