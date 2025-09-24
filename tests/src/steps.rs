use common::BoxError;
use fs_err as fs;

use crate::testlib::{
    ctx::TestCtx,
    fixtures::{DatasetPackage, FlightClient},
    helpers as test_helpers,
};

#[derive(Debug, serde::Deserialize)]
#[serde(untagged)]
pub(crate) enum TestStep {
    Dump(DumpStep),
    Stream(StreamStep),
    StreamTake(StreamTakeStep),
    Query(QueryStep),
    Restore(RestoreStep),
    Deploy(DeployStep),
    CleanDumpLocation(CleanDumpLocationStep),
}

#[derive(Debug, serde::Deserialize)]
pub struct DeployStep {
    pub name: String,
    pub deploy: String,
    pub config: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
pub struct RestoreStep {
    pub name: String,

    /// Name of the dataset to restore.
    pub restore: String,
}

#[derive(Debug, serde::Deserialize)]
pub struct CleanDumpLocationStep {
    pub clean_dump_location: String,
}

#[derive(Debug, serde::Deserialize)]
pub struct DumpStep {
    pub name: String,
    pub dataset: String,
    pub end: u64,
    #[serde(default)]
    pub expect_fail: bool,
}

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamStep {
    pub name: String,
    pub stream: String,
}

#[derive(Debug, serde::Deserialize)]
pub struct StreamTakeStep {
    pub name: String,
    pub stream: String,
    pub take: usize,
    #[serde(flatten)]
    pub results: SqlTestResult,
}

impl StreamTakeStep {
    pub async fn run(&self, client: &mut FlightClient) -> Result<(), BoxError> {
        let actual_result = client.take_from_stream(&self.stream, self.take).await;
        self.results.assert_eq(actual_result)
    }
}

#[derive(Debug, serde::Deserialize)]
pub struct QueryStep {
    pub name: String,
    /// SQL query to execute.
    pub query: String,
    /// JSON-encoded results.
    #[serde(flatten)]
    pub result: SqlTestResult,
    #[serde(rename = "streamingOptions", default)]
    pub streaming_options: Option<StreamingOptions>,
}

impl QueryStep {
    pub async fn run(&self, client: &mut FlightClient) -> Result<(), BoxError> {
        let actual_result = client
            .run_query(
                &self.query,
                self.streaming_options
                    .as_ref()
                    .map(|s| s.at_least_rows)
                    .flatten(),
            )
            .await;
        self.result.assert_eq(actual_result)
    }
}

#[derive(Debug, serde::Deserialize)]
pub struct StreamingOptions {
    #[serde(rename = "atLeastRows")]
    pub at_least_rows: Option<usize>,
}

impl TestStep {
    pub fn name(&self) -> &str {
        match self {
            TestStep::Dump(step) => &step.name,
            TestStep::StreamTake(step) => &step.name,
            TestStep::Query(step) => &step.name,
            TestStep::Stream(step) => &step.name,
            TestStep::Restore(step) => &step.name,
            TestStep::Deploy(step) => &step.name,
            TestStep::CleanDumpLocation(step) => &step.clean_dump_location,
        }
    }

    pub async fn run(&self, ctx: &TestCtx, client: &mut FlightClient) -> Result<(), BoxError> {
        let result = match self {
            TestStep::Dump(step) => {
                let result = async {
                    let physical_tables = test_helpers::dump_dataset(
                        ctx.daemon_server().config(),
                        ctx.metadata_db(),
                        &step.dataset,
                        step.end,
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

                if step.expect_fail {
                    assert!(result.is_err(), "Expected dump to fail, but it succeeded");
                    Ok(())
                } else {
                    result
                }
            }
            TestStep::StreamTake(step) => step.run(client).await,
            TestStep::Query(step) => step.run(client).await,
            TestStep::Stream(step) => client.register_stream(&step.name, &step.stream).await,
            TestStep::Restore(step) => {
                test_helpers::restore_dataset_snapshot(
                    ctx.daemon_server().config(),
                    ctx.metadata_db(),
                    ctx.daemon_server().dataset_store(),
                    &step.restore,
                )
                .await?;
                Ok(())
            }
            TestStep::Deploy(step) => {
                let dataset_package = DatasetPackage::new(&step.deploy, step.config.as_deref());
                let cli = ctx.new_nozzl_cli();
                dataset_package.install(&cli).await?;
                dataset_package.register(&cli).await
            }
            TestStep::CleanDumpLocation(step) => {
                let mut path =
                    std::path::PathBuf::from(ctx.daemon_server().config().data_store.url().path());
                path.push(&step.clean_dump_location);
                if path.exists() {
                    fs::remove_dir_all(path)?;
                }
                Ok(())
            }
        };

        if result.is_err() {
            Err(format!("Test step \"{}\" failed: {:?}", self.name(), result).into())
        } else {
            Ok(())
        }
    }
}

pub(crate) fn load_test_steps(file_name: &str) -> Result<Vec<TestStep>, BoxError> {
    let crate_path = env!("CARGO_MANIFEST_DIR");
    let path = format!("{crate_path}/specs/{file_name}");
    let content = fs::read(&path)
        .map_err(|err| BoxError::from(format!("Failed to read {file_name}: {err}")))?;
    serde_yaml::from_slice(&content)
        .map_err(|err| BoxError::from(format!("Failed to parse {file_name}: {err}")))
}

#[derive(Debug, Clone, serde::Deserialize)]
#[serde(untagged)]
pub enum SqlTestResult {
    Success { results: String },
    Failure { failure: String },
}

impl SqlTestResult {
    fn assert_eq(
        &self,
        actual_result: Result<serde_json::Value, BoxError>,
    ) -> Result<(), BoxError> {
        match self {
            SqlTestResult::Success {
                results: expected_json_str,
            } => {
                let expected: serde_json::Value = serde_json::from_str(expected_json_str)
                    .unwrap_or_else(|e| panic!("failed to parse expected JSON: {e}"));

                let actual = actual_result.expect(&format!("expected success, got error",));

                pretty_assertions::assert_str_eq!(
                    actual.to_string(),
                    expected.to_string(),
                    "Test returned unexpected results",
                );
            }

            SqlTestResult::Failure { failure } => {
                let expected_substring = failure.trim();

                let actual_error =
                    actual_result.expect_err(&format!("expected failure, got success"));

                let actual_error_str = actual_error.to_string();

                if !actual_error_str.contains(expected_substring) {
                    panic!(
                        "Expected substring: \"{}\"\nActual error: \"{}\"",
                        expected_substring, actual_error_str
                    );
                }
            }
        }
        Ok(())
    }
}
