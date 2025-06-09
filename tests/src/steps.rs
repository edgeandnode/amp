use common::{arrow::json, BoxError};
use fs_err as fs;
use serde::Deserialize;

use crate::{
    test_client::TestClient,
    test_support::{dump_dataset, SqlTestResult, TestEnv},
};

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub(crate) enum TestStep {
    Dump(DumpStep),
    Stream(StreamStep),
    StreamTake(StreamTakeStep),
    Query(QueryStep),
}

#[derive(Debug, Deserialize)]
pub struct DumpStep {
    pub name: String,
    pub dataset: String,
    pub start: u64,
    pub end: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamStep {
    pub name: String,
    pub stream: String,
}

#[derive(Debug, Deserialize)]
pub struct StreamTakeStep {
    pub name: String,
    pub stream: String,
    pub take: usize,
    #[serde(flatten)]
    pub results: SqlTestResult,
}

impl StreamTakeStep {
    pub async fn run(&self, client: &mut TestClient) -> Result<(), BoxError> {
        let actual_result = {
            let batch = client.take_from_stream(&self.stream, self.take).await;

            let mut buf = Vec::new();
            let mut writer = json::ArrayWriter::new(&mut buf);
            writer.write(&batch?)?;
            writer.finish()?;
            Ok(serde_json::from_slice(&buf)?)
        };

        self.results.assert_eq(actual_result)?;
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
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
    pub async fn run(&self, client: &mut TestClient) -> Result<(), BoxError> {
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

#[derive(Debug, Deserialize)]
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
        }
    }

    pub async fn run(&self, test_env: &TestEnv, client: &mut TestClient) -> Result<(), BoxError> {
        let result = match self {
            TestStep::Dump(step) => {
                let config = test_env.config.clone();
                dump_dataset(config, &step.dataset, step.start, step.end, 1).await
            }
            TestStep::StreamTake(step) => step.run(client).await,
            TestStep::Query(step) => step.run(client).await,
            TestStep::Stream(step) => client.register_stream(&step.name, &step.stream).await,
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
    let content =
        fs::read(&path).map_err(|e| BoxError::from(format!("Failed to read {file_name}: {e}")))?;
    serde_yaml::from_slice(&content)
        .map_err(|e| BoxError::from(format!("Failed to parse {file_name}: {e}")))
}
