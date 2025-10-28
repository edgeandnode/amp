//! Test step definitions and execution framework.
//!
//! This module provides a framework for defining and executing test steps
//! in integration tests. Each step type is defined in its own submodule
//! with specific functionality for different testing scenarios.

use std::path::PathBuf;

use fs_err as fs;

// Submodules of the step implementations
mod clean_dump_location;
mod dump;
mod query;
mod register;
mod restore;
mod stream;
mod stream_take;

use crate::testlib::{ctx::TestCtx, fixtures::FlightClient};

/// Enumeration of all supported test step types.
///
/// Each variant corresponds to a specific type of test operation,
/// from data dumping and restoration to query execution and stream processing.
#[derive(Debug, serde::Deserialize)]
#[serde(untagged)]
pub enum TestStep {
    /// Dump dataset data to storage.
    Dump(dump::Step),
    /// Register a stream with the client.
    Stream(stream::Step),
    /// Take data from a registered stream.
    StreamTake(stream_take::Step),
    /// Execute SQL query.
    Query(query::Step),
    /// Restore dataset snapshot.
    Restore(restore::Step),
    /// Register dataset package.
    Register(register::Step),
    /// Clean dump location directory.
    CleanDumpLocation(clean_dump_location::Step),
}

impl TestStep {
    /// Gets the name of the test step.
    ///
    /// Returns the step name for logging and identification purposes.
    /// Note that CleanDumpLocation steps use their location as the name.
    pub fn name(&self) -> &str {
        match self {
            TestStep::Dump(step) => &step.name,
            TestStep::StreamTake(step) => &step.name,
            TestStep::Query(step) => &step.name,
            TestStep::Stream(step) => &step.name,
            TestStep::Restore(step) => &step.name,
            TestStep::Register(step) => &step.name,
            TestStep::CleanDumpLocation(step) => &step.clean_dump_location,
        }
    }

    /// Executes the test step.
    ///
    /// Dispatches to the appropriate step implementation based on the step type,
    /// with comprehensive logging and error handling.
    pub async fn run(&self, ctx: &TestCtx, client: &mut FlightClient) -> Result<(), TestStepError> {
        let result = match self {
            TestStep::Dump(step) => step.run(ctx).await,
            TestStep::StreamTake(step) => step.run(client).await,
            TestStep::Query(step) => step.run(client).await,
            TestStep::Stream(step) => step.run(client).await,
            TestStep::Restore(step) => step.run(ctx).await,
            TestStep::Register(step) => step.run(ctx).await,
            TestStep::CleanDumpLocation(step) => step.run(ctx).await,
        };

        match result {
            Ok(()) => {
                tracing::trace!("Test step '{}' completed successfully", self.name());
                Ok(())
            }
            Err(err) => {
                tracing::trace!("Test step '{}' failed: {:?}", self.name(), err);
                Err(TestStepError {
                    name: self.name().to_string(),
                    source: err,
                })
            }
        }
    }
}

/// Loads test steps from a YAML specification file.
///
/// Reads and parses a YAML file containing test step definitions from the
/// specs directory relative to the crate manifest directory.
pub fn load_test_spec(name: &str) -> Result<Vec<TestStep>, LoadTestSpecError> {
    let crate_root_path = env!("CARGO_MANIFEST_DIR");
    let mut spec_path = PathBuf::from(format!("{crate_root_path}/specs/{name}"));
    spec_path.set_extension("yaml");

    let content = fs::read(&spec_path).map_err(|source| LoadTestSpecError::ReadError {
        name: name.to_string(),
        source,
    })?;

    let steps =
        serde_yaml::from_slice(&content).map_err(|source| LoadTestSpecError::ParseError {
            name: name.to_string(),
            source,
        })?;
    Ok(steps)
}

/// Error type for test step execution failures.
#[derive(Debug, thiserror::Error)]
#[error("Test step '{name}' failed")]
pub struct TestStepError {
    /// Name of the test step that failed.
    pub name: String,
    /// Source error that caused the step to fail.
    pub source: Box<dyn std::error::Error + Send + Sync>,
}

/// Error types for test specification loading.
#[derive(Debug, thiserror::Error)]
pub enum LoadTestSpecError {
    /// Failed to read the specification file.
    #[error("Failed to read spec file '{name}'")]
    ReadError {
        name: String,
        source: std::io::Error,
    },
    /// Failed to parse the YAML content.
    #[error("Failed to parse spec file '{name}'")]
    ParseError {
        name: String,
        source: serde_yaml::Error,
    },
}

/// Internal macro to panic with detailed error messages including full error chain.
///
/// Walks through the error source chain and formats a comprehensive panic message.
#[macro_export]
macro_rules! fail_with_error {
    ($err:expr, $prefix:expr) => {{
        let err_chain = $crate::steps::build_error_chain(&$err);
        panic!("{}: {}{}", $prefix, $err, err_chain);
    }};
}

/// Macro to run test specification steps.
///
/// Loads a test specification from YAML and executes all its steps sequentially.
/// Panics with detailed error messages including full error chain if the spec cannot be loaded.
#[macro_export]
macro_rules! run_spec {
    ($spec_name:expr, ($test_ctx:expr, $client:expr)) => {
        let steps = match $crate::steps::load_test_spec($spec_name) {
            Ok(steps) => steps,
            Err(err) => $crate::fail_with_error!(err, "Failed to load test spec"),
        };

        for step in steps {
            if let Err(err) = step.run($test_ctx, $client).await {
                $crate::fail_with_error!(err, "Failed to execute step");
            }
        }
    };
    ($spec_name:expr, ($test_ctx:expr, $client:expr), delay = $duration:expr) => {
        let steps = match $crate::steps::load_test_spec($spec_name) {
            Ok(steps) => steps,
            Err(err) => $crate::fail_with_error!(err, "Failed to load test spec"),
        };

        for step in steps {
            if let Err(err) = step.run($test_ctx, $client).await {
                $crate::fail_with_error!(err, "Failed to execute step");
            }

            ::tokio::time::sleep($duration).await;
        }
    };
}

/// Builds an error chain string from an error and its sources.
///
/// Walks through the error source chain and returns a formatted string
/// containing the chain of error causes.
#[doc(hidden)]
pub fn build_error_chain(err: &dyn std::error::Error) -> String {
    let mut error_chain = Vec::new();

    let mut current = err;
    while let Some(source) = current.source() {
        error_chain.push(source.to_string());
        current = source;
    }

    if error_chain.is_empty() {
        String::new()
    } else {
        format!(" | Caused by: {}", error_chain.join(" -> "))
    }
}
