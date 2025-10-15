//! TypeScript SDK integration tests.
//!
//! This module runs the TypeScript SDK test suite by spawning isolated test infrastructure
//! (Anvil + Amp + PostgreSQL) for each TypeScript test file and invoking vitest to run
//! the tests against that infrastructure.
//!
//! Each test function corresponds to one TypeScript test file, allowing nextest to parallelize
//! across test files while maintaining isolation.

use std::process::Stdio;

use common::BoxError;
use monitoring::logging;

use crate::testlib;

/// Run a single TypeScript test file with isolated infrastructure.
///
/// This function:
/// 1. Creates an isolated test environment with Anvil and Amp
/// 2. Invokes vitest to run the specified test file
/// 3. Passes connection information via environment variables
/// 4. Streams vitest output to stdout/stderr
/// 5. Returns an error if the test fails
async fn run_vitest_file(file: &str) -> Result<(), BoxError> {
    // Extract test name for directory naming
    let name = file
        .trim_end_matches(".test.ts")
        .replace("/", "_")
        .replace(".", "_");

    // Create isolated test infrastructure using HTTP with auto-allocated port (avoids port conflicts!)
    // This allows forge scripts to work properly (forge doesn't support IPC)
    let ctx = testlib::ctx::TestCtxBuilder::new(&format!("typescript_{}", name))
        .with_dataset_manifest("anvil_rpc")
        .with_anvil_http()
        .build()
        .await?;

    let path: String = format!("typescript/amp/test/{}", file);
    tracing::info!("Running vitest for test file {}", path);

    // Run vitest with isolated infrastructure connection info
    let status = tokio::process::Command::new("pnpm")
        .args(&["vitest", "run", &path, "--no-file-parallelism"])
        .env("AMP_ADMIN_URL", ctx.daemon_controller().admin_api_url())
        .env("AMP_JSONL_URL", ctx.daemon_server().jsonl_server_url())
        .env("AMP_FLIGHT_URL", ctx.daemon_server().flight_server_url())
        .env("ANVIL_RPC_URL", ctx.anvil().connection_url())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .await?;

    if !status.success() {
        return Err(format!("Test file {} failed with status: {}", file, status).into());
    }

    tracing::info!("Test file {} completed successfully", file);
    Ok(())
}

#[tokio::test]
async fn typescript_basic() {
    logging::init();
    run_vitest_file("Amp.test.ts")
        .await
        .expect("Amp.test.ts failed");
}
