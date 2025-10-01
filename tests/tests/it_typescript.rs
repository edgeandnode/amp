//! TypeScript SDK integration tests.
//!
//! This module runs the TypeScript SDK test suite by spawning isolated test infrastructure
//! (Anvil + Nozzle + PostgreSQL) for each TypeScript test file and invoking vitest to run
//! the tests against that infrastructure.
//!
//! Each test function corresponds to one TypeScript test file, allowing nextest to parallelize
//! across test files while maintaining isolation.

use std::{
    path::{Path, PathBuf},
    process::Stdio,
};

use common::BoxError;
use monitoring::logging;
use tests::testlib;

/// Run a single TypeScript test file with isolated infrastructure.
///
/// This function:
/// 1. Creates an isolated test environment with Anvil and Nozzle
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

    // Determine TypeScript workspace root relative to Rust workspace
    let workspace = get_typescript_workspace_path()?;
    let path: String = format!("packages/nozzle/test/{}", file);
    tracing::info!(
        "Running vitest in {} for test file {}",
        workspace.display(),
        path
    );

    // Run vitest with isolated infrastructure connection info
    let status = tokio::process::Command::new("pnpm")
        .args(&["vitest", "run", &path, "--no-file-parallelism"])
        .current_dir(&workspace)
        .env(
            "NOZZLE_ADMIN_URL",
            ctx.daemon_server().admin_api_server_url(),
        )
        .env("NOZZLE_JSONL_URL", ctx.daemon_server().jsonl_server_url())
        .env("NOZZLE_FLIGHT_URL", ctx.daemon_server().flight_server_url())
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

/// Get the path to the TypeScript workspace from the Rust workspace.
fn get_typescript_workspace_path() -> Result<PathBuf, BoxError> {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let workspace_root = manifest_dir
        .parent()
        .ok_or("Failed to find workspace root")?;
    Ok(workspace_root.join("typescript"))
}

#[tokio::test]
async fn typescript_basic() {
    logging::init();
    run_vitest_file("Nozzle.test.ts")
        .await
        .expect("Nozzle.test.ts failed");
}
