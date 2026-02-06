//! Test step for initializing Anvil blockchain fixture.

use anyhow::Result;

use crate::testlib::ctx::TestCtx;

/// Test step that validates Anvil is available for the test.
///
/// This step serves as a marker that the test requires Anvil and validates
/// that the test context was configured with Anvil support. It should appear
/// before any `mine` or `reorg` steps in the YAML spec.
///
/// Note: The actual Anvil instance must be configured via `TestCtxBuilder::with_anvil_ipc()`
/// or `with_anvil_http()` in the test harness. This step validates that configuration.
#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Step {
    /// Anvil configuration options.
    pub anvil: AnvilConfig,
}

/// Configuration options for the Anvil step.
#[derive(Debug, Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AnvilConfig {
    /// Connection mode: "ipc" (default) or "http".
    #[serde(default)]
    pub mode: Option<String>,
}

impl Step {
    /// Validates that Anvil is available in the test context.
    ///
    /// This method checks that the test context was configured with Anvil support.
    /// If Anvil is not available, it returns an error with guidance on how to fix it.
    pub async fn run(&self, ctx: &TestCtx) -> Result<()> {
        tracing::debug!("Validating Anvil fixture is available");

        // This will panic if Anvil is not configured, which is the expected behavior
        // to fail fast with a clear error message
        let _anvil = ctx.anvil();

        tracing::info!("Anvil fixture validated successfully");
        Ok(())
    }
}
