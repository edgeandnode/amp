//! Test step for initializing Surfpool Solana fixture.

use common::BoxError;

use crate::testlib::ctx::TestCtx;

/// Test step that validates Surfpool is available for the test.
///
/// This step serves as a marker that the test requires Surfpool and validates
/// that the test context was configured with Surfpool support. It should appear
/// before any `surfpool_send` or `surfpool_advance` steps in the YAML spec.
///
/// Note: The actual Surfpool instance must be configured via
/// `TestCtxBuilder::with_surfpool()` in the test harness. This step validates
/// that configuration.
#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Step {
    /// Surfpool configuration options (currently unused, reserved for future config).
    pub surfpool: SurfpoolConfig,
}

/// Configuration options for the Surfpool step.
#[derive(Debug, Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SurfpoolConfig {}

impl Step {
    /// Validates that Surfpool is available in the test context.
    ///
    /// This method checks that the test context was configured with Surfpool support.
    /// If Surfpool is not available, it panics with guidance on how to fix it.
    pub async fn run(&self, ctx: &TestCtx) -> Result<(), BoxError> {
        tracing::debug!("Validating Surfpool fixture is available");

        // This will panic if Surfpool is not configured, which is the expected behavior
        // to fail fast with a clear error message
        let _surfpool = ctx.surfpool();

        tracing::info!("Surfpool fixture validated successfully");
        Ok(())
    }
}
