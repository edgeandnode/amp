//! Test step for deploying dataset packages.

use crate::{
    BoxError,
    testlib::{ctx::TestCtx, fixtures::DatasetPackage},
};

/// Test step that deploys and registers dataset packages.
///
/// This step handles the installation and registration of dataset packages
/// using the amp CLI. It supports optional configuration files.
#[derive(Debug, serde::Deserialize)]
pub struct Step {
    /// The name of this test step.
    pub name: String,
    /// The path or identifier of the dataset package to deploy.
    pub dataset: String,
    /// Optional configuration file path for the deployment.
    pub config: Option<String>,
    /// Optional version tag for the dataset registration
    #[serde(default)]
    pub tag: Option<String>,
    /// Expected failure message substring (if registration should fail).
    pub failure: Option<String>,
}

impl Step {
    /// Installs and registers the specified dataset package.
    ///
    /// Creates a dataset package with the specified deploy path and optional
    /// configuration, then uses the amp CLI to install and register it.
    /// If failure is specified, the step succeeds only if the registration
    /// fails with the expected error.
    pub async fn run(&self, ctx: &TestCtx) -> Result<(), BoxError> {
        tracing::debug!(
            "Registering dataset '{}' (tag: {:?}, failure={:?})",
            self.dataset,
            self.tag,
            self.failure
        );

        let dataset_package = DatasetPackage::new(&self.dataset, self.config.as_deref());
        let cli = ctx.new_amp_cli();
        let result = dataset_package.register(&cli, self.tag.as_deref()).await;

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
                Ok(_) => Err("Expected registration to fail, but it succeeded".into()),
            }
        } else {
            result
        }
    }
}
