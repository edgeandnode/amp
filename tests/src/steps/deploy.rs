//! Test step for deploying dataset packages.

use common::BoxError;

use crate::testlib::{ctx::TestCtx, fixtures::DatasetPackage};

/// Test step that deploys and registers dataset packages.
///
/// This step handles the installation and registration of dataset packages
/// using the amp CLI. It supports optional configuration files.
#[derive(Debug, serde::Deserialize)]
pub struct Step {
    /// The name of this test step.
    pub name: String,
    /// The path or identifier of the dataset package to deploy.
    pub deploy: String,
    /// Optional configuration file path for the deployment.
    pub config: Option<String>,
}

impl Step {
    /// Installs and registers the specified dataset package.
    ///
    /// Creates a dataset package with the specified deploy path and optional
    /// configuration, then uses the amp CLI to install and register it.
    pub async fn run(&self, ctx: &TestCtx) -> Result<(), BoxError> {
        tracing::debug!("Deploying '{}'", self.deploy);

        let dataset_package = DatasetPackage::new(&self.deploy, self.config.as_deref());
        let cli = ctx.new_amp_cli();
        dataset_package.register(&cli).await
    }
}
